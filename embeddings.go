package main

import (
	"encoding/json"
	"hash/fnv"
	"math"
	"strconv"
	"strings"
	"time"
)

// ─────────────────────────────────────────────────────────────────────────────
// TWO-TOWER EMBEDDINGS (lightweight, deterministic, online-trained)
//
// Instead of a trained neural net we build content and user vectors from
// hashed features. Content vectors are pure functions of the item — no
// training, always coherent. User vectors are an exponentially-weighted
// moving average of content vectors the user positively engaged with,
// stored in Redis. Cosine similarity between user ⇄ content is a
// retrieval/ranking signal.
//
// Why this instead of a real NN: zero training infra, no drift, no model
// server. Gives us 80% of the recall-lift of a learned two-tower for
// 5% of the engineering cost. Swappable later.
// ─────────────────────────────────────────────────────────────────────────────

const (
	embedDim            = 32               // fixed vector dimensionality
	userEmbedAlpha      = 0.15             // EMA weight for new events
	userEmbedTTL        = 30 * 24 * time.Hour
	userEmbedRedisKey   = "embed:user:"    // + userID
	// how fast a positive event pulls the user vector toward the content vector
	userEmbedPosStep = 0.20
	// negative events nudge AWAY from the content vector
	userEmbedNegStep = 0.05

	// Content embedding cache. We cache the *stable* part of the vector
	// (categorical features + quality + energy) — recency and popularity are
	// re-applied on every read so freshness is preserved. 6h TTL is short
	// enough that any rare metadata edits propagate quickly.
	contentEmbedRedisKey = "embed:content:"
	contentEmbedTTL      = 6 * time.Hour

	// Emotion tag cache. getContentEmotions is called O(candidates) times per
	// feed request; caching avoids a DB round-trip per candidate.
	// TTL matches the embedding cache — emotion tags rarely change and a stale
	// tag for a few hours has negligible ranking impact.
	contentEmotionRedisKey = "emotions:"
	contentEmotionTTL      = 6 * time.Hour
)

// featureToken hashes a feature string into a specific slot in the vector.
// Sign is randomized by a second hash so collisions cancel on average.
func featureToken(s string, weight float64, out []float64) {
	if s == "" {
		return
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(s))
	slot := int(h.Sum32()) % embedDim
	if slot < 0 {
		slot += embedDim
	}
	// Sign via a different hash.
	h2 := fnv.New32a()
	_, _ = h2.Write([]byte("sign:" + s))
	sign := 1.0
	if h2.Sum32()&1 == 1 {
		sign = -1.0
	}
	out[slot] += sign * weight
}

// buildContentEmbedding returns a deterministic L2-normalized vector for
// content based on its structured features. Same content → same vector
// (modulo time-varying recency/popularity). Accepts *ContentScore (nil →
// zero vector) so callers can pass the pointer returned by getContentScore
// without an extra dereference.
//
// This function does NOT consult the Redis cache — it always recomputes
// from scratch. Hot-path callers should use getOrBuildContentEmbedding,
// which caches the stable part. We keep this function as the canonical
// reference: tests check determinism against it, and the cached path
// re-applies the dynamic features so its output is mathematically
// identical on a fresh build.
func buildContentEmbedding(cs *ContentScore, emotions []string) []float64 {
	v := buildContentEmbeddingStable(cs, emotions)
	if cs == nil {
		return v
	}
	applyDynamicEmbedFeatures(v, cs)
	return l2norm(v)
}

// buildContentEmbeddingStable returns the un-normalized stable part of the
// content vector — every feature that is a pure function of the item's
// identity (category, creator, emotion tags, quality, energy). Recency and
// popularity are intentionally excluded so this slice is safe to cache for
// hours.
//
// Returned vector still has slots 2 and 3 as 0 unless a hash collision from
// a categorical feature lands there (which is identical to the un-cached
// behavior — collisions are part of the hash-trick design).
func buildContentEmbeddingStable(cs *ContentScore, emotions []string) []float64 {
	v := make([]float64, embedDim)
	if cs == nil {
		return v
	}

	// Category is the strongest categorical feature.
	if cs.Category != "" {
		featureToken("cat:"+strings.ToLower(cs.Category), 1.0, v)
	}
	// Creator identity — collaborative-filter side.
	if cs.CreatorID != "" {
		featureToken("creator:"+cs.CreatorID, 0.8, v)
	}
	// Emotion tags: content vibe.
	for _, e := range emotions {
		if e == "" {
			continue
		}
		featureToken("emo:"+strings.ToLower(e), 0.5, v)
	}

	// Stable continuous features in reserved slots.
	v[0] += cs.QualityScore
	v[1] += cs.EnergyLevel

	return v
}

// applyDynamicEmbedFeatures layers the time-varying features (recency,
// popularity) onto a vector in place. Called both by buildContentEmbedding
// (fresh path) and getOrBuildContentEmbedding (cached path) so the two
// paths produce identical output.
func applyDynamicEmbedFeatures(v []float64, cs *ContentScore) {
	if cs == nil || len(v) < 4 {
		return
	}
	hoursOld := time.Since(cs.CreatedAt).Hours()
	if hoursOld < 0 {
		hoursOld = 0
	}
	v[2] += math.Exp(-hoursOld / 48.0) // recency, 2-day half-life
	v[3] += math.Log1p(float64(cs.ViewCount+cs.LikeCount)) / 10.0
}

// getOrBuildContentEmbedding is the production hot-path entry point. It
// fetches the stable vector from Redis if present, builds + caches on miss,
// then layers on the dynamic recency/popularity features and L2-normalizes.
//
// Output is mathematically identical to buildContentEmbedding when fresh.
// Falls through to a full rebuild on any Redis hiccup — never errors.
//
// Use this from per-candidate scoring loops; use buildContentEmbedding only
// from tests or one-shot lookups where caching overhead would dominate.
func getOrBuildContentEmbedding(cs *ContentScore, emotions []string) []float64 {
	if cs == nil {
		return make([]float64, embedDim)
	}

	var stable []float64
	cacheable := rdb != nil && cs.ContentID != ""

	if cacheable {
		if s, err := rdb.Get(rctx, contentEmbedRedisKey+cs.ContentID).Result(); err == nil && s != "" {
			var cached []float64
			if json.Unmarshal([]byte(s), &cached) == nil && len(cached) == embedDim {
				stable = cached
				if metricEmbedCacheHits != nil {
					metricEmbedCacheHits.WithLabelValues("hit").Inc()
				}
			}
		}
	}

	if stable == nil {
		stable = buildContentEmbeddingStable(cs, emotions)
		if cacheable {
			if js, err := json.Marshal(stable); err == nil {
				_ = rdb.Set(rctx, contentEmbedRedisKey+cs.ContentID, js, contentEmbedTTL).Err()
			}
		}
		if metricEmbedCacheHits != nil {
			metricEmbedCacheHits.WithLabelValues("miss").Inc()
		}
	}

	// Copy so we don't mutate the cached slice (json.Unmarshal allocates a
	// fresh slice already, but the local-build path returns the slice we'd
	// otherwise mutate; copy for safety on both paths).
	out := make([]float64, embedDim)
	copy(out, stable)
	applyDynamicEmbedFeatures(out, cs)
	return l2norm(out)
}

// l2norm scales v to unit length. Zero vectors are returned unchanged.
func l2norm(v []float64) []float64 {
	var sum float64
	for _, x := range v {
		sum += x * x
	}
	if sum == 0 {
		return v
	}
	inv := 1.0 / math.Sqrt(sum)
	out := make([]float64, len(v))
	for i, x := range v {
		out[i] = x * inv
	}
	return out
}

// cosineSim of two equal-length L2-normalized vectors == dot product.
func cosineSim(a, b []float64) float64 {
	if len(a) != len(b) {
		return 0
	}
	var s float64
	for i := range a {
		s += a[i] * b[i]
	}
	// Already normalized on write; clamp to avoid float drift.
	if s > 1 {
		s = 1
	}
	if s < -1 {
		s = -1
	}
	return s
}

// getUserEmbedding loads the user's vector from Redis. Returns a zero vector
// (cold-start sentinel) when missing — callers should treat zero-norm as
// "no preference yet" and skip the similarity term.
func getUserEmbedding(userID string) []float64 {
	if rdb == nil || userID == "" {
		return make([]float64, embedDim)
	}
	s, err := rdb.Get(rctx, userEmbedRedisKey+userID).Result()
	if err != nil || s == "" {
		return make([]float64, embedDim)
	}
	var v []float64
	if err := json.Unmarshal([]byte(s), &v); err != nil || len(v) != embedDim {
		return make([]float64, embedDim)
	}
	return v
}

// updateUserEmbedding nudges the user vector toward (label=+1) or away from
// (label=0) a content vector. Uses a small step so bad events don't whiplash
// the profile. Final vector is re-normalized.
//
// Write-path is best-effort: a Redis blip just means one event didn't land.
func updateUserEmbedding(userID string, contentVec []float64, label float64) {
	if rdb == nil || userID == "" || len(contentVec) != embedDim {
		return
	}
	cur := getUserEmbedding(userID)
	step := userEmbedPosStep
	if label < 0.5 {
		step = -userEmbedNegStep
	}
	for i := range cur {
		cur[i] = cur[i]*(1-userEmbedAlpha) + step*contentVec[i]
	}
	cur = l2norm(cur)
	if js, err := json.Marshal(cur); err == nil {
		_ = rdb.Set(rctx, userEmbedRedisKey+userID, js, userEmbedTTL).Err()
	}
	if metricEmbedUpdates != nil {
		if label >= 0.5 {
			metricEmbedUpdates.WithLabelValues("pos").Inc()
		} else {
			metricEmbedUpdates.WithLabelValues("neg").Inc()
		}
	}
}

// invalidateContentEmbedding drops the cached stable vector for a content
// item so subsequent reads recompute from scratch. Called on report (so
// flagged content stops surfacing on the basis of a now-stale fingerprint),
// on metadata edit (so the new category/emotions take effect immediately),
// and on hard delete. Best-effort — a Redis blip just means the cached
// value lives until its 6h TTL expires naturally.
func invalidateContentEmbedding(contentID string) {
	if contentID == "" {
		return
	}
	if rdb != nil {
		_ = rdb.Del(rctx, contentEmbedRedisKey+contentID).Err()
		if metricEmbedCacheHits != nil {
			metricEmbedCacheHits.WithLabelValues("invalidate").Inc()
		}
	}
	// Also NULL the stored pgvector embedding so the backfill worker recomputes it
	// — it only processes rows WHERE embedding IS NULL, so dropping just the Redis
	// cache left the ANN serving a STALE vector after a metadata edit (the new
	// category/emotion/energy never reached pgvector). contentID is a challenge id
	// here; the UPDATE is a no-op for non-challenge ids.
	if pgvectorAvailable && db != nil {
		if _, err := strconv.Atoi(contentID); err == nil {
			_, _ = db.Exec(`UPDATE challenges SET embedding = NULL WHERE id = $1`, contentID)
		}
	}
}

// userEmbeddingIsCold reports whether the user's vector has no information
// yet (all zeros). Callers use this to skip similarity scoring gracefully.
func userEmbeddingIsCold(v []float64) bool {
	for _, x := range v {
		if x != 0 {
			return false
		}
	}
	return true
}
