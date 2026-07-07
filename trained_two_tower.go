package main

import (
	"encoding/json"
	"time"
)

// ─────────────────────────────────────────────────────────────────────────────
// TRAINED ONLINE TWO-TOWER
//
// The hash-trick `buildContentEmbedding` is deterministic: the same
// (category, creator, emotions, quality, energy) → same vector forever. It
// captures what we *told* the system about content but learns nothing from
// behavior. The user side already trains via EMA, but the content side is
// frozen.
//
// This module trains the CONTENT side too. We start each content vector
// from its hash-trick prior, then SGD-update it on every observed
// engagement event:
//
//   On positive: content vector V drifts toward user vector U
//                (content earned a slot in this user's preferences →
//                 align with whoever liked it)
//   On negative: V drifts away from U
//
// This makes the content vector encode what *kinds of users* engage with
// it — a learned representation, not a hard-coded one. After a few hundred
// engagement events per item, V no longer matches its hash-trick prior;
// it matches the latent preferences of its actual audience.
//
// SAFETY:
//   - Bounded learning rate (small step per event)
//   - L2 regularization toward the hash-trick prior so a small audience
//     can't completely capture the vector
//   - Renormalization after each step (vectors stay unit-length)
//   - Persisted in Redis under tt:content:{type}:{id} (24h TTL)
//   - getTrainedContentEmbedding falls back to hash-trick on cold-start
// ─────────────────────────────────────────────────────────────────────────────

const (
	ttContentRedisKey      = "tt:content:" // + {type}:{id}
	ttContentTTL           = 24 * time.Hour
	ttLearningRate         = 0.05  // SGD step per event
	ttPriorRegStrength     = 0.005 // L2 pull toward hash-trick prior
	ttMinUpdatesForTrained = 5     // below this we still return the prior (avoid noise)
)

// ttContentVec is the trained content-side state. The hash-trick prior is
// stored alongside the trained vector so regularization keeps the trained
// version anchored — a few weird users can't drag it arbitrarily far.
type ttContentVec struct {
	Trained []float64 `json:"v"`     // current learned vector (unit length)
	Prior   []float64 `json:"p"`     // hash-trick prior at first creation
	Updates int       `json:"n"`     // how many SGD steps so far
}

// In-process cache of trained vectors so a hot ranker request doesn't hit
// Redis per candidate. 5-min TTL — same shape as your other signal caches.
var ttContentCache = NewSignalCache[*ttContentVec](5 * time.Minute)

// getTrainedContentEmbedding returns the trained content vector if it has
// enough updates to be reliable; otherwise falls back to the hash-trick
// prior. Always returns a unit-length vector or the zero vector.
func getTrainedContentEmbedding(cs *ContentScore, emotions []string) []float64 {
	if cs == nil {
		return make([]float64, embedDim)
	}
	tt := loadOrInitTrainedContent(cs, emotions)
	if tt == nil {
		// Pure fallback to the cached/built embedding.
		return getOrBuildContentEmbedding(cs, emotions)
	}
	if tt.Updates < ttMinUpdatesForTrained {
		// Not enough behavioral evidence yet — use the prior augmented with
		// dynamic features (recency, popularity) so the vector still tracks
		// freshness.
		return getOrBuildContentEmbedding(cs, emotions)
	}
	// Apply dynamic features (recency, popularity) on top of the trained
	// stable part the same way the cached path does.
	out := make([]float64, embedDim)
	copy(out, tt.Trained)
	applyDynamicEmbedFeatures(out, cs)
	return l2norm(out)
}

// loadOrInitTrainedContent fetches a content's trained vector from Redis,
// initializing it from the hash-trick prior if missing.
func loadOrInitTrainedContent(cs *ContentScore, emotions []string) *ttContentVec {
	if cs == nil || cs.ContentID == "" {
		return nil
	}
	key := cs.ContentType + ":" + cs.ContentID

	if v, ok := ttContentCache.Get(key); ok && v != nil {
		return v
	}

	if rdb != nil {
		if s, err := rdb.Get(rctx, ttContentRedisKey+key).Result(); err == nil && s != "" {
			var v ttContentVec
			if json.Unmarshal([]byte(s), &v) == nil &&
				len(v.Trained) == embedDim && len(v.Prior) == embedDim {
				ttContentCache.Set(key, &v)
				return &v
			}
		}
	}

	// Cold init from hash-trick prior.
	prior := buildContentEmbeddingStable(cs, emotions)
	prior = l2norm(prior)
	v := &ttContentVec{
		Trained: append([]float64(nil), prior...),
		Prior:   append([]float64(nil), prior...),
		Updates: 0,
	}
	persistTrainedContent(key, v)
	ttContentCache.Set(key, v)
	return v
}

// updateTrainedContentEmbedding applies one SGD step:
//   On positive label: trained → trained + lr * (userVec - trained)
//   On negative label: trained → trained - lr * (userVec - trained)
// Plus an L2 pull back toward the prior so a few weird users can't run away.
//
// userVec must be unit-length (caller's getUserEmbedding output qualifies).
// label in [0, 1].
func updateTrainedContentEmbedding(cs *ContentScore, emotions []string, userVec []float64, label float64) {
	if cs == nil || cs.ContentID == "" || len(userVec) != embedDim {
		return
	}
	tt := loadOrInitTrainedContent(cs, emotions)
	if tt == nil {
		return
	}
	// Direction: positive moves toward user; negative moves away.
	stepSign := 1.0
	if label < 0.5 {
		stepSign = -1.0
	}
	// Avoid wasted compute on cold/zero user vectors.
	if userEmbeddingIsCold(userVec) {
		return
	}
	for i := range tt.Trained {
		// SGD: pull toward (or push away from) the user vector.
		grad := stepSign * (userVec[i] - tt.Trained[i])
		// L2 prior regularization: pull gently back toward the original prior.
		regGrad := ttPriorRegStrength * (tt.Prior[i] - tt.Trained[i])
		tt.Trained[i] += ttLearningRate*grad + regGrad
	}
	tt.Trained = l2norm(tt.Trained)
	tt.Updates++

	key := cs.ContentType + ":" + cs.ContentID
	persistTrainedContent(key, tt)
	ttContentCache.Set(key, tt)

	if metricTwoTowerUpdates != nil {
		outcome := "pos"
		if label < 0.5 {
			outcome = "neg"
		}
		metricTwoTowerUpdates.WithLabelValues(outcome).Inc()
	}
}

// persistTrainedContent best-effort writes to Redis.
func persistTrainedContent(key string, v *ttContentVec) {
	if rdb == nil {
		return
	}
	js, err := json.Marshal(v)
	if err != nil {
		return
	}
	_ = rdb.Set(rctx, ttContentRedisKey+key, js, ttContentTTL).Err()
}

