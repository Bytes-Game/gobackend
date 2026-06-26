package main

import (
	"encoding/json"
	"fmt"
	"math"
	"sync"
	"time"
)

// ════════════════════════════════════════════════════════════════════════════════
// LEARNING-TO-RANK (online logistic regression, per cohort)
// ════════════════════════════════════════════════════════════════════════════════
//
// WHY: The hand-tuned score in scoreForUser is a strong prior, but the weights
// are the same for everyone. Different cohorts respond differently — at-risk
// users might care more about quality, power users might want more novelty.
// Rather than A/B-testing every combination manually, we let a small online
// model learn a correction term from observed outcomes (completions).
//
// WHAT: For each cohort we keep a weight vector over the score-breakdown keys.
// At request time we compute a cohort-specific linear score over the same
// breakdown features and emit a bounded delta the ranker adds on top of its
// base score. At event-ingest time we update the weights with SGD using
// sigmoid loss on a binary label: 1 if the user completed (>80% watch) or
// engaged (like/share/rewatch), 0 if they skipped or bounced.
//
// SAFETY:
//  - Delta is bounded to ±ltrMaxDelta so a bad update can't capsize the feed.
//  - Learning rate is small and scales down over time (1/√updates).
//  - If we have no data for a cohort, delta is 0 (ranker falls back to base).
//
// PERSISTENCE: Weights live in Redis under ltr:weights:{cohort} as JSON, loaded
// on boot and flushed every 5 minutes.

const (
	ltrMaxDelta      = 0.25  // Hard cap on LTR correction
	ltrLearningRate  = 0.02  // Base step size; scaled down by 1/√updates
	ltrFlushInterval = 5 * time.Minute
	ltrRedisKey      = "ltr:weights:"
)

// ltrFeatureKeys are the breakdown keys LTR learns over. We deliberately skip
// raw outputs (finalScore, baseScore, cohort) to avoid the model just learning
// to reproduce the base score.
var ltrFeatureKeys = []string{
	"social", "freshness", "energyFit", "relevance", "quality", "novelty",
	"tieStrength", "creatorAffinityBoost", "dwellIntentBoost",
	"searchBoost", "fatiguePenalty", "creatorFatigue", "sequencePenalty",
	"dopaminePenalty", "unseenBonus", "coldContentBonus", "trendingBonus",
	"hourBonus", "emotionBonus", "egoContextBonus", "wellbeingBonus",
	"collabBonus", "momentumBonus", "variableReward", "reentryBonus",
	"streakBonus", "impressionBouncePenalty", "scrollBackBonus",
	"completeBonus", "loopBonus", "unmuteBonus", "profileVisitBonus",
	"egoBoost",
	// NB: embedSim / embedBonus are deliberately NOT in this list. They are
	// written to the breakdown AFTER scoreForUser returns (in the post-scoring
	// loop), so at SERVE time the LTR / Platt / watch-ratio heads never see them
	// (absent → contribute 0), while at TRAIN time they were stashed present.
	// That train/serve feature-presence mismatch biased every calibrated
	// probability. Train and serve must iterate the same key set.
}

type ltrModel struct {
	Weights map[string]float64 `json:"weights"`
	Bias    float64            `json:"bias"`
	Updates int                `json:"updates"`
}

type ltrStore struct {
	mu      sync.RWMutex
	byCoh   map[Cohort]*ltrModel
	dirty   map[Cohort]bool
	loaded  bool
}

var ltr = &ltrStore{
	byCoh: make(map[Cohort]*ltrModel),
	dirty: make(map[Cohort]bool),
}

// ltrEnsureLoaded loads weights for every cohort from Redis on first use.
// Missing keys produce zero-weight models — equivalent to "no correction yet".
func ltrEnsureLoaded() {
	ltr.mu.Lock()
	defer ltr.mu.Unlock()
	if ltr.loaded {
		return
	}
	cohorts := []Cohort{CohortColdStart, CohortNew, CohortEngaged, CohortPower, CohortAtRisk}
	for _, c := range cohorts {
		m := &ltrModel{Weights: make(map[string]float64)}
		if rdb != nil {
			if s, err := rdb.Get(rctx, ltrRedisKey+string(c)).Result(); err == nil && s != "" {
				_ = json.Unmarshal([]byte(s), m)
				if m.Weights == nil {
					m.Weights = make(map[string]float64)
				}
			}
		}
		ltr.byCoh[c] = m
	}
	ltr.loaded = true
}

// ltrRawLogit returns the model's raw logit z for the breakdown and whether the
// cohort model is warmed (>=20 updates). This is the exact quantity the model is
// trained on (and that plattRecord + the creator-residual path calibrate
// against), so any caller needing a calibrated probability must pass THIS to
// plattCalibrate — never the bounded delta from ltrScoreDelta, which would be a
// train/serve scale mismatch that pins the calibrated bonus near a constant.
func ltrRawLogit(cohort Cohort, breakdown map[string]float64) (float64, bool) {
	ltrEnsureLoaded()
	ltr.mu.RLock()
	m, ok := ltr.byCoh[cohort]
	ltr.mu.RUnlock()
	if !ok || m == nil || m.Updates < 20 {
		return 0, false // Not enough data yet — don't add noise
	}
	z := m.Bias
	for _, k := range ltrFeatureKeys {
		if v, ok := breakdown[k]; ok {
			z += m.Weights[k] * v
		}
	}
	return z, true
}

// ltrScoreDelta returns a bounded correction that scoreForUser adds on top.
// Read path — hot path, must be fast.
func ltrScoreDelta(cohort Cohort, breakdown map[string]float64) float64 {
	z, ok := ltrRawLogit(cohort, breakdown)
	if !ok {
		return 0
	}
	// Map logit to [-ltrMaxDelta, +ltrMaxDelta] via scaled tanh.
	return ltrMaxDelta * math.Tanh(z)
}

// ltrObserve records a (breakdown, label) pair and updates the cohort's model.
// Label: 1 for positive outcomes (complete, like, share, rewatch, scroll_back),
// 0 for negative (skip, not_interested, bounce).
func ltrObserve(cohort Cohort, breakdown map[string]float64, label float64) {
	ltrObserveWeighted(cohort, breakdown, label, 1.0)
}

// ltrObserveWeighted is ltrObserve but with a sample weight.
// Position-bias correction multiplies the gradient step by 1/propensity(pos)
// so items at position 1 don't dominate learning just because they were seen.
// Weight is clamped to [0.25, 4.0] to prevent runaway updates from pathological
// positions (bottom of a very long scroll with tiny propensity).
func ltrObserveWeighted(cohort Cohort, breakdown map[string]float64, label, weight float64) {
	if breakdown == nil {
		return
	}
	if weight < 0.25 {
		weight = 0.25
	}
	if weight > 4.0 {
		weight = 4.0
	}
	ltrEnsureLoaded()
	ltr.mu.Lock()
	defer ltr.mu.Unlock()
	m, ok := ltr.byCoh[cohort]
	if !ok {
		m = &ltrModel{Weights: make(map[string]float64)}
		ltr.byCoh[cohort] = m
	}
	// Logistic regression SGD step.
	var z float64 = m.Bias
	for _, k := range ltrFeatureKeys {
		if v, ok := breakdown[k]; ok {
			z += m.Weights[k] * v
		}
	}
	pred := 1.0 / (1.0 + math.Exp(-z))
	err := (pred - label) * weight
	// Feed every training sample to the Platt calibrator too.
	plattRecord(z, label)
	// Track per-cohort predictive variance so the ranker can do uncertainty-
	// aware exploration via bayesianUncertaintyBonus.
	bayesianRecord(cohort, pred, label)
	// Decaying learning rate: 1 / sqrt(1 + updates/50)
	lr := ltrLearningRate / math.Sqrt(1.0+float64(m.Updates)/50.0)
	for _, k := range ltrFeatureKeys {
		if v, ok := breakdown[k]; ok {
			// L2 shrink (weight decay) keeps weights bounded.
			m.Weights[k] = m.Weights[k]*0.9995 - lr*err*v
		}
	}
	m.Bias -= lr * err
	m.Updates++
	ltr.dirty[cohort] = true
}

// positionPropensity returns the empirical probability that an item at
// position `pos` (1-indexed) receives any engagement *regardless of quality*.
// Shape: 1/(pos)^0.7 — a standard fit to log-scroll-depth curves. Caller
// trains on 1/propensity to recover the effect of quality alone.
//
// Values: pos=1 → 1.0 | pos=5 → ~0.32 | pos=20 → ~0.11 | pos=100 → ~0.03.
func positionPropensity(pos int) float64 {
	if pos < 1 {
		pos = 1
	}
	return math.Pow(float64(pos), -0.7)
}

// startLTRFlusher periodically persists dirty cohort weights to Redis.
func startLTRFlusher() {
	go func() {
		ticker := time.NewTicker(ltrFlushInterval)
		defer ticker.Stop()
		for range ticker.C {
			ltrFlush()
		}
	}()
}

// ltrBreakdownKey is the Redis key we stash a score breakdown under when the
// ranker hands out a feed item. We fetch it back when a terminal outcome event
// fires (complete/like/skip/etc.) and use it as the features for an SGD step.
// 30-minute TTL is generous — sessions rarely exceed that before the user
// either engages or moves on.
const ltrBreakdownTTL = 30 * time.Minute

func ltrBreakdownKey(userID, contentType, contentID string) string {
	return fmt.Sprintf("ltr:bd:%s:%s:%s", userID, contentType, contentID)
}

// ltrStashBreakdown persists a score breakdown next to the served item so a
// later outcome event can learn from it. Best-effort — a Redis blip just
// means this particular item won't contribute a training sample.
func ltrStashBreakdown(userID, contentType, contentID string, cohort Cohort, breakdown map[string]float64) {
	ltrStashBreakdownWithPos(userID, contentType, contentID, cohort, breakdown, 0)
}

// ltrStashBreakdownWithPos also records the 1-indexed feed position so the
// later observe step can apply inverse-propensity weighting. Additionally
// stashes the creator ID so per-creator residual calibration can update
// at terminal-event time without an extra DB lookup.
func ltrStashBreakdownWithPos(userID, contentType, contentID string, cohort Cohort, breakdown map[string]float64, pos int) {
	ltrStashBreakdownFull(userID, contentType, contentID, cohort, breakdown, pos, "")
}

// ltrStashBreakdownFull is the creator-aware version. Most callers should
// use ltrStashBreakdownAll which also records the source attribution.
func ltrStashBreakdownFull(userID, contentType, contentID string, cohort Cohort, breakdown map[string]float64, pos int, creatorID string) {
	ltrStashBreakdownAll(userID, contentType, contentID, cohort, breakdown, pos, creatorID, "")
}

// ltrStashBreakdownAll is the canonical full-fledged stash. Records cohort,
// breakdown, position, creator, and the candidate source that produced the
// item so later observe steps can update per-creator residual calibration
// and per-cohort source-blending rewards in one DB pass.
func ltrStashBreakdownAll(userID, contentType, contentID string, cohort Cohort, breakdown map[string]float64, pos int, creatorID, source string) {
	if rdb == nil || userID == "" || contentID == "" {
		return
	}
	payload := map[string]interface{}{
		"c":  string(cohort),
		"b":  breakdown,
		"p":  pos,
		"cr": creatorID,
		"s":  source,
	}
	js, err := json.Marshal(payload)
	if err != nil {
		return
	}
	_ = rdb.Set(rctx, ltrBreakdownKey(userID, contentType, contentID), js, ltrBreakdownTTL).Err()
}

// ltrObserveEvent looks up the stashed breakdown for this (user, content) and
// turns the event into a training sample. Only called for terminal outcomes.
//
// If a position was stashed with the breakdown, the training step is weighted
// by 1/propensity(pos) so the model learns item quality rather than item
// prominence. Position-0 (legacy path, no IPW) falls through as weight=1.
//
// latencyMs is the impression-to-engagement latency in milliseconds. Faster
// engagements multiply the weight up to 2x (compelling content); slower
// ones scale toward 0.5x. Combined multiplicatively with the IPW position
// weight so both corrections compose cleanly.
//
// Also feeds the watch-ratio prediction head when watchRatio >= 0 (caller
// passes -1 if no reliable ratio is available, e.g. on a like with no view
// completion data). The watch-ratio model is read-only here; LTR remains
// the breakdown's owner and is responsible for the eventual delete.
func ltrObserveEvent(userID, contentType, contentID string, label, watchRatio float64) {
	ltrObserveEventWithLatency(userID, contentType, contentID, label, watchRatio, 0)
}

// ltrObserveEventWithLatency is the full version. ltrObserveEvent above
// is a thin wrapper for callers that don't carry latency.
func ltrObserveEventWithLatency(userID, contentType, contentID string, label, watchRatio float64, latencyMs int) {
	if rdb == nil || userID == "" || contentID == "" {
		return
	}
	s, err := rdb.Get(rctx, ltrBreakdownKey(userID, contentType, contentID)).Result()
	if err != nil || s == "" {
		return
	}
	var payload struct {
		C  string             `json:"c"`
		B  map[string]float64 `json:"b"`
		P  int                `json:"p"`
		CR string             `json:"cr"`
		S  string             `json:"s"`
	}
	if json.Unmarshal([]byte(s), &payload) != nil {
		return
	}
	weight := 1.0
	if payload.P > 0 {
		weight = 1.0 / positionPropensity(payload.P)
	}
	// Compose latency weight: fast engagements contribute more, slow ones
	// less. Skips use the symmetric companion (early skips are stronger
	// negatives than late ones).
	if latencyMs > 0 {
		if label >= 0.5 {
			weight *= engagementLatencyWeight(latencyMs)
		} else {
			weight *= skipLatencyWeight(latencyMs)
		}
	}
	ltrObserveWeighted(Cohort(payload.C), payload.B, label, weight)
	// Train the watch-ratio head on the same breakdown when we have a ratio.
	if watchRatio >= 0 {
		wrObserve(Cohort(payload.C), payload.B, watchRatio)
	}
	// Per-creator residual calibration: compare the predicted Platt-
	// calibrated probability against the actual binary outcome and EMA
	// the running residual per creator. Self-correcting reach bias.
	if payload.CR != "" {
		// Recompute predicted from the breakdown — same logit pipeline as
		// ltrScoreDelta but we want the probability, not the bounded delta.
		var z float64
		ltr.mu.RLock()
		if m, ok := ltr.byCoh[Cohort(payload.C)]; ok && m != nil {
			z = m.Bias
			for _, k := range ltrFeatureKeys {
				if v, ok := payload.B[k]; ok {
					z += m.Weights[k] * v
				}
			}
		}
		ltr.mu.RUnlock()
		predicted := plattCalibrate(z)
		observeCreatorResidual(payload.CR, predicted, label)
	}
	// Per-cohort source-blending reward: credit (or debit) the source that
	// produced this item based on the binary outcome.
	if payload.S != "" {
		reward := cohortBlendRewardPositive
		if label < 0.5 {
			reward = cohortBlendRewardNegative
		}
		observeSourceReward(Cohort(payload.C), payload.S, reward)
	}
	// Delete so the same item can't contribute multiple times (first-outcome wins).
	_ = rdb.Del(rctx, ltrBreakdownKey(userID, contentType, contentID)).Err()
}

// ltrLabelForEvent maps event types to a binary training label.
// Returns (label, observable). observable=false means "don't train on this".
func ltrLabelForEvent(eventType string, completionRate float64) (float64, bool) {
	switch eventType {
	case "complete", "like", "share", "rewatch", "loop", "scroll_back", "save", "unmute":
		return 1.0, true
	case "skip", "not_interested", "report", "block":
		return 0.0, true
	case "view":
		// A "view" event only counts if we can infer watch quality.
		if completionRate >= 0.8 {
			return 1.0, true
		}
		if completionRate > 0 && completionRate < 0.2 {
			return 0.0, true
		}
		return 0, false
	}
	return 0, false
}

func ltrFlush() {
	if rdb == nil {
		return
	}
	ltr.mu.Lock()
	snapshots := make(map[Cohort]*ltrModel, len(ltr.dirty))
	for c := range ltr.dirty {
		if m, ok := ltr.byCoh[c]; ok && m != nil {
			cp := ltrModel{
				Weights: make(map[string]float64, len(m.Weights)),
				Bias:    m.Bias,
				Updates: m.Updates,
			}
			for k, v := range m.Weights {
				cp.Weights[k] = v
			}
			snapshots[c] = &cp
		}
	}
	ltr.dirty = make(map[Cohort]bool)
	ltr.mu.Unlock()

	for c, m := range snapshots {
		ok := false
		if js, err := json.Marshal(m); err == nil {
			if err := rdb.Set(rctx, ltrRedisKey+string(c), js, 0).Err(); err == nil {
				ok = true
			}
		}
		if metricLTRFlushes != nil {
			if ok {
				metricLTRFlushes.WithLabelValues("ok").Inc()
			} else {
				metricLTRFlushes.WithLabelValues("error").Inc()
			}
		}
		if metricLTRUpdates != nil {
			metricLTRUpdates.WithLabelValues(string(c)).Set(float64(m.Updates))
		}
	}
}
