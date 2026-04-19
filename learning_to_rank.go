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

// ltrScoreDelta returns a bounded correction that scoreForUser adds on top.
// Read path — hot path, must be fast.
func ltrScoreDelta(cohort Cohort, breakdown map[string]float64) float64 {
	ltrEnsureLoaded()
	ltr.mu.RLock()
	m, ok := ltr.byCoh[cohort]
	ltr.mu.RUnlock()
	if !ok || m == nil || m.Updates < 20 {
		return 0 // Not enough data yet — don't add noise
	}
	var z float64 = m.Bias
	for _, k := range ltrFeatureKeys {
		if v, ok := breakdown[k]; ok {
			z += m.Weights[k] * v
		}
	}
	// Map logit to [-ltrMaxDelta, +ltrMaxDelta] via scaled tanh.
	return ltrMaxDelta * math.Tanh(z)
}

// ltrObserve records a (breakdown, label) pair and updates the cohort's model.
// Label: 1 for positive outcomes (complete, like, share, rewatch, scroll_back),
// 0 for negative (skip, not_interested, bounce).
func ltrObserve(cohort Cohort, breakdown map[string]float64, label float64) {
	if breakdown == nil {
		return
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
	err := pred - label
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
	if rdb == nil || userID == "" || contentID == "" {
		return
	}
	payload := map[string]interface{}{
		"c": string(cohort),
		"b": breakdown,
	}
	js, err := json.Marshal(payload)
	if err != nil {
		return
	}
	_ = rdb.Set(rctx, ltrBreakdownKey(userID, contentType, contentID), js, ltrBreakdownTTL).Err()
}

// ltrObserveEvent looks up the stashed breakdown for this (user, content) and
// turns the event into a training sample. Only called for terminal outcomes.
func ltrObserveEvent(userID, contentType, contentID string, label float64) {
	if rdb == nil || userID == "" || contentID == "" {
		return
	}
	s, err := rdb.Get(rctx, ltrBreakdownKey(userID, contentType, contentID)).Result()
	if err != nil || s == "" {
		return
	}
	var payload struct {
		C string             `json:"c"`
		B map[string]float64 `json:"b"`
	}
	if json.Unmarshal([]byte(s), &payload) != nil {
		return
	}
	ltrObserve(Cohort(payload.C), payload.B, label)
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
		if js, err := json.Marshal(m); err == nil {
			_ = rdb.Set(rctx, ltrRedisKey+string(c), js, 0).Err()
		} else {
			_ = fmt.Errorf("%v", err) // keep linter quiet
		}
	}
}
