package main

import (
	"encoding/json"
	"math"
	"sync"
	"time"
)

// ─────────────────────────────────────────────────────────────────────────────
// WATCH-TIME PREDICTION HEAD
//
// The existing LTR predicts P(positive engagement) via logistic regression.
// That's a classifier — it tells us "will the user like this?" But TikTok's
// real secret sauce is predicting *expected watch ratio*: of the video's
// total duration, what fraction will the user actually watch?
//
// Watch ratio is a stronger gradient than binary engagement because:
//   - It's not all-or-nothing: 70% watched > 30% watched > skip
//   - It's hard to fake — bots can simulate likes but not full watches
//   - It correlates directly with the metric we ultimately care about
//     (total session time, retention, ad-revenue surface)
//
// This module trains a *separate* per-cohort linear regression head on the
// same breakdown features as LTR, with target = observed watch_ratio in
// [0, 1]. At score time it produces a small bonus (±0.18) the ranker adds
// on top of the engagement-classifier delta.
//
// Why a separate head and not a single multi-objective model: keeps the
// engagement classifier interpretable (Platt-calibrated probabilities are
// usable for downstream business logic — push-notif thresholds, etc.) and
// lets us tune the two heads' relative influence without retraining either.
//
// SAFETY:
//   - Bonus is bounded ±wrMaxBonus
//   - Per-cohort minimum sample threshold before predictions kick in
//   - Per-feature L2 shrink keeps weights bounded
//   - Persisted to Redis under wr:weights:{cohort} every 5 min
// ─────────────────────────────────────────────────────────────────────────────

const (
	wrMaxBonus      = 0.18 // hard cap on the watch-ratio bonus added to score
	wrLearningRate  = 0.04 // a touch faster than LTR — labels are continuous and richer
	wrFlushInterval = 5 * time.Minute
	wrRedisKey      = "wr:weights:"
	wrMinSamples    = 30 // per-cohort warmup before predictions are emitted
)

// wrModel is a per-cohort linear regression on the same feature keys LTR
// uses. Target is watch_ratio in [0, 1].
type wrModel struct {
	Weights map[string]float64 `json:"weights"`
	Bias    float64            `json:"bias"`
	Samples int                `json:"samples"`
}

type wrStore struct {
	mu     sync.RWMutex
	byCoh  map[Cohort]*wrModel
	dirty  map[Cohort]bool
	loaded bool
}

var watchRatio = &wrStore{
	byCoh: make(map[Cohort]*wrModel),
	dirty: make(map[Cohort]bool),
}

// wrEnsureLoaded hydrates the store from Redis on first use. Missing keys
// produce empty models (predict 0 until enough samples accumulate).
func wrEnsureLoaded() {
	watchRatio.mu.Lock()
	defer watchRatio.mu.Unlock()
	if watchRatio.loaded {
		return
	}
	cohorts := []Cohort{CohortColdStart, CohortNew, CohortEngaged, CohortPower, CohortAtRisk}
	for _, c := range cohorts {
		m := &wrModel{Weights: make(map[string]float64)}
		if rdb != nil {
			if s, err := rdb.Get(rctx, wrRedisKey+string(c)).Result(); err == nil && s != "" {
				_ = json.Unmarshal([]byte(s), m)
				if m.Weights == nil {
					m.Weights = make(map[string]float64)
				}
			}
		}
		watchRatio.byCoh[c] = m
	}
	watchRatio.loaded = true
}

// wrPredictBonus returns a bounded score adjustment based on predicted watch
// ratio for this candidate. Read-path; must be fast.
//
// Mapping: predicted_ratio ∈ [0, 1] → bonus ∈ [-wrMaxBonus, +wrMaxBonus].
// We center on 0.5 (the population mean of "kinda watched") so a prediction
// of 0.5 contributes zero — only items expected to do meaningfully better
// or worse than average move the score.
func wrPredictBonus(cohort Cohort, breakdown map[string]float64) float64 {
	wrEnsureLoaded()
	watchRatio.mu.RLock()
	m, ok := watchRatio.byCoh[cohort]
	watchRatio.mu.RUnlock()
	if !ok || m == nil || m.Samples < wrMinSamples {
		return 0
	}
	z := m.Bias
	for _, k := range ltrFeatureKeys {
		if v, ok := breakdown[k]; ok {
			z += m.Weights[k] * v
		}
	}
	// Clamp predicted ratio to [0, 1] via sigmoid then center.
	pred := 1.0 / (1.0 + math.Exp(-z))
	delta := (pred - 0.5) * 2.0 // ∈ [-1, 1]
	return wrMaxBonus * delta
}

// wrObserve records a (breakdown, watch_ratio) sample and SGD-updates the
// per-cohort weights. Watch ratio must be in [0, 1]; out-of-range values
// are clamped.
//
// The loss is MSE on sigmoid(z) vs target; gradient is (sigmoid(z)-target)
// times the feature value. L2 shrink keeps weights bounded against drift.
func wrObserve(cohort Cohort, breakdown map[string]float64, watchRatio01 float64) {
	if breakdown == nil {
		return
	}
	if watchRatio01 < 0 {
		watchRatio01 = 0
	}
	if watchRatio01 > 1 {
		watchRatio01 = 1
	}
	wrEnsureLoaded()
	watchRatio.mu.Lock()
	defer watchRatio.mu.Unlock()
	m, ok := watchRatio.byCoh[cohort]
	if !ok {
		m = &wrModel{Weights: make(map[string]float64)}
		watchRatio.byCoh[cohort] = m
	}
	z := m.Bias
	for _, k := range ltrFeatureKeys {
		if v, ok := breakdown[k]; ok {
			z += m.Weights[k] * v
		}
	}
	pred := 1.0 / (1.0 + math.Exp(-z))
	err := pred - watchRatio01
	// Decaying learning rate: 1 / sqrt(1 + samples/100)
	lr := wrLearningRate / math.Sqrt(1.0+float64(m.Samples)/100.0)
	for _, k := range ltrFeatureKeys {
		if v, ok := breakdown[k]; ok {
			m.Weights[k] = m.Weights[k]*0.9995 - lr*err*v
		}
	}
	m.Bias -= lr * err
	m.Samples++
	watchRatio.dirty[cohort] = true

	if metricWatchRatioObserve != nil {
		metricWatchRatioObserve.WithLabelValues(string(cohort)).Inc()
	}
}

// wrObserveEvent translates an event into a watch-ratio sample using the
// same stashed breakdown LTR uses. Only call for events where we have a
// reliable watch_ratio (view + completion_rate, complete=1.0, skip=0.0).
//
// Unlike LTR which deletes the breakdown after the first outcome, this
// reads non-destructively — LTR is the breakdown's owner. We just look it
// up and train alongside.
func wrObserveEvent(userID, contentType, contentID string, watchRatio01 float64) {
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
		P int                `json:"p"`
	}
	if json.Unmarshal([]byte(s), &payload) != nil {
		return
	}
	wrObserve(Cohort(payload.C), payload.B, watchRatio01)
}

// startWatchRatioFlusher persists dirty cohort weights every 5 minutes.
// Mirror of startLTRFlusher — same cadence, separate Redis namespace.
func startWatchRatioFlusher() {
	go func() {
		t := time.NewTicker(wrFlushInterval)
		defer t.Stop()
		for range t.C {
			wrFlush()
		}
	}()
}

func wrFlush() {
	if rdb == nil {
		return
	}
	watchRatio.mu.Lock()
	snapshots := make(map[Cohort]*wrModel, len(watchRatio.dirty))
	for c := range watchRatio.dirty {
		if m, ok := watchRatio.byCoh[c]; ok && m != nil {
			cp := wrModel{
				Weights: make(map[string]float64, len(m.Weights)),
				Bias:    m.Bias,
				Samples: m.Samples,
			}
			for k, v := range m.Weights {
				cp.Weights[k] = v
			}
			snapshots[c] = &cp
		}
	}
	watchRatio.dirty = make(map[Cohort]bool)
	watchRatio.mu.Unlock()

	for c, m := range snapshots {
		ok := false
		if js, err := json.Marshal(m); err == nil {
			if err := rdb.Set(rctx, wrRedisKey+string(c), js, 0).Err(); err == nil {
				ok = true
			}
		}
		if metricWatchRatioFlush != nil {
			if ok {
				metricWatchRatioFlush.WithLabelValues("ok").Inc()
			} else {
				metricWatchRatioFlush.WithLabelValues("error").Inc()
			}
		}
	}
}
