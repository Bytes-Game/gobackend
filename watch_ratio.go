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
	// EMA of observed watch ratio — the actual per-cohort population mean. Used
	// as the bonus center so a typical item nets ~0 instead of a persistent
	// negative bonus (short-form watch ratios sit well below 0.5).
	MeanRatio float64 `json:"meanRatio"`
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
// We center on the cohort's ACTUAL mean watch ratio (MeanRatio) so an average
// item contributes ~0 and only items expected to do meaningfully better or worse
// than this cohort's norm move the score. Centering on a hard 0.5 (the old code)
// gave every typical short-form item a persistent NEGATIVE bonus, because the
// model's mean output equals the true population mean, which is well below 0.5.
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
	pred := 1.0 / (1.0 + math.Exp(-z))
	center := m.MeanRatio
	if center <= 0 || center >= 1 {
		// Only the impossible/unset boundary values (e.g. a pre-migration model
		// with MeanRatio==0) fall back to neutral. A GENUINE low-but-nonzero mean
		// (skip-heavy cohort, ~0.05-0.15) is respected — forcing it to 0.5 would
		// restore the persistent-negative-bonus bias the EMA centering removes.
		center = 0.5
	}
	// Piecewise-normalize around the center so delta spans [-1,1] using EACH side's
	// own range. The old (pred-center)*2.0 over-saturated: with center=0.3 it hit
	// +1 already at pred=0.8 (everything above maxed out) while the negative side
	// only reached -0.6 — asymmetric and lossy. Now pred=1→+1, pred=center→0,
	// pred=0→-1, with no early saturation.
	delta := 0.0
	if pred >= center {
		if center < 1 {
			delta = (pred - center) / (1 - center)
		}
	} else if center > 0 {
		delta = (pred - center) / center
	}
	if delta > 1 {
		delta = 1
	} else if delta < -1 {
		delta = -1
	}
	return wrMaxBonus * delta
}

// wrObserve records a (breakdown, watch_ratio) sample and SGD-updates the
// per-cohort weights. Watch ratio must be in [0, 1]; out-of-range values
// are clamped.
//
// weight is the inverse-propensity (position) weight — the watch-ratio head
// MUST apply the same IPW correction the LTR head does (1/positionPropensity),
// or position-confounded watch ratios (head slots are watched more) bias the
// regression. Pass 1.0 when no correction applies.
//
// The loss is MSE on sigmoid(z) vs target; gradient is (sigmoid(z)-target)
// times the feature value. L2 shrink keeps weights bounded against drift.
func wrObserve(cohort Cohort, breakdown map[string]float64, watchRatio01, weight float64) {
	if breakdown == nil {
		return
	}
	if watchRatio01 < 0 {
		watchRatio01 = 0
	}
	if watchRatio01 > 1 {
		watchRatio01 = 1
	}
	// Clamp the IPW weight to [0.25, 4.0] exactly like ltrObserveWeighted. The raw
	// 1/positionPropensity (composed with up to 2x skip-latency) can reach ~33-66
	// for deep positions, which would spike the effective learning rate and
	// saturate/destabilize the regression at the warmup boundary.
	if weight <= 0 {
		weight = 1.0
	}
	if weight < 0.25 {
		weight = 0.25
	}
	if weight > 4.0 {
		weight = 4.0
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
	// Decaying learning rate, scaled by the IPW weight (position correction).
	lr := weight * wrLearningRate / math.Sqrt(1.0+float64(m.Samples)/100.0)
	for _, k := range ltrFeatureKeys {
		if v, ok := breakdown[k]; ok {
			m.Weights[k] = m.Weights[k]*0.9995 - lr*err*v
		}
	}
	m.Bias -= lr * err
	// EMA the observed watch ratio so wrPredictBonus can center on the cohort's
	// real mean. Seed from the first sample so it converges fast and never sits
	// at the misleading 0 zero-value.
	if m.Samples == 0 {
		m.MeanRatio = watchRatio01
	} else {
		const wrMeanRatioEMA = 0.02
		m.MeanRatio = m.MeanRatio*(1-wrMeanRatioEMA) + watchRatio01*wrMeanRatioEMA
	}
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
	// Apply the same inverse-propensity (position) weight LTR uses.
	weight := 1.0
	if payload.P > 0 {
		weight = 1.0 / positionPropensity(payload.P)
	}
	wrObserve(Cohort(payload.C), payload.B, watchRatio01, weight)
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
				Weights:   make(map[string]float64, len(m.Weights)),
				Bias:      m.Bias,
				Samples:   m.Samples,
				MeanRatio: m.MeanRatio,
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
