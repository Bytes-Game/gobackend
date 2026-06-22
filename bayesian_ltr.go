package main

import (
	"encoding/json"
	"math"
	"math/rand"
	"sync"
)

// ─────────────────────────────────────────────────────────────────────────────
// BAYESIAN LTR — uncertainty-aware online ranking.
//
// The existing logistic LTR returns a point estimate: "this item should
// score 0.7." But that 0.7 might come from 5 observations or 5000. Without
// uncertainty, the ranker can't distinguish "I'm 95% sure" from "I'm
// guessing." It treats both the same.
//
// This module maintains, per cohort and per feature, a **mean and variance**
// of the weight. When predicting:
//
//   - Compute the predicted logit using the means (same as before)
//   - Compute the predicted variance using the per-feature variances
//   - The score adjustment becomes: mean ± k * std
//
// At score time we draw a *Thompson sample* from the posterior (mean +
// noise scaled by std). Items with HIGH uncertainty get a noisy boost —
// they're surfaced for learning. Items with LOW uncertainty get exploited
// at their predicted value. This is "active learning" baked into the
// ranker — the model literally explores what it's uncertain about.
//
// IMPORTANT: This complements (does not replace) the existing LTR. We
// *track* the variances alongside the existing mean weights without
// forking the training code path — same observe step, just with a Welford-
// style running variance update.
//
// SAFETY:
//   - Uncertainty bonus is bounded ±bayesianMaxBonus
//   - Disabled until per-cohort sample count > bayesianMinSamples
//   - Uses sample-level (not per-feature) variance summary so the
//     overhead is one float per cohort, not 30
// ─────────────────────────────────────────────────────────────────────────────

const (
	bayesianMaxBonus     = 0.10  // hard cap on the uncertainty bonus
	bayesianExplorationK = 1.5   // multiplier on stddev when drawing noise
	bayesianMinSamples   = 25    // warmup before uncertainty kicks in
	bayesianRedisKey     = "blr:" // + cohort
)

// bayesianStats tracks running mean & variance of the LTR prediction
// residual for one cohort. We use the residual (predicted - actual) rather
// than per-weight variance because it's a single scalar per cohort and
// captures total predictive uncertainty including all feature
// interactions. Welford's online algorithm gives us numerically-stable
// variance without needing two passes over the data.
type bayesianStats struct {
	N       int     `json:"n"`     // sample count
	Mean    float64 `json:"mean"`  // running mean of (pred - actual) — should drift to 0 if calibrated
	M2      float64 `json:"m2"`    // sum of squared deviations (Welford state)
}

// variance returns the unbiased sample variance, or 0 when too few samples.
func (b *bayesianStats) variance() float64 {
	if b.N < 2 {
		return 0
	}
	return b.M2 / float64(b.N-1)
}

func (b *bayesianStats) stddev() float64 {
	v := b.variance()
	if v <= 0 {
		return 0
	}
	return math.Sqrt(v)
}

// observeBayesian records one (predicted, actual) pair. Welford update —
// O(1) per call, no need to retain the sample stream.
func (b *bayesianStats) observeBayesian(predicted, actual float64) {
	residual := predicted - actual
	b.N++
	delta := residual - b.Mean
	b.Mean += delta / float64(b.N)
	delta2 := residual - b.Mean
	b.M2 += delta * delta2
}

type bayesianStore struct {
	mu    sync.RWMutex
	byCoh map[Cohort]*bayesianStats
	loaded bool
}

var bayesianLTR = &bayesianStore{
	byCoh: make(map[Cohort]*bayesianStats),
}

// bayesianEnsureLoaded hydrates the per-cohort stats from Redis on first use.
func bayesianEnsureLoaded() {
	bayesianLTR.mu.Lock()
	defer bayesianLTR.mu.Unlock()
	if bayesianLTR.loaded {
		return
	}
	cohorts := []Cohort{CohortColdStart, CohortNew, CohortEngaged, CohortPower, CohortAtRisk}
	for _, c := range cohorts {
		s := &bayesianStats{}
		if rdb != nil {
			if v, err := rdb.Get(rctx, bayesianRedisKey+string(c)).Result(); err == nil && v != "" {
				_ = json.Unmarshal([]byte(v), s)
			}
		}
		bayesianLTR.byCoh[c] = s
	}
	bayesianLTR.loaded = true
}

// bayesianRecord is called from the LTR observe path with the predicted
// score (sigmoid of logit) and the actual binary label. Cheap (one Welford
// step). Persistence is deferred to the LTR flusher tick.
func bayesianRecord(cohort Cohort, predicted, actual float64) {
	bayesianEnsureLoaded()
	bayesianLTR.mu.Lock()
	defer bayesianLTR.mu.Unlock()
	s, ok := bayesianLTR.byCoh[cohort]
	if !ok {
		s = &bayesianStats{}
		bayesianLTR.byCoh[cohort] = s
	}
	s.observeBayesian(predicted, actual)
}

// bayesianUncertaintyBonus returns a Thompson-sampled adjustment based on
// the current cohort's predictive uncertainty. Used by the ranker to add
// stochastic exploration where the model is unsure, deterministic
// exploitation where it's confident.
//
// rnd allows tests to seed determinism. Returns 0 until warmup.
func bayesianUncertaintyBonus(cohort Cohort, baseScore float64, rnd *rand.Rand) float64 {
	bayesianEnsureLoaded()
	bayesianLTR.mu.RLock()
	s, ok := bayesianLTR.byCoh[cohort]
	bayesianLTR.mu.RUnlock()
	if !ok || s == nil || s.N < bayesianMinSamples {
		return 0
	}
	std := s.stddev()
	if std <= 0 {
		return 0
	}
	// Standard error of the mean residual (~std/√N): the exploration noise
	// SHRINKS as the cohort accumulates evidence — the "active learning that
	// converges" this module documents. The previous code used the raw residual
	// std, which never shrinks with data, so the bonus stayed a permanent ±cap
	// coin-flip forever.
	se := std / math.Sqrt(float64(s.N))
	// Per-item modulation: exploration is most valuable where the ranker is
	// least sure (mid scores) and near-useless where it's already confident
	// (extreme scores). p = σ(baseScore); 4·p·(1-p) peaks at 1 (p=0.5) and → 0
	// at the tails. (baseScore was previously ignored entirely.)
	p := 1.0 / (1.0 + math.Exp(-baseScore))
	itemUncertainty := 4.0 * p * (1.0 - p)
	// Draw N(0,1) from the caller's RNG when provided; otherwise use the
	// package-global (concurrency-safe, and no per-candidate reseed — the old
	// code re-seeded from time.Now() on every single call).
	var draw float64
	if rnd != nil {
		draw = rnd.NormFloat64()
	} else {
		draw = rand.NormFloat64()
	}
	noise := draw * se * bayesianExplorationK * itemUncertainty
	if noise > bayesianMaxBonus {
		noise = bayesianMaxBonus
	}
	if noise < -bayesianMaxBonus {
		noise = -bayesianMaxBonus
	}
	return noise
}

// flushBayesianStats persists per-cohort stats to Redis. Called from the
// existing LTR flusher tick.
func flushBayesianStats() {
	if rdb == nil {
		return
	}
	bayesianLTR.mu.RLock()
	snap := make(map[Cohort]bayesianStats, len(bayesianLTR.byCoh))
	for c, s := range bayesianLTR.byCoh {
		if s != nil {
			snap[c] = *s
		}
	}
	bayesianLTR.mu.RUnlock()
	for c, s := range snap {
		if js, err := json.Marshal(s); err == nil {
			_ = rdb.Set(rctx, bayesianRedisKey+string(c), js, 0).Err()
		}
	}
}
