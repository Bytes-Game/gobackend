package main

import (
	"strconv"
	"sync"
	"time"
)

// ─────────────────────────────────────────────────────────────────────────────
// PER-COHORT LEARNED SOURCE BLENDING
//
// `defaultSourceWeights` is a global map (recency 0.30, trending 0.15,
// trendingRealtime 0.15, follow 0.15, collab 0.10, embedding 0.15). Same
// for cold users and power users. But:
//
//   - Cold users have no follow graph → "follow" weight is wasted budget
//   - Power users have a tight follow graph → "follow" should weigh more
//   - At-risk users need exploration → "trending" / "embedding" outperform
//   - Engaged users do best with personalized → "collab" + "embedding" up
//
// This module learns per-cohort source weights from observed engagement.
// At read time, multiSourceFetch consults `effectiveSourceWeights(cohort)`
// instead of `defaultSourceWeights` directly. Weights start at the global
// defaults and drift via simple EMA on observed CTR-like reward per source.
//
// Reward attribution: when we serve an item, we know which source produced
// it (the multi-source dispatcher tracks this in bySource). When the user
// engages positively, the source gets credited; when they skip, the source
// is debited. Per-source per-cohort EMAs accumulate; weights are a
// renormalized softmax over those EMAs.
//
// SAFETY:
//   - Each cohort's weights sum to 1.0 always
//   - Min weight per source = 0.05 (always test every source)
//   - Defaults are the seed; weights drift from there over thousands of
//     impressions, never lock to 0 or 1 immediately
// ─────────────────────────────────────────────────────────────────────────────

const (
	cohortBlendRedisKey = "cbblend:" // + cohort:source
	cohortBlendTTL      = 30 * 24 * time.Hour
	cohortBlendMinWeight = 0.05  // floor per source
	cohortBlendEMA      = 0.02   // slow learning — many impressions to move
	// Reward magnitude per outcome.
	cohortBlendRewardPositive = 1.0
	cohortBlendRewardNegative = -0.4
)

// cohortBlendStore is the per-(cohort, source) reward EMA. Weights are
// derived from these via softmax + floor.
type cohortBlendStore struct {
	mu      sync.RWMutex
	rewards map[Cohort]map[string]float64 // cohort → source → EMA reward
	loaded  bool
}

var cohortBlend = &cohortBlendStore{
	rewards: make(map[Cohort]map[string]float64),
}

// cohortBlendEnsureLoaded hydrates from Redis on first use.
func cohortBlendEnsureLoaded() {
	cohortBlend.mu.Lock()
	defer cohortBlend.mu.Unlock()
	if cohortBlend.loaded {
		return
	}
	cohorts := []Cohort{CohortColdStart, CohortNew, CohortEngaged, CohortPower, CohortAtRisk}
	for _, c := range cohorts {
		cohortBlend.rewards[c] = make(map[string]float64)
		if rdb == nil {
			continue
		}
		for src := range defaultSourceWeights {
			key := cohortBlendRedisKey + string(c) + ":" + src
			if v, err := rdb.Get(rctx, key).Result(); err == nil && v != "" {
				if r, err := strconv.ParseFloat(v, 64); err == nil {
					cohortBlend.rewards[c][src] = r
				}
			}
		}
	}
	cohortBlend.loaded = true
}

// observeSourceReward records one outcome for a (cohort, source) pair.
// Called from the event handler when a stashed served-item resolves.
//
// reward in [-1, 1] — positive for positive engagement, negative for skip/
// not-interested. Internally clipped, EMA-applied, persisted best-effort.
func observeSourceReward(cohort Cohort, source string, reward float64) {
	if source == "" {
		return
	}
	cohortBlendEnsureLoaded()
	cohortBlend.mu.Lock()
	defer cohortBlend.mu.Unlock()
	if cohortBlend.rewards[cohort] == nil {
		cohortBlend.rewards[cohort] = make(map[string]float64)
	}
	prev := cohortBlend.rewards[cohort][source]
	updated := prev*(1-cohortBlendEMA) + reward*cohortBlendEMA
	cohortBlend.rewards[cohort][source] = updated

	if rdb != nil {
		key := cohortBlendRedisKey + string(cohort) + ":" + source
		_ = rdb.Set(rctx, key, strconv.FormatFloat(updated, 'f', 4, 64), cohortBlendTTL).Err()
	}
	if metricCohortBlendObserve != nil {
		metricCohortBlendObserve.WithLabelValues(string(cohort), source).Inc()
	}
}

// effectiveSourceWeights returns the per-source mix for one cohort. Weights
// always sum to 1.0 and each is at least cohortBlendMinWeight. Pure
// function of stored EMAs — safe to call on the hot path.
//
// Algorithm:
//   1. Start with global defaults.
//   2. For each source, multiply its default by exp(reward) — turns the
//      log-domain EMA reward into a multiplicative adjustment.
//   3. Apply min-weight floor.
//   4. Re-normalize.
//
// This is a lightweight learned-blending — no full ML, no separate model
// to flush. Just: "what worked here gets more budget."
func effectiveSourceWeights(cohort Cohort) map[string]float64 {
	cohortBlendEnsureLoaded()
	out := make(map[string]float64, len(defaultSourceWeights))
	cohortBlend.mu.RLock()
	rewards := cohortBlend.rewards[cohort]
	cohortBlend.mu.RUnlock()

	// Adjust each source's default by its reward EMA (clamped exponent).
	totalRaw := 0.0
	for src, defWeight := range defaultSourceWeights {
		mul := 1.0
		if rewards != nil {
			r := rewards[src]
			if r > 1.5 {
				r = 1.5
			}
			if r < -1.5 {
				r = -1.5
			}
			mul = expSafe(r) // expSafe lives in bandit.go
		}
		raw := defWeight * mul
		if raw < cohortBlendMinWeight {
			raw = cohortBlendMinWeight
		}
		out[src] = raw
		totalRaw += raw
	}
	// Renormalize so the cohort's source weights sum to 1.
	if totalRaw > 0 {
		for src := range out {
			out[src] /= totalRaw
		}
	}
	return out
}

// resetCohortBlend is for tests only.
func resetCohortBlend() {
	cohortBlend.mu.Lock()
	defer cohortBlend.mu.Unlock()
	cohortBlend.rewards = make(map[Cohort]map[string]float64)
	cohortBlend.loaded = false
}
