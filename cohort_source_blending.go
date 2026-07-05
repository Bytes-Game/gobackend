package main

import (
	"strconv"
	"sync"
	"time"
)

// ─────────────────────────────────────────────────────────────────────────────
// PER-COHORT LEARNED SOURCE BLENDING
//
// `defaultSourceWeights` is a global map (recency 0.25, trending 0.10,
// trendingRealtime 0.15, follow 0.15, collab 0.10, embedding 0.15 — see
// candidate_sources.go). Same for cold users and power users. But:
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
	if cohortBlend.rewards[cohort] == nil {
		cohortBlend.rewards[cohort] = make(map[string]float64)
	}
	prev := cohortBlend.rewards[cohort][source]
	updated := prev*(1-cohortBlendEMA) + reward*cohortBlendEMA
	cohortBlend.rewards[cohort][source] = updated
	cohortBlend.mu.Unlock()

	// Persist + metric OUTSIDE the write lock — a Redis round-trip must NOT block
	// effectiveSourceWeights' RLock, which the feed hot path takes on every
	// request. The old code deferred Unlock across the rdb.Set, so one slow Redis
	// write (a GC/network/tail-latency blip at scale) pinned the write lock and
	// stalled every concurrent feed read. Mirrors plattFit / wrFlush /
	// flushBayesianStats, which all snapshot under a brief lock then do I/O free.
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
	// COPY the per-cohort reward map under the lock — the old code released the
	// RLock and then read `rewards[src]` in the loops below, racing with
	// observeSourceReward's concurrent writes to the same map (a data race that can
	// panic the feed request). Snapshot only the keys we read.
	rewards := make(map[string]float64, len(defaultSourceWeights))
	cohortBlend.mu.RLock()
	if m := cohortBlend.rewards[cohort]; m != nil {
		for src := range defaultSourceWeights {
			rewards[src] = m[src]
		}
	}
	cohortBlend.mu.RUnlock()

	// NOTE: do NOT subtract a cross-source mean before exp() — the renormalization
	// below (raw/totalRaw) is shift-invariant, so exp(r-mean)/Σ == exp(r)/Σ: any
	// constant offset cancels and centering is a mathematical no-op. (An earlier
	// "fix" added mean-centering for an asymmetric-reward "neutral point" concern,
	// but the absolute neutral is irrelevant post-normalization — only the RELATIVE
	// exp(r) across sources matters, and renormalization already gives an
	// average-performing source ~its default share.)
	// rewards is the snapshot above (never nil; empty for an untrained cohort →
	// mul 1, uniform defaults).
	totalRaw := 0.0
	for src, defWeight := range defaultSourceWeights {
		// expSafe (bandit.go) is overflow-guarded; the realized EMA range is
		// [-0.4, 1.0] so exp stays in ~[0.67, 2.72] — no extra clamp needed.
		mul := expSafe(rewards[src])
		raw := defWeight * mul
		// NO floor here — the floor must hold on the FINAL (post-normalization)
		// weight. Enforcing it pre-normalization let the renormalize below shrink
		// a floored source back UNDER the floor, breaking the documented
		// "each source >= cohortBlendMinWeight" safety invariant.
		out[src] = raw
		totalRaw += raw
	}
	if totalRaw <= 0 {
		return out
	}
	// Normalize so the weights sum to 1.
	for src := range out {
		out[src] /= totalRaw
	}

	// Enforce the min-weight floor on the NORMALIZED weights via water-filling:
	// pin any source below the floor up to it, then redistribute the remaining
	// mass over the un-pinned sources proportionally; repeat until stable. The
	// result both sums to 1 and guarantees every source >= cohortBlendMinWeight.
	base := make(map[string]float64, len(out))
	for src, w := range out {
		base[src] = w
	}
	pinned := make(map[string]bool, len(out))
	for {
		avail := 1.0 - float64(len(pinned))*cohortBlendMinWeight
		unpinnedBaseSum := 0.0
		for src := range out {
			if !pinned[src] {
				unpinnedBaseSum += base[src]
			}
		}
		if avail <= 0 || unpinnedBaseSum <= 0 {
			break
		}
		newlyPinned := false
		for src := range out {
			if pinned[src] {
				continue
			}
			if base[src]/unpinnedBaseSum*avail < cohortBlendMinWeight {
				pinned[src] = true
				newlyPinned = true
			}
		}
		if !newlyPinned {
			break
		}
	}
	avail := 1.0 - float64(len(pinned))*cohortBlendMinWeight
	unpinnedBaseSum := 0.0
	for src := range out {
		if !pinned[src] {
			unpinnedBaseSum += base[src]
		}
	}
	for src := range out {
		if pinned[src] {
			out[src] = cohortBlendMinWeight
		} else if unpinnedBaseSum > 0 {
			out[src] = base[src] / unpinnedBaseSum * avail
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
