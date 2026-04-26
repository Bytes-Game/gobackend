package main

import "math"

// ─────────────────────────────────────────────────────────────────────────────
// TIME-TO-ENGAGEMENT REWARD WEIGHTING
//
// A like at 2 seconds means "I love this." A like at 30 seconds means
// "okay, fine, I'll like it." Currently both add 1.0 to the reward.
// That's information left on the table.
//
// This module returns a multiplier in [0.5, 2.0] based on the latency
// from impression to engagement. Fast = strong signal = bigger update.
// Slow = weak signal = smaller update.
//
// Formula: 2 * exp(-latency_ms / 8000) clipped to [0.5, 2.0].
//
//   latency=0s     → 2.0  (instant — clearly compelling)
//   latency=2s     → 1.55
//   latency=5s     → 1.07
//   latency=8s     → 0.74
//   latency=15s    → 0.5  (delayed — engaged but not enthusiastic)
//
// We also expose a NEGATIVE weighting for skips: skips that happen FAST
// are stronger negative signals than skips that happen after the user has
// watched a meaningful chunk. Same shape, opposite intent.
// ─────────────────────────────────────────────────────────────────────────────

const (
	engLatencyTimeConstantMs = 8000.0
	engLatencyMinWeight      = 0.5
	engLatencyMaxWeight      = 2.0
)

// engagementLatencyWeight returns the multiplier to apply to a reward
// based on how quickly the engagement happened after the impression.
// Pass 0 if latency is unknown — returns 1.0 (neutral).
func engagementLatencyWeight(latencyMs int) float64 {
	if latencyMs <= 0 {
		return 1.0
	}
	w := 2.0 * math.Exp(-float64(latencyMs)/engLatencyTimeConstantMs)
	if w < engLatencyMinWeight {
		w = engLatencyMinWeight
	}
	if w > engLatencyMaxWeight {
		w = engLatencyMaxWeight
	}
	return w
}

// skipLatencyWeight is the symmetric companion for negative events. A skip
// that happens in 800ms is much stronger than one at 12s. Same exponential
// curve, applied to the negative magnitude.
//
// Used by the bandit's reward update + LTR's training weight.
func skipLatencyWeight(latencyMs int) float64 {
	if latencyMs <= 0 {
		return 1.0
	}
	w := 2.0 * math.Exp(-float64(latencyMs)/engLatencyTimeConstantMs)
	if w < engLatencyMinWeight {
		w = engLatencyMinWeight
	}
	if w > engLatencyMaxWeight {
		w = engLatencyMaxWeight
	}
	return w
}

// engagementLatencyFromEvent extracts the latency from impression to event
// using the watch_duration_ms field. For events that fire after the user
// has watched some of the content (view, complete, skip), this is a good
// proxy for "how long did they wait before engaging."
//
// For events that fire on tap (like, save, share), we'd ideally have a
// separate impression timestamp; for now we use watch_duration_ms which
// the Flutter client sets to "ms since impression" for these too.
func engagementLatencyFromEvent(event FeedEvent) int {
	if event.WatchDurationMs > 0 {
		return event.WatchDurationMs
	}
	return 0
}
