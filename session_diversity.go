package main

import (
	"context"
	"strconv"
	"strings"
	"time"
)

// ─────────────────────────────────────────────────────────────────────────────
// SESSION-LEVEL CROSS-PAGE DIVERSITY
//
// MMR diversifies WITHIN a feed page — same creator/embedding can't sweep
// the visible window. But across PAGES of a session, the same category
// can creep back. A user fetches page 1 (5 cooking, then variety), then
// page 2 (5 cooking, then variety), then page 3 (5 cooking, then variety).
// MMR is happy; the user is bored.
//
// This module tracks per-session category counts in Redis (TTL = session
// length). On each new page, the ranker subtracts a penalty proportional
// to how often that category has appeared in this session's earlier pages.
// Penalty grows superlinearly so the 4th appearance of a category is much
// more punished than the 2nd — protecting variety without preventing a
// theme from establishing in the first place.
//
// The state is purely Redis-backed and tied to the session ID, so it
// resets naturally when sessions roll over (your existing 30-min idle
// rotation in EventTracker).
// ─────────────────────────────────────────────────────────────────────────────

const (
	// Hash key per session storing category → page count.
	sessionDiversityKeyPrefix = "sessdiv:"
	sessionDiversityTTL       = 90 * time.Minute
	// Penalty per duplicate appearance, applied superlinearly:
	//   1st time: 0 (free)
	//   2nd:      sessionDiversityPenaltyBase * 1
	//   3rd:      sessionDiversityPenaltyBase * 4
	//   4th:      sessionDiversityPenaltyBase * 9 (cap)
	sessionDiversityPenaltyBase = 0.04
	sessionDiversityMaxPenalty  = 0.36 // cap at 9 * base
)

// noteSessionCategories records the categories served on this page so
// future pages of the same session can see what's already happened.
// Best-effort; errors are swallowed.
func noteSessionCategories(sessionID string, categories []string) {
	if rdb == nil || sessionID == "" || len(categories) == 0 {
		return
	}
	key := sessionDiversityKeyPrefix + sessionID
	pipe := rdb.TxPipeline()
	// Aggregate same-page duplicates so one HIncrBy per unique category.
	counts := make(map[string]int)
	for _, c := range categories {
		c = strings.ToLower(strings.TrimSpace(c))
		if c == "" {
			continue
		}
		counts[c]++
	}
	for c, n := range counts {
		pipe.HIncrBy(rctx, key, c, int64(n))
	}
	pipe.Expire(rctx, key, sessionDiversityTTL)
	_, _ = pipe.Exec(rctx)
}

// sessionDiversityPenalty looks up how often this category has appeared
// earlier in the session and returns a bounded score penalty. Returns 0
// for the first appearance, growing superlinearly thereafter.
//
// Cheap: one HGET per call. For a typical 30-item feed × 6 categories,
// that's 6 lookups per page (callers should bulk-fetch via
// loadSessionCategoryCounts when ranking many candidates from few categories).
func sessionDiversityPenalty(sessionID, category string) float64 {
	if rdb == nil || sessionID == "" || category == "" {
		return 0
	}
	v, err := rdb.HGet(rctx, sessionDiversityKeyPrefix+sessionID, strings.ToLower(category)).Result()
	if err != nil || v == "" {
		return 0
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return 0
	}
	return diversityPenaltyForCount(n)
}

// loadSessionCategoryCounts hydrates the full session-categories hash so
// the per-candidate penalty lookup is O(1) without a Redis round-trip per
// candidate. Returns an empty map on any error.
//
// Production usage: call once at the top of SmartFeedHandler, then pass
// the map into scoring via a closure or a SessionState field.
func loadSessionCategoryCounts(_ context.Context, sessionID string) map[string]int {
	out := make(map[string]int, 8)
	if rdb == nil || sessionID == "" {
		return out
	}
	m, err := rdb.HGetAll(rctx, sessionDiversityKeyPrefix+sessionID).Result()
	if err != nil {
		return out
	}
	for k, v := range m {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			out[strings.ToLower(k)] = n
		}
	}
	return out
}

// diversityPenaltyForCount maps "how many times has this category appeared
// in the session" to the score penalty. Pure function — exposed so the
// in-memory hot-path can apply the penalty without extra Redis calls.
func diversityPenaltyForCount(n int) float64 {
	if n <= 1 {
		return 0
	}
	// (n-1)^2 * base, capped.
	excess := n - 1
	pen := float64(excess*excess) * sessionDiversityPenaltyBase
	if pen > sessionDiversityMaxPenalty {
		pen = sessionDiversityMaxPenalty
	}
	return pen
}
