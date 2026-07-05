package main

import (
	"fmt"
	"testing"
)

// pushRawImpression writes one impression entry in recordImpression's exact wire
// format: category|dwellMs|timestamp|creator|contentId|contentType.
func pushRawImpression(t *testing.T, userID, category string, dwellMs int, ts int64) {
	t.Helper()
	entry := fmt.Sprintf("%s|%d|%d|%s|%s|%s", category, dwellMs, ts, "creatorX", "c1", "challenge")
	if err := rdb.LPush(rctx, "impressions:"+userID, entry).Err(); err != nil {
		t.Fatalf("LPush: %v", err)
	}
}

// TestImpressionFreshGating is the regression guard for #12 (re-decay
// saturation). The aggregator must compute per-category stats over the FULL
// retained window (stable rates / minCategoryImpressions) but only mark a
// category "fresh" — eligible for a ±affinity nudge — when it received an
// impression newer than the aggregator's cursor. That gate is what stops the
// same 48h-retained impressions from nudging the same category every 5-min
// cycle and saturating CategoryAffinity to 0/1.
func TestImpressionFreshGating(t *testing.T) {
	resetRedis(t)
	u := "u_redecay"

	// Old impressions (ts=1000) in "comedy"; newer ones (ts=2000) in "sports".
	for i := 0; i < 6; i++ {
		pushRawImpression(t, u, "comedy", 100, 1000)
	}
	for i := 0; i < 6; i++ {
		pushRawImpression(t, u, "sports", 100, 2000)
	}

	// Full window (cursor=0): both categories present with their full counts, and
	// NOTHING is marked fresh — the full-window callers (admin/diagnostics,
	// serve-time signal cache) must never see a fresh set.
	bc, _, fresh0 := getImpressionStatsWithFresh(u, 0)
	if bc["comedy"] == nil || bc["sports"] == nil {
		t.Fatalf("full-window stats missing a category: %v", bc)
	}
	if len(fresh0) != 0 {
		t.Errorf("cursor=0 must mark nothing fresh, got %v", fresh0)
	}

	// Cursor at 1500: only "sports" (ts=2000 > 1500) is fresh; "comedy"
	// (ts=1000 <= 1500) is stale and must NOT be nudged again.
	bc2, _, fresh := getImpressionStatsWithFresh(u, 1500)
	if !fresh["sports"] {
		t.Errorf("sports (ts=2000 > cursor 1500) must be fresh")
	}
	if fresh["comedy"] {
		t.Errorf("comedy (ts=1000 <= cursor 1500) must NOT be fresh — this is the re-decay guard")
	}

	// Crucially, the full-window sample sizes are INDEPENDENT of the cursor, so
	// rate/threshold decisions stay stable regardless of what's fresh.
	if bc2["comedy"] == nil || bc2["comedy"].Count != 6 {
		t.Errorf("comedy full-window count must stay 6 regardless of cursor, got %v", bc2["comedy"])
	}
	if bc2["sports"] == nil || bc2["sports"].Count != 6 {
		t.Errorf("sports full-window count must stay 6 regardless of cursor, got %v", bc2["sports"])
	}

	// A cursor at/after the newest impression makes NOTHING fresh — the
	// steady-state path where the aggregator advances its cursor and applies no
	// nudge (idempotent over already-processed impressions).
	_, _, freshNone := getImpressionStatsWithFresh(u, 2000)
	if len(freshNone) != 0 {
		t.Errorf("cursor at newest ts must mark nothing fresh, got %v", freshNone)
	}
}
