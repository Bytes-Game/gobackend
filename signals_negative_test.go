package main

import (
	"testing"
	"time"
)

// ────────────────────────────────────────────────────────────────────────────
// Pure scoring-helper tests (no Redis round-trip)
// ────────────────────────────────────────────────────────────────────────────

func TestNegativeCreatorPenalty_NilSignals(t *testing.T) {
	if got := negativeCreatorPenalty(nil, "c1"); got != 1.0 {
		t.Errorf("nil signals should yield 1.0, got %v", got)
	}
}

func TestNegativeCreatorPenalty_Blocked(t *testing.T) {
	ns := &negativeSignals{blocked: map[string]bool{"c1": true}}
	if got := negativeCreatorPenalty(ns, "c1"); got != 0.0 {
		t.Errorf("blocked creator should yield 0.0, got %v", got)
	}
}

func TestNegativeCreatorPenalty_UnfollowedRecent(t *testing.T) {
	ns := &negativeSignals{
		blocked:    map[string]bool{},
		unfollowed: map[string]float64{"c1": float64(time.Now().Unix())},
	}
	got := negativeCreatorPenalty(ns, "c1")
	// Fresh unfollow → max attenuation (0.5).
	if got < 0.49 || got > 0.51 {
		t.Errorf("fresh unfollow should yield ~0.5, got %v", got)
	}
}

func TestNegativeCreatorPenalty_UnfollowedExpired(t *testing.T) {
	past := time.Now().Add(-8 * 24 * time.Hour).Unix()
	ns := &negativeSignals{
		blocked:    map[string]bool{},
		unfollowed: map[string]float64{"c1": float64(past)},
	}
	if got := negativeCreatorPenalty(ns, "c1"); got != 1.0 {
		t.Errorf("expired unfollow should yield 1.0, got %v", got)
	}
}

func TestBouncePenalty_NilSignals(t *testing.T) {
	if got := bouncePenalty(nil, "post", "p1"); got != 1.0 {
		t.Errorf("nil signals should yield 1.0, got %v", got)
	}
}

func TestBouncePenalty_Bounced(t *testing.T) {
	ns := &negativeSignals{bounces: map[string]bool{"post:p1": true}}
	if got := bouncePenalty(ns, "post", "p1"); got != 0.0 {
		t.Errorf("bounced item should yield 0.0, got %v", got)
	}
}

func TestBouncePenalty_NotBounced(t *testing.T) {
	ns := &negativeSignals{bounces: map[string]bool{}}
	if got := bouncePenalty(ns, "post", "p1"); got != 1.0 {
		t.Errorf("unbounced item should yield 1.0, got %v", got)
	}
}

func TestSearchBoost_MatchMostRecent(t *testing.T) {
	ns := &negativeSignals{recentQueries: []string{"coding tutorial", "food"}}
	got := searchBoost(ns, "coding", "learn coding tutorial basics")
	if got < 0.99 {
		t.Errorf("top query match should yield ~1.0, got %v", got)
	}
}

func TestSearchBoost_NoMatch(t *testing.T) {
	ns := &negativeSignals{recentQueries: []string{"coding"}}
	got := searchBoost(ns, "dance", "hip-hop moves")
	if got != 0 {
		t.Errorf("no match should yield 0, got %v", got)
	}
}

func TestSearchBoost_EmptyQueries(t *testing.T) {
	ns := &negativeSignals{recentQueries: nil}
	if got := searchBoost(ns, "x", "y"); got != 0 {
		t.Errorf("no queries should yield 0, got %v", got)
	}
}

func TestSessionContinuityFactor_NoSession(t *testing.T) {
	ns := &negativeSignals{}
	if got := sessionContinuityFactor(ns); got != 1.0 {
		t.Errorf("no prior session should yield 1.0, got %v", got)
	}
}

func TestSessionContinuityFactor_Tiers(t *testing.T) {
	now := time.Now()
	cases := []struct {
		gap    time.Duration
		expect float64
	}{
		{30 * time.Minute, 0.2},
		{4 * time.Hour, 0.4},
		{12 * time.Hour, 0.7},
		{2 * 24 * time.Hour, 0.9},
		{10 * 24 * time.Hour, 1.0},
	}
	for _, c := range cases {
		ns := &negativeSignals{lastSessionEnd: now.Add(-c.gap)}
		got := sessionContinuityFactor(ns)
		if got != c.expect {
			t.Errorf("gap=%v expect %v got %v", c.gap, c.expect, got)
		}
	}
}

// ────────────────────────────────────────────────────────────────────────────
// Redis-roundtrip integration tests (miniredis)
// ────────────────────────────────────────────────────────────────────────────

func TestSignals_BlockRoundTrip(t *testing.T) {
	resetRedis(t)
	MarkBlocked("u1", "c1")
	warmNegativeSignals("u1")
	ns := getNegativeSignals("u1")
	if ns == nil || !ns.blocked["c1"] {
		t.Fatalf("block not persisted, ns=%+v", ns)
	}
	if negativeCreatorPenalty(ns, "c1") != 0 {
		t.Error("blocked creator should score 0")
	}
	UnmarkBlocked("u1", "c1")
	// Bust request cache — warmNegativeSignals re-reads Redis.
	negativeCache = NewSignalCache[*negativeSignals](2 * time.Minute)
	warmNegativeSignals("u1")
	ns = getNegativeSignals("u1")
	if ns.blocked["c1"] {
		t.Error("unblock should clear the flag")
	}
}

func TestSignals_UnfollowRoundTrip(t *testing.T) {
	resetRedis(t)
	negativeCache = NewSignalCache[*negativeSignals](2 * time.Minute)
	MarkUnfollowed("u1", "c2")
	warmNegativeSignals("u1")
	ns := getNegativeSignals("u1")
	if _, ok := ns.unfollowed["c2"]; !ok {
		t.Fatalf("unfollow not persisted, ns=%+v", ns)
	}
	p := negativeCreatorPenalty(ns, "c2")
	if p > 0.6 || p < 0.4 {
		t.Errorf("fresh unfollow expected ~0.5 penalty, got %v", p)
	}
}

func TestSignals_BounceRoundTrip(t *testing.T) {
	resetRedis(t)
	negativeCache = NewSignalCache[*negativeSignals](2 * time.Minute)
	MarkBounce("u1", "post:p9")
	warmNegativeSignals("u1")
	ns := getNegativeSignals("u1")
	if !ns.bounces["post:p9"] {
		t.Errorf("bounce not persisted, ns=%+v", ns)
	}
	if bouncePenalty(ns, "post", "p9") != 0 {
		t.Error("bounced content should score 0")
	}
	if bouncePenalty(ns, "post", "p10") != 1 {
		t.Error("non-bounced content should score 1")
	}
}

func TestSignals_SearchRoundTrip(t *testing.T) {
	resetRedis(t)
	negativeCache = NewSignalCache[*negativeSignals](2 * time.Minute)
	RecordSearchQuery("u1", "DANCE tutorials   ")
	RecordSearchQuery("u1", "food recipes")
	warmNegativeSignals("u1")
	ns := getNegativeSignals("u1")
	if len(ns.recentQueries) != 2 {
		t.Fatalf("expected 2 recent queries, got %v", ns.recentQueries)
	}
	if ns.recentQueries[0] != "food recipes" {
		t.Errorf("most recent query should be first, got %q", ns.recentQueries[0])
	}
	// searchBoost uses substring match on "category + space + caption".
	// Recent queries after LPUSH: ["food recipes", "dance tutorials"].
	if searchBoost(ns, "home cooking", "food recipes for beginners") < 0.99 {
		t.Error("matching top query should boost ~1.0")
	}
	if searchBoost(ns, "sports", "football highlights") != 0 {
		t.Error("non-matching category should not boost")
	}
}

func TestSignals_SearchCapEnforced(t *testing.T) {
	resetRedis(t)
	negativeCache = NewSignalCache[*negativeSignals](2 * time.Minute)
	for i := 0; i < 25; i++ {
		RecordSearchQuery("u1", "q"+string(rune('a'+i%26)))
	}
	warmNegativeSignals("u1")
	ns := getNegativeSignals("u1")
	if len(ns.recentQueries) > recentSearchCap {
		t.Errorf("cap=%d but got %d queries", recentSearchCap, len(ns.recentQueries))
	}
}

func TestSignals_SessionEndRoundTrip(t *testing.T) {
	resetRedis(t)
	negativeCache = NewSignalCache[*negativeSignals](2 * time.Minute)
	RecordSessionEnd("u1")
	warmNegativeSignals("u1")
	ns := getNegativeSignals("u1")
	if ns.lastSessionEnd.IsZero() {
		t.Error("session end not persisted")
	}
	// Gap should be tiny — continuity factor should be minimal (0.2 tier).
	if f := sessionContinuityFactor(ns); f != 0.2 {
		t.Errorf("fresh session-end should yield 0.2 continuity, got %v", f)
	}
}

func TestSignals_EmptyUserIDIsNoop(t *testing.T) {
	resetRedis(t)
	MarkBlocked("", "c1")
	MarkUnfollowed("", "c1")
	MarkBounce("", "post:1")
	RecordSearchQuery("", "x")
	RecordSessionEnd("")
	keys := mr.Keys()
	if len(keys) != 0 {
		t.Errorf("empty userID should write no keys, got %v", keys)
	}
}
