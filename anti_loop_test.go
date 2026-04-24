package main

import "testing"

func TestDetectLoop_NilSession(t *testing.T) {
	if d := detectLoop(nil); d.Stuck {
		t.Fatal("nil session should not be stuck")
	}
}

func TestDetectLoop_SkipStreakFires(t *testing.T) {
	s := &SessionState{SkipStreak: 5}
	d := detectLoop(s)
	if !d.Stuck || d.Reason != "skip_streak" {
		t.Fatalf("expected skip_streak, got %+v", d)
	}
	if d.SuggestedStrat == "" {
		t.Fatal("expected a suggested strategy")
	}
}

func TestDetectLoop_CategoryMonoculture(t *testing.T) {
	s := &SessionState{
		LastCategories: []string{"gaming", "gaming", "gaming", "music"},
	}
	d := detectLoop(s)
	if !d.Stuck || d.Reason != "category_monoculture" {
		t.Fatalf("expected category_monoculture, got %+v", d)
	}
}

func TestDetectLoop_CreatorFlood(t *testing.T) {
	s := &SessionState{
		LastCreators: []string{"alice", "alice", "bob", "alice", "carol"},
	}
	d := detectLoop(s)
	if !d.Stuck || d.Reason != "creator_flood" {
		t.Fatalf("expected creator_flood, got %+v", d)
	}
}

func TestDetectLoop_DopamineCollapse(t *testing.T) {
	s := &SessionState{DopamineBudget: 0.05}
	d := detectLoop(s)
	if !d.Stuck || d.Reason != "dopamine_collapse" {
		t.Fatalf("expected dopamine_collapse, got %+v", d)
	}
}

func TestDetectLoop_HealthyReturnsEmpty(t *testing.T) {
	s := &SessionState{
		SkipStreak:     1,
		LastCategories: []string{"a", "b", "c"},
		LastCreators:   []string{"x", "y", "z"},
		DopamineBudget: 0.9,
	}
	d := detectLoop(s)
	if d.Stuck {
		t.Fatalf("healthy session should not be stuck, got %+v", d)
	}
}

func TestPickUntriedStrategy_FirstUnseenWins(t *testing.T) {
	s := &SessionState{TriedStrategies: []string{"discovery"}}
	got := pickUntriedStrategy(s, []string{"discovery", "calming", "fresh_blood"})
	if got != "calming" {
		t.Fatalf("expected calming (first untried), got %q", got)
	}
}

func TestPickUntriedStrategy_AllTriedFallbackFirst(t *testing.T) {
	s := &SessionState{TriedStrategies: []string{"a", "b"}}
	got := pickUntriedStrategy(s, []string{"a", "b"})
	if got != "a" {
		t.Fatalf("expected fallback to first pref, got %q", got)
	}
}

func TestDominantValue_TailWindow(t *testing.T) {
	got, n := dominantValue([]string{"old", "old", "old", "new", "new", "new"}, 3)
	if got != "new" || n != 3 {
		t.Fatalf("expected new:3, got %q:%d", got, n)
	}
}

func TestDominantValue_EmptyInputs(t *testing.T) {
	if _, n := dominantValue(nil, 5); n != 0 {
		t.Fatalf("empty input should give n=0")
	}
	if _, n := dominantValue([]string{"a"}, 0); n != 0 {
		t.Fatalf("tailN=0 should give n=0")
	}
}
