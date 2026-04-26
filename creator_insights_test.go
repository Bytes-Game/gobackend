package main

import (
	"testing"
)

// ─────────────────────────────────────────────────────────────────────────────
// Creator-insights pure-logic tests.
//
// As with notifications, the SQL paths need a real Postgres — out of scope
// here. We test the parts that are CPU-only: percentile math, watch-bucket
// labeling, hook-strength heuristics, recommendation generators, and the
// helpers we own.
// ─────────────────────────────────────────────────────────────────────────────

func TestInsights_PercentileSorted(t *testing.T) {
	xs := []float64{0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0}
	if got := percentileSorted(xs, 50); got < 0.4 || got > 0.6 {
		t.Errorf("p50 of 0.1..1.0 should be ~0.5, got %v", got)
	}
	if got := percentileSorted(xs, 90); got < 0.85 {
		t.Errorf("p90 should be near 0.9, got %v", got)
	}
	if got := percentileSorted([]float64{}, 50); got != 0 {
		t.Errorf("empty input should return 0, got %v", got)
	}
}

func TestInsights_WatchBucketLabel(t *testing.T) {
	cases := []struct {
		ratio float64
		want  string
	}{
		{0.10, "0-25%"},
		{0.30, "25-50%"},
		{0.65, "50-75%"},
		{0.85, "75-100%"},
		{1.0, "100%+"},
		{1.5, "100%+"},
	}
	for _, c := range cases {
		if got := watchBucketLabel(c.ratio); got != c.want {
			t.Errorf("ratio=%v: want %q, got %q", c.ratio, c.want, got)
		}
	}
}

func TestInsights_HookStrengthLabel(t *testing.T) {
	if hookStrengthLabel(0.20, 0.70) != "strong" {
		t.Errorf("low early-skip + high completion → strong")
	}
	if hookStrengthLabel(0.45, 0.40) != "ok" {
		t.Errorf("mid signals → ok")
	}
	if hookStrengthLabel(0.75, 0.20) != "weak" {
		t.Errorf("high skip + low completion → weak")
	}
}

func TestInsights_RecommendationsForContent(t *testing.T) {
	// High early-skip percentage triggers hook recommendation.
	c := CreatorPerContent{EarlySkipPct: 0.7, Completion: 0.5, Views: 100}
	recs := recommendationsForContent(c)
	if len(recs) == 0 {
		t.Fatal("should produce recommendations")
	}
	if !containsAny(recs, "hook") {
		t.Errorf("expected a hook recommendation, got %v", recs)
	}

	// Low completion triggers length recommendation.
	c = CreatorPerContent{EarlySkipPct: 0.2, Completion: 0.20, Views: 100}
	recs = recommendationsForContent(c)
	if !containsAny(recs, "30%") && !containsAny(recs, "trimming") {
		t.Errorf("expected trim/length recommendation, got %v", recs)
	}

	// Strong content triggers positive reinforcement.
	c = CreatorPerContent{EarlySkipPct: 0.1, Completion: 0.85, Views: 50}
	recs = recommendationsForContent(c)
	if !containsAny(recs, "Strong") || !containsAny(recs, "reach") {
		t.Errorf("expected strong+reach recommendation, got %v", recs)
	}
}

func TestInsights_RecommendationsForOverviewLowCompletion(t *testing.T) {
	o := CreatorOverview{
		AvgCompletion: 0.30,
		TotalPosts:    5,
		CategoryStats: map[string]CategoryStat{},
	}
	contents := []CreatorContentSummary{
		{Completion: 0.3, SkipRate: 0.4},
		{Completion: 0.3, SkipRate: 0.4},
	}
	recs := recommendationsForOverview(o, contents)
	if !containsAny(recs, "completion") {
		t.Errorf("low completion should trigger completion advice, got %v", recs)
	}
}

func TestInsights_RecommendationsForOverviewTopTier(t *testing.T) {
	o := CreatorOverview{
		AvgCompletion:  0.85,
		TotalPosts:     10,
		CompletionRank: "top 10% (you beat 95% of creators)",
		CategoryStats:  map[string]CategoryStat{},
	}
	contents := []CreatorContentSummary{{Completion: 0.85, SkipRate: 0.1}}
	recs := recommendationsForOverview(o, contents)
	if !containsAny(recs, "top 10%") {
		t.Errorf("top tier should get positive recommendation, got %v", recs)
	}
}

func TestInsights_Min3AndMax1(t *testing.T) {
	if min3(2) != 2 || min3(5) != 3 {
		t.Errorf("min3 broken")
	}
	if max1(0) != 1 || max1(5) != 5 {
		t.Errorf("max1 broken")
	}
}

func TestInsights_ReverseSummaries(t *testing.T) {
	in := []CreatorContentSummary{{ContentID: "a"}, {ContentID: "b"}, {ContentID: "c"}}
	out := reverseSummaries(in)
	if out[0].ContentID != "c" || out[2].ContentID != "a" {
		t.Errorf("reverseSummaries broken: %v", out)
	}
}

func containsAny(xs []string, sub string) bool {
	for _, x := range xs {
		if len(x) >= len(sub) && (indexOfSubstr(x, sub) >= 0) {
			return true
		}
	}
	return false
}

func indexOfSubstr(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
