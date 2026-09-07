package main

import (
	"math"
	"os"
	"strings"
	"testing"
)

func bench(m map[int]float64) *trendingBenchmarks {
	return &trendingBenchmarks{byTier: m}
}

// ── The thing that was broken twice ─────────────────────────────────────────

// This is the whole point. A small video doing well with its own audience must
// be able to beat a huge video doing ordinarily with its own.
//
// Under the old count-based version this was impossible by construction: the
// big video had more of everything, so it won on arithmetic before quality was
// ever considered.
func TestTrending_ANewVideoCanBeatAnEstablishedOne(t *testing.T) {
	b := bench(map[int]float64{
		1: 0.08, // small stage: the busy quarter get 8%
		3: 0.02, // big stage: the busy quarter get 2%
	})

	// 50 views, 10 people acted — 20%, way past its stage's 8%.
	newcomer := trendingBreakout(10, 50, b)
	// 500,000 views, 10,000 acted — 2%, exactly ordinary for its stage.
	established := trendingBreakout(10000, 500000, b)

	if newcomer <= established {
		t.Fatalf("a 50-view video doing 20%% scored %.3f and a 500k-view video "+
			"doing an ordinary 2%% scored %.3f. If size decides this, nothing "+
			"new can ever surface and the signal only ever repeats what is "+
			"already big.", newcomer, established)
	}
	if newcomer < 0.99 {
		t.Errorf("a video well past its stage's bar should be full marks, got %.3f", newcomer)
	}
}

// Being enormous is not the same as breaking out. Size is rewarded elsewhere
// in the score; it must not be paid twice here.
func TestTrending_CoastingOnABigAudienceIsNotTrending(t *testing.T) {
	b := bench(map[int]float64{3: 0.02})
	// A million views but a rate well under its stage's bar.
	got := trendingBreakout(5000, 1000000, b)
	if got > 0.4 {
		t.Errorf("a huge video performing below its own stage scored %.3f. "+
			"Big is not the same as breaking out.", got)
	}
}

// The same rate has to mean the same thing at every stage, or the bar is
// really just a size threshold wearing a disguise.
func TestTrending_BeatingYourStageMeansTheSameAtEveryStage(t *testing.T) {
	b := bench(map[int]float64{1: 0.10, 2: 0.05, 3: 0.02, 4: 0.01})
	cases := []struct {
		views, eng int
	}{
		{50, 10},      // 20% vs 10%
		{500, 50},     // 10% vs 5%
		{5000, 200},   // 4%  vs 2%
		{50000, 1000}, // 2%  vs 1%
	}
	var scores []float64
	for _, c := range cases {
		scores = append(scores, trendingBreakout(c.eng, c.views, b))
	}
	for i, s := range scores {
		if s < 0.9 {
			t.Errorf("case %d (%d views, %d acted) doubled its stage's bar but "+
				"scored %.3f. Doubling the bar should read the same at any size.",
				i, cases[i].views, cases[i].eng, s)
		}
	}
}

// ── Not judged yet, versus judged badly ─────────────────────────────────────

func TestTrending_ATinyAudienceIsNotJudgedYet(t *testing.T) {
	b := bench(map[int]float64{0: 0.05, 1: 0.08})
	// Three views, all three acted. Arithmetically 100%. Actually three people.
	if got := trendingBreakout(3, 3, b); got != 0 {
		t.Errorf("three-for-three scored %.3f. That is not a 100%% engagement "+
			"rate, it is three people, and treating it as a phenomenon is how "+
			"noise reaches the top of a feed.", got)
	}
	// Right at the floor it starts being measured.
	if got := trendingBreakout(10, trendingMinExposure, b); got <= 0 {
		t.Error("at the exposure floor a strong rate should start to count")
	}
}

func TestTrending_NoPeersMeansNoAnswer(t *testing.T) {
	// A brand-new platform: no benchmarks at all.
	if got := trendingBreakout(50, 100, bench(nil)); got != 0 {
		t.Errorf("with nothing to compare against, a video scored %.3f. "+
			"Trending is a comparison; inventing an answer would make the "+
			"first video of a quiet hour a phenomenon every time.", got)
	}
	if got := trendingBreakout(50, 100, nil); got != 0 {
		t.Errorf("nil benchmarks must be survivable, got %.3f", got)
	}
}

// An empty stage must not mean "everything here wins".
func TestTrending_AnEmptyStageBorrowsTheNearestOne(t *testing.T) {
	b := bench(map[int]float64{1: 0.10})
	// Tier 3 has no bar of its own; it should fall back to tier 1's, not to
	// "no bar, therefore full marks".
	got := trendingBreakout(20, 2000, b) // 1% against a borrowed 10% bar
	if got > 0.3 {
		t.Errorf("a video in an empty stage scored %.3f against a borrowed "+
			"bar it is well under. An empty stage must not be a free pass.", got)
	}
	if got := b.forTier(3); math.Abs(got-0.10) > 1e-9 {
		t.Errorf("forTier(3) = %v, want the nearest real bar 0.10", got)
	}
}

// ── Stages ──────────────────────────────────────────────────────────────────

func TestTrending_StagesGrowByMultiples(t *testing.T) {
	cases := []struct {
		views, tier int
	}{{0, 0}, {1, 0}, {9, 0}, {10, 1}, {99, 1}, {100, 2}, {5000, 3}, {1000000, 6}}
	for _, c := range cases {
		if got := exposureTier(c.views); got != c.tier {
			t.Errorf("exposureTier(%d) = %d, want %d", c.views, got, c.tier)
		}
	}
	if got := exposureTier(-5); got != 0 {
		t.Errorf("a negative view count must not produce a negative stage, got %d", got)
	}
}

func TestTrending_ScoreStaysInRange(t *testing.T) {
	b := bench(map[int]float64{0: 0.001, 1: 0.5, 2: 0.02, 3: 0.02})
	for _, c := range []struct{ eng, views int }{
		{1000000, 100}, {0, 500}, {-5, 500}, {50, 0}, {50, -3}, {1, 1000000},
	} {
		got := trendingBreakout(c.eng, c.views, b)
		if got < 0 || got > 1 || math.IsNaN(got) {
			t.Errorf("trendingBreakout(%d, %d) = %v, outside 0..1",
				c.eng, c.views, got)
		}
	}
}

// ── The guard ───────────────────────────────────────────────────────────────

// The one that matters in a year. Both previous versions of this signal were
// fine on the day they shipped and wrong later.
func TestTrending_NeitherOldModelComesBack(t *testing.T) {
	src, err := os.ReadFile("feed_engine.go")
	if err != nil {
		t.Fatalf("cannot read feed_engine.go: %v", err)
	}
	s := string(src)

	for _, gone := range []struct{ frag, why string }{
		{"float64(recentEng)/15.0",
			"a typed-in threshold: every video ties at the maximum once the platform outgrows it"},
		{"viewsPerHour/30.0",
			"the same typed-in threshold on the view-count path"},
		{"noteworthy for a small platform",
			"a rule justified by how small we are today"},
		{"trendingVelocity(",
			"comparing raw engagement counts: the biggest audiences set the bar, so nothing new can ever surface"},
	} {
		if strings.Contains(s, gone.frag) {
			t.Errorf("%q is back in feed_engine.go — %s", gone.frag, gone.why)
		}
	}

	if !strings.Contains(s, "trendingBreakout(") {
		t.Error("feed_engine.go no longer scores trending by how a video does " +
			"with the people who saw it, against its own stage")
	}
}

func TestTrending_TheBarIsAProportionNotACount(t *testing.T) {
	if trendingReferencePercentile <= 0 || trendingReferencePercentile >= 1 {
		t.Fatalf("the bar is %v; it has to be a proportion of the field, "+
			"which is the whole point", trendingReferencePercentile)
	}
	if exposureTierBase <= 1 {
		t.Fatalf("stages must grow by a multiple, got %v", exposureTierBase)
	}
}

func TestTrending_NoDatabaseIsSurvivable(t *testing.T) {
	saved := db
	db = nil
	defer func() { db = saved }()

	b := measureTrendingBenchmarks()
	if b == nil || len(b.byTier) != 0 {
		t.Error("with no database there should be no benchmarks, not a crash")
	}
	if got := measureViewVelocityReference(); got != 0 {
		t.Errorf("with no database the view pace should be 0, got %v", got)
	}
}

// ── The row-count fallback (content the analytics pipeline never saw) ────────

func TestRowTrending_IsAlsoRelativeToTheField(t *testing.T) {
	quiet := rowTrendingScore(100, 10, 5)
	busy := rowTrendingScore(100, 10, 2000)
	if quiet <= busy {
		t.Fatalf("the same 10 views/hour scored %.3f in a quiet field and "+
			"%.3f in a busy one; it has to be a comparison", quiet, busy)
	}
}

func TestRowTrending_StaysBelowTheRealSignal(t *testing.T) {
	got := rowTrendingScore(1000000, 1, 0.001)
	if got > rowTrendingCap+0.0001 {
		t.Errorf("the row fallback scored %.3f, above its %.2f cap. It infers "+
			"a burst from a lifetime view count, which is much weaker than "+
			"measured engagement, and must never outrank it.", got, rowTrendingCap)
	}
}

func TestRowTrending_OldContentIsNotABurst(t *testing.T) {
	if got := rowTrendingScore(5000, rowTrendingWindowHours+1, 1); got != 0 {
		t.Errorf("content older than the window scored %.3f. A big lifetime "+
			"view count on an old video is accumulation, not a burst.", got)
	}
}

func TestRowTrending_ViewsAreWeakerEvidenceThanEngagement(t *testing.T) {
	if got := rowTrendingScore(3, 1, 1); got > 0.1 {
		t.Errorf("three views scored %.3f. Three views per hour is arithmetic, "+
			"not evidence.", got)
	}
}

func TestRowTrending_HandlesNonsenseWithoutPanicking(t *testing.T) {
	for _, c := range []struct {
		views int
		age   float64
		ref   float64
	}{{0, 5, 1}, {10, 0, 1}, {10, -3, 1}, {-5, 5, 1}, {10, 5, 0}} {
		got := rowTrendingScore(c.views, c.age, c.ref)
		if got < 0 || got > rowTrendingCap+0.0001 {
			t.Errorf("rowTrendingScore(%d, %v, %v) = %.3f, outside 0..%.2f",
				c.views, c.age, c.ref, got, rowTrendingCap)
		}
	}
}

// Exact powers of ten are where floating point quietly puts a video in the
// wrong stage — and being in the stage below means being judged against
// easier company, which nothing would ever flag as an error.
func TestTrending_ExactStageBoundariesLandInTheRightStage(t *testing.T) {
	for tier, views := range map[int]int{
		1: 10, 2: 100, 3: 1000, 4: 10000, 5: 100000, 6: 1000000, 7: 10000000,
	} {
		if got := exposureTier(views); got != tier {
			t.Errorf("exposureTier(%d) = %d, want %d. A video sitting exactly "+
				"on a stage boundary is being compared against the wrong peers.",
				views, got, tier)
		}
		// And one view short is still the stage below.
		if got := exposureTier(views - 1); got != tier-1 {
			t.Errorf("exposureTier(%d) = %d, want %d", views-1, got, tier-1)
		}
	}
}
