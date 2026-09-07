package main

import (
	"math"
	"os"
	"strings"
	"testing"
)

// The point of the whole file: the same amount of engagement should mean
// different things on a quiet platform and a busy one. A fixed divisor could
// never do that.
func TestTrending_TheSameBurstMeansDifferentThingsAtDifferentSizes(t *testing.T) {
	const burst = 20

	// A small platform where the busy quarter of content gets 5 engagements
	// in two hours. Twenty is a phenomenon.
	small := trendingVelocity(burst, 5)
	// A large one where the busy quarter gets 400. Twenty is a quiet day.
	large := trendingVelocity(burst, 400)

	if small <= large {
		t.Fatalf("the same burst of %d scored %.3f on a quiet platform and "+
			"%.3f on a busy one. Trending has to be relative to the field, "+
			"or it stops meaning anything as the platform grows.",
			burst, small, large)
	}
	if small < 0.99 {
		t.Errorf("well past the pace should be full marks, got %.3f", small)
	}
	if large > 0.2 {
		t.Errorf("well under the pace should be a small score, got %.3f", large)
	}
}

func TestTrending_AtThePaceIsFullMarksAtAnyScale(t *testing.T) {
	for _, pace := range []float64{5, 50, 5000, 500000} {
		got := trendingVelocity(int(pace), pace)
		if math.Abs(got-1.0) > 0.001 {
			t.Errorf("content exactly at a pace of %.0f scored %.3f, want 1.0. "+
				"Being at the pace has to mean the same thing at every size.",
				pace, got)
		}
	}
}

func TestTrending_HalfThePaceIsAboutHalf(t *testing.T) {
	got := trendingVelocity(50, 100)
	if got < 0.45 || got > 0.55 {
		t.Errorf("half the pace scored %.3f, want about 0.5", got)
	}
}

// One person clicking is one person, however quiet it is around them.
func TestTrending_ATinyBurstIsNotATrend(t *testing.T) {
	// A very quiet platform: the pace to beat is a single engagement.
	one := trendingVelocity(1, 1)
	if one >= 0.5 {
		t.Errorf("a single engagement scored %.3f. On a quiet platform that "+
			"is technically 'at the pace', but one person is not a trend and "+
			"it must not score like one.", one)
	}
	five := trendingVelocity(5, 5)
	if five <= one {
		t.Error("five engagements should beat one")
	}
}

// A brand-new platform has no pace to compare against. That must not turn
// every like into a maximum score.
func TestTrending_NoPaceKnownDoesNotMakeEverythingTrend(t *testing.T) {
	if got := trendingVelocity(1, 0); got >= 0.5 {
		t.Errorf("with no pace known, one engagement scored %.3f. A platform "+
			"with no history should be cautious, not generous.", got)
	}
	if got := trendingVelocity(0, 0); got != 0 {
		t.Errorf("no engagement must score 0, got %.3f", got)
	}
}

func TestTrending_NeverExceedsOne(t *testing.T) {
	for _, c := range []struct {
		eng int
		ref float64
	}{{1000, 1}, {50, 0}, {1, 0.001}, {999999, 2}} {
		if got := trendingVelocity(c.eng, c.ref); got > 1.0 {
			t.Errorf("velocity(%d, %v) = %.3f, above the 0..1 range every "+
				"scoring term is clamped to", c.eng, c.ref, got)
		}
	}
}

// ── The row-count fallback, same rules ──────────────────────────────────────

func TestRowTrending_IsAlsoRelativeToTheField(t *testing.T) {
	// 100 views in 10 hours = 10 views/hour, on two platforms.
	quiet := rowTrendingScore(100, 10, 5)   // field does 5/hour
	busy := rowTrendingScore(100, 10, 2000) // field does 2000/hour
	if quiet <= busy {
		t.Fatalf("the same 10 views/hour scored %.3f in a quiet field and "+
			"%.3f in a busy one; it has to be a comparison", quiet, busy)
	}
}

func TestRowTrending_StaysBelowTheRealSignal(t *testing.T) {
	// However extreme, this path infers a burst from a lifetime view count.
	// It knows much less than the engagement path and must never outrank it.
	got := rowTrendingScore(1000000, 1, 0.001)
	if got > rowTrendingCap+0.0001 {
		t.Errorf("the row fallback scored %.3f, above its %.2f cap. It is a "+
			"weaker signal than measured engagement and has to stay one.",
			got, rowTrendingCap)
	}
}

func TestRowTrending_OldContentIsNotABurst(t *testing.T) {
	if got := rowTrendingScore(5000, rowTrendingWindowHours+1, 1); got != 0 {
		t.Errorf("content older than the window scored %.3f. A large lifetime "+
			"view count on an old video is accumulation, not a burst.", got)
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

// ── The guard ───────────────────────────────────────────────────────────────

// This is the test that matters in a year. The old code was not wrong; it was
// wrong LATER, and silently. If the typed-in numbers come back, so does that.
func TestTrending_NoFixedThresholdsComeBack(t *testing.T) {
	src, err := os.ReadFile("feed_engine.go")
	if err != nil {
		t.Fatalf("cannot read feed_engine.go: %v", err)
	}
	s := string(src)

	for _, gone := range []string{
		"float64(recentEng)/15.0",
		"viewsPerHour/30.0",
		"noteworthy for a small platform",
	} {
		if strings.Contains(s, gone) {
			t.Errorf("%q is back in feed_engine.go. Trending measured against "+
				"a fixed number works until the platform changes size, then "+
				"every video ties at the maximum and the signal goes flat "+
				"with no error to notice.", gone)
		}
	}

	if !strings.Contains(s, "trendingVelocity(") {
		t.Error("feed_engine.go no longer measures trending against the " +
			"platform's own pace")
	}
}

func TestTrending_ReferenceIsAProportionNotACount(t *testing.T) {
	if trendingReferencePercentile <= 0 || trendingReferencePercentile >= 1 {
		t.Fatalf("the pace anchor is %v. It has to be a proportion of the "+
			"field — a raw count would be the very thing this replaced.",
			trendingReferencePercentile)
	}
}

// With no database there is no pace, and that has to be survivable rather
// than fatal: a missing signal costs sharpness, never the feed.
func TestTrending_NoDatabaseIsSurvivable(t *testing.T) {
	saved := db
	db = nil
	defer func() { db = saved }()

	if got := measureTrendingReference(); got != 0 {
		t.Errorf("with no database the pace should be 0 (unknown), got %v", got)
	}
	if got := measureViewVelocityReference(); got != 0 {
		t.Errorf("with no database the view pace should be 0, got %v", got)
	}
}

// A view is much weaker evidence than a share, so it must take more of them.
func TestRowTrending_ViewsAreWeakerEvidenceThanEngagement(t *testing.T) {
	if rowTrendingConfidenceFull <= trendingConfidenceFull {
		t.Fatalf("views reach full confidence at %v and engagements at %v. "+
			"A view is barely a decision and a share is one, so it has to "+
			"take more views to say the same amount.",
			rowTrendingConfidenceFull, trendingConfidenceFull)
	}
	// A handful of views on a quiet field must not read as a burst.
	if got := rowTrendingScore(3, 1, 1); got > 0.1 {
		t.Errorf("three views scored %.3f. Three views per hour is arithmetic, "+
			"not evidence.", got)
	}
}
