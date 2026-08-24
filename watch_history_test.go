package main

import (
	"math"
	"testing"
	"time"
)

// The complaint this exists to answer, in one sentence: a video watched
// yesterday came back today and beat content the viewer had never seen.
//
// It could, because past the twelve-hour seen window the only thing between
// them was a flat 0.15 bonus on the competition. These tests are about the
// handicap that closes that gap, and about the line it must not cross —
// nothing here is ever allowed to withhold an item, because this codebase
// has broken its own feed three separate times doing exactly that.

func day(n float64) int64 { return int64(n * 24 * 3600) }

func historyAt(secondsAgo int64, strength float64) (watchHistory, int64) {
	now := time.Now().Unix()
	return exactHistory(map[string]watchMemory{
		"challenge:v1": {LastAt: now - secondsAgo, Strength: strength},
	}), now
}

func exactHistory(m map[string]watchMemory) watchHistory {
	return watchHistory{exact: m}
}

func TestSuppression_NeverWatchedIsZero(t *testing.T) {
	h := watchHistory{}
	if got := h.suppression("challenge:v1", time.Now().Unix()); got != 0 {
		t.Errorf("something nobody watched carried a handicap of %v", got)
	}
	if h.seen("challenge:v1") {
		t.Error("an empty history claimed to have seen something")
	}
}

func TestSuppression_FadesWithAge(t *testing.T) {
	// The property the whole design rests on: strong right after watching,
	// gone by the end of the window, monotonically decreasing in between. A
	// non-monotonic curve would mean a video getting HARDER to see as the
	// memory ages, which nobody could reason about.
	prev := math.Inf(1)
	for _, age := range []int64{0, day(1), day(7), day(30), day(60), day(89)} {
		h, now := historyAt(age, 1)
		got := h.suppression("challenge:v1", now)
		if got > prev {
			t.Errorf("handicap went UP with age: %v at %d days after %v",
				got, age/86400, prev)
		}
		if got < 0 {
			t.Errorf("negative handicap %v — that would PROMOTE a repeat", got)
		}
		prev = got
	}
}

func TestSuppression_YesterdayStillOutweighsTheUnseenBonus(t *testing.T) {
	// This is the bug, expressed as a number.
	//
	// The unseen bonus is 0.15. If a video watched yesterday carries less
	// handicap than that, it is competing on level terms with content nobody
	// has been shown, and a good one wins — which is exactly what was
	// reported.
	h, now := historyAt(day(1), 1)
	got := h.suppression("challenge:v1", now)
	if got <= 0.15 {
		t.Errorf("a video watched yesterday carries a handicap of %v against "+
			"an unseen bonus of 0.15 — it can still beat content the viewer "+
			"has never seen, which is the whole complaint", got)
	}
}

func TestSuppression_HandsOffFromTheTwelveHourCurve(t *testing.T) {
	// The two memories have to meet. seen_filter.go's penalty reaches ZERO at
	// twelve hours; if this one has not taken over by then, there is a gap
	// where a just-watched video is suddenly unprotected.
	h, now := historyAt(int64(12*time.Hour.Seconds()), 1)
	got := h.suppression("challenge:v1", now)
	if got < seenPenaltyMax*0.9 {
		t.Errorf("at twelve hours — exactly where the short-term penalty hits "+
			"zero — this carries only %v against that curve's starting %v. "+
			"That is a gap a repeat walks straight through.", got, seenPenaltyMax)
	}
}

func TestSuppression_ExpiresAtTheEndOfTheWindow(t *testing.T) {
	// Past the window content genuinely may come round again. On a small
	// catalogue that is not a concession, it is what keeps there being
	// anything to watch.
	h, now := historyAt(day(float64(watchMemoryDays)+1), 1)
	if got := h.suppression("challenge:v1", now); got != 0 {
		t.Errorf("something watched beyond the %d-day window still carried %v",
			watchMemoryDays, got)
	}
}

func TestSuppression_ScalesWithHowMuchTheyActuallyWatched(t *testing.T) {
	// A video somebody finished is definitively watched. One that flew past
	// while they were scrolling barely registered, and holding it back for
	// three months on that basis quietly shrinks the catalogue.
	finished, now := historyAt(day(1), 1.0)
	glimpsed, _ := historyAt(day(1), 0.25)
	a := finished.suppression("challenge:v1", now)
	b := glimpsed.suppression("challenge:v1", now)
	if b >= a {
		t.Errorf("a glimpse was suppressed as hard as a finished watch: "+
			"glimpse=%v finished=%v", b, a)
	}
	if b <= 0 {
		t.Errorf("a glimpse carried no handicap at all (%v)", b)
	}
}

// ── The line this must not cross ────────────────────────────────────────────

func TestSuppression_IsFiniteAndNeverWithholds(t *testing.T) {
	// Three earlier attempts at this problem hard-filtered seen content, and
	// all three broke the feed: it emptied on a small catalogue, a request for
	// thirty came back as eight, and a page filled by backfill reported "no
	// more" and ended the feed outright. See seen_filter.go.
	//
	// The handicap is bounded so a repeat always keeps a real, comparable
	// score. When there is nothing else left, repeats rank among themselves
	// and the feed keeps moving instead of running dry.
	h, now := historyAt(0, 1)
	worst := h.suppression("challenge:v1", now)
	if worst > rewatchPenaltyMax {
		t.Errorf("handicap %v exceeds its own ceiling %v", worst, rewatchPenaltyMax)
	}
	if math.IsInf(worst, 0) || math.IsNaN(worst) {
		t.Errorf("handicap was %v — an item with no comparable score is a "+
			"filter wearing a score's clothes", worst)
	}
}

func TestSuppression_SurvivesNonsenseTimestamps(t *testing.T) {
	now := time.Now().Unix()
	// A clock that ran backwards. Clamps to "just watched" — the strongest
	// handicap — rather than going negative and PROMOTING the repeat.
	future := exactHistory(map[string]watchMemory{"challenge:v1": {LastAt: now + 9999, Strength: 1}})
	if got := future.suppression("challenge:v1", now); got < 0 {
		t.Errorf("a future timestamp produced %v, which would promote a repeat", got)
	}
	// A record with no time at all is not evidence of anything.
	zero := exactHistory(map[string]watchMemory{"challenge:v1": {LastAt: 0, Strength: 1}})
	if got := zero.suppression("challenge:v1", now); got != 0 {
		t.Errorf("a record with no timestamp produced %v", got)
	}
	// Strength outside 0..1 cannot escape the ceiling.
	wild := exactHistory(map[string]watchMemory{"challenge:v1": {LastAt: now, Strength: 50}})
	if got := wild.suppression("challenge:v1", now); got > rewatchPenaltyMax {
		t.Errorf("a strength of 50 produced %v, past the ceiling %v",
			got, rewatchPenaltyMax)
	}
}

func TestWatchMemory_HoldsMoreThanASession(t *testing.T) {
	// A single real session started 130 videos. A memory of a thousand events
	// is about a week, after which a regular viewer stops being remembered
	// properly — which is the shape of the original bug.
	if watchMemoryEvents < 5000 {
		t.Errorf("the memory holds %d events, roughly %d sessions at the "+
			"130-a-session seen in a real log", watchMemoryEvents,
			watchMemoryEvents/130)
	}
	if watchMemoryDays < 30 {
		t.Errorf("the memory reaches back %d days; below about a month, "+
			"recently watched content starts reading as new", watchMemoryDays)
	}
}

func TestBuildWatchHistory_NoDatabaseIsEmptyNotAPanic(t *testing.T) {
	// db is nil under test. Empty reads as never-watched, which hands out the
	// unseen bonus freely and applies no handicap — wrong in the harmless
	// direction, and the twelve-hour seen-set still holds regardless.
	if got := buildWatchHistory("42"); len(got.exact) != 0 || len(got.buckets) != 0 {
		t.Errorf("got %v with neither a database nor Redis", got)
	}
}
