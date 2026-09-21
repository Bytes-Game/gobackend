package main

// The feed that forgot what it had just shown.
//
// ════════════════════════════════════════════════════════════════════════════
// WHAT A DEVICE LOG CAUGHT
// ════════════════════════════════════════════════════════════════════════════
//
// Two lines, from one session on a real phone:
//
//	feed forYou page 1: 17 items  new=17 repeat=0 againThisRun=0
//	feed forYou page 1: 17 items  new=17 repeat=0 againThisRun=18
//
// The same seventeen videos, served twice, and the server called every one of
// them NEW both times. The app knew better — it had counted eighteen it had
// already put on screen.
//
// The cause was one missing call. SmartFeedHandler's cold-start branch built
// a page, encoded it, and returned — straight past the markShownBatch that
// the warm path runs further down. Nothing a new account saw was ever
// recorded.
//
// It landed on the worst possible cohort. Cold start IS the brand-new
// account. Its paging is keyed to the page number, so page 1 is the same top
// videos every time it is asked for; with nothing recorded there was nothing
// to rank them down, so pulling to refresh changed nothing whatsoever. That
// is the first thing somebody does when a feed looks stuck.
//
// These tests hold the recording in place and, separately, hold the two
// halves of the honesty contract: a repeat sinks, and a repeat says so.

import (
	"strings"
	"testing"
	"time"
)

func TestSinkSeenItems_SaysWhichOnesAreRepeats(t *testing.T) {
	// Sinking without labelling leaves the client unable to tell a deliberate
	// re-serve from a bug — which is the whole reason Challenge.Repeat exists.
	// Every unscored feed used to sink silently.
	resetRedis(t)
	u := "urepeatlabel"
	seedSeen(t, u, "challenge", "watched", 1*time.Minute)

	items := []HomeFeedItem{
		{Type: "challenge", Challenge: &Challenge{ID: "watched"}},
		{Type: "challenge", Challenge: &Challenge{ID: "fresh"}},
	}
	out := sinkSeenItems(items, loadSeenSet(u))

	if len(out) != 2 {
		t.Fatalf("nothing may be dropped: got %d", len(out))
	}
	if got := getItemID(out[0]); got != "fresh" {
		t.Errorf("unseen must lead; slot 0 is %q", got)
	}
	if !out[1].Challenge.Repeat {
		t.Error("the watched item was moved down the page but not labelled " +
			"a repeat, so the app is still told it is new")
	}
	// And the presence half: a genuinely fresh item must NOT be labelled, or
	// this could be satisfied by marking everything.
	if out[0].Challenge.Repeat {
		t.Error("a video the viewer has never seen was labelled a repeat")
	}
}

func TestSinkSeenItems_LabelsNothingWhenNothingWasWatched(t *testing.T) {
	// The early-return path. It skips the labelling loop entirely, so it
	// needs its own check.
	resetRedis(t)
	items := []HomeFeedItem{
		{Type: "challenge", Challenge: &Challenge{ID: "a"}},
		{Type: "challenge", Challenge: &Challenge{ID: "b"}},
	}
	out := sinkSeenItems(items, loadSeenSet("unevermind"))
	for i, it := range out {
		if it.Challenge.Repeat {
			t.Errorf("slot %d labelled a repeat for a viewer with no history", i)
		}
	}
}

// TestColdStartPath_RecordsWhatItServed is the wire check.
//
// It reads the source, with comments stripped, because the branch is not
// reachable from a test without a full HTTP request, a profile that counts as
// cold, and a seeded catalogue — and the failure being guarded against is the
// call quietly going missing again, which a test that called markShownBatch
// by hand would never notice. That is the most common bug in this repo.
func TestColdStartPath_RecordsWhatItServed(t *testing.T) {
	src := codeWithoutComments(readSourceFile(t, "feed_engine.go"))

	start := indexOfOrFail(t, src, "if isColdStartUser(profile) {")
	// The branch ends at its own return, just before the warm path resumes.
	end := indexOfOrFail(t, src[start:], `"coldStart": true`) + start
	branch := src[start:end]

	if !strings.Contains(branch, "markShownBatch(userID, items)") {
		t.Error("the cold-start branch serves a page without recording it. " +
			"Every brand-new account is served by this branch, its paging is " +
			"keyed to the page number, and with nothing recorded page 1 is " +
			"identical every time it is asked for — pull-to-refresh does " +
			"nothing at all.")
	}
	if !strings.Contains(branch, "sinkSeenItems(") {
		t.Error("the cold-start branch no longer sinks what the viewer has " +
			"already watched, so repeats come back at the TOP of the page " +
			"rather than behind anything fresh")
	}
}

func indexOfOrFail(t *testing.T, haystack, needle string) int {
	t.Helper()
	i := strings.Index(haystack, needle)
	if i < 0 {
		t.Fatalf("could not find %q — this test reads the source and the "+
			"shape it reads has changed", needle)
	}
	return i
}
