package main

import (
	"os"
	"strings"
	"testing"
)

// ════════════════════════════════════════════════════════════════════════════
// THE SEEN-SET MUST ONLY EVER RECORD WHAT REACHED THE PHONE
// ════════════════════════════════════════════════════════════════════════════
//
// This is a source-order test, which is unusual, and it is here because the
// bug it guards is invisible to the compiler and to every ordinary test. Both
// orderings compile. Both return a correct-looking page. The difference only
// shows up weeks later as a feed that has quietly gone wrong for reasons
// nobody can trace.
//
// What went wrong: opening the Battles tab fetched a mixed pool, wrote the
// WHOLE pool into the viewer's watched history, and then discarded the shorts.
// So roughly thirty shorts nobody had seen were recorded as watched every time
// somebody tapped the tab. They then sank in For You as "already watched", and
// the taste profile learned from videos that were never on screen.
//
// The filter was moved to the server specifically to prevent that. Putting it
// after the recording undid the entire point of the decision.
//
// So: in every handler that records impressions, the kind filter must appear
// BEFORE markShownBatch. If someone reorders them, this fails and says why.

type seenOrderCase struct {
	file    string
	handler string
}

func TestSeenSetRecordsOnlyWhatWasServed(t *testing.T) {
	for _, c := range []seenOrderCase{
		{"feed_engine.go", "SmartFeedHandler (For You)"},
		{"explore_feed.go", "ExploreFeedHandler"},
	} {
		src, err := os.ReadFile(c.file)
		if err != nil {
			t.Fatalf("%s: %v", c.file, err)
		}
		text := string(src)

		mark := strings.Index(text, "markShownBatch(userID, items)")
		if mark < 0 {
			t.Fatalf("%s: no markShownBatch call found. If impressions moved "+
				"somewhere else, this test has to move with them — do not "+
				"just delete it", c.file)
		}
		filter := indexOfAny(text, "filterFeedKindScored(composed, kindFilter)",
			"filterFeedKind(items, kindFilter)")
		if filter < 0 {
			t.Fatalf("%s: no kind filter found", c.file)
		}

		if filter > mark {
			t.Errorf(`%s — %s: the tab filter runs AFTER the seen-set write.

That records videos the viewer never saw as watched. On a Battles page it
is roughly thirty shorts per tap: they sink in For You as "already watched"
and the taste profile learns from content that was never on screen.

Move the filter above markShownBatch. The seen-set is a claim about what
reached the phone, so it has to be written from the final page.`,
				c.file, c.handler)
		}
	}
}

// The other half of the same rule, and the one a reader is most likely to get
// backwards: battle-ness is read from TopResponseVideoUrl, and
// finalizeFeedItems is what fills that in. Filter first and every item looks
// like a short, so the Battles tab returns nothing at all and the Shorts tab
// returns everything.
func TestEnrichmentRunsBeforeTheFilter(t *testing.T) {
	for _, c := range []seenOrderCase{
		{"feed_engine.go", "SmartFeedHandler (For You)"},
		{"explore_feed.go", "ExploreFeedHandler"},
	} {
		src, err := os.ReadFile(c.file)
		if err != nil {
			t.Fatalf("%s: %v", c.file, err)
		}
		text := string(src)
		fin := indexOfAny(text, "finalizeFeedItemsScored(composed)", "finalizeFeedItems(items)")
		filter := indexOfAny(text, "filterFeedKindScored(composed, kindFilter)",
			"filterFeedKind(items, kindFilter)")
		if fin < 0 || filter < 0 {
			t.Fatalf("%s: could not find both the finalizer and the filter", c.file)
		}
		if fin > filter {
			t.Errorf("%s — %s: the kind filter runs before finalizeFeedItems. "+
				"Battle-ness comes from TopResponseVideoUrl, which the "+
				"finalizer fills in, so every item would read as a short: the "+
				"Battles tab returns nothing and the Shorts tab returns "+
				"everything.", c.file, c.handler)
		}
	}
}

// One history, not one per tab.
//
// A battle watched in For You is watched when the Battles tab opens, and a
// short watched in the Shorts tab is watched in For You. That is what makes
// tabs feel like views onto one feed instead of separate apps, and it is what
// the viewer expects: they have seen it, so it should behave as seen
// everywhere.
//
// It holds because the seen-set is keyed by user alone. If a kind, tab or
// mode ever appears in that key, this fails.
func TestSeenSetIsSharedAcrossEveryTab(t *testing.T) {
	src, err := os.ReadFile("feed_engine.go")
	if err != nil {
		t.Fatal(err)
	}
	text := string(src)
	for _, call := range []string{
		"markShownBatch(userID, items)",
		"loadSeenSet(userID)",
	} {
		if !strings.Contains(text, call) {
			t.Errorf("expected the seen-set to be addressed by user alone via "+
				"%q. If it is now keyed by tab as well, every tab has its own "+
				"history and a video watched in For You will come back as new "+
				"in Shorts.", call)
		}
	}
}

func indexOfAny(text string, needles ...string) int {
	for _, n := range needles {
		if i := strings.Index(text, n); i >= 0 {
			return i
		}
	}
	return -1
}

// ════════════════════════════════════════════════════════════════════════════
// THE LONG MEMORY MUST BE THE RECENT PAST, NOT A LOTTERY
// ════════════════════════════════════════════════════════════════════════════
//
// There are two memories. The seen-set is the fast one: twelve hours, with a
// penalty that decays to nothing. buildInteractedSet is the slow one, and
// what it feeds — the unseen bonus — is the ONLY thing standing between a
// viewer and a video they watched yesterday once that penalty has gone.
//
// It used to be `SELECT DISTINCT … LIMIT 1000` with no ordering. SQL promises
// nothing about which rows an unordered LIMIT returns. Against a real
// Postgres, with events appended over time the way they actually are, that
// query forgot a video watched ONE MINUTE ago and remembered 332 of the most
// recent 1000. Everything it forgot scored as never-watched.
//
// This test reads the query rather than running it, because the failure needs
// a database with more history than the cap to show up at all — and by then
// it is live and looks like "the feed keeps repeating itself".

func TestInteractedMemory_IsOrderedAndBounded(t *testing.T) {
	src, err := os.ReadFile("feed_engine.go")
	if err != nil {
		t.Fatal(err)
	}
	text := string(src)
	start := strings.Index(text, "func buildInteractedSet")
	if start < 0 {
		t.Fatal("buildInteractedSet is gone; if the long-term memory moved, " +
			"this test has to move with it rather than be deleted")
	}
	body := text[start:]
	if end := strings.Index(body, "\nfunc "); end > 0 {
		body = body[:end]
	}

	if !strings.Contains(body, "ORDER BY created_at DESC") {
		t.Error(`buildInteractedSet has a LIMIT with no ORDER BY.

An unordered LIMIT returns arbitrary rows — Postgres need not even answer the
same way twice. Everything outside that arbitrary slice reads as never
watched and collects the unseen bonus, so videos the viewer finished
yesterday come back today ranked above content they have never seen.

Measured on a real 3000-item history: a video watched one minute ago was
forgotten, and only 332 of the most recent 1000 survived.`)
	}

	if !strings.Contains(body, "LIMIT $3") && !strings.Contains(body, "LIMIT $2") {
		t.Error("buildInteractedSet no longer bounds how much it reads. This " +
			"runs on every feed request, so an unbounded read gets slower for " +
			"every viewer, forever, as their history grows.")
	}

	if !strings.Contains(body, "created_at >") {
		t.Error("buildInteractedSet reads without a time window. The window is " +
			"what keeps the cost flat as a history grows, and it is also the " +
			"honest answer to how long this app remembers a view.")
	}
}

func TestInteractedMemory_HoldsMoreThanASession(t *testing.T) {
	// A single real session started 130 videos. A memory of a thousand events
	// is about a week, after which a regular viewer stops being remembered
	// properly at all — which is the shape of the original complaint.
	if interactedMemoryEvents < 5000 {
		t.Errorf("the long memory holds %d events, roughly %d sessions at the "+
			"130-videos-a-session seen in a real log. Too short and watched "+
			"content starts scoring as new again.",
			interactedMemoryEvents, interactedMemoryEvents/130)
	}
	if interactedMemoryDays < 30 {
		t.Errorf("the memory reaches back %d days; below about a month, "+
			"content a viewer watched recently starts reading as new",
			interactedMemoryDays)
	}
}
