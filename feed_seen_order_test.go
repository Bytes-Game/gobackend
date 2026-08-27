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
// THE TAB MUST BE NARROWED BEFORE THE EXPENSIVE PART, NOT AFTER
// ════════════════════════════════════════════════════════════════════════════
//
// Another source-order test, for the same reason as the ones above: both
// orderings compile, both return a correct-looking page, and the difference
// only shows up as a tab that quietly offers a quarter of what it has.
//
// What went wrong before: the Battles tab scored 250 candidates to serve 9.
// The waste was the lesser half. composeFeed will not take more than
// maxItemsPerCreator from one creator and it counted that against the MIXED
// page — so a creator whose three slots went to shorts contributed nothing to
// the tab, and their other battles were never looked at.
//
// narrowCandidatesToKind has to run before warmContentAggregates, because that
// is the batch load feeding the scoring loop. Once it is below that line, the
// pool being scored is the mixed one again and the ceiling is back.
func TestTabIsNarrowedBeforeScoring(t *testing.T) {
	// Each handler has its own first-expensive-thing, so each names the line
	// the narrowing has to stay above.
	for _, c := range []struct {
		file, handler, mustPrecede string
	}{
		// The batch aggregate load that feeds the scoring loop.
		{"feed_engine.go", "SmartFeedHandler (For You)", "warmContentAggregates(candidates)"},
		// Explore scores with exploreScore and has no batch warm; the loop
		// itself is where the cost starts.
		{"explore_feed.go", "ExploreFeedHandler", "scored := make([]ScoredItem, 0, len(candidates))"},
	} {
		src, err := os.ReadFile(c.file)
		if err != nil {
			t.Fatalf("%s: %v", c.file, err)
		}
		text := string(src)

		narrow := strings.Index(text, "narrowCandidatesToKind(candidates, kindFilter)")
		if narrow < 0 {
			t.Errorf(`%s — %s: the candidate pool is not narrowed to the tab at all.

Every short is then scored so a Battles page can throw it away, and the
per-creator diversity cap is spent on items the viewer will never see —
which is what held the tab to about a quarter of the battles it had.`,
				c.file, c.handler)
			continue
		}
		spend := strings.Index(text, c.mustPrecede)
		if spend < 0 {
			t.Fatalf("%s: could not find %q. If the scoring path moved, this "+
				"test has to move with it — do not just delete it",
				c.file, c.mustPrecede)
		}
		if narrow > spend {
			t.Errorf(`%s — %s: the tab is narrowed AFTER %s.

That is where the cost starts, so everything the tab is about to discard
gets paid for anyway, and composeFeed still spends its per-creator budget
on items the viewer will never see. Move narrowCandidatesToKind above it.`,
				c.file, c.handler, c.mustPrecede)
		}
	}
}
