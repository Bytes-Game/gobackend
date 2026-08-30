package main

import (
	"fmt"
	"testing"
)

func TestFeedHasMore(t *testing.T) {
	cases := []struct {
		name                        string
		candidates, composed, limit int
		want                        bool
		why                         string
	}{
		{
			name: "short page with candidates left over",
			// The regression this exists for. Composition stopped early
			// because of maxItemsPerCreator, not because it ran out.
			candidates: 23, composed: 15, limit: 20, want: true,
			why: "8 scored candidates were never placed",
		},
		{
			name:       "short page that used everything it had",
			candidates: 12, composed: 12, limit: 20, want: false,
			why: "the pool really was 12; there is nothing to page to",
		},
		{
			name:       "full page",
			candidates: 20, composed: 20, limit: 20, want: true,
			why: "landing exactly on the page size means the bounded fetch was the constraint",
		},
		{
			name:       "full page with more behind it",
			candidates: 60, composed: 20, limit: 20, want: true,
		},
		{
			name: "empty page",
			// Must stop even though 5 candidates were scored: a client that
			// only stops on an empty result would spin here forever.
			candidates: 5, composed: 0, limit: 20, want: false,
			why: "nothing was served, so there is no next page to ask for",
		},
		{
			name:       "empty page from an empty pool",
			candidates: 0, composed: 0, limit: 20, want: false,
		},
		{
			name: "over-full page",
			// composeFeed is bounded by the pattern, but nothing downstream
			// should depend on that.
			candidates: 40, composed: 25, limit: 20, want: true,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// These cases predate repeat-marking, when every served item was
			// by definition fresh. Passing composed as fresh keeps them
			// asserting exactly what they always did.
			got := feedHasMore(c.candidates, c.composed, c.limit)
			if got != c.want {
				msg := fmt.Sprintf(
					"feedHasMore(candidates=%d, composed=%d, fresh=%d, limit=%d) = %v, want %v",
					c.candidates, c.composed, c.composed, c.limit, got, c.want)
				if c.why != "" {
					msg += " — " + c.why
				}
				t.Error(msg)
			}
		})
	}
}

// The shape of the live catalog that exposed this, driven through the real
// composeFeed rather than through arithmetic — the per-creator cap is the
// mechanism under test and asserting it second-hand would not have caught
// the original bug either.
//
// 28 challenges from 6 creators, served to one of those 6. Own content is
// filtered upstream, so composition sees 23 items from 5 creators, and
// maxItemsPerCreator caps the page at 15. The client asked for 20.
// seedNoEmotions pre-answers the emotion-tag lookup for a challenge id so
// composeFeed's slot classification never falls through to the database.
func seedNoEmotions(challengeID string) {
	if rdb == nil {
		return
	}
	rdb.Set(rctx, contentEmotionRedisKey+"challenge:"+challengeID, "[]", 0)
}

func TestForYouPageIsNotDeclaredFinalWhenTheCreatorCapTruncatedIt(t *testing.T) {
	perCreator := map[string]int{
		// player1 is the viewer; their 5 are excluded before this point.
		"player2": 5, "maya": 5, "deven": 5, "omar": 4, "nina": 4,
	}

	scored := []ScoredItem{}
	id := 0
	for creator, n := range perCreator {
		for i := 0; i < n; i++ {
			id++
			// composeFeed's emotion-tag slotting falls back to
			// getContentEmotions when the struct carries no tags, and that
			// reads the DB — nil in tests. Seeding the Redis cache miniredis
			// provides keeps the lookup off the DB path.
			seedNoEmotions(fmt.Sprintf("%d", 100+id))
			scored = append(scored, ScoredItem{
				Item: HomeFeedItem{
					Type: "challenge",
					Challenge: &Challenge{
						ID:        fmt.Sprintf("%d", 100+id),
						CreatorID: creator,
					},
				},
				Score:          1.0 - float64(id)/1000,
				ScoreBreakdown: map[string]float64{},
			})
		}
	}
	if len(scored) != 23 {
		t.Fatalf("fixture built %d candidates, want 23", len(scored))
	}

	const limit = 20
	pattern := make([]string, limit)
	for i := range pattern {
		pattern[i] = slotHook
	}
	composed, held := composeFeed(scored, pattern, map[string]bool{})

	wantComposed := len(perCreator) * maxItemsPerCreator
	if len(composed) != wantComposed {
		t.Fatalf("composed %d items, want %d (%d creators × %d cap) — the "+
			"fixture no longer reproduces the reported shape",
			len(composed), wantComposed, len(perCreator), maxItemsPerCreator)
	}
	if len(composed) >= limit {
		t.Fatalf("composed %d of a %d-item page; the whole point is that "+
			"this page cannot fill", len(composed), limit)
	}

	// The mixed feed already gets this right, because feedHasMore is handed
	// the whole scored pool and can see that composition left items on the
	// table. A single-kind tab is not — it never sees that number — so
	// composeFeed reports what it held back and the tab uses that instead.
	if held <= 0 {
		t.Errorf("composition held nothing back, with %d of %d candidates "+
			"unplaced. The tab has no other way to tell a page shortened by "+
			"the creator cap from a catalogue that ran out.",
			len(scored)-len(composed), len(scored))
	}
	if held != len(scored)-len(composed) {
		t.Errorf("held %d but %d candidates went unplaced; every one of them "+
			"was skipped for the creator cap in this fixture", held,
			len(scored)-len(composed))
	}

	if !feedHasMore(len(scored), len(composed), limit) {
		t.Errorf("hasMore = false with %d of %d candidates unplaced. The "+
			"page is short because %d creators × %d is %d, not because the "+
			"catalog ran out — and since that ceiling is structural, "+
			"reporting it as the end makes page 2 unreachable for the whole "+
			"session no matter how much content exists",
			len(scored)-len(composed), len(scored),
			len(perCreator), maxItemsPerCreator, wantComposed)
	}
}

// The rule this function gained when repeats started being marked: a page made
// entirely of things the viewer has already seen is the end of what we have to
// give right now, and saying otherwise sends the client after a page that
// cannot exist.
//
// A device run walked six pages before this existed. Pages four, five and six
// yielded 7, 16 and 0 new items out of 21 sent, and every one of them reported
// "keep going" — because a full page of repeats is indistinguishable from a
// full page of new videos unless somebody counts.
func TestFeedHasMore_KeepsGoingWhenEverythingIsARepeat(t *testing.T) {
	// The inverse of what this file used to assert.
	//
	// The old rule ended the feed the moment a page contained nothing unseen.
	// On a 108-video catalogue a viewer reaches that in one sitting, and the
	// app showed "you have reached the end" while the whole library sat there
	// ready to be shown again.
	//
	// Repeats are not an empty catalogue. The seen record is a timestamp that
	// is restamped every time an item is served, and the penalty is largest
	// when that timestamp is newest — so serving a page is exactly what pushes
	// it to the back, and the next page comes from whatever has been waiting
	// longest. The feed walks the catalogue in least-recently-seen order
	// instead of stopping at the end of it.
	//
	// The only honest terminator left is a page with nothing on it.
	cases := []struct {
		name                        string
		candidates, composed, limit int
		want                        bool
		why                         string
	}{
		{
			name:       "nothing was served",
			candidates: 60, composed: 0, limit: 20, want: false,
			why: "an empty page is the one real ending; claiming otherwise " +
				"spins a client that stops only on an empty result",
		},
		{
			name:       "full page, every item already seen",
			candidates: 60, composed: 20, limit: 20, want: true,
			why: "this is the case that used to end the feed. A full page of " +
				"repeats means the catalogue is being cycled, not that it is gone",
		},
		{
			name:       "short page with candidates left over",
			candidates: 60, composed: 8, limit: 20, want: true,
			why: "the per-creator cap deferred those items; the next page has " +
				"a fresh budget and can reach them",
		},
		{
			name:       "short page, pool was the constraint",
			candidates: 8, composed: 8, limit: 20, want: false,
			why: "everything scored was used and it did not fill a page, so " +
				"the pool really is all there was",
		},
		{
			name:       "exactly a full page, pool exhausted",
			candidates: 20, composed: 20, limit: 20, want: true,
			why: "landing exactly on the page size says the fetch was the " +
				"constraint, not the catalogue",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := feedHasMore(c.candidates, c.composed, c.limit)
			if got != c.want {
				msg := fmt.Sprintf(
					"feedHasMore(candidates=%d, composed=%d, limit=%d) = %v, want %v",
					c.candidates, c.composed, c.limit, got, c.want)
				if c.why != "" {
					msg += " — " + c.why
				}
				t.Error(msg)
			}
		})
	}
}
