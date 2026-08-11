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
			why:        "the pool really was 12; there is nothing to page to",
		},
		{
			name:       "full page",
			candidates: 20, composed: 20, limit: 20, want: true,
			why:        "landing exactly on the page size means the bounded fetch was the constraint",
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
	composed := composeFeed(scored, pattern, map[string]bool{})

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
