package main

import (
	"fmt"
	"testing"
)

func widenItem(id string) HomeFeedItem {
	return HomeFeedItem{Type: "challenge", Challenge: &Challenge{ID: id}}
}

func widenItems(prefix string, n int) []HomeFeedItem {
	out := make([]HomeFeedItem, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, widenItem(fmt.Sprintf("%s%d", prefix, i)))
	}
	return out
}

func widenIDs(items []HomeFeedItem) []string {
	out := make([]string, 0, len(items))
	for _, it := range items {
		out = append(out, getItemID(it))
	}
	return out
}

func TestWidenUntilFull_FullStrictWindowDoesNotWiden(t *testing.T) {
	// The common case must behave exactly as it did before: a strict window
	// that fills the pool returns immediately and no wider query is issued.
	windows := []string{"14 days", "60 days", "365 days"}
	var asked []string
	out := widenUntilFull(windows, 5, func(w string) []HomeFeedItem {
		asked = append(asked, w)
		return widenItems("fresh", 5)
	})
	if len(out) != 5 {
		t.Fatalf("expected 5 items, got %d", len(out))
	}
	if len(asked) != 1 || asked[0] != "14 days" {
		t.Fatalf("only the strictest window should be queried, got %v", asked)
	}
}

func TestWidenUntilFull_ShortWindowTopsUpFromOlderContent(t *testing.T) {
	// THE REGRESSION THIS FILE EXISTS FOR.
	//
	// The old ladder returned at the first window with ANY rows. A 14-day
	// window holding 3 videos capped the entire candidate pool at 3 and the
	// rest of the catalog was never fetched — not ranked low, never seen by
	// the ranker at all, so no downstream tier could rescue it.
	windows := []string{"14 days", "60 days", "365 days"}
	byWindow := map[string][]HomeFeedItem{
		"14 days":  widenItems("recent", 3),
		"60 days":  append(widenItems("recent", 3), widenItems("mid", 4)...),
		"365 days": append(append(widenItems("recent", 3), widenItems("mid", 4)...), widenItems("old", 20)...),
	}
	out := widenUntilFull(windows, 20, func(w string) []HomeFeedItem {
		return byWindow[w]
	})
	if len(out) != 20 {
		t.Fatalf("pool should have been topped up to 20, got %d: %v", len(out), widenIDs(out))
	}
	// Freshest first: the strict window's rows still lead the pool.
	got := widenIDs(out)
	for i, want := range []string{"recent0", "recent1", "recent2", "mid0"} {
		if got[i] != want {
			t.Fatalf("slot %d: want %q, got %q (full order %v)", i, want, got[i], got)
		}
	}
}

func TestWidenUntilFull_DedupsOverlapBetweenWindows(t *testing.T) {
	// A wider window is a superset of a narrower one, so the ladder re-returns
	// rows already collected. They must not appear twice.
	windows := []string{"14 days", "60 days"}
	byWindow := map[string][]HomeFeedItem{
		"14 days": widenItems("a", 2),
		"60 days": append(widenItems("a", 2), widenItems("b", 2)...),
	}
	out := widenUntilFull(windows, 50, func(w string) []HomeFeedItem {
		return byWindow[w]
	})
	want := []string{"a0", "a1", "b0", "b1"}
	got := widenIDs(out)
	if len(got) != len(want) {
		t.Fatalf("expected %v, got %v", want, got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("slot %d: want %q, got %q (full %v)", i, want[i], got[i], got)
		}
	}
}

func TestWidenUntilFull_NeverExceedsLimit(t *testing.T) {
	windows := []string{"14 days", "60 days"}
	out := widenUntilFull(windows, 3, func(w string) []HomeFeedItem {
		return widenItems("x", 10)
	})
	if len(out) != 3 {
		t.Fatalf("limit 3 should cap the pool at 3, got %d", len(out))
	}
}

func TestWidenUntilFull_EmptyLadderReturnsNil(t *testing.T) {
	out := widenUntilFull([]string{"14 days", "365 days"}, 10, func(w string) []HomeFeedItem {
		return nil
	})
	if out != nil {
		t.Fatalf("an exhausted ladder should return nil, got %v", widenIDs(out))
	}
}

func TestWidenUntilFull_ZeroLimitFetchesNothing(t *testing.T) {
	called := false
	out := widenUntilFull([]string{"14 days"}, 0, func(w string) []HomeFeedItem {
		called = true
		return widenItems("x", 5)
	})
	if called {
		t.Fatalf("limit 0 must not issue a query")
	}
	if out != nil {
		t.Fatalf("limit 0 should return nil, got %v", widenIDs(out))
	}
}

func TestWidenUntilFull_KeepsIdlessItems(t *testing.T) {
	// Items without a resolvable id cannot be deduped. Keeping them is the
	// safe direction: the alternative silently deletes them from the pool.
	out := widenUntilFull([]string{"14 days"}, 5, func(w string) []HomeFeedItem {
		return []HomeFeedItem{{Type: "suggestedAccounts"}, {Type: "suggestedAccounts"}}
	})
	if len(out) != 2 {
		t.Fatalf("id-less items should survive, got %d", len(out))
	}
}

func TestExploreScore_InteractionIsARankingSignalNotADelete(t *testing.T) {
	// Prior interaction withholds a small bonus. It must not zero the item,
	// and the un-interacted version must rank above the interacted one.
	cs := &ContentScore{
		ContentID:    "c1",
		ContentType:  "challenge",
		QualityScore: 0.8,
		ViewCount:    100,
		LikeCount:    10,
	}
	fresh, freshBreak := exploreScore(cs, nil, false)
	touched, touchedBreak := exploreScore(cs, nil, true)

	if touched <= 0 {
		t.Fatalf("an interacted item must keep a real score, got %f", touched)
	}
	if fresh <= touched {
		t.Fatalf("un-interacted should outrank interacted: fresh=%f touched=%f", fresh, touched)
	}
	if freshBreak["unseenBonus"] != exploreUnseenBonus {
		t.Fatalf("expected unseenBonus %f, got %f", exploreUnseenBonus, freshBreak["unseenBonus"])
	}
	if touchedBreak["unseenBonus"] != 0 {
		t.Fatalf("interacted item should get no unseenBonus, got %f", touchedBreak["unseenBonus"])
	}
}
