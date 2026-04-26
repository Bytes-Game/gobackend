package main

import (
	"testing"
)

func TestSeenFilter_EmptyUserIsNoop(t *testing.T) {
	items := []HomeFeedItem{{Type: "post", Post: &Post{ID: "p1"}}}
	out := filterUnseen("", items)
	if len(out) != len(items) {
		t.Fatalf("empty userID should return items as-is, got %d", len(out))
	}
}

func TestSeenFilter_DropsAlreadySeen(t *testing.T) {
	resetRedis(t)
	u := "useen1"
	items := []HomeFeedItem{
		{Type: "post", Post: &Post{ID: "pA"}},
		{Type: "post", Post: &Post{ID: "pB"}},
		{Type: "post", Post: &Post{ID: "pC"}},
	}
	// Mark pA and pC as seen.
	markShown(u, "post", "pA")
	markShown(u, "post", "pC")

	out := filterUnseen(u, items)
	if len(out) != 1 {
		t.Fatalf("expected 1 unseen item, got %d", len(out))
	}
	if getItemID(out[0]) != "pB" {
		t.Fatalf("expected pB to survive, got %q", getItemID(out[0]))
	}
}

func TestSeenFilter_ScoredVariant_LargeCatalog_StrictDrop(t *testing.T) {
	// With ≥ seenFilterMinKeep unseen items, the strict path runs: every
	// seen item is dropped and only fresh content is returned.
	resetRedis(t)
	u := "useen2"
	items := make([]ScoredItem, 0, seenFilterMinKeep+2)
	for i := 0; i < seenFilterMinKeep+2; i++ {
		items = append(items, ScoredItem{
			Item:  HomeFeedItem{Type: "post", Post: &Post{ID: "p" + string(rune('A'+i))}},
			Score: float64(seenFilterMinKeep + 2 - i),
		})
	}
	// Mark the first item as seen.
	markShown(u, "post", "pA")
	out := filterUnseenScored(u, items)
	// Strict drop: 1 fewer item than input.
	if len(out) != len(items)-1 {
		t.Fatalf("expected %d unseen items, got %d", len(items)-1, len(out))
	}
	// pA must not appear.
	for _, si := range out {
		if getItemID(si.Item) == "pA" {
			t.Fatalf("seen item pA leaked into result")
		}
	}
}

func TestSeenFilter_ScoredVariant_SmallCatalog_GracefulFallback(t *testing.T) {
	// With < seenFilterMinKeep unseen items left, the graceful fallback
	// re-admits the best-scored seen items so the page isn't empty.
	resetRedis(t)
	u := "useen2b"
	items := []ScoredItem{
		{Item: HomeFeedItem{Type: "post", Post: &Post{ID: "p1"}}, Score: 5},
		{Item: HomeFeedItem{Type: "post", Post: &Post{ID: "p2"}}, Score: 4},
	}
	markShown(u, "post", "p1")
	out := filterUnseenScored(u, items)
	// Only 1 unseen + 1 seen total = below floor → both should be returned.
	if len(out) != 2 {
		t.Fatalf("graceful fallback should keep both items, got %d", len(out))
	}
	// Unseen item must come FIRST (preserves freshness preference).
	if getItemID(out[0].Item) != "p2" {
		t.Fatalf("unseen item p2 should lead, got %q", getItemID(out[0].Item))
	}
}

func TestSeenFilter_BatchMark(t *testing.T) {
	resetRedis(t)
	u := "useen3"
	items := []HomeFeedItem{
		{Type: "post", Post: &Post{ID: "b1"}},
		{Type: "post", Post: &Post{ID: "b2"}},
	}
	markShownBatch(u, items)
	seen := loadSeenSet(u)
	if !seen[seenMember("post", "b1")] || !seen[seenMember("post", "b2")] {
		t.Fatalf("batch mark should have recorded both items, got %v", seen)
	}
}
