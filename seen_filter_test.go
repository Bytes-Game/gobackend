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

func TestSeenFilter_ScoredVariant(t *testing.T) {
	resetRedis(t)
	u := "useen2"
	items := []ScoredItem{
		{Item: HomeFeedItem{Type: "post", Post: &Post{ID: "p1"}}, Score: 1},
		{Item: HomeFeedItem{Type: "post", Post: &Post{ID: "p2"}}, Score: 2},
	}
	markShown(u, "post", "p1")
	out := filterUnseenScored(u, items)
	if len(out) != 1 {
		t.Fatalf("expected 1 unseen scored item, got %d", len(out))
	}
	if getItemID(out[0].Item) != "p2" {
		t.Fatalf("expected p2 to survive")
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
