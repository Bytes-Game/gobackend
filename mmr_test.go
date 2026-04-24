package main

import "testing"

// MMR should retain the top items but prefer diverse ones when scores are close.
func TestApplyMMR_PreservesItemCount(t *testing.T) {
	items := []ScoredItem{
		{Item: HomeFeedItem{Type: "post", Post: &Post{ID: "a"}}, Score: 1.0},
		{Item: HomeFeedItem{Type: "post", Post: &Post{ID: "b"}}, Score: 0.9},
		{Item: HomeFeedItem{Type: "post", Post: &Post{ID: "c"}}, Score: 0.8},
	}
	embed := func(si ScoredItem) []float64 {
		v := make([]float64, embedDim)
		switch getItemID(si.Item) {
		case "a":
			v[0] = 1
		case "b":
			v[0] = 1 // near-duplicate of a
		case "c":
			v[1] = 1 // orthogonal
		}
		return l2norm(v)
	}
	out := applyMMR(items, 0.5, 3, embed)
	if len(out) != 3 {
		t.Fatalf("expected 3 items back, got %d", len(out))
	}
}

func TestApplyMMR_PrefersDiverseOverDuplicate(t *testing.T) {
	items := []ScoredItem{
		{Item: HomeFeedItem{Type: "post", Post: &Post{ID: "a"}}, Score: 1.0},
		{Item: HomeFeedItem{Type: "post", Post: &Post{ID: "b"}}, Score: 0.95}, // near-dup of a
		{Item: HomeFeedItem{Type: "post", Post: &Post{ID: "c"}}, Score: 0.90}, // diverse
	}
	embed := func(si ScoredItem) []float64 {
		v := make([]float64, embedDim)
		switch getItemID(si.Item) {
		case "a":
			v[0] = 1
		case "b":
			v[0] = 1 // identical to a
		case "c":
			v[1] = 1
		}
		return l2norm(v)
	}
	// Low lambda prioritises diversity over raw score.
	out := applyMMR(items, 0.2, 3, embed)
	// After 'a' is picked first, 'c' (diverse) should rank higher than 'b' (near-duplicate).
	if getItemID(out[0].Item) != "a" {
		t.Fatalf("expected 'a' first, got %q", getItemID(out[0].Item))
	}
	if getItemID(out[1].Item) != "c" {
		t.Fatalf("expected diverse 'c' second under low lambda, got %q", getItemID(out[1].Item))
	}
}

func TestApplyMMR_EmptyInput(t *testing.T) {
	out := applyMMR(nil, 0.5, 10, func(ScoredItem) []float64 { return nil })
	if len(out) != 0 {
		t.Fatalf("empty input should return empty output")
	}
}
