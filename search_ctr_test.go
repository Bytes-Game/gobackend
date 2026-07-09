package main

// Tests for the search click-through learner (search_ctr.go).

import (
	"testing"
)

func TestNormalizeSearchQuery(t *testing.T) {
	cases := map[string]string{
		"  Dance  ":      "dance",
		"DANCE  BATTLE":  "dance battle",
		"dance\tbattle":  "dance battle",
		"":               "",
		"   ":            "",
	}
	for in, want := range cases {
		if got := normalizeSearchQuery(in); got != want {
			t.Errorf("normalizeSearchQuery(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestSearchCTRResultKey(t *testing.T) {
	cases := [][3]string{
		{"challenge", "42", "challenge:42"},
		{"battle", "42", "challenge:42"},
		{"account", "7", "user:7"},
		{"users", "7", "user:7"},
		{"user", "7", "user:7"},
	}
	for _, c := range cases {
		if got := searchCTRResultKey(c[0], c[1]); got != c[2] {
			t.Errorf("searchCTRResultKey(%q,%q) = %q, want %q", c[0], c[1], got, c[2])
		}
	}
}

// TestSearchCTRRoundtrip: impressions + clicks in, boost out — and the
// position debias makes a rank-9 click teach more than a rank-1 click.
func TestSearchCTRRoundtrip(t *testing.T) {
	resetRedis(t)

	// 10 impressions for two results under the same query.
	for range [10]struct{}{} {
		searchLogImpressions("Dance ", []string{"challenge:1", "challenge:2"})
	}

	// Result 1 clicked at position 0 (top), result 2 at position 8 —
	// same click count, deeper position ⇒ more click mass.
	clickAt := func(id string, pos float64) {
		searchObserveClickFromEvent(FeedEvent{
			ContentID:   id,
			ContentType: "challenge",
			Metadata:    map[string]interface{}{"query": "dance", "position": pos},
		})
	}
	clickAt("1", 0)
	clickAt("2", 8)

	boosts := searchCTRBoosts("DANCE") // any casing resolves to the same hash
	b1, b2 := boosts["challenge:1"], boosts["challenge:2"]
	if b1 <= 0 || b2 <= 0 {
		t.Fatalf("both clicked results should carry a boost, got %v / %v", b1, b2)
	}
	if b2 <= b1 {
		t.Fatalf("deep-position click must outweigh top-position click: pos8=%v pos0=%v", b2, b1)
	}
	if b2 > searchCTRBoostWeight {
		t.Fatalf("boost must stay within weight cap, got %v", b2)
	}

	// A result with impressions but no clicks gets nothing.
	if _, ok := boosts["challenge:99"]; ok {
		t.Fatal("unclicked result must not appear in boosts")
	}
}

// TestSearchCTRMinImpressions: below the impression floor, no boost —
// one lucky click on a rarely-shown result shouldn't move rankings.
func TestSearchCTRMinImpressions(t *testing.T) {
	resetRedis(t)
	searchLogImpressions("rare", []string{"challenge:5"}) // 1 impression < floor
	searchObserveClickFromEvent(FeedEvent{
		ContentID: "5", ContentType: "challenge",
		Metadata: map[string]interface{}{"query": "rare", "position": 0.0},
	})
	if b := searchCTRBoosts("rare"); len(b) != 0 {
		t.Fatalf("below-floor result must not get a boost, got %v", b)
	}
}

func TestSearchIntent(t *testing.T) {
	// Username-shaped + an account hit → user intent.
	if got := searchIntent("player_1", true); got != "user" {
		t.Errorf("username-shaped with account hit = %q, want user", got)
	}
	// Username-shaped but NO account hit → falls through (word, not a person).
	if got := searchIntent("zzqxv_9", false); got == "user" {
		t.Error("username-shaped with no account hit must not classify as user")
	}
	// Multi-word queries are never usernames.
	if got := searchIntent("dance battle", true); got == "user" {
		t.Error("multi-word query must not classify as user")
	}
	// Empty → general.
	if got := searchIntent("  ", false); got != "general" {
		t.Errorf("empty query = %q, want general", got)
	}
}
