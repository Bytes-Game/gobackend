package main

import "testing"

// ════════════════════════════════════════════════════════════════════════════
// NOBODY IS SHOWN THEIR OWN UPLOAD
// ════════════════════════════════════════════════════════════════════════════
//
// A creator reported their own video appearing in their own Shorts tab, over
// and over. The rule was there — nine times, once per retriever — and one of
// them had been forgotten: sourceSearchAffinity builds candidates out of
// Meilisearch hits rather than SQL, so it never had the predicate the SQL
// lanes carry. Two follow-graph queries have no predicate either.
//
// The check now lives where every lane's output meets, so these test that one
// place rather than nine.

func ownItem(id, creator string) HomeFeedItem {
	return HomeFeedItem{Type: "challenge", Challenge: &Challenge{ID: id, CreatorID: creator}}
}

func TestIsOwnContent_DropsYourOwnChallenge(t *testing.T) {
	if !isOwnContent(ownItem("1", "player1"), "player1") {
		t.Error("a creator's own challenge was not recognised as theirs, so it " +
			"would be served back to them in their own feed")
	}
	if isOwnContent(ownItem("1", "player2"), "player1") {
		t.Error("somebody else's challenge was treated as the viewer's own")
	}
}

func TestIsOwnContent_KeepsABattleYouAnswered(t *testing.T) {
	// The case that must NOT be dropped. The challenge belongs to whoever set
	// it; you are the opponent behind the flip. Dropping these would hide half
	// the battles from everybody who takes part in them.
	battle := HomeFeedItem{Type: "challenge", Challenge: &Challenge{
		ID:                  "7",
		CreatorID:           "player2",
		TopResponseVideoUrl: "https://x/mine.mp4", // the viewer's own response
	}}
	if isOwnContent(battle, "player1") {
		t.Error("a battle the viewer answered was dropped as their own content")
	}
}

func TestIsOwnContent_AnonymousViewerLosesNothing(t *testing.T) {
	// An empty viewer id must never match a creator id, or a signed-out or
	// mis-parsed request would come back with an empty feed rather than a
	// normal one.
	if isOwnContent(ownItem("1", ""), "") {
		t.Error("an empty viewer id matched an empty creator id and would " +
			"empty the whole feed")
	}
	if isOwnContent(ownItem("1", "player1"), "") {
		t.Error("an empty viewer id matched a real creator")
	}
}

func TestIsOwnContent_HandlesAnItemWithNoContent(t *testing.T) {
	// Suggested-account cards carry neither a challenge nor a post. They are
	// furniture and belong on every feed.
	if isOwnContent(HomeFeedItem{Type: "suggestedAccounts"}, "player1") {
		t.Error("a suggested-accounts card was dropped as the viewer's own content")
	}
}
