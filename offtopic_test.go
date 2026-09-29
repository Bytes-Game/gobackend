package main

import (
	"strings"
	"testing"
)

// The rules of offtopic.go that need no database.

func TestOffTopic_WhatItTakesToComeDown(t *testing.T) {
	for _, c := range []struct {
		model          string
		party, viewers int
		down           bool
		why            string
	}{
		{"no", 0, 0, false, "the model alone never takes anything down"},
		{"no", 1, 0, true, "the model and the other side agree"},
		{"no", 0, 1, true, "the model and a viewer agree"},
		{"unsure", 1, 0, false, "an unsure model is no evidence"},
		{"yes", 1, 2, false, "the other side and two viewers are not enough against a yes"},
		{"", 0, 2, false, "two viewers alone are not enough"},
		{"", 0, 3, true, "three viewers are, whatever the model said"},
		{"yes", 0, 3, true, "three viewers are, even against a yes"},
		{"", 5, 0, false, "the other side can't bring it down alone, however many of them"},
	} {
		if got := offTopicVerdict(c.model, c.party, c.viewers); got != c.down {
			t.Errorf("model %q, %d from the battle, %d viewers: down=%v — %s",
				c.model, c.party, c.viewers, got, c.why)
		}
	}
}

func TestOffTopic_WhatALostBattleCosts(t *testing.T) {
	// One side left, both rated the same: the owner loses what a loss costs
	// plus the penalty, and the other side gains what a win is worth.
	after, gains := forfeitRatings(1000, []int{1000})
	if after != 1000-16-integrityPenalty || len(gains) != 1 || gains[0] != 16 {
		t.Errorf("even battle: owner → %d, gains %v; want %d and [16]",
			after, gains, 1000-16-integrityPenalty)
	}
	// Two sides left share it, the way a tie on top does in decideBattle.
	after, gains = forfeitRatings(1000, []int{1000, 1000})
	if after != 1000-16-integrityPenalty || gains[0] != 8 || gains[1] != 8 {
		t.Errorf("two sides: owner → %d, gains %v", after, gains)
	}
	// Never below the floor.
	if after, _ := forfeitRatings(ratingFloor+3, []int{2000}); after != ratingFloor {
		t.Errorf("near the floor: %d, want %d", after, ratingFloor)
	}
	if penaltyRating(1000) != 1000-integrityPenalty || penaltyRating(ratingFloor) != ratingFloor {
		t.Error("the penalty with no battle to lose is wrong")
	}
}

func TestOffTopic_OnlyTheModelsThreeAnswersCount(t *testing.T) {
	for in, want := range map[string]string{
		"no": "no", " YES ": "yes", "unsure": "unsure", "": "", "maybe": "", "No, it does not": "",
	} {
		if got := cleanMatch(in); got != want {
			t.Errorf("cleanMatch(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestCreate_LimitsCountCharactersNotBytes(t *testing.T) {
	if msg := challengeWordsTooLong(strings.Repeat("a", maxPrefixChars), strings.Repeat("b", maxSubjectChars)); msg != "" {
		t.Errorf("text exactly at the limits was refused: %s", msg)
	}
	// An emoji is several bytes and one character — the app counts it as one.
	if msg := challengeWordsTooLong("Who can", strings.Repeat("🔥", maxSubjectChars)); msg != "" {
		t.Errorf("thirty emoji were refused: %s", msg)
	}
	// Spaces around the words are not held against anybody.
	if msg := challengeWordsTooLong("  Who can  ", "  "+strings.Repeat("b", maxSubjectChars)+"  "); msg != "" {
		t.Errorf("spaces around the words counted: %s", msg)
	}
	if challengeWordsTooLong(strings.Repeat("a", maxPrefixChars+1), "x") == "" ||
		challengeWordsTooLong("x", strings.Repeat("b", maxSubjectChars+1)) == "" {
		t.Error("one character over was let through")
	}
}

func TestOffTopic_IsWiredIn(t *testing.T) {
	src := codeOf(t, "main.go")
	if !strings.Contains(src, `"/challenges/{id}/report", authed(ReportOffTopicHandler)`) {
		t.Error("main.go no longer routes reports to ReportOffTopicHandler — nothing reaches it")
	}
}
