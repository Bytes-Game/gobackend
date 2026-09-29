package main

import (
	"strings"
	"testing"
)

// The rules of offtopic.go that need no database.

func TestOffTopic_WhatTheEvidenceAddsUpTo(t *testing.T) {
	const (
		nothing  = offTopicNothing
		penalise = offTopicPenalise
		down     = offTopicTakeDown
	)
	for _, c := range []struct {
		model          string
		party, viewers int
		want           offTopicAction
		why            string
	}{
		{"no", 0, 0, nothing, "the model alone never does anything"},
		{"no", 1, 0, down, "the model and the other side agree"},
		{"no", 0, 1, down, "the model and a viewer agree"},
		{"unsure", 1, 0, nothing, "an unsure model is no evidence"},
		{"yes", 1, 2, nothing, "the other side and two viewers are not enough against a yes"},
		{"", 0, 2, nothing, "two viewers alone are not enough"},
		{"", 0, 3, penalise, "three viewers charge the owner — and leave the video up"},
		{"yes", 0, 3, penalise, "even against a yes, but still only a charge"},
		{"", 5, 0, nothing, "the other side can't do anything alone, however many of them"},
	} {
		if got := offTopicVerdict(c.model, c.party, c.viewers); got != c.want {
			t.Errorf("model %q, %d from the battle, %d viewers: %v, want %v — %s",
				c.model, c.party, c.viewers, got, c.want, c.why)
		}
	}
}

func TestOffTopic_WhatALostBattleCosts(t *testing.T) {
	// One side left, both rated the same: the owner loses what a loss costs
	// plus the penalty, and the other side gains what a win is worth.
	after, gains := forfeitRatings(1000, []int{1000}, integrityPenalty)
	if after != 1000-16-integrityPenalty || len(gains) != 1 || gains[0] != 16 {
		t.Errorf("even battle: owner → %d, gains %v; want %d and [16]",
			after, gains, 1000-16-integrityPenalty)
	}
	// Two sides left share it, the way a tie on top does in decideBattle.
	after, gains = forfeitRatings(1000, []int{1000, 1000}, integrityPenalty)
	if after != 1000-16-integrityPenalty || gains[0] != 8 || gains[1] != 8 {
		t.Errorf("two sides: owner → %d, gains %v", after, gains)
	}
	// Never below the floor.
	if after, _ := forfeitRatings(ratingFloor+3, []int{2000}, integrityPenalty); after != ratingFloor {
		t.Errorf("near the floor: %d, want %d", after, ratingFloor)
	}
	if penaltyRating(1000, integrityPenalty) != 1000-integrityPenalty ||
		penaltyRating(ratingFloor, integrityPenalty) != ratingFloor {
		t.Error("the penalty with no battle to lose is wrong")
	}
	// Already charged for reports: losing costs the battle only.
	if after, _ := forfeitRatings(1000, []int{1000}, 0); after != 1000-16 {
		t.Errorf("already charged: owner → %d, want %d", after, 1000-16)
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
