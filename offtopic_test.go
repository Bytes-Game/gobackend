package main

import (
	"strings"
	"testing"
)

// The rules of offtopic.go that need no database.

func TestOffTopic_WhatTheEvidenceAddsUpTo(t *testing.T) {
	for _, c := range []struct {
		model          string
		party, viewers int
		want           int
		why            string
	}{
		{"no", 0, 0, levelNone, "the model alone never charges anybody"},
		{"no", 1, 0, levelConfirmed, "the model and the other side agree: the higher charge"},
		{"no", 0, 1, levelConfirmed, "the model and a viewer agree: the higher charge"},
		{"unsure", 1, 0, levelNone, "an unsure model is no evidence"},
		{"yes", 1, 2, levelNone, "the other side and two viewers are not enough"},
		{"", 0, 2, levelNone, "two viewers alone are not enough"},
		{"", 0, 3, levelReported, "three viewers: the smaller charge"},
		{"yes", 0, 3, levelReported, "even against a yes, three viewers are the smaller charge"},
		{"", 5, 0, levelNone, "people in the battle can't charge the other side alone, however many"},
	} {
		if got := offTopicVerdict(c.model, c.party, c.viewers); got != c.want {
			t.Errorf("model %q, %d from the battle, %d viewers: level %d, want %d — %s",
				c.model, c.party, c.viewers, got, c.want, c.why)
		}
	}
}

func TestOffTopic_WhatEachLevelCosts(t *testing.T) {
	if pointsFor(levelNone) != 0 || pointsFor(levelReported) != 25 || pointsFor(levelConfirmed) != 50 {
		t.Errorf("levels cost %d, %d, %d — want 0, 25 and 50",
			pointsFor(levelNone), pointsFor(levelReported), pointsFor(levelConfirmed))
	}
	// Going from the smaller charge to the higher one charges the difference,
	// so a video never costs more than the higher charge in all.
	if d := pointsFor(levelConfirmed) - pointsFor(levelReported); d != 25 {
		t.Errorf("topping up costs %d, want 25", d)
	}
	if penaltyRating(1000, 50) != 950 || penaltyRating(ratingFloor+3, 50) != ratingFloor {
		t.Error("a charge takes the points off, never below the floor")
	}
	if falseReportPenalty >= penaltyReported {
		t.Error("a false report should cost less than a video that doesn't match")
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
