package main

import (
	"strings"
	"testing"
)

// ════════════════════════════════════════════════════════════════════════════
// BOTH SIDES OF A BATTLE GET TOLD IT STARTED
// ════════════════════════════════════════════════════════════════════════════
//
// Only the challenger was told, which reads as an odd asymmetry once you look
// at what actually happened: two people are now in a contest people will vote
// on, and one of them found out.
//
// Neither user is online in these tests, so both notifications take the
// store-for-later path. That is the half worth testing anyway — a message
// delivered live is gone once it is read, and a stored one is what somebody
// sees when they next open the app.

func storedFor(t *testing.T, username string) []Notification {
	t.Helper()
	got, _ := GetStoredNotifications(username)
	return got
}

func TestBattleStarted_TellsTheResponderTheyAreInABattle(t *testing.T) {
	resetRedis(t)

	SendBattleStartedNotification("responder_kar", "challenger_ana", "can you beat this")

	got := storedFor(t, "responder_kar")
	if len(got) != 1 {
		t.Fatalf("the responder got %d notifications, want 1. They are the "+
			"one who just entered a contest; nothing told them.", len(got))
	}
	if got[0].Type != "battle_started" {
		t.Errorf("type is %q, want battle_started", got[0].Type)
	}
	if !strings.Contains(got[0].Message, "challenger_ana") {
		t.Errorf("message does not say who they are up against: %q", got[0].Message)
	}
	if !strings.Contains(got[0].Message, "can you beat this") {
		t.Errorf("message does not say WHICH battle: %q. A responder may have "+
			"answered several, and one they cannot identify is one they "+
			"cannot act on.", got[0].Message)
	}
}

func TestBattleStarted_TheChallengerStillHearsSeparately(t *testing.T) {
	// The two messages go to different people and say different things. The
	// risk in adding the second one is sending both to the same person, or
	// sending the responder's wording to the challenger.
	resetRedis(t)

	SendChallengeAcceptedNotification("responder_kar", "challenger_ana", "can you beat this")
	SendBattleStartedNotification("responder_kar", "challenger_ana", "can you beat this")

	challenger := storedFor(t, "challenger_ana")
	responder := storedFor(t, "responder_kar")

	if len(challenger) != 1 {
		t.Fatalf("challenger got %d notifications, want exactly 1", len(challenger))
	}
	if len(responder) != 1 {
		t.Fatalf("responder got %d notifications, want exactly 1", len(responder))
	}
	if challenger[0].Type != "challenge_accepted" {
		t.Errorf("challenger got type %q, want challenge_accepted", challenger[0].Type)
	}
	if responder[0].Type != "battle_started" {
		t.Errorf("responder got type %q, want battle_started", responder[0].Type)
	}
	if challenger[0].Message == responder[0].Message {
		t.Error("both sides got the same sentence. They did different things " +
			"and need to be told different things.")
	}
	// The challenger's says who accepted; the responder's says who they face.
	if !strings.Contains(challenger[0].Message, "responder_kar") {
		t.Errorf("the challenger is not told who accepted: %q", challenger[0].Message)
	}
	if !strings.Contains(responder[0].Message, "challenger_ana") {
		t.Errorf("the responder is not told who they face: %q", responder[0].Message)
	}
}

func TestBattleStarted_CarriesNoContentURL(t *testing.T) {
	// VideoURL on a Notification means "prefetch hint" and the app suppresses
	// the whole notification from view when it is set. A human-visible
	// message must never carry one, or nobody ever sees it.
	resetRedis(t)

	SendBattleStartedNotification("responder_kar", "challenger_ana", "can you beat this")

	got := storedFor(t, "responder_kar")
	if len(got) != 1 {
		t.Fatalf("expected one notification, got %d", len(got))
	}
	if got[0].VideoURL != "" {
		t.Errorf("carries videoUrl %q, so the app will treat it as a prefetch "+
			"hint and hide it", got[0].VideoURL)
	}
	if got[0].Timestamp == "" {
		t.Error("has no timestamp, so the app cannot order it")
	}
}
