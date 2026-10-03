package main

// Following someone across a block — against a real database, through the
// same routes the app uses.
//
// Blocking already ended any follow between the two. But the next tap on
// Follow put it straight back, either way round: you could follow someone
// you had blocked, and someone who had blocked you could follow you.

import (
	"encoding/json"
	"io"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
)

// followAs taps Follow as [from] on [to], and answers the status and body.
func followAs(t *testing.T, srv *httptest.Server, from, to int) (int, map[string]string) {
	t.Helper()
	actionLimitersMu.Lock()
	delete(actionLimiters, "follow")
	actionLimitersMu.Unlock()
	res := authedDo(t, srv, from, "POST", "/api/v1/follow",
		`{"followerId":"`+strconv.Itoa(from)+`","followingId":"`+strconv.Itoa(to)+`"}`)
	raw, _ := io.ReadAll(res.Body)
	out := map[string]string{}
	_ = json.Unmarshal(raw, &out)
	return res.StatusCode, out
}

func follows(t *testing.T, from, to int) bool {
	t.Helper()
	var ok bool
	if err := db.QueryRow(`SELECT EXISTS (SELECT 1 FROM follows
		WHERE follower_id = $1 AND following_id = $2)`, from, to).Scan(&ok); err != nil {
		t.Fatal(err)
	}
	return ok
}

func blockAs(t *testing.T, srv *httptest.Server, blocker, blocked int) {
	t.Helper()
	actionLimitersMu.Lock()
	delete(actionLimiters, "block")
	actionLimitersMu.Unlock()
	res := authedDo(t, srv, blocker, "POST", "/api/v1/blocks",
		`{"blockerId":"`+strconv.Itoa(blocker)+`","blockedId":"`+strconv.Itoa(blocked)+`"}`)
	if res.StatusCode != 200 {
		t.Fatalf("blocking = %d", res.StatusCode)
	}
}

func TestFollow_SomeoneYouBlockedSaysSoAndIsNotFollowed(t *testing.T) {
	srv := liveSetup(t)
	blockAs(t, srv, liveMaya, liveLeo)
	code, body := followAs(t, srv, liveMaya, liveLeo)
	if code != 409 || body["reason"] != "you_blocked" {
		t.Fatalf("maya following leo, whom she blocked = %d %v, want 409 you_blocked", code, body)
	}
	if !strings.Contains(body["error"], "Unblock") {
		t.Fatalf("the message does not say what to do: %q", body["error"])
	}
	if follows(t, liveMaya, liveLeo) {
		t.Fatal("the follow went in anyway")
	}

	// Unblocked, the same tap works.
	res := authedDo(t, srv, liveMaya, "POST", "/api/v1/unblock",
		`{"blockerId":"`+strconv.Itoa(liveMaya)+`","blockedId":"`+strconv.Itoa(liveLeo)+`"}`)
	if res.StatusCode != 200 {
		t.Fatalf("unblocking = %d", res.StatusCode)
	}
	if code, body := followAs(t, srv, liveMaya, liveLeo); code != 200 {
		t.Fatalf("after unblocking, following = %d %v", code, body)
	}
	if !follows(t, liveMaya, liveLeo) {
		t.Fatal("unblocked and followed, but no follow")
	}
}

func TestFollow_SomeoneWhoBlockedYouIsNotFollowedAndTheBlockIsNotNamed(t *testing.T) {
	srv := liveSetup(t)
	blockAs(t, srv, liveMaya, liveLeo)
	code, body := followAs(t, srv, liveLeo, liveMaya)
	if code != 403 || body["reason"] != "unavailable" {
		t.Fatalf("leo following maya, who blocked him = %d %v, want 403 unavailable", code, body)
	}
	if strings.Contains(strings.ToLower(body["error"]), "block") {
		t.Fatalf("leo is told he was blocked: %q", body["error"])
	}
	if follows(t, liveLeo, liveMaya) {
		t.Fatal("the follow went in anyway")
	}
}

func TestFollow_NoBlockFollowsAsBefore(t *testing.T) {
	srv := liveSetup(t)
	if code, body := followAs(t, srv, liveSam, liveMaya); code != 200 {
		t.Fatalf("sam following maya = %d %v", code, body)
	}
	if !follows(t, liveSam, liveMaya) {
		t.Fatal("no follow")
	}
	// Someone else's block between two other people changes nothing.
	blockAs(t, srv, liveMaya, liveLeo)
	if code, _ := followAs(t, srv, liveSam, liveLeo); code != 200 {
		t.Fatalf("sam following leo = %d", code)
	}
}
