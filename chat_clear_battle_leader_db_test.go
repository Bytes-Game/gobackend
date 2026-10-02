package main

// Two things the owner asked for, against a real database:
//
//   - "Delete chat": gone from the list of the person who deleted it, kept
//     for the other person, and started again by a new message.
//   - A battle says which side is ahead as it arrives, so the app can open
//     on the side that is winning instead of waiting for a second answer
//     that, on a slow server, often came too late.

import (
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
)

func listedWith(t *testing.T, me, other int) *Conversation {
	t.Helper()
	for _, c := range GetConversations(me) {
		if c.UserID == strconv.Itoa(other) {
			c := c
			return &c
		}
	}
	return nil
}

func TestClearChat_GoneForMeKeptForThemBackOnANewMessage(t *testing.T) {
	srv := liveSetup(t)
	message(t, liveLeo, liveMaya, "first")
	message(t, liveLeo, liveMaya, "second")
	message(t, liveMaya, liveLeo, "reply")
	if c := listedWith(t, liveMaya, liveLeo); c == nil || c.UnreadCount != 2 {
		t.Fatalf("before: maya's chat with leo = %+v, want it listed with 2 unread", c)
	}

	res := authedDo(t, srv, liveMaya, "POST", "/api/v1/chat/clear",
		`{"otherUserId":"`+strconv.Itoa(liveLeo)+`"}`)
	if res.StatusCode != 200 {
		t.Fatalf("delete chat = %d", res.StatusCode)
	}

	if c := listedWith(t, liveMaya, liveLeo); c != nil {
		t.Fatalf("the deleted chat is still on maya's list: %+v", c)
	}
	if got := GetChatMessages(liveMaya, liveLeo, 50, 0); len(got) != 0 {
		t.Fatalf("maya still sees %d messages in the deleted chat", len(got))
	}
	// Leo still has all of it.
	if c := listedWith(t, liveLeo, liveMaya); c == nil {
		t.Fatal("deleting took the chat away from leo too")
	}
	if got := GetChatMessages(liveLeo, liveMaya, 50, 0); len(got) != 3 {
		t.Fatalf("leo sees %d messages, want all 3", len(got))
	}

	// A new message brings it back for maya, with only the new message.
	time.Sleep(5 * time.Millisecond)
	message(t, liveLeo, liveMaya, "still there?")
	c := listedWith(t, liveMaya, liveLeo)
	if c == nil || c.LastMessage != "still there?" || c.UnreadCount != 1 {
		t.Fatalf("after a new message, maya's chat = %+v, want it back with 1 unread", c)
	}
	got := GetChatMessages(liveMaya, liveLeo, 50, 0)
	if len(got) != 1 || got[0].Message != "still there?" {
		t.Fatalf("maya sees %v, want only the new message", got)
	}
}

func TestClearChat_RefusesNonsense(t *testing.T) {
	srv := liveSetup(t)
	for _, body := range []string{`{}`, `{"otherUserId":"` + strconv.Itoa(liveMaya) + `"}`, `not json`} {
		res := authedDo(t, srv, liveMaya, "POST", "/api/v1/chat/clear", body)
		if res.StatusCode != 400 {
			t.Errorf("delete chat with %s = %d, want 400", body, res.StatusCode)
		}
	}
	// Without signing in it does not run at all.
	req := httptest.NewRequest("POST", "/api/v1/chat/clear",
		strings.NewReader(`{"otherUserId":"`+strconv.Itoa(liveLeo)+`"}`))
	w := httptest.NewRecorder()
	authed(ClearChatHandler)(w, req)
	if w.Code != 401 {
		t.Errorf("signed out = %d, want 401", w.Code)
	}
}

// leaderOf runs a battle through the same filler every feed, search,
// profile and saved list uses, and reads what it says.
func leaderOf(t *testing.T, cid string) string {
	t.Helper()
	items := []HomeFeedItem{{Type: "challenge", Challenge: &Challenge{ID: cid}}}
	populateTopResponses(items)
	if items[0].Challenge.TopResponseID == "" {
		t.Fatalf("battle %s came back without its answer", cid)
	}
	return items[0].Challenge.Leader
}

func TestBattleLeader_SaysWhoIsAheadWithTheVideo(t *testing.T) {
	defer withDB(t)()
	if _, err := db.Exec(`UPDATE users SET created_at = NOW() - INTERVAL '90 days' WHERE id IN (1, 2)`); err != nil {
		t.Fatal(err)
	}

	// Nobody has anything yet: nobody is ahead.
	cid, rid := liveBattle(t)
	if got := leaderOf(t, cid); got != "" {
		t.Fatalf("an untouched battle says %q is ahead", got)
	}

	// One genuine vote for the answer: the answer is ahead.
	ridN, _ := strconv.Atoi(rid)
	voter(t, 5301, 6)
	watched(t, 5301, "response", rid, 4000, "")
	if _, err := db.Exec(`INSERT INTO challenge_votes (challenge_id, response_id, voter_id)
		VALUES ($1, $2, 5301)`, cid, ridN); err != nil {
		t.Fatal(err)
	}
	if got := leaderOf(t, cid); got != "answer" {
		t.Fatalf("with the only vote, the answer is not ahead: %q", got)
	}

	// A genuine vote for the creator too: level, so nobody is ahead.
	voter(t, 5302, 6)
	watched(t, 5302, "challenge", cid, 4000, `{"creatorMs":4000}`)
	if _, err := db.Exec(`INSERT INTO challenge_votes (challenge_id, response_id, voter_id)
		VALUES ($1, NULL, 5302)`, cid); err != nil {
		t.Fatal(err)
	}
	if got := leaderOf(t, cid); got != "" {
		t.Fatalf("one vote each says %q is ahead", got)
	}

	// Votes level, and the creator has a like: likes break the tie — the
	// side people are liking comes first.
	if _, err := db.Exec(`INSERT INTO challenge_likes (challenge_id, user_id) VALUES ($1, 5302)`, cid); err != nil {
		t.Fatal(err)
	}
	if got := leaderOf(t, cid); got != "creator" {
		t.Fatalf("with votes level and a like for the creator: %q, want creator", got)
	}
}

func TestBattleLeader_LevelOrUnknownSaysNobody(t *testing.T) {
	two := BattleStandings{Participants: []Standing{
		{Role: "creator", Leading: true},
		{Role: "responder", ResponseID: "9", Leading: true},
	}}
	if got := leaderFor(two, "9"); got != "" {
		t.Errorf("two sides level: %q, want nobody", got)
	}
	other := BattleStandings{Participants: []Standing{
		{Role: "creator"},
		{Role: "responder", ResponseID: "8", Leading: true},
	}}
	if got := leaderFor(other, "9"); got != "" {
		t.Errorf("an answer other than the one shown is ahead: %q, want nobody", got)
	}
}
