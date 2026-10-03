package main

// The two privacy choices, saved the way the app saves them (PATCH
// /users/{id} with settings) and honoured by the server — against a real
// database.

import (
	"encoding/json"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/mux"
)

// choose saves [settings] for [id] through the same handler the app uses.
func choose(t *testing.T, id int, settings string) {
	t.Helper()
	actionLimitersMu.Lock()
	delete(actionLimiters, "settings")
	actionLimitersMu.Unlock()
	sid := strconv.Itoa(id)
	r := httptest.NewRequest("PATCH", "/api/v1/users/"+sid,
		strings.NewReader(`{"settings":`+settings+`}`))
	r = mux.SetURLVars(withUser(r, sid, liveNames[id]), map[string]string{"id": sid})
	w := httptest.NewRecorder()
	UpdateUserProfileHandler(w, r)
	if w.Code != 200 {
		t.Fatalf("saving settings for %d = %d: %s", id, w.Code, w.Body.String())
	}
}

func sendAs(t *testing.T, srv *httptest.Server, from, to int, text string) int {
	t.Helper()
	res := authedDo(t, srv, from, "POST", "/api/v1/chat/send",
		`{"senderId":"`+strconv.Itoa(from)+`","receiverId":"`+strconv.Itoa(to)+
			`","message":"`+text+`"}`)
	return res.StatusCode
}

func TestPrivacy_OnlyPeopleIFollowCanMessageMe(t *testing.T) {
	srv := liveSetup(t)
	// Before choosing anything, anyone can.
	if code := sendAs(t, srv, liveLeo, liveMaya, "hi"); code != 200 {
		t.Fatalf("by default leo messaging maya = %d, want 200", code)
	}
	choose(t, liveMaya, `{"messages":"following"}`)
	if code := sendAs(t, srv, liveLeo, liveMaya, "hello?"); code != 403 {
		t.Fatalf("maya takes messages only from people she follows; leo got %d", code)
	}
	// Forwarding is a message too.
	id := message(t, liveLeo, liveSam, "pass it on")
	res := authedDo(t, srv, liveLeo, "POST", "/api/v1/chat/forward",
		`{"messageId":"`+strconv.Itoa(id)+`","receiverId":"`+strconv.Itoa(liveMaya)+`"}`)
	if res.StatusCode != 403 {
		t.Fatalf("forwarding to maya = %d, want 403", res.StatusCode)
	}
	// Maya can still message leo; and once she follows him, he can reply.
	if code := sendAs(t, srv, liveMaya, liveLeo, "hey"); code != 200 {
		t.Fatalf("maya messaging leo = %d", code)
	}
	if _, err := db.Exec(`INSERT INTO follows (follower_id, following_id) VALUES ($1, $2)`,
		liveMaya, liveLeo); err != nil {
		t.Fatal(err)
	}
	if code := sendAs(t, srv, liveLeo, liveMaya, "now?"); code != 200 {
		t.Fatalf("maya follows leo now; leo messaging her = %d, want 200", code)
	}
	// And back to everyone.
	choose(t, liveMaya, `{"messages":"everyone"}`)
	if code := sendAs(t, srv, liveSam, liveMaya, "hi maya"); code != 200 {
		t.Fatalf("maya takes messages from everyone again; sam got %d", code)
	}
}

// goPrivate makes [id]'s account private (or public again) through the same
// handler the app's Private account switch uses.
func goPrivate(t *testing.T, id int, private bool) {
	t.Helper()
	actionLimitersMu.Lock()
	delete(actionLimiters, "settings")
	delete(actionLimiters, "profile_edit")
	actionLimitersMu.Unlock()
	vis := "public"
	if private {
		vis = "friends"
	}
	sid := strconv.Itoa(id)
	r := httptest.NewRequest("PATCH", "/api/v1/users/"+sid,
		strings.NewReader(`{"visibility":"`+vis+`"}`))
	r = mux.SetURLVars(withUser(r, sid, liveNames[id]), map[string]string{"id": sid})
	w := httptest.NewRecorder()
	UpdateUserProfileHandler(w, r)
	if w.Code != 200 {
		t.Fatalf("making %d %s = %d: %s", id, vis, w.Code, w.Body.String())
	}
}

// A private account greys "Everyone" out in the app, so it must be off on
// the server too — even for someone who picked "Everyone" before going
// private.
func TestPrivacy_APrivateAccountTakesMessagesOnlyFromPeopleItFollows(t *testing.T) {
	srv := liveSetup(t)
	choose(t, liveMaya, `{"messages":"everyone"}`)
	goPrivate(t, liveMaya, true)
	if code := sendAs(t, srv, liveLeo, liveMaya, "hi"); code != 403 {
		t.Fatalf("maya is private; leo, whom she does not follow, got %d, want 403", code)
	}
	if _, err := db.Exec(`INSERT INTO follows (follower_id, following_id) VALUES ($1, $2)`,
		liveMaya, liveLeo); err != nil {
		t.Fatal(err)
	}
	if code := sendAs(t, srv, liveLeo, liveMaya, "now?"); code != 200 {
		t.Fatalf("maya follows leo; leo messaging her = %d, want 200", code)
	}
	if code := sendAs(t, srv, liveSam, liveMaya, "and me?"); code != 403 {
		t.Fatalf("maya does not follow sam; sam got %d, want 403", code)
	}
	// Public again: "Everyone" means everyone again.
	goPrivate(t, liveMaya, false)
	if code := sendAs(t, srv, liveSam, liveMaya, "hi maya"); code != 200 {
		t.Fatalf("maya is public and takes messages from everyone; sam got %d", code)
	}
}

func TestPrivacy_APrivateAccountTakesCallsOnlyFromPeopleItFollows(t *testing.T) {
	srv := liveSetup(t)
	goPrivate(t, liveMaya, true)
	leo := dial(t, srv, liveLeo)
	maya := dial(t, srv, liveMaya)
	send(t, leo, map[string]interface{}{"type": "call_offer", "to": strconv.Itoa(liveMaya), "callId": "pv1"})
	if ev := next(t, leo, "call_unavailable", 3*time.Second); ev["callId"] != "pv1" {
		t.Fatalf("call_unavailable = %v", ev)
	}
	none(t, maya, "call_offer", 300*time.Millisecond)
}

func TestPrivacy_OnlyPeopleIFollowCanCallMe(t *testing.T) {
	srv := liveSetup(t)
	choose(t, liveMaya, `{"messages":"following"}`)
	leo := dial(t, srv, liveLeo)
	maya := dial(t, srv, liveMaya)
	send(t, leo, map[string]interface{}{"type": "call_offer", "to": strconv.Itoa(liveMaya), "callId": "p1"})
	if ev := next(t, leo, "call_unavailable", 3*time.Second); ev["callId"] != "p1" {
		t.Fatalf("call_unavailable = %v", ev)
	}
	none(t, maya, "call_offer", 300*time.Millisecond)
	// Typing still passes: it is not a message or a call.
	send(t, leo, map[string]interface{}{"type": "typing", "to": strconv.Itoa(liveMaya), "typing": true})
	next(t, maya, "typing", 3*time.Second)
}

func onlineStatus(t *testing.T, username string) map[string]interface{} {
	t.Helper()
	r := mux.SetURLVars(httptest.NewRequest("GET", "/api/v1/chat/online/"+username, nil),
		map[string]string{"username": username})
	w := httptest.NewRecorder()
	OnlineStatusHandler(w, r)
	var out map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestPrivacy_SwitchesInARowAreNotRefused(t *testing.T) {
	_ = liveSetup(t)
	actionLimitersMu.Lock()
	delete(actionLimiters, "settings")
	delete(actionLimiters, "profile_edit")
	actionLimitersMu.Unlock()
	// Five switches on the Privacy page, one after another.
	for i, s := range []string{
		`{"messages":"following"}`, `{"showActivity":false}`,
		`{"messages":"everyone"}`, `{"showActivity":true}`, `{"theme":"dark"}`,
	} {
		sid := strconv.Itoa(liveMaya)
		r := httptest.NewRequest("PATCH", "/api/v1/users/"+sid,
			strings.NewReader(`{"settings":`+s+`}`))
		r = mux.SetURLVars(withUser(r, sid, "maya"), map[string]string{"id": sid})
		w := httptest.NewRecorder()
		UpdateUserProfileHandler(w, r)
		if w.Code != 200 {
			t.Fatalf("switch %d = %d: %s", i+1, w.Code, w.Body.String())
		}
	}
}

func TestPrivacy_HideWhenImActive(t *testing.T) {
	_ = liveSetup(t)
	if _, err := db.Exec(`UPDATE users SET last_seen = NOW() - INTERVAL '1 hour' WHERE id = $1`,
		liveMaya); err != nil {
		t.Fatal(err)
	}
	if got := onlineStatus(t, "maya"); got["lastSeen"] == "" {
		t.Fatalf("by default maya's last visit shows: %v", got)
	}
	choose(t, liveMaya, `{"showActivity":false}`)
	got := onlineStatus(t, "maya")
	if got["online"] != false || got["lastSeen"] != "" {
		t.Fatalf("maya hides when she is active, but others see %v", got)
	}
	// Somebody else's still shows.
	if _, err := db.Exec(`UPDATE users SET last_seen = NOW() WHERE id = $1`, liveSam); err != nil {
		t.Fatal(err)
	}
	if got := onlineStatus(t, "sam"); got["lastSeen"] == "" {
		t.Fatalf("sam never chose to hide: %v", got)
	}
	choose(t, liveMaya, `{"showActivity":true}`)
	if got := onlineStatus(t, "maya"); got["lastSeen"] == "" {
		t.Fatalf("maya shows again: %v", got)
	}
}
