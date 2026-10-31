package main

// Changing your username, once every 30 days (username_change.go).
//
// Through the real routes against a real database — and for the live
// connection, a real connection: the rules and the 30 days; every other
// sign-in signed out and this one handed a fresh one; the connection under
// the old name closed, so a message for whoever takes that name next
// reaches them and nobody else; what was waiting moving with the name; and
// a name freed by a change or a deletion carrying nothing to its next owner.

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/gorilla/websocket"
)

// renameServer is the routes these tests go through, wired as main.go wires
// them.
func renameServer(t *testing.T) *httptest.Server {
	t.Helper()
	r := mux.NewRouter()
	api := r.PathPrefix("/api/v1").Subrouter()
	api.HandleFunc("/users/{id}/username", authed(UsernameStatusHandler)).Methods("GET")
	api.HandleFunc("/users/{id}/username", authed(ChangeUsernameHandler)).Methods("PUT")
	api.HandleFunc("/chat/send", authed(SendMessageHandler)).Methods("POST")
	api.HandleFunc("/auth/refresh", authed(RefreshTokenHandler)).Methods("POST")
	r.HandleFunc("/ws/{username}", WebsocketHandler).Methods("GET")
	srv := httptest.NewServer(r)
	t.Cleanup(srv.Close)
	return srv
}

// call makes one request with sign-in [token], and reads the answer.
func call(t *testing.T, srv *httptest.Server, token, method, path, body string) (int, map[string]any) {
	t.Helper()
	// The "== nil" and the returns are for NilAway, which cannot see that
	// t.Fatalf stops the test (the same shape as authedDo).
	req, err := http.NewRequest(method, srv.URL+path, strings.NewReader(body))
	if err != nil || req == nil {
		t.Fatalf("%s %s: %v", method, path, err)
		return 0, nil
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	res, err := http.DefaultClient.Do(req)
	if err != nil || res == nil {
		t.Fatalf("%s %s: %v", method, path, err)
		return 0, nil
	}
	defer res.Body.Close()
	var got map[string]any
	_ = json.NewDecoder(res.Body).Decode(&got)
	return res.StatusCode, got
}

// rename asks for [id]'s username to become [name], signed in with [token].
func rename(t *testing.T, srv *httptest.Server, token string, id int, name string) (int, map[string]any) {
	t.Helper()
	return call(t, srv, token, "PUT", "/api/v1/users/"+strconv.Itoa(id)+"/username",
		`{"username":"`+name+`"}`)
}

// connectAs opens a live connection as [name] with sign-in [token], the way
// the app does. The error is the server saying no.
func connectAs(t *testing.T, srv *httptest.Server, name, token string) (*liveClient, error) {
	t.Helper()
	url := "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws/" + name + "?token=" + token
	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		return nil, err
	}
	t.Cleanup(func() { conn.Close() })
	c := &liveClient{conn: conn, events: make(chan map[string]interface{}, 256)}
	go func() {
		for {
			_, data, err := conn.ReadMessage()
			if err != nil {
				close(c.events)
				return
			}
			var ev map[string]interface{}
			if json.Unmarshal(data, &ev) == nil {
				c.events <- ev
			}
		}
	}()
	deadline := time.Now().Add(2 * time.Second)
	for !IsUserOnline(name) {
		if time.Now().After(deadline) {
			t.Fatalf("%s never showed as online", name)
		}
		time.Sleep(5 * time.Millisecond)
	}
	return c, nil
}

// seenLongAgo marks account [id] as last seen long ago, so finished() can
// tell when the server stamps it again.
func seenLongAgo(t *testing.T, id int) {
	t.Helper()
	mustExec(t, `UPDATE users SET last_seen = '2000-01-01' WHERE id = $1`, id)
}

// finished waits until the server has finished with account [id]'s
// connection: it no longer shows [name] as online, and it has stamped the
// account as last seen — the last thing it does, in the background, so
// nothing of the connection is still running when the test's database
// closes. Call seenLongAgo before the connection closes. (The stamp landing
// on [id] also shows it is made by account, not by a name that may have
// changed hands.)
func finished(t *testing.T, id int, name string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		var stamped bool
		_ = db.QueryRow(`SELECT last_seen > '2001-01-01' FROM users WHERE id = $1`, id).Scan(&stamped)
		if !IsUserOnline(name) && stamped {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the server never finished with account %d's connection", id)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// hangUp closes [c] as the app does, and waits until the server is done.
func hangUp(t *testing.T, c *liveClient, id int, name string) {
	t.Helper()
	seenLongAgo(t, id)
	_ = c.conn.Close()
	finished(t, id, name)
}

// closedWithin is whether the server closed [c]'s connection within [d].
func closedWithin(c *liveClient, d time.Duration) bool {
	timeout := time.After(d)
	for {
		select {
		case _, open := <-c.events:
			if !open {
				return true
			}
		case <-timeout:
			return false
		}
	}
}

func TestUsernameChange_TheRulesAndTheThirtyDays(t *testing.T) {
	defer withDB(t)()
	resetRedis(t)
	resetActionLimiters(t)
	srv := renameServer(t)
	defer leaver(t, 9871, "first_name", "sunset42go")()
	defer leaver(t, 9872, "Taken_Name", "sunset42go")() // an older account, with capitals
	before := signedInAt(t, "9871", "first_name", time.Now().Add(-time.Hour))
	status := "/api/v1/users/9871/username"

	// Never changed: it may change now.
	code, got := call(t, srv, before, "GET", status, "")
	if code != 200 || got["username"] != "first_name" || got["nextChangeAt"] != nil {
		t.Fatalf("before any change the status answered %d %v", code, got)
	}

	// Sign-up's rules, your own name, and names taken in any capitals.
	for name, want := range map[string]struct {
		code  int
		words string
	}{
		"ab":         {400, "3 to 20 characters"},
		"no spaces":  {400, "3 to 20 characters"},
		"First_Name": {400, "already your username"},
		"taken_name": {409, "taken"},
		"TAKEN_NAME": {409, "taken"},
	} {
		code, got := rename(t, srv, before, 9871, name)
		words, _ := got["error"].(string)
		if code != want.code || got["field"] != "username" || !strings.Contains(words, want.words) {
			t.Errorf("changing to %q answered %d %v, want %d saying %q", name, code, got, want.code, want.words)
		}
	}

	// Changed — stored in lower case, as sign-up stores names.
	code, got = rename(t, srv, before, 9871, "Second_Name")
	if code != 200 {
		t.Fatalf("changing to a free name answered %d %v", code, got)
	}
	user, _ := got["user"].(map[string]any)
	if user["username"] != "second_name" {
		t.Errorf("the account came back as %v", user["username"])
	}
	fresh, _ := got["token"].(string)
	claims, err := parseToken(fresh)
	if err != nil || claims.Username != "second_name" || claims.Subject != "9871" {
		t.Fatalf("the fresh sign-in does not work as the new name: %v %+v", err, claims)
	}
	next, err := time.Parse(time.RFC3339, got["nextChangeAt"].(string))
	if err != nil || next.Before(time.Now().Add(29*24*time.Hour)) || next.After(time.Now().Add(31*24*time.Hour)) {
		t.Errorf("nextChangeAt = %v (%v), want about 30 days from now", got["nextChangeAt"], err)
	}

	// Every sign-in from before the change carries the old name: signed out.
	if code, _ := call(t, srv, before, "GET", status, ""); code != 401 {
		t.Errorf("a sign-in from before the change still works (%d)", code)
	}
	code, got = call(t, srv, fresh, "GET", status, "")
	if code != 200 || got["username"] != "second_name" || got["nextChangeAt"] == nil {
		t.Errorf("after the change the status answered %d %v", code, got)
	}

	// Not again for 30 days — counted from the change, not from the asking:
	// ten days on, the date given is twenty days away.
	mustExec(t, `UPDATE users SET username_changed_at = username_changed_at - INTERVAL '10 days' WHERE id = 9871`)
	code, got = rename(t, srv, fresh, 9871, "third_name")
	words, _ := got["error"].(string)
	when, _ := got["nextChangeAt"].(string)
	soonest, err := time.Parse(time.RFC3339, when)
	if code != 409 || got["tooSoon"] != true || !strings.Contains(words, "again on") || err != nil ||
		soonest.Before(time.Now().Add(19*24*time.Hour)) || soonest.After(time.Now().Add(21*24*time.Hour)) {
		t.Errorf("a second change ten days on answered %d %v, want the date twenty days away", code, got)
	}

	// Nobody else's name: not to change, nor to look at.
	if code, _ := rename(t, srv, fresh, 9872, "not_yours"); code != 403 {
		t.Errorf("changing somebody else's username answered %d", code)
	}
	if code, _ := call(t, srv, fresh, "GET", "/api/v1/users/9872/username", ""); code != 403 {
		t.Errorf("reading somebody else's username status answered %d", code)
	}

	// The old name is anybody's now, and signing in takes the new one.
	if UserExists("first_name") {
		t.Error("the old name is still taken")
	}
	if code, _ := signInAs(t, "second_name", "sunset42go", false); code != 200 {
		t.Errorf("signing in with the new name answered %d", code)
	}
	if code, _ := signInAs(t, "first_name", "sunset42go", false); code != 401 {
		t.Errorf("signing in with the old name answered %d", code)
	}

	// 30 days on, it may change again.
	mustExec(t, `UPDATE users SET username_changed_at = NOW() - INTERVAL '30 days 1 minute' WHERE id = 9871`)
	if code, got := rename(t, srv, fresh, 9871, "third_name"); code != 200 {
		t.Errorf("a change 30 days on answered %d %v", code, got)
	}
}

func TestUsernameChange_TheLiveConnectionFollowsTheName(t *testing.T) {
	// The database closes last, after the connections (as liveSetup does).
	t.Cleanup(withDB(t))
	resetRedis(t)
	resetActionLimiters(t)
	srv := renameServer(t)
	defer db.Exec(`DELETE FROM chat_messages WHERE sender_id IN (9881, 9882, 9883) OR receiver_id IN (9881, 9882, 9883)`)
	defer db.Exec(`DELETE FROM users WHERE id = 9883`)
	defer leaver(t, 9881, "before_name", "sunset42go")()
	defer leaver(t, 9882, "chat_friend", "sunset42go")()

	before := signedInAt(t, "9881", "before_name", time.Now().Add(-time.Hour))
	old, err := connectAs(t, srv, "before_name", before)
	if err != nil {
		t.Fatalf("could not connect before the change: %v", err)
	}
	seenLongAgo(t, 9881)
	code, got := rename(t, srv, before, 9881, "after_name")
	if code != 200 {
		t.Fatalf("the change answered %d %v", code, got)
	}
	fresh, _ := got["token"].(string)

	// The connection filed under the old name is closed at once.
	if !closedWithin(old, 2*time.Second) {
		t.Fatal("the live connection under the old name is still open")
	}
	finished(t, 9881, "before_name")

	// Somebody else takes the old name and connects; the renamed account
	// connects under its new name with its fresh sign-in.
	mustExec(t, `INSERT INTO users (id, username, password) VALUES (9883, 'before_name', '')`)
	newcomer, err := connectAs(t, srv, "before_name", testToken(t, "9883", "before_name"))
	if err != nil {
		t.Fatalf("the old name's new owner could not connect: %v", err)
	}
	renamed, err := connectAs(t, srv, "after_name", fresh)
	if err != nil {
		t.Fatalf("the renamed account could not connect under its new name: %v", err)
	}

	// A message for each reaches each, and only each.
	friend := testToken(t, "9882", "chat_friend")
	for to, text := range map[int]string{9883: "for the new before_name", 9881: "for after_name"} {
		if code, got := call(t, srv, friend, "POST", "/api/v1/chat/send",
			`{"senderId":"9882","receiverId":"`+strconv.Itoa(to)+`","message":"`+text+`"}`); code != 200 {
			t.Fatalf("sending to %d answered %d %v", to, code, got)
		}
	}
	for c, want := range map[*liveClient]string{newcomer: "for the new before_name", renamed: "for after_name"} {
		if ev := next(t, c, "chat", 2*time.Second); ev["message"] != want {
			t.Errorf("a connection got %v, want %q", ev["message"], want)
		}
		none(t, c, "chat", 300*time.Millisecond)
	}

	// A sign-in from before the change cannot come back under the old name;
	// nor can one that outlived the change still carrying the old name.
	if _, err := connectAs(t, srv, "before_name", before); err == nil {
		t.Error("a sign-in from before the change connected under the old name")
	}
	if _, err := connectAs(t, srv, "before_name", testToken(t, "9881", "before_name")); err == nil {
		t.Error("a sign-in carrying the old name connected under it, after the change")
	}
	hangUp(t, newcomer, 9883, "before_name")
	hangUp(t, renamed, 9881, "after_name")
}

func TestUsernameChange_WhatWasWaitingMovesWithTheName(t *testing.T) {
	defer withDB(t)()
	resetRedis(t)
	resetActionLimiters(t)
	srv := renameServer(t)
	defer leaver(t, 9884, "waiting_name", "sunset42go")()
	StoreNotificationInRedis("waiting_name", Notification{Type: "comment", Message: "for the renamer"})
	// Left under the new name by somebody who had it before.
	StoreNotificationInRedis("moved_name", Notification{Type: "comment", Message: "for a previous owner"})

	if code, got := rename(t, srv, testToken(t, "9884", "waiting_name"), 9884, "moved_name"); code != 200 {
		t.Fatalf("the change answered %d %v", code, got)
	}
	got, _ := GetStoredNotifications("moved_name")
	if len(got) != 1 || got[0].Message != "for the renamer" {
		t.Errorf("waiting under the new name: %+v, want only the renamer's own", got)
	}
	if left, found := GetStoredNotifications("waiting_name"); found {
		t.Errorf("still waiting under the old name, for whoever takes it: %+v", left)
	}

	// Somebody with nothing waiting takes a name with something left under
	// it: that is not theirs either.
	defer leaver(t, 9887, "nothing_waiting", "sunset42go")()
	StoreNotificationInRedis("left_over", Notification{Type: "comment", Message: "for a previous owner"})
	if code, got := rename(t, srv, testToken(t, "9887", "nothing_waiting"), 9887, "left_over"); code != 200 {
		t.Fatalf("the change answered %d %v", code, got)
	}
	if left, found := GetStoredNotifications("left_over"); found {
		t.Errorf("a renamed account was handed what was left under its new name: %+v", left)
	}
}

// Ten tries an hour: only one can succeed every 30 days, and a stream of
// taken names is somebody finding out which names exist.
func TestUsernameChange_TriesAreLimited(t *testing.T) {
	defer withDB(t)()
	resetRedis(t)
	resetActionLimiters(t)
	srv := renameServer(t)
	defer leaver(t, 9888, "many_tries", "sunset42go")()
	defer leaver(t, 9889, "taken_one", "sunset42go")()
	tok := testToken(t, "9888", "many_tries")
	for i := 0; i < 10; i++ {
		if code, _ := rename(t, srv, tok, 9888, "taken_one"); code != 409 {
			t.Fatalf("try %d answered %d, want 409 (taken)", i+1, code)
		}
	}
	if code, _ := rename(t, srv, tok, 9888, "taken_one"); code != 429 {
		t.Errorf("the eleventh try in an hour answered %d, want 429", code)
	}
}

// While two older names still clash, the database's lock is off
// (usernames.go). A change must not make a clash of its own then.
func TestUsernameChange_ATakenNameIsRefusedEvenWithoutTheLock(t *testing.T) {
	defer withDB(t)()
	resetRedis(t)
	resetActionLimiters(t)
	srv := renameServer(t)
	defer db.Exec(`CREATE UNIQUE INDEX IF NOT EXISTS users_username_lower_key ON users (lower(username))`)
	mustExec(t, `DROP INDEX IF EXISTS users_username_lower_key`)
	defer leaver(t, 9890, "wants_a_name", "sunset42go")()
	defer leaver(t, 9891, "Has_It", "sunset42go")()
	if code, got := rename(t, srv, testToken(t, "9890", "wants_a_name"), 9890, "has_it"); code != 409 {
		t.Errorf("with the lock off, a name taken in other capitals answered %d %v", code, got)
	}
	var n int
	_ = db.QueryRow(`SELECT COUNT(*) FROM users WHERE lower(username) = 'has_it'`).Scan(&n)
	if n != 1 {
		t.Errorf("%d accounts are now named has_it", n)
	}
}

func TestUsernameChange_AFreedNameCarriesNothingToItsNextOwner(t *testing.T) {
	defer withDB(t)()
	resetRedis(t)
	resetActionLimiters(t)

	// Deleted for good: what waited under its name goes with it.
	defer leaver(t, 9885, "gone_name", "sunset42go")()
	StoreNotificationInRedis("gone_name", Notification{Type: "comment", Message: "for the deleted account"})
	deleteForGood(t, 9885, "gone_name")
	if left, found := GetStoredNotifications("gone_name"); found {
		t.Errorf("deleting the account left %+v under its freed name", left)
	}

	// Signing up as a name somebody left something under: none of it is theirs.
	defer db.Exec(`DELETE FROM users WHERE username = 'fresh_owner' OR lower(email) = 'new85@example.com'`)
	StoreNotificationInRedis("fresh_owner", Notification{Type: "comment", Message: "for an owner before"})
	body := goodSignup(85)
	body["username"] = "fresh_owner"
	if code, got := signUp(t, body, "203.0.113.85"); code != 201 {
		t.Fatalf("signing up answered %d %v", code, got)
	}
	if left, found := GetStoredNotifications("fresh_owner"); found {
		t.Errorf("a new account was handed what was left under its name: %+v", left)
	}
}

func TestUsernameChange_ARefreshedSignInCarriesTheNameAsItIsNow(t *testing.T) {
	defer withDB(t)()
	resetRedis(t)
	srv := renameServer(t)
	defer leaver(t, 9886, "current_name", "sunset42go")()

	// A sign-in that outlived a change, still carrying the old name.
	code, got := call(t, srv, testToken(t, "9886", "old_name_kept"), "POST", "/api/v1/auth/refresh", "")
	if code != 200 {
		t.Fatalf("refreshing answered %d %v", code, got)
	}
	tok, _ := got["token"].(string)
	claims, err := parseToken(tok)
	if err != nil || claims.Username != "current_name" {
		t.Errorf("the refreshed sign-in carries %+v (%v), want the name the account has now", claims, err)
	}
}

// With more than one server, the old name's connection can be on another.
func TestUsernameChange_OtherServersAreAskedToCloseTheOldName(t *testing.T) {
	srv := liveSetup(t)
	multiReplica() // settle the switch, then turn it on for this test only
	was := multiReplicaVal
	multiReplicaVal = true
	defer func() { multiReplicaVal = was }()

	sub := rdb.Subscribe(rctx, wsRelayChannel)
	defer sub.Close()
	if _, err := sub.Receive(rctx); err != nil {
		t.Fatal(err)
	}
	wsDisconnect("leo")
	var payload string
	select {
	case msg := <-sub.Channel():
		payload = msg.Payload
	case <-time.After(2 * time.Second):
		t.Fatal("the other servers were not asked to close leo's connection")
	}
	var env wsRelayEnvelope
	if err := json.Unmarshal([]byte(payload), &env); err != nil || env.Username != "leo" || !env.Close {
		t.Fatalf("the request to the other servers was %s (%v)", payload, err)
	}

	// And the server that holds the connection closes it when asked.
	leo := dial(t, srv, liveLeo)
	seenLongAgo(t, liveLeo)
	wsRelayed(payload)
	if !closedWithin(leo, 2*time.Second) {
		t.Fatal("asked by another server, this one left leo's connection open")
	}
	finished(t, liveLeo, "leo")
}

func TestUsernameChange_MainWiresTheRoutes(t *testing.T) {
	src, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	code := regexp.MustCompile(`(?m)^\s*//.*$`).ReplaceAllString(string(src), "")
	for _, want := range []string{
		`"/users/{id}/username", authed(UsernameStatusHandler)`,
		`"/users/{id}/username", authed(ChangeUsernameHandler)`,
	} {
		if !strings.Contains(code, want) {
			t.Errorf("main.go does not wire %s", want)
		}
	}
}
