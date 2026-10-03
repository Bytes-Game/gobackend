package main

// "Seen", "Delivered", "typing…" and call set-up, end to end: a real
// server, real live connections, a real database.
//
// The bug: opening a chat marked the messages read and told nobody, so the
// sender's phone said "Sent" until they reopened the chat. These tests open
// the sender's live connection FIRST and wait for the signal to arrive on
// it, so a server that marks the rows and stays quiet fails.

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

const (
	liveLeo  = 9501
	liveMaya = 9502
	liveSam  = 9503
)

// liveServer is the routes these tests go through, wired the way main.go
// wires them.
func liveServer(t *testing.T) *httptest.Server {
	t.Helper()
	r := mux.NewRouter()
	api := r.PathPrefix("/api/v1").Subrouter()
	api.HandleFunc("/chat/send", authed(SendMessageHandler)).Methods("POST")
	api.HandleFunc("/chat/messages/{userId}/{otherUserId}", authed(GetMessagesHandler)).Methods("GET")
	api.HandleFunc("/chat/read", authed(MarkReadHandler)).Methods("POST")
	api.HandleFunc("/chat/forward", authed(ForwardMessageHandler)).Methods("POST")
	api.HandleFunc("/chat/clear", authed(ClearChatHandler)).Methods("POST")
	api.HandleFunc("/calls/ice", authed(CallIceServersHandler)).Methods("GET")
	api.HandleFunc("/notifications/register", authed(HandleRegisterPushToken)).Methods("POST")
	api.HandleFunc("/chat/media", authed(ChatMediaPresignHandler)).Methods("POST")
	api.HandleFunc("/chat/edit", authed(EditMessageHandler)).Methods("POST")
	api.HandleFunc("/chat/delete", authed(DeleteMessageHandler)).Methods("POST")
	api.HandleFunc("/chat/conversations/{userId}", authed(GetConversationsHandler)).Methods("GET")
	api.HandleFunc("/follow", authed(HandleFollowEvent)).Methods("POST")
	api.HandleFunc("/blocks", authed(BlockUserHandler)).Methods("POST")
	api.HandleFunc("/unblock", authed(UnblockUserHandler)).Methods("POST")
	r.HandleFunc("/ws/{username}", WebsocketHandler).Methods("GET")
	srv := httptest.NewServer(r)
	t.Cleanup(srv.Close)
	return srv
}

func liveUsers(t *testing.T) {
	t.Helper()
	for id, name := range map[int]string{liveLeo: "leo", liveMaya: "maya", liveSam: "sam"} {
		if _, err := db.Exec(`INSERT INTO users (id, username, password) VALUES ($1, $2, 'x')
			ON CONFLICT (id) DO NOTHING`, id, name); err != nil {
			t.Fatalf("seed %s: %v", name, err)
		}
	}
}

var liveNames = map[int]string{liveLeo: "leo", liveMaya: "maya", liveSam: "sam"}

// liveClient is one phone's live connection. Everything that arrives is
// read straight away into [events]: gorilla's connection cannot be read
// again after a read has timed out, so the tests wait on the channel
// instead of on the connection.
type liveClient struct {
	conn   *websocket.Conn
	events chan map[string]interface{}
}

// dial opens [id]'s live connection, the way the app does.
func dial(t *testing.T, srv *httptest.Server, id int) *liveClient {
	t.Helper()
	name := liveNames[id]
	url := "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws/" + name +
		"?token=" + testToken(t, strconv.Itoa(id), name)
	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatalf("%s could not connect: %v", name, err)
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
	// The server registers the connection after the handshake; wait for it
	// so nothing sent in the next instant is lost.
	deadline := time.Now().Add(2 * time.Second)
	for !IsUserOnline(name) {
		if time.Now().After(deadline) {
			t.Fatalf("%s never showed as online", name)
		}
		time.Sleep(5 * time.Millisecond)
	}
	return c
}

// next waits for the first event of [kind], skipping others.
func next(t *testing.T, c *liveClient, kind string, within time.Duration) map[string]interface{} {
	t.Helper()
	timeout := time.After(within)
	for {
		select {
		case ev, ok := <-c.events:
			if !ok {
				t.Fatalf("the connection closed before a %q arrived", kind)
			}
			if ev["type"] == kind {
				return ev
			}
		case <-timeout:
			t.Fatalf("no %q arrived within %v", kind, within)
		}
	}
}

// none checks that nothing of [kind] arrives for a while.
func none(t *testing.T, c *liveClient, kind string, wait time.Duration) {
	t.Helper()
	timeout := time.After(wait)
	for {
		select {
		case ev, ok := <-c.events:
			if !ok {
				return
			}
			if ev["type"] == kind {
				t.Fatalf("a %q arrived and should not have: %v", kind, ev)
			}
		case <-timeout:
			return
		}
	}
}

func send(t *testing.T, c *liveClient, ev map[string]interface{}) {
	t.Helper()
	if err := c.conn.WriteJSON(ev); err != nil {
		t.Fatalf("send %v: %v", ev["type"], err)
	}
}

// authedDo makes a request as [id] with a real token.
func authedDo(t *testing.T, srv *httptest.Server, id int, method, path, body string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(method, srv.URL+path, strings.NewReader(body))
	if err != nil || req == nil {
		t.Fatalf("%s %s: %v", method, path, err)
		return nil
	}
	req.Header.Set("Authorization", "Bearer "+testToken(t, strconv.Itoa(id), liveNames[id]))
	req.Header.Set("Content-Type", "application/json")
	res, err := http.DefaultClient.Do(req)
	if err != nil || res == nil {
		t.Fatalf("%s %s: %v", method, path, err)
		return nil
	}
	t.Cleanup(func() { res.Body.Close() })
	return res
}

func message(t *testing.T, from, to int, text string) int {
	t.Helper()
	id, err := SendChatMessage(from, to, text, nil, ChatMedia{})
	if err != nil {
		t.Fatalf("send message: %v", err)
	}
	return id
}

func readState(t *testing.T, id int) (bool, string) {
	t.Helper()
	var read bool
	var status string
	if err := db.QueryRow(`SELECT is_read, COALESCE(status, '') FROM chat_messages WHERE id = $1`,
		id).Scan(&read, &status); err != nil {
		t.Fatalf("read message %d: %v", id, err)
	}
	return read, status
}

func liveSetup(t *testing.T) *httptest.Server {
	t.Helper()
	t.Cleanup(withDB(t))
	resetRedis(t)
	freshLimits(t)
	liveUsers(t)
	// Notification settings are not tied to users, so emptying users does
	// not empty them: start every live test on the defaults.
	if _, err := db.Exec(`DELETE FROM notification_prefs WHERE user_id IN ($1, $2, $3)`,
		strconv.Itoa(liveLeo), strconv.Itoa(liveMaya), strconv.Itoa(liveSam)); err != nil {
		t.Fatal(err)
	}
	return liveServer(t)
}

// freshLimits starts the typing and calling limits full. They are kept in
// the process, not in the database, so one test's calls would otherwise
// count against the next.
func freshLimits(t *testing.T) {
	t.Helper()
	actionLimitersMu.Lock()
	delete(actionLimiters, "call")
	delete(actionLimiters, "typing")
	actionLimitersMu.Unlock()
}

func TestLive_OpeningTheChatTellsTheSenderAtOnce(t *testing.T) {
	srv := liveSetup(t)
	id := message(t, liveLeo, liveMaya, "up for a battle?")
	leo := dial(t, srv, liveLeo)

	// Maya opens the chat.
	res := authedDo(t, srv, liveMaya, "GET",
		"/api/v1/chat/messages/"+strconv.Itoa(liveMaya)+"/"+strconv.Itoa(liveLeo), "")
	if res.StatusCode != 200 {
		t.Fatalf("opening the chat = %d", res.StatusCode)
	}

	ev := next(t, leo, "chat_read", 3*time.Second)
	if ev["readerId"] != strconv.Itoa(liveMaya) || ev["readerUsername"] != "maya" {
		t.Fatalf("chat_read names the wrong reader: %v", ev)
	}
	if read, status := readState(t, id); !read || status != "read" {
		t.Fatalf("message is read=%v status=%q, want read", read, status)
	}
}

func TestLive_MarkReadTellsTheSender(t *testing.T) {
	srv := liveSetup(t)
	message(t, liveLeo, liveMaya, "hi")
	leo := dial(t, srv, liveLeo)

	authedDo(t, srv, liveMaya, "POST", "/api/v1/chat/read",
		`{"senderId":"`+strconv.Itoa(liveLeo)+`"}`)
	next(t, leo, "chat_read", 3*time.Second)
}

func TestLive_NothingNewToReadSaysNothing(t *testing.T) {
	srv := liveSetup(t)
	leo := dial(t, srv, liveLeo)
	// Maya has nothing from Leo: no signal, so Leo's phone is not woken for
	// nothing.
	authedDo(t, srv, liveMaya, "POST", "/api/v1/chat/read",
		`{"senderId":"`+strconv.Itoa(liveLeo)+`"}`)
	none(t, leo, "chat_read", 400*time.Millisecond)

	// The same connection does hear a real one.
	message(t, liveLeo, liveMaya, "now there is")
	authedDo(t, srv, liveMaya, "POST", "/api/v1/chat/read",
		`{"senderId":"`+strconv.Itoa(liveLeo)+`"}`)
	next(t, leo, "chat_read", 3*time.Second)
}

func TestLive_SendingToSomebodyOnlineSaysDelivered(t *testing.T) {
	srv := liveSetup(t)
	leo := dial(t, srv, liveLeo)
	maya := dial(t, srv, liveMaya)

	res := authedDo(t, srv, liveLeo, "POST", "/api/v1/chat/send",
		`{"receiverId":"`+strconv.Itoa(liveMaya)+`","message":"hello"}`)
	var sent ChatMessage
	if err := json.NewDecoder(res.Body).Decode(&sent); err != nil || sent.ID == "" {
		t.Fatalf("send answered %d without an id: %v", res.StatusCode, err)
	}

	got := next(t, maya, "chat", 3*time.Second)
	if got["messageId"] != sent.ID {
		t.Fatalf("maya got %v, want message %s", got, sent.ID)
	}
	ev := next(t, leo, "chat_delivered", 3*time.Second)
	ids, _ := ev["messageIds"].([]interface{})
	if len(ids) != 1 || ids[0] != sent.ID || ev["receiverId"] != strconv.Itoa(liveMaya) {
		t.Fatalf("chat_delivered = %v, want message %s to maya", ev, sent.ID)
	}
	id, _ := strconv.Atoi(sent.ID)
	if _, status := readState(t, id); status != "delivered" {
		t.Fatalf("status = %q, want delivered", status)
	}
}

func TestLive_SeenNeverGoesBackToDelivered(t *testing.T) {
	t.Cleanup(withDB(t))
	resetRedis(t)
	liveUsers(t)
	id := message(t, liveLeo, liveMaya, "hi")
	MarkMessagesRead(liveLeo, liveMaya)
	markDeliveredAndTell(id, "leo", strconv.Itoa(liveMaya))
	if read, status := readState(t, id); !read || status != "read" {
		t.Fatalf("a late Delivered undid Seen: read=%v status=%q", read, status)
	}

	// Messages read before status was kept say read but status "sent".
	// Those are read too, and stay that way.
	old := message(t, liveLeo, liveMaya, "from before")
	if _, err := db.Exec(`UPDATE chat_messages SET is_read = TRUE WHERE id = $1`, old); err != nil {
		t.Fatal(err)
	}
	markDeliveredAndTell(old, "leo", strconv.Itoa(liveMaya))
	deliverWaiting(liveMaya)
	if _, status := readState(t, old); status != "sent" {
		t.Fatalf("an already-read message was marked %q", status)
	}
}

func TestLive_ComingOnlineDeliversWhatWaited(t *testing.T) {
	srv := liveSetup(t)
	a := message(t, liveLeo, liveMaya, "one")
	b := message(t, liveLeo, liveMaya, "two")
	c := message(t, liveSam, liveMaya, "from sam")
	leo := dial(t, srv, liveLeo)
	sam := dial(t, srv, liveSam)

	dial(t, srv, liveMaya)

	ev := next(t, leo, "chat_delivered", 3*time.Second)
	ids, _ := ev["messageIds"].([]interface{})
	got := map[string]bool{}
	for _, id := range ids {
		got[id.(string)] = true
	}
	if len(ids) != 2 || !got[strconv.Itoa(a)] || !got[strconv.Itoa(b)] {
		t.Fatalf("leo was told %v, want his two messages %d and %d", ids, a, b)
	}
	ev = next(t, sam, "chat_delivered", 3*time.Second)
	if ids, _ := ev["messageIds"].([]interface{}); len(ids) != 1 || ids[0] != strconv.Itoa(c) {
		t.Fatalf("sam was told %v, want only %d", ev, c)
	}
}

func TestLive_TypingReachesTheOtherPersonFromTheRealSender(t *testing.T) {
	srv := liveSetup(t)
	leo := dial(t, srv, liveLeo)
	maya := dial(t, srv, liveMaya)

	// Leo claims to be Sam. The server says who it is really from.
	send(t, leo, map[string]interface{}{
		"type": "typing", "to": strconv.Itoa(liveMaya), "typing": true,
		"from": strconv.Itoa(liveSam), "fromUsername": "sam",
	})
	ev := next(t, maya, "typing", 3*time.Second)
	if ev["from"] != strconv.Itoa(liveLeo) || ev["fromUsername"] != "leo" || ev["typing"] != true {
		t.Fatalf("typing = %v, want from leo", ev)
	}
	if _, ok := ev["to"]; ok {
		t.Fatalf("typing still carries its address: %v", ev)
	}
}

func TestLive_APhoneCannotSendAChatMessageDownTheSocket(t *testing.T) {
	srv := liveSetup(t)
	leo := dial(t, srv, liveLeo)
	maya := dial(t, srv, liveMaya)
	send(t, leo, map[string]interface{}{
		"type": "chat", "to": strconv.Itoa(liveMaya), "message": "not through here",
	})
	none(t, maya, "chat", 400*time.Millisecond)
}

func TestLive_BlockedPeopleGetNothingFromEachOther(t *testing.T) {
	srv := liveSetup(t)
	if _, err := db.Exec(`INSERT INTO user_blocks (blocker_id, blocked_id) VALUES ($1, $2)`,
		liveMaya, liveLeo); err != nil {
		t.Fatalf("block: %v", err)
	}
	leo := dial(t, srv, liveLeo)
	maya := dial(t, srv, liveMaya)
	sam := dial(t, srv, liveSam)

	send(t, leo, map[string]interface{}{"type": "typing", "to": strconv.Itoa(liveMaya), "typing": true})
	send(t, leo, map[string]interface{}{"type": "call_offer", "to": strconv.Itoa(liveMaya), "callId": "c1"})
	none(t, maya, "typing", 300*time.Millisecond)
	ev := next(t, leo, "call_unavailable", 3*time.Second)
	if ev["callId"] != "c1" {
		t.Fatalf("call_unavailable = %v, want call c1", ev)
	}

	// Leo is not cut off from everybody.
	send(t, leo, map[string]interface{}{"type": "typing", "to": strconv.Itoa(liveSam), "typing": true})
	next(t, sam, "typing", 3*time.Second)
}

func missedCalls(t *testing.T, userID int) []string {
	t.Helper()
	rows, err := db.Query(`SELECT body FROM user_notifications WHERE user_id = $1 AND kind = $2`, userID, noteMissedCall)
	if err != nil {
		t.Fatalf("read notifications: %v", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var b string
		if err := rows.Scan(&b); err != nil {
			t.Fatalf("scan: %v", err)
		}
		out = append(out, b)
	}
	return out
}

// settled waits until everything [from] has sent so far has been handled
// completely. A phone's messages are handled one at a time, in order, and a
// missed call is written down after the call_end is passed on; so a typing
// signal sent last arrives only once the ones before it are finished.
func settled(t *testing.T, from, to *liveClient) {
	t.Helper()
	send(t, from, map[string]interface{}{"type": "typing", "to": strconv.Itoa(liveMaya), "typing": false})
	next(t, to, "typing", 3*time.Second)
}

func TestCall_TheWholeIntroductionPassesBothWays(t *testing.T) {
	srv := liveSetup(t)
	leo := dial(t, srv, liveLeo)
	maya := dial(t, srv, liveMaya)
	to := func(id int) string { return strconv.Itoa(id) }

	send(t, leo, map[string]interface{}{"type": "call_offer", "to": to(liveMaya), "callId": "k1",
		"video": true, "sdp": "v=0 offer", "sdpType": "offer"})
	ev := next(t, maya, "call_offer", 3*time.Second)
	if ev["from"] != to(liveLeo) || ev["sdp"] != "v=0 offer" || ev["video"] != true {
		t.Fatalf("offer arrived as %v", ev)
	}
	send(t, maya, map[string]interface{}{"type": "call_ringing", "to": to(liveLeo), "callId": "k1"})
	next(t, leo, "call_ringing", 3*time.Second)
	send(t, maya, map[string]interface{}{"type": "call_answer", "to": to(liveLeo), "callId": "k1",
		"sdp": "v=0 answer", "sdpType": "answer"})
	if ev := next(t, leo, "call_answer", 3*time.Second); ev["sdp"] != "v=0 answer" {
		t.Fatalf("answer arrived as %v", ev)
	}
	// A burst of network addresses, the way a phone sends them.
	for i := 0; i < 20; i++ {
		send(t, leo, map[string]interface{}{"type": "call_ice", "to": to(liveMaya), "callId": "k1",
			"candidate": "candidate:" + strconv.Itoa(i), "sdpMid": "0", "sdpMLineIndex": 0})
	}
	for i := 0; i < 20; i++ {
		if ev := next(t, maya, "call_ice", 3*time.Second); ev["candidate"] != "candidate:"+strconv.Itoa(i) {
			t.Fatalf("address %d arrived as %v", i, ev)
		}
	}
	send(t, maya, map[string]interface{}{"type": "call_end", "to": to(liveLeo), "callId": "k1", "reason": "hangup"})
	next(t, leo, "call_end", 3*time.Second)
	// Leo ending an answered call is not a missed call either.
	send(t, leo, map[string]interface{}{"type": "call_end", "to": to(liveMaya), "callId": "k1", "reason": "hangup"})
	settled(t, leo, maya)
	if got := missedCalls(t, liveMaya); len(got) != 0 {
		t.Fatalf("an answered call left a missed call: %v", got)
	}
}

func TestCall_RingingSomebodyOfflineLeavesAMissedCall(t *testing.T) {
	srv := liveSetup(t)
	leo := dial(t, srv, liveLeo)
	send(t, leo, map[string]interface{}{"type": "call_offer", "to": strconv.Itoa(liveMaya),
		"callId": "k2", "video": true})
	if ev := next(t, leo, "call_unavailable", 3*time.Second); ev["callId"] != "k2" {
		t.Fatalf("call_unavailable = %v", ev)
	}
	got := missedCalls(t, liveMaya)
	if len(got) != 1 || got[0] != "tried to video call you." {
		t.Fatalf("maya's missed calls = %v, want one video call", got)
	}
}

func TestCall_HangingUpBeforeAnAnswerIsAMissedCallOnce(t *testing.T) {
	srv := liveSetup(t)
	leo := dial(t, srv, liveLeo)
	maya := dial(t, srv, liveMaya)
	send(t, leo, map[string]interface{}{"type": "call_offer", "to": strconv.Itoa(liveMaya), "callId": "k3"})
	next(t, maya, "call_offer", 3*time.Second)

	// "missed" for a call Leo never placed is not believed.
	send(t, leo, map[string]interface{}{"type": "call_end", "to": strconv.Itoa(liveMaya), "callId": "nope", "reason": "missed"})
	next(t, maya, "call_end", 3*time.Second)
	// The real one is, once.
	send(t, leo, map[string]interface{}{"type": "call_end", "to": strconv.Itoa(liveMaya), "callId": "k3", "reason": "missed"})
	send(t, leo, map[string]interface{}{"type": "call_end", "to": strconv.Itoa(liveMaya), "callId": "k3", "reason": "missed"})
	settled(t, leo, maya)
	if got := missedCalls(t, liveMaya); len(got) != 1 || got[0] != "tried to call you." {
		t.Fatalf("maya's missed calls = %v, want exactly one", got)
	}
}

func TestCall_RingingOverAndOverStops(t *testing.T) {
	srv := liveSetup(t)
	leo := dial(t, srv, liveLeo)
	maya := dial(t, srv, liveMaya)
	rang := 0
	for i := 0; i < 8; i++ {
		send(t, leo, map[string]interface{}{"type": "call_offer", "to": strconv.Itoa(liveMaya),
			"callId": "r" + strconv.Itoa(i)})
	}
	timeout := time.After(1500 * time.Millisecond)
wait:
	for {
		select {
		case ev := <-maya.events:
			if ev["type"] == "call_offer" {
				rang++
			}
		case <-timeout:
			break wait
		}
	}
	if rang != actionLimitTable["call"].burst {
		t.Fatalf("maya's phone rang %d times for 8 calls in a row, want %d",
			rang, actionLimitTable["call"].burst)
	}
}

func TestCall_IceServersOfferTheRelayWhenSet(t *testing.T) {
	srv := liveSetup(t)
	get := func() map[string]interface{} {
		res := authedDo(t, srv, liveLeo, "GET", "/api/v1/calls/ice", "")
		var out map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&out); err != nil {
			t.Fatalf("decode: %v", err)
		}
		return out
	}
	t.Setenv("TURN_URLS", "")
	if got := get(); got["relay"] != false || len(got["iceServers"].([]interface{})) != 1 {
		t.Fatalf("without a relay = %v", got)
	}
	t.Setenv("TURN_URLS", "turn:relay.example:3478, turns:relay.example:5349")
	t.Setenv("TURN_USERNAME", "u")
	t.Setenv("TURN_CREDENTIAL", "p")
	got := get()
	servers := got["iceServers"].([]interface{})
	if got["relay"] != true || len(servers) != 2 {
		t.Fatalf("with a relay = %v", got)
	}
	turn := servers[1].(map[string]interface{})
	if urls := turn["urls"].([]interface{}); len(urls) != 2 || urls[1] != "turns:relay.example:5349" ||
		turn["username"] != "u" || turn["credential"] != "p" {
		t.Fatalf("relay entry = %v", turn)
	}
}

// The test server above is wired by hand, so it cannot notice main.go
// losing a route. This reads main.go itself, comments stripped.
func TestLive_MainWiresTheRoutes(t *testing.T) {
	src, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	code := regexp.MustCompile(`(?m)^\s*//.*$`).ReplaceAllString(string(src), "")
	for _, want := range []string{
		`"/calls/ice", authed(CallIceServersHandler)`,
		`"/ws/{username}", WebsocketHandler`,
		`"/chat/read", authed(MarkReadHandler)`,
		`"/chat/forward", authed(ForwardMessageHandler)`,
		`"/chat/clear", authed(ClearChatHandler)`,
	} {
		if !strings.Contains(code, want) {
			t.Errorf("main.go does not wire %s", want)
		}
	}
}

// The chat list says how far your last message got, so it can say "Seen"
// without opening the chat.
func TestLive_TheChatListKnowsHowFarYourLastMessageGot(t *testing.T) {
	t.Cleanup(withDB(t))
	liveUsers(t)
	status := func() (bool, string) {
		for _, c := range GetConversations(liveLeo) {
			if c.UserID == strconv.Itoa(liveMaya) {
				return c.LastFromMe, c.LastStatus
			}
		}
		t.Fatal("leo has no chat with maya")
		return false, ""
	}
	id := message(t, liveLeo, liveMaya, "hi")
	if mine, st := status(); !mine || st != "sent" {
		t.Fatalf("after sending: mine=%v status=%q, want mine and sent", mine, st)
	}
	markDeliveredAndTell(id, "leo", strconv.Itoa(liveMaya))
	if _, st := status(); st != "delivered" {
		t.Fatalf("after delivery: %q", st)
	}
	MarkMessagesRead(liveLeo, liveMaya)
	if _, st := status(); st != "read" {
		t.Fatalf("after reading: %q", st)
	}
	message(t, liveMaya, liveLeo, "hey back")
	if mine, _ := status(); mine {
		t.Fatal("maya's reply shows as leo's own")
	}
}
