package main

// Messages buzz the phone; they never land on the notifications page. And
// the chat list puts the newest conversation on top.
//
// Real server, real database, with only the push service itself faked —
// the fake records exactly what would have gone to Google's push service
// (Firebase), phone by phone.

import (
	"strconv"
	"sync"
	"testing"
	"time"
)

type pushed struct {
	row    OutboxRow
	tokens []DeviceTokenRow
}

// recordingPusher stands in for Firebase. Tokens in [dead] answer "this
// phone is gone", the way a phone that deleted the app does.
type recordingPusher struct {
	mu   sync.Mutex
	sent []pushed
	dead map[string]bool
}

func (*recordingPusher) Name() string { return "recording" }

func (p *recordingPusher) Send(n OutboxRow, tokens []DeviceTokenRow) []SendResult {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.sent = append(p.sent, pushed{row: n, tokens: tokens})
	out := make([]SendResult, 0, len(tokens))
	for _, t := range tokens {
		if p.dead[t.Token] {
			out = append(out, SendResult{Token: t.Token, Dead: true, Reason: "unregistered"})
		} else {
			out = append(out, SendResult{Token: t.Token, OK: true})
		}
	}
	return out
}

// waitFor waits for the [n]th push (1-based) and returns it.
func (p *recordingPusher) waitFor(t *testing.T, n int) pushed {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		p.mu.Lock()
		if len(p.sent) >= n {
			got := p.sent[n-1]
			p.mu.Unlock()
			return got
		}
		p.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("push number %d never went out", n)
	return pushed{}
}

func (p *recordingPusher) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.sent)
}

func withPusher(t *testing.T) *recordingPusher {
	t.Helper()
	before := getCurrentSender()
	p := &recordingPusher{dead: map[string]bool{}}
	// Phones are not tied to the users table, so clearing the users
	// between tests leaves the last test's phones behind.
	if _, err := db.Exec(`DELETE FROM device_tokens`); err != nil {
		t.Fatal(err)
	}
	setCurrentSender(p)
	t.Cleanup(func() { setCurrentSender(before) })
	return p
}

func phone(t *testing.T, userID int, token string) {
	t.Helper()
	if err := registerDeviceToken(strconv.Itoa(userID), token, "fcm"); err != nil {
		t.Fatalf("register phone: %v", err)
	}
}

func notesFor(t *testing.T, userID int) int {
	t.Helper()
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM user_notifications WHERE user_id = $1`,
		userID).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestPush_AMessageBuzzesTheirPhoneAndNotTheNotificationsPage(t *testing.T) {
	srv := liveSetup(t)
	p := withPusher(t)
	phone(t, liveMaya, "maya-phone")

	authedDo(t, srv, liveLeo, "POST", "/api/v1/chat/send",
		`{"receiverId":"`+strconv.Itoa(liveMaya)+`","message":"up for a battle? 🔥"}`)
	got := p.waitFor(t, 1)

	if got.row.UserID != strconv.Itoa(liveMaya) || len(got.tokens) != 1 ||
		got.tokens[0].Token != "maya-phone" {
		t.Fatalf("went to %s on %v, want maya's phone", got.row.UserID, got.tokens)
	}
	if got.row.Title != "leo" || got.row.Body != "up for a battle? 🔥" {
		t.Fatalf("push reads %q / %q", got.row.Title, got.row.Body)
	}
	if got.row.Tag != "chat_"+strconv.Itoa(liveLeo) || got.row.Channel != "messages" {
		t.Fatalf("tag %q channel %q", got.row.Tag, got.row.Channel)
	}
	if got.row.Data["type"] != "chat" || got.row.Data["senderId"] != strconv.Itoa(liveLeo) ||
		got.row.Data["senderUsername"] != "leo" || got.row.Data["messageId"] == "" {
		t.Fatalf("data %v cannot open the chat", got.row.Data)
	}
	if n := notesFor(t, liveMaya); n != 0 {
		t.Fatalf("the message also made %d notification(s) on maya's page", n)
	}

	// A second one says how many are waiting.
	authedDo(t, srv, liveLeo, "POST", "/api/v1/chat/send",
		`{"receiverId":"`+strconv.Itoa(liveMaya)+`","message":"hello?"}`)
	if got := p.waitFor(t, 2); got.row.Title != "leo (2 messages)" {
		t.Fatalf("second push title %q", got.row.Title)
	}
}

func TestPush_AForwardedMessageBuzzesToo(t *testing.T) {
	srv := liveSetup(t)
	p := withPusher(t)
	phone(t, liveSam, "sam-phone")
	id := message(t, liveMaya, liveLeo, "look at this")
	authedDo(t, srv, liveLeo, "POST", "/api/v1/chat/forward",
		`{"messageId":"`+strconv.Itoa(id)+`","receiverId":"`+strconv.Itoa(liveSam)+`"}`)
	got := p.waitFor(t, 1)
	if got.row.UserID != strconv.Itoa(liveSam) || got.row.Body != "look at this" {
		t.Fatalf("forward pushed %+v", got.row)
	}
}

// Forwarding is only for messages from your own chats, and a block stops
// it as it stops sending.
func TestForward_OnlyYourOwnMessagesAndNeverPastABlock(t *testing.T) {
	srv := liveSetup(t)
	withPusher(t)
	theirs := message(t, liveMaya, liveSam, "a private message between maya and sam")
	res := authedDo(t, srv, liveLeo, "POST", "/api/v1/chat/forward",
		`{"messageId":"`+strconv.Itoa(theirs)+`","receiverId":"`+strconv.Itoa(liveLeo)+`"}`)
	if res.StatusCode != 404 {
		t.Fatalf("forwarding somebody else's message = %d, want 404", res.StatusCode)
	}
	mine := message(t, liveMaya, liveLeo, "for leo")
	if _, err := db.Exec(`INSERT INTO user_blocks (blocker_id, blocked_id) VALUES ($1, $2)`,
		liveSam, liveLeo); err != nil {
		t.Fatal(err)
	}
	res = authedDo(t, srv, liveLeo, "POST", "/api/v1/chat/forward",
		`{"messageId":"`+strconv.Itoa(mine)+`","receiverId":"`+strconv.Itoa(liveSam)+`"}`)
	if res.StatusCode != 403 {
		t.Fatalf("forwarding to somebody who blocked you = %d, want 403", res.StatusCode)
	}
	// Your own message, to somebody who has not blocked you, goes.
	res = authedDo(t, srv, liveLeo, "POST", "/api/v1/chat/forward",
		`{"messageId":"`+strconv.Itoa(mine)+`","receiverId":"`+strconv.Itoa(liveMaya)+`"}`)
	if res.StatusCode != 200 {
		t.Fatalf("a normal forward = %d, want 200", res.StatusCode)
	}
}

func TestPush_NoPhoneNoPush(t *testing.T) {
	srv := liveSetup(t)
	p := withPusher(t)
	authedDo(t, srv, liveLeo, "POST", "/api/v1/chat/send",
		`{"receiverId":"`+strconv.Itoa(liveMaya)+`","message":"hi"}`)
	// The same request to somebody with a phone shows the path does run.
	phone(t, liveSam, "sam-phone")
	authedDo(t, srv, liveLeo, "POST", "/api/v1/chat/send",
		`{"receiverId":"`+strconv.Itoa(liveSam)+`","message":"hi"}`)
	got := p.waitFor(t, 1)
	time.Sleep(200 * time.Millisecond)
	if p.count() != 1 || got.row.UserID != strconv.Itoa(liveSam) {
		t.Fatalf("%d pushes, first to %s; want one, to sam", p.count(), got.row.UserID)
	}
}

func TestPush_APhoneThatIsGoneIsForgotten(t *testing.T) {
	srv := liveSetup(t)
	p := withPusher(t)
	phone(t, liveMaya, "old-phone")
	phone(t, liveMaya, "new-phone")
	p.dead["old-phone"] = true
	authedDo(t, srv, liveLeo, "POST", "/api/v1/chat/send",
		`{"receiverId":"`+strconv.Itoa(liveMaya)+`","message":"hi"}`)
	p.waitFor(t, 1)
	time.Sleep(200 * time.Millisecond)
	var active bool
	if err := db.QueryRow(`SELECT active FROM device_tokens WHERE token = 'old-phone'`).Scan(&active); err != nil {
		t.Fatal(err)
	}
	if active {
		t.Fatal("a phone that has gone is still tried")
	}
	left := activeTokensForUser(strconv.Itoa(liveMaya))
	if len(left) != 1 || left[0].Token != "new-phone" {
		t.Fatalf("maya's phones now %v, want only the new one", left)
	}
}

func TestPush_AMissedCallBuzzesTheirPhone(t *testing.T) {
	srv := liveSetup(t)
	p := withPusher(t)
	phone(t, liveMaya, "maya-phone")
	leo := dial(t, srv, liveLeo)
	send(t, leo, map[string]interface{}{"type": "call_offer", "to": strconv.Itoa(liveMaya),
		"callId": "m1", "video": true})
	next(t, leo, "call_unavailable", 3*time.Second)
	got := p.waitFor(t, 1)
	if got.row.Title != "Missed video call" || got.row.Body != "leo tried to call you" ||
		got.row.Data["type"] != "missed_call" || got.row.Data["senderId"] != strconv.Itoa(liveLeo) {
		t.Fatalf("missed call pushed %+v", got.row)
	}
}

func TestPush_ThePhoneIsToldToGroupByChat(t *testing.T) {
	m := fcmPayload(OutboxRow{
		Title: "leo", Body: "hi", Tag: "chat_9", Channel: "messages",
		Data: map[string]string{"type": "chat", "senderId": "9"},
	}, "tok")["message"].(map[string]interface{})
	android := m["android"].(map[string]interface{})
	note := android["notification"].(map[string]interface{})
	if note["tag"] != "chat_9" || note["channel_id"] != "messages" || android["collapse_key"] != "chat_9" {
		t.Fatalf("android part %v", android)
	}
	apns := m["apns"].(map[string]interface{})
	if apns["headers"].(map[string]string)["apns-collapse-id"] != "chat_9" {
		t.Fatalf("iPhone part %v", apns)
	}
	data := m["data"].(map[string]string)
	if data["type"] != "chat" || data["senderId"] != "9" || data["outboxId"] != "0" {
		t.Fatalf("data %v", data)
	}
	// The app's own nudges, with no tag, stay as they were.
	plain := fcmPayload(OutboxRow{Title: "t", Body: "b"}, "tok")["message"].(map[string]interface{})
	if _, ok := plain["apns"]; ok {
		t.Fatal("a push without a tag grew an iPhone grouping")
	}
}

func TestPush_EmojiAreNeverCutInHalf(t *testing.T) {
	long := ""
	for i := 0; i < 100; i++ {
		long += "🔥"
	}
	got := truncateText(long, pushBodyLimit)
	for _, r := range got {
		if r == '�' {
			t.Fatalf("cut an emoji in half: %q", got)
		}
	}
	if len(got) > pushBodyLimit+3 {
		t.Fatalf("%d bytes, over the limit", len(got))
	}
}

// The chat list: newest conversation on top, the way every chat app does,
// and a new message moves its chat up at once.
func TestChatList_NewestConversationOnTop(t *testing.T) {
	t.Cleanup(withDB(t))
	liveUsers(t)
	at := func(from, to int, text string, ago time.Duration) {
		t.Helper()
		if _, err := db.Exec(`INSERT INTO chat_messages (sender_id, receiver_id, message, created_at)
			VALUES ($1, $2, $3, NOW() - $4::interval)`, from, to, text,
			strconv.Itoa(int(ago/time.Millisecond))+" milliseconds"); err != nil {
			t.Fatal(err)
		}
	}
	names := func() []string {
		var out []string
		for _, c := range GetConversations(liveLeo) {
			out = append(out, c.Username)
		}
		return out
	}
	at(liveLeo, liveMaya, "old", 2*time.Hour)
	at(liveSam, liveLeo, "newer", time.Hour)
	if got := names(); len(got) != 2 || got[0] != "sam" || got[1] != "maya" {
		t.Fatalf("order %v, want sam (newer) then maya", got)
	}
	// Maya writes: her chat goes to the top.
	at(liveMaya, liveLeo, "back on top", 0)
	if got := names(); got[0] != "maya" {
		t.Fatalf("order %v, want maya on top after her message", got)
	}
	// Two messages within the same second still come in order.
	at(liveSam, liveLeo, "a moment later", -300*time.Millisecond)
	if got := names(); got[0] != "sam" {
		t.Fatalf("order %v: a message 0.3s later did not go on top", got)
	}
}
