package main

// Muting a chat, and the number on the Messages button (chat_mute.go) —
// through the real routes, against real Postgres, with only the push
// service faked.

import (
	"encoding/json"
	"net/http/httptest"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"
)

// setMuted sets [who]'s chat with [other] to [muted]: the status, and what
// the answer says.
func setMuted(t *testing.T, srv *httptest.Server, who int, body string) (int, map[string]any) {
	t.Helper()
	res := authedDo(t, srv, who, "POST", "/api/v1/chat/mute", body)
	var got map[string]any
	_ = json.NewDecoder(res.Body).Decode(&got)
	return res.StatusCode, got
}

func muteBody(other int, muted bool) string {
	return `{"otherUserId":"` + strconv.Itoa(other) + `","muted":` + strconv.FormatBool(muted) + `}`
}

// say sends a message from [from] to [to] the way the app does.
func say(t *testing.T, srv *httptest.Server, from, to int, text string) {
	t.Helper()
	res := authedDo(t, srv, from, "POST", "/api/v1/chat/send",
		`{"receiverId":"`+strconv.Itoa(to)+`","message":"`+text+`"}`)
	if res.StatusCode != 200 {
		t.Fatalf("%s could not send to %s: %d", liveNames[from], liveNames[to], res.StatusCode)
	}
}

// unreadPeople is the number on [who]'s Messages button.
func unreadPeople(t *testing.T, srv *httptest.Server, who int) int {
	t.Helper()
	res := authedDo(t, srv, who, "GET", "/api/v1/chat/unread", "")
	var got struct {
		People *int `json:"people"`
	}
	if err := json.NewDecoder(res.Body).Decode(&got); err != nil || res.StatusCode != 200 || got.People == nil {
		t.Fatalf("the unread number answered %d: %v", res.StatusCode, err)
	}
	return *got.People
}

// unreadBecomes waits for [who]'s number to be [want]: opening a chat
// marks it read a moment later, not in the same request.
func unreadBecomes(t *testing.T, srv *httptest.Server, who, want int, why string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	n := unreadPeople(t, srv, who)
	for n != want && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
		n = unreadPeople(t, srv, who)
	}
	if n != want {
		t.Fatalf("%s: the number is %d, want %d", why, n, want)
	}
}

// chatList is [who]'s chat list, by the other person's name.
func chatList(t *testing.T, srv *httptest.Server, who int) map[string]Conversation {
	t.Helper()
	res := authedDo(t, srv, who, "GET", "/api/v1/chat/conversations/"+strconv.Itoa(who), "")
	var list []Conversation
	if err := json.NewDecoder(res.Body).Decode(&list); err != nil {
		t.Fatalf("the chat list could not be read: %v", err)
	}
	out := map[string]Conversation{}
	for _, c := range list {
		out[c.Username] = c
	}
	return out
}

// pushesFrom is how many pushes so far were about a message from [sender].
func pushesFrom(p *recordingPusher, sender int) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := 0
	for _, s := range p.sent {
		if s.row.Data["senderId"] == strconv.Itoa(sender) {
			n++
		}
	}
	return n
}

func TestChatMute_NoNotificationFromAMutedChat_AndBackOnceUnmuted(t *testing.T) {
	srv := liveSetup(t)
	p := withPusher(t)
	phone(t, liveMaya, "maya-phone")
	phone(t, liveLeo, "leo-phone")

	if code, got := setMuted(t, srv, liveMaya, muteBody(liveLeo, true)); code != 200 || got["muted"] != true {
		t.Fatalf("muting answered %d %v", code, got)
	}
	say(t, srv, liveLeo, liveMaya, "you there?")
	// Sam is not muted: his message buzzes, and by then leo's would have.
	say(t, srv, liveSam, liveMaya, "hey")
	p.waitFor(t, 1)
	time.Sleep(200 * time.Millisecond)
	if n := pushesFrom(p, liveLeo); n != 0 {
		t.Errorf("%d notifications went for a muted chat", n)
	}
	if n := pushesFrom(p, liveSam); n != 1 {
		t.Errorf("%d notifications for a chat that is not muted, want 1", n)
	}
	// Muting is one person's own: leo still hears from maya.
	say(t, srv, liveMaya, liveLeo, "yes!")
	p.waitFor(t, 2)
	if n := pushesFrom(p, liveMaya); n != 1 {
		t.Errorf("maya muting leo silenced maya's messages to leo too (%d pushes)", n)
	}

	// Unmuted: it buzzes again.
	if code, got := setMuted(t, srv, liveMaya, muteBody(liveLeo, false)); code != 200 || got["muted"] != false {
		t.Fatalf("unmuting answered %d %v", code, got)
	}
	say(t, srv, liveLeo, liveMaya, "now?")
	p.waitFor(t, 3)
	if n := pushesFrom(p, liveLeo); n != 1 {
		t.Errorf("after unmuting, %d notifications from leo, want 1", n)
	}
}

func TestChatMute_TheLiveMessageSaysItIsFromAMutedChat(t *testing.T) {
	srv := liveSetup(t)
	maya := dial(t, srv, liveMaya)
	setMuted(t, srv, liveMaya, muteBody(liveLeo, true))

	say(t, srv, liveLeo, liveMaya, "muted one")
	ev := next(t, maya, "chat", 3*time.Second)
	if ev["message"] != "muted one" || ev["muted"] != true {
		t.Fatalf("the live message from a muted chat: %v", ev)
	}
	say(t, srv, liveSam, liveMaya, "loud one")
	ev = next(t, maya, "chat", 3*time.Second)
	if ev["message"] != "loud one" || ev["muted"] != false {
		t.Fatalf("the live message from a chat that is not muted: %v", ev)
	}
}

func TestChatMute_TheChatListSaysWhichAreMuted(t *testing.T) {
	srv := liveSetup(t)
	say(t, srv, liveLeo, liveMaya, "one")
	say(t, srv, liveSam, liveMaya, "two")
	setMuted(t, srv, liveMaya, muteBody(liveLeo, true))
	list := chatList(t, srv, liveMaya)
	if !list["leo"].Muted || list["sam"].Muted {
		t.Fatalf("leo muted %v, sam muted %v; want only leo", list["leo"].Muted, list["sam"].Muted)
	}
	// Leo's own list does not say maya is muted: it is maya's choice.
	if chatList(t, srv, liveLeo)["maya"].Muted {
		t.Error("leo's list shows his chat with maya as muted")
	}
}

func TestChatMute_OnlyYourOwnChatWithARealAccount(t *testing.T) {
	srv := liveSetup(t)
	for _, body := range []string{
		muteBody(liveMaya, true),                          // yourself
		`{"otherUserId":"` + strconv.Itoa(liveLeo) + `"}`, // muted or not, unsaid
		`{"muted":true}`,                                  // nobody
	} {
		if code, got := setMuted(t, srv, liveMaya, body); code != 400 {
			t.Errorf("%s answered %d %v, want 400", body, code, got)
		}
	}
	if code, got := setMuted(t, srv, liveMaya, muteBody(999999, true)); code != 404 {
		t.Errorf("muting an account that does not exist answered %d %v, want 404", code, got)
	}
	// Muting twice is still muted; unmuting what is not muted is fine.
	setMuted(t, srv, liveMaya, muteBody(liveLeo, true))
	if code, _ := setMuted(t, srv, liveMaya, muteBody(liveLeo, true)); code != 200 {
		t.Errorf("muting twice answered %d", code)
	}
	if code, _ := setMuted(t, srv, liveMaya, muteBody(liveSam, false)); code != 200 {
		t.Errorf("unmuting a chat that was not muted answered %d", code)
	}
	// An account deleted for good takes its mutes with it.
	setMuted(t, srv, liveMaya, muteBody(liveSam, true))
	mustExec(t, `DELETE FROM users WHERE id = $1`, liveSam)
	if n := countRows(t, `SELECT COUNT(*) FROM chat_mutes WHERE other_id = $1`, liveSam); n != 0 {
		t.Errorf("%d mutes kept for an account deleted for good", n)
	}
}

func TestChatUnread_CountsPeopleNotMessages(t *testing.T) {
	srv := liveSetup(t)
	if n := unreadPeople(t, srv, liveMaya); n != 0 {
		t.Fatalf("with no messages the number is %d", n)
	}
	say(t, srv, liveLeo, liveMaya, "one")
	say(t, srv, liveLeo, liveMaya, "two")
	say(t, srv, liveLeo, liveMaya, "three")
	say(t, srv, liveSam, liveMaya, "hi")
	say(t, srv, liveMaya, liveLeo, "my own message does not count")
	if n := unreadPeople(t, srv, liveMaya); n != 2 {
		t.Fatalf("two people wrote four messages; the number is %d, want 2", n)
	}

	// Reading leo's chat: one person left.
	authedDo(t, srv, liveMaya, "GET",
		"/api/v1/chat/messages/"+strconv.Itoa(liveMaya)+"/"+strconv.Itoa(liveLeo), "")
	unreadBecomes(t, srv, liveMaya, 1, "after reading leo's chat")
	// A muted chat is not counted; unmuted, it is again.
	setMuted(t, srv, liveMaya, muteBody(liveSam, true))
	if n := unreadPeople(t, srv, liveMaya); n != 0 {
		t.Fatalf("with sam muted the number is %d, want 0", n)
	}
	setMuted(t, srv, liveMaya, muteBody(liveSam, false))
	if n := unreadPeople(t, srv, liveMaya); n != 1 {
		t.Fatalf("with sam unmuted the number is %d, want 1", n)
	}
	// An account waiting to be deleted is off the chat list, and off the
	// number too.
	mustExec(t, `UPDATE users SET deletion_requested_at = NOW() WHERE id = $1`, liveSam)
	if n := unreadPeople(t, srv, liveMaya); n != 0 {
		t.Fatalf("with sam waiting to be deleted the number is %d, want 0", n)
	}
	mustExec(t, `UPDATE users SET deletion_requested_at = NULL WHERE id = $1`, liveSam)
	// A deleted chat: what came before is gone from the number.
	authedDo(t, srv, liveMaya, "POST", "/api/v1/chat/clear",
		`{"otherUserId":"`+strconv.Itoa(liveSam)+`"}`)
	if n := unreadPeople(t, srv, liveMaya); n != 0 {
		t.Fatalf("after deleting sam's chat the number is %d, want 0", n)
	}
	// And a new message after that counts.
	time.Sleep(10 * time.Millisecond)
	say(t, srv, liveSam, liveMaya, "again")
	if n := unreadPeople(t, srv, liveMaya); n != 1 {
		t.Fatalf("a new message after deleting the chat: the number is %d, want 1", n)
	}
}

// The number is what the chat list shows: the chats with unread messages
// that are not muted.
func TestChatUnread_IsWhatTheChatListShows(t *testing.T) {
	srv := liveSetup(t)
	say(t, srv, liveLeo, liveMaya, "one")
	say(t, srv, liveSam, liveMaya, "two")
	setMuted(t, srv, liveMaya, muteBody(liveSam, true))
	want := 0
	for _, c := range chatList(t, srv, liveMaya) {
		if c.UnreadCount > 0 && !c.Muted {
			want++
		}
	}
	if n := unreadPeople(t, srv, liveMaya); n != want || want != 1 {
		t.Fatalf("the number is %d; the chat list shows %d unread chats not muted (want 1)", n, want)
	}
}

func TestChatMute_MainWiresTheRoutes(t *testing.T) {
	src, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	code := regexp.MustCompile(`(?m)^\s*//.*$`).ReplaceAllString(string(src), "")
	for _, want := range []string{
		`"/chat/mute", authed(MuteChatHandler)`,
		`"/chat/unread", authed(UnreadChatsHandler)`,
	} {
		if !strings.Contains(code, want) {
			t.Errorf("main.go does not wire %s", want)
		}
	}
}
