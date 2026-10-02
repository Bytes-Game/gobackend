package main

// Notification settings: each switch the app shows is one the server knows,
// a save changes only the switches it names, and "Messages and calls" off
// means a message no longer buzzes the phone. Against a real database.

import (
	"encoding/json"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
)

// freshPrefs forgets the test people's notification settings. The table is
// not tied to users, so emptying users between tests leaves them behind —
// and a test that assumes the defaults would read the last run's.
func freshPrefs(t *testing.T) {
	t.Helper()
	if _, err := db.Exec(`DELETE FROM notification_prefs WHERE user_id IN ($1, $2, $3)`,
		strconv.Itoa(liveLeo), strconv.Itoa(liveMaya), strconv.Itoa(liveSam)); err != nil {
		t.Fatal(err)
	}
}

func savePrefs(t *testing.T, id int, body string) NotificationPrefs {
	t.Helper()
	sid := strconv.Itoa(id)
	r := withUser(httptest.NewRequest("POST", "/api/v1/notifications/prefs",
		strings.NewReader(body)), sid, liveNames[id])
	w := httptest.NewRecorder()
	HandleSetNotificationPrefs(w, r)
	if w.Code != 200 {
		t.Fatalf("saving %s = %d: %s", body, w.Code, w.Body.String())
	}
	r = withUser(httptest.NewRequest("GET", "/api/v1/notifications/prefs", nil), sid, liveNames[id])
	w = httptest.NewRecorder()
	HandleGetNotificationPrefs(w, r)
	var p NotificationPrefs
	if err := json.Unmarshal(w.Body.Bytes(), &p); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestNotificationPrefs_ASaveChangesOnlyWhatItNames(t *testing.T) {
	_ = liveSetup(t)
	freshPrefs(t)
	p := savePrefs(t, liveMaya, `{"youWillLove": false}`)
	if p.YouWillLove {
		t.Fatal("the switch that was turned off is still on")
	}
	if !p.Messages || !p.FriendResponse || !p.EndingSoon || !p.InactiveWinback {
		t.Fatalf("a save of one switch turned others off: %+v", p)
	}
	if p.QuietHoursStart != 22 || p.QuietHoursEnd != 8 || p.MaxPerDay != 4 {
		t.Fatalf("a save of one switch reset quiet hours or the daily cap: %+v", p)
	}
	// What the app used to send — switch names the server did not know —
	// changes nothing at all now, rather than turning everything off.
	p = savePrefs(t, liveMaya, `{"userId":"x","likes":true,"comments":false}`)
	if !p.Messages || !p.FriendResponse || p.YouWillLove {
		t.Fatalf("unknown names changed the settings: %+v", p)
	}
	p = savePrefs(t, liveMaya, `{"quietHoursStart": 0, "quietHoursEnd": 0}`)
	if p.QuietHoursStart != 0 || p.QuietHoursEnd != 0 {
		t.Fatalf("quiet hours could not be switched off: %+v", p)
	}
}

func TestNotificationPrefs_MessagesOffMeansNoPush(t *testing.T) {
	srv := liveSetup(t)
	freshPrefs(t)
	p := withPusher(t)
	phone(t, liveMaya, "maya-phone")
	savePrefs(t, liveMaya, `{"messages": false}`)
	authedDo(t, srv, liveLeo, "POST", "/api/v1/chat/send",
		`{"receiverId":"`+strconv.Itoa(liveMaya)+`","message":"hello"}`)
	pushMissedCall(strconv.Itoa(liveMaya), strconv.Itoa(liveLeo), "leo", false)
	time.Sleep(300 * time.Millisecond)
	if n := p.count(); n != 0 {
		t.Fatalf("messages and calls are off, but %d push(es) went", n)
	}
	// On again: it buzzes.
	savePrefs(t, liveMaya, `{"messages": true}`)
	authedDo(t, srv, liveLeo, "POST", "/api/v1/chat/send",
		`{"receiverId":"`+strconv.Itoa(liveMaya)+`","message":"now?"}`)
	if got := p.waitFor(t, 1); got.row.Body != "now?" {
		t.Fatalf("push reads %q", got.row.Body)
	}
}
