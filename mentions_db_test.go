package main

// Mentioning people in a comment — against a real database, through the
// same route the app posts comments to.

import (
	"net/http/httptest"
	"reflect"
	"strconv"
	"testing"
	"time"
)

func TestMentionedNames(t *testing.T) {
	for _, c := range []struct {
		text string
		want []string
	}{
		{"hi @maya and @leo.", []string{"maya", "leo"}},
		{"@MAYA twice @maya", []string{"maya"}},
		{"write to sam@leo.com", nil},
		{"@ab is too short, @sam_1 is fine", []string{"sam_1"}},
		{"(@maya)", []string{"maya"}},
		{"no mentions here", nil},
	} {
		if got := mentionedNames(c.text); !reflect.DeepEqual(got, c.want) {
			t.Errorf("mentionedNames(%q) = %v, want %v", c.text, got, c.want)
		}
	}
	many := ""
	for i := 0; i < 15; i++ {
		many += " @user" + strconv.Itoa(100+i)
	}
	if got := mentionedNames(many); len(got) != mentionMax {
		t.Errorf("a comment naming 15 people mentions %d, want %d", len(got), mentionMax)
	}
}

// commentAs posts [text] as [from] on challenge [cid].
func commentAs(t *testing.T, srv *httptest.Server, from, cid int, text string) {
	t.Helper()
	actionLimitersMu.Lock()
	delete(actionLimiters, "comment")
	actionLimitersMu.Unlock()
	res := authedDo(t, srv, from, "POST", "/api/v1/challenges/comments",
		`{"challengeId":"`+strconv.Itoa(cid)+`","text":`+strconv.Quote(text)+`}`)
	if res.StatusCode != 200 {
		t.Fatalf("commenting = %d", res.StatusCode)
	}
}

// mentionsOf counts [who]'s "mention" notifications in their list, and
// their queued mention pushes.
func mentionsOf(t *testing.T, who int) (listed, pushed int) {
	t.Helper()
	if err := db.QueryRow(`SELECT COUNT(*) FROM user_notifications
		WHERE user_id = $1 AND kind = 'mention'`, who).Scan(&listed); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM notification_outbox
		WHERE user_id = $1 AND trigger_kind = 'mention'`, strconv.Itoa(who)).Scan(&pushed); err != nil {
		t.Fatal(err)
	}
	return listed, pushed
}

func mentionSetup(t *testing.T) *httptest.Server {
	t.Helper()
	srv := liveSetup(t)
	if _, err := db.Exec(`DELETE FROM user_notifications WHERE user_id IN ($1, $2, $3)`,
		liveLeo, liveMaya, liveSam); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`DELETE FROM notification_outbox WHERE user_id IN ($1, $2, $3)`,
		strconv.Itoa(liveLeo), strconv.Itoa(liveMaya), strconv.Itoa(liveSam)); err != nil {
		t.Fatal(err)
	}
	return srv
}

func TestMention_TheNamedPersonIsToldInTheirListAndOnTheirPhone(t *testing.T) {
	srv := mentionSetup(t)
	cid, _ := shareVideo(t, liveSam, 0, "arena")
	maya := dial(t, srv, liveMaya)
	commentAs(t, srv, liveLeo, cid, "@Maya look at this one")

	listed, pushed := mentionsOf(t, liveMaya)
	if listed != 1 || pushed != 1 {
		t.Fatalf("maya was mentioned: %d in her list, %d pushes; want 1 and 1", listed, pushed)
	}
	var actor, challenge int
	var body string
	if err := db.QueryRow(`SELECT actor_id, challenge_id, body FROM user_notifications
		WHERE user_id = $1 AND kind = 'mention'`, liveMaya).Scan(&actor, &challenge, &body); err != nil {
		t.Fatal(err)
	}
	if actor != liveLeo || challenge != cid ||
		body != "mentioned you in a comment: “@Maya look at this one”" {
		t.Fatalf("the note is %d / %d / %q", actor, challenge, body)
	}
	// Live, if she is online.
	ev := next(t, maya, "mention", 3*time.Second)
	if ev["actorUsername"] != "leo" || ev["challengeId"] != strconv.Itoa(cid) {
		t.Fatalf("live note = %v", ev)
	}
	// Nobody else is told.
	if l, p := mentionsOf(t, liveSam); l != 0 || p != 0 {
		t.Fatalf("sam, not mentioned, was told: %d %d", l, p)
	}
}

func TestMention_NotYourselfNotStrangersNamesNotNobody(t *testing.T) {
	srv := mentionSetup(t)
	cid, _ := shareVideo(t, liveSam, 0, "arena")
	commentAs(t, srv, liveLeo, cid, "@leo talking to myself, and @nobody_here")
	if l, p := mentionsOf(t, liveLeo); l != 0 || p != 0 {
		t.Fatalf("leo was told he mentioned himself: %d in his list, %d pushes", l, p)
	}
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM user_notifications WHERE kind = 'mention'`).
		Scan(&n); err != nil || n != 0 {
		t.Fatalf("%d mention notes (%v), want none", n, err)
	}
}

func TestMention_NotAcrossABlockEitherWay(t *testing.T) {
	srv := mentionSetup(t)
	cid, _ := shareVideo(t, liveSam, 0, "arena")
	blockAs(t, srv, liveMaya, liveLeo)
	commentAs(t, srv, liveLeo, cid, "hey @maya")
	if l, p := mentionsOf(t, liveMaya); l != 0 || p != 0 {
		t.Fatalf("maya blocked leo, and was told he mentioned her: %d %d", l, p)
	}
	commentAs(t, srv, liveMaya, cid, "and @leo")
	if l, p := mentionsOf(t, liveLeo); l != 0 || p != 0 {
		t.Fatalf("leo is blocked by maya, and was told she mentioned him: %d %d", l, p)
	}
}

func TestMention_NotAboutAVideoTheyMayNotWatch(t *testing.T) {
	srv := mentionSetup(t)
	// Sam's friends-only video. Leo follows sam; maya does not.
	if _, err := db.Exec(`INSERT INTO follows (follower_id, following_id) VALUES ($1, $2)`,
		liveLeo, liveSam); err != nil {
		t.Fatal(err)
	}
	cid, _ := shareVideo(t, liveSam, 0, "friends")
	commentAs(t, srv, liveSam, cid, "@maya @leo")
	if l, p := mentionsOf(t, liveMaya); l != 0 || p != 0 {
		t.Fatalf("maya cannot watch it and was told about it: %d %d", l, p)
	}
	if l, _ := mentionsOf(t, liveLeo); l != 1 {
		t.Fatalf("leo, who can watch it, has %d mention notes, want 1", l)
	}
}

func TestMention_TheFriendsSwitchOffStopsThePushNotTheNote(t *testing.T) {
	srv := mentionSetup(t)
	cid, _ := shareVideo(t, liveSam, 0, "arena")
	p := defaultNotificationPrefs(strconv.Itoa(liveMaya))
	p.FriendResponse = false
	if err := saveNotificationPrefs(p); err != nil {
		t.Fatal(err)
	}
	commentAs(t, srv, liveLeo, cid, "@maya")
	if l, pushed := mentionsOf(t, liveMaya); l != 1 || pushed != 0 {
		t.Fatalf("with friends' activity off: %d in the list, %d pushes; want 1 and 0", l, pushed)
	}
}
