package main

// "Open to battles" — through the real handlers against real Postgres.
//
// Each test posts, answers or flips the switch the way the app does, then
// reads back what the next person would see, so a wire that stops carrying
// the setting goes red here rather than on somebody's phone.

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/mux"
)

// postNormal posts as the app does with the switch off, and returns the
// status and the post.
func postNormal(t *testing.T, prefix, caption, visibility string) (int, Challenge, string) {
	t.Helper()
	body, _ := json.Marshal(map[string]any{
		"videoUrl":      "http://127.0.0.1:1/clip.mp4",
		"prefix":        prefix,
		"subject":       caption,
		"visibility":    visibility,
		"openToBattles": false,
	})
	r := withAuth(httptest.NewRequest("POST", "/api/v1/challenges", bytes.NewReader(body)),
		strconv.Itoa(nfCreator), nfName(nfCreator))
	w := httptest.NewRecorder()
	CreateChallengeHandler(w, r)
	var c Challenge
	_ = json.Unmarshal(w.Body.Bytes(), &c)
	return w.Code, c, w.Body.String()
}

// flipBattles is the owner's switch, through the handler.
func flipBattles(t *testing.T, as int, cid string, open bool) int {
	t.Helper()
	body, _ := json.Marshal(map[string]any{"open": open})
	r := withAuth(httptest.NewRequest("PATCH", "/api/v1/challenges/"+cid+"/battles",
		bytes.NewReader(body)), strconv.Itoa(as), nfName(as))
	r = mux.SetURLVars(r, map[string]string{"id": cid})
	w := httptest.NewRecorder()
	SetOpenToBattlesHandler(w, r)
	return w.Code
}

// tryAnswer is answer() without failing: the status and what was said.
func tryAnswer(t *testing.T, cid string) (int, string) {
	t.Helper()
	body, _ := json.Marshal(map[string]any{
		"challengeId": cid, "videoUrl": "http://127.0.0.1:1/answer-" +
			strconv.FormatInt(time.Now().UnixNano(), 10) + ".mp4",
		"durationMs": 5000,
	})
	r := withAuth(httptest.NewRequest("POST", "/api/v1/challenges/accept", bytes.NewReader(body)),
		strconv.Itoa(nfAnswerer), nfName(nfAnswerer))
	w := httptest.NewRecorder()
	AcceptChallengeHandler(w, r)
	return w.Code, w.Body.String()
}

func TestOpenToBattles_ANormalPostIsSavedClosedWithItsCaption(t *testing.T) {
	defer withDB(t)()
	resetRedis(t)
	resetActionLimiters(t)
	seedNotifyPeople(t)

	caption := "Sunset at the beach with my dog, best evening"
	code, c, raw := postNormal(t, "", caption, "arena")
	if code != 201 {
		t.Fatalf("a normal post was refused: %d %s", code, raw)
	}
	if !c.ClosedToBattles || c.Prefix != "" || c.Subject != caption {
		t.Fatalf("it came back as %+v", c)
	}
	var open bool
	if err := db.QueryRow(`SELECT open_to_battles FROM challenges WHERE id = $1`, c.ID).
		Scan(&open); err != nil || open {
		t.Fatalf("saved open=%v (%v)", open, err)
	}

	// A caption is not a challenge subject: it does not feed the subject
	// suggestions other people are offered.
	time.Sleep(200 * time.Millisecond)
	subjectUsageMu.Lock()
	used := subjectUsageCache[strings.ToLower(caption)]
	subjectUsageMu.Unlock()
	if used != 0 {
		t.Errorf("the caption was counted as a challenge subject %d times", used)
	}

	// And an ordinary challenge, from an app that never sends the switch,
	// is open as every post always was.
	open = false
	cid := postChallenge(t, "arena", nil)
	if err := db.QueryRow(`SELECT open_to_battles FROM challenges WHERE id = $1`, cid).
		Scan(&open); err != nil || !open {
		t.Fatalf("a challenge with no switch saved open=%v (%v)", open, err)
	}
	// Its subject is counted, as before.
	ch, _ := GetChallengeByID(cid)
	time.Sleep(200 * time.Millisecond)
	subjectUsageMu.Lock()
	used = subjectUsageCache[strings.ToLower(ch.Subject)]
	subjectUsageMu.Unlock()
	if used == 0 {
		t.Error("a challenge's subject was not counted for suggestions")
	}
}

func TestOpenToBattles_WhatAPostNeedsToSay(t *testing.T) {
	defer withDB(t)()
	resetRedis(t)
	resetActionLimiters(t)
	seedNotifyPeople(t)

	if code, _, raw := postNormal(t, "", "", "arena"); code != 400 ||
		!strings.Contains(raw, "caption") {
		t.Errorf("a normal post with no caption: %d %s", code, raw)
	}
	resetActionLimiters(t)
	if code, _, raw := postNormal(t, "", strings.Repeat("a", maxCaptionChars+1), "arena"); code != 400 ||
		!strings.Contains(raw, "caption can be at most") {
		t.Errorf("a caption one too long: %d %s", code, raw)
	}
	resetActionLimiters(t)
	// Longer than a challenge's subject may be, as a caption may.
	if code, _, raw := postNormal(t, "", strings.Repeat("a", maxCaptionChars), "arena"); code != 201 {
		t.Errorf("a caption at the limit: %d %s", code, raw)
	}
	resetActionLimiters(t)
	// A post open to battles is still a question with both halves.
	body, _ := json.Marshal(map[string]any{
		"videoUrl": "http://127.0.0.1:1/clip.mp4", "prefix": "", "subject": "pranks",
		"visibility": "arena", "openToBattles": true,
	})
	r := withAuth(httptest.NewRequest("POST", "/api/v1/challenges", bytes.NewReader(body)),
		strconv.Itoa(nfCreator), nfName(nfCreator))
	w := httptest.NewRecorder()
	CreateChallengeHandler(w, r)
	if w.Code != 400 {
		t.Errorf("a battle with no opener: %d %s", w.Code, w.Body.String())
	}
}

func TestOpenToBattles_NobodyCanAnswerAClosedPost_UntilTheOwnerOpensIt(t *testing.T) {
	defer withDB(t)()
	resetRedis(t)
	resetActionLimiters(t)
	seedNotifyPeople(t)

	_, c, _ := postNormal(t, "", "My cat ignoring me", "arena")
	if code, msg := tryAnswer(t, c.ID); code != 400 || !strings.Contains(msg, notOpenToBattles) {
		t.Fatalf("answering a closed post: %d %s", code, msg)
	}
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM challenge_responses WHERE challenge_id = $1`,
		c.ID).Scan(&n); err != nil || n != 0 {
		t.Fatalf("an answer was saved anyway: %d (%v)", n, err)
	}

	// Somebody else cannot open it.
	if code := flipBattles(t, nfStranger, c.ID, true); code != 403 {
		t.Fatalf("a stranger opening it got %d", code)
	}
	// The owner can, and then it can be answered.
	if code := flipBattles(t, nfCreator, c.ID, true); code != 200 {
		t.Fatalf("the owner opening it got %d", code)
	}
	if code, msg := tryAnswer(t, c.ID); code != 201 {
		t.Fatalf("answering once opened: %d %s", code, msg)
	}
	waitInbox(t, nfCreator, 1)

	// Closed again: the battle carries on, but nobody new may join.
	if code := flipBattles(t, nfCreator, c.ID, false); code != 200 {
		t.Fatalf("the owner closing it got %d", code)
	}
	var status string
	if err := db.QueryRow(`SELECT status FROM challenges WHERE id = $1`, c.ID).Scan(&status); err != nil ||
		status != "active" {
		t.Fatalf("the battle stopped: %q (%v)", status, err)
	}
	if code := flipBattles(t, nfCreator, "999999", false); code != 404 {
		t.Errorf("a post that is not there: %d", code)
	}
}

func TestOpenToBattles_EveryListSaysSo(t *testing.T) {
	defer withDB(t)()
	resetRedis(t)
	resetActionLimiters(t)
	seedNotifyPeople(t)

	_, closed, _ := postNormal(t, "", "Morning run by the river", "arena")
	resetActionLimiters(t)
	open := postChallenge(t, "arena", nil)

	// The feed reads posts with queries of its own that never select the
	// column; whatever hands posts to the app marks them.
	feedCopies := []*Challenge{{ID: closed.ID}, {ID: open}}
	markViewerState(strconv.Itoa(nfStranger), feedCopies)
	if !feedCopies[0].ClosedToBattles || feedCopies[1].ClosedToBattles {
		t.Fatalf("marked closed=%v open=%v", feedCopies[0].ClosedToBattles,
			feedCopies[1].ClosedToBattles)
	}

	// The shared reader carries it too.
	got, found := GetChallengeByID(closed.ID)
	if !found || !got.ClosedToBattles {
		t.Fatalf("read back: %+v %v", got, found)
	}

	// The battle page says nobody may answer, and why.
	req := withAuth(httptest.NewRequest("GET", "/api/v1/challenges/"+closed.ID, nil),
		strconv.Itoa(nfStranger), nfName(nfStranger))
	req = mux.SetURLVars(req, map[string]string{"id": closed.ID})
	rec := httptest.NewRecorder()
	GetChallengeDetailHandler(rec, req)
	var detail struct {
		Challenge     Challenge `json:"challenge"`
		CanAccept     bool      `json:"canAccept"`
		LeagueMessage string    `json:"leagueMessage"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &detail); err != nil {
		t.Fatal(err)
	}
	if detail.CanAccept || detail.LeagueMessage != notOpenToBattles ||
		!detail.Challenge.ClosedToBattles {
		t.Fatalf("the battle page: %+v", detail)
	}

	// Not one of the owner's open challenges, in the count or the tab.
	ctx := context.Background()
	summary, err := loadBattleSummary(ctx, nfCreator, false)
	if err != nil {
		t.Fatal(err)
	}
	if summary.Counts["open"] != 1 {
		t.Errorf("open challenges counted %d, want just the real one", summary.Counts["open"])
	}
	cards, err := loadBattleCards(ctx, db, nfCreator, "open", false, 50, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, card := range cards {
		if card.ChallengeID == closed.ID {
			t.Errorf("the normal post is in the Open tab: %+v", card)
		}
	}
	if len(cards) != 1 {
		t.Errorf("the Open tab has %d, want the one challenge", len(cards))
	}

	// Not in the arena's list of live contests.
	for _, c := range GetArenaChallenges() {
		if c.ID == closed.ID {
			t.Error("a normal post nobody can answer is listed as a live contest")
		}
	}
}

func TestOpenToBattles_FriendsAreToldItIsAPostNotAChallenge(t *testing.T) {
	defer withDB(t)()
	resetRedis(t)
	resetActionLimiters(t)
	seedNotifyPeople(t)

	_, c, _ := postNormal(t, "", "Family dinner", "friends")
	waitFriendsTold(t, c.ID, 2)
	items := waitInbox(t, nfFriendA, 1)
	if len(items) != 1 || !strings.Contains(items[0].Text, "shared a post with friends") ||
		strings.Contains(items[0].Text, "challenge") || strings.Contains(items[0].Text, "?") {
		t.Fatalf("friend was told: %+v", items)
	}
}

func TestOpenToBattles_ACaptionIsNotAQuestion(t *testing.T) {
	if got := challengeTitle("", "Family dinner"); got != "Family dinner" {
		t.Errorf("a caption became %q", got)
	}
	if got := challengeTitle("Who can", "dance"); got != "Who can dance?" {
		t.Errorf("a challenge became %q", got)
	}
}

func TestOpenToBattles_ANormalPostHasNothingToMatch(t *testing.T) {
	defer withDB(t)()
	resetRedis(t)
	resetActionLimiters(t)
	seedNotifyPeople(t)

	_, c, _ := postNormal(t, "", "Rainy day", "arena")
	cid, _ := strconv.Atoi(c.ID)
	if q := jobQuestion("challenges", cid); q != "" {
		t.Errorf("the worker would check it against %q", q)
	}
	status, msg, err := reportOffTopic(context.Background(), nfStranger, cid, 0)
	if err != nil || status != 409 || !strings.Contains(msg, "normal post") {
		t.Errorf("reporting it: %d %q %v", status, msg, err)
	}

	// A challenge is still checked against its question.
	resetActionLimiters(t)
	open, _ := strconv.Atoi(postChallenge(t, "arena", nil))
	if q := jobQuestion("challenges", open); !strings.HasPrefix(q, "Who can juggle") {
		t.Errorf("a challenge's question: %q", q)
	}
}
