package main

// Notifications, friends-only challenges, and who voted — through the real
// handlers against real Postgres.
//
// Each test posts, accepts or votes the way the app does and then reads
// back what the person on the other end would see, so a handler that stops
// telling anyone — or tells the wrong people — goes red here.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/gorilla/mux"
)

const (
	nfCreator  = 9401
	nfFriendA  = 9402
	nfFriendB  = 9403
	nfStranger = 9404
	nfAnswerer = 9405
	nfVoter    = 9406
)

func nfName(id int) string { return fmt.Sprintf("nf_user_%d", id) }

func seedNotifyPeople(t *testing.T) {
	t.Helper()
	for _, id := range []int{nfCreator, nfFriendA, nfFriendB, nfStranger, nfAnswerer, nfVoter} {
		if _, err := db.Exec(`INSERT INTO users (id, username, password) VALUES ($1, $2, 'x')
		                      ON CONFLICT (id) DO NOTHING`, id, nfName(id)); err != nil {
			t.Fatalf("seed user: %v", err)
		}
	}
	for _, f := range []int{nfFriendA, nfFriendB} {
		if _, err := db.Exec(`INSERT INTO follows (follower_id, following_id) VALUES ($1, $2)
		                      ON CONFLICT DO NOTHING`, f, nfCreator); err != nil {
			t.Fatalf("seed follow: %v", err)
		}
	}
	if _, err := db.Exec(`DELETE FROM user_notifications WHERE user_id = ANY($1)`,
		"{9401,9402,9403,9404,9405,9406}"); err != nil {
		t.Fatalf("clear notifications: %v", err)
	}
}

// postChallenge posts as the app does and returns the new challenge's id.
func postChallenge(t *testing.T, visibility string, visibleTo []string) string {
	t.Helper()
	body, _ := json.Marshal(map[string]any{
		// Nothing listens here, so the size check cannot measure it and
		// lets it through at once.
		"videoUrl":   "http://127.0.0.1:1/clip.mp4",
		"prefix":     "Who can",
		"subject":    fmt.Sprintf("juggle %d", time.Now().UnixNano()%100000),
		"visibility": visibility,
		"visibleTo":  visibleTo,
	})
	r := withAuth(httptest.NewRequest("POST", "/api/v1/challenges", bytes.NewReader(body)),
		strconv.Itoa(nfCreator), nfName(nfCreator))
	w := httptest.NewRecorder()
	CreateChallengeHandler(w, r)
	if w.Code != 201 {
		t.Fatalf("post answered %d: %s", w.Code, w.Body.String())
	}
	var c Challenge
	if err := json.Unmarshal(w.Body.Bytes(), &c); err != nil {
		t.Fatal(err)
	}
	return c.ID
}

// inbox is what [user]'s notifications page would list.
func inbox(t *testing.T, user int) (items []InboxItem, unread int) {
	t.Helper()
	r := withAuth(httptest.NewRequest("GET", "/api/v1/notifications", nil),
		strconv.Itoa(user), nfName(user))
	w := httptest.NewRecorder()
	ListNotificationsHandler(w, r)
	if w.Code != 200 {
		t.Fatalf("list answered %d: %s", w.Code, w.Body.String())
	}
	var body struct {
		Items  []InboxItem `json:"items"`
		Unread int         `json:"unread"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	return body.Items, body.Unread
}

// waitInbox waits for the notifications a handler sends in the background.
func waitInbox(t *testing.T, user, want int) []InboxItem {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		items, _ := inbox(t, user)
		if len(items) >= want || time.Now().After(deadline) {
			return items
		}
		time.Sleep(25 * time.Millisecond)
	}
}

// waitFriendsTold waits until the background telling of a friends-only
// post has queued [want] pushes — the last thing it does for each friend.
//
// A test that posts one and finishes sooner leaves that telling running
// into the next test, after this test's database is gone. It then crashes
// the whole run (it did, in CI), and which test it takes down is luck.
func waitFriendsTold(t *testing.T, cid string, want int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		var n int
		if err := db.QueryRow(`SELECT COUNT(*) FROM notification_outbox
		                        WHERE dedupe_key = $1`, "friend_challenge:"+cid).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n >= want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("after 3s only %d of %d friends were told about challenge %s", n, want, cid)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func TestFriendsOnly_EveryFriendIsToldAndNobodyElse(t *testing.T) {
	defer withDB(t)()
	resetRedis(t)
	resetActionLimiters(t)
	seedNotifyPeople(t)
	cid := postChallenge(t, "friends", nil)

	for _, f := range []int{nfFriendA, nfFriendB} {
		items := waitInbox(t, f, 1)
		if len(items) != 1 {
			t.Fatalf("friend %d got %d notifications, want 1", f, len(items))
		}
		it := items[0]
		if it.Type != "friend_challenge" || it.ChallengeID != cid ||
			it.ActorUsername != nfName(nfCreator) {
			t.Errorf("friend %d was told the wrong thing: %+v", f, it)
		}
		if it.ChallengeTitle == "" || it.Read {
			t.Errorf("the notification has no challenge named, or starts read: %+v", it)
		}
	}
	time.Sleep(200 * time.Millisecond)
	if items, _ := inbox(t, nfStranger); len(items) != 0 {
		t.Fatalf("someone who does not follow the creator was told: %+v", items)
	}
	// And a push went into the queue for each friend.
	var pushes int
	if err := db.QueryRow(`SELECT COUNT(*) FROM notification_outbox
	                        WHERE dedupe_key = $1`, "friend_challenge:"+cid).Scan(&pushes); err != nil {
		t.Fatal(err)
	}
	if pushes != 2 {
		t.Errorf("%d pushes queued, want one per friend (2)", pushes)
	}
}

func TestFriendsOnly_PickedFriendsOnlyAndStrangersCannotBePicked(t *testing.T) {
	defer withDB(t)()
	resetRedis(t)
	resetActionLimiters(t)
	seedNotifyPeople(t)
	// A stranger's id slipped into the list: dropped, and never told.
	cid := postChallenge(t, "friends",
		[]string{strconv.Itoa(nfFriendA), strconv.Itoa(nfStranger)})

	items := waitInbox(t, nfFriendA, 1)
	if len(items) != 1 || items[0].Text == "" {
		t.Fatalf("the picked friend was not told: %+v", items)
	}
	if want := "challenged you"; !bytes.Contains([]byte(items[0].Text), []byte(want)) {
		t.Errorf("a picked friend should read %q: %q", want, items[0].Text)
	}
	time.Sleep(200 * time.Millisecond)
	for _, other := range []int{nfFriendB, nfStranger} {
		if got, _ := inbox(t, other); len(got) != 0 {
			t.Errorf("user %d was not picked but was told: %+v", other, got)
		}
	}
	var saved []int
	rows, err := db.Query(`SELECT user_id FROM challenge_visible_to
	                        WHERE challenge_id = $1 ORDER BY user_id`, cid)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var id int
		_ = rows.Scan(&id)
		saved = append(saved, id)
	}
	rows.Close()
	if len(saved) != 1 || saved[0] != nfFriendA {
		t.Fatalf("who may see it: %v, want only %d", saved, nfFriendA)
	}

	// The following feed agrees: friend A gets it, friend B does not.
	see := func(user int) bool {
		for _, it := range sourceFollowGraphWindowed(strconv.Itoa(user), 50, "30 days") {
			if it.Challenge != nil && it.Challenge.ID == cid {
				return true
			}
		}
		return false
	}
	if !see(nfFriendA) {
		t.Error("the picked friend does not get it in their following feed")
	}
	if see(nfFriendB) {
		t.Error("a friend who was not picked gets it in their following feed")
	}
}

func TestFriendsOnly_OnlyStrangersPickedMeansTheCreatorAloneNotEveryone(t *testing.T) {
	defer withDB(t)()
	resetRedis(t)
	resetActionLimiters(t)
	seedNotifyPeople(t)
	cid := postChallenge(t, "friends", []string{strconv.Itoa(nfStranger)})
	var n, creatorRow int
	if err := db.QueryRow(`SELECT COUNT(*), COUNT(*) FILTER (WHERE user_id = $2)
	                        FROM challenge_visible_to WHERE challenge_id = $1`,
		cid, nfCreator).Scan(&n, &creatorRow); err != nil {
		t.Fatal(err)
	}
	if n != 1 || creatorRow != 1 {
		t.Fatalf("an all-invalid pick became %d rows (creator %d): an empty "+
			"list would have meant every follower", n, creatorRow)
	}
	time.Sleep(300 * time.Millisecond)
	for _, f := range []int{nfFriendA, nfFriendB, nfStranger} {
		if got, _ := inbox(t, f); len(got) != 0 {
			t.Errorf("user %d was told about a challenge not for them: %+v", f, got)
		}
	}
}

func TestPublicPost_TellsNobody(t *testing.T) {
	defer withDB(t)()
	resetRedis(t)
	resetActionLimiters(t)
	seedNotifyPeople(t)
	postChallenge(t, "arena", nil)
	time.Sleep(300 * time.Millisecond)
	if got, _ := inbox(t, nfFriendA); len(got) != 0 {
		t.Fatalf("a public post pinged a follower: %+v", got)
	}
}

// answer accepts [cid] as the answerer, through the handler.
func answer(t *testing.T, cid string) string {
	t.Helper()
	body, _ := json.Marshal(map[string]any{
		"challengeId": cid, "videoUrl": "http://127.0.0.1:1/answer.mp4",
		"durationMs": 5000,
	})
	r := withAuth(httptest.NewRequest("POST", "/api/v1/challenges/accept", bytes.NewReader(body)),
		strconv.Itoa(nfAnswerer), nfName(nfAnswerer))
	w := httptest.NewRecorder()
	AcceptChallengeHandler(w, r)
	if w.Code != 201 {
		t.Fatalf("accept answered %d: %s", w.Code, w.Body.String())
	}
	var resp ChallengeResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	return resp.ID
}

func TestAccepting_BothPlayersAreToldDifferentThings(t *testing.T) {
	defer withDB(t)()
	resetRedis(t)
	resetActionLimiters(t)
	seedNotifyPeople(t)
	cid := postChallenge(t, "arena", nil)
	answer(t, cid)

	creator := waitInbox(t, nfCreator, 1)
	answerer := waitInbox(t, nfAnswerer, 1)
	if len(creator) != 1 || creator[0].Type != "challenge_accepted" ||
		creator[0].ActorUsername != nfName(nfAnswerer) || creator[0].ChallengeID != cid {
		t.Fatalf("the creator was not told who accepted: %+v", creator)
	}
	if len(answerer) != 1 || answerer[0].Type != "battle_started" ||
		answerer[0].ActorUsername != nfName(nfCreator) {
		t.Fatalf("the answerer was not told who they face: %+v", answerer)
	}
	if creator[0].Text == answerer[0].Text {
		t.Error("both got the same sentence; they did different things")
	}
}

func TestVoting_TellsNobodyButAnyoneWatchingCanSeeWhoVoted(t *testing.T) {
	defer withDB(t)()
	resetRedis(t)
	resetActionLimiters(t)
	seedNotifyPeople(t)
	cid := postChallenge(t, "arena", nil)
	rid := answer(t, cid)
	time.Sleep(200 * time.Millisecond)
	before, _ := inbox(t, nfAnswerer)

	body, _ := json.Marshal(map[string]string{"challengeId": cid, "responseId": rid})
	r := withAuth(httptest.NewRequest("POST", "/api/v1/challenges/vote", bytes.NewReader(body)),
		strconv.Itoa(nfVoter), nfName(nfVoter))
	w := httptest.NewRecorder()
	VoteChallengeHandler(w, r)
	if w.Code != 200 {
		t.Fatalf("vote answered %d: %s", w.Code, w.Body.String())
	}
	time.Sleep(300 * time.Millisecond)
	if after, _ := inbox(t, nfAnswerer); len(after) != len(before) {
		t.Fatalf("a vote sent a notification: %+v", after[:len(after)-len(before)])
	}

	voters := func(viewer int) (int, []VoterSide) {
		req := withAuth(httptest.NewRequest("GET", "/api/v1/challenges/"+cid+"/voters", nil),
			strconv.Itoa(viewer), nfName(viewer))
		req = mux.SetURLVars(req, map[string]string{"id": cid})
		rec := httptest.NewRecorder()
		ChallengeVotersHandler(rec, req)
		var body struct {
			Sides []VoterSide `json:"sides"`
		}
		_ = json.Unmarshal(rec.Body.Bytes(), &body)
		return rec.Code, body.Sides
	}
	// Open to anyone who may watch it, the way Instagram shows who liked a
	// post — the players, and people who are not in the battle at all.
	for _, viewer := range []int{nfCreator, nfAnswerer, nfVoter, nfStranger} {
		code, sides := voters(viewer)
		if code != 200 || len(sides) != 2 {
			t.Fatalf("user %d cannot see the votes: %d %+v", viewer, code, sides)
		}
		if len(sides[0].Voters) != 0 || len(sides[1].Voters) != 1 ||
			sides[1].Voters[0].Username != nfName(nfVoter) ||
			sides[1].Username != nfName(nfAnswerer) {
			t.Fatalf("the vote is on the wrong side: %+v", sides)
		}
	}
}

func TestLikers_AnyoneWatchingSeesWhoLiked(t *testing.T) {
	defer withDB(t)()
	resetRedis(t)
	resetActionLimiters(t)
	seedNotifyPeople(t)
	cid := postChallenge(t, "arena", nil)
	if _, err := db.Exec(`INSERT INTO challenge_likes (challenge_id, user_id) VALUES ($1, $2)`,
		cid, nfFriendA); err != nil {
		t.Fatal(err)
	}
	likers := func(viewer int) (int, []PersonAt) {
		req := withAuth(httptest.NewRequest("GET", "/api/v1/challenges/"+cid+"/likers", nil),
			strconv.Itoa(viewer), nfName(viewer))
		req = mux.SetURLVars(req, map[string]string{"id": cid})
		rec := httptest.NewRecorder()
		ChallengeLikersHandler(rec, req)
		var body struct {
			Likers []PersonAt `json:"likers"`
		}
		_ = json.Unmarshal(rec.Body.Bytes(), &body)
		return rec.Code, body.Likers
	}
	for _, viewer := range []int{nfCreator, nfFriendA, nfStranger} {
		code, people := likers(viewer)
		if code != 200 || len(people) != 1 || people[0].Username != nfName(nfFriendA) {
			t.Fatalf("user %d cannot see who liked: %d %+v", viewer, code, people)
		}
	}
}

func TestInbox_MarkReadClearsTheCount(t *testing.T) {
	defer withDB(t)()
	resetRedis(t)
	resetActionLimiters(t)
	seedNotifyPeople(t)
	waitFriendsTold(t, postChallenge(t, "friends", nil), 2)
	if _, unread := inbox(t, nfFriendA); unread != 1 {
		t.Fatalf("unread %d, want 1", unread)
	}
	r := withAuth(httptest.NewRequest("POST", "/api/v1/notifications/read", nil),
		strconv.Itoa(nfFriendA), nfName(nfFriendA))
	w := httptest.NewRecorder()
	MarkNotificationsReadHandler(w, r)
	if w.Code != 200 {
		t.Fatalf("mark read answered %d", w.Code)
	}
	items, unread := inbox(t, nfFriendA)
	if unread != 0 || len(items) != 1 || !items[0].Read {
		t.Fatalf("still unread after marking: %d %+v", unread, items)
	}
}

func TestSearch_OnlyAskedForSearchesGoIntoHistory(t *testing.T) {
	defer withDB(t)()
	resetRedis(t)
	resetActionLimiters(t)
	seedNotifyPeople(t)
	run := func(url string, signedIn bool) {
		req := httptest.NewRequest("GET", url, nil)
		if signedIn {
			req.Header.Set("Authorization",
				"Bearer "+testToken(t, strconv.Itoa(nfFriendA), nfName(nfFriendA)))
		}
		SearchHandler(httptest.NewRecorder(), req)
	}
	uid := strconv.Itoa(nfFriendA)
	run("/api/v1/search?q=dan&userId="+uid, true)             // typing
	run("/api/v1/search?q=dance&record=1&userId="+uid, false) // no token
	run("/api/v1/search?q=dance&record=1", true)              // asked for
	deadline := time.Now().Add(2 * time.Second)
	var got []string
	for time.Now().Before(deadline) {
		got, _ = rdb.LRange(rctx, "recent_searches:"+uid, 0, -1).Result()
		if len(got) > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	time.Sleep(100 * time.Millisecond)
	got, _ = rdb.LRange(rctx, "recent_searches:"+uid, 0, -1).Result()
	if len(got) != 1 || got[0] != "dance" {
		t.Fatalf("history is %q, want only the search that was asked for", got)
	}
}
