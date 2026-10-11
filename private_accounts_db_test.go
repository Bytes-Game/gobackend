package main

// A private account's videos are for its followers — through the real
// handlers against real Postgres. See private_accounts.go.

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"

	"github.com/gorilla/mux"
)

const (
	privOwner    = 8301
	privFollower = 8302
	privStranger = 8303
)

// seedPrivate makes a private account with one public-looking video, a
// follower, and a stranger; it answers the video's id.
func seedPrivate(t *testing.T) string {
	t.Helper()
	for _, p := range []struct {
		id        int
		name, vis string
	}{
		{privOwner, "hidden_owner", "friends"},
		{privFollower, "their_friend", "public"},
		{privStranger, "a_stranger", "public"},
	} {
		if _, err := db.Exec(`
			INSERT INTO users (id, username, password, visibility)
			VALUES ($1, $2, 'x', $3) ON CONFLICT (id) DO NOTHING`, p.id, p.name, p.vis); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Exec(`INSERT INTO follows (follower_id, following_id) VALUES ($1, $2)
		ON CONFLICT DO NOTHING`, privFollower, privOwner); err != nil {
		t.Fatal(err)
	}
	var id int
	if err := db.QueryRow(`
		INSERT INTO challenges (creator_id, video_url, thumbnail_url, prefix, subject,
		                        visibility, status)
		VALUES ($1, 'https://v/p.mp4', 'https://v/p.jpg', 'who is better at',
		        'zanzibar kite racing', 'arena', 'open')
		RETURNING id`, privOwner).Scan(&id); err != nil {
		t.Fatal(err)
	}
	return strconv.Itoa(id)
}

func as(r *http.Request, who int) *http.Request {
	if who == 0 {
		return r
	}
	return withAuth(r, strconv.Itoa(who), "u"+strconv.Itoa(who))
}

func detailCode(t *testing.T, who int, cid string) int {
	t.Helper()
	r := mux.SetURLVars(as(httptest.NewRequest("GET", "/api/v1/challenges/"+cid, nil), who),
		map[string]string{"id": cid})
	w := httptest.NewRecorder()
	GetChallengeDetailHandler(w, r)
	return w.Code
}

func TestPrivateAccount_TheBattlePageOpensOnlyForWhoMayWatch(t *testing.T) {
	defer withDB(t)()
	resetRedis(t)
	cid := seedPrivate(t)
	for _, c := range []struct {
		name string
		who  int
		want int
	}{
		{"the owner", privOwner, 200},
		{"a follower", privFollower, 200},
		{"a stranger", privStranger, 404},
		{"nobody signed in", 0, 404},
	} {
		if got := detailCode(t, c.who, cid); got != c.want {
			t.Errorf("%s: %d, want %d", c.name, got, c.want)
		}
	}

	// And a friends-only video of a public account: the same page used to
	// open for anybody with its number.
	friends := gridVideo(t, privFollower, 1, "friends", "open")
	if got := detailCode(t, privStranger, friends); got != 404 {
		t.Errorf("a friends-only video opened for a stranger: %d", got)
	}
	if got := detailCode(t, privFollower, friends); got != 200 {
		t.Errorf("a friends-only video did not open for its owner: %d", got)
	}
	public := gridVideo(t, privFollower, 1, "arena", "open")
	if got := detailCode(t, 0, public); got != 200 {
		t.Errorf("a public video did not open for somebody signed out: %d", got)
	}
}

func TestPrivateAccount_TheProfileListsVideosOnlyForFollowers(t *testing.T) {
	defer withDB(t)()
	resetRedis(t)
	cid := seedPrivate(t)
	list := func(who int) []string {
		owner := strconv.Itoa(privOwner)
		r := mux.SetURLVars(as(httptest.NewRequest("GET",
			"/api/v1/users/"+owner+"/challenges", nil), who), map[string]string{"id": owner})
		w := httptest.NewRecorder()
		GetUserChallengesHandler(w, r)
		var got []Challenge
		_ = json.Unmarshal(w.Body.Bytes(), &got)
		ids := []string{}
		for _, c := range got {
			ids = append(ids, c.ID)
		}
		return ids
	}
	if got := list(privFollower); len(got) != 1 || got[0] != cid {
		t.Errorf("a follower sees %v, want [%s]", got, cid)
	}
	if got := list(privStranger); len(got) != 0 {
		t.Errorf("a stranger sees %v of a private account's videos", got)
	}
	if got := list(privOwner); len(got) != 1 {
		t.Errorf("the owner sees %v of their own", got)
	}
}

func TestPrivateAccount_KeptOutOfTheGridAndSearchForStrangers(t *testing.T) {
	defer withDB(t)()
	resetRedis(t)
	cid := seedPrivate(t)

	explore := func(who int) bool {
		r := as(httptest.NewRequest("GET",
			"/api/v1/feed/explore?page=1&limit=30&markShown=false", nil), who)
		w := httptest.NewRecorder()
		ExploreFeedHandler(w, r)
		return strings.Contains(w.Body.String(), `"id":"`+cid+`"`)
	}
	if !explore(privFollower) {
		t.Fatal("a follower's grid does not have the video, so nothing below " +
			"says anything")
	}
	if explore(privStranger) {
		t.Error("a private account's video is in a stranger's Search grid")
	}

	search := func(who int) bool {
		r := as(httptest.NewRequest("GET", "/search?type=all&q="+
			url.QueryEscape("zanzibar kite racing"), nil), who)
		w := httptest.NewRecorder()
		SearchHandler(w, r)
		return strings.Contains(w.Body.String(), `"id":"`+cid+`"`)
	}
	if !search(privFollower) {
		t.Fatal("a follower's search does not find the video, so the next " +
			"check says nothing")
	}
	if search(privStranger) {
		t.Error("a private account's video came up in a stranger's search")
	}
}

func TestPrivateAccount_KeptOutOfForYouForStrangers(t *testing.T) {
	defer withDB(t)()
	resetRedis(t)
	cid := seedPrivate(t)
	forYou := func(who int) bool {
		r := as(httptest.NewRequest("GET", "/api/v1/feed/smart?page=1&limit=20", nil), who)
		w := httptest.NewRecorder()
		SmartFeedHandler(w, r)
		return strings.Contains(w.Body.String(), `"id":"`+cid+`"`)
	}
	if !forYou(privFollower) {
		t.Fatal("a follower's For You does not have the video, so the next " +
			"check says nothing")
	}
	if forYou(privStranger) {
		t.Error("a private account's video is in a stranger's For You")
	}

	// The same for people the app already knows: the feed takes a different
	// road for them, past a different place where the page is put together.
	for _, who := range []int{privFollower, privStranger} {
		if _, err := db.Exec(`
			INSERT INTO user_profiles (user_id, event_count) VALUES ($1, 50)
			ON CONFLICT (user_id) DO UPDATE SET event_count = 50`, strconv.Itoa(who)); err != nil {
			t.Fatal(err)
		}
	}
	resetRedis(t)
	if !forYou(privFollower) {
		t.Fatal("a known follower's For You does not have the video, so the " +
			"next check says nothing")
	}
	if forYou(privStranger) {
		t.Error("a private account's video is in a known stranger's For You")
	}
}

func TestPrivateAccount_BattlesTabIsForFollowers(t *testing.T) {
	defer withDB(t)()
	resetRedis(t)
	cid := seedPrivate(t)
	tab := func(who int) string {
		owner := strconv.Itoa(privOwner)
		r := mux.SetURLVars(as(httptest.NewRequest("GET",
			"/api/v1/users/"+owner+"/battles?tab=open", nil), who), map[string]string{"id": owner})
		w := httptest.NewRecorder()
		UserBattlesHandler(w, r)
		return w.Body.String()
	}
	if !strings.Contains(tab(privFollower), `"challengeId":"`+cid+`"`) {
		t.Fatal("a follower does not see the open challenge, so the next check " +
			"says nothing")
	}
	if strings.Contains(tab(privStranger), `"challengeId":"`+cid+`"`) {
		t.Error("a stranger sees a private account's battles")
	}
}

func TestPrivateAccount_NotOnTheOpenWebPage(t *testing.T) {
	defer withDB(t)()
	resetRedis(t)
	cid := seedPrivate(t)
	code, body := publicPage(t, PublicVideoPageHandler, "/v/"+cid, map[string]string{"id": cid})
	if code != 404 || strings.Contains(body, "<video") {
		t.Errorf("a private account's video plays on the open web page: %d", code)
	}
}

func TestPrivateAccount_ListsAboutAVideoFollowTheSameRule(t *testing.T) {
	defer withDB(t)()
	resetRedis(t)
	cid := seedPrivate(t)
	about := func(who int) int {
		r := mux.SetURLVars(as(httptest.NewRequest("GET", "/api/v1/challenges/"+cid+"/about", nil), who),
			map[string]string{"id": cid})
		w := httptest.NewRecorder()
		VideoAboutHandler(w, r)
		return w.Code
	}
	if got := about(privFollower); got != 200 {
		t.Errorf("a follower was refused what the video is about: %d", got)
	}
	if got := about(privStranger); got == 200 {
		t.Error("a stranger was told what a private account's video is about")
	}
}

func TestPrivateAccount_CommentsFollowTheSameRule(t *testing.T) {
	defer withDB(t)()
	resetRedis(t)
	resetActionLimiters(t)
	cid := seedPrivate(t)
	read := func(who int) int {
		r := mux.SetURLVars(as(httptest.NewRequest("GET", "/api/v1/challenges/"+cid+"/comments", nil), who),
			map[string]string{"id": cid})
		w := httptest.NewRecorder()
		GetChallengeCommentsHandler(w, r)
		return w.Code
	}
	write := func(who int) int {
		body := strings.NewReader(`{"challengeId":"` + cid + `","text":"nice"}`)
		r := as(httptest.NewRequest("POST", "/api/v1/challenges/comments", body), who)
		w := httptest.NewRecorder()
		AddChallengeCommentHandler(w, r)
		return w.Code
	}
	if got := write(privFollower); got != 200 {
		t.Fatalf("a follower could not comment: %d", got)
	}
	if got := read(privFollower); got != 200 {
		t.Errorf("a follower could not read the comments: %d", got)
	}
	if got := read(privStranger); got != 404 {
		t.Errorf("a stranger read the comments under a private account's video: %d", got)
	}
	if got := read(0); got != 404 {
		t.Errorf("somebody signed out read them: %d", got)
	}
	if got := write(privStranger); got == 200 {
		t.Error("a stranger commented under a private account's video")
	}
}
