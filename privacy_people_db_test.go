package main

// What other people may see about you — through the real handlers against
// real Postgres. See showUserTo in private_accounts.go.

import (
	"encoding/json"
	"net/http/httptest"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/gorilla/mux"
)

// profileAs is the profile of [username] as [who] sees it.
func profileAs(t *testing.T, who int, username string) map[string]any {
	t.Helper()
	r := mux.SetURLVars(as(httptest.NewRequest("GET", "/api/v1/users/"+username, nil), who),
		map[string]string{"username": username})
	w := httptest.NewRecorder()
	GetUserHandler(w, r)
	var got map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("profile of %s: %d %s", username, w.Code, w.Body.String())
	}
	return got
}

func TestPrivacy_YourSettingsAndSecondStepAreYoursAlone(t *testing.T) {
	defer withDB(t)()
	resetRedis(t)
	seedPrivate(t)
	if _, err := db.Exec(`UPDATE users SET settings = '{"theme":"dark","messages":"following"}'
		WHERE id = $1`, privFollower); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO user_totp (user_id, secret, is_active)
		VALUES ($1, 'x', TRUE)`, privFollower); err != nil {
		t.Fatal(err)
	}

	mine := profileAs(t, privFollower, "their_friend")
	if mine["settings"] == nil || mine["twoFactorEnabled"] != true {
		t.Fatalf("your own profile is missing your settings or second step: %v", mine)
	}
	for name, who := range map[string]int{"somebody else": privStranger, "nobody signed in": 0} {
		got := profileAs(t, who, "their_friend")
		if got["settings"] != nil {
			t.Errorf("%s sees the settings: %v", name, got["settings"])
		}
		if got["twoFactorEnabled"] != nil {
			t.Errorf("%s is told whether there is a second step", name)
		}
		if got["username"] != "their_friend" {
			t.Errorf("%s does not get the profile at all: %v", name, got)
		}
	}
}

func TestPrivateAccount_FollowListsAreForFollowers(t *testing.T) {
	defer withDB(t)()
	resetRedis(t)
	seedPrivate(t)
	// The private account follows the stranger, so it has a following list.
	follow(t, privOwner, privStranger)
	owner := strconv.Itoa(privOwner)

	lists := func(who int) (followers, following string) {
		r := mux.SetURLVars(as(httptest.NewRequest("GET", "/api/v1/users/"+owner+"/followers", nil), who),
			map[string]string{"id": owner})
		w := httptest.NewRecorder()
		GetFollowersHandler(w, r)
		followers = w.Body.String()
		r = mux.SetURLVars(as(httptest.NewRequest("GET", "/api/v1/users/"+owner+"/following", nil), who),
			map[string]string{"id": owner})
		w = httptest.NewRecorder()
		GetFollowingHandler(w, r)
		following = w.Body.String()
		return
	}
	followers, following := lists(privFollower)
	if !strings.Contains(followers, `"their_friend"`) || !strings.Contains(following, `"a_stranger"`) {
		t.Fatalf("a follower does not see the lists, so the next checks say nothing: %s %s",
			followers, following)
	}
	followers, following = lists(privStranger)
	if strings.Contains(followers, `"username"`) || strings.Contains(following, `"username"`) {
		t.Errorf("a stranger sees a private account's lists: %s %s", followers, following)
	}

	// On the profile: the list is left out, the count still shows.
	got := profileAs(t, privStranger, "hidden_owner")
	if l, _ := got["followingList"].([]any); len(l) != 0 {
		t.Errorf("a stranger sees who a private account follows: %v", l)
	}
	if got["following"] != float64(1) {
		t.Errorf("the following count is %v, want 1", got["following"])
	}
	got = profileAs(t, privFollower, "hidden_owner")
	if l, _ := got["followingList"].([]any); len(l) != 1 {
		t.Errorf("a follower does not see who it follows: %v", got["followingList"])
	}

	// And in somebody else's list of people: the stranger's followers
	// include the private account, whose own list stays its business.
	stranger := strconv.Itoa(privStranger)
	inList := func(who int) []any {
		r := mux.SetURLVars(as(httptest.NewRequest("GET", "/api/v1/users/"+stranger+"/followers", nil), who),
			map[string]string{"id": stranger})
		w := httptest.NewRecorder()
		GetFollowersHandler(w, r)
		var people []map[string]any
		_ = json.Unmarshal(w.Body.Bytes(), &people)
		for _, p := range people {
			if p["username"] == "hidden_owner" {
				l, _ := p["followingList"].([]any)
				if l == nil {
					l = []any{}
				}
				return l
			}
		}
		t.Fatalf("the private account is not in the list at all: %s", w.Body.String())
		return nil
	}
	if l := inList(privFollower); len(l) != 1 {
		t.Errorf("in a list, a follower does not see who it follows: %v", l)
	}
	if l := inList(0); len(l) != 0 {
		t.Errorf("in a list, somebody signed out sees who a private account follows: %v", l)
	}
}

func TestPrivateAccount_TheScoreIsForWhoMayWatch(t *testing.T) {
	defer withDB(t)()
	resetRedis(t)
	cid := seedPrivate(t)
	score := func(who int) int {
		r := mux.SetURLVars(as(httptest.NewRequest("GET", "/api/v1/challenges/"+cid+"/standings", nil), who),
			map[string]string{"id": cid})
		w := httptest.NewRecorder()
		BattleStandingsHandler(w, r)
		return w.Code
	}
	if got := score(privFollower); got != 200 {
		t.Fatalf("a follower could not see the score: %d", got)
	}
	if got := score(privStranger); got != 404 {
		t.Errorf("a stranger saw the score of a private account's battle: %d", got)
	}
	if got := score(0); got != 404 {
		t.Errorf("somebody signed out saw it: %d", got)
	}
}

func TestPrivateAccount_NotInThePublicList(t *testing.T) {
	defer withDB(t)()
	resetRedis(t)
	cid := seedPrivate(t)
	arena := func(who int) bool {
		r := as(httptest.NewRequest("GET", "/api/v1/challenges/arena", nil), who)
		w := httptest.NewRecorder()
		GetArenaChallengesHandler(w, r)
		return strings.Contains(w.Body.String(), `"id":"`+cid+`"`)
	}
	if !arena(privFollower) {
		t.Fatal("a follower's list does not have the video, so the next check says nothing")
	}
	if arena(privStranger) {
		t.Error("a private account's video is in a stranger's public list")
	}
	if arena(0) {
		t.Error("a private account's video is in the public list for somebody signed out")
	}
}

func TestMetrics_OnlyWithTheToken(t *testing.T) {
	h := metricsGuard(MetricsHandler())
	get := func(token string) int {
		r := httptest.NewRequest("GET", "/metrics", nil)
		if token != "" {
			r.Header.Set("Authorization", "Bearer "+token)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w.Code
	}
	was, had := os.LookupEnv("METRICS_TOKEN")
	defer func() {
		if had {
			os.Setenv("METRICS_TOKEN", was)
		} else {
			os.Unsetenv("METRICS_TOKEN")
		}
	}()
	os.Unsetenv("METRICS_TOKEN")
	if got := get(""); got != 404 {
		t.Errorf("with no token set up, the numbers are open: %d", got)
	}
	if got := get("anything"); got != 404 {
		t.Errorf("with no token set up, any token opens them: %d", got)
	}
	os.Setenv("METRICS_TOKEN", "s3cret")
	if got := get("s3cret"); got != 200 {
		t.Errorf("the right token is refused: %d", got)
	}
	if got := get("wrong"); got != 404 {
		t.Errorf("a wrong token opens them: %d", got)
	}
	if got := get(""); got != 404 {
		t.Errorf("no token opens them: %d", got)
	}
}

// The tests above call the guard by hand, so they cannot notice main.go
// handing out the numbers without it. This reads main.go itself, comments
// stripped.
func TestMetrics_MainPutsTheGuardInFront(t *testing.T) {
	src, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	code := regexp.MustCompile(`(?m)^\s*//.*$`).ReplaceAllString(string(src), "")
	if !strings.Contains(code, `"/metrics", metricsGuard(MetricsHandler())`) {
		t.Error("main.go serves /metrics without metricsGuard")
	}
	if strings.Count(code, "MetricsHandler()") != 1 {
		t.Error("main.go serves the numbers somewhere else as well")
	}
}
