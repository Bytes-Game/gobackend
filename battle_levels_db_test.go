package main

// Who may battle whom — through the real handlers against real Postgres.
// See battle_levels.go.

import (
	"bytes"
	"encoding/json"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/gorilla/mux"
)

const (
	lvHost    = 8401 // the challenge's creator: 1,200 points, in Bronze
	lvClose   = 8402 // 1,160: 40 below
	lvEdge    = 8403 // 1,250: exactly 50 above
	lvJustOut = 8404 // 1,149: 51 below
	lvFar     = 8405 // 1,600
	lvNew     = 8406 // 1,000, no battle decided yet
)

// seedLevels makes a Bronze player on 1,200 points with a challenge, and
// people at various distances from them; it answers the challenge's id.
// Everybody here is in Bronze, so the window is 50 points.
func seedLevels(t *testing.T, visibility string) string {
	t.Helper()
	for _, p := range []struct {
		id, rating, wins int
		name             string
	}{
		{lvHost, 1200, 4, "host_player"},
		{lvClose, 1160, 4, "close_player"},
		{lvEdge, 1250, 4, "edge_player"},
		{lvJustOut, 1149, 4, "just_out"},
		{lvFar, 1600, 4, "far_player"},
		{lvNew, 1000, 0, "brand_new"},
	} {
		// The league column says Bronze for everybody, as it does for
		// anybody whose battles have not been decided since it was added:
		// the rule goes by points, not by it.
		if _, err := db.Exec(`
			INSERT INTO users (id, username, password, rating, wins, league)
			VALUES ($1, $2, 'x', $3, $4, 'Bronze') ON CONFLICT (id) DO NOTHING`,
			p.id, p.name, p.rating, p.wins); err != nil {
			t.Fatal(err)
		}
	}
	return gridVideo(t, lvHost, 0, visibility, "open")
}

// levelAnswer answers [cid] through the handler, as the app does.
func levelAnswer(t *testing.T, who int, cid string) (int, string) {
	t.Helper()
	body, _ := json.Marshal(map[string]any{
		"challengeId": cid, "videoUrl": testUpload(who, "720p.mp4"),
		"durationMs": 5000,
	})
	r := withAuth(httptest.NewRequest("POST", "/api/v1/challenges/accept", bytes.NewReader(body)),
		strconv.Itoa(who), "u"+strconv.Itoa(who))
	w := httptest.NewRecorder()
	AcceptChallengeHandler(w, r)
	return w.Code, w.Body.String()
}

// detailSays is what the battle page tells [who] about answering [cid].
func detailSays(t *testing.T, who int, cid string) (bool, string) {
	t.Helper()
	r := mux.SetURLVars(as(httptest.NewRequest("GET", "/api/v1/challenges/"+cid, nil), who),
		map[string]string{"id": cid})
	w := httptest.NewRecorder()
	GetChallengeDetailHandler(w, r)
	var got struct {
		CanAccept     bool   `json:"canAccept"`
		LeagueMessage string `json:"leagueMessage"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("battle page for %d: %d %s", who, w.Code, w.Body.String())
	}
	return got.CanAccept, got.LeagueMessage
}

func follow(t *testing.T, from, to int) {
	t.Helper()
	if _, err := db.Exec(`INSERT INTO follows (follower_id, following_id) VALUES ($1, $2)
		ON CONFLICT DO NOTHING`, from, to); err != nil {
		t.Fatal(err)
	}
}

func TestBattleLevels_OnlyWithin50PointsMayAnswer(t *testing.T) {
	defer withDB(t)()
	resetRedis(t)
	resetActionLimiters(t)
	cid := seedLevels(t, "arena")

	for _, c := range []struct {
		name  string
		who   int
		allow bool
	}{
		{"40 points below", lvClose, true},
		{"exactly 50 points above", lvEdge, true},
		{"51 points below", lvJustOut, false},
		{"400 points above", lvFar, false},
		{"somebody new, 200 points below", lvNew, false},
	} {
		code, body := levelAnswer(t, c.who, cid)
		if c.allow && code != 201 {
			t.Errorf("%s was refused: %d %s", c.name, code, body)
		}
		if !c.allow && code != 403 {
			t.Errorf("%s was let in: %d %s", c.name, code, body)
		}
	}

	// And the refusal says, in words, what the person can do instead.
	_, said := levelAnswer(t, lvJustOut, cid)
	for _, want := range []string{"a Bronze player with 1,200 points", "51 more than your 1,149",
		"Bronze players battle within 50 points", "friends at any level"} {
		if !strings.Contains(said, want) {
			t.Errorf("the refusal %q does not say %q", said, want)
		}
	}
}

func TestBattleLevels_TheBattlePageSaysSoBeforeAnybodyRecords(t *testing.T) {
	defer withDB(t)()
	resetRedis(t)
	cid := seedLevels(t, "arena")

	if ok, msg := detailSays(t, lvClose, cid); !ok || msg != "" {
		t.Errorf("somebody 40 points away is told %v %q", ok, msg)
	}
	ok, msg := detailSays(t, lvJustOut, cid)
	if ok || !strings.Contains(msg, "within 50 points") {
		t.Errorf("somebody 51 points away is told %v %q", ok, msg)
	}
	if ok, msg := detailSays(t, lvHost, cid); ok || !strings.Contains(msg, "your own") {
		t.Errorf("the creator is told %v %q about their own challenge", ok, msg)
	}
	// Nobody signed in: nothing to say, and the page still opens.
	if ok, msg := detailSays(t, 0, cid); !ok || msg != "" {
		t.Errorf("somebody signed out is told %v %q", ok, msg)
	}
}

func TestBattleLevels_FriendsBattleAtAnyLevel(t *testing.T) {
	defer withDB(t)()
	resetRedis(t)
	resetActionLimiters(t)
	cid := seedLevels(t, "arena")

	// Following somebody is not enough: anybody can follow anybody.
	follow(t, lvJustOut, lvHost)
	if code, body := levelAnswer(t, lvJustOut, cid); code != 403 {
		t.Fatalf("a one-way follow got round the level rule: %d %s", code, body)
	}
	// Following each other is.
	follow(t, lvHost, lvJustOut)
	if ok, msg := detailSays(t, lvJustOut, cid); !ok {
		t.Errorf("friends are told they cannot battle: %q", msg)
	}
	if code, body := levelAnswer(t, lvJustOut, cid); code != 201 {
		t.Errorf("friends could not battle across levels: %d %s", code, body)
	}
}

func TestBattleLevels_PickedByNameForAFriendsOnlyPostCounts(t *testing.T) {
	defer withDB(t)()
	resetRedis(t)
	resetActionLimiters(t)
	cid := seedLevels(t, "friends")
	// Both follow the creator, so both may see it; only one was picked.
	follow(t, lvFar, lvHost)
	follow(t, lvJustOut, lvHost)
	if _, err := db.Exec(`INSERT INTO challenge_visible_to (challenge_id, user_id)
		VALUES ($1, $2), ($1, $3)`, cid, lvJustOut, lvClose); err != nil {
		t.Fatal(err)
	}
	if code, body := levelAnswer(t, lvJustOut, cid); code != 201 {
		t.Errorf("somebody the creator picked by name was refused: %d %s", code, body)
	}
	if code, _ := levelAnswer(t, lvFar, cid); code == 201 {
		t.Error("somebody not picked answered a friends-only post")
	}
}

func TestBattleLevels_NobodyAnswersTheirOwnChallenge(t *testing.T) {
	defer withDB(t)()
	resetRedis(t)
	resetActionLimiters(t)
	cid := seedLevels(t, "arena")
	code, body := levelAnswer(t, lvHost, cid)
	if code != 403 || !strings.Contains(body, "your own challenge") {
		t.Errorf("the creator answered their own challenge: %d %s", code, body)
	}
}

func TestBattleLevels_NobodyAnswersAVideoTheyMayNotSee(t *testing.T) {
	defer withDB(t)()
	resetRedis(t)
	resetActionLimiters(t)
	cid := seedPrivate(t) // a private account's video
	if _, err := db.Exec(`UPDATE users SET rating = 1000, wins = 0 WHERE id IN ($1, $2)`,
		privStranger, privFollower); err != nil {
		t.Fatal(err)
	}
	if code, _ := levelAnswer(t, privStranger, cid); code != 404 {
		t.Errorf("a stranger answered a private account's video: %d", code)
	}
	if code, body := levelAnswer(t, privFollower, cid); code != 201 {
		t.Errorf("a follower could not answer it: %d %s", code, body)
	}
}

func TestBattleLevels_TheRefusalInWords(t *testing.T) {
	got := outOfRangeWords("Gold", 3035, 2990)
	want := "This challenge is from a Gold player with 3,035 points, 45 more than " +
		"your 2,990. Gold players battle within 30 points, so you can't answer this " +
		"one. Win battles to climb closer, or battle friends at any level."
	if got != want {
		t.Errorf("got  %q\nwant %q", got, want)
	}
	// Somebody far below: climbing would not help, so it is not suggested.
	down := outOfRangeWords("Unranked", 1000, 1200)
	for _, w := range []string{"a new player", "200 fewer than your 1,200",
		"Bronze players battle within 50 points", "You can still battle friends"} {
		if !strings.Contains(down, w) {
			t.Errorf("%q does not say %q", down, w)
		}
	}
	if strings.Contains(down, "climb") {
		t.Errorf("somebody 200 points ABOVE the challenger is told to climb: %q", down)
	}
}

// The window narrows each league up: 50, 40, 30, 20, 10, 10.
func TestBattleLevels_TheWindowNarrowsEachLeagueUp(t *testing.T) {
	for _, c := range []struct {
		league string
		points int
		window int
	}{
		{"Bronze", 1500, 50}, {"Silver", 2500, 40}, {"Gold", 3500, 30},
		{"Platinum", 4500, 20}, {"Diamond", 5500, 10}, {"Ace", 6500, 10},
	} {
		if !inRange(c.points+c.window, c.points) || !inRange(c.points-c.window, c.points) {
			t.Errorf("%s: %d points either way was refused", c.league, c.window)
		}
		if inRange(c.points+c.window+1, c.points) || inRange(c.points-c.window-1, c.points) {
			t.Errorf("%s: %d points either way was let in", c.league, c.window+1)
		}
	}
}

// Where two leagues meet, the higher one's rule applies — whoever posted.
func TestBattleLevels_TheHigherLeaguesRuleApplies(t *testing.T) {
	// Bronze 1,980 and Silver 2,025: 45 apart, inside Bronze's 50 but
	// outside Silver's 40.
	if inRange(2025, 1980) || inRange(1980, 2025) {
		t.Error("a Silver player was put in a battle 45 points wide")
	}
	// 40 apart is inside Silver's rule, from either side.
	if !inRange(2020, 1980) || !inRange(1980, 2020) {
		t.Error("Bronze and Silver 40 points apart could not battle")
	}
	// Close to the next league you can reach into it.
	if !inRange(2010, 1990) {
		t.Error("a Bronze player 10 points under Silver could not battle into it")
	}
}

// Through the real answer step: a Silver player's challenge is held to
// Silver's 40, not Bronze's 50. If the league stopped reaching the window,
// 45 points would get in here.
func TestBattleLevels_ASilverChallengeTakesSilversWindow(t *testing.T) {
	defer withDB(t)()
	resetRedis(t)
	resetActionLimiters(t)
	for _, p := range []struct {
		id, rating int
		name       string
	}{
		{8411, 2500, "silver_host"}, {8412, 2540, "silver_near"}, {8413, 2545, "silver_out"},
	} {
		if _, err := db.Exec(`
			INSERT INTO users (id, username, password, rating, wins, league)
			VALUES ($1, $2, 'x', $3, 20, 'Silver') ON CONFLICT (id) DO NOTHING`,
			p.id, p.name, p.rating); err != nil {
			t.Fatal(err)
		}
	}
	cid := gridVideo(t, 8411, 0, "arena", "open")
	code, body := levelAnswer(t, 8413, cid)
	if code != 403 || !strings.Contains(body, "Silver players battle within 40 points") {
		t.Errorf("45 points apart in Silver: %d %s", code, body)
	}
	if code, body := levelAnswer(t, 8412, cid); code != 201 {
		t.Errorf("40 points apart in Silver was refused: %d %s", code, body)
	}
}

// findChallenge walks a feed's JSON for the challenge with [id].
func findChallenge(v any, id string) map[string]any {
	switch x := v.(type) {
	case map[string]any:
		if x["id"] == id {
			if _, ok := x["creatorId"]; ok {
				return x
			}
		}
		for _, e := range x {
			if got := findChallenge(e, id); got != nil {
				return got
			}
		}
	case []any:
		for _, e := range x {
			if got := findChallenge(e, id); got != nil {
				return got
			}
		}
	}
	return nil
}

func TestBattleLevels_TheFeedSaysWhichAcceptButtonsAreOff(t *testing.T) {
	defer withDB(t)()
	resetRedis(t)
	cid := seedLevels(t, "arena")
	closed := gridVideo(t, lvHost, 0, "arena", "open")
	if _, err := db.Exec(`UPDATE challenges SET open_to_battles = FALSE WHERE id = $1`, closed); err != nil {
		t.Fatal(err)
	}
	// The For You feed, as the reel reads it.
	feed := func(who int) any {
		r := as(httptest.NewRequest("GET", "/api/v1/feed/smart?page=1&limit=30", nil), who)
		w := httptest.NewRecorder()
		SmartFeedHandler(w, r)
		var got any
		if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
			t.Fatalf("feed for %d: %d %s", who, w.Code, w.Body.String())
		}
		return got
	}
	far := findChallenge(feed(lvJustOut), cid)
	if far == nil {
		t.Fatal("the challenge is not in the feed, so nothing below says anything")
	}
	if why, _ := far["answerBlocked"].(string); !strings.Contains(why, "within 50 points") {
		t.Errorf("somebody 51 points away is not told their Accept is off: %v", far["answerBlocked"])
	}
	near := findChallenge(feed(lvClose), cid)
	if near == nil {
		t.Fatal("the challenge is not in the near player's feed")
	}
	if why, ok := near["answerBlocked"]; ok {
		t.Errorf("somebody 40 points away has their Accept off: %v", why)
	}
	// A post nobody can answer is not marked: there is no button to switch.
	if got := findChallenge(feed(lvJustOut), closed); got != nil {
		if _, ok := got["answerBlocked"]; ok {
			t.Error("a post closed to battles was marked as well")
		}
	}
	// Friends: the button is on at any distance.
	follow(t, lvJustOut, lvHost)
	follow(t, lvHost, lvJustOut)
	resetRedis(t)
	if got := findChallenge(feed(lvJustOut), cid); got == nil {
		t.Fatal("the challenge left the feed after following")
	} else if why, ok := got["answerBlocked"]; ok {
		t.Errorf("a friend's Accept is off: %v", why)
	}
}

// Migration 022 works everybody's league out again with the new lines, the
// same way leagueFor does, and leaves people with no battle decided alone.
func TestMigration022_EveryLeagueIsWorkedOutAgain(t *testing.T) {
	defer withDB(t)()
	type person struct {
		id, rating, wins int
		stale            string
	}
	people := []person{
		{8421, 1100, 3, "Silver"},   // Silver under the old lines; Bronze now
		{8422, 1100, 0, "Unranked"}, // nothing decided: left alone
	}
	// One point under every line and exactly on it, so a line in the
	// migration that is off by any amount shows here.
	id := 8430
	for line := 2000; line <= 6000; line += 1000 {
		people = append(people, person{id, line - 1, 3, "Ace"}, person{id + 1, line, 3, "Bronze"})
		id += 2
	}
	for _, p := range people {
		if _, err := db.Exec(`
			INSERT INTO users (id, username, password, rating, wins, league)
			VALUES ($1, $2, 'x', $3, $4, $5)`,
			p.id, "m022_"+strconv.Itoa(p.id), p.rating, p.wins, p.stale); err != nil {
			t.Fatal(err)
		}
	}
	sqlText, err := os.ReadFile("migrations/022_leagues_every_thousand_points.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(string(sqlText)); err != nil {
		t.Fatalf("running migration 022: %v", err)
	}
	for _, p := range people {
		var got string
		if err := db.QueryRow(`SELECT league FROM users WHERE id = $1`, p.id).Scan(&got); err != nil {
			t.Fatal(err)
		}
		want := leagueFor(p.rating, p.wins)
		if p.wins == 0 {
			want = p.stale
		}
		if got != want {
			t.Errorf("%d points, %d wins: league %q after the migration, want %q",
				p.rating, p.wins, got, want)
		}
	}
}
