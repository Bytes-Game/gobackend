package main

// Who may battle whom — through the real handlers against real Postgres.
// See battle_levels.go.

import (
	"bytes"
	"encoding/json"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/gorilla/mux"
)

const (
	lvGold    = 8401 // the challenge's creator: 1,200 points
	lvClose   = 8402 // 1,160: 40 below
	lvEdge    = 8403 // 1,250: exactly 50 above
	lvJustOut = 8404 // 1,149: 51 below
	lvFar     = 8405 // 1,600
	lvNew     = 8406 // 1,000, no battle decided yet
)

// seedLevels makes a Gold player on 1,200 points with a challenge, and
// people at various distances from them; it answers the challenge's id.
func seedLevels(t *testing.T, visibility string) string {
	t.Helper()
	for _, p := range []struct {
		id, rating, wins int
		name             string
	}{
		{lvGold, 1200, 4, "gold_player"},
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
	return gridVideo(t, lvGold, 0, visibility, "open")
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
	for _, want := range []string{"a Gold player with 1,200 points", "You have 1,149 points",
		"from 1,099 to 1,199 points", "friends at any level"} {
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
	if ok || !strings.Contains(msg, "1,099 to 1,199") {
		t.Errorf("somebody 51 points away is told %v %q", ok, msg)
	}
	if ok, msg := detailSays(t, lvGold, cid); ok || !strings.Contains(msg, "your own") {
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
	follow(t, lvJustOut, lvGold)
	if code, body := levelAnswer(t, lvJustOut, cid); code != 403 {
		t.Fatalf("a one-way follow got round the level rule: %d %s", code, body)
	}
	// Following each other is.
	follow(t, lvGold, lvJustOut)
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
	follow(t, lvFar, lvGold)
	follow(t, lvJustOut, lvGold)
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
	code, body := levelAnswer(t, lvGold, cid)
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
	got := outOfRangeWords("Ace", 1800, 1000)
	want := "This challenge is from an Ace player with 1,800 points. You have 1,000 " +
		"points, so you can battle players from 950 to 1,050 points, or friends at " +
		"any level. Win battles to climb."
	if got != want {
		t.Errorf("got  %q\nwant %q", got, want)
	}
	if !strings.Contains(outOfRangeWords("Unranked", 1000, 1200), "a new player") {
		t.Error("somebody with no battle decided is not called a new player")
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
	closed := gridVideo(t, lvGold, 0, "arena", "open")
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
	if why, _ := far["answerBlocked"].(string); !strings.Contains(why, "1,099 to 1,199") {
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
	follow(t, lvJustOut, lvGold)
	follow(t, lvGold, lvJustOut)
	resetRedis(t)
	if got := findChallenge(feed(lvJustOut), cid); got == nil {
		t.Fatal("the challenge left the feed after following")
	} else if why, ok := got["answerBlocked"]; ok {
		t.Errorf("a friend's Accept is off: %v", why)
	}
}
