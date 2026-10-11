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
	lvGold     = 8401 // the challenge's creator
	lvBronze   = 8402
	lvSilver   = 8403
	lvPlatinum = 8404
	lvDiamond  = 8405
	lvNew      = 8406 // no battle decided yet
)

// seedLevels makes one person in each league and a Gold player's challenge;
// it answers the challenge's id.
func seedLevels(t *testing.T, visibility string) string {
	t.Helper()
	for _, p := range []struct {
		id, rating, wins int
		name             string
	}{
		{lvGold, 1200, 4, "gold_player"},
		{lvBronze, 1000, 4, "bronze_player"},
		{lvSilver, 1100, 4, "silver_player"},
		{lvPlatinum, 1350, 4, "platinum_player"},
		{lvDiamond, 1600, 4, "diamond_player"},
		{lvNew, 1000, 0, "brand_new"},
	} {
		// The league column says Bronze for everybody, as it does for
		// anybody whose battles have not been decided since it was added:
		// the rule must go by the rating and the record, not by it.
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

func TestBattleLevels_OnlyTheNeighbouringLeaguesMayAnswer(t *testing.T) {
	defer withDB(t)()
	resetRedis(t)
	resetActionLimiters(t)
	cid := seedLevels(t, "arena")

	for _, c := range []struct {
		name  string
		who   int
		allow bool
	}{
		{"Silver, one below Gold", lvSilver, true},
		{"Platinum, one above Gold", lvPlatinum, true},
		{"Bronze, two below Gold", lvBronze, false},
		{"Diamond, two above Gold", lvDiamond, false},
		{"somebody with no battle decided yet", lvNew, false},
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
	_, said := levelAnswer(t, lvBronze, cid)
	for _, want := range []string{"Gold player", "You're in Bronze",
		"Bronze and Silver", "friends at any level"} {
		if !strings.Contains(said, want) {
			t.Errorf("the refusal %q does not say %q", said, want)
		}
	}
	_, said = levelAnswer(t, lvNew, cid)
	if !strings.Contains(said, "start in Bronze") {
		t.Errorf("somebody new is told %q, not that they start in Bronze", said)
	}
}

func TestBattleLevels_TheBattlePageSaysSoBeforeAnybodyRecords(t *testing.T) {
	defer withDB(t)()
	resetRedis(t)
	cid := seedLevels(t, "arena")

	if ok, msg := detailSays(t, lvSilver, cid); !ok || msg != "" {
		t.Errorf("a Silver player is told %v %q about a Gold challenge", ok, msg)
	}
	ok, msg := detailSays(t, lvBronze, cid)
	if ok || !strings.Contains(msg, "Bronze and Silver") {
		t.Errorf("a Bronze player is told %v %q about a Gold challenge", ok, msg)
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
	follow(t, lvBronze, lvGold)
	if code, body := levelAnswer(t, lvBronze, cid); code != 403 {
		t.Fatalf("a one-way follow got round the level rule: %d %s", code, body)
	}
	// Following each other is.
	follow(t, lvGold, lvBronze)
	if ok, msg := detailSays(t, lvBronze, cid); !ok {
		t.Errorf("friends are told they cannot battle: %q", msg)
	}
	if code, body := levelAnswer(t, lvBronze, cid); code != 201 {
		t.Errorf("friends could not battle across levels: %d %s", code, body)
	}
}

func TestBattleLevels_PickedByNameForAFriendsOnlyPostCounts(t *testing.T) {
	defer withDB(t)()
	resetRedis(t)
	resetActionLimiters(t)
	cid := seedLevels(t, "friends")
	// Both follow the creator, so both may see it; only one was picked.
	follow(t, lvDiamond, lvGold)
	follow(t, lvBronze, lvGold)
	if _, err := db.Exec(`INSERT INTO challenge_visible_to (challenge_id, user_id)
		VALUES ($1, $2), ($1, $3)`, cid, lvBronze, lvSilver); err != nil {
		t.Fatal(err)
	}
	if code, body := levelAnswer(t, lvBronze, cid); code != 201 {
		t.Errorf("somebody the creator picked by name was refused: %d %s", code, body)
	}
	if code, _ := levelAnswer(t, lvDiamond, cid); code == 201 {
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

func TestBattleLevels_InReachInWords(t *testing.T) {
	for tier, want := range map[int]string{
		1: "Bronze and Silver",
		3: "Silver, Gold and Platinum",
		5: "Platinum and Diamond",
	} {
		if got := leaguesInReach(tier); got != want {
			t.Errorf("tier %d: %q, want %q", tier, got, want)
		}
	}
}
