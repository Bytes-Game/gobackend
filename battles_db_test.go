package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/mux"
)

// Battles against a real database: the votes as the endpoint records them,
// the clock AcceptChallenge starts, the standings' queries, and the resolver
// that writes wins, losses and ratings. Every one of these is SQL a fake
// database would wave through.

// openDB is the database withDB opened, checked, for the functions that take
// one as an argument.
func openDB(t *testing.T) *sql.DB {
	t.Helper()
	if db == nil {
		t.Fatal("no database: call withDB first")
	}
	return db
}

// voter makes an account that existed well before any battle here and has
// been active on activeDays different days.
func voter(t *testing.T, id int, activeDays int) {
	t.Helper()
	seedAccount(t, id, time.Now().Add(-60*24*time.Hour), activeDays)
}

func seedAccount(t *testing.T, id int, created time.Time, activeDays int) {
	t.Helper()
	if _, err := db.Exec(`
		INSERT INTO users (id, username, password, created_at)
		VALUES ($1, $2, 'x', $3) ON CONFLICT (id) DO NOTHING`,
		id, fmt.Sprintf("voter%d", id), created); err != nil {
		t.Fatalf("seed account %d: %v", id, err)
	}
	for d := 0; d < activeDays; d++ {
		if _, err := db.Exec(`
			INSERT INTO feed_events (user_id, content_id, content_type, event_type, created_at)
			VALUES ($1, 'elsewhere', 'challenge', 'view', NOW() - ($2 || ' days')::interval - INTERVAL '1 hour')`,
			strconv.Itoa(id), strconv.Itoa(d+1)); err != nil {
			t.Fatalf("seed activity for %d: %v", id, err)
		}
	}
}

// watched records that userID had a video of this battle on screen, an hour
// ago: inside every battle here, which all started two days ago, and before
// the end of the ones a test closes a minute ago. Recorded at NOW() it would
// land after that end, and every vote would be thrown out as unwatched.
func watched(t *testing.T, userID int, contentType, contentID string, ms int, meta string) {
	t.Helper()
	if meta == "" {
		meta = "{}"
	}
	if _, err := db.Exec(`
		INSERT INTO feed_events (user_id, content_id, content_type, event_type, watch_duration_ms, metadata, created_at)
		VALUES ($1, $2, $3, 'view', $4, $5::jsonb, NOW() - INTERVAL '1 hour')`,
		strconv.Itoa(userID), contentID, contentType, ms, meta); err != nil {
		t.Fatalf("seed view: %v", err)
	}
}

// liveBattle is a challenge by user 1 answered by user 2, accepted two days
// ago so accounts can be older or newer than its start.
func liveBattle(t *testing.T) (cid, rid string) {
	t.Helper()
	cid = auditChallenge(t, map[string]any{"subject": "battle " + strconv.Itoa(int(time.Now().UnixNano()%100000))})
	resp, err := AcceptChallenge(AcceptChallengePayload{
		ChallengeID: cid, ResponderID: "2", VideoURL: "https://v/answer.mp4", DurationMs: 5000,
	})
	if err != nil {
		t.Fatalf("AcceptChallenge: %v", err)
	}
	if _, err := db.Exec(`
		UPDATE challenges
		   SET accepted_at = NOW() - INTERVAL '2 days',
		       voting_ends_at = NOW() - INTERVAL '2 days' + battle_days * INTERVAL '1 day'
		 WHERE id = $1`, cid); err != nil {
		t.Fatalf("backdate battle: %v", err)
	}
	return cid, resp.ID
}

func postVote(t *testing.T, voterID, cid, responseID, side string) *httptest.ResponseRecorder {
	t.Helper()
	body, _ := json.Marshal(map[string]string{
		"challengeId": cid, "responseId": responseID, "side": side,
	})
	r := httptest.NewRequest("POST", "/api/v1/challenges/vote", bytes.NewReader(body))
	w := httptest.NewRecorder()
	VoteChallengeHandler(w, withUser(r, voterID, "voter"+voterID))
	return w
}

func storedVote(t *testing.T, cid string, voterID int) (responseID *int) {
	t.Helper()
	if err := db.QueryRow(`SELECT response_id FROM challenge_votes
		WHERE challenge_id = $1 AND voter_id = $2`, cid, voterID).Scan(&responseID); err != nil {
		t.Fatalf("read vote: %v", err)
	}
	return responseID
}

// ── votes ─────────────────────────────────────────────────────────────────

func TestVote_TheCreatorCanBeVotedFor(t *testing.T) {
	defer withDB(t)()
	cid, rid := liveBattle(t)
	voter(t, 5001, 5)
	voter(t, 5002, 5)

	// The app on phones today names the creator by sending the challenge's
	// own id as the response. That used to fail, or land on a stranger.
	if w := postVote(t, "5001", cid, cid, ""); w.Code != http.StatusOK {
		t.Fatalf("vote for the creator (old form) got %d: %s", w.Code, w.Body.String())
	}
	if got := storedVote(t, cid, 5001); got != nil {
		t.Errorf("a vote for the creator was stored against response %d", *got)
	}
	if w := postVote(t, "5002", cid, "", "creator"); w.Code != http.StatusOK {
		t.Fatalf("vote for the creator (side) got %d: %s", w.Code, w.Body.String())
	}

	sum := GetVoteSummary(cid)
	var creatorVotes int
	for _, v := range sum {
		if v.ResponseID == cid && v.Username == "creator" {
			creatorVotes = v.Votes
		}
	}
	if creatorVotes != 2 {
		t.Errorf("the summary shows %d votes for the creator, want 2: %+v", creatorVotes, sum)
	}

	// Changing your mind moves the one vote.
	if w := postVote(t, "5002", cid, rid, ""); w.Code != http.StatusOK {
		t.Fatalf("change of vote got %d", w.Code)
	}
	if got := storedVote(t, cid, 5002); got == nil || strconv.Itoa(*got) != rid {
		t.Errorf("changed vote points at %v, want response %s", got, rid)
	}
}

func TestVote_WhatIsRefused(t *testing.T) {
	defer withDB(t)()
	cid, rid := liveBattle(t)
	voter(t, 5010, 5)
	other, otherRid := liveBattle(t)
	_ = other

	cases := []struct {
		name, voter, cid, rid, side string
		want                        int
	}{
		{"the creator voting in their own battle", "1", cid, rid, "", http.StatusForbidden},
		{"the responder voting in their own battle", "2", cid, "", "creator", http.StatusForbidden},
		{"an answer from a different battle", "5010", cid, otherRid, "", http.StatusBadRequest},
	}
	for _, c := range cases {
		if w := postVote(t, c.voter, c.cid, c.rid, c.side); w.Code != c.want {
			t.Errorf("%s: got %d (%s), want %d", c.name, w.Code, w.Body.String(), c.want)
		}
	}

	open := auditChallenge(t, map[string]any{"subject": "nobody answered"})
	if w := postVote(t, "5010", open, "", "creator"); w.Code != http.StatusConflict {
		t.Errorf("a vote before anyone accepted got %d, want 409", w.Code)
	}
	if _, err := db.Exec(`UPDATE challenges SET voting_ends_at = NOW() - INTERVAL '1 minute' WHERE id = $1`, cid); err != nil {
		t.Fatal(err)
	}
	if w := postVote(t, "5010", cid, rid, ""); w.Code != http.StatusConflict {
		t.Errorf("a vote after the battle ended got %d, want 409", w.Code)
	}
}

// ── the clock ─────────────────────────────────────────────────────────────

func TestAccept_StartsTheClockForTheChosenLength(t *testing.T) {
	defer withDB(t)()
	cid := auditChallenge(t, map[string]any{"subject": "a fortnight"})
	if _, err := db.Exec(`UPDATE challenges SET battle_days = 14 WHERE id = $1`, cid); err != nil {
		t.Fatal(err)
	}
	if _, err := AcceptChallenge(AcceptChallengePayload{
		ChallengeID: cid, ResponderID: "2", VideoURL: "https://v/a.mp4", DurationMs: 4000,
	}); err != nil {
		t.Fatalf("AcceptChallenge: %v", err)
	}
	var accepted, ends time.Time
	var status string
	if err := db.QueryRow(`SELECT status, accepted_at, voting_ends_at FROM challenges WHERE id = $1`, cid).
		Scan(&status, &accepted, &ends); err != nil {
		t.Fatalf("the battle has no clock: %v", err)
	}
	if status != "active" {
		t.Errorf("status %q after the first answer", status)
	}
	if d := ends.Sub(accepted); d < 14*24*time.Hour-time.Minute || d > 14*24*time.Hour+time.Minute {
		t.Errorf("voting runs %v, want the chosen 14 days", d)
	}

	// A second answer joins the battle; it does not restart it.
	if _, err := AcceptChallenge(AcceptChallengePayload{
		ChallengeID: cid, ResponderID: "3", VideoURL: "https://v/b.mp4", DurationMs: 4000,
	}); err != nil {
		t.Fatalf("second answer: %v", err)
	}
	var ends2 time.Time
	if err := db.QueryRow(`SELECT voting_ends_at FROM challenges WHERE id = $1`, cid).Scan(&ends2); err != nil {
		t.Fatal(err)
	}
	if !ends2.Equal(ends) {
		t.Error("a second answer moved the end of the battle")
	}
}

func TestCreate_BattleLengthStaysInsideTheRules(t *testing.T) {
	defer withDB(t)()
	for asked, want := range map[int]int{0: 7, 3: 7, 14: 14, 45: 30} {
		ch, err := CreateChallenge(CreateChallengePayload{
			CreatorID: "1", VideoURL: "https://v/c.mp4", Prefix: "can you", Subject: "length",
			Visibility: "arena", Category: "comedy", BattleDays: asked,
		})
		if err != nil {
			t.Fatalf("CreateChallenge: %v", err)
		}
		var days int
		if err := db.QueryRow(`SELECT battle_days FROM challenges WHERE id = $1`, ch.ID).Scan(&days); err != nil {
			t.Fatal(err)
		}
		if days != want {
			t.Errorf("asked for %d days, stored %d, want %d", asked, days, want)
		}
	}
}

// ── standings ─────────────────────────────────────────────────────────────

func TestStandings_CountOnlyWhatIsGenuine(t *testing.T) {
	defer withDB(t)()
	cid, rid := liveBattle(t)
	ridN, _ := strconv.Atoi(rid)
	// Both competitors are established accounts, as real ones would be. Left
	// brand new, their own likes are thrown out as a new account's, and the
	// rule against liking your own battle is never what removes them.
	if _, err := db.Exec(`UPDATE users SET created_at = NOW() - INTERVAL '90 days' WHERE id IN (1, 2)`); err != nil {
		t.Fatal(err)
	}

	voter(t, 5101, 6)                                      // a regular, votes creator
	voter(t, 5102, 6)                                      // a regular, votes the answer
	voter(t, 5103, 0)                                      // one day of history, votes the answer: counts half
	voter(t, 5104, 6)                                      // never watched, votes creator: removed
	seedAccount(t, 5105, time.Now().Add(-24*time.Hour), 0) // made after the start: removed

	watched(t, 5101, "challenge", cid, 3000, `{"creatorMs":3000}`)
	watched(t, 5102, "response", rid, 0, "")
	watched(t, 5103, "challenge", cid, 0, "")
	watched(t, 5105, "challenge", cid, 0, "")
	voter(t, 5106, 6) // watches but does not vote
	if _, err := db.Exec(`
		INSERT INTO feed_events (user_id, content_id, content_type, event_type, watch_duration_ms)
		VALUES ('5102', $1, 'response', 'complete', 5000),
		       ('5103', $1, 'response', 'share', 0),
		       ('5106', $2, 'challenge', 'view', 9000)`, rid, cid); err != nil {
		t.Fatal(err)
	}
	// 5106's view above is UNTAGGED: from before the app said which side it
	// was for, so it must not be counted for either. 5104 has no event on
	// this battle at all.
	//
	// On the battle reel itself: 5107 flicked past the creator (half a
	// second) and watched the answer; 5108 shared while the creator was on
	// screen; 5109's event carries a value that is not a number, which must
	// count for nobody and must not stop everybody else being counted.
	voter(t, 5107, 6)
	voter(t, 5108, 6)
	voter(t, 5109, 6)
	for _, ev := range []struct{ user, typ, meta string }{
		{"5107", "view", `{"creatorMs": 500, "opponentMs": 4000, "responseId": "` + rid + `"}`},
		{"5108", "share", `{"side": "creator", "responseId": "` + rid + `"}`},
		{"5109", "view", `{"creatorMs": "lots", "opponentMs": 1.5, "responseId": "` + rid + `"}`},
	} {
		if _, err := db.Exec(`
			INSERT INTO feed_events (user_id, content_id, content_type, event_type, metadata, created_at)
			VALUES ($1, $2, 'challenge', $3, $4::jsonb, NOW() - INTERVAL '1 hour')`,
			ev.user, cid, ev.typ, ev.meta); err != nil {
			t.Fatal(err)
		}
	}

	vote := func(v int, side any) {
		if _, err := db.Exec(`INSERT INTO challenge_votes (challenge_id, response_id, voter_id)
			VALUES ($1, $2, $3)`, cid, side, v); err != nil {
			t.Fatalf("seed vote %d: %v", v, err)
		}
	}
	vote(5101, nil)
	vote(5102, ridN)
	vote(5103, ridN)
	vote(5104, nil)
	vote(5105, ridN)
	vote(1, ridN) // the creator, before the endpoint refused it

	for _, like := range []string{
		fmt.Sprintf(`INSERT INTO challenge_likes (challenge_id, user_id) VALUES (%s, 5101)`, cid),
		fmt.Sprintf(`INSERT INTO challenge_likes (challenge_id, user_id) VALUES (%s, 1)`, cid),
		fmt.Sprintf(`INSERT INTO challenge_response_likes (response_id, user_id) VALUES (%s, 5102)`, rid),
		fmt.Sprintf(`INSERT INTO challenge_response_likes (response_id, user_id) VALUES (%s, 5105)`, rid),
		fmt.Sprintf(`INSERT INTO challenge_response_likes (response_id, user_id) VALUES (%s, 2)`, rid),
	} {
		if _, err := db.Exec(like); err != nil {
			t.Fatalf("seed like: %v", err)
		}
	}

	id, _ := strconv.Atoi(cid)
	st, ok, err := loadBattleStandings(context.Background(), openDB(t), id)
	if err != nil || !ok {
		t.Fatalf("standings: ok=%v err=%v", ok, err)
	}
	sides := map[string]Standing{}
	for _, p := range st.Participants {
		sides[p.Role] = p
	}
	c, r := sides["creator"], sides["responder"]

	check := func(what string, got, want any) {
		t.Helper()
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Errorf("%s = %v, want %v", what, got, want)
		}
	}
	check("creator genuine votes", c.Votes, 1.0)
	check("creator raw votes", c.RawVotes, 2)
	check("creator removed", c.RemovedVotes, 1)
	check("answer genuine votes", r.Votes, 1.5)
	check("answer raw votes", r.RawVotes, 4)
	check("answer removed", r.RemovedVotes, 2)
	check("creator likes (not their own)", c.Likes, 1)
	check("answer likes (not the fresh account's, not their own)", r.Likes, 1)
	check("creator views (tagged only, 2s or more)", c.Views, 1)
	check("answer views (on its own, and on the reel)", r.Views, 2)
	check("answer shares", r.Shares, 1)
	check("creator shares (from the reel)", c.Shares, 1)
	check("removed for not watching", st.Removed[reasonNoWatch], 1)
	check("removed as a new account", st.Removed[reasonNewAccount], 1)
	check("removed as own battle", st.Removed[reasonOwnBattle], 1)
	if !r.Leading || c.Leading {
		t.Errorf("leading: creator %v, answer %v — the answer has more genuine votes", c.Leading, r.Leading)
	}
	if st.EndsAt == nil || st.BattleDays != 7 {
		t.Errorf("the standings do not say when voting closes: %+v", st)
	}

	// And the same through the endpoint, which is what the app calls.
	req := mux.SetURLVars(httptest.NewRequest("GET", "/x", nil), map[string]string{"id": cid})
	w := httptest.NewRecorder()
	BattleStandingsHandler(w, req)
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"removed"`) {
		t.Errorf("standings endpoint: %d %s", w.Code, w.Body.String())
	}
}

func TestStandings_ABurstOfThrowawaysIsRemoved(t *testing.T) {
	defer withDB(t)()
	cid, _ := liveBattle(t)

	// Three accounts made within the same hour, well before the battle, each
	// with two days of history: old enough to pass every single-vote rule,
	// thin enough to be part of a burst.
	made := time.Now().Add(-10 * 24 * time.Hour)
	for i, id := range []int{5601, 5602, 5603} {
		seedAccount(t, id, made.Add(time.Duration(i)*15*time.Minute), 1)
	}
	voter(t, 5604, 6) // a regular, for the same side
	for _, id := range []int{5601, 5602, 5603, 5604} {
		watched(t, id, "challenge", cid, 3000, `{"creatorMs":3000}`)
		if _, err := db.Exec(`INSERT INTO challenge_votes (challenge_id, response_id, voter_id)
			VALUES ($1, NULL, $2)`, cid, id); err != nil {
			t.Fatal(err)
		}
	}

	id, _ := strconv.Atoi(cid)
	st, ok, err := loadBattleStandings(context.Background(), openDB(t), id)
	if err != nil || !ok {
		t.Fatalf("standings: ok=%v err=%v", ok, err)
	}
	var c Standing
	for _, p := range st.Participants {
		if p.Role == "creator" {
			c = p
		}
	}
	if c.RawVotes != 4 || c.Votes != 1 || st.Removed[reasonBurst] != 3 {
		t.Errorf("creator: %v genuine of %d cast, %d removed as a burst — "+
			"want the regular's 1 of 4, and the three made in one hour removed",
			c.Votes, c.RawVotes, st.Removed[reasonBurst])
	}
}

func TestStandings_ReadWhatTheAppSends(t *testing.T) {
	// The whole wire, from the app's event to the count: the JSON exactly as
	// the app's EventTracker builds it (see BattleFaceClock in the app),
	// decoded and saved by the server's own event code, then counted. If the
	// server stopped saving an event's details, or the app and the count
	// disagreed about a name, every battle view would count for nobody — and
	// nothing would say so.
	defer withDB(t)()
	cid, rid := liveBattle(t)
	voter(t, 5901, 6)
	voter(t, 5902, 6)

	send := func(user, typ, meta string) {
		t.Helper()
		var ev FeedEvent
		body := `{"contentId": "` + cid + `", "contentType": "challenge", "eventType": "` + typ + `",
			"watchDurationMs": 6000, "totalDurationMs": 9000, "metadata": ` + meta + `}`
		if err := json.Unmarshal([]byte(body), &ev); err != nil {
			t.Fatal(err)
		}
		ev.UserID = user
		if err := recordFeedEvent(ev); err != nil {
			t.Fatalf("saving the event: %v", err)
		}
	}
	// 5901 watched the creator for 2.5s, then the answer for 3.5s.
	send("5901", "view", `{"responseId": "`+rid+`", "creatorMs": 2500, "opponentMs": 3500}`)
	// 5902 finished the answer, and shared while it was on screen.
	send("5902", "complete", `{"side": "opponent", "responseId": "`+rid+`"}`)
	send("5902", "share", `{"side": "opponent", "responseId": "`+rid+`"}`)

	id, _ := strconv.Atoi(cid)
	st, _, err := loadBattleStandings(context.Background(), openDB(t), id)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range st.Participants {
		wantViews, wantShares := 1, 0
		if p.Role == "responder" {
			wantViews, wantShares = 2, 1
		}
		if p.Views != wantViews || p.Shares != wantShares {
			t.Errorf("%s side: %d views, %d shares — want %d and %d",
				p.Role, p.Views, p.Shares, wantViews, wantShares)
		}
	}
}

func TestLikeAnswer_CountsForTheAnswerNotTheCreator(t *testing.T) {
	defer withDB(t)()
	cid, rid := liveBattle(t)
	voter(t, 5801, 6)

	like := func(who, responseID string) (int, map[string]any) {
		r := httptest.NewRequest("POST", "/api/v1/challenges/responses/like",
			strings.NewReader(`{"responseId": "`+responseID+`"}`))
		w := httptest.NewRecorder()
		LikeResponseHandler(w, withUser(r, who, "u"+who))
		var out map[string]any
		_ = json.Unmarshal(w.Body.Bytes(), &out)
		return w.Code, out
	}

	code, out := like("5801", rid)
	if code != http.StatusOK || out["liked"] != true || out["likes"].(float64) != 1 {
		t.Fatalf("liking an answer: %d %v", code, out)
	}
	id, _ := strconv.Atoi(cid)
	st, _, err := loadBattleStandings(context.Background(), openDB(t), id)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range st.Participants {
		want := 0
		if p.Role == "responder" {
			want = 1
		}
		if p.Likes != want {
			t.Errorf("%s side has %d likes, want %d — a like on the answer "+
				"must count for the answer", p.Role, p.Likes, want)
		}
	}

	// Pressing again takes it back.
	if code, out := like("5801", rid); code != http.StatusOK || out["liked"] != false || out["likes"].(float64) != 0 {
		t.Errorf("un-liking: %d %v", code, out)
	}
	if code, _ := like("5801", "999999"); code != http.StatusNotFound {
		t.Errorf("liking an answer that does not exist got %d, want 404", code)
	}
}

func TestFeed_ABattleCarriesItsAnswersOwnLikes(t *testing.T) {
	// The reel's heart shows the answer's likes while the answer is on
	// screen, so the feed has to send them.
	defer withDB(t)()
	cid, rid := liveBattle(t)
	ridN, _ := strconv.Atoi(rid)
	for _, v := range []int{5951, 5952} {
		voter(t, v, 3)
		if _, _, err := toggleResponseLike(context.Background(), ridN, v); err != nil {
			t.Fatal(err)
		}
	}
	items := []HomeFeedItem{{Type: "challenge", Challenge: &Challenge{ID: cid}}}
	populateTopResponses(items)
	if got := items[0].Challenge; got.TopResponseID != rid || got.TopResponseLikes != 2 {
		t.Errorf("feed item for the battle: answer %q with %d likes, want %s with 2",
			got.TopResponseID, got.TopResponseLikes, rid)
	}
}

// ── deciding ──────────────────────────────────────────────────────────────

func TestResolve_DecidesOnceAndUpdatesTheRecords(t *testing.T) {
	defer withDB(t)()
	cid, _ := liveBattle(t)
	for _, v := range []int{5201, 5202} {
		voter(t, v, 5)
		watched(t, v, "challenge", cid, 3000, `{"creatorMs":3000}`)
		if _, err := db.Exec(`INSERT INTO challenge_votes (challenge_id, response_id, voter_id)
			VALUES ($1, NULL, $2)`, cid, v); err != nil {
			t.Fatal(err)
		}
	}

	ctx := context.Background()
	id, _ := strconv.Atoi(cid)
	if ok, err := resolveBattle(ctx, id); err != nil || ok {
		t.Fatalf("decided a battle still being voted on: ok=%v err=%v", ok, err)
	}

	if _, err := db.Exec(`UPDATE challenges SET voting_ends_at = NOW() - INTERVAL '1 minute' WHERE id = $1`, cid); err != nil {
		t.Fatal(err)
	}
	// One round of the resolver, the way the running server does it.
	battleResolverRound(ctx, 1)
	var decided bool
	if err := db.QueryRow(`SELECT resolved_at IS NOT NULL FROM challenges WHERE id = $1`, cid).Scan(&decided); err != nil {
		t.Fatal(err)
	}
	if !decided {
		t.Fatal("a round of the resolver left a battle whose time was up undecided")
	}

	type rec struct {
		wins, losses, rating int
		league               string
	}
	read := func(uid int) rec {
		var r rec
		if err := db.QueryRow(`SELECT wins, losses, rating, league FROM users WHERE id = $1`, uid).
			Scan(&r.wins, &r.losses, &r.rating, &r.league); err != nil {
			t.Fatal(err)
		}
		return r
	}
	creator, responder := read(1), read(2)
	if creator.wins != 1 || creator.rating != 1016 || creator.league != "Bronze" {
		t.Errorf("creator after winning: %+v", creator)
	}
	if responder.losses != 1 || responder.rating != 984 {
		t.Errorf("responder after losing: %+v", responder)
	}
	// And what a profile reads back, which is a different query.
	if u, ok := GetUserByID("1"); !ok || u.Rating != 1016 || u.League != "Bronze" || u.Wins != 1 {
		t.Errorf("the creator's profile shows rating %d, league %q, %d wins — "+
			"want what the battle just wrote", u.Rating, u.League, u.Wins)
	}
	if u, ok := GetUserByUsername("liker2"); !ok || u.Rating != 984 || u.Losses != 1 {
		t.Errorf("looked up by name, the responder shows rating %d and %d losses, "+
			"want 984 and 1 (found: %v)", u.Rating, u.Losses, ok)
	}
	var status string
	var outcomes int
	if err := db.QueryRow(`SELECT status FROM challenges WHERE id = $1`, cid).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM battle_results WHERE challenge_id = $1`, cid).Scan(&outcomes); err != nil {
		t.Fatal(err)
	}
	if status != "completed" || outcomes != 2 {
		t.Errorf("battle left as %q with %d results", status, outcomes)
	}

	// Again: nothing changes.
	if n, _ := resolveDueBattles(ctx, 10); n != 0 {
		t.Errorf("a decided battle was decided again (%d)", n)
	}
	if again := read(1); again != creator {
		t.Errorf("deciding twice changed the record: %+v → %+v", creator, again)
	}
}

func TestResolve_BoughtVotesCostRatingAndLeaveAStrike(t *testing.T) {
	defer withDB(t)()
	cid, rid := liveBattle(t)
	ridN, _ := strconv.Atoi(rid)

	// The creator's side: three accounts made after the battle started. All
	// three are removed, which is every vote they had — a scheme, not noise.
	for _, v := range []int{5701, 5702, 5703} {
		seedAccount(t, v, time.Now().Add(-time.Hour), 0)
		watched(t, v, "challenge", cid, 3000, `{"creatorMs":3000}`)
		if _, err := db.Exec(`INSERT INTO challenge_votes (challenge_id, response_id, voter_id)
			VALUES ($1, NULL, $2)`, cid, v); err != nil {
			t.Fatal(err)
		}
	}
	// The answer: one regular.
	voter(t, 5704, 6)
	watched(t, 5704, "response", rid, 3000, "")
	if _, err := db.Exec(`INSERT INTO challenge_votes (challenge_id, response_id, voter_id)
		VALUES ($1, $2, 5704)`, cid, ridN); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE challenges SET voting_ends_at = NOW() - INTERVAL '1 minute' WHERE id = $1`, cid); err != nil {
		t.Fatal(err)
	}
	// A regular's view of the creator's side, recorded after voting closed.
	voter(t, 5705, 6)
	if _, err := db.Exec(`
		INSERT INTO feed_events (user_id, content_id, content_type, event_type, watch_duration_ms, metadata)
		VALUES ('5705', $1, 'challenge', 'view', 9000, '{"creatorMs":9000}')`, cid); err != nil {
		t.Fatal(err)
	}
	id, _ := strconv.Atoi(cid)
	if ok, err := resolveBattle(context.Background(), id); err != nil || !ok {
		t.Fatalf("resolveBattle: ok=%v err=%v", ok, err)
	}
	// Neither the throwaways' views nor the late one count.
	var creatorViews, answerViews int
	if err := db.QueryRow(`
		SELECT MAX(views) FILTER (WHERE role = 'creator'), MAX(views) FILTER (WHERE role = 'responder')
		  FROM battle_results WHERE challenge_id = $1`, cid).Scan(&creatorViews, &answerViews); err != nil {
		t.Fatal(err)
	}
	if creatorViews != 0 || answerViews != 1 {
		t.Errorf("views counted: creator %d, answer %d — want 0 and 1: the "+
			"throwaways' views and the one after the end do not count", creatorViews, answerViews)
	}

	type after struct {
		rating, strikes int
		flagged         bool
	}
	read := func(uid int) after {
		var a after
		if err := db.QueryRow(`
			SELECT u.rating, u.integrity_strikes, br.flagged
			  FROM users u JOIN battle_results br ON br.user_id = u.id
			 WHERE u.id = $1 AND br.challenge_id = $2`, uid, cid).
			Scan(&a.rating, &a.strikes, &a.flagged); err != nil {
			t.Fatal(err)
		}
		return a
	}
	cheat, clean := read(1), read(2)
	if !cheat.flagged || cheat.strikes != 1 || cheat.rating != 1000-16-integrityPenalty {
		t.Errorf("the side whose every vote was bought: %+v — want flagged, one "+
			"strike, and the loss plus the %d penalty (rating %d)",
			cheat, integrityPenalty, 1000-16-integrityPenalty)
	}
	if clean.flagged || clean.strikes != 0 || clean.rating != 1016 {
		t.Errorf("the clean side: %+v — want no flag, no strike, and the plain win", clean)
	}
}

func TestDecay_ATopRatingHasToBeDefended(t *testing.T) {
	defer withDB(t)()
	seed := func(id, rating int, lastBattle time.Duration) {
		if _, err := db.Exec(`
			INSERT INTO users (id, username, password, rating, wins, league, last_battle_at)
			VALUES ($1, $2, 'x', $3, 10, 'Platinum', NOW() - $4 * INTERVAL '1 hour')`,
			id, fmt.Sprintf("champ%d", id), rating, int(lastBattle.Hours())); err != nil {
			t.Fatal(err)
		}
	}
	seed(5301, 1300, 10*24*time.Hour) // idle
	seed(5302, 1300, 2*24*time.Hour)  // battling
	seed(5303, 1210, 10*24*time.Hour) // idle, just above the line

	ctx := context.Background()
	if _, err := decayIdleRatings(ctx); err != nil {
		t.Fatal(err)
	}
	rating := func(id int) (int, string) {
		var r int
		var l string
		if err := db.QueryRow(`SELECT rating, league FROM users WHERE id = $1`, id).Scan(&r, &l); err != nil {
			t.Fatal(err)
		}
		return r, l
	}
	if r, l := rating(5301); r != 1285 || l != "Gold" {
		t.Errorf("an idle 1300 is now %d (%s), want 1285 and the league to follow", r, l)
	}
	if r, _ := rating(5302); r != 1300 {
		t.Errorf("somebody who battled this week lost rating: %d", r)
	}
	if r, _ := rating(5303); r != decayAbove {
		t.Errorf("decay went below its floor: %d", r)
	}
	if _, err := decayIdleRatings(ctx); err != nil {
		t.Fatal(err)
	}
	if r, _ := rating(5301); r != 1285 {
		t.Errorf("charged twice in one week: %d", r)
	}
}

// ── the profile's tabs ────────────────────────────────────────────────────

func getBattles(t *testing.T, viewer, owner, tab string) map[string]any {
	t.Helper()
	r := httptest.NewRequest("GET", "/api/v1/users/"+owner+"/battles?tab="+tab, nil)
	r = mux.SetURLVars(r, map[string]string{"id": owner})
	w := httptest.NewRecorder()
	UserBattlesHandler(w, withUser(r, viewer, "viewer"))
	if w.Code != http.StatusOK {
		t.Fatalf("battles %s: %d %s", tab, w.Code, w.Body.String())
	}
	var out map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestProfileBattles_EveryTabAndTheRecord(t *testing.T) {
	defer withDB(t)()
	open := auditChallenge(t, map[string]any{"subject": "still open"})
	private := auditChallenge(t, map[string]any{"subject": "friends only"})
	if _, err := db.Exec(`UPDATE challenges SET visibility = 'arena' WHERE id = $1;`, open); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE challenges SET visibility = 'friends' WHERE id = $1`, private); err != nil {
		t.Fatal(err)
	}
	live, _ := liveBattle(t)
	won, _ := liveBattle(t)
	if _, err := db.Exec(`UPDATE challenges SET visibility = 'arena' WHERE id IN ($1, $2)`, live, won); err != nil {
		t.Fatal(err)
	}
	voter(t, 5401, 5)
	watched(t, 5401, "challenge", won, 3000, `{"creatorMs":3000}`)
	if _, err := db.Exec(`INSERT INTO challenge_votes (challenge_id, voter_id) VALUES ($1, 5401)`, won); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE challenges SET voting_ends_at = NOW() - INTERVAL '1 minute' WHERE id = $1`, won); err != nil {
		t.Fatal(err)
	}
	if _, err := resolveDueBattles(context.Background(), 10); err != nil {
		t.Fatal(err)
	}

	ids := func(res map[string]any) []string {
		var out []string
		for _, b := range res["battles"].([]any) {
			out = append(out, b.(map[string]any)["challengeId"].(string))
		}
		return out
	}
	has := func(list []string, id string) bool {
		for _, x := range list {
			if x == id {
				return true
			}
		}
		return false
	}

	own := getBattles(t, "1", "1", "open")
	if !has(ids(own), open) || !has(ids(own), private) {
		t.Errorf("the owner's Open tab %v is missing %s or %s", ids(own), open, private)
	}
	visitor := getBattles(t, "5401", "1", "open")
	if has(ids(visitor), private) {
		t.Error("a friends-only challenge was shown to a visitor")
	}
	if !has(ids(visitor), open) {
		t.Error("a visitor could not see an arena challenge")
	}

	liveTab := getBattles(t, "5401", "1", "live")
	if l := ids(liveTab); !has(l, live) || has(l, won) {
		t.Errorf("Live tab %v: want %s and not the decided %s", l, live, won)
	}
	// Nothing about a live battle is stored yet: who it is against and the
	// score come from counting it now.
	for _, b := range liveTab["battles"].([]any) {
		card := b.(map[string]any)
		if card["challengeId"] == live && card["opponent"] != "liker2" {
			t.Errorf("a live card says it is against %q, want the person who answered", card["opponent"])
		}
	}
	w := getBattles(t, "5401", "1", "won")
	if !has(ids(w), won) {
		t.Errorf("Won tab %v is missing %s", ids(w), won)
	}
	card := w["battles"].([]any)[0].(map[string]any)
	if card["ratingChange"].(float64) <= 0 || card["opponent"].(string) == "" {
		t.Errorf("a won card does not say what it earned or against whom: %v", card)
	}
	if ids(getBattles(t, "5401", "2", "lost"))[0] != won {
		t.Error("the loser's Lost tab does not show the battle they lost")
	}

	sum := w["summary"].(map[string]any)
	if sum["wins"].(float64) != 1 || sum["streak"].(float64) != 1 || sum["streakOf"].(string) != "won" {
		t.Errorf("summary %v", sum)
	}
	counts := sum["counts"].(map[string]any)
	if counts["won"].(float64) != 1 || counts["live"].(float64) != 1 {
		t.Errorf("tab counts %v", counts)
	}
}

func TestExtend_LongerNeverShorterAndOnlyByTheCreator(t *testing.T) {
	defer withDB(t)()
	cid, _ := liveBattle(t)
	var before time.Time
	if err := db.QueryRow(`SELECT voting_ends_at FROM challenges WHERE id = $1`, cid).Scan(&before); err != nil {
		t.Fatal(err)
	}
	extend := func(who string, days int) int {
		body := fmt.Sprintf(`{"days": %d}`, days)
		r := httptest.NewRequest("POST", "/x", strings.NewReader(body))
		r = mux.SetURLVars(r, map[string]string{"id": cid})
		w := httptest.NewRecorder()
		ExtendBattleHandler(w, withUser(r, who, "u"+who))
		return w.Code
	}
	if c := extend("2", 14); c != http.StatusForbidden {
		t.Errorf("the responder changed the length: %d", c)
	}
	if c := extend("1", 3); c != http.StatusBadRequest {
		t.Errorf("a battle was made shorter: %d", c)
	}
	if c := extend("1", 14); c != http.StatusOK {
		t.Fatalf("the creator could not extend: %d", c)
	}
	var after time.Time
	if err := db.QueryRow(`SELECT voting_ends_at FROM challenges WHERE id = $1`, cid).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if d := after.Sub(before); d < 7*24*time.Hour-time.Minute || d > 7*24*time.Hour+time.Minute {
		t.Errorf("extending 7→14 days moved the end by %v", d)
	}
	if c := extend("1", 90); c != http.StatusOK {
		t.Fatalf("extend to 90: %d", c)
	}
	var days int
	if err := db.QueryRow(`SELECT battle_days FROM challenges WHERE id = $1`, cid).Scan(&days); err != nil {
		t.Fatal(err)
	}
	if days != battleMaxDays {
		t.Errorf("asked for 90, got %d, want the %d-day ceiling", days, battleMaxDays)
	}

	// A battle that was already running when battles got an end: answered a
	// month ago, given a fresh week by migration 011, three days of it left.
	old, _ := liveBattle(t)
	if _, err := db.Exec(`
		UPDATE challenges
		   SET accepted_at = NOW() - INTERVAL '30 days',
		       voting_ends_at = NOW() + INTERVAL '3 days'
		 WHERE id = $1`, old); err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest("POST", "/x", strings.NewReader(`{"days": 10}`))
	r = mux.SetURLVars(r, map[string]string{"id": old})
	w := httptest.NewRecorder()
	ExtendBattleHandler(w, withUser(r, "1", "u1"))
	var ends time.Time
	if err := db.QueryRow(`SELECT voting_ends_at FROM challenges WHERE id = $1`, old).Scan(&ends); err != nil {
		t.Fatal(err)
	}
	if left := time.Until(ends); w.Code != http.StatusOK || left < 6*24*time.Hour-time.Hour || left > 6*24*time.Hour+time.Hour {
		t.Errorf("a month-old battle with 3 days left, extended 7→10 days: %d, "+
			"%v left — want 6 days, not an end in the past", w.Code, left.Round(time.Hour))
	}
	if !strings.Contains(w.Body.String(), `"endsAt"`) {
		t.Errorf("the reply does not say when it now ends: %s", w.Body.String())
	}
}

// ── migration 011 puts back the votes the bug misfiled ────────────────────

func TestMigration011_RepairsTheCreatorVotes(t *testing.T) {
	defer withDB(t)()
	cid, _ := liveBattle(t)
	elsewhere, _ := liveBattle(t)
	cidN, _ := strconv.Atoi(cid)
	voter(t, 5501, 3)
	voter(t, 5502, 3)

	// The bug's two shapes. An answer in ANOTHER battle that happens to carry
	// this battle's number, so the creator vote the app sent landed on it...
	if _, err := db.Exec(`INSERT INTO challenge_responses (id, challenge_id, responder_id, video_url)
		VALUES ($1, $2, 3, 'https://v/x.mp4') ON CONFLICT (id) DO NOTHING`, cidN, elsewhere); err != nil {
		t.Fatalf("seed colliding answer: %v", err)
	}
	var otherRid int
	if err := db.QueryRow(`SELECT id FROM challenge_responses WHERE challenge_id = $1 AND id <> $2 LIMIT 1`,
		elsewhere, cidN).Scan(&otherRid); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO challenge_votes (challenge_id, response_id, voter_id) VALUES
		($1, $2, 5501), ($1, $3, 5502)`, cid, cidN, otherRid); err != nil {
		t.Fatalf("seed misfiled votes: %v", err)
	}

	sqlText, err := os.ReadFile("migrations/011_battles_have_an_end.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(string(sqlText)); err != nil {
		t.Fatalf("re-running migration 011: %v", err)
	}

	if got := storedVote(t, cid, 5501); got != nil {
		t.Errorf("the misfiled creator vote still points at response %d", *got)
	}
	var left int
	if err := db.QueryRow(`SELECT COUNT(*) FROM challenge_votes WHERE challenge_id = $1 AND voter_id = 5502`, cid).Scan(&left); err != nil {
		t.Fatal(err)
	}
	if left != 0 {
		t.Error("a vote naming an answer from a different battle was kept")
	}
}

// ── wired in ──────────────────────────────────────────────────────────────

// codeOf reads a Go file with its comment lines removed, so a check below
// matches code and never the comment explaining it.
func codeOf(t *testing.T, path string) string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var code []string
	for _, l := range strings.Split(string(raw), "\n") {
		if !strings.HasPrefix(strings.TrimSpace(l), "//") {
			code = append(code, l)
		}
	}
	return strings.Join(code, "\n")
}

func TestBattles_AreWiredIn(t *testing.T) {
	src := codeOf(t, "main.go")
	for _, want := range []string{
		"startBattleResolver()",
		`"/challenges/{id}/standings", BattleStandingsHandler`,
		`"/challenges/{id}/battle-length", authed(ExtendBattleHandler)`,
		`"/users/{id}/battles", authed(UserBattlesHandler)`,
		`"/challenges/responses/like", authed(LikeResponseHandler)`,
	} {
		if !strings.Contains(src, want) {
			t.Errorf("main.go no longer has %s — nothing reaches it", want)
		}
	}

	// The resolver's loop is the one piece no test can run: it waits ten
	// minutes between rounds. So check it still calls the round the tests
	// DO run.
	battles := codeOf(t, "battles.go")
	start := strings.Index(battles, "func startBattleResolver() {")
	if start < 0 {
		t.Fatal("startBattleResolver is gone from battles.go")
	}
	body := battles[start:]
	if end := strings.Index(body, "\n}\n"); end > 0 {
		body = body[:end]
	}
	if !strings.Contains(body, "battleResolverRound(") {
		t.Error("startBattleResolver no longer runs battleResolverRound — " +
			"battles would never be decided")
	}
}
