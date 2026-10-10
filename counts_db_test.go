package main

// One count for everything: every screen shows the same numbers, and each is
// counted once. All against real Postgres, through the real handlers.

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/gorilla/mux"
)

// watch sends what the reel sends when someone leaves a card.
func watch(t *testing.T, uid, cid string, body map[string]any) {
	t.Helper()
	body["contentId"] = cid
	body["contentType"] = "challenge"
	if _, ok := body["watchTime"]; !ok {
		body["watchTime"] = 4000
	}
	b, _ := json.Marshal(body)
	r := httptest.NewRequest("POST", "/api/v1/watch", bytes.NewReader(b))
	w := httptest.NewRecorder()
	HandleWatchEvent(w, withUser(r, uid, "watcher"+uid))
	if w.Code != http.StatusCreated {
		t.Fatalf("watch got %d: %s", w.Code, w.Body.String())
	}
}

func share(t *testing.T, uid, cid, rid string) int {
	t.Helper()
	b, _ := json.Marshal(map[string]string{"challengeId": cid, "responseId": rid})
	r := httptest.NewRequest("POST", "/api/v1/challenges/share", bytes.NewReader(b))
	w := httptest.NewRecorder()
	ShareChallengeHandler(w, withUser(r, uid, "sharer"+uid))
	if w.Code != http.StatusOK {
		t.Fatalf("share got %d: %s", w.Code, w.Body.String())
	}
	var out struct{ Shares int }
	_ = json.Unmarshal(w.Body.Bytes(), &out)
	return out.Shares
}

// standingsJSON is the live score as the app receives it.
func standingsJSON(t *testing.T, cid string) map[string]any {
	t.Helper()
	r := httptest.NewRequest("GET", "/api/v1/challenges/"+cid+"/standings", nil)
	r = mux.SetURLVars(r, map[string]string{"id": cid})
	w := httptest.NewRecorder()
	BattleStandingsHandler(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("standings got %d: %s", w.Code, w.Body.String())
	}
	var out map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	return out
}

func side(t *testing.T, st map[string]any, i int) map[string]any {
	t.Helper()
	ps := st["participants"].([]any)
	return ps[i].(map[string]any)
}

// detailJSON is the battle page's own read of the challenge.
func detailJSON(t *testing.T, cid, viewer string) map[string]any {
	t.Helper()
	r := httptest.NewRequest("GET", "/api/v1/challenges/"+cid, nil)
	r = mux.SetURLVars(r, map[string]string{"id": cid})
	if viewer != "" {
		r = withUser(r, viewer, "viewer"+viewer)
	}
	w := httptest.NewRecorder()
	GetChallengeDetailHandler(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("detail got %d: %s", w.Code, w.Body.String())
	}
	var out map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	return out["challenge"].(map[string]any)
}

func num(v any) int {
	f, _ := v.(float64)
	return int(f)
}

func TestCounts_AViewIsOnePersonOneVideoOnceADay(t *testing.T) {
	defer withDB(t)()
	cid, rid := liveBattle(t)
	for _, v := range []int{6001, 6002, 6003} {
		voter(t, v, 3)
	}
	// The test battle is made with some views already; count from there.
	var start int
	if err := db.QueryRow(`SELECT views FROM challenges WHERE id = $1`, cid).Scan(&start); err != nil {
		t.Fatal(err)
	}
	// 6001 watched both videos, twice over; 6002 only the answer; 6003 flicked
	// past the answer too fast to count; and an older app says nothing about
	// sides, which counts for the creator's video.
	both := map[string]any{"creatorMs": 3000, "opponentMs": 2500, "responseId": rid}
	watch(t, "6001", cid, both)
	watch(t, "6001", cid, map[string]any{"creatorMs": 3000, "opponentMs": 2500, "responseId": rid})
	watch(t, "6002", cid, map[string]any{"opponentMs": 4000, "responseId": rid})
	watch(t, "6003", cid, map[string]any{"creatorMs": 5000, "opponentMs": 400, "responseId": rid})
	watch(t, "5", cid, map[string]any{})

	// Opening the battle page is not a view, however often.
	for i := 0; i < 3; i++ {
		detailJSON(t, cid, "6001")
	}

	st := standingsJSON(t, cid)
	creator, answer := side(t, st, 0), side(t, st, 1)
	if got := num(creator["views"]) - start; got != 3 {
		t.Errorf("the creator's video shows %d views, want 3 (6001, 6003, the older app)", got)
	}
	if got := num(answer["views"]); got != 2 {
		t.Errorf("the answer shows %d views, want 2 (6001, 6002)", got)
	}
	// Everywhere else shows the card's total: the two added up.
	if got := num(detailJSON(t, cid, "")["views"]) - start; got != 5 {
		t.Errorf("the battle page shows %d views, want 5 = 3 + 2", got)
	}
}

func TestCounts_SharesOncePerPersonPerVideo(t *testing.T) {
	defer withDB(t)()
	cid, rid := liveBattle(t)
	voter(t, 6101, 3)
	voter(t, 6102, 3)
	share(t, "6101", cid, "")
	share(t, "6101", cid, "") // again: still one
	share(t, "6101", cid, rid)
	if got := share(t, "6102", cid, cid); got != 3 {
		t.Errorf("after 6101 shared both and 6102 the creator's, the total is %d, want 3", got)
	}
	st := standingsJSON(t, cid)
	if c, a := num(side(t, st, 0)["shares"]), num(side(t, st, 1)["shares"]); c != 2 || a != 1 {
		t.Errorf("shares per video: creator %d, answer %d — want 2 and 1", c, a)
	}
	if got := num(detailJSON(t, cid, "")["shareCount"]); got != 3 {
		t.Errorf("the battle page shows %d shares, want 3", got)
	}
}

func TestCounts_EveryScreenGetsTheSameCounts(t *testing.T) {
	defer withDB(t)()
	cid, rid := liveBattle(t)
	ridN, _ := strconv.Atoi(rid)
	for _, v := range []int{6201, 6202, 6203} {
		voter(t, v, 3)
	}
	if _, err := db.Exec(`
		INSERT INTO challenge_votes (challenge_id, response_id, voter_id)
		VALUES ($1, NULL, 6201), ($1, $2, 6202), ($1, $2, 6203)`, cid, ridN); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`
		INSERT INTO challenge_comments (challenge_id, author_id, text)
		VALUES ($1, 6201, 'one'), ($1, 6202, 'two')`, cid); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`
		INSERT INTO saved_challenges (user_id, challenge_id) VALUES (6203, $1)`, cid); err != nil {
		t.Fatal(err)
	}
	// The players' own likes count too: a like is a like.
	if _, err := db.Exec(`
		INSERT INTO challenge_likes (challenge_id, user_id) VALUES ($1, 2), ($1, 6201)`, cid); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`
		INSERT INTO challenge_response_likes (response_id, user_id) VALUES ($1, 1)`, ridN); err != nil {
		t.Fatal(err)
	}
	share(t, "6201", cid, rid)

	// The battle page's read.
	c := detailJSON(t, cid, "")
	want := map[string]int{
		"voteCount": 3, "commentCount": 2, "saveCount": 1, "shareCount": 1, "likes": 2,
	}
	for k, n := range want {
		if got := num(c[k]); got != n {
			t.Errorf("battle page %s = %d, want %d", k, got, n)
		}
	}
	// The same, by way of a profile's list of videos — another handler,
	// the same fill.
	list := []Challenge{{ID: cid}}
	markViewerStateChallenges("", list)
	if list[0].VoteCount != 3 || list[0].CommentCount != 2 || list[0].SaveCount != 1 ||
		list[0].ShareCount != 1 {
		t.Errorf("a list of videos got votes %d, comments %d, saves %d, shares %d",
			list[0].VoteCount, list[0].CommentCount, list[0].SaveCount, list[0].ShareCount)
	}
	// And the live score, per player, adds up to the same.
	st := standingsJSON(t, cid)
	cr, an := side(t, st, 0), side(t, st, 1)
	if v := num(cr["votes"]) + num(an["votes"]); v != 3 {
		t.Errorf("live score votes add up to %d, want 3", v)
	}
	if num(cr["likes"]) != 2 || num(an["likes"]) != 1 {
		t.Errorf("live score likes: creator %d, answer %d — want 2 and 1, the "+
			"players' own likes included", num(cr["likes"]), num(an["likes"]))
	}
	// What decides the battle still leaves the players' own likes out.
	if num(cr["countedLikes"]) != 1 || num(an["countedLikes"]) != 0 {
		t.Errorf("counted likes: creator %d, answer %d — want 1 and 0",
			num(cr["countedLikes"]), num(an["countedLikes"]))
	}
}

func TestCounts_WhoLikedVotedShared_ForAnyoneWatching(t *testing.T) {
	defer withDB(t)()
	cid, rid := liveBattle(t)
	ridN, _ := strconv.Atoi(rid)
	voter(t, 6301, 3)
	voter(t, 6302, 3)
	if _, err := db.Exec(`INSERT INTO challenge_likes (challenge_id, user_id) VALUES ($1, 6301)`, cid); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO challenge_response_likes (response_id, user_id) VALUES ($1, 6302)`, ridN); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO challenge_votes (challenge_id, response_id, voter_id)
		VALUES ($1, $2, 6301)`, cid, ridN); err != nil {
		t.Fatal(err)
	}
	share(t, "6302", cid, "")

	ask := func(viewer, what string) (int, []map[string]any) {
		r := httptest.NewRequest("GET", "/api/v1/challenges/"+cid+"/people?what="+what, nil)
		r = mux.SetURLVars(r, map[string]string{"id": cid})
		w := httptest.NewRecorder()
		ChallengePeopleHandler(w, withUser(r, viewer, "v"+viewer))
		var out struct {
			Sides []map[string]any `json:"sides"`
		}
		_ = json.Unmarshal(w.Body.Bytes(), &out)
		return w.Code, out.Sides
	}
	names := func(s map[string]any) []string {
		var out []string
		for _, p := range s["people"].([]any) {
			out = append(out, p.(map[string]any)["username"].(string))
		}
		return out
	}
	for _, c := range []struct {
		what            string
		creator, answer []string
	}{
		{"likes", []string{"voter6301"}, []string{"voter6302"}},
		{"votes", nil, []string{"voter6301"}},
		{"shares", []string{"voter6302"}, nil},
	} {
		// The poster, the answerer, and somebody outside the battle: open
		// to anyone who may watch it, the way Instagram shows who liked.
		for _, viewer := range []string{"1", "2", "6301"} {
			code, sides := ask(viewer, c.what)
			if code != http.StatusOK || len(sides) != 2 {
				t.Fatalf("%s for user %s: %d, %d sides", c.what, viewer, code, len(sides))
			}
			if got := names(sides[0]); !sameNames(got, c.creator) {
				t.Errorf("%s on the creator's video: %v, want %v", c.what, got, c.creator)
			}
			if got := names(sides[1]); !sameNames(got, c.answer) {
				t.Errorf("%s on the answer: %v, want %v", c.what, got, c.answer)
			}
		}
	}
	if code, _ := ask("1", "anything"); code != http.StatusBadRequest {
		t.Errorf("an unknown list got %d, want 400", code)
	}
}

func sameNames(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestBattleLength_CannotBeChanged(t *testing.T) {
	defer withDB(t)()
	cid, _ := liveBattle(t)
	var before string
	if err := db.QueryRow(`SELECT voting_ends_at::text FROM challenges WHERE id = $1`, cid).Scan(&before); err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(map[string]int{"days": 14})
	r := httptest.NewRequest("POST", "/api/v1/challenges/"+cid+"/battle-length", bytes.NewReader(b))
	r = mux.SetURLVars(r, map[string]string{"id": cid})
	w := httptest.NewRecorder()
	ExtendBattleHandler(w, withUser(r, "1", "creator"))
	if w.Code != http.StatusForbidden {
		t.Errorf("the creator extending got %d, want 403", w.Code)
	}
	var after string
	if err := db.QueryRow(`SELECT voting_ends_at::text FROM challenges WHERE id = $1`, cid).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if after != before {
		t.Errorf("the end moved from %s to %s", before, after)
	}
}

// The handlers above are called directly, so nothing there proves the app
// can reach them. This does: the routes, in main.go, with comments taken out
// first so a route mentioned in a comment does not count.
func TestCounts_TheNewAddressesAreRegistered(t *testing.T) {
	src, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	var code []string
	for _, l := range strings.Split(string(src), "\n") {
		if !strings.HasPrefix(strings.TrimSpace(l), "//") {
			code = append(code, l)
		}
	}
	body := strings.Join(code, "\n")
	for _, want := range []string{
		`api.HandleFunc("/challenges/{id}/people", authed(ChallengePeopleHandler)).Methods("GET", "OPTIONS")`,
		`api.HandleFunc("/challenges/share", authed(ShareChallengeHandler)).Methods("POST", "OPTIONS")`,
		`api.HandleFunc("/watch", authed(HandleWatchEvent)).Methods("POST", "OPTIONS")`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("missing route: %s", want)
		}
	}
}
