package main

// "I commented but that did not count." Every number under a video, after
// every way of moving it, read back from every screen that shows it.
//
// Each action goes through the real handler the app calls, not a row put in
// by hand, so a handler that saves to the wrong place, or a screen that
// forgets to fill a number in, turns this red. All against real Postgres.

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"

	"github.com/gorilla/mux"
)

// act sends one POST the way the app does, as [uid].
func act(t *testing.T, h http.HandlerFunc, path, uid string, body any) map[string]any {
	t.Helper()
	b, _ := json.Marshal(body)
	r := withUser(httptest.NewRequest("POST", path, bytes.NewReader(b)), uid, "voter"+uid)
	w := httptest.NewRecorder()
	h(w, r)
	if w.Code != http.StatusOK && w.Code != http.StatusCreated {
		t.Fatalf("%s as %s got %d: %s", path, uid, w.Code, w.Body.String())
	}
	var out map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &out)
	return out
}

// screen reads one GET the way the app does, as [viewer], and returns every
// copy of video [cid] in the answer — a feed nests it, a battle tab hangs it
// off a card, a list has it at the top.
func screen(t *testing.T, h http.HandlerFunc, path string, vars map[string]string,
	viewer, cid string) []map[string]any {
	t.Helper()
	r := httptest.NewRequest("GET", path, nil)
	if vars != nil {
		r = mux.SetURLVars(r, vars)
	}
	if viewer != "" {
		r = withUser(r, viewer, "voter"+viewer)
	}
	w := httptest.NewRecorder()
	h(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("%s got %d: %s", path, w.Code, w.Body.String())
	}
	var body any
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("%s: %v", path, err)
	}
	var found []map[string]any
	var walk func(v any)
	walk = func(v any) {
		switch x := v.(type) {
		case map[string]any:
			if x["id"] == cid {
				if _, isVideo := x["prefix"]; isVideo {
					found = append(found, x)
				}
			}
			for _, c := range x {
				walk(c)
			}
		case []any:
			for _, c := range x {
				walk(c)
			}
		}
	}
	walk(body)
	return found
}

func TestCounts_EveryActionShowsOnEveryScreen(t *testing.T) {
	defer withDB(t)()
	cid, rid := liveBattle(t)
	// Posted for everyone, the way the app posts ("arena"): a visitor's
	// view of a profile shows only those.
	if _, err := db.Exec(`UPDATE challenges SET visibility = 'arena' WHERE id = $1`, cid); err != nil {
		t.Fatal(err)
	}
	// 6501 does everything once; 6502 follows the creator and only looks.
	voter(t, 6501, 6)
	voter(t, 6502, 6)
	if _, err := db.Exec(`INSERT INTO follows (follower_id, following_id)
		VALUES (6502, 1) ON CONFLICT DO NOTHING`); err != nil {
		t.Fatal(err)
	}
	var startViews int
	if err := db.QueryRow(`SELECT views FROM challenges WHERE id = $1`, cid).Scan(&startViews); err != nil {
		t.Fatal(err)
	}

	// ── every action, through the handler the app calls ──────────────────
	act(t, LikeChallengeHandler, "/api/v1/challenges/like", "6501",
		map[string]string{"challengeId": cid})
	act(t, LikeResponseHandler, "/api/v1/challenges/responses/like", "6501",
		map[string]string{"responseId": rid})
	act(t, AddChallengeCommentHandler, "/api/v1/challenges/comments", "6501",
		map[string]string{"challengeId": cid, "text": "this counts"})
	act(t, SaveChallengeHandler, "/api/v1/save", "6501",
		map[string]string{"challengeId": cid})
	if w := postVote(t, "6501", cid, rid, ""); w.Code != http.StatusOK {
		t.Fatalf("vote got %d: %s", w.Code, w.Body.String())
	}
	share(t, "6501", cid, "")

	// A view answers with the video's real total, so the app can show it
	// instead of guessing "one more".
	got := act(t, HandleWatchEvent, "/api/v1/watch", "6501", map[string]any{
		"contentId": cid, "contentType": "challenge", "watchTime": 4000,
		"creatorMs": 4000,
	})
	if num(got["views"]) != startViews+1 {
		t.Errorf("the watch answered views = %v, want %d", got["views"], startViews+1)
	}
	// Watched again the same day: still one view, and it says so.
	got = act(t, HandleWatchEvent, "/api/v1/watch", "6501", map[string]any{
		"contentId": cid, "contentType": "challenge", "watchTime": 4000,
		"creatorMs": 4000,
	})
	if num(got["views"]) != startViews+1 {
		t.Errorf("a second watch the same day answered views = %v, want still %d",
			got["views"], startViews+1)
	}

	want := map[string]int{
		"likes":            1,
		"topResponseLikes": 1,
		"commentCount":     1,
		"saveCount":        1,
		"voteCount":        1,
		"shareCount":       1,
		"views":            startViews + 1,
	}
	check := func(name string, copies []map[string]any) {
		t.Helper()
		if len(copies) == 0 {
			t.Errorf("%s: the video is not there at all", name)
			return
		}
		for _, c := range copies {
			for k, n := range want {
				if got := num(c[k]); got != n {
					t.Errorf("%s: %s = %d, want %d", name, k, got, n)
				}
			}
		}
	}

	// ── every screen that shows those numbers ────────────────────────────
	check("battle page", []map[string]any{detailJSON(t, cid, "6502")})
	check("the creator's profile videos", screen(t, GetUserChallengesHandler,
		"/api/v1/users/1/challenges", map[string]string{"id": "1"}, "6502", cid))
	check("the answerer's battles tab", screen(t, UserBattlesHandler,
		"/api/v1/users/2/battles?tab=live", map[string]string{"id": "2"}, "6502", cid))
	check("saved videos", screen(t, GetSavedChallengesHandler,
		"/api/v1/saved/6501", map[string]string{"userId": "6501"}, "6501", cid))
	check("liked videos", screen(t, GetLikedChallengesHandler,
		"/api/v1/users/6501/likes", map[string]string{"id": "6501"}, "6501", cid))
	check("following feed", screen(t, FollowingFeedV2Handler,
		"/api/v1/feed/following/v2?userId=6502&page=1&limit=50", nil, "6502", cid))
	check("explore (the search grid)", screen(t, ExploreFeedHandler,
		"/api/v1/feed/explore?userId=6502&page=1&limit=50", nil, "6502", cid))
	check("the main feed", screen(t, SmartFeedHandler,
		"/api/v1/feed/smart?userId=6502&page=1&limit=50", nil, "6502", cid))
	var subject string
	if err := db.QueryRow(`SELECT subject FROM challenges WHERE id = $1`, cid).Scan(&subject); err != nil {
		t.Fatal(err)
	}
	check("search", screen(t, SearchHandler,
		"/search?type=battles&userId=6502&q="+url.QueryEscape(subject), nil, "6502", cid))

	// The live score's totals are what the battle page and the reel's live
	// score show.
	st := standingsJSON(t, cid)
	cr, an := side(t, st, 0), side(t, st, 1)
	if v := num(cr["votes"]) + num(an["votes"]); v != 1 {
		t.Errorf("live score votes = %d, want 1", v)
	}
	if num(cr["likes"]) != 1 || num(an["likes"]) != 1 {
		t.Errorf("live score likes: creator %d, answer %d — want 1 and 1",
			num(cr["likes"]), num(an["likes"]))
	}
	if s := num(cr["shares"]) + num(an["shares"]); s != 1 {
		t.Errorf("live score shares = %d, want 1", s)
	}
	if v := num(cr["views"]) + num(an["views"]); v != startViews+1 {
		t.Errorf("live score views = %d, want %d", v, startViews+1)
	}

	// The two lists that used to go out with every count at 0: the arena
	// (Search falls back to it) and friends-only videos.
	check("arena", screen(t, GetArenaChallengesHandler,
		"/api/v1/challenges/arena", nil, "6502", cid))
	if _, err := db.Exec(`UPDATE challenges SET visibility = 'friends' WHERE id = $1`, cid); err != nil {
		t.Fatal(err)
	}
	check("friends-only", screen(t, GetFriendsChallengesHandler,
		"/api/v1/challenges/friends?userId=6502", nil, "6502", cid))
}

// Unliking, unsaving and a comment from someone else move the numbers the
// other way and on, and the battle page agrees.
func TestCounts_UndoingMovesThemBack(t *testing.T) {
	defer withDB(t)()
	cid, _ := liveBattle(t)
	voter(t, 6601, 3)
	voter(t, 6602, 3)
	like := func(uid string) int {
		out := act(t, LikeChallengeHandler, "/api/v1/challenges/like", uid,
			map[string]string{"challengeId": cid})
		return num(out["likes"])
	}
	if n := like("6601"); n != 1 {
		t.Fatalf("after one like the answer says %d", n)
	}
	if n := like("6602"); n != 2 {
		t.Fatalf("after two likes the answer says %d", n)
	}
	if n := like("6601"); n != 1 {
		t.Fatalf("after one unlike the answer says %d", n)
	}
	act(t, SaveChallengeHandler, "/api/v1/save", "6601", map[string]string{"challengeId": cid})
	act(t, SaveChallengeHandler, "/api/v1/save", "6601", map[string]string{"challengeId": cid})
	for i, uid := range []string{"6601", "6602", "6601"} {
		act(t, AddChallengeCommentHandler, "/api/v1/challenges/comments", uid,
			map[string]string{"challengeId": cid, "text": "comment " + strconv.Itoa(i)})
	}
	c := detailJSON(t, cid, "")
	if num(c["likes"]) != 1 || num(c["saveCount"]) != 0 || num(c["commentCount"]) != 3 {
		t.Errorf("battle page: likes %d, saves %d, comments %d — want 1, 0, 3",
			num(c["likes"]), num(c["saveCount"]), num(c["commentCount"]))
	}
}
