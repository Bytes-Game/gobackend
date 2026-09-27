package main

// A video you liked comes back liked — for you, and only for you.
//
// These go through the real handlers that send videos to the app (a feed,
// a profile, search, the profile's battle tabs) against real Postgres, so a
// handler that forgets to mark them, or marks them for the wrong person,
// turns something here red.

import (
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"testing"

	"github.com/gorilla/mux"
)

const (
	vsViewer   = 9301
	vsStranger = 9302
	vsMaker    = 9303
	vsAnswerer = 9304
)

// seedViewerState makes three videos by vsMaker — A (liked by the viewer),
// B (saved by the viewer), C (a live battle answered by vsAnswerer, the
// viewer voted for the maker and liked the answer) — and returns their ids.
func seedViewerState(t *testing.T) (a, b, c, answer int) {
	t.Helper()
	db := openDB(t)
	exec := func(q string, args ...any) {
		t.Helper()
		if _, err := db.Exec(q, args...); err != nil {
			t.Fatalf("seed: %v\n%s", err, q)
		}
	}
	for _, id := range []int{vsViewer, vsStranger, vsMaker, vsAnswerer} {
		exec(`INSERT INTO users (id, username, password) VALUES ($1, $2, 'x')
		      ON CONFLICT (id) DO NOTHING`, id, fmt.Sprintf("vs_user_%d", id))
	}
	newVideo := func(subject string) int {
		var id int
		if err := db.QueryRow(`
			INSERT INTO challenges (creator_id, video_url, prefix, subject, visibility)
			VALUES ($1, 'https://v/x.mp4', 'who can', $2, 'arena') RETURNING id`,
			vsMaker, subject).Scan(&id); err != nil {
			t.Fatalf("seed video: %v", err)
		}
		return id
	}
	a, b, c = newVideo("liked one"), newVideo("saved one"), newVideo("battle one")
	if err := db.QueryRow(`
		INSERT INTO challenge_responses (challenge_id, responder_id, video_url)
		VALUES ($1, $2, 'https://v/r.mp4') RETURNING id`, c, vsAnswerer).Scan(&answer); err != nil {
		t.Fatalf("seed answer: %v", err)
	}
	exec(`UPDATE challenges SET status = 'active', accepted_at = NOW(),
	        voting_ends_at = NOW() + INTERVAL '3 days' WHERE id = $1`, c)
	exec(`INSERT INTO challenge_likes (challenge_id, user_id) VALUES ($1, $2)`, a, vsViewer)
	exec(`INSERT INTO saved_challenges (user_id, challenge_id) VALUES ($1, $2)`, vsViewer, b)
	exec(`INSERT INTO challenge_votes (challenge_id, response_id, voter_id) VALUES ($1, NULL, $2)`, c, vsViewer)
	exec(`INSERT INTO challenge_response_likes (response_id, user_id) VALUES ($1, $2)`, answer, vsViewer)
	return a, b, c, answer
}

type wireVideo struct {
	ID               string `json:"id"`
	IsLiked          bool   `json:"isLiked"`
	IsSaved          bool   `json:"isSaved"`
	HasVoted         bool   `json:"hasVoted"`
	VotedFor         string `json:"votedFor"`
	TopResponseLiked bool   `json:"topResponseLiked"`
	TopResponseID    string `json:"topResponseId"`
	VideoURL         string `json:"videoUrl"`
}

func profileVideosAs(t *testing.T, viewer int) map[string]wireVideo {
	t.Helper()
	req := httptest.NewRequest("GET",
		fmt.Sprintf("/api/v1/users/%d/challenges", vsMaker), nil)
	req = mux.SetURLVars(req, map[string]string{"id": fmt.Sprint(vsMaker)})
	req = withAuth(req, fmt.Sprint(viewer), "viewer")
	rec := httptest.NewRecorder()
	GetUserChallengesHandler(rec, req)
	if rec.Code != 200 {
		t.Fatalf("profile answered %d: %s", rec.Code, rec.Body.String())
	}
	var list []wireVideo
	if err := json.Unmarshal(rec.Body.Bytes(), &list); err != nil {
		t.Fatal(err)
	}
	out := map[string]wireVideo{}
	for _, v := range list {
		out[v.ID] = v
	}
	return out
}

func TestViewerState_YourLikesSavesAndVotesComeBackMarked(t *testing.T) {
	defer withDB(t)()
	a, b, c, answer := seedViewerState(t)
	got := profileVideosAs(t, vsViewer)

	if !got[fmt.Sprint(a)].IsLiked {
		t.Error("a video you liked came back with an empty heart")
	}
	if !got[fmt.Sprint(b)].IsSaved {
		t.Error("a video you saved came back unsaved")
	}
	battle := got[fmt.Sprint(c)]
	if !battle.HasVoted || battle.VotedFor != fmt.Sprintf("vs_user_%d", vsMaker) {
		t.Errorf("your vote for the maker was lost: %+v", battle)
	}
	if battle.TopResponseID != fmt.Sprint(answer) || !battle.TopResponseLiked {
		t.Errorf("the answer you liked is not marked: %+v", battle)
	}
	// Presence of the thing itself, not only its marks.
	if battle.VideoURL == "" {
		t.Error("the battle came back with no video")
	}
	if got[fmt.Sprint(b)].IsLiked {
		t.Error("a video you did not like came back liked")
	}
}

func TestViewerState_SomeoneElseSeesNoneOfIt(t *testing.T) {
	defer withDB(t)()
	a, b, c, _ := seedViewerState(t)
	got := profileVideosAs(t, vsStranger)
	for _, id := range []int{a, b, c} {
		v := got[fmt.Sprint(id)]
		if v.ID == "" {
			t.Fatalf("video %d missing for the stranger", id)
		}
		if v.IsLiked || v.IsSaved || v.HasVoted || v.TopResponseLiked {
			t.Errorf("the stranger sees the viewer's marks on %d: %+v", id, v)
		}
	}
}

func TestViewerState_SearchNeverTrustsAUserIdInTheAddress(t *testing.T) {
	defer withDB(t)()
	seedViewerState(t)
	// No token, but the viewer's id in the address: nothing is marked.
	req := httptest.NewRequest("GET",
		fmt.Sprintf("/api/v1/search?q=liked&userId=%d", vsViewer), nil)
	rec := httptest.NewRecorder()
	SearchHandler(rec, req)
	if rec.Code != 200 {
		t.Fatalf("search answered %d", rec.Code)
	}
	var body struct {
		Shorts  []wireVideo `json:"shorts"`
		Battles []wireVideo `json:"battles"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	for _, v := range append(body.Shorts, body.Battles...) {
		if v.IsLiked || v.IsSaved || v.HasVoted {
			t.Fatalf("search told a stranger what user %d liked: %+v",
				vsViewer, v)
		}
	}
}

func TestProfileBattleTabs_CarryTheWholeVideo(t *testing.T) {
	defer withDB(t)()
	_, _, c, answer := seedViewerState(t)
	req := httptest.NewRequest("GET",
		fmt.Sprintf("/api/v1/users/%d/battles?tab=live", vsMaker), nil)
	req = mux.SetURLVars(req, map[string]string{"id": fmt.Sprint(vsMaker)})
	req = withAuth(req, fmt.Sprint(vsViewer), "viewer")
	rec := httptest.NewRecorder()
	UserBattlesHandler(rec, req)
	if rec.Code != 200 {
		t.Fatalf("battles answered %d: %s", rec.Code, rec.Body.String())
	}
	var body struct {
		Battles []struct {
			ChallengeID string     `json:"challengeId"`
			Video       *wireVideo `json:"video"`
		} `json:"battles"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	for _, card := range body.Battles {
		if card.ChallengeID != fmt.Sprint(c) {
			continue
		}
		if card.Video == nil || card.Video.VideoURL == "" {
			t.Fatal("the live tab's battle has no video to play")
		}
		if card.Video.TopResponseID != fmt.Sprint(answer) {
			t.Fatalf("the battle has no answer video: %+v", card.Video)
		}
		if !card.Video.HasVoted {
			t.Fatalf("the viewer's vote is not marked on the card: %+v",
				card.Video)
		}
		return
	}
	t.Fatalf("live battle %d not in the live tab: %s", c, rec.Body.String())
}

func TestViewerState_AFeedMarksThemToo(t *testing.T) {
	defer withDB(t)()
	resetRedis(t)
	a, _, _, _ := seedViewerState(t)
	req := withAuth(httptest.NewRequest("GET",
		"/api/v1/feed/explore?page=1&limit=20", nil), fmt.Sprint(vsViewer), "v")
	rec := httptest.NewRecorder()
	ExploreFeedHandler(rec, req)
	if rec.Code != 200 {
		t.Fatalf("explore answered %d: %s", rec.Code, rec.Body.String())
	}
	var body struct {
		Items []struct {
			Challenge *wireVideo `json:"challenge"`
		} `json:"items"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	for _, it := range body.Items {
		if it.Challenge != nil && it.Challenge.ID == fmt.Sprint(a) {
			if !it.Challenge.IsLiked {
				t.Fatal("the feed sent back a video you liked with an empty heart")
			}
			return
		}
	}
	t.Fatalf("liked video %d not in the explore page at all", a)
}

func TestViewerState_TheBattlePageToo(t *testing.T) {
	defer withDB(t)()
	a, _, _, _ := seedViewerState(t)
	get := func(signedIn bool) wireVideo {
		t.Helper()
		req := httptest.NewRequest("GET",
			fmt.Sprintf("/api/v1/challenges/%d?userId=%d", a, vsViewer), nil)
		req = mux.SetURLVars(req, map[string]string{"id": fmt.Sprint(a)})
		if signedIn {
			req.Header.Set("Authorization",
				"Bearer "+testToken(t, fmt.Sprint(vsViewer), "viewer"))
		}
		rec := httptest.NewRecorder()
		GetChallengeDetailHandler(rec, req)
		if rec.Code != 200 {
			t.Fatalf("detail answered %d", rec.Code)
		}
		var body struct {
			Challenge wireVideo `json:"challenge"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		return body.Challenge
	}
	if !get(true).IsLiked {
		t.Error("the battle page shows your own like as not liked")
	}
	if get(false).IsLiked {
		t.Error("without signing in, a userId in the address revealed a like")
	}
}

func TestLikedList_WholeVideosInOrderAndNoneSkippedBetweenPages(t *testing.T) {
	defer withDB(t)()
	a, _, c, answer := seedViewerState(t)
	db := openDB(t)
	// Both likes inside the same second, the battle a tenth later, so a
	// page marker in whole seconds would lose the first.
	if _, err := db.Exec(`UPDATE challenge_likes SET created_at = '2026-01-01 12:00:00.100+00'
	                       WHERE user_id = $1`, vsViewer); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO challenge_likes (challenge_id, user_id, created_at)
	                      VALUES ($1, $2, '2026-01-01 12:00:00.200+00')`,
		c, vsViewer); err != nil {
		t.Fatal(err)
	}
	page := func(before string) (items []wireVideo, next string) {
		t.Helper()
		url := fmt.Sprintf("/api/v1/users/%d/likes?limit=1", vsViewer)
		if before != "" {
			url += "&before=" + before
		}
		req := httptest.NewRequest("GET", url, nil)
		req = mux.SetURLVars(req, map[string]string{"id": fmt.Sprint(vsViewer)})
		req = withAuth(req, fmt.Sprint(vsViewer), "viewer")
		rec := httptest.NewRecorder()
		GetLikedChallengesHandler(rec, req)
		if rec.Code != 200 {
			t.Fatalf("likes answered %d: %s", rec.Code, rec.Body.String())
		}
		var body struct {
			Items      []wireVideo `json:"items"`
			NextCursor string      `json:"nextCursor"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		return body.Items, body.NextCursor
	}

	first, next := page("")
	if len(first) != 1 || first[0].ID != fmt.Sprint(c) {
		t.Fatalf("first page should be the battle liked last: %+v", first)
	}
	battle := first[0]
	if !battle.IsLiked || battle.VideoURL == "" {
		t.Errorf("a liked video came back unliked or with nothing to play: %+v", battle)
	}
	if battle.TopResponseID != fmt.Sprint(answer) || !battle.HasVoted {
		t.Errorf("the battle came back without its answer or your vote: %+v", battle)
	}
	if next == "" {
		t.Fatal("no marker for the next page")
	}
	second, _ := page(next)
	if len(second) != 1 || second[0].ID != fmt.Sprint(a) {
		t.Fatalf("the next page skipped a like: %+v", second)
	}
}
