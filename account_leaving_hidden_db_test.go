package main

// An account waiting to be deleted is hidden everywhere, and everything
// comes back if it is kept (account_leaving.go).
//
// One person leaves with everything a real account has: a video, an answer
// in somebody else's battle, a comment, a like, follows both ways, a chat,
// a notification they caused, a place in a viewer's likes, history, saves
// and recent searches. Every screen that could show any of it is asked
// three times, through the real handlers against a real database:
//
//   1. while the account is active — it must be there, or the check below
//      it proves nothing;
//   2. while it waits to be deleted — it must be gone;
//   3. after it is kept — it must be back.

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/mux"
)

const (
	sweepViewer  = 9911
	sweepLeaver  = 9912
	sweepCreator = 9913
)

// sweepSeed builds the leaving person and their traces, and says the ids of
// their video, the other person's battle, and their answer in it. Defer the
// func it returns: it removes everything again, before withDB closes the
// database.
func sweepSeed(t *testing.T) (leaverVideo, battle, answer string, done func()) {
	t.Helper()
	clean := func() {
		db.Exec(`DELETE FROM leaving_set_aside WHERE user_id IN (9911, 9912, 9913)`)
		db.Exec(`DELETE FROM user_notifications WHERE user_id IN (9911, 9912, 9913)`)
		db.Exec(`DELETE FROM user_blocks WHERE blocker_id IN (9911, 9912, 9913) OR blocked_id IN (9911, 9912, 9913)`)
		db.Exec(`DELETE FROM chat_messages WHERE sender_id IN (9911, 9912, 9913) OR receiver_id IN (9911, 9912, 9913)`)
		db.Exec(`DELETE FROM watch_events WHERE user_id IN (9911, 9912, 9913)`)
		db.Exec(`DELETE FROM follows WHERE follower_id IN (9911, 9912, 9913) OR following_id IN (9911, 9912, 9913)`)
		db.Exec(`DELETE FROM challenges WHERE creator_id IN (9911, 9912, 9913)`)
		db.Exec(`DELETE FROM users WHERE id IN (9911, 9912, 9913)`)
	}
	clean()
	hash, err := hashPassword("sunset42go")
	if err != nil {
		t.Fatal(err)
	}
	for id, name := range map[int]string{
		sweepViewer: "sweep_viewer", sweepLeaver: "sweep_leaver", sweepCreator: "sweep_creator",
	} {
		mustExec(t, `INSERT INTO users (id, username, password, password_hash, visibility)
		             VALUES ($1, $2, '', $3, 'public')`, id, name, hash)
	}
	mustExec(t, `UPDATE users SET avatar_url = 'https://media.example/u/9912/a/photo.jpg',
	                              bio = 'the leaver bio' WHERE id = 9912`)

	lv := mustID(t, `INSERT INTO challenges (creator_id, video_url, thumbnail_url, prefix, subject, visibility, status)
	                 VALUES (9912, 'https://v/leaver.mp4', 'https://v/leaver.jpg', 'who is better at',
	                         'quokka kite racing', 'arena', 'open') RETURNING id`)
	ac := mustID(t, `INSERT INTO challenges (creator_id, video_url, thumbnail_url, prefix, subject, visibility,
	                                         status, accepted_at, voting_ends_at, resolved_at, battle_days)
	                 VALUES (9913, 'https://v/creator.mp4', 'https://v/creator.jpg', 'who can',
	                         'juggle wombat plushies', 'arena', 'completed',
	                         NOW() - INTERVAL '5 days', NOW() - INTERVAL '2 days', NOW() - INTERVAL '2 days', 3)
	                 RETURNING id`)
	ans := mustID(t, `INSERT INTO challenge_responses (challenge_id, responder_id, video_url)
	                  VALUES ($1, 9912, 'https://v/answer.mp4') RETURNING id`, ac)
	mustExec(t, `INSERT INTO battle_results (challenge_id, user_id, role, response_id, outcome,
	                                         raw_votes, rating_before, rating_after)
	             VALUES ($1, 9913, 'creator', NULL, 'won', 2, 1000, 1030),
	                    ($1, 9912, 'responder', $2, 'lost', 1, 1000, 980)`, ac, ans)

	// The leaver's marks on other people's things.
	mustExec(t, `INSERT INTO challenge_comments (challenge_id, author_id, text)
	             VALUES ($1, 9912, 'comment by the leaver')`, ac)
	mustExec(t, `INSERT INTO challenge_likes (challenge_id, user_id) VALUES ($1, 9912)`, ac)
	mustExec(t, `INSERT INTO follows (follower_id, following_id) VALUES (9911, 9912), (9912, 9911), (9911, 9913)`)
	mustExec(t, `INSERT INTO chat_messages (sender_id, receiver_id, message, kind)
	             VALUES (9912, 9911, 'hello from the leaver', 'text'), (9911, 9912, 'hi back', 'text')`)
	mustExec(t, `INSERT INTO user_notifications (user_id, kind, actor_id, body)
	             VALUES (9911, 'follow', 9912, 'followed you')`)
	mustExec(t, `INSERT INTO challenge_votes (challenge_id, response_id, voter_id) VALUES ($1, $2, 9912)`, ac, ans)
	mustExec(t, `INSERT INTO challenge_dislikes (challenge_id, user_id) VALUES ($1, 9912)`, ac)
	mustExec(t, `INSERT INTO saved_challenges (user_id, challenge_id) VALUES (9912, $1)`, ac)
	mustExec(t, `INSERT INTO video_shares (user_id, challenge_id, response_id) VALUES (9912, $1, 0)`, ac)
	mustExec(t, `INSERT INTO user_blocks (blocker_id, blocked_id) VALUES (9913, 9912)`)
	mustExec(t, `UPDATE users SET last_seen = NOW() - INTERVAL '1 hour' WHERE id = 9912`)

	// The leaver's video in the viewer's own lists.
	mustExec(t, `INSERT INTO challenge_likes (challenge_id, user_id) VALUES ($1, 9911)`, lv)
	mustExec(t, `INSERT INTO saved_challenges (user_id, challenge_id) VALUES (9911, $1)`, lv)
	mustExec(t, `INSERT INTO watch_events (user_id, content_id, content_type, watch_time, completed)
	             VALUES (9911, $1, 'challenge', 9000, true)`, lv)
	searchedForLeaver()

	forgetLeaving()
	return strconv.Itoa(lv), strconv.Itoa(ac), strconv.Itoa(ans), func() {
		clean()
		forgetLeaving()
	}
}

// searchedForLeaver records that the viewer opened the leaver's account from
// search. That history lives in Redis, which the test empties between steps
// (so no feed page is served from a cache), so it is recorded again after.
func searchedForLeaver() {
	r := as(httptest.NewRequest("POST", "/api/v1/search/recent/account",
		strings.NewReader(`{"userId":"9912"}`)), sweepViewer)
	RecordSearchedAccountHandler(httptest.NewRecorder(), r)
}

// answerCount finds the video [id] anywhere in a JSON answer and reads how
// many answers it says the video has; -1 when the video is not there.
func answerCount(body, id string) int {
	var doc any
	if json.Unmarshal([]byte(body), &doc) != nil {
		return -1
	}
	found := -1
	var walk func(any)
	walk = func(v any) {
		switch x := v.(type) {
		case map[string]any:
			if x["id"] == id {
				if n, ok := x["responseCount"].(float64); ok {
					found = int(n)
				}
			}
			for _, child := range x {
				walk(child)
			}
		case []any:
			for _, child := range x {
				walk(child)
			}
		}
	}
	walk(doc)
	return found
}

// sweepCall runs one handler as the viewer and answers the status and body.
func sweepCall(h http.HandlerFunc, method, target string, vars map[string]string, body string) (int, string) {
	var rd *bytes.Reader
	if body != "" {
		rd = bytes.NewReader([]byte(body))
	} else {
		rd = bytes.NewReader(nil)
	}
	r := as(httptest.NewRequest(method, target, rd), sweepViewer)
	if vars != nil {
		r = mux.SetURLVars(r, vars)
	}
	w := httptest.NewRecorder()
	h(w, r)
	return w.Code, w.Body.String()
}

// sweepSurfaces are every screen that could show the leaver. Each answers
// whether the leaver (or their video, answer, comment) is on it.
func sweepSurfaces(t *testing.T, leaverVideo, battle, answer string) map[string]func() bool {
	v := strconv.Itoa(sweepViewer)
	l := strconv.Itoa(sweepLeaver)
	has := func(body, what string) bool { return strings.Contains(body, what) }
	id := func(x string) string { return `"id":"` + x + `"` }
	return map[string]func() bool{
		"their profile": func() bool {
			code, _ := sweepCall(GetUserHandler, "GET", "/api/v1/users/sweep_leaver",
				map[string]string{"username": "sweep_leaver"}, "")
			return code == 200
		},
		"their open web page": func() bool {
			code, _ := sweepCall(PublicProfilePageHandler, "GET", "/u/sweep_leaver",
				map[string]string{"username": "sweep_leaver"}, "")
			return code == 200
		},
		"their photo by name": func() bool {
			_, b := sweepCall(UserAvatarsHandler, "GET", "/api/v1/users/avatars?names=sweep_leaver", nil, "")
			return has(b, "sweep_leaver")
		},
		"the viewer's followers": func() bool {
			_, b := sweepCall(GetFollowersHandler, "GET", "/api/v1/users/"+v+"/followers",
				map[string]string{"id": v}, "")
			return has(b, "sweep_leaver")
		},
		"who the viewer follows": func() bool {
			_, b := sweepCall(GetFollowingHandler, "GET", "/api/v1/users/"+v+"/following",
				map[string]string{"id": v}, "")
			return has(b, "sweep_leaver")
		},
		"their video list": func() bool {
			_, b := sweepCall(GetUserChallengesHandler, "GET", "/api/v1/users/"+l+"/challenges",
				map[string]string{"id": l}, "")
			return has(b, id(leaverVideo))
		},
		"their video's page": func() bool {
			code, _ := sweepCall(GetChallengeDetailHandler, "GET", "/api/v1/challenges/"+leaverVideo,
				map[string]string{"id": leaverVideo}, "")
			return code == 200
		},
		"their answer on the other person's battle page": func() bool {
			_, b := sweepCall(GetChallengeDetailHandler, "GET", "/api/v1/challenges/"+battle,
				map[string]string{"id": battle}, "")
			return has(b, id(answer))
		},
		"their answer in the battle's standings": func() bool {
			_, b := sweepCall(BattleStandingsHandler, "GET", "/api/v1/challenges/"+battle+"/standings",
				map[string]string{"id": battle}, "")
			return has(b, "sweep_leaver")
		},
		"their comment under the battle": func() bool {
			_, b := sweepCall(GetChallengeCommentsHandler, "GET", "/api/v1/challenges/"+battle+"/comments",
				map[string]string{"id": battle}, "")
			return has(b, "comment by the leaver")
		},
		"their like in the battle's likes list": func() bool {
			_, b := sweepCall(ChallengePeopleHandler, "GET", "/api/v1/challenges/"+battle+"/people?what=likes",
				map[string]string{"id": battle}, "")
			return has(b, "sweep_leaver")
		},
		"their like in the battle's like count": func() bool {
			var n int
			if err := db.QueryRow(`SELECT likes_count FROM challenges WHERE id = $1`, battle).Scan(&n); err != nil {
				t.Fatal(err)
			}
			return n > 0
		},
		"their name as the opponent on the other person's battles tab": func() bool {
			c := strconv.Itoa(sweepCreator)
			_, b := sweepCall(UserBattlesHandler, "GET", "/api/v1/users/"+c+"/battles?tab=won",
				map[string]string{"id": c}, "")
			return has(b, "sweep_leaver")
		},
		"the Search grid": func() bool {
			_, b := sweepCall(ExploreFeedHandler, "GET", "/api/v1/feed/explore?page=1&limit=30&markShown=false", nil, "")
			return has(b, id(leaverVideo))
		},
		"For You": func() bool {
			_, b := sweepCall(SmartFeedHandler, "GET", "/api/v1/feed/smart?page=1&limit=20", nil, "")
			return has(b, id(leaverVideo))
		},
		"a search for their video": func() bool {
			_, b := sweepCall(SearchHandler, "GET", "/search?type=all&q="+url.QueryEscape("quokka kite racing"), nil, "")
			return has(b, id(leaverVideo))
		},
		"a search for their name": func() bool {
			_, b := sweepCall(SearchHandler, "GET", "/search?type=all&q=sweep_leaver", nil, "")
			return has(b, `"username":"sweep_leaver"`)
		},
		"the viewer's recent searches": func() bool {
			_, b := sweepCall(RecentSearchesHandler, "GET", "/api/v1/search/recent", nil, "")
			return has(b, "sweep_leaver")
		},
		"the viewer's notifications": func() bool {
			_, b := sweepCall(ListNotificationsHandler, "GET", "/api/v1/notifications", nil, "")
			return has(b, "sweep_leaver")
		},
		"the viewer's chat list": func() bool {
			_, b := sweepCall(GetConversationsHandler, "GET", "/api/v1/chat/conversations/"+v,
				map[string]string{"userId": v}, "")
			return has(b, "sweep_leaver")
		},
		"the chat itself": func() bool {
			_, b := sweepCall(GetMessagesHandler, "GET", "/api/v1/chat/messages/"+v+"/"+l,
				map[string]string{"userId": v, "otherUserId": l}, "")
			return has(b, "hello from the leaver")
		},
		"the viewer's liked videos": func() bool {
			_, b := sweepCall(GetLikedChallengesHandler, "GET", "/api/v1/users/"+v+"/likes",
				map[string]string{"id": v}, "")
			return has(b, id(leaverVideo))
		},
		"the viewer's watch history": func() bool {
			_, b := sweepCall(GetWatchHistoryHandler, "GET", "/api/v1/users/"+v+"/history",
				map[string]string{"id": v}, "")
			return has(b, leaverVideo)
		},
		"their answer on the other person's battle card in the Search grid": func() bool {
			// Their own video is gone from the grid; their name on another
			// person's battle card would be the answer shown with it.
			_, b := sweepCall(ExploreFeedHandler, "GET", "/api/v1/feed/explore?page=1&limit=30&markShown=false", nil, "")
			return has(b, id(battle)) && has(b, "sweep_leaver")
		},
		"the viewer's unread count": func() bool {
			_, b := sweepCall(ListNotificationsHandler, "GET", "/api/v1/notifications", nil, "")
			return has(b, `"unread":1`)
		},
		"the other person's block list": func() bool {
			c := strconv.Itoa(sweepCreator)
			r := withAuth(httptest.NewRequest("GET", "/api/v1/users/"+c+"/blocks", nil), c, "sweep_creator")
			r = mux.SetURLVars(r, map[string]string{"id": c})
			w := httptest.NewRecorder()
			ListBlockedUsersHandler(w, r)
			return has(w.Body.String(), "sweep_leaver")
		},
		"when they were last on": func() bool {
			_, b := sweepCall(OnlineStatusHandler, "GET", "/api/v1/chat/online/sweep_leaver",
				map[string]string{"username": "sweep_leaver"}, "")
			return !has(b, `"lastSeen":""`)
		},
		"what they did on other people's things, in the tables": func() bool {
			var n int
			if err := db.QueryRow(`SELECT
			    (SELECT COUNT(*) FROM challenge_likes WHERE user_id = 9912)
			  + (SELECT COUNT(*) FROM challenge_votes WHERE voter_id = 9912)
			  + (SELECT COUNT(*) FROM challenge_dislikes WHERE user_id = 9912)
			  + (SELECT COUNT(*) FROM challenge_comments WHERE author_id = 9912)
			  + (SELECT COUNT(*) FROM saved_challenges WHERE user_id = 9912)
			  + (SELECT COUNT(*) FROM video_shares WHERE user_id = 9912)
			  + (SELECT COUNT(*) FROM follows WHERE follower_id = 9912 OR following_id = 9912)`).Scan(&n); err != nil {
				t.Fatal(err)
			}
			// All of it, or none of it: half put back is not "kept".
			switch n {
			case 8:
				return true
			case 0:
				return false
			}
			t.Errorf("%d of their 8 marks on other people's things are in the tables", n)
			return n > 0
		},
		"their answer in the battle page's answer count": func() bool {
			_, b := sweepCall(GetChallengeDetailHandler, "GET", "/api/v1/challenges/"+battle,
				map[string]string{"id": battle}, "")
			return answerCount(b, battle) > 0
		},
		"their answer making the other person's video a battle in the Search grid": func() bool {
			_, b := sweepCall(ExploreFeedHandler, "GET", "/api/v1/feed/explore?page=1&limit=30&markShown=false", nil, "")
			n := answerCount(b, battle)
			if n < 0 {
				t.Errorf("the other person's video is not in the Search grid at all: %.300s", b)
			}
			return n > 0
		},
		"the viewer's saved videos": func() bool {
			_, b := sweepCall(GetSavedChallengesHandler, "GET", "/api/v1/saved/"+v,
				map[string]string{"userId": v}, "")
			return has(b, id(leaverVideo))
		},
	}
}

func TestAccountLeaving_HiddenEverywhereAndBackWhenKept(t *testing.T) {
	defer withDB(t)()
	resetRedis(t)
	resetActionLimiters(t)
	video, battle, answer, done := sweepSeed(t)
	defer done()
	surfaces := sweepSurfaces(t, video, battle, answer)

	// 1. Active: on every screen, or the checks after this say nothing.
	for name, on := range surfaces {
		if !on() {
			t.Errorf("while the account is active, it is not on %s — the check "+
				"for it being hidden there would prove nothing", name)
		}
	}
	if t.Failed() {
		t.FailNow()
	}

	// 2. Waiting to be deleted: on none.
	if w := deleteAccount(t, sweepLeaver, "sweep_leaver"); w.Code != 200 {
		t.Fatalf("delete answered %d: %s", w.Code, w.Body.String())
	}
	resetRedis(t)
	searchedForLeaver()
	for name, on := range surfaces {
		if on() {
			t.Errorf("an account waiting to be deleted is still on %s", name)
		}
	}
	// Nobody can start anything new with it either.
	if code, b := sweepCall(HandleFollowEvent, "POST", "/api/v1/follow", nil,
		`{"followingId":"9912","followingUsername":"sweep_leaver"}`); code != 403 || !strings.Contains(b, "unavailable") {
		t.Errorf("following an account waiting to be deleted answered %d %s", code, b)
	}
	if code, _ := sweepCall(SendMessageHandler, "POST", "/api/v1/chat/send", nil,
		`{"receiverId":"9912","message":"are you there?"}`); code != 403 {
		t.Errorf("messaging an account waiting to be deleted answered %d", code)
	}

	// 3. Kept: on every screen again.
	if code, got := signInAs(t, "sweep_leaver", "sunset42go", true); code != 200 {
		t.Fatalf("keeping the account answered %d %v", code, got)
	}
	resetRedis(t)
	searchedForLeaver()
	for name, on := range surfaces {
		if !on() {
			t.Errorf("after keeping the account, it is still missing from %s", name)
		}
	}
}

// Not shown is not enough: a video the feed scores and then drops still took
// a place in the ranking, and was compared with the staying accounts' videos
// it pushed aside. The candidates — what scoring is handed — must not have
// it at all.
func TestAccountLeaving_TheirVideosAreNeverScored(t *testing.T) {
	defer withDB(t)()
	resetRedis(t)
	video, _, _, done := sweepSeed(t)
	defer done()
	viewer := strconv.Itoa(sweepViewer)
	inItems := func(items []HomeFeedItem) bool {
		for _, it := range items {
			if getItemID(it) == video {
				return true
			}
		}
		return false
	}
	pools := map[string]func() bool{
		"every lane of For You, merged (multiSourceFetchForCohort)": func() bool {
			items, _ := multiSourceFetchForCohort(viewer, 200, "")
			return inItems(items)
		},
		"the newest videos, For You's fallback and Explore's other half (fetchCandidates)": func() bool {
			return inItems(fetchCandidates(viewer, 200))
		},
		"the feed for somebody new (coldStartChallengesTiered)": func() bool {
			return inItems(coldStartChallengesTiered(viewer, "short", 100, 0, "720 hours", 0, 0))
		},
		"what search, the index and Explore may return (searchableWhere)": func() bool {
			for _, c := range GetSearchableChallenges() {
				if c.ID == video {
					return true
				}
			}
			return false
		},
	}
	for name, has := range pools {
		if !has() {
			t.Errorf("while the account is active, its video is not in %s — the "+
				"check below says nothing", name)
		}
	}
	if t.Failed() {
		t.FailNow()
	}
	if w := deleteAccount(t, sweepLeaver, "sweep_leaver"); w.Code != 200 {
		t.Fatalf("delete answered %d", w.Code)
	}
	resetRedis(t)
	for name, has := range pools {
		if has() {
			t.Errorf("an account waiting to be deleted still has its video in %s, "+
				"so it is scored against the staying accounts' videos", name)
		}
	}
}

// "Nothing of theirs is scored" covers their taste as well as their videos:
// the feed borrows what people similar to you liked, and suggests accounts.
// Neither may use an account waiting to be deleted. Nor may the list of
// everybody that search falls back on and the index is filled from.
func TestAccountLeaving_NoTasteIsBorrowedAndNobodyIsOfferedThem(t *testing.T) {
	defer withDB(t)()
	resetRedis(t)
	_, battle, answer, done := sweepSeed(t)
	defer done()
	viewer := strconv.Itoa(sweepViewer)
	mustExec(t, `UPDATE challenge_responses SET content_topics = '["quokkaology"]'::jsonb WHERE id = $1`, answer)
	const stranger = 9914
	db.Exec(`DELETE FROM users WHERE id = $1`, stranger)
	defer db.Exec(`DELETE FROM users WHERE id = $1`, stranger)
	mustExec(t, `INSERT INTO users (id, username, password) VALUES ($1, 'sweep_stranger', 'x')`, stranger)
	mustExec(t, `INSERT INTO user_similarities (user_id, similar_user_id, similarity_score)
	             VALUES ('9911', '9912', 0.9)`)
	defer db.Exec(`DELETE FROM user_similarities WHERE user_id = '9911'`)
	mustExec(t, `INSERT INTO feed_events (user_id, content_id, content_type, event_type, completion_rate)
	             VALUES ('9912', $1, 'challenge', 'like', 1.0)`, battle)
	defer db.Exec(`DELETE FROM feed_events WHERE user_id = '9912'`)

	uses := map[string]func() bool{
		"the people similar to the viewer": func() bool {
			for _, s := range getSimilarUsers(viewer) {
				if s.SimilarID == "9912" {
					return true
				}
			}
			return false
		},
		"the 'people like you liked this' score": func() bool {
			collabBonusCache = NewSignalCache[float64](2 * time.Minute)
			return getCollaborativeBonus(viewer, battle, "challenge") > 0
		},
		"the 'people like you liked this' feed lane": func() bool {
			for _, it := range sourceCollaborativeWindowed(viewer, 50, "720 hours") {
				if getItemID(it) == battle {
					return true
				}
			}
			return false
		},
		"the accounts a stranger may be offered": func() bool {
			return !buildSuggestedExclusions(strconv.Itoa(stranger))["9912"]
		},
		"the subjects 'people also searched for' learns from": func() bool {
			return buildTopicGraph().freq["quokkaology"] > 0
		},
		"everybody, as search and the index read it": func() bool {
			for _, u := range GetAllUsers() {
				if u.ID == "9912" {
					return true
				}
			}
			return false
		},
	}
	for name, uses := range uses {
		if !uses() {
			t.Errorf("while the account is active, it is not in %s — the check below says nothing", name)
		}
	}
	if t.Failed() {
		t.FailNow()
	}
	if w := deleteAccount(t, sweepLeaver, "sweep_leaver"); w.Code != 200 {
		t.Fatalf("delete answered %d", w.Code)
	}
	for name, uses := range uses {
		if uses() {
			t.Errorf("an account waiting to be deleted is still in %s", name)
		}
	}
}
