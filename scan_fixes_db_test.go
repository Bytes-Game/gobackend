package main

// What the full scan before release found, and what now stops each one:
// through the real handlers, against real Postgres.

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/mux"
)

// asUser sends [body] to an authed handler as [id]/[name], through the
// real sign-in check.
func asUser(t *testing.T, h http.HandlerFunc, method, path string, id int, name string, body any) (int, map[string]any) {
	t.Helper()
	var raw []byte
	if body != nil {
		raw, _ = json.Marshal(body)
	}
	r := httptest.NewRequest(method, path, bytes.NewReader(raw))
	r.Header.Set("Authorization", "Bearer "+testToken(t, strconv.Itoa(id), name))
	w := httptest.NewRecorder()
	authed(h)(w, r)
	var got map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &got)
	return w.Code, got
}

type inboxNoteRow struct {
	kind  string
	actor int
}

// inboxNotesFor is the notifications kept for [userID], oldest first. Waits a
// moment first: they are written off the response path.
func inboxNotesFor(t *testing.T, userID int) []inboxNoteRow {
	t.Helper()
	time.Sleep(150 * time.Millisecond)
	rows, err := db.Query(`SELECT kind, COALESCE(actor_id, 0) FROM user_notifications
		WHERE user_id = $1 ORDER BY id`, userID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []inboxNoteRow
	for rows.Next() {
		var n inboxNoteRow
		if err := rows.Scan(&n.kind, &n.actor); err != nil {
			t.Fatal(err)
		}
		out = append(out, n)
	}
	return out
}

func people(t *testing.T, visibility map[int]string) {
	t.Helper()
	for id, v := range visibility {
		mustExec(t, `INSERT INTO users (id, username, password, visibility) VALUES ($1, $2, 'x', $3)`,
			id, "person"+strconv.Itoa(id), v)
	}
}

// A follow notice is from whoever is signed in, to whoever was followed.
// The names used to come from the request: anybody could make "X started
// following you" appear to somebody, from somebody, with nothing behind it.
func TestScan_AFollowNoticeCannotBeFaked(t *testing.T) {
	defer withDB(t)()
	resetRedis(t)
	resetActionLimiters(t)
	const me, followed, famous, bystander = 9701, 9702, 9703, 9704
	people(t, map[int]string{me: "public", followed: "public", famous: "public", bystander: "public"})

	// Follows 9702, and types somebody else's name as the follower and a
	// bystander's as the person followed.
	code, _ := asUser(t, HandleFollowEvent, "POST", "/api/v1/follow", me, "person9701", map[string]any{
		"followingId": strconv.Itoa(followed), "followingUsername": "person9704",
		"followerId": strconv.Itoa(famous), "followerUsername": "person9703",
	})
	if code != http.StatusOK {
		t.Fatalf("following answered %d", code)
	}
	if n := countRows(t, `SELECT COUNT(*) FROM follows WHERE follower_id = $1 AND following_id = $2`, me, followed); n != 1 {
		t.Fatalf("%d follows from the signed-in person to the account followed, want 1", n)
	}
	if n := countRows(t, `SELECT COUNT(*) FROM follows WHERE follower_id = $1 OR following_id = $2`, famous, bystander); n != 0 {
		t.Fatal("the names typed in the request made a follow")
	}
	if got := inboxNotesFor(t, bystander); len(got) != 0 {
		t.Fatalf("a bystander named in the request was sent %v", got)
	}
	got := inboxNotesFor(t, followed)
	if len(got) != 1 || got[0].kind != noteFollow || got[0].actor != me {
		t.Fatalf("the person followed was told %v, want one follow from %d", got, me)
	}

	// Following again is not news.
	resetActionLimiters(t)
	if code, _ := asUser(t, HandleFollowEvent, "POST", "/api/v1/follow", me, "person9701",
		map[string]any{"followingId": strconv.Itoa(followed)}); code != http.StatusOK {
		t.Fatalf("following again answered %d", code)
	}
	if got := inboxNotesFor(t, followed); len(got) != 1 {
		t.Fatalf("following twice sent %d notices, want 1", len(got))
	}
	// Nobody follows themselves, and nobody who is not there is followed.
	if code, _ := asUser(t, HandleFollowEvent, "POST", "/api/v1/follow", me, "person9701",
		map[string]any{"followingId": strconv.Itoa(me)}); code != http.StatusBadRequest {
		t.Errorf("following yourself answered %d", code)
	}
	if code, _ := asUser(t, HandleFollowEvent, "POST", "/api/v1/follow", me, "person9701",
		map[string]any{"followingId": "99999999"}); code != http.StatusNotFound {
		t.Errorf("following nobody answered %d", code)
	}
}

// A private account says who follows it. Following one is a request; only
// its owner's yes makes a follower, and until then its videos stay closed.
func TestScan_APrivateAccountChoosesItsFollowers(t *testing.T) {
	defer withDB(t)()
	resetRedis(t)
	resetActionLimiters(t)
	const owner, asker, other = 9711, 9712, 9713
	people(t, map[int]string{owner: "friends", asker: "public", other: "public"})
	video := strconv.Itoa(mustID(t, `INSERT INTO challenges (creator_id, video_url, prefix, subject)
		VALUES ($1, 'https://v/private.mp4', 'who can', 'keep a secret') RETURNING id`, owner))
	follow := func(who int) (int, map[string]any) {
		resetActionLimiters(t)
		return asUser(t, HandleFollowEvent, "POST", "/api/v1/follow", who, "person"+strconv.Itoa(who),
			map[string]any{"followingId": strconv.Itoa(owner)})
	}
	follows := func(who int) bool {
		return countRows(t, `SELECT COUNT(*) FROM follows WHERE follower_id = $1 AND following_id = $2`, who, owner) == 1
	}
	profile := func(viewer int) map[string]any {
		r := mux.SetURLVars(httptest.NewRequest("GET", "/api/v1/users/person9711", nil), map[string]string{"username": "person9711"})
		r.Header.Set("Authorization", "Bearer "+testToken(t, strconv.Itoa(viewer), "person"+strconv.Itoa(viewer)))
		w := httptest.NewRecorder()
		GetUserHandler(w, r)
		var got map[string]any
		_ = json.Unmarshal(w.Body.Bytes(), &got)
		return got
	}

	// Asking: no follower yet, and the video stays closed.
	code, got := follow(asker)
	if code != http.StatusAccepted || got["requested"] != true {
		t.Fatalf("following a private account answered %d %v", code, got)
	}
	if follows(asker) {
		t.Fatal("following a private account made a follower with nobody's yes")
	}
	if viewerMayWatch(strconv.Itoa(asker), video) {
		t.Fatal("somebody who only asked can watch a private account's video")
	}
	if profile(asker)["followRequested"] != true {
		t.Error("the profile does not tell the person who asked that they are waiting")
	}
	if profile(other)["followRequested"] != nil {
		t.Error("the profile tells somebody else that a request is waiting")
	}
	// The owner is told once, however many times they ask.
	follow(asker)
	notes := inboxNotesFor(t, owner)
	if len(notes) != 1 || notes[0].kind != noteFollowRequest || notes[0].actor != asker {
		t.Fatalf("the owner was told %v, want one follow request from %d", notes, asker)
	}

	// The owner's list, and nobody else's.
	code, got = asUser(t, FollowRequestsHandler, "GET", "/api/v1/follow-requests", owner, "person9711", nil)
	list, _ := got["requests"].([]any)
	if code != http.StatusOK || len(list) != 1 || list[0].(map[string]any)["username"] != "person9712" {
		t.Fatalf("the owner's follow requests: %d %v", code, got)
	}
	if _, got := asUser(t, FollowRequestsHandler, "GET", "/api/v1/follow-requests", other, "person9713", nil); got["count"] != float64(0) {
		t.Fatalf("somebody else's list shows the owner's requests: %v", got)
	}
	// Only the owner can answer it.
	if code, _ := asUser(t, AnswerFollowRequestHandler, "POST", "/api/v1/follow-requests/answer", other, "person9713",
		map[string]any{"requesterId": strconv.Itoa(asker), "accept": true}); code != http.StatusNotFound {
		t.Fatalf("somebody else answering the request got %d", code)
	}
	if follows(asker) {
		t.Fatal("somebody else accepted a request for the owner")
	}

	// Yes: a follower, told so, who can watch.
	if code, got := asUser(t, AnswerFollowRequestHandler, "POST", "/api/v1/follow-requests/answer", owner, "person9711",
		map[string]any{"requesterId": strconv.Itoa(asker), "accept": true}); code != http.StatusOK || got["accepted"] != true {
		t.Fatalf("accepting answered %d %v", code, got)
	}
	if !follows(asker) || !viewerMayWatch(strconv.Itoa(asker), video) {
		t.Fatal("after a yes they are not a follower, or still cannot watch")
	}
	if notes := inboxNotesFor(t, asker); len(notes) != 1 || notes[0].kind != noteFollowAccepted || notes[0].actor != owner {
		t.Fatalf("the person accepted was told %v", notes)
	}
	if n := countRows(t, `SELECT COUNT(*) FROM follow_requests`); n != 0 {
		t.Fatal("an answered request is still waiting")
	}
	// Answering twice: nothing left to answer.
	if code, _ := asUser(t, AnswerFollowRequestHandler, "POST", "/api/v1/follow-requests/answer", owner, "person9711",
		map[string]any{"requesterId": strconv.Itoa(asker), "accept": true}); code != http.StatusNotFound {
		t.Errorf("answering an answered request got %d", code)
	}
	// A follower who follows again is still a follower, not a new request.
	if code, got := follow(asker); code != http.StatusOK || got["requested"] != nil {
		t.Errorf("a follower following again answered %d %v", code, got)
	}

	// No: nothing, and nobody is told.
	follow(other)
	if code, _ := asUser(t, AnswerFollowRequestHandler, "POST", "/api/v1/follow-requests/answer", owner, "person9711",
		map[string]any{"requesterId": strconv.Itoa(other), "accept": false}); code != http.StatusOK {
		t.Fatalf("declining answered %d", code)
	}
	if follows(other) || viewerMayWatch(strconv.Itoa(other), video) {
		t.Fatal("a declined request made a follower")
	}
	if notes := inboxNotesFor(t, other); len(notes) != 0 {
		t.Errorf("the person declined was told %v", notes)
	}

	// Taking a request back, by unfollowing.
	follow(other)
	resetActionLimiters(t)
	if code, _ := asUser(t, HandleUnfollowEvent, "POST", "/api/v1/unfollow", other, "person9713",
		map[string]any{"unfollowedId": strconv.Itoa(owner)}); code != http.StatusOK {
		t.Fatalf("taking a request back answered %d", code)
	}
	if n := countRows(t, `SELECT COUNT(*) FROM follow_requests`); n != 0 {
		t.Fatal("a request taken back is still waiting")
	}

	// The account goes public: everybody waiting becomes a follower.
	follow(other)
	raw, _ := json.Marshal(map[string]any{"visibility": "public"})
	r := mux.SetURLVars(httptest.NewRequest("PATCH", "/api/v1/users/9711", bytes.NewReader(raw)), map[string]string{"id": "9711"})
	r.Header.Set("Authorization", "Bearer "+testToken(t, "9711", "person9711"))
	w := httptest.NewRecorder()
	authed(UpdateUserProfileHandler)(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("going public answered %d %s", w.Code, w.Body.String())
	}
	if !follows(other) || countRows(t, `SELECT COUNT(*) FROM follow_requests`) != 0 {
		t.Fatal("going public left somebody waiting")
	}
	// And a public account is followed at once, as before.
	mustExec(t, `INSERT INTO users (id, username, password) VALUES (9714, 'person9714', 'x')`)
	if code, got := follow(9714); code != http.StatusOK || got["requested"] != nil || !follows(9714) {
		t.Fatalf("following a public account answered %d %v", code, got)
	}
}

// A request to follow does not outlive a block, whichever of the two
// blocks the other. Left waiting, it stayed on the owner's list with an
// Accept button, and accepting made a follower of somebody blocked.
func TestScan_ABlockEndsAFollowRequest(t *testing.T) {
	defer withDB(t)()
	resetRedis(t)
	const owner, asker = 9716, 9717
	people(t, map[int]string{owner: "friends", asker: "public"})
	ask := func() {
		t.Helper()
		resetActionLimiters(t)
		if code, got := asUser(t, HandleFollowEvent, "POST", "/api/v1/follow", asker, "person9717",
			map[string]any{"followingId": strconv.Itoa(owner)}); code != http.StatusAccepted {
			t.Fatalf("asking to follow answered %d %v", code, got)
		}
	}
	waiting := func() int {
		return countRows(t, `SELECT COUNT(*) FROM follow_requests WHERE requester_id = $1 AND target_id = $2`, asker, owner)
	}
	block := func(blocker, blocked int) {
		t.Helper()
		resetActionLimiters(t)
		if code, got := asUser(t, BlockUserHandler, "POST", "/api/v1/block", blocker, "person"+strconv.Itoa(blocker),
			map[string]any{"blockedId": strconv.Itoa(blocked)}); code != http.StatusOK {
			t.Fatalf("blocking answered %d %v", code, got)
		}
	}

	// The owner blocks the person asking.
	ask()
	if waiting() != 1 {
		t.Fatal("there is no request to start from")
	}
	block(owner, asker)
	if waiting() != 0 {
		t.Fatal("the owner blocked the person asking, and the request is still waiting for an answer")
	}
	resetActionLimiters(t)
	if code, _ := asUser(t, AnswerFollowRequestHandler, "POST", "/api/v1/follow-requests/answer", owner, "person9716",
		map[string]any{"requesterId": strconv.Itoa(asker), "accept": true}); code != http.StatusNotFound {
		t.Errorf("accepting a request that a block ended answered %d", code)
	}
	if n := countRows(t, `SELECT COUNT(*) FROM follows WHERE follower_id = $1 AND following_id = $2`, asker, owner); n != 0 {
		t.Fatal("somebody blocked became a follower")
	}

	// The other way round: the person who asked blocks the owner.
	mustExec(t, `DELETE FROM user_blocks WHERE blocker_id = $1 AND blocked_id = $2`, owner, asker)
	ask()
	block(asker, owner)
	if waiting() != 0 {
		t.Fatal("somebody blocked an account they had asked to follow, and the request is still waiting")
	}
}

// The votes on a battle name its two players. For a private account's
// battle that told a stranger who was in it.
func TestScan_VotesAreOnlyForThoseWhoMayWatch(t *testing.T) {
	defer withDB(t)()
	const owner, follower, stranger = 9721, 9722, 9723
	people(t, map[int]string{owner: "friends", follower: "public", stranger: "public"})
	follow(t, follower, owner)
	video := strconv.Itoa(mustID(t, `INSERT INTO challenges (creator_id, video_url, prefix, subject)
		VALUES ($1, 'https://v/p2.mp4', 'who can', 'vote') RETURNING id`, owner))
	mustExec(t, `INSERT INTO challenge_votes (challenge_id, voter_id) VALUES ($1, $2)`, video, follower)
	votes := func(viewer int) (int, string) {
		r := mux.SetURLVars(httptest.NewRequest("GET", "/api/v1/challenges/"+video+"/votes", nil), map[string]string{"id": video})
		if viewer > 0 {
			r.Header.Set("Authorization", "Bearer "+testToken(t, strconv.Itoa(viewer), "person"+strconv.Itoa(viewer)))
		}
		w := httptest.NewRecorder()
		GetVoteResultsHandler(w, r)
		return w.Code, w.Body.String()
	}
	if code, body := votes(follower); code != http.StatusOK || !strings.Contains(body, "person9721") {
		t.Fatalf("a follower asking for the votes got %d %s", code, body)
	}
	for who, viewer := range map[string]int{"a stranger": stranger, "nobody signed in": 0} {
		if code, body := votes(viewer); code != http.StatusNotFound || strings.Contains(body, "person9721") {
			t.Errorf("%s asking for a private battle's votes got %d %s", who, code, body)
		}
	}
}

// Whether somebody is online is not told across a block.
func TestScan_OnlineIsNotToldAcrossABlock(t *testing.T) {
	defer withDB(t)()
	const watched, friend, blockedOne, blocker = 9731, 9732, 9733, 9734
	people(t, map[int]string{watched: "public", friend: "public", blockedOne: "public", blocker: "public"})
	mustExec(t, `UPDATE users SET last_seen = NOW() - INTERVAL '5 minutes' WHERE id = $1`, watched)
	mustExec(t, `INSERT INTO user_blocks (blocker_id, blocked_id) VALUES ($1, $2), ($3, $1)`, watched, blockedOne, blocker)
	seen := func(viewer int) string {
		r := mux.SetURLVars(httptest.NewRequest("GET", "/api/v1/chat/online/person9731", nil), map[string]string{"username": "person9731"})
		r.Header.Set("Authorization", "Bearer "+testToken(t, strconv.Itoa(viewer), "person"+strconv.Itoa(viewer)))
		w := httptest.NewRecorder()
		authed(OnlineStatusHandler)(w, r)
		var got map[string]any
		_ = json.Unmarshal(w.Body.Bytes(), &got)
		s, _ := got["lastSeen"].(string)
		return s
	}
	if seen(friend) == "" {
		t.Fatal("somebody with no block between them is not told when they were last seen: the check below would pass on nothing")
	}
	if got := seen(blockedOne); got != "" {
		t.Errorf("somebody they blocked is told they were last seen %q", got)
	}
	if got := seen(blocker); got != "" {
		t.Errorf("somebody who blocked them is told they were last seen %q", got)
	}
}

// One account counts once in a video's numbers, however many times its
// phone says it watched, liked and shared.
func TestScan_OneAccountCountsOnceInAVideosNumbers(t *testing.T) {
	defer withDB(t)()
	const maker = 9741
	people(t, map[int]string{maker: "public"})
	video := strconv.Itoa(mustID(t, `INSERT INTO challenges (creator_id, video_url, prefix, subject)
		VALUES ($1, 'https://v/c.mp4', 'who can', 'count') RETURNING id`, maker))
	report := func(user, kind string, completion float64) {
		mustExec(t, `INSERT INTO feed_events (user_id, content_id, content_type, event_type, completion_rate, watch_duration_ms)
			VALUES ($1, $2, 'challenge', $3, $4, 4000)`, user, video, kind, completion)
	}
	// One account with a script.
	for i := 0; i < 50; i++ {
		report("7001", "view", 1.0)
		report("7001", "like", 0)
		report("7001", "share", 0)
	}
	// Three real people, one of whom watched twice and left early.
	report("7002", "view", 0.5)
	report("7002", "view", 0.5)
	report("7003", "view", 0.5)
	report("7003", "like", 0)
	report("7004", "view", 0.5)

	a := &contentEventAggregates{}
	into := map[string]*contentEventAggregates{video: a}
	if err := loadEngagementAggregates("challenge", []string{video}, into); err != nil {
		t.Fatal(err)
	}
	if a.ViewCount != 4 || a.LikeCount != 2 || a.ShareCount != 1 {
		t.Errorf("counted %d views, %d likes, %d shares; want 4 people who watched, 2 who liked, 1 who shared",
			a.ViewCount, a.LikeCount, a.ShareCount)
	}
	// The average is of people too: (1.0 + 0.5 + 0.5 + 0.5) / 4.
	if a.AvgCompletion < 0.62 || a.AvgCompletion > 0.63 {
		t.Errorf("average watched is %.3f, want 0.625: one account's fifty reports must not outweigh three people", a.AvgCompletion)
	}
	if err := loadRecentEngagement("challenge", []string{video}, into); err != nil {
		t.Fatal(err)
	}
	if a.RecentViews != 4 || a.RecentEng != 3 {
		t.Errorf("the last two hours: %d views and %d other things; want 4 and 3", a.RecentViews, a.RecentEng)
	}
}

// How fast one account may start uploads and report activity, and how
// many pieces one upload may have.
// withStorageSettings gives a test pretend storage settings.
//
// The server reads its storage settings once and keeps them. So in a full
// run, whichever test asked first, with nothing set, decided the answer
// for every test after it: this file's upload test passed alone and failed
// with the rest ("storage is not set up"). What was kept is cleared before
// and after, so the test reads its own settings and leaves none behind.
func withStorageSettings(t *testing.T) {
	t.Helper()
	t.Setenv("R2_ACCOUNT_ID", "acct")
	t.Setenv("R2_BUCKET", "bucket")
	t.Setenv("R2_ACCESS_KEY_ID", "key")
	t.Setenv("R2_SECRET_ACCESS_KEY", "secret")
	t.Setenv("R2_PUBLIC_BASE_URL", "https://media.example")
	forget := func() { r2Once, r2Cached, r2Err = sync.Once{}, nil, nil }
	forget()
	t.Cleanup(forget)
}

func TestScan_UploadsAndReportsHaveABudget(t *testing.T) {
	defer withDB(t)()
	resetActionLimiters(t)
	const uploader = 9751
	people(t, map[int]string{uploader: "public"})
	// As in a full run, where an earlier test has already asked for the
	// storage settings and been told there are none.
	_, _ = loadR2Config()
	withStorageSettings(t)

	start := func() int {
		code, _ := asUser(t, PresignMediaUploadHandler, "POST", "/api/v1/media/presign", uploader, "person9751",
			map[string]any{"items": []map[string]string{{"kind": "video", "variant": "720p"}}})
		return code
	}
	allowed, refused := 0, 0
	for i := 0; i < 40; i++ {
		switch start() {
		case http.StatusOK:
			allowed++
		case http.StatusTooManyRequests:
			refused++
		default:
			t.Fatalf("starting an upload answered %d", start())
		}
	}
	if allowed != actionLimitTable["upload_start"].burst || refused == 0 {
		t.Fatalf("%d uploads started at once and %d refused; want %d and then no more",
			allowed, refused, actionLimitTable["upload_start"].burst)
	}

	// A piece numbered past the limit is a file too big to be one of ours.
	resetActionLimiters(t)
	part := func(n int) int {
		code, _ := asUser(t, MultipartPresignHandler, "POST", "/api/v1/media/multipart", uploader, "person9751",
			map[string]any{"action": "part", "key": "u/9751/abc/720p.mp4", "uploadId": "up1", "partNumber": n})
		return code
	}
	if code := part(multipartMaxParts); code != http.StatusOK {
		t.Fatalf("piece %d answered %d", multipartMaxParts, code)
	}
	for _, n := range []int{multipartMaxParts + 1, 10000, 0, -3} {
		if code := part(n); code != http.StatusRequestEntityTooLarge {
			t.Errorf("piece %d answered %d, want it refused as too big", n, code)
		}
	}
	// Somebody else's upload cannot be signed for at all.
	if code, _ := asUser(t, MultipartPresignHandler, "POST", "/api/v1/media/multipart", uploader, "person9751",
		map[string]any{"action": "part", "key": "u/1/abc/720p.mp4", "uploadId": "up1", "partNumber": 1}); code != http.StatusForbidden {
		t.Errorf("signing a piece of somebody else's upload answered %d", code)
	}

	// Activity reports: a big batch costs more than a small one, and the
	// budget runs out.
	resetActionLimiters(t)
	batch := func(n int) int {
		events := make([]map[string]any, n)
		for i := range events {
			events[i] = map[string]any{"eventType": "page_view"}
		}
		code, _ := asUser(t, TrackBatchEventsHandler, "POST", "/api/v1/events/batch", uploader, "person9751",
			map[string]any{"events": events})
		return code
	}
	sent := 0
	for i := 0; i < 20 && batch(200) == http.StatusOK; i++ {
		sent++
	}
	want := actionLimitTable["events"].burst / (1 + 200/eventsPerToken)
	if sent != want {
		t.Fatalf("%d batches of 200 reports went through at once, want %d and then no more", sent, want)
	}
	// The reports are written after the answer is sent: let them land
	// before the database is handed back.
	waitFor(t, "every accepted report is written", func() bool {
		return countRows(t, `SELECT COUNT(*) FROM feed_events WHERE user_id = '9751'`) == sent*200
	})
}

// Two-step codes in Settings, and forwarding a message, are counted like
// their siblings: five tries at a code, and forwarding is sending.
func TestScan_CodesAndForwardsAreCounted(t *testing.T) {
	defer withDB(t)()
	resetActionLimiters(t)
	const me, friend = 9761, 9762
	people(t, map[int]string{me: "public", friend: "public"})
	try := func(h http.HandlerFunc, path string) int {
		raw, _ := json.Marshal(map[string]any{"code": "000000"})
		r := mux.SetURLVars(httptest.NewRequest("POST", path, bytes.NewReader(raw)), map[string]string{"id": "9761"})
		r.Header.Set("Authorization", "Bearer "+testToken(t, "9761", "person9761"))
		w := httptest.NewRecorder()
		authed(h)(w, r)
		return w.Code
	}
	for name, c := range map[string]struct {
		h    http.HandlerFunc
		path string
	}{
		"turning two-step on":  {VerifyTOTPHandler, "/api/v1/users/9761/totp/verify"},
		"turning two-step off": {DisableTOTPHandler, "/api/v1/users/9761/totp/disable"},
	} {
		resetActionLimiters(t)
		tries := 0
		for i := 0; i < 12 && try(c.h, c.path) != http.StatusTooManyRequests; i++ {
			tries++
		}
		if tries != actionLimitTable["totp_code"].burst {
			t.Errorf("%s: %d tries at the code went through, want %d and then no more", name, tries, actionLimitTable["totp_code"].burst)
		}
	}

	resetActionLimiters(t)
	msg := mustID(t, `INSERT INTO chat_messages (sender_id, receiver_id, message) VALUES ($1, $2, 'hello') RETURNING id`, me, friend)
	forwards := 0
	for i := 0; i < 30; i++ {
		code, _ := asUser(t, ForwardMessageHandler, "POST", "/api/v1/chat/forward", me, "person9761",
			map[string]any{"messageId": strconv.Itoa(msg), "receiverId": strconv.Itoa(friend)})
		if code == http.StatusTooManyRequests {
			break
		}
		forwards++
	}
	if forwards != actionLimitTable["chat"].burst {
		t.Errorf("%d forwards went through at once, want %d and then no more", forwards, actionLimitTable["chat"].burst)
	}
}

// A tap on a notification counts only from the person it was sent to.
func TestScan_ATapCountsOnlyFromItsOwner(t *testing.T) {
	defer withDB(t)()
	const owner, stranger = 9771, 9772
	people(t, map[int]string{owner: "public", stranger: "public"})
	// A key of its own each run: the table keeps one row per key, and is
	// not emptied between tests.
	id := mustID(t, `INSERT INTO notification_outbox (user_id, trigger_kind, title, body, status, dedupe_key)
		VALUES ('9771', 'follow', 't', 'b', 'sent', $1) RETURNING id`,
		"scan-test-"+strconv.FormatInt(time.Now().UnixNano(), 10))
	defer db.Exec(`DELETE FROM notification_outbox WHERE id = $1`, id)
	tapped := func() bool {
		return countRows(t, `SELECT COUNT(*) FROM notification_outbox WHERE id = $1 AND status = 'clicked'`, id) == 1
	}
	asUser(t, HandleNotificationClicked, "POST", "/api/v1/notifications/clicked", stranger, "person9772",
		map[string]any{"id": strconv.Itoa(id)})
	if tapped() {
		t.Fatal("somebody else's tap was counted")
	}
	asUser(t, HandleNotificationClicked, "POST", "/api/v1/notifications/clicked", owner, "person9771",
		map[string]any{"id": strconv.Itoa(id)})
	if !tapped() {
		t.Fatal("the owner's own tap was not counted")
	}
}

// The caller's address for rate limits: the one only Cloudflare can write,
// where Cloudflare is known to be in front; otherwise as before.
func TestScan_TheCallersAddressIsTheOneTheyCannotChoose(t *testing.T) {
	req := func() *http.Request {
		r := httptest.NewRequest("GET", "/", nil)
		r.RemoteAddr = "10.0.0.9:4000"
		r.Header.Set("X-Forwarded-For", "6.6.6.6, 203.0.113.50")
		r.Header.Set("CF-Connecting-IP", "203.0.113.50")
		return r
	}
	t.Setenv("TRUST_CF_CONNECTING_IP", "")
	t.Setenv("RENDER", "true")
	if got := clientIP(req()); got != "203.0.113.50" {
		t.Errorf("on Render the caller is %q, want the address Cloudflare wrote", got)
	}
	// Something that is not an address there is not believed.
	r := req()
	r.Header.Set("CF-Connecting-IP", "not-an-address")
	if got := clientIP(r); got != "6.6.6.6" {
		t.Errorf("with a broken Cloudflare header the caller is %q, want the old rule", got)
	}
	// Off Render nothing is assumed: a caller could type that header.
	t.Setenv("RENDER", "")
	if got := clientIP(req()); got != "6.6.6.6" {
		t.Errorf("off Render the caller is %q, want the old rule", got)
	}
	t.Setenv("TRUST_CF_CONNECTING_IP", "true")
	if got := clientIP(req()); got != "203.0.113.50" {
		t.Errorf("with the owner's say-so the caller is %q", got)
	}
	t.Setenv("RENDER", "true")
	t.Setenv("TRUST_CF_CONNECTING_IP", "false")
	if got := clientIP(req()); got != "6.6.6.6" {
		t.Errorf("with the owner's no the caller is %q", got)
	}
}

// The live connection takes the sign-in from a header first; the address
// only for an older app.
func TestScan_TheLiveConnectionsSignInIsNotInTheAddress(t *testing.T) {
	r := httptest.NewRequest("GET", "/ws/maya?token=from-the-address", nil)
	if got := wsToken(r); got != "from-the-address" {
		t.Errorf("an older app's sign-in is read as %q", got)
	}
	r.Header.Set("Authorization", "Bearer from-authorization")
	if got := wsToken(r); got != "from-authorization" {
		t.Errorf("with an Authorization header the sign-in is %q", got)
	}
	r.Header.Set("Sec-WebSocket-Protocol", "bearer, from-the-protocol")
	if got := wsToken(r); got != "from-the-protocol" {
		t.Errorf("with the protocol header the sign-in is %q", got)
	}
	// "bearer" with nothing after it is not a sign-in.
	r = httptest.NewRequest("GET", "/ws/maya", nil)
	r.Header.Set("Sec-WebSocket-Protocol", "bearer")
	if got := wsToken(r); got != "" {
		t.Errorf("a protocol header with no sign-in gave %q", got)
	}
	if len(upgrader.Subprotocols) != 1 || upgrader.Subprotocols[0] != wsBearerProtocol {
		t.Errorf("the server does not answer with %q, so the app would think the connection was refused", wsBearerProtocol)
	}
}

// A fault that is the server's own is said plainly, never in the
// database's words.
func TestScan_AServerFaultDoesNotSayWhatBroke(t *testing.T) {
	w := httptest.NewRecorder()
	serverFault(w, "saving a profile", errors.New(`pq: duplicate key value violates unique constraint "users_username_key"`))
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("answered %d", w.Code)
	}
	if body := w.Body.String(); strings.Contains(body, "pq:") || strings.Contains(body, "users_username_key") || !strings.Contains(body, "went wrong") {
		t.Errorf("the caller is told %q", body)
	}

	// And no handler outside the owner's own pages hands back an error's
	// own words with a 500 or a 503. Read from the source, comments out.
	comment := regexp.MustCompile(`(?m)^\s*//.*$`)
	leak := regexp.MustCompile(`http\.Error\(w, [^\n]*err\.Error\(\), http\.Status(InternalServerError|ServiceUnavailable|BadGateway)\)`)
	files, _ := os.ReadDir(".")
	for _, f := range files {
		name := f.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") || strings.HasPrefix(name, "admin_") {
			continue
		}
		src, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		for _, hit := range leak.FindAllString(comment.ReplaceAllString(string(src), ""), -1) {
			t.Errorf("%s answers with an error's own words: %s", name, hit)
		}
	}
}

// When the database fails, the caller is told it was us, and nothing more.
// Three routes said it as the caller's own fault instead, with the
// database's error text: "400" for a phone's notification address, and
// "403, not allowed" for editing or deleting a chat message.
func TestScan_AFaultInsideIsNotSaidAsARefusal(t *testing.T) {
	defer withDB(t)()
	resetActionLimiters(t)
	const sender, other = 9761, 9762
	people(t, map[int]string{sender: "public", other: "public"})
	mine := mustID(t, `INSERT INTO chat_messages (sender_id, receiver_id, message) VALUES ($1, $2, 'hello') RETURNING id`, sender, other)
	theirs := mustID(t, `INSERT INTO chat_messages (sender_id, receiver_id, message) VALUES ($1, $2, 'hi') RETURNING id`, other, sender)

	said := func(h http.HandlerFunc, path string, body map[string]any) (int, string) {
		t.Helper()
		resetActionLimiters(t)
		raw, _ := json.Marshal(body)
		r := httptest.NewRequest("POST", path, bytes.NewReader(raw))
		r.Header.Set("Authorization", "Bearer "+testToken(t, strconv.Itoa(sender), "person9761"))
		w := httptest.NewRecorder()
		authed(h)(w, r)
		return w.Code, w.Body.String()
	}
	// Breaks a table for as long as the returned function has not run.
	without := func(table string) (back func()) {
		t.Helper()
		mustExec(t, `ALTER TABLE `+table+` RENAME TO `+table+`_away`)
		return func() { mustExec(t, `ALTER TABLE `+table+`_away RENAME TO `+table) }
	}
	ours := func(what string, code int, body string) {
		t.Helper()
		if code != http.StatusInternalServerError || !strings.Contains(body, "went wrong") {
			t.Errorf("%s with the database failing answered %d %q, want 500 and that it was us", what, code, body)
		}
		for _, leak := range []string{"pq:", "relation", "chat_messages", "device_tokens"} {
			if strings.Contains(body, leak) {
				t.Errorf("%s tells the caller %q", what, body)
			}
		}
	}

	phone := map[string]any{"token": "a-phone", "platform": "fcm"}
	edit := map[string]any{"messageId": strconv.Itoa(mine), "text": "hello again"}
	del := map[string]any{"messageId": strconv.Itoa(mine)}

	// Working: a real "no" is still said in its own words.
	if code, body := said(HandleRegisterPushToken, "/api/v1/notifications/register",
		map[string]any{"token": "a-phone", "platform": "pager"}); code != http.StatusBadRequest || !strings.Contains(body, "unsupported platform") {
		t.Errorf("a platform that does not exist answered %d %q", code, body)
	}
	if code, body := said(HandleRegisterPushToken, "/api/v1/notifications/register",
		map[string]any{"token": strings.Repeat("x", 5000), "platform": "fcm"}); code != http.StatusBadRequest {
		t.Errorf("a notification address 5,000 characters long answered %d %q", code, body)
	}
	if code, body := said(HandleRegisterPushToken, "/api/v1/notifications/register", phone); code != http.StatusOK {
		t.Fatalf("a phone's notification address answered %d %q", code, body)
	}
	if code, body := said(EditMessageHandler, "/api/v1/chat/edit",
		map[string]any{"messageId": strconv.Itoa(theirs), "text": "not mine"}); code != http.StatusForbidden || !strings.Contains(body, "not yours") {
		t.Errorf("editing somebody else's message answered %d %q", code, body)
	}
	if code, body := said(DeleteMessageHandler, "/api/v1/chat/delete",
		map[string]any{"messageId": strconv.Itoa(theirs)}); code != http.StatusForbidden || !strings.Contains(body, "not yours") {
		t.Errorf("deleting somebody else's message answered %d %q", code, body)
	}

	// Failing: it was us.
	back := without("device_tokens")
	code, body := said(HandleRegisterPushToken, "/api/v1/notifications/register", phone)
	back()
	ours("saving a phone's notification address", code, body)

	back = without("chat_messages")
	code, body = said(EditMessageHandler, "/api/v1/chat/edit", edit)
	code2, body2 := said(DeleteMessageHandler, "/api/v1/chat/delete", del)
	back()
	ours("editing a message", code, body)
	ours("deleting a message", code2, body2)

	// And with the table back, the same two requests go through.
	if code, body := said(EditMessageHandler, "/api/v1/chat/edit", edit); code != http.StatusOK {
		t.Errorf("editing my own message answered %d %q", code, body)
	}
	if code, body := said(DeleteMessageHandler, "/api/v1/chat/delete", del); code != http.StatusOK {
		t.Errorf("deleting my own message answered %d %q", code, body)
	}
}

// The wires, read from main.go with the comments taken out.
func TestScan_IsWiredIn(t *testing.T) {
	src, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	code := regexp.MustCompile(`(?m)^\s*//.*$`).ReplaceAllString(string(src), "")
	for _, want := range []string{
		`"/follow-requests", authed(FollowRequestsHandler)`,
		`"/follow-requests/answer", authed(AnswerFollowRequestHandler)`,
		`"/experiments", adminOnly(ExperimentsListHandler)`,
		`"/notifications/clicked", authed(HandleNotificationClicked)`,
		`"/chat/online/{username}", authed(OnlineStatusHandler)`,
	} {
		if !strings.Contains(code, want) {
			t.Errorf("main.go does not have %s", want)
		}
	}
	for file, wants := range map[string][]string{
		"search.go":           {"userID := signedInUserID(r)"},
		"suggest_handlers.go": {"userID := signedInUserID(r)"},
	} {
		raw, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		body := regexp.MustCompile(`(?m)^\s*//.*$`).ReplaceAllString(string(raw), "")
		for _, want := range wants {
			if !strings.Contains(body, want) {
				t.Errorf("%s no longer has: %s", file, want)
			}
		}
		if strings.Contains(body, `Query().Get("userId")`) && file != "search_history.go" {
			t.Errorf("%s takes a user id from the address again", file)
		}
	}
	_ = fmt.Sprint
}
