package main

// A post's files must be its poster's own uploads in our storage — through
// the real handlers against real Postgres. See post_media.go.

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
)

const (
	pmPoster = 8501
	pmOther  = 8502
)

func seedPosters(t *testing.T) {
	t.Helper()
	for _, id := range []int{pmPoster, pmOther} {
		if _, err := db.Exec(`INSERT INTO users (id, username, password)
			VALUES ($1, $2, 'x') ON CONFLICT (id) DO NOTHING`,
			id, "poster"+strconv.Itoa(id)); err != nil {
			t.Fatal(err)
		}
	}
}

// postWith posts a challenge as [who] with these files, through the handler.
func postWith(t *testing.T, who int, files map[string]any) (int, string) {
	t.Helper()
	body := map[string]any{"prefix": "Who can", "subject": "whistle louder",
		"visibility": "arena", "durationMs": 5000}
	for k, v := range files {
		body[k] = v
	}
	raw, _ := json.Marshal(body)
	r := withAuth(httptest.NewRequest("POST", "/api/v1/challenges", bytes.NewReader(raw)),
		strconv.Itoa(who), "u"+strconv.Itoa(who))
	w := httptest.NewRecorder()
	CreateChallengeHandler(w, r)
	return w.Code, w.Body.String()
}

func countPosts(t *testing.T, who int) int {
	t.Helper()
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM challenges WHERE creator_id = $1`, who).
		Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestPostMedia_OnlyYourOwnUploadsArePosted(t *testing.T) {
	defer withDB(t)()
	resetRedis(t)
	resetActionLimiters(t)
	seedPosters(t)

	// Your own upload goes up, so the refusals below are about the files.
	if code, body := postWith(t, pmPoster, map[string]any{
		"videoUrl":     testUpload(pmPoster, "720p.mp4"),
		"thumbnailUrl": testUpload(pmPoster, "default.jpg"),
	}); code != 201 {
		t.Fatalf("posting your own upload: %d %s", code, body)
	}

	for name, files := range map[string]map[string]any{
		"somebody else's upload": {"videoUrl": testUpload(pmOther, "720p.mp4")},
		"a file anywhere on the internet": {
			"videoUrl": "https://example.com/u/" + strconv.Itoa(pmPoster) + "/a/720p.mp4"},
		"an address inside the server's own network": {
			"videoUrl": "http://169.254.169.254/latest/meta-data/"},
		"your video with somebody else's picture": {
			"videoUrl":     testUpload(pmPoster, "720p.mp4"),
			"thumbnailUrl": testUpload(pmOther, "default.jpg")},
		"your video with another size from elsewhere": {
			"videoUrl":      testUpload(pmPoster, "720p.mp4"),
			"videoVariants": map[string]string{"480p": "https://example.com/480p.mp4"}},
		"a way out of your folder": {
			"videoUrl": postTestBase + "/u/" + strconv.Itoa(pmPoster) + "/a/../../" +
				strconv.Itoa(pmOther) + "/b/720p.mp4"},
		"an address with more on the end": {
			"videoUrl": testUpload(pmPoster, "720p.mp4") + "?x=1"},
	} {
		resetActionLimiters(t)
		code, body := postWith(t, pmPoster, files)
		if code != http.StatusBadRequest {
			t.Errorf("%s: %d %s", name, code, body)
		}
	}
	if n := countPosts(t, pmPoster); n != 1 {
		t.Errorf("%d posts stored, want only the one with your own upload", n)
	}
}

func TestPostMedia_TheServerNeverOpensAnAddressThatIsNotAnUpload(t *testing.T) {
	defer withDB(t)()
	resetRedis(t)
	resetActionLimiters(t)
	seedPosters(t)

	// Stands in for an address the server should never be made to open.
	var opened atomic.Int32
	inside := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		opened.Add(1)
		w.WriteHeader(http.StatusNotFound)
	}))
	defer inside.Close()

	if code, _ := postWith(t, pmPoster, map[string]any{"videoUrl": inside.URL + "/admin"}); code != 400 {
		t.Errorf("a post pointing at another address: %d, want 400", code)
	}
	cid := gridVideo(t, pmOther, 0, "arena", "open")
	body, _ := json.Marshal(map[string]any{
		"challengeId": cid, "videoUrl": inside.URL + "/admin", "durationMs": 5000,
	})
	r := withAuth(httptest.NewRequest("POST", "/api/v1/challenges/accept", bytes.NewReader(body)),
		strconv.Itoa(pmPoster), "poster")
	w := httptest.NewRecorder()
	AcceptChallengeHandler(w, r)
	if w.Code != 400 {
		t.Errorf("an answer pointing at another address: %d %s", w.Code, w.Body.String())
	}
	if n := opened.Load(); n != 0 {
		t.Errorf("the server opened the address %d times", n)
	}
}

func TestPostMedia_AnswersFollowTheSameRule(t *testing.T) {
	defer withDB(t)()
	resetRedis(t)
	resetActionLimiters(t)
	seedPosters(t)
	cid := gridVideo(t, pmOther, 0, "arena", "open")
	answer := func(files map[string]any) int {
		b := map[string]any{"challengeId": cid, "durationMs": 5000}
		for k, v := range files {
			b[k] = v
		}
		raw, _ := json.Marshal(b)
		r := withAuth(httptest.NewRequest("POST", "/api/v1/challenges/accept", bytes.NewReader(raw)),
			strconv.Itoa(pmPoster), "poster")
		w := httptest.NewRecorder()
		AcceptChallengeHandler(w, r)
		return w.Code
	}
	if got := answer(map[string]any{"videoUrl": testUpload(pmOther, "720p.mp4")}); got != 400 {
		t.Errorf("an answer made of somebody else's upload: %d", got)
	}
	if got := answer(map[string]any{"videoUrl": testUpload(pmPoster, "720p.mp4"),
		"thumbnailUrl": "https://example.com/pic.jpg"}); got != 400 {
		t.Errorf("an answer with a picture from elsewhere: %d", got)
	}
	if got := answer(map[string]any{"videoUrl": testUpload(pmPoster, "720p.mp4")}); got != 201 {
		t.Errorf("an answer made of your own upload: %d", got)
	}
}

func TestPostMedia_NoStorageMeansNoUploadIsYours(t *testing.T) {
	was := postMediaStorage
	defer func() { postMediaStorage = was }()
	postMediaStorage = func() (*R2Config, error) { return nil, errNoDimensions }
	if ownUpload("1", testUpload(1, "720p.mp4")) {
		t.Error("with no storage set up, an address was taken as an upload")
	}
	postMediaStorage = was
	if !ownUpload("1", testUpload(1, "720p.mp4")) {
		t.Error("your own upload was not taken as yours")
	}
	if ownUpload("1", testUpload(12, "720p.mp4")) {
		t.Error("user 12's upload was taken as user 1's")
	}
}
