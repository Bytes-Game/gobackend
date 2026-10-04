package main

// Photo challenges, against a real database: posted, answered with a photo,
// kept out of the video worker, and marked as photos on the lists the app
// reads. See photo_posts.go.

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/gorilla/mux"
)

func photoServer(t *testing.T) *httptest.Server {
	t.Helper()
	r := mux.NewRouter()
	api := r.PathPrefix("/api/v1").Subrouter()
	api.HandleFunc("/challenges", authed(CreateChallengeHandler)).Methods("POST")
	api.HandleFunc("/challenges/accept", authed(AcceptChallengeHandler)).Methods("POST")
	api.HandleFunc("/challenges/arena", GetArenaChallengesHandler).Methods("GET")
	api.HandleFunc("/challenges/{id}", GetChallengeDetailHandler).Methods("GET")
	srv := httptest.NewServer(r)
	t.Cleanup(srv.Close)
	return srv
}

func photoSetup(t *testing.T) *httptest.Server {
	t.Helper()
	t.Cleanup(withDB(t))
	// Several posts a test, more than the hourly allowance lets through.
	actionLimitersMu.Lock()
	delete(actionLimiters, "challenge_create")
	delete(actionLimiters, "challenge_accept")
	actionLimitersMu.Unlock()
	liveUsers(t)
	return photoServer(t)
}

// postAs posts a challenge as [as] and answers the status and the record.
func postAs(t *testing.T, srv *httptest.Server, as int, body string) (int, Challenge, string) {
	t.Helper()
	res := authedDo(t, srv, as, "POST", "/api/v1/challenges", body)
	raw, _ := io.ReadAll(res.Body)
	var ch Challenge
	_ = json.Unmarshal(raw, &ch)
	return res.StatusCode, ch, string(raw)
}

func answerAs(t *testing.T, srv *httptest.Server, as int, body string) (int, string) {
	t.Helper()
	res := authedDo(t, srv, as, "POST", "/api/v1/challenges/accept", body)
	raw, _ := io.ReadAll(res.Body)
	return res.StatusCode, string(raw)
}

const photoChallengeBody = `{"prefix":"Who looks","subject":"better in red",` +
	`"visibility":"arena","mediaType":"photo",` +
	`"videoUrl":"https://cdn/u/1/a/photo.jpg","thumbnailUrl":"https://cdn/u/1/a/default.jpg"}`

func TestPhotoChallengeIsPostedAsAPhoto(t *testing.T) {
	srv := photoSetup(t)
	status, ch, raw := postAs(t, srv, liveMaya, photoChallengeBody)
	if status != http.StatusCreated {
		t.Fatalf("posting a photo challenge: %d %s", status, raw)
	}
	if ch.MediaType != mediaPhoto {
		t.Errorf("the answer says %q, want photo", ch.MediaType)
	}
	var kind, url string
	if err := db.QueryRow(`SELECT media_type, video_url FROM challenges WHERE id = $1`,
		ch.ID).Scan(&kind, &url); err != nil {
		t.Fatal(err)
	}
	if kind != mediaPhoto || url != "https://cdn/u/1/a/photo.jpg" {
		t.Errorf("stored as %q at %q", kind, url)
	}
}

func TestPhotoChallengeIsNotHeldToVideoLength(t *testing.T) {
	srv := photoSetup(t)
	// A length no video may have: a photo has none, and what an app sends
	// for it is not a reason to refuse.
	body := strings.Replace(photoChallengeBody, `"mediaType":"photo"`,
		`"mediaType":"photo","durationMs":99999999`, 1)
	if status, _, raw := postAs(t, srv, liveMaya, body); status != http.StatusCreated {
		t.Fatalf("a photo was refused as a long video: %d %s", status, raw)
	}
	// The same length on a video is still refused: the check is not gone.
	video := strings.Replace(body, `"mediaType":"photo"`, `"mediaType":"video"`, 1)
	if status, _, _ := postAs(t, srv, liveMaya, video); status != http.StatusBadRequest {
		t.Errorf("an over-long video was let through: %d", status)
	}
}

func TestPhotoChallengeNeedsItsPhoto(t *testing.T) {
	srv := photoSetup(t)
	body := strings.Replace(photoChallengeBody, `"videoUrl":"https://cdn/u/1/a/photo.jpg"`,
		`"videoUrl":""`, 1)
	if status, _, _ := postAs(t, srv, liveMaya, body); status != http.StatusBadRequest {
		t.Errorf("a photo challenge with no photo: %d, want 400", status)
	}
	odd := strings.Replace(photoChallengeBody, `"mediaType":"photo"`, `"mediaType":"gif"`, 1)
	if status, _, _ := postAs(t, srv, liveMaya, odd); status != http.StatusBadRequest {
		t.Errorf("an unknown kind of post: %d, want 400", status)
	}
}

func TestAPhotoChallengeIsAnsweredWithAPhoto(t *testing.T) {
	srv := photoSetup(t)
	_, ch, _ := postAs(t, srv, liveMaya, photoChallengeBody)

	video := `{"challengeId":"` + ch.ID + `","videoUrl":"https://cdn/u/2/b/720p.mp4",` +
		`"durationMs":8000}`
	status, raw := answerAs(t, srv, liveLeo, video)
	if status != http.StatusBadRequest || !strings.Contains(raw, "answer it with a photo") {
		t.Errorf("a video answer to a photo challenge: %d %s", status, raw)
	}

	photo := `{"challengeId":"` + ch.ID + `","mediaType":"photo",` +
		`"videoUrl":"https://cdn/u/2/b/photo.jpg","thumbnailUrl":"https://cdn/u/2/b/default.jpg"}`
	status, raw = answerAs(t, srv, liveLeo, photo)
	if status != http.StatusCreated {
		t.Fatalf("a photo answer, with no length: %d %s", status, raw)
	}
	var kind string
	if err := db.QueryRow(`SELECT media_type FROM challenge_responses
	                        WHERE challenge_id = $1`, ch.ID).Scan(&kind); err != nil {
		t.Fatal(err)
	}
	if kind != mediaPhoto {
		t.Errorf("the answer is stored as %q", kind)
	}
}

func TestAVideoChallengeIsNotAnsweredWithAPhoto(t *testing.T) {
	srv := photoSetup(t)
	video := strings.Replace(photoChallengeBody, `"mediaType":"photo"`, `"mediaType":"video"`, 1)
	_, ch, _ := postAs(t, srv, liveMaya, video)
	photo := `{"challengeId":"` + ch.ID + `","mediaType":"photo",` +
		`"videoUrl":"https://cdn/u/2/b/photo.jpg"}`
	status, raw := answerAs(t, srv, liveLeo, photo)
	if status != http.StatusBadRequest || !strings.Contains(raw, "answer it with a video") {
		t.Errorf("a photo answer to a video challenge: %d %s", status, raw)
	}
}

func TestPhotosAreNeverOfferedToTheVideoWorker(t *testing.T) {
	t.Cleanup(withDB(t))
	liveUsers(t)
	ids := map[string]int{}
	for _, kind := range []string{mediaVideo, mediaPhoto} {
		var id int
		if err := db.QueryRow(`
			INSERT INTO challenges (creator_id, video_url, prefix, subject, visibility, media_type)
			VALUES ($1, 'https://cdn/x', 'Who', 'is best', 'arena', $2) RETURNING id`,
			liveMaya, kind).Scan(&id); err != nil {
			t.Fatal(err)
		}
		var rid int
		if err := db.QueryRow(`
			INSERT INTO challenge_responses (challenge_id, responder_id, video_url, media_type)
			VALUES ($1, $2, 'https://cdn/y', $3) RETURNING id`,
			id, liveLeo, kind).Scan(&rid); err != nil {
			t.Fatal(err)
		}
		ids[kind] = id
		ids[kind+"-answer"] = rid
	}
	offered := func(table string, id int) bool {
		var ok bool
		if err := db.QueryRow(`SELECT EXISTS (SELECT 1 FROM `+table+
			` WHERE id = $2 AND `+hlsClaimableWhere()+`)`, 60, id).Scan(&ok); err != nil {
			t.Fatal(err)
		}
		return ok
	}
	if !offered("challenges", ids[mediaVideo]) ||
		!offered("challenge_responses", ids[mediaVideo+"-answer"]) {
		t.Fatal("a video waiting to be converted is not offered — the test proves nothing")
	}
	if offered("challenges", ids[mediaPhoto]) {
		t.Error("a photo challenge is offered to the video worker")
	}
	if offered("challenge_responses", ids[mediaPhoto+"-answer"]) {
		t.Error("a photo answer is offered to the video worker")
	}

	// Nor is a photo listed in the admin page's conversion queue, where it
	// would sit as "waiting" for ever.
	rows, err := readHLSQueue("challenges", "challenge", 500)
	if err != nil {
		t.Fatal(err)
	}
	listed := map[int]bool{}
	for _, r := range rows {
		listed[r.ID] = true
	}
	if !listed[ids[mediaVideo]] {
		t.Fatal("the video is not in the admin queue — the check proves nothing")
	}
	if listed[ids[mediaPhoto]] {
		t.Error("a photo is listed in the admin conversion queue")
	}
}

func TestListsTheAppReadsSayWhichPostsArePhotos(t *testing.T) {
	srv := photoSetup(t)
	_, photo, _ := postAs(t, srv, liveMaya, photoChallengeBody)
	video := strings.Replace(photoChallengeBody, `"mediaType":"photo"`, `"mediaType":"video"`, 1)
	_, vid, _ := postAs(t, srv, liveMaya, video)

	res := authedDo(t, srv, liveLeo, "GET", "/api/v1/challenges/arena", "")
	var list []Challenge
	_ = json.NewDecoder(res.Body).Decode(&list)
	kinds := map[string]string{}
	for _, c := range list {
		kinds[c.ID] = c.MediaType
	}
	if kinds[photo.ID] != mediaPhoto {
		t.Errorf("the arena list has the photo as %q", kinds[photo.ID])
	}
	if k := kinds[vid.ID]; k != mediaVideo && k != "" {
		t.Errorf("the arena list has the video as %q", k)
	}

	res = authedDo(t, srv, liveLeo, "GET", "/api/v1/challenges/"+photo.ID, "")
	var detail struct {
		Challenge Challenge `json:"challenge"`
	}
	raw, _ := io.ReadAll(res.Body)
	_ = json.Unmarshal(raw, &detail)
	if detail.Challenge.MediaType != mediaPhoto {
		t.Errorf("the battle page has it as %q: %s", detail.Challenge.MediaType, raw)
	}
}

func TestAFeedPageBuiltWithoutTheSharedQueryIsMarkedToo(t *testing.T) {
	srv := photoSetup(t)
	_, photo, _ := postAs(t, srv, liveMaya, photoChallengeBody)
	// The feeds make their own records, with no media type on them; the
	// marking step every feed page goes through has to fill it in.
	items := []HomeFeedItem{{Type: "challenge", Challenge: &Challenge{ID: photo.ID}}}
	markViewerStateItems(strconv.Itoa(liveLeo), items)
	if items[0].Challenge.MediaType != mediaPhoto {
		t.Errorf("a feed page has the photo as %q", items[0].Challenge.MediaType)
	}
}

func TestThePhoneIsNeverAskedToPreloadAPhotoAsAVideo(t *testing.T) {
	srv := photoSetup(t)
	_, photo, _ := postAs(t, srv, liveMaya, photoChallengeBody)
	video := strings.Replace(photoChallengeBody, `"mediaType":"photo"`, `"mediaType":"video"`, 1)
	video = strings.Replace(video, "u/1/a/photo.jpg", "u/1/v/720p.mp4", 1)
	_, vid, _ := postAs(t, srv, liveMaya, video)

	// As the trending source builds them: no media type on the record.
	candidates := []HomeFeedItem{
		{Type: "challenge", Challenge: &Challenge{ID: photo.ID, VideoURL: "https://cdn/u/1/a/photo.jpg"}},
		{Type: "challenge", Challenge: &Challenge{ID: vid.ID, VideoURL: "https://cdn/u/1/v/720p.mp4"}},
	}
	if got := hintURLFrom(candidates, ""); got != "https://cdn/u/1/v/720p.mp4" {
		t.Errorf("hinted %q; want the video, not the photo ahead of it", got)
	}
	// And the one on screen is still never hinted back.
	if got := hintURLFrom(candidates, vid.ID); got != "" {
		t.Errorf("hinted %q with only a photo and the reel on screen left", got)
	}
}
