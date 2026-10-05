package main

// Free music, against a real database, through the real handlers: a song is
// picked (and read fresh from Openverse, not taken on the app's word),
// posted with a video or an answer, credited on every list the app reads,
// and a song taken down stops being offered. Openverse is stood in for
// (fakeOpenverse in free_music_test.go). See free_music.go.

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

func musicSetup(t *testing.T) (*httptest.Server, *fakeOpenverse) {
	t.Helper()
	t.Cleanup(withDB(t))
	if _, err := db.Exec(`TRUNCATE music_tracks CASCADE`); err != nil {
		t.Fatal(err)
	}
	actionLimitersMu.Lock()
	for _, a := range []string{"challenge_create", "challenge_accept", "music_search", "music_pick"} {
		delete(actionLimiters, a)
	}
	actionLimitersMu.Unlock()
	liveUsers(t)
	f := newFakeOpenverse(t)

	r := mux.NewRouter()
	api := r.PathPrefix("/api/v1").Subrouter()
	api.HandleFunc("/challenges", authed(CreateChallengeHandler)).Methods("POST")
	api.HandleFunc("/challenges/accept", authed(AcceptChallengeHandler)).Methods("POST")
	api.HandleFunc("/challenges/arena", GetArenaChallengesHandler).Methods("GET")
	api.HandleFunc("/challenges/{id}", GetChallengeDetailHandler).Methods("GET")
	api.HandleFunc("/music/search", authed(MusicSearchHandler)).Methods("GET")
	api.HandleFunc("/music/tracks", authed(PickMusicHandler)).Methods("POST")
	// The admin check is its own test elsewhere; this is about what a block does.
	api.HandleFunc("/admin/music/block", AdminBlockMusicHandler).Methods("POST")
	srv := httptest.NewServer(r)
	t.Cleanup(srv.Close)
	return srv, f
}

// pickSong picks Openverse song [sourceID] as [as] and answers the status
// and what came back.
func pickSong(t *testing.T, srv *httptest.Server, as int, sourceID string) (int, MusicTrack, string) {
	t.Helper()
	res := authedDo(t, srv, as, "POST", "/api/v1/music/tracks", `{"sourceId":"`+sourceID+`"}`)
	raw, _ := io.ReadAll(res.Body)
	var tr MusicTrack
	_ = json.Unmarshal(raw, &tr)
	return res.StatusCode, tr, string(raw)
}

func videoWithSong(trackID string) string {
	return `{"prefix":"Who dances","subject":"best to this","visibility":"arena",` +
		`"videoUrl":"https://cdn/u/1/a/720p.mp4","thumbnailUrl":"https://cdn/u/1/a/default.jpg",` +
		`"musicTrackId":"` + trackID + `"}`
}

func TestAPickedSongIsKeptWithTheCreditOpenverseGives(t *testing.T) {
	srv, f := musicSetup(t)
	f.songs["s1"] = freeSong("s1", "Sunny", "by")

	status, tr, raw := pickSong(t, srv, liveMaya, "s1")
	if status != http.StatusOK {
		t.Fatalf("picking a free song: %d %s", status, raw)
	}
	if tr.ID == "" || tr.Title != "Sunny" || tr.Artist != "Artist s1" || tr.License != "by" ||
		tr.AudioURL != "https://files.example/s1.mp3" || !strings.Contains(tr.Attribution, "Sunny") {
		t.Errorf("kept as %+v", tr)
	}
	var title, licence, source string
	if err := db.QueryRow(`SELECT title, license, source_url FROM music_tracks WHERE id = $1`,
		tr.ID).Scan(&title, &licence, &source); err != nil {
		t.Fatal(err)
	}
	if title != "Sunny" || licence != "by" || source != "https://www.jamendo.com/track/s1" {
		t.Errorf("stored %q %q %q", title, licence, source)
	}
	// Picked again, by anybody: the same song, not a second copy.
	_, again, _ := pickSong(t, srv, liveLeo, "s1")
	if again.ID != tr.ID {
		t.Errorf("picked twice: ids %s and %s", tr.ID, again.ID)
	}
}

func TestASongThatIsNotFreeCannotBePicked(t *testing.T) {
	srv, f := musicSetup(t)
	f.songs["nc"] = freeSong("nc", "Not for apps", "by-nc")
	status, _, raw := pickSong(t, srv, liveMaya, "nc")
	if status != http.StatusUnprocessableEntity || !strings.Contains(raw, "not free") {
		t.Errorf("a non-commercial song: %d %s", status, raw)
	}
	status, _, _ = pickSong(t, srv, liveMaya, "nobody-has-this")
	if status == http.StatusOK {
		t.Error("a song Openverse does not have was picked")
	}
	var n int
	db.QueryRow(`SELECT COUNT(*) FROM music_tracks`).Scan(&n)
	if n != 0 {
		t.Errorf("%d songs kept", n)
	}
}

func TestAPostAndItsAnswerCarryTheirSongsOnEveryScreen(t *testing.T) {
	srv, f := musicSetup(t)
	f.songs["s1"] = freeSong("s1", "Sunny", "by")
	f.songs["s2"] = freeSong("s2", "Rainy", "cc0")
	_, sunny, _ := pickSong(t, srv, liveMaya, "s1")
	_, rainy, _ := pickSong(t, srv, liveLeo, "s2")

	status, ch, raw := postAs(t, srv, liveMaya, videoWithSong(sunny.ID))
	if status != http.StatusCreated {
		t.Fatalf("posting with a song: %d %s", status, raw)
	}
	var stored int64
	var uses int
	if err := db.QueryRow(`SELECT music_track_id FROM challenges WHERE id = $1`, ch.ID).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	db.QueryRow(`SELECT use_count FROM music_tracks WHERE id = $1`, sunny.ID).Scan(&uses)
	if strconv.FormatInt(stored, 10) != sunny.ID || uses != 1 {
		t.Errorf("stored song %d, used %d times", stored, uses)
	}

	answer := `{"challengeId":"` + ch.ID + `","videoUrl":"https://cdn/u/2/b/720p.mp4",` +
		`"durationMs":8000,"musicTrackId":"` + rainy.ID + `"}`
	if status, raw := answerAs(t, srv, liveLeo, answer); status != http.StatusCreated {
		t.Fatalf("answering with a song: %d %s", status, raw)
	}

	// The battle page: the post, its top answer, and the answers list.
	res := authedDo(t, srv, liveSam, "GET", "/api/v1/challenges/"+ch.ID, "")
	var detail struct {
		Challenge Challenge           `json:"challenge"`
		Responses []ChallengeResponse `json:"responses"`
	}
	body, _ := io.ReadAll(res.Body)
	_ = json.Unmarshal(body, &detail)
	if m := detail.Challenge.Music; m == nil || m.Title != "Sunny" || m.Artist != "Artist s1" || m.License != "by" {
		t.Errorf("the battle page credits the post with %+v", m)
	}
	if m := detail.Challenge.TopResponseMusic; m == nil || m.Title != "Rainy" {
		t.Errorf("the battle page credits the top answer with %+v", m)
	}
	if len(detail.Responses) != 1 || detail.Responses[0].Music == nil ||
		detail.Responses[0].Music.Title != "Rainy" {
		t.Errorf("the answers list: %s", body)
	}

	// A list.
	res = authedDo(t, srv, liveSam, "GET", "/api/v1/challenges/arena", "")
	var list []Challenge
	_ = json.NewDecoder(res.Body).Decode(&list)
	found := false
	for _, c := range list {
		if c.ID == ch.ID {
			found = true
			if c.Music == nil || c.Music.Title != "Sunny" {
				t.Errorf("the arena list credits it with %+v", c.Music)
			}
		}
	}
	if !found {
		t.Error("the post is not on the arena list")
	}

	// A feed page, whose records are built without the shared query.
	items := []HomeFeedItem{{Type: "challenge", Challenge: &Challenge{ID: ch.ID}}}
	markViewerStateItems(strconv.Itoa(liveSam), items)
	if m := items[0].Challenge.Music; m == nil || m.Title != "Sunny" {
		t.Errorf("a feed page credits it with %+v", m)
	}
}

func TestAPostWithNoSongCarriesNoCredit(t *testing.T) {
	srv, _ := musicSetup(t)
	status, ch, raw := postAs(t, srv, liveMaya, videoWithSong(""))
	if status != http.StatusCreated {
		t.Fatalf("%d %s", status, raw)
	}
	res := authedDo(t, srv, liveSam, "GET", "/api/v1/challenges/"+ch.ID, "")
	body, _ := io.ReadAll(res.Body)
	if strings.Contains(string(body), `"music"`) {
		t.Errorf("a post with no song says it has one: %s", body)
	}
}

func TestAPostCannotClaimASongTheLibraryDoesNotHave(t *testing.T) {
	srv, f := musicSetup(t)
	for _, id := range []string{"999999", "abc", "-4"} {
		if status, _, raw := postAs(t, srv, liveMaya, videoWithSong(id)); status != http.StatusBadRequest {
			t.Errorf("song %q: %d %s", id, status, raw)
		}
	}
	f.songs["s1"] = freeSong("s1", "Sunny", "by")
	_, sunny, _ := pickSong(t, srv, liveMaya, "s1")
	photo := strings.Replace(photoChallengeBody, `"mediaType":"photo"`,
		`"mediaType":"photo","musicTrackId":"`+sunny.ID+`"`, 1)
	if status, _, raw := postAs(t, srv, liveMaya, photo); status != http.StatusBadRequest ||
		!strings.Contains(raw, "photo has no sound") {
		t.Errorf("a photo with a song: %d %s", status, raw)
	}
	var n int
	db.QueryRow(`SELECT COUNT(*) FROM challenges`).Scan(&n)
	if n != 0 {
		t.Errorf("%d posts went up", n)
	}
}

func TestASongTakenDownIsNotOfferedPickedOrPosted(t *testing.T) {
	srv, f := musicSetup(t)
	f.songs["bad"] = freeSong("bad", "Film song, uploaded as free", "by")
	f.results = []openverseAudio{f.songs["bad"], freeSong("ok", "Fine", "by")}
	_, bad, _ := pickSong(t, srv, liveMaya, "bad")
	// A song nobody here has used yet can be taken down too.
	f.results = append(f.results, freeSong("unused", "Also bad", "by"))

	for _, id := range []string{"bad", "unused"} {
		res := authedDo(t, srv, 1, "POST", "/api/v1/admin/music/block", `{"sourceId":"`+id+`"}`)
		if res.StatusCode != http.StatusOK {
			t.Fatalf("blocking %s: %d", id, res.StatusCode)
		}
	}

	res := authedDo(t, srv, liveLeo, "GET", "/api/v1/music/search?q=song", "")
	var page struct {
		Tracks []MusicTrack `json:"tracks"`
	}
	_ = json.NewDecoder(res.Body).Decode(&page)
	if len(page.Tracks) != 1 || page.Tracks[0].SourceID != "ok" {
		t.Errorf("search offers %+v, want only the song not taken down", page.Tracks)
	}
	if status, _, _ := pickSong(t, srv, liveLeo, "bad"); status != http.StatusUnprocessableEntity {
		t.Errorf("picking a song taken down: %d", status)
	}
	if status, _, raw := postAs(t, srv, liveMaya, videoWithSong(bad.ID)); status != http.StatusBadRequest ||
		!strings.Contains(raw, "taken down") {
		t.Errorf("posting with a song taken down: %d %s", status, raw)
	}
}

func TestThePickerOpensOnTheMostUsedSongsAndSaysWhenSearchIsBusy(t *testing.T) {
	srv, f := musicSetup(t)
	f.songs["once"] = freeSong("once", "Used once", "by")
	f.songs["twice"] = freeSong("twice", "Used twice", "cc0")
	f.results = []openverseAudio{freeSong("new", "Brand new", "by")}
	_, once, _ := pickSong(t, srv, liveMaya, "once")
	_, twice, _ := pickSong(t, srv, liveMaya, "twice")
	postAs(t, srv, liveMaya, videoWithSong(once.ID))
	postAs(t, srv, liveMaya, videoWithSong(twice.ID))
	postAs(t, srv, liveLeo, videoWithSong(twice.ID))
	// Picked, never posted: not popular.
	f.songs["picked"] = freeSong("picked", "Only picked", "by")
	pickSong(t, srv, liveMaya, "picked")

	type pickerPage struct {
		Popular []MusicTrack `json:"popular"`
		Tracks  []MusicTrack `json:"tracks"`
		Busy    bool         `json:"busy"`
	}
	res := authedDo(t, srv, liveSam, "GET", "/api/v1/music/search", "")
	var first pickerPage
	_ = json.NewDecoder(res.Body).Decode(&first)
	if len(first.Popular) != 2 || first.Popular[0].Title != "Used twice" || first.Popular[1].Title != "Used once" {
		t.Errorf("popular: %+v", first.Popular)
	}
	if len(first.Tracks) != 1 || first.Tracks[0].Title != "Brand new" || first.Busy {
		t.Errorf("tracks %+v busy %v", first.Tracks, first.Busy)
	}

	f.mu.Lock()
	f.busy = true
	f.mu.Unlock()
	res = authedDo(t, srv, liveSam, "GET", "/api/v1/music/search?q=never+asked", "")
	if res.StatusCode != http.StatusOK {
		t.Fatalf("a busy search failed outright: %d", res.StatusCode)
	}
	var busy pickerPage
	_ = json.NewDecoder(res.Body).Decode(&busy)
	if !busy.Busy || len(busy.Tracks) != 0 {
		t.Errorf("busy search: %+v", busy)
	}
}
