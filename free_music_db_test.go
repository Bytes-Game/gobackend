package main

// Free music, against a real database, through the real handlers: a song is
// picked (and read fresh from Openverse, not taken on the app's word),
// posted with a video or an answer, credited on every list the app reads,
// and a song taken down stops being offered. Openverse is stood in for
// (fakeOpenverse in free_music_test.go). See free_music.go.

import (
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/mux"
)

func musicSetup(t *testing.T) (*httptest.Server, *fakeOpenverse) {
	t.Helper()
	t.Cleanup(withDB(t))
	if _, err := db.Exec(`TRUNCATE music_tracks, music_searches CASCADE`); err != nil {
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

// searchAs asks the picker's search as [as] and answers its songs' ids, in
// order, and whether it said busy.
func searchAs(t *testing.T, srv *httptest.Server, as int, q string) ([]string, bool) {
	t.Helper()
	res := authedDo(t, srv, as, "GET", "/api/v1/music/search?q="+q, "")
	var page struct {
		Tracks []MusicTrack `json:"tracks"`
		Busy   bool         `json:"busy"`
	}
	if err := json.NewDecoder(res.Body).Decode(&page); err != nil {
		t.Fatalf("search %q: %v", q, err)
	}
	ids := []string{}
	for _, tr := range page.Tracks {
		ids = append(ids, tr.SourceID)
	}
	return ids, page.Busy
}

func TestASearchIsSavedAndAnsweredFromTheDatabaseAfterARestart(t *testing.T) {
	srv, f := musicSetup(t)
	f.results = []openverseAudio{freeSong("a", "Happy A", "by"), freeSong("b", "Happy B", "cc0")}
	if ids, _ := searchAs(t, srv, liveMaya, "Happy"); strings.Join(ids, ",") != "a,b" {
		t.Fatalf("first search: %v", ids)
	}
	// The server restarts: its memory is gone.
	forgetMusicSearches()
	// Somebody else, later, the same words.
	ids, busy := searchAs(t, srv, liveLeo, "happy")
	if strings.Join(ids, ",") != "a,b" || busy {
		t.Errorf("after a restart: %v busy=%v", ids, busy)
	}
	if n := f.searchCount(); n != 1 {
		t.Errorf("Openverse was asked %d times for the same search, want once", n)
	}
	var used int
	db.QueryRow(`SELECT times_used FROM music_searches WHERE query = 'happy' AND page = 1`).Scan(&used)
	if used != 2 {
		t.Errorf("the saved search was used %d times, want 2", used)
	}
}

func TestASavedSearchIsAskedAgainAfterThirtyDaysAndKeptWhenOpenverseIsBusy(t *testing.T) {
	srv, f := musicSetup(t)
	f.results = []openverseAudio{freeSong("a", "Old", "by")}
	searchAs(t, srv, liveMaya, "song")
	age := func() {
		if _, err := db.Exec(`UPDATE music_searches SET fetched_at = NOW() - INTERVAL '31 days'`); err != nil {
			t.Fatal(err)
		}
		forgetMusicSearches()
	}

	age()
	f.mu.Lock()
	f.results = []openverseAudio{freeSong("c", "New", "by")}
	f.mu.Unlock()
	if ids, busy := searchAs(t, srv, liveLeo, "song"); strings.Join(ids, ",") != "c" || busy {
		t.Errorf("after 30 days: %v busy=%v, want the fresh answer", ids, busy)
	}
	if n := f.searchCount(); n != 2 {
		t.Errorf("Openverse asked %d times, want 2", n)
	}

	age()
	f.mu.Lock()
	f.busy = true
	f.mu.Unlock()
	if ids, busy := searchAs(t, srv, liveSam, "song"); strings.Join(ids, ",") != "c" || !busy {
		t.Errorf("old and Openverse busy: %v busy=%v, want the saved answer, marked busy", ids, busy)
	}
}

func TestASongTakenDownDropsOutOfSavedSearches(t *testing.T) {
	srv, f := musicSetup(t)
	f.results = []openverseAudio{freeSong("a", "Fine", "by"), freeSong("b", "Bad", "by")}
	searchAs(t, srv, liveMaya, "song")
	res := authedDo(t, srv, 1, "POST", "/api/v1/admin/music/block", `{"sourceId":"b"}`)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("block: %d", res.StatusCode)
	}
	forgetMusicSearches()
	if ids, _ := searchAs(t, srv, liveLeo, "song"); strings.Join(ids, ",") != "a" {
		t.Errorf("a saved search still offers the song taken down: %v", ids)
	}
	if n := f.searchCount(); n != 1 {
		t.Errorf("answered from Openverse again (%d), not from the saved search", n)
	}
}

func TestSavedSearchesAreKeptToTheirLimitWithTheSongsStillNeeded(t *testing.T) {
	srv, f := musicSetup(t)
	prev := musicSavedMax
	musicSavedMax = 2
	t.Cleanup(func() { musicSavedMax = prev })

	for _, q := range []string{"one", "two", "three"} {
		f.mu.Lock()
		f.results = []openverseAudio{freeSong("only-"+q, q, "by"), freeSong("shared", "Shared", "by")}
		f.mu.Unlock()
		searchAs(t, srv, liveMaya, q)
	}
	// "one" was used longest ago.
	if _, err := db.Exec(`UPDATE music_searches SET used_at = NOW() - INTERVAL '3 days' WHERE query = 'one';
		UPDATE music_searches SET used_at = NOW() - INTERVAL '2 days' WHERE query = 'two'`); err != nil {
		t.Fatal(err)
	}
	// Songs posted with: a challenge's and an answer's.
	for _, id := range []string{"posted", "answered", "repicked"} {
		f.songs[id] = freeSong(id, id, "by")
	}
	_, posted, _ := pickSong(t, srv, liveMaya, "posted")
	_, ch, _ := postAs(t, srv, liveMaya, videoWithSong(posted.ID))
	_, answered, _ := pickSong(t, srv, liveLeo, "answered")
	answerAs(t, srv, liveLeo, `{"challengeId":"`+ch.ID+`","videoUrl":"https://cdn/u/2/b/720p.mp4",`+
		`"durationMs":8000,"musicTrackId":"`+answered.ID+`"}`)
	// Picked a week ago, not posted yet: picked again just now, below.
	pickSong(t, srv, liveSam, "repicked")
	// A song taken down, and one counted as used though no post holds it now.
	authedDo(t, srv, 1, "POST", "/api/v1/admin/music/block", `{"sourceId":"blocked"}`)
	if _, err := db.Exec(`INSERT INTO music_tracks (source, source_id, license, use_count)
		VALUES ('openverse', 'counted', 'by', 3)`); err != nil {
		t.Fatal(err)
	}
	// Everything last seen long ago but "only-two", seen just now; and the
	// two posts' counts lost, so only the posts themselves can keep them.
	if _, err := db.Exec(`UPDATE music_tracks SET seen_at = NOW() - INTERVAL '8 days' WHERE source_id <> 'only-two';
		UPDATE music_tracks SET use_count = 0 WHERE source_id IN ('posted', 'answered')`); err != nil {
		t.Fatal(err)
	}
	pickSong(t, srv, liveSam, "repicked")

	pruneSavedMusic(t.Context())

	var kept []string
	rows, err := db.Query(`SELECT query FROM music_searches ORDER BY query`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var q string
		rows.Scan(&q)
		kept = append(kept, q)
	}
	rows.Close()
	if strings.Join(kept, ",") != "three,two" {
		t.Errorf("kept searches %v, want the two used most recently", kept)
	}
	has := func(id string) bool {
		var n int
		db.QueryRow(`SELECT COUNT(*) FROM music_tracks WHERE source_id = $1`, id).Scan(&n)
		return n == 1
	}
	if has("only-one") {
		t.Error("a song only the dropped search had is still kept")
	}
	for id, why := range map[string]string{
		"shared":     "a saved search still has it",
		"only-three": "a saved search still has it",
		"only-two":   "it was seen this week",
		"repicked":   "somebody picked it just now",
		"posted":     "a challenge uses it",
		"answered":   "an answer uses it",
		"blocked":    "it is taken down, and must stay so",
		"counted":    "it counts as used",
	} {
		if !has(id) {
			t.Errorf("song %q was removed, but %s", id, why)
		}
	}
}

func TestASavedSearchTakesAboutAKilobyte(t *testing.T) {
	srv, f := musicSetup(t)
	var songs []openverseAudio
	r := rand.New(rand.NewSource(1))
	for i := 0; i < musicPageSize; i++ {
		// Openverse ids are random 36-character UUIDs, which do not squash
		// down the way a run of zeros would.
		id := fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
			r.Uint32(), r.Intn(1<<16), r.Intn(1<<16), r.Intn(1<<16), r.Int63n(1<<48))
		songs = append(songs, freeSong(id, "A song", "by"))
	}
	f.results = songs
	searchAs(t, srv, liveMaya, "size")
	var bytes int
	if err := db.QueryRow(`SELECT pg_column_size(s.*) FROM music_searches s`).Scan(&bytes); err != nil {
		t.Fatal(err)
	}
	if bytes > 1500 {
		t.Errorf("one saved search of %d songs takes %d bytes, more than the "+
			"kilobyte or so the README promises", musicPageSize, bytes)
	}
	t.Logf("one saved search of %d songs: %d bytes", musicPageSize, bytes)
}

func TestSavingASearchKeepsThemToTheLimitAtMostOnceAnHour(t *testing.T) {
	srv, f := musicSetup(t)
	prev := musicSavedMax
	musicSavedMax = 1
	t.Cleanup(func() { musicSavedMax = prev })
	count := func() int {
		var n int
		db.QueryRow(`SELECT COUNT(*) FROM music_searches`).Scan(&n)
		return n
	}
	f.results = []openverseAudio{freeSong("a", "A", "by")}
	searchAs(t, srv, liveMaya, "one")
	// An hour on: the next save keeps them to the limit.
	musicPrune.Lock()
	musicPrune.last = time.Now().Add(-2 * time.Hour)
	musicPrune.Unlock()
	searchAs(t, srv, liveMaya, "two")
	if n := count(); n != 1 {
		t.Fatalf("%d saved searches after a save an hour on, want the limit of 1", n)
	}
	// Within the hour: not again — a full clean-up on every search would
	// cost more than the space it saves.
	searchAs(t, srv, liveMaya, "three")
	if n := count(); n != 2 {
		t.Errorf("%d saved searches, want 2: kept to the limit twice within an hour", n)
	}
}
