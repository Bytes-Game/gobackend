package main

// Free music, without a database: which licences count as free, what the
// server asks Openverse for, the six-hour memory of searches, and the
// registered account. Openverse itself is stood in for (fakeOpenverse).
// See free_music.go.

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeOpenverse stands in for api.openverse.org and records what it was
// asked.
type fakeOpenverse struct {
	mu        sync.Mutex
	srv       *httptest.Server
	searches  []url.Values
	auths     []string
	details   []string
	logins    int
	busy      bool
	loginFail bool
	results   []openverseAudio
	pageCount int
	songs     map[string]openverseAudio
}

func newFakeOpenverse(t *testing.T) *fakeOpenverse {
	t.Helper()
	f := &fakeOpenverse{pageCount: 1, songs: map[string]openverseAudio{}}
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/auth_tokens/token/", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.logins++
		fail := f.loginFail
		f.mu.Unlock()
		_ = r.ParseForm()
		if fail || r.PostForm.Get("grant_type") != "client_credentials" ||
			r.PostForm.Get("client_id") != "app-id" || r.PostForm.Get("client_secret") != "app-secret" {
			http.Error(w, `{"error":"invalid_client"}`, http.StatusUnauthorized)
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"access_token": "pass-1", "expires_in": 3600})
	})
	mux.HandleFunc("/v1/audio/", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.auths = append(f.auths, r.Header.Get("Authorization"))
		if f.busy {
			http.Error(w, `{"detail":"Request was throttled."}`, http.StatusTooManyRequests)
			return
		}
		id := strings.Trim(strings.TrimPrefix(r.URL.Path, "/v1/audio/"), "/")
		if id != "" {
			f.details = append(f.details, id)
			song, ok := f.songs[id]
			if !ok {
				http.NotFound(w, r)
				return
			}
			json.NewEncoder(w).Encode(song)
			return
		}
		f.searches = append(f.searches, r.URL.Query())
		json.NewEncoder(w).Encode(map[string]any{
			"result_count": len(f.results),
			"page_count":   f.pageCount,
			"results":      f.results,
		})
	})
	f.srv = httptest.NewServer(mux)
	prevBase := openverseBase
	openverseBase = f.srv.URL
	forgetMusicSearches()
	t.Cleanup(func() {
		f.srv.Close()
		openverseBase = prevBase
		forgetMusicSearches()
	})
	return f
}

// forgetMusicSearches empties the six-hour memory and the login, so one
// test's answers are not the next one's.
func forgetMusicSearches() {
	musicCache.Lock()
	musicCache.m = map[string]musicCacheEntry{}
	musicCache.Unlock()
	openverseLogin.Lock()
	openverseLogin.token, openverseLogin.until, openverseLogin.retryAt = "", time.Time{}, time.Time{}
	openverseLogin.Unlock()
}

func (f *fakeOpenverse) searchCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.searches)
}

// freeSong is a song as Openverse describes it.
func freeSong(id, title, licence string) openverseAudio {
	return openverseAudio{
		ID:                id,
		Title:             title,
		Creator:           "Artist " + id,
		License:           licence,
		LicenseVersion:    "4.0",
		LicenseURL:        "https://creativecommons.org/licenses/" + licence + "/4.0/",
		Source:            "jamendo",
		Category:          "music",
		Duration:          180000,
		URL:               "https://files.example/" + id + ".mp3",
		ForeignLandingURL: "https://www.jamendo.com/track/" + id,
		Attribution:       `"` + title + `" by Artist ` + id + ` is licensed under CC ` + strings.ToUpper(licence) + ` 4.0.`,
		Genres:            []string{"pop"},
	}
}

func TestOnlyLicencesThatAllowAVideoInAnAppThatMakesMoneyAreOffered(t *testing.T) {
	for _, c := range []struct {
		licence string
		free    bool
	}{
		{"by", true}, {"cc0", true}, {"pdm", true}, {"BY", true},
		// Non-commercial: this app may make money.
		{"by-nc", false}, {"by-nc-sa", false}, {"by-nc-nd", false}, {"nc-sampling+", false},
		// No changes: a song under a video is a change.
		{"by-nd", false},
		// Share-alike: the whole video would have to be given away.
		{"by-sa", false},
		{"sampling+", false}, {"", false},
	} {
		_, why := trackFromOpenverse(freeSong("s1", "Song", c.licence))
		if got := why == ""; got != c.free {
			t.Errorf("licence %q: offered=%v, want %v (%s)", c.licence, got, c.free, why)
		}
	}

	adult := freeSong("s1", "Song", "by")
	adult.Mature = true
	if _, why := trackFromOpenverse(adult); why == "" {
		t.Error("a song marked for adults is offered")
	}
	silent := freeSong("s1", "Song", "by")
	silent.URL = ""
	if _, why := trackFromOpenverse(silent); why == "" {
		t.Error("a song with nothing to play is offered")
	}
}

func TestACreditIsNeverBlank(t *testing.T) {
	a := freeSong("s1", "  ", "cc0")
	a.Creator = ""
	tr, why := trackFromOpenverse(a)
	if why != "" {
		t.Fatal(why)
	}
	if tr.Title != "Untitled" || tr.Artist != "Unknown artist" {
		t.Errorf("credit %q by %q", tr.Title, tr.Artist)
	}
}

func TestASearchAsksOpenverseForFreeMusicOnly(t *testing.T) {
	f := newFakeOpenverse(t)
	f.pageCount = 5
	// One that Openverse should not have sent: kept out all the same.
	f.results = []openverseAudio{freeSong("a", "Happy Day", "by"), freeSong("b", "Sneaky", "by-nc")}

	page, stale, err := searchFreeMusic(t.Context(), "happy", 2)
	if err != nil || stale {
		t.Fatalf("search: %v (stale %v)", err, stale)
	}
	asked := f.searches[0]
	for k, want := range map[string]string{
		"q": "happy", "license": "by,cc0,pdm", "category": "music",
		"mature": "false", "page": "2", "page_size": "20",
	} {
		if got := asked.Get(k); got != want {
			t.Errorf("asked %s=%q, want %q", k, got, want)
		}
	}
	if len(page.Tracks) != 1 || page.Tracks[0].SourceID != "a" {
		t.Fatalf("offered %+v, want only the CC BY song", page.Tracks)
	}
	got := page.Tracks[0]
	if got.Title != "Happy Day" || got.Artist != "Artist a" || got.License != "by" ||
		got.AudioURL != "https://files.example/a.mp3" || got.Attribution == "" {
		t.Errorf("track %+v", got)
	}
	if !page.HasMore {
		t.Error("page 2 of 5 says there is no more")
	}
}

func TestAnEmptySearchIsEveryFreeSong(t *testing.T) {
	f := newFakeOpenverse(t)
	if _, _, err := searchFreeMusic(t.Context(), "", 1); err != nil {
		t.Fatal(err)
	}
	if _, has := f.searches[0]["q"]; has {
		t.Error("an empty search sends q, which narrows it to nothing")
	}
}

func TestASearchIsRememberedForSixHours(t *testing.T) {
	f := newFakeOpenverse(t)
	f.results = []openverseAudio{freeSong("a", "Happy", "by")}
	for i := 0; i < 3; i++ {
		if _, _, err := searchFreeMusic(t.Context(), "Happy", 1); err != nil {
			t.Fatal(err)
		}
	}
	if _, _, err := searchFreeMusic(t.Context(), "happy", 1); err != nil {
		t.Fatal(err)
	}
	if n := f.searchCount(); n != 1 {
		t.Fatalf("four searches for the same thing asked Openverse %d times", n)
	}

	// Six hours on, it is asked again.
	musicCache.Lock()
	for k, e := range musicCache.m {
		e.at = e.at.Add(-musicCacheFor - time.Minute)
		musicCache.m[k] = e
	}
	musicCache.Unlock()
	if _, _, err := searchFreeMusic(t.Context(), "happy", 1); err != nil {
		t.Fatal(err)
	}
	if n := f.searchCount(); n != 2 {
		t.Errorf("after six hours Openverse was asked %d times in all, want 2", n)
	}
}

func TestWhenOpenverseIsBusyTheLastAnswerIsShownAndSaysSo(t *testing.T) {
	f := newFakeOpenverse(t)
	f.results = []openverseAudio{freeSong("a", "Happy", "by")}
	if _, _, err := searchFreeMusic(t.Context(), "happy", 1); err != nil {
		t.Fatal(err)
	}
	musicCache.Lock()
	for k, e := range musicCache.m {
		e.at = e.at.Add(-musicCacheFor - time.Hour)
		musicCache.m[k] = e
	}
	musicCache.Unlock()
	f.mu.Lock()
	f.busy = true
	f.mu.Unlock()

	page, stale, err := searchFreeMusic(t.Context(), "happy", 1)
	if err != nil || !stale || len(page.Tracks) != 1 {
		t.Errorf("busy, with an old answer: %d tracks, stale=%v, err=%v", len(page.Tracks), stale, err)
	}
	if _, _, err := searchFreeMusic(t.Context(), "never asked", 1); err != errMusicBusy {
		t.Errorf("busy, with nothing remembered: %v, want errMusicBusy", err)
	}
}

func TestARegisteredAccountIsUsedAndItsPassKept(t *testing.T) {
	f := newFakeOpenverse(t)
	t.Setenv("OPENVERSE_CLIENT_ID", "app-id")
	t.Setenv("OPENVERSE_CLIENT_SECRET", "app-secret")
	for _, q := range []string{"one", "two"} {
		if _, _, err := searchFreeMusic(t.Context(), q, 1); err != nil {
			t.Fatal(err)
		}
	}
	if f.logins != 1 {
		t.Errorf("logged in %d times for two searches, want once", f.logins)
	}
	for i, a := range f.auths {
		if a != "Bearer pass-1" {
			t.Errorf("search %d went as %q, not with the account's pass", i, a)
		}
	}
}

func TestAFailedLoginStillSearchesAsAVisitorAndDoesNotRetryEveryTime(t *testing.T) {
	f := newFakeOpenverse(t)
	f.loginFail = true
	t.Setenv("OPENVERSE_CLIENT_ID", "app-id")
	t.Setenv("OPENVERSE_CLIENT_SECRET", "wrong")
	for _, q := range []string{"one", "two"} {
		if _, _, err := searchFreeMusic(t.Context(), q, 1); err != nil {
			t.Fatalf("a failed login stopped the search: %v", err)
		}
	}
	if f.logins != 1 {
		t.Errorf("tried to log in %d times in a row, want once per 10 minutes", f.logins)
	}
	for i, a := range f.auths {
		if a != "" {
			t.Errorf("search %d sent %q with no pass to send", i, a)
		}
	}
}

func TestWithNoAccountNothingIsSentForOne(t *testing.T) {
	f := newFakeOpenverse(t)
	t.Setenv("OPENVERSE_CLIENT_ID", "")
	t.Setenv("OPENVERSE_CLIENT_SECRET", "")
	if _, _, err := searchFreeMusic(t.Context(), "x", 1); err != nil {
		t.Fatal(err)
	}
	if f.logins != 0 || f.auths[0] != "" {
		t.Errorf("logins %d, auth %q", f.logins, f.auths[0])
	}
}

func TestAPickedSongIsReadFreshAndMustStillBeFree(t *testing.T) {
	f := newFakeOpenverse(t)
	f.songs["ok"] = freeSong("ok", "Fine", "cc0")
	f.songs["nc"] = freeSong("nc", "Not for apps", "by-nc")
	tr, err := openverseTrack(t.Context(), "ok")
	if err != nil || tr.Title != "Fine" || tr.License != "cc0" {
		t.Errorf("a free song: %+v, %v", tr, err)
	}
	if _, err := openverseTrack(t.Context(), "nc"); err == nil {
		t.Error("a non-commercial song was accepted")
	}
	if f.details[0] != "ok" {
		t.Errorf("asked for %v", f.details)
	}
}

func TestMusicIsRoutedAndOnlyAnAdminCanTakeASongDown(t *testing.T) {
	// The tests above use their own router. These are the real routes —
	// read with the comment lines left out, so a comment naming a route is
	// not mistaken for the route.
	var code []string
	for _, l := range strings.Split(readSourceFile(t, "main.go"), "\n") {
		if !strings.HasPrefix(strings.TrimSpace(l), "//") {
			code = append(code, l)
		}
	}
	routes := map[string]string{}
	for _, l := range code {
		for _, path := range []string{`"/music/search"`, `"/music/tracks"`, `"/admin/music/block"`} {
			if strings.Contains(l, path) {
				routes[path] = l
			}
		}
	}
	for path, want := range map[string]string{
		`"/music/search"`:      "authed(MusicSearchHandler)",
		`"/music/tracks"`:      "authed(PickMusicHandler)",
		`"/admin/music/block"`: "adminOnly(AdminBlockMusicHandler)",
	} {
		if !strings.Contains(routes[path], want) {
			t.Errorf("%s is routed as %q, want %s", path, strings.TrimSpace(routes[path]), want)
		}
	}

	t.Setenv("ADMIN_USER", "admin")
	t.Setenv("ADMIN_PASS", "letmein")
	rec := httptest.NewRecorder()
	adminOnly(AdminBlockMusicHandler)(rec, httptest.NewRequest(http.MethodPost,
		"/admin/music/block", strings.NewReader(`{"sourceId":"x"}`)))
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("taking a song down with no password: %d", rec.Code)
	}
}
