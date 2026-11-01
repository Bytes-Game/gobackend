package main

// The pages a shared link or a QR code opens — through the real handlers
// against real Postgres. See public_pages.go.

import (
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/gorilla/mux"
)

const (
	pubPerson  = 8201
	pubPrivate = 8202
)

func seedPublicPeople(t *testing.T) {
	t.Helper()
	for _, p := range []struct {
		id                  int
		name, vis, bio, url string
	}{
		{pubPerson, "maya_pub", "public", "Dancer <script>alert(1)</script>", "https://cdn/u/maya.jpg"},
		{pubPrivate, "quiet_pub", "friends", "", ""},
	} {
		if _, err := db.Exec(`
			INSERT INTO users (id, username, password, full_name, visibility, bio, avatar_url)
			VALUES ($1, $2, 'x', $3, $4, $5, $6)
			ON CONFLICT (id) DO NOTHING`, p.id, p.name, "Maya Singh", p.vis, p.bio, p.url); err != nil {
			t.Fatalf("seed %s: %v", p.name, err)
		}
	}
}

func publicPage(t *testing.T, h http.HandlerFunc, path string, vars map[string]string) (int, string) {
	t.Helper()
	r := mux.SetURLVars(httptest.NewRequest("GET", path, nil), vars)
	w := httptest.NewRecorder()
	h(w, r)
	return w.Code, w.Body.String()
}

func TestPublicPages_AProfileShowsWhoTheyAreAndTheirPublicVideos(t *testing.T) {
	defer withDB(t)()
	seedPublicPeople(t)
	public := gridVideo(t, pubPerson, 1, "arena", "open")
	private := gridVideo(t, pubPerson, 1, "friends", "open")
	removed := gridVideo(t, pubPerson, 1, "arena", "removed")
	if _, err := db.Exec(`INSERT INTO follows (follower_id, following_id) VALUES ($1, $2)`,
		pubPrivate, pubPerson); err != nil {
		t.Fatal(err)
	}

	code, body := publicPage(t, PublicProfilePageHandler, "/u/maya_pub",
		map[string]string{"username": "maya_pub"})
	if code != 200 {
		t.Fatalf("answered %d", code)
	}
	for _, want := range []string{
		"Maya Singh", "@maya_pub", `og:title`, `https://cdn/u/maya.jpg`,
		`href="/v/` + public + `"`,
		"<strong>1</strong><span>Followers</span>",
		"<strong>1</strong><span>Posts</span>",
		"(@maya_pub) on Flex</title>", "<b>f</b>Flex",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("the page does not have %q", want)
		}
	}
	if strings.Contains(strings.ToLower(body), "devf") {
		t.Error("the page still says the app's old name, devf")
	}
	for name, id := range map[string]string{"friends-only": private, "removed": removed} {
		if strings.Contains(body, `href="/v/`+id+`"`) {
			t.Errorf("a %s video is on the public page", name)
		}
	}
	if strings.Contains(body, "<script>alert(1)") {
		t.Error("the bio went onto the page as code, not words")
	}
}

func TestPublicPages_APrivateAccountShowsOnlyThatItIsPrivate(t *testing.T) {
	defer withDB(t)()
	seedPublicPeople(t)
	v := gridVideo(t, pubPrivate, 1, "arena", "open")
	code, body := publicPage(t, PublicProfilePageHandler, "/u/quiet_pub",
		map[string]string{"username": "quiet_pub"})
	if code != 200 || !strings.Contains(body, "This account is private") {
		t.Fatalf("answered %d without saying it is private", code)
	}
	if strings.Contains(body, `href="/v/`+v+`"`) {
		t.Error("a private account's video is on its public page")
	}
}

func TestPublicPages_AVideoPlaysOnlyWhenAnybodyMayWatchIt(t *testing.T) {
	defer withDB(t)()
	seedPublicPeople(t)
	public := gridVideo(t, pubPerson, 1, "arena", "open")
	if _, err := db.Exec(`UPDATE challenges SET video_variants = '{"480p":"https://v/480.mp4"}'::jsonb
		WHERE id = $1`, public); err != nil {
		t.Fatal(err)
	}
	code, body := publicPage(t, PublicVideoPageHandler, "/v/"+public,
		map[string]string{"id": public})
	if code != 200 {
		t.Fatalf("a public video answered %d", code)
	}
	for _, want := range []string{`<video`, `src="https://v/480.mp4"`,
		`poster="https://v/clip.jpg"`, "@maya_pub", "og:image"} {
		if !strings.Contains(body, want) {
			t.Errorf("the video page does not have %q", want)
		}
	}

	for name, vis := range map[string][2]string{
		"friends-only": {"friends", "open"}, "removed": {"arena", "removed"},
	} {
		id := gridVideo(t, pubPerson, 1, vis[0], vis[1])
		code, body := publicPage(t, PublicVideoPageHandler, "/v/"+id,
			map[string]string{"id": id})
		if code != 404 || strings.Contains(body, "<video") {
			t.Errorf("a %s video answered %d with a player on it", name, code)
		}
	}
	if code, _ := publicPage(t, PublicVideoPageHandler, "/v/999999",
		map[string]string{"id": "999999"}); code != 404 {
		t.Errorf("no such video answered %d", code)
	}
	if code, _ := publicPage(t, PublicProfilePageHandler, "/u/nobody_here_x",
		map[string]string{"username": "nobody_here_x"}); code != 404 {
		t.Errorf("no such person answered %d", code)
	}
}

func TestPublicPages_AreRouted(t *testing.T) {
	raw, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	var code strings.Builder
	for _, line := range strings.Split(string(raw), "\n") {
		if i := strings.Index(line, "//"); i >= 0 && !strings.Contains(line[:i], `"`) {
			line = line[:i]
		}
		code.WriteString(line + "\n")
	}
	for _, want := range []string{
		`r.HandleFunc("/u/{username}", PublicProfilePageHandler)`,
		`r.HandleFunc("/v/{id:[0-9]+}", PublicVideoPageHandler)`,
	} {
		if !strings.Contains(code.String(), want) {
			t.Errorf("main.go does not route %s", want)
		}
	}
	_ = strconv.Itoa
}
