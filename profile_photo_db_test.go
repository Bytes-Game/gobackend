package main

// Profile photos and tags, the lists of who liked and who voted, and "What
// is this video about?" — through the real handlers against real Postgres.

import (
	"bytes"
	"encoding/json"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/gorilla/mux"
)

// withPhotoStorage makes our storage answer at a known address for the
// length of a test.
func withPhotoStorage(t *testing.T) string {
	t.Helper()
	const base = "https://media.example"
	before := profilePhotoStorage
	profilePhotoStorage = func() (*R2Config, error) {
		return &R2Config{PublicBaseURL: base}, nil
	}
	t.Cleanup(func() { profilePhotoStorage = before })
	return base
}

func TestProfilePhoto_OnlyYourOwnUploadIsAccepted(t *testing.T) {
	base := withPhotoStorage(t)
	for url, ok := range map[string]bool{
		"":                                           true,
		base + "/u/42/up_ABC-1/photo.jpg":            true,
		base + "/u/43/up_ABC-1/photo.jpg":            false, // somebody else's
		base + "/u/42/up_ABC-1/default.jpg":          false, // not a photo upload
		base + "/u/42/a/b/photo.jpg":                 false, // extra folders
		base + "/u/42/up_ABC-1/photo.jpg?x=1":        false, // a query string
		"https://elsewhere.example/u/42/a/photo.jpg": false,
		base + "/u/42/../43/x/photo.jpg":             false,
	} {
		if got := validProfilePhoto("42", url); got != ok {
			t.Errorf("validProfilePhoto(42, %q) = %v, want %v", url, got, ok)
		}
	}
}

func TestProfileTag_OnlyFromTheList(t *testing.T) {
	for tag, ok := range map[string]bool{
		"": true, "Influencer": true, "Philanthropist": true, "Motivator": true,
		"influencer": false, "Billionaire": false, " Creator": false,
	} {
		if got := validProfileTag(tag); got != ok {
			t.Errorf("validProfileTag(%q) = %v, want %v", tag, got, ok)
		}
	}
}

func TestRoutes_UsersFixedNamesAreReachable(t *testing.T) {
	// The router takes the first route that matches, and /users/{username}
	// matches any word. A fixed name registered after it is never reached:
	// GET /users/suggested looked up an account called "suggested". Read
	// with comments stripped, so a comment cannot stand in for the route.
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
	src := code.String()
	catchAll := strings.Index(src, `"/users/{username}"`)
	if catchAll < 0 {
		t.Fatal("no /users/{username} route found")
	}
	for _, fixed := range []string{"avatars", "similar", "suggested"} {
		at := strings.Index(src, `"/users/`+fixed+`"`)
		if at < 0 {
			t.Errorf("/users/%s is not registered", fixed)
		} else if at > catchAll {
			t.Errorf("/users/%s comes after /users/{username}, so it is never reached", fixed)
		}
	}
}

func patchProfile(t *testing.T, user int, body map[string]any) int {
	t.Helper()
	raw, _ := json.Marshal(body)
	req := withAuth(httptest.NewRequest("PATCH", "/api/v1/users/"+strconv.Itoa(user),
		bytes.NewReader(raw)), strconv.Itoa(user), nfName(user))
	req = mux.SetURLVars(req, map[string]string{"id": strconv.Itoa(user)})
	rec := httptest.NewRecorder()
	UpdateUserProfileHandler(rec, req)
	return rec.Code
}

func TestProfilePhoto_SavedAndReadBack(t *testing.T) {
	defer withDB(t)()
	resetRedis(t)
	resetActionLimiters(t)
	seedNotifyPeople(t)
	base := withPhotoStorage(t)
	defer db.Exec(`UPDATE users SET avatar_url = '', profile_tag = '' WHERE id = $1`, nfCreator)
	photo := base + "/u/" + strconv.Itoa(nfCreator) + "/up1/photo.jpg"

	if code := patchProfile(t, nfCreator, map[string]any{
		"avatarUrl": photo, "profileTag": "Motivator"}); code != 200 {
		t.Fatalf("saving a photo and tag answered %d", code)
	}
	u, ok := GetUserByID(strconv.Itoa(nfCreator))
	if !ok || u.AvatarURL != photo || u.ProfileTag != "Motivator" {
		t.Fatalf("read back %q %q", u.AvatarURL, u.ProfileTag)
	}
	if byName, _ := GetUserByUsername(nfName(nfCreator)); byName.AvatarURL != photo ||
		byName.ProfileTag != "Motivator" {
		t.Fatalf("by name read back %q %q", byName.AvatarURL, byName.ProfileTag)
	}

	// Refused, and nothing changed.
	other := base + "/u/" + strconv.Itoa(nfStranger) + "/up1/photo.jpg"
	if code := patchProfile(t, nfCreator, map[string]any{"avatarUrl": other}); code != 400 {
		t.Errorf("somebody else's upload answered %d", code)
	}
	if code := patchProfile(t, nfCreator, map[string]any{"profileTag": "Billionaire"}); code != 400 {
		t.Errorf("a tag not on the list answered %d", code)
	}
	if u, _ := GetUserByID(strconv.Itoa(nfCreator)); u.AvatarURL != photo || u.ProfileTag != "Motivator" {
		t.Fatalf("a refused change still changed something: %q %q", u.AvatarURL, u.ProfileTag)
	}

	// Removed. (A refused change still counts towards the edit limit of
	// three in a burst, as it should, so the limit is reset first.)
	resetActionLimiters(t)
	if code := patchProfile(t, nfCreator, map[string]any{"avatarUrl": "", "profileTag": ""}); code != 200 {
		t.Fatalf("removing answered %d", code)
	}
	if u, _ := GetUserByID(strconv.Itoa(nfCreator)); u.AvatarURL != "" || u.ProfileTag != "" {
		t.Fatalf("not removed: %q %q", u.AvatarURL, u.ProfileTag)
	}
}

func TestAvatars_ManyPeopleInOneQuestion(t *testing.T) {
	defer withDB(t)()
	seedNotifyPeople(t)
	if _, err := db.Exec(`UPDATE users SET avatar_url = 'https://media.example/u/9402/a/photo.jpg'
	                       WHERE id = $1`, nfFriendA); err != nil {
		t.Fatal(err)
	}
	defer db.Exec(`UPDATE users SET avatar_url = '' WHERE id = $1`, nfFriendA)

	names := nfName(nfFriendA) + "," + nfName(nfFriendB) + ",nobody_has_this_name," + nfName(nfFriendA)
	req := withAuth(httptest.NewRequest("GET", "/api/v1/users/avatars?names="+names, nil),
		strconv.Itoa(nfCreator), nfName(nfCreator))
	rec := httptest.NewRecorder()
	UserAvatarsHandler(rec, req)
	var body struct {
		Avatars map[string]string `json:"avatars"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil || rec.Code != 200 {
		t.Fatalf("answered %d %s", rec.Code, rec.Body.String())
	}
	if body.Avatars[nfName(nfFriendA)] != "https://media.example/u/9402/a/photo.jpg" {
		t.Errorf("the person with a photo: %+v", body.Avatars)
	}
	if url, known := body.Avatars[nfName(nfFriendB)]; !known || url != "" {
		t.Errorf("the person with none should be there, empty: %+v", body.Avatars)
	}
	if _, known := body.Avatars["nobody_has_this_name"]; known {
		t.Errorf("a name with no account was answered: %+v", body.Avatars)
	}
}

func TestLists_FriendsOnlyStayClosedAndShowFaces(t *testing.T) {
	defer withDB(t)()
	resetRedis(t)
	resetActionLimiters(t)
	seedNotifyPeople(t)
	cid := postChallenge(t, "friends", nil)
	if _, err := db.Exec(`INSERT INTO challenge_likes (challenge_id, user_id) VALUES ($1, $2)`,
		cid, nfFriendA); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE users SET avatar_url = 'https://media.example/u/9402/a/photo.jpg'
	                       WHERE id = $1`, nfFriendA); err != nil {
		t.Fatal(err)
	}
	defer db.Exec(`UPDATE users SET avatar_url = '' WHERE id = $1`, nfFriendA)

	likers := func(viewer int) (int, []PersonAt) {
		req := withAuth(httptest.NewRequest("GET", "/api/v1/challenges/"+cid+"/likers", nil),
			strconv.Itoa(viewer), nfName(viewer))
		req = mux.SetURLVars(req, map[string]string{"id": cid})
		rec := httptest.NewRecorder()
		ChallengeLikersHandler(rec, req)
		var body struct {
			Likers []PersonAt `json:"likers"`
		}
		_ = json.Unmarshal(rec.Body.Bytes(), &body)
		return rec.Code, body.Likers
	}
	code, people := likers(nfFriendB) // a follower: may watch it
	if code != 200 || len(people) != 1 ||
		people[0].AvatarURL != "https://media.example/u/9402/a/photo.jpg" {
		t.Fatalf("a friend sees %d %+v", code, people)
	}
	if code, _ := likers(nfStranger); code != 404 {
		t.Errorf("a stranger asking about a friends-only video got %d, want 404", code)
	}

	voters := func(viewer int) int {
		req := withAuth(httptest.NewRequest("GET", "/api/v1/challenges/"+cid+"/voters", nil),
			strconv.Itoa(viewer), nfName(viewer))
		req = mux.SetURLVars(req, map[string]string{"id": cid})
		rec := httptest.NewRecorder()
		ChallengeVotersHandler(rec, req)
		return rec.Code
	}
	if code := voters(nfStranger); code != 404 {
		t.Errorf("a stranger asking who voted on a friends-only video got %d, want 404", code)
	}
	if code := voters(nfFriendB); code != 200 {
		t.Errorf("a friend asking who voted got %d", code)
	}
}

func askAbout(t *testing.T, viewer int, cid, rid string) (int, VideoAbout) {
	t.Helper()
	url := "/api/v1/challenges/" + cid + "/about"
	if rid != "" {
		url += "?response=" + rid
	}
	req := withAuth(httptest.NewRequest("GET", url, nil), strconv.Itoa(viewer), nfName(viewer))
	req = mux.SetURLVars(req, map[string]string{"id": cid})
	rec := httptest.NewRecorder()
	VideoAboutHandler(rec, req)
	var out VideoAbout
	_ = json.Unmarshal(rec.Body.Bytes(), &out)
	return rec.Code, out
}

func TestAbout_WhatTheWorkerWroteReachesTheViewer(t *testing.T) {
	defer withDB(t)()
	resetRedis(t)
	resetActionLimiters(t)
	seedNotifyPeople(t)
	cid := postChallenge(t, "arena", nil)

	// Moments after upload: nothing has looked at it yet.
	code, got := askAbout(t, nfStranger, cid, "")
	if code != 200 || got.Looked || got.About != "" {
		t.Fatalf("before the worker: %d %+v", code, got)
	}

	// The worker's reading, stored the way storeVideoAnalysis stores it.
	reading := `{"passes":["shape","speech","understand"],"speech":"private words",` +
		`"about":"A man makes chai on a street stall.","aboutFrom":"said"}`
	if _, err := db.Exec(`UPDATE challenges SET video_analysis = $2, content_topics = $3
	                       WHERE id = $1`, cid, reading, `["chai","street food"]`); err != nil {
		t.Fatal(err)
	}
	code, got = askAbout(t, nfStranger, cid, "")
	if code != 200 || !got.Looked || got.About != "A man makes chai on a street stall." ||
		got.From != "said" || len(got.Topics) != 2 {
		t.Fatalf("after the worker: %d %+v", code, got)
	}

	// The transcript itself is never handed over.
	req := withAuth(httptest.NewRequest("GET", "/api/v1/challenges/"+cid+"/about", nil),
		strconv.Itoa(nfStranger), nfName(nfStranger))
	req = mux.SetURLVars(req, map[string]string{"id": cid})
	rec := httptest.NewRecorder()
	VideoAboutHandler(rec, req)
	if strings.Contains(rec.Body.String(), "private words") {
		t.Fatalf("the transcript leaked: %s", rec.Body.String())
	}

	// An answer in the battle has its own.
	rid := answer(t, cid)
	if _, err := db.Exec(`UPDATE challenge_responses SET video_analysis = $2 WHERE id = $1`,
		rid, `{"passes":["shape","frames"],"about":"Someone juggles five balls.","aboutFrom":"shown"}`); err != nil {
		t.Fatal(err)
	}
	code, got = askAbout(t, nfStranger, cid, rid)
	if code != 200 || got.About != "Someone juggles five balls." || got.From != "shown" {
		t.Fatalf("the answer's: %d %+v", code, got)
	}
}

func TestAbout_FriendsOnlyStaysClosed(t *testing.T) {
	defer withDB(t)()
	resetRedis(t)
	resetActionLimiters(t)
	seedNotifyPeople(t)
	cid := postChallenge(t, "friends", nil)
	if code, _ := askAbout(t, nfStranger, cid, ""); code != 404 {
		t.Errorf("a stranger asking about a friends-only video got %d, want 404", code)
	}
	if code, _ := askAbout(t, nfFriendA, cid, ""); code != 200 {
		t.Errorf("a friend asking got %d", code)
	}
}
