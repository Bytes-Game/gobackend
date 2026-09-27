package main

// What led the last refresh is remembered per feed, and for a day.
//
// A pull-to-refresh pushes down whatever led the previous refresh, so the
// first video actually changes. Two things stopped that from working:
//
//   - The memory lasted ten minutes. The app now refreshes every time it is
//     opened, and people open it again hours later. By then the memory was
//     gone and the same first video came back.
//   - Every feed shared one memory per person. Refreshing Search overwrote
//     what had led the home feed, so the next home refresh demoted Search's
//     videos instead of its own.
//
// These go through the real handlers, so a handler that stops saving, or
// saves under the wrong feed's name, turns something here red.

import (
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"testing"
	"time"
)

func seedRefreshCatalog(t *testing.T, userID int) {
	t.Helper()
	db := openDB(t)
	if _, err := db.Exec(`
		INSERT INTO users (id, username, password)
		VALUES ($1, $2, 'x'), ($3, $4, 'x')
		ON CONFLICT (id) DO NOTHING`,
		userID, fmt.Sprintf("viewer%d", userID),
		userID+1, fmt.Sprintf("maker%d", userID+1)); err != nil {
		t.Fatalf("seed users: %v", err)
	}
	for i := 0; i < 8; i++ {
		if _, err := db.Exec(`
			INSERT INTO challenges (creator_id, video_url, prefix, subject)
			VALUES ($1, $2, 'who can', $3)`,
			userID+1, fmt.Sprintf("https://v/r%d-%d.mp4", userID, i),
			fmt.Sprintf("thing %d", i)); err != nil {
			t.Fatalf("seed challenge: %v", err)
		}
	}
}

// warmUp gives the viewer enough history to be ranked as a regular user
// rather than a brand-new one — the two go down different paths.
func warmUp(t *testing.T, userID int) {
	t.Helper()
	db := openDB(t)
	for i := 0; i < coldStartThreshold+1; i++ {
		if _, err := db.Exec(`
			INSERT INTO feed_events (user_id, content_id, content_type, event_type, created_at)
			VALUES ($1, 'x', 'challenge', 'view', NOW())`, userID); err != nil {
			t.Fatalf("seed history: %v", err)
		}
	}
}

// refreshOnce pulls page 1 as a refresh and says which videos came back,
// in order.
func refreshOnce(t *testing.T, path string, userID int) []string {
	t.Helper()
	return refreshPage(t, path, userID, 5)
}

func refreshPage(t *testing.T, path string, userID, limit int) []string {
	t.Helper()
	req := withAuth(
		httptest.NewRequest("GET",
			fmt.Sprintf("%s?page=1&limit=%d&refresh=true", path, limit), nil),
		fmt.Sprint(userID), "viewer",
	)
	rec := httptest.NewRecorder()
	switch path {
	case "/api/v1/feed/explore":
		ExploreFeedHandler(rec, req)
	default:
		SmartFeedHandler(rec, req)
	}
	if rec.Code != 200 {
		t.Fatalf("%s answered %d: %s", path, rec.Code, rec.Body.String())
	}
	var body struct {
		Items []struct {
			Type      string `json:"type"`
			Challenge *struct {
				ID string `json:"id"`
			} `json:"challenge"`
		} `json:"items"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("%s: unreadable answer: %v", path, err)
	}
	var ids []string
	for _, it := range body.Items {
		if it.Challenge != nil {
			ids = append(ids, it.Challenge.ID)
		}
	}
	if len(ids) < 2 {
		t.Fatalf("%s: only %d videos came back, too few to tell anything",
			path, len(ids))
	}
	return ids
}

// The head is written in the background; give it a moment to land.
func waitForHead(t *testing.T, userID int, surface string) map[string]int {
	t.Helper()
	uid := fmt.Sprint(userID)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if got := loadPrevRefreshTops(uid, surface); len(got) > 0 {
			return got
		}
		time.Sleep(20 * time.Millisecond)
	}
	return loadPrevRefreshTops(uid, surface)
}

func TestRefreshMemory_EachFeedKeepsItsOwn(t *testing.T) {
	defer withDB(t)()
	resetRedis(t)
	const viewer = 8801
	seedRefreshCatalog(t, viewer)
	warmUp(t, viewer)

	refreshOnce(t, "/api/v1/feed", viewer)
	home := waitForHead(t, viewer, refreshSurfaceFeed(feedKindAll))
	if len(home) == 0 {
		t.Fatal("a home refresh remembered nothing, so the next one cannot " +
			"move past its first video")
	}

	refreshOnce(t, "/api/v1/feed/explore", viewer)
	if got := waitForHead(t, viewer, refreshSurfaceExplore); len(got) == 0 {
		t.Fatal("an explore refresh remembered nothing")
	}

	// The point: Search's refresh did not overwrite the home feed's.
	after := loadPrevRefreshTops(fmt.Sprint(viewer), refreshSurfaceFeed(feedKindAll))
	if len(after) != len(home) {
		t.Fatalf("home's memory changed after an explore refresh: %v -> %v",
			home, after)
	}
	for k, rank := range home {
		if after[k] != rank {
			t.Fatalf("home's memory changed after an explore refresh: "+
				"%v -> %v", home, after)
		}
	}
}

func TestRefreshMemory_OutlastsTheGapBetweenOpeningTheApp(t *testing.T) {
	resetRedis(t)
	items := []HomeFeedItem{
		{Type: "challenge", Challenge: &Challenge{ID: "11"}},
		{Type: "challenge", Challenge: &Challenge{ID: "12"}},
	}
	savePrevRefreshTops("8900", refreshSurfaceFeed(feedKindAll), items)

	// Opened again this evening: still remembered.
	mr.FastForward(6 * time.Hour)
	got := loadPrevRefreshTops("8900", refreshSurfaceFeed(feedKindAll))
	if got["challenge:11"] != 1 || got["challenge:12"] != 2 {
		t.Fatalf("six hours later the last first video was forgotten: %v", got)
	}

	// Not for ever: after a day it may lead again.
	mr.FastForward(19 * time.Hour)
	if got := loadPrevRefreshTops("8900", refreshSurfaceFeed(feedKindAll)); len(got) != 0 {
		t.Fatalf("still remembered after more than a day: %v", got)
	}
}

func TestRefreshMemory_ShortsAndBattlesTabsAreSeparateFeeds(t *testing.T) {
	names := map[string]bool{
		refreshSurfaceFeed(feedKindAll):     true,
		refreshSurfaceFeed(feedKindShorts):  true,
		refreshSurfaceFeed(feedKindBattles): true,
		refreshSurfaceExplore:               true,
	}
	if len(names) != 4 {
		t.Fatalf("two feeds share one memory: %v", names)
	}
}

// A brand-new account goes down a separate path with no scores to nudge, so
// a refresh there used to change nothing: page 1 is the same top videos
// every time. With the app refreshing on every open, that was the same first
// video every open.
func TestRefresh_ANewAccountGetsADifferentFirstVideo(t *testing.T) {
	defer withDB(t)()
	resetRedis(t)
	const viewer = 8811
	seedRefreshCatalog(t, viewer)

	// A page bigger than the catalog, so the first refresh shows every
	// video and the second has nothing unseen to lead with. That is the
	// case the person in the device log was in: everything already
	// watched. Only the refresh can change the first video then.
	first := refreshPage(t, "/api/v1/feed", viewer, 20)
	waitForHead(t, viewer, refreshSurfaceFeed(feedKindAll))
	second := refreshPage(t, "/api/v1/feed", viewer, 20)
	if first[0] == second[0] {
		t.Fatalf("two refreshes opened on the same video %s:\n  %v\n  %v",
			first[0], first, second)
	}
}

func TestDemotePreviousHead_KeepsTheRestInOrder(t *testing.T) {
	item := func(id string) HomeFeedItem {
		return HomeFeedItem{Type: "challenge", Challenge: &Challenge{ID: id}}
	}
	got := demotePreviousHead(
		[]HomeFeedItem{item("1"), item("2"), item("3"), item("4")},
		map[string]int{"challenge:1": 1, "challenge:3": 2},
	)
	var ids []string
	for _, it := range got {
		ids = append(ids, it.Challenge.ID)
	}
	if fmt.Sprint(ids) != "[2 4 1 3]" {
		t.Fatalf("got %v, want [2 4 1 3]", ids)
	}
}
