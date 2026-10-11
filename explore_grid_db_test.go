package main

// The Search grid, scrolled to the end — through the real handler against
// real Postgres. See explore_grid.go.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"strconv"
	"testing"
)

const (
	gridViewer  = 8101
	gridCreator = 8102
	gridBlocked = 8103
)

func seedGridPeople(t *testing.T) {
	t.Helper()
	for _, id := range []int{gridViewer, gridCreator, gridBlocked} {
		if _, err := db.Exec(`
			INSERT INTO users (id, username, password) VALUES ($1, $2, 'x')
			ON CONFLICT (id) DO NOTHING`, id, fmt.Sprintf("grid%d", id)); err != nil {
			t.Fatalf("seed user %d: %v", id, err)
		}
	}
}

// gridVideo posts one video, [daysOld] days ago, and returns its id.
func gridVideo(t *testing.T, creator, daysOld int, visibility, status string) string {
	t.Helper()
	var id int
	if err := db.QueryRow(`
		INSERT INTO challenges (creator_id, video_url, thumbnail_url, prefix,
		                        subject, visibility, status, created_at)
		VALUES ($1, 'https://v/clip.mp4', 'https://v/clip.jpg', 'who is better at',
		        'grid test', $2, $3, NOW() - ($4 || ' days')::interval)
		RETURNING id`, creator, visibility, status, strconv.Itoa(daysOld)).Scan(&id); err != nil {
		t.Fatalf("seed video: %v", err)
	}
	return strconv.Itoa(id)
}

// gridBatch asks for one batch the way the app does: the first with a GET,
// every later one with a POST naming what the grid already holds.
func gridBatch(t *testing.T, held []string) (ids []string, hasMore bool) {
	t.Helper()
	path := "/api/v1/feed/explore?page=1&limit=30&markShown=false"
	method, body := "GET", []byte(nil)
	if len(held) > 0 {
		nums := make([]int, 0, len(held))
		for _, h := range held {
			n, _ := strconv.Atoi(h)
			nums = append(nums, n)
		}
		method = "POST"
		body, _ = json.Marshal(map[string]any{"exclude": nums})
		path = fmt.Sprintf("/api/v1/feed/explore?page=%d&limit=30&markShown=false",
			len(held)/30+1)
	}
	r := withAuth(httptest.NewRequest(method, path, bytes.NewReader(body)),
		strconv.Itoa(gridViewer), "grid")
	w := httptest.NewRecorder()
	ExploreFeedHandler(w, r)
	if w.Code != 200 {
		t.Fatalf("%s %s answered %d: %s", method, path, w.Code, w.Body.String())
	}
	var out struct {
		Items []struct {
			Challenge *struct {
				ID        string `json:"id"`
				CreatorID string `json:"creatorId"`
			} `json:"challenge"`
		} `json:"items"`
		HasMore bool `json:"hasMore"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatalf("unreadable answer: %v", err)
	}
	for _, it := range out.Items {
		if it.Challenge != nil {
			ids = append(ids, it.Challenge.ID)
		}
	}
	return ids, out.HasMore
}

// scrollToTheEnd keeps asking until an answer comes back empty, the way the
// grid does, and returns everything it was given in order.
func scrollToTheEnd(t *testing.T) []string {
	t.Helper()
	var held []string
	for round := 0; round < 20; round++ {
		got, _ := gridBatch(t, held)
		if len(got) == 0 {
			return held
		}
		held = append(held, got...)
	}
	t.Fatalf("still answering after 20 batches (%d videos) — the grid would "+
		"never reach an end", len(held))
	return nil
}

func TestExploreGrid_ScrollingReachesEveryVideoOnceThenStops(t *testing.T) {
	defer withDB(t)()
	resetRedis(t)
	seedGridPeople(t)

	public := map[string]bool{}
	// 75 videos: more than two batches, and some older than the year the
	// feed's own recency lane ever looks back.
	for i := 0; i < 75; i++ {
		public[gridVideo(t, gridCreator, i*7, "arena", "open")] = true
	}
	private := gridVideo(t, gridCreator, 1, "friends", "open")
	removed := gridVideo(t, gridCreator, 1, "arena", "removed")
	mine := gridVideo(t, gridViewer, 1, "arena", "open")
	// Some of them trending too. Trending is asked again for every batch,
	// so it is where a video the grid already holds would come back from.
	n := 0
	for id := range public {
		if err := rdb.ZIncrBy(rctx, trendingRealtimeKey, float64(10+n),
			"challenge:"+id).Err(); err != nil {
			t.Fatal(err)
		}
		if n++; n == 8 {
			break
		}
	}

	first, hasMore := gridBatch(t, nil)
	if len(first) != 30 || !hasMore {
		t.Fatalf("first batch: %d videos, hasMore=%v — want a full 30 and "+
			"more to come", len(first), hasMore)
	}

	all := scrollToTheEnd(t)
	seen := map[string]int{}
	for _, id := range all {
		seen[id]++
	}
	for id, n := range seen {
		if n > 1 {
			t.Errorf("video %s came %d times", id, n)
		}
	}
	for id := range public {
		if seen[id] == 0 {
			t.Errorf("video %s was never reached", id)
		}
	}
	for name, id := range map[string]string{
		"a friends-only video": private, "a removed video": removed,
		"the viewer's own video": mine,
	} {
		if seen[id] > 0 {
			t.Errorf("%s (%s) was in the grid", name, id)
		}
	}
	if len(all) != len(public) {
		t.Errorf("scrolled through %d videos; there are %d", len(all), len(public))
	}
}

func TestExploreGrid_ABlockedPersonsVideosNeverShow(t *testing.T) {
	defer withDB(t)()
	resetRedis(t)
	seedGridPeople(t)

	theirs := map[string]bool{}
	for i := 0; i < 5; i++ {
		theirs[gridVideo(t, gridBlocked, i, "arena", "open")] = true
	}
	others := 0
	for i := 0; i < 40; i++ {
		gridVideo(t, gridCreator, i, "arena", "open")
		others++
	}
	if err := rdb.SAdd(rctx, "blocked_creators:"+strconv.Itoa(gridViewer),
		strconv.Itoa(gridBlocked)).Err(); err != nil {
		t.Fatal(err)
	}

	all := scrollToTheEnd(t)
	for _, id := range all {
		if theirs[id] {
			t.Errorf("video %s by somebody the viewer blocked was in the grid", id)
		}
	}
	if len(all) != others {
		t.Errorf("scrolled through %d videos; want the %d by people not blocked",
			len(all), others)
	}
}

func TestTrending_OnlyWhatAnybodyMayFindCanTrend(t *testing.T) {
	defer withDB(t)()
	resetRedis(t)
	seedGridPeople(t)

	public := gridVideo(t, gridCreator, 1, "arena", "open")
	private := gridVideo(t, gridCreator, 1, "friends", "open")
	removed := gridVideo(t, gridCreator, 1, "arena", "removed")
	for _, id := range []string{public, private, removed} {
		if err := rdb.ZIncrBy(rctx, trendingRealtimeKey, 50, "challenge:"+id).Err(); err != nil {
			t.Fatal(err)
		}
	}

	got := map[string]bool{}
	for _, it := range sourceTrendingRealtime(strconv.Itoa(gridViewer), 10) {
		got[getItemID(it)] = true
	}
	if !got[public] {
		t.Fatalf("the public video is not trending at all, so nothing here " +
			"says anything")
	}
	if got[private] {
		t.Error("a friends-only video trended, onto a stranger's screen")
	}
	if got[removed] {
		t.Error("a removed video trended")
	}
}
