package main

// explore_grid.go — the Search page's grid of videos, scrolled past its
// first screen.
//
// ════════════════════════════════════════════════════════════════════════════
// WHY THE GRID COULD NOT SCROLL ON
// ════════════════════════════════════════════════════════════════════════════
//
// The grid asks for its videos without marking them as watched (a grid of
// pictures is not thirty videos watched). But "page 2" of the explore feed
// is only different from page 1 BECAUSE page 1 was marked as watched — the
// feed walks the catalogue by what you saw least recently. With nothing
// marked, page 2 was page 1 again. So the grid asked once, got thirty, and
// that was the whole grid.
//
// And the feed's recency lane only looks back so far (a fortnight, widening
// to a year), so even a grid that could page would never reach older videos.
//
// So the grid's next batch says what it already has, and gets the best of
// the rest:
//
//   - The app sends the ids of the videos it holds (exclude).
//   - The recency lane becomes "the newest findable videos not in that
//     list, from any time" — each batch reaches further back.
//   - Trending still leads, minus what the grid holds.
//   - When nothing is left, the answer is empty and the grid stops. That is
//     the end of the database, not a guess.

import (
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/lib/pq"
)

// exploreGridExcludeMax bounds how many ids one request may name. Far more
// than anybody scrolls in one go; it stops one request from naming a
// million.
const exploreGridExcludeMax = 3000

// exploreGridExclude reads the videos the grid already holds, as
// "challenge:<id>" keys. From a JSON body {"exclude": [ids]} on a POST — the
// app's way, because a list that long does not belong in an address — or a
// comma list in ?exclude= for a short one. Empty for every first page.
func exploreGridExclude(r *http.Request) map[string]bool {
	var ids []string
	if raw := r.URL.Query().Get("exclude"); raw != "" {
		ids = strings.Split(raw, ",")
	}
	if r.Method == http.MethodPost && r.Body != nil {
		var body struct {
			Exclude []json.Number `json:"exclude"`
		}
		dec := json.NewDecoder(io.LimitReader(r.Body, 64<<10))
		dec.UseNumber()
		if err := dec.Decode(&body); err == nil {
			for _, n := range body.Exclude {
				ids = append(ids, n.String())
			}
		}
	}
	out := make(map[string]bool, len(ids))
	for _, id := range ids {
		id = strings.TrimSpace(id)
		if _, err := strconv.Atoi(id); err != nil {
			continue
		}
		out["challenge:"+id] = true
		if len(out) >= exploreGridExcludeMax {
			break
		}
	}
	return out
}

// fetchGridCandidates is the newest findable videos the grid does not hold
// yet, from any time, never the viewer's own.
func fetchGridCandidates(userID string, limit int, exclude map[string]bool) []HomeFeedItem {
	if db == nil || limit <= 0 {
		return nil
	}
	held := make([]int, 0, len(exclude))
	for key := range exclude {
		if n, err := strconv.Atoi(strings.TrimPrefix(key, "challenge:")); err == nil {
			held = append(held, n)
		}
	}
	rows, err := db.Query(`
		SELECT c.id, c.creator_id, u.username, u.league, c.video_url,
			c.thumbnail_url, c.prefix, c.subject, c.visibility, c.status,
			c.views,
			(SELECT COUNT(*) FROM challenge_likes l WHERE l.challenge_id = c.id),
			c.created_at,
			(SELECT COUNT(*) FROM challenge_responses r WHERE r.challenge_id = c.id)
		  FROM challenges c
		  JOIN users u ON c.creator_id = u.id
		 WHERE `+searchableWhere("c")+`
		   AND c.creator_id <> CAST($1 AS INT)
		   AND NOT (c.id = ANY($3::int[]))
		 ORDER BY c.created_at DESC
		 LIMIT $2`, userID, limit, pq.Array(held))
	if queryFailed("finding more videos for the search grid",
		"ending the grid where it is", err) {
		return nil
	}
	defer rows.Close()
	var items []HomeFeedItem
	bad := 0
	for rows.Next() {
		var ch Challenge
		var creatorID, views, likes, responses int
		var createdAt time.Time
		if scanFailed("a video for the search grid", rows.Scan(&ch.ID, &creatorID,
			&ch.CreatorUsername, &ch.CreatorLeague, &ch.VideoURL, &ch.ThumbnailURL,
			&ch.Prefix, &ch.Subject, &ch.Visibility, &ch.Status, &views, &likes,
			&createdAt, &responses), &bad) {
			continue
		}
		ch.CreatorID = strconv.Itoa(creatorID)
		ch.Views = views
		ch.Likes = likes
		ch.CreatedAt = createdAt.Format(time.RFC3339)
		ch.ResponseCount = responses
		items = append(items, HomeFeedItem{Type: "challenge", Challenge: &ch})
	}
	return items
}

// dropUnservable takes out what must not be served to this person at all:
// anything by somebody they blocked, and anything the grid already holds.
//
// Blocking used to be a score of zero, which sorts last but still serves —
// harmless while the grid stopped at thirty, and the first thing a grid that
// scrolls to the end would show.
func dropUnservable(items []ScoredItem, ns *negativeSignals, exclude map[string]bool) []ScoredItem {
	out := make([]ScoredItem, 0, len(items))
	for _, si := range items {
		if negativeCreatorPenalty(ns, defaultCreatorOf(si)) == 0 {
			continue
		}
		if id := getItemID(si.Item); id != "" && exclude[si.Item.Type+":"+id] {
			continue
		}
		out = append(out, si)
	}
	return out
}
