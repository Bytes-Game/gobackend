package main

import (
	"context"
	"math"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// ─────────────────────────────────────────────────────────────────────────────
// REAL-TIME TRENDING
//
// The classic sourceTrending reads 48h SQL aggregates — it sees the world as
// of last analytics-job run, not as of right now. Viral content can blow up
// in 30 minutes; by the time SQL trending notices, the moment has passed.
//
// This module maintains a Redis ZSET keyed `trending:realtime` whose member
// is `{type}:{id}` and whose score is a cumulative engagement weight.
// Every event ingestion bumps the score by an event-type weight (positive
// for likes/shares/completes, negative for skips/reports). A background
// pruner runs every minute and scales ALL scores by a fixed decay factor
// (`exp(-ln2 / halfLife * pruneInterval)`); members that fall below a tiny
// floor get evicted so memory stays bounded.
//
// At read time we just `ZREVRANGE` and return the top-K — no math required,
// because the prune cycle has already shaped the score curve. This keeps
// the read path fast (single ZSET range) while still giving us proper
// exponential decay semantics.
//
// Half-life is intentionally short (20 min) so the score curve responds to
// minute-scale spikes. Items naturally fall off the head once they stop
// receiving fresh engagement.
// ─────────────────────────────────────────────────────────────────────────────

const (
	trendingRealtimeKey     = "trending:realtime"
	trendingHalfLifeMinutes = 20
	trendingPruneInterval   = 1 * time.Minute
	// Members whose score falls below this are dropped in the pruner.
	// Picked so a single "view" (weight 1) decays to noise after ~20
	// half-lives (≈ 7 hours), well past when the item is interesting.
	trendingFloorScore = 0.05
	trendingMaxReturn  = 200
)

// trendingDecayFactor is the per-prune-tick multiplier applied to every
// member's score. = 2^(-pruneInterval / halfLife). For 1-min prune and
// 20-min half-life: ≈ 0.9659. After 20 ticks (20 min) the score is halved,
// which is the definition of "half-life".
var trendingDecayFactor = math.Pow(2,
	-float64(trendingPruneInterval)/float64(time.Duration(trendingHalfLifeMinutes)*time.Minute))

// trendingEventWeight maps event types to a base reward magnitude.
// Picked so a single "share" outweighs a "view" (low-effort signal),
// while skips/blocks subtract score so spammy content gets pushed down.
func trendingEventWeight(eventType string, completionRate float64) float64 {
	switch eventType {
	case "share", "save":
		return 5.0
	case "like", "rewatch", "loop":
		return 3.0
	case "follow_from_content":
		return 4.0
	case "complete":
		return 2.5
	case "comment":
		return 2.0
	case "scroll_back", "unmute":
		return 1.5
	case "view":
		// A view is valuable only if completion was meaningful.
		if completionRate >= 0.7 {
			return 1.0
		}
		if completionRate > 0 && completionRate < 0.2 {
			return -0.5 // bounce — slight downweight
		}
		return 0
	case "skip", "not_interested":
		return -1.0
	case "report", "block":
		return -8.0 // strong demotion: surfaced spam shouldn't trend
	}
	return 0
}

// noteTrendingEvent applies a score bump for one engagement event. Cheap
// (one ZIncrBy), so it's fine on the request hot path. Decay is applied
// in pruneTrendingRealtime, not here — keeps writes simple and correct.
//
// Pass userID to weight the event by that user's engagement-quality
// multiplier (anti-spam, anti-bot). Pass "" to skip user weighting (back-
// compat for callers that don't carry the userID). The user multiplier
// is in [0.2, 2.0] so a single high-trust user counts ~10x more than a
// brand-new account hammering likes.
func noteTrendingEvent(contentType, contentID, eventType string, completionRate float64) {
	noteTrendingEventByUser("", contentType, contentID, eventType, completionRate)
}

// noteTrendingEventByUser is noteTrendingEvent with the user multiplier
// applied. Production callers should use this when they have the userID;
// back-compat callers fall through to the unweighted path with neutral 1.0.
func noteTrendingEventByUser(userID, contentType, contentID, eventType string, completionRate float64) {
	if rdb == nil || contentID == "" {
		return
	}
	w := trendingEventWeight(eventType, completionRate)
	if w == 0 {
		return
	}
	if userID != "" {
		w *= userEngagementQuality(userID)
	}
	member := contentType + ":" + contentID
	if err := rdb.ZIncrBy(rctx, trendingRealtimeKey, w, member).Err(); err == nil {
		_ = rdb.Expire(rctx, trendingRealtimeKey, 24*time.Hour).Err()
	}
}

// fetchTrendingRealtime returns the top-K (type, id) pairs by current trending
// score. Members whose score has decayed below trendingFloorScore are skipped
// (they'll be evicted by the next prune anyway, but read-time defense is cheap).
// Negative-score members (reported/heavily-skipped content) are also skipped.
func fetchTrendingRealtime(limit int) []trendingRealtimeEntry {
	if rdb == nil || limit <= 0 {
		return nil
	}
	if limit > trendingMaxReturn {
		limit = trendingMaxReturn
	}
	res, err := rdb.ZRevRangeWithScores(rctx, trendingRealtimeKey, 0, int64(limit-1)).Result()
	if err != nil || len(res) == 0 {
		return nil
	}
	out := make([]trendingRealtimeEntry, 0, len(res))
	for _, m := range res {
		if m.Score < trendingFloorScore {
			// Below floor or negative — not trending.
			continue
		}
		mem, ok := m.Member.(string)
		if !ok {
			continue
		}
		colon := strings.IndexByte(mem, ':')
		if colon < 1 || colon == len(mem)-1 {
			continue
		}
		out = append(out, trendingRealtimeEntry{
			Type:  mem[:colon],
			ID:    mem[colon+1:],
			Score: m.Score,
		})
	}
	return out
}

type trendingRealtimeEntry struct {
	Type  string
	ID    string
	Score float64
}

// sourceTrendingRealtime is a candidate source backed by the realtime ZSET.
// Fast path: read top-K from Redis, then enrich each ID via getContentScore /
// getContentEmotions to materialize a HomeFeedItem. Falls back to nothing
// (caller's other sources will fill the budget).
func sourceTrendingRealtime(userID string, limit int) []HomeFeedItem {
	entries := fetchTrendingRealtime(limit * 2) // overfetch; exclude self below
	if len(entries) == 0 {
		return nil
	}
	out := make([]HomeFeedItem, 0, limit)
	for _, e := range entries {
		if len(out) >= limit {
			break
		}
		item, ok := loadHomeFeedItemByID(e.Type, e.ID)
		if !ok {
			continue
		}
		// Skip self-content so users don't get fed their own posts as trending.
		if item.Post != nil && item.Post.AuthorID == userID {
			continue
		}
		if item.Challenge != nil && item.Challenge.CreatorID == userID {
			continue
		}
		out = append(out, item)
	}
	return out
}

// loadHomeFeedItemByID fetches one post or challenge by ID and wraps it in a
// HomeFeedItem. Returns ok=false on any DB error or missing row.
func loadHomeFeedItemByID(itemType, id string) (HomeFeedItem, bool) {
	if db == nil || id == "" {
		return HomeFeedItem{}, false
	}
	switch itemType {
	case "challenge":
		var ch Challenge
		var creatorID, views, likes int
		var createdAt time.Time
		err := db.QueryRow(`
			SELECT c.id, c.creator_id, u.username, u.league, c.video_url,
				c.thumbnail_url, c.prefix, c.subject, c.visibility, c.status,
				c.views, COALESCE(cl.likes, 0), c.created_at
			FROM challenges c
			JOIN users u ON c.creator_id = u.id
			LEFT JOIN (SELECT challenge_id, COUNT(*) AS likes FROM challenge_likes GROUP BY challenge_id) cl
				ON cl.challenge_id = c.id
			WHERE c.id = $1`, id).Scan(
			&ch.ID, &creatorID, &ch.CreatorUsername, &ch.CreatorLeague,
			&ch.VideoURL, &ch.ThumbnailURL, &ch.Prefix, &ch.Subject,
			&ch.Visibility, &ch.Status, &views, &likes, &createdAt)
		if err != nil {
			return HomeFeedItem{}, false
		}
		ch.CreatorID = itoa(creatorID)
		ch.Views = views
		ch.Likes = likes
		ch.CreatedAt = createdAt.Format(time.RFC3339)
		return HomeFeedItem{Type: "challenge", Challenge: &ch}, true

	case "post":
		var p Post
		var createdAt time.Time
		err := db.QueryRow(`
			SELECT p.id, p.user_id, u.username, u.league, p.type, p.content_url,
				COALESCE(p.thumbnail_url, ''), p.caption, p.likes, p.views,
				p.comments, p.created_at
			FROM posts p
			JOIN users u ON p.user_id = u.id
			WHERE p.id = $1`, id).Scan(
			&p.ID, &p.AuthorID, &p.AuthorUsername, &p.AuthorLeague,
			&p.Type, &p.ContentURL, &p.ThumbnailURL, &p.Caption,
			&p.Likes, &p.Views, &p.Comments, &createdAt)
		if err != nil {
			return HomeFeedItem{}, false
		}
		p.CreatedAt = createdAt.Format(time.RFC3339)
		return HomeFeedItem{Type: "post", Post: &p}, true
	}
	return HomeFeedItem{}, false
}

// startTrendingPruner runs the decay-and-evict pass every trendingPruneInterval.
// The pass scales every member's score by trendingDecayFactor and drops
// members whose score has fallen below trendingFloorScore. This is what
// gives the system its exponential-decay semantics — without it the score
// would be a pure cumulative sum.
func startTrendingPruner() {
	go func() {
		t := time.NewTicker(trendingPruneInterval)
		defer t.Stop()
		for range t.C {
			pruneTrendingRealtime(context.Background())
		}
	}()
}

// pruneTrendingRealtime applies one decay tick: read the full ZSET, scale
// every score by trendingDecayFactor, drop members below the floor, and
// rewrite the surviving ones in a single pipeline.
//
// O(N) where N is the ZSET size. Bounded by trendingMaxReturn (200) in
// practice because we only ever surface the top-200 anyway.
func pruneTrendingRealtime(_ context.Context) {
	if rdb == nil {
		return
	}
	res, err := rdb.ZRangeWithScores(rctx, trendingRealtimeKey, 0, -1).Result()
	if err != nil {
		return
	}
	// Pre-allocate the survivor list to avoid reallocs.
	survivors := make([]redis.Z, 0, len(res))
	for _, m := range res {
		mem, ok := m.Member.(string)
		if !ok {
			continue
		}
		newScore := m.Score * trendingDecayFactor
		// Drop members whose score has decayed below the noise floor in
		// either direction. Negative members are kept until they decay
		// toward zero so a barrage of reports can keep an item suppressed
		// while spam-fighting is still relevant, but they'll naturally
		// drift back toward the floor and be evicted.
		if newScore < trendingFloorScore && newScore > -trendingFloorScore {
			continue
		}
		survivors = append(survivors, redis.Z{Score: newScore, Member: mem})
	}
	pipe := rdb.TxPipeline()
	pipe.Del(rctx, trendingRealtimeKey)
	if len(survivors) > 0 {
		pipe.ZAdd(rctx, trendingRealtimeKey, survivors...)
		pipe.Expire(rctx, trendingRealtimeKey, 24*time.Hour)
	}
	_, _ = pipe.Exec(rctx)
}

// itoa is a tiny strconv shim — keeps the file from depending on strconv just
// for one Itoa call inside loadHomeFeedItemByID's two branches. Same
// implementation strategy as the rest of the codebase.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	negative := n < 0
	if negative {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if negative {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
