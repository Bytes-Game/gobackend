package main

import (
	"sync"
	"time"
)

// ════════════════════════════════════════════════════════════════════════════════
// SIGNAL CACHES
// ════════════════════════════════════════════════════════════════════════════════
//
// WHY these caches exist:
//
// The scoring function runs for every candidate item on every feed request.
// If it hit the DB for "has this user scrolled back to this creator?" for each
// of the 100 candidates × 1000 requests/sec, we'd melt Postgres.
//
// Instead, we compute these once per user per feed request and cache them.
// Cache is per-user, short TTL (2 min), invalidated automatically.

// SignalCache is a thread-safe cache of user-specific signal data.
// Designed for scoring-time lookups: populated once per feed request, read many times.
type SignalCache[T any] struct {
	mu   sync.RWMutex
	data map[string]cacheEntry[T]
	ttl  time.Duration
}

type cacheEntry[T any] struct {
	value     T
	expiresAt time.Time
}

func NewSignalCache[T any](ttl time.Duration) *SignalCache[T] {
	c := &SignalCache[T]{
		data: make(map[string]cacheEntry[T]),
		ttl:  ttl,
	}
	// Periodic cleanup
	go c.cleanupLoop()
	return c
}

func (c *SignalCache[T]) Get(key string) (T, bool) {
	c.mu.RLock()
	entry, ok := c.data[key]
	c.mu.RUnlock()
	if !ok || time.Now().After(entry.expiresAt) {
		var zero T
		return zero, false
	}
	return entry.value, true
}

func (c *SignalCache[T]) Set(key string, value T) {
	c.mu.Lock()
	c.data[key] = cacheEntry[T]{
		value:     value,
		expiresAt: time.Now().Add(c.ttl),
	}
	c.mu.Unlock()
}

func (c *SignalCache[T]) cleanupLoop() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		c.mu.Lock()
		now := time.Now()
		for k, e := range c.data {
			if now.After(e.expiresAt) {
				delete(c.data, k)
			}
		}
		c.mu.Unlock()
	}
}

// ── Caches used by scoring ──

// Scroll-back: creator IDs user has scrolled back to recently.
// Bool map so scoring can do O(1) membership check.
var scrollBackCache = NewSignalCache[map[string]bool](2 * time.Minute)

// Completions: creator IDs → count of times user has completed their content.
var completionCache = NewSignalCache[map[string]int](2 * time.Minute)

// Loops: categories where user has looped videos (strong retention signal).
var loopCache = NewSignalCache[map[string]bool](2 * time.Minute)

// Unmutes: creator IDs user has unmuted (wants to hear them).
var unmuteCache = NewSignalCache[map[string]bool](2 * time.Minute)

// Profile visits: creator IDs user has tapped through to in the last 24h.
var profileVisitCache = NewSignalCache[map[string]bool](2 * time.Minute)

// Impression stats: per-user per-category aggregates from the aggregator.
var impressionStatsCache = NewSignalCache[map[string]*ImpressionStats](2 * time.Minute)

// Impression stats per creator — same source as impressionStatsCache, powers
// the per-creator bounce penalty in scoring (was computed but only ever read by
// the admin diagnostics endpoint).
var impressionCreatorStatsCache = NewSignalCache[map[string]*ImpressionStats](2 * time.Minute)

// warmUserSignalCaches populates all caches for a user before scoring begins.
// Called once at the top of SmartFeedHandler. One pass, many reads downstream.
//
// This is 5 queries total instead of 500 (5 × 100 candidates).
func warmUserSignalCaches(userID string) {
	// Scroll-back creators (last 24h)
	scrollBack := make(map[string]bool)
	rows, err := db.Query(`
		SELECT DISTINCT COALESCE(c.creator_id::text, p.author_id::text, '')
		FROM feed_events fe
		LEFT JOIN challenges c ON fe.content_type = 'challenge' AND fe.content_id = c.id::text
		LEFT JOIN posts p ON fe.content_type = 'post' AND fe.content_id = p.id::text
		WHERE fe.user_id = $1
		  AND fe.event_type = 'scroll_back'
		  AND fe.created_at > NOW() - INTERVAL '24 hours'
	`, userID)
	if err == nil {
		for rows.Next() {
			var id string
			if rows.Scan(&id) == nil && id != "" {
				scrollBack[id] = true
			}
		}
		rows.Close()
	}
	scrollBackCache.Set(userID, scrollBack)

	// Completion creators (last 7 days) with counts
	completions := make(map[string]int)
	rows, err = db.Query(`
		SELECT COALESCE(c.creator_id::text, p.author_id::text, ''), COUNT(*)
		FROM feed_events fe
		LEFT JOIN challenges c ON fe.content_type = 'challenge' AND fe.content_id = c.id::text
		LEFT JOIN posts p ON fe.content_type = 'post' AND fe.content_id = p.id::text
		WHERE fe.user_id = $1
		  AND fe.event_type = 'complete'
		  AND fe.created_at > NOW() - INTERVAL '7 days'
		GROUP BY COALESCE(c.creator_id::text, p.author_id::text, '')
	`, userID)
	if err == nil {
		for rows.Next() {
			var id string
			var cnt int
			if rows.Scan(&id, &cnt) == nil && id != "" {
				completions[id] = cnt
			}
		}
		rows.Close()
	}
	completionCache.Set(userID, completions)

	// Looped categories (last 7 days)
	loops := make(map[string]bool)
	rows, err = db.Query(`
		SELECT DISTINCT COALESCE(c.category, p.category, 'other')
		FROM feed_events fe
		LEFT JOIN challenges c ON fe.content_type = 'challenge' AND fe.content_id = c.id::text
		LEFT JOIN posts p ON fe.content_type = 'post' AND fe.content_id = p.id::text
		WHERE fe.user_id = $1
		  AND fe.event_type = 'loop'
		  AND fe.created_at > NOW() - INTERVAL '7 days'
	`, userID)
	if err == nil {
		for rows.Next() {
			var cat string
			if rows.Scan(&cat) == nil && cat != "" {
				loops[cat] = true
			}
		}
		rows.Close()
	}
	loopCache.Set(userID, loops)

	// Unmuted creators (last 7 days)
	unmutes := make(map[string]bool)
	rows, err = db.Query(`
		SELECT DISTINCT COALESCE(c.creator_id::text, p.author_id::text, '')
		FROM feed_events fe
		LEFT JOIN challenges c ON fe.content_type = 'challenge' AND fe.content_id = c.id::text
		LEFT JOIN posts p ON fe.content_type = 'post' AND fe.content_id = p.id::text
		WHERE fe.user_id = $1
		  AND fe.event_type = 'unmute'
		  AND fe.created_at > NOW() - INTERVAL '7 days'
	`, userID)
	if err == nil {
		for rows.Next() {
			var id string
			if rows.Scan(&id) == nil && id != "" {
				unmutes[id] = true
			}
		}
		rows.Close()
	}
	unmuteCache.Set(userID, unmutes)

	// Profile visits (creator IDs from metadata, last 24h)
	visits := make(map[string]bool)
	rows, err = db.Query(`
		SELECT DISTINCT COALESCE(metadata->>'creatorId', '')
		FROM feed_events
		WHERE user_id = $1
		  AND event_type = 'profile_visit'
		  AND created_at > NOW() - INTERVAL '24 hours'
	`, userID)
	if err == nil {
		for rows.Next() {
			var id string
			if rows.Scan(&id) == nil && id != "" {
				visits[id] = true
			}
		}
		rows.Close()
	}
	profileVisitCache.Set(userID, visits)

	// Impression stats from Redis — per-category AND per-creator.
	byCategory, byCreator := getImpressionStats(userID)
	impressionStatsCache.Set(userID, byCategory)
	impressionCreatorStatsCache.Set(userID, byCreator)
}
