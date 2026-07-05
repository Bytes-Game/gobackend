package main

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ════════════════════════════════════════════════════════════════════════════════
// IMPRESSION AGGREGATOR
// ════════════════════════════════════════════════════════════════════════════════
//
// WHY a separate system for impressions:
//
// Every piece of content that appears on a user's screen is an "impression".
// A user scrolling through 50 items = 50 impressions per minute. For a million
// users that's 50M impressions/minute → 72B rows/day in PostgreSQL. Unsustainable.
//
// Solution: treat impressions as high-frequency ephemeral data.
//  1. Raw impressions go to Redis (fast writes, TTL 48h)
//  2. Background worker aggregates into user-level category stats every 5 min
//  3. Stats update the user_profiles.category_affinity — the long-term signal
//  4. Raw impressions expire. Only the learned preferences persist.
//
// THE KEY INSIGHT: we don't need the raw impression row forever. We need to know
// "this user consistently ignores dance content" — that fact lives in their
// CategoryAffinity. The 10,000 individual dance impressions that led to that
// conclusion can be discarded.

const (
	// Dwell time thresholds for classifying impressions
	dwellBounceThresholdMs  = 500  // < 500ms visible = user didn't even glance
	dwellCuriosityMs        = 1500 // 500-1500ms = saw it, chose not to engage
	// Signals user might care but didn't commit — weak positive
	dwellInterestMs         = 3000 // > 3000ms = seriously considered

	// Aggregation cadence
	impressionAggregationInterval = 5 * time.Minute
	impressionRedisTTL            = 48 * time.Hour

	// Bounce rate thresholds for affinity adjustment
	bounceRateNegativeThreshold = 0.6 // >60% bounces in a category = dislike signal
	bounceRatePositiveThreshold = 0.2 // <20% bounces + some engagement = strong interest

	// How much to decay category affinity per aggregation cycle
	affinityDecayPerCycle    = 0.02 // Slow drift down for ignored categories
	affinityBoostPerCycle    = 0.03 // Boost for categories user pauses on
	minCategoryImpressions   = 5    // Need at least 5 impressions to draw conclusions
	// Pseudo-observations for the Beta(K/2,K/2) prior used by ShrunkBounceRate.
	// Keeps a rate from 5 raw impressions from reading as a confident dislike.
	bounceShrinkPriorK = 10.0
)

// recordImpression stores a single impression in Redis.
// Key format: impressions:{userId} → LIST of "category|dwellMs|timestamp|contentId"
// Using a LIST keeps insertion O(1) and we trim to last 1000 per user to cap memory.
func recordImpression(event FeedEvent) {
	if event.UserID == "" || event.ContentID == "" {
		return
	}

	// Look up category for the content (cached)
	category := getContentCategory(event.ContentID, event.ContentType)
	if category == "" {
		category = "other"
	}

	// Fetch creator for per-creator aggregation
	creator := getContentCreator(event.ContentID, event.ContentType)

	// Format: category|dwellMs|timestamp|creator|contentId|contentType
	entry := fmt.Sprintf("%s|%d|%d|%s|%s|%s",
		category, event.WatchDurationMs, time.Now().Unix(),
		creator, event.ContentID, event.ContentType,
	)

	key := fmt.Sprintf("impressions:%s", event.UserID)
	pipe := rdb.Pipeline()
	pipe.LPush(rctx, key, entry)
	pipe.LTrim(rctx, key, 0, 999) // Keep last 1000 impressions per user
	pipe.Expire(rctx, key, impressionRedisTTL)
	if _, err := pipe.Exec(rctx); err != nil {
		log.Printf("recordImpression failed for user %s: %v", event.UserID, err)
	}

	// Also feed the impression into the live session state so resistance
	// detection can fire on bounce rate (sub-500ms dwells) BEFORE the user
	// bothers to issue explicit skip events.
	updateSessionFromImpression(event)
}

// updateSessionFromImpression tracks impression counts + bounces on the
// current Redis session state. Kept separate from the main event update loop
// because impressions don't deplete dopamine or advance ItemsSeen — they are
// a pure "we showed this, how did they react" signal.
func updateSessionFromImpression(event FeedEvent) {
	if event.UserID == "" || event.SessionID == "" {
		return
	}
	// Same session lock as updateSessionFromEvent so impression and engagement
	// writes to the same session blob don't clobber each other.
	unlock := sessionKeyLocks.lock(event.UserID + ":" + event.SessionID)
	defer unlock()
	state := getSessionState(event.UserID, event.SessionID)
	state.ImpressionCount++
	if event.WatchDurationMs < dwellBounceThresholdMs {
		state.BounceCount++
		state.BounceStreak++
		// Tier 1.2 / 2.7: persist the bounce on the per-user negative-signal
		// ZSET so the ranker will zero-out this exact content for 24h across
		// sessions — not just within this session.
		if event.ContentID != "" && event.ContentType != "" {
			go MarkBounce(event.UserID, event.ContentType+":"+event.ContentID)
		}
	} else {
		state.BounceStreak = 0
	}
	// Recompute resistance level AND mood from new bounce data — a bounce-heavy
	// burst with no explicit skip events would otherwise leave DetectedMood stale
	// (frustrated never detected until an engagement event re-ran detectMood).
	state.ResistanceLevel = detectResistance(state)
	state.DetectedMood = detectMood(state)
	saveSessionState(state)
}

// ImpressionStats holds aggregated impression metrics per category or creator.
type ImpressionStats struct {
	Count         int
	BounceCount   int // dwell < 500ms
	CuriosityCount int // dwell 500-1500ms
	InterestCount int // dwell > 3000ms
	TotalDwellMs  int
}

// BounceRate returns fraction of impressions that were bounces (<500ms).
func (s ImpressionStats) BounceRate() float64 {
	if s.Count == 0 {
		return 0
	}
	return float64(s.BounceCount) / float64(s.Count)
}

// ShrunkBounceRate returns the bounce fraction shrunk toward a neutral 0.5
// prior with bounceShrinkPriorK pseudo-observations, so a rate computed from
// only a handful of impressions (e.g. 4 fast scrolls out of 5 in one
// distracted session) doesn't read as a proven category/creator dislike. The
// estimate only approaches the raw BounceRate() as real evidence accumulates.
func (s ImpressionStats) ShrunkBounceRate() float64 {
	return (float64(s.BounceCount) + 0.5*bounceShrinkPriorK) / (float64(s.Count) + bounceShrinkPriorK)
}

// AvgDwellMs returns mean dwell time.
func (s ImpressionStats) AvgDwellMs() float64 {
	if s.Count == 0 {
		return 0
	}
	return float64(s.TotalDwellMs) / float64(s.Count)
}

// getImpressionStats reads ALL raw impressions from Redis and aggregates by
// category. Used by callers that want the full retained window (admin
// diagnostics, serve-time signal cache).
func getImpressionStats(userID string) (byCategory map[string]*ImpressionStats, byCreator map[string]*ImpressionStats) {
	byCategory, byCreator, _ = getImpressionStatsWithFresh(userID, 0)
	return
}

// getImpressionStatsWithFresh is getImpressionStats plus the set of categories
// that received at least one impression recorded strictly AFTER sinceUnix (the
// entry timestamp, field 3). The aggregator computes rates/sample sizes over the
// FULL window (so minCategoryImpressions and bounce rates stay stable) but only
// nudges a category present in freshCategories — so a category with NO new
// impressions this cycle is not re-nudged over the same 48h-retained list. That
// stale re-nudging applied the fixed ±delta every 5-min cycle (~576 times over
// 48h even with zero new activity) and saturated CategoryAffinity to 0/1.
// sinceUnix<=0 means "track nothing fresh" (freshCategories stays empty), which
// is what the full-window callers above want.
func getImpressionStatsWithFresh(userID string, sinceUnix int64) (byCategory, byCreator map[string]*ImpressionStats, freshCategories map[string]bool) {
	byCategory = make(map[string]*ImpressionStats)
	byCreator = make(map[string]*ImpressionStats)
	freshCategories = make(map[string]bool)

	key := fmt.Sprintf("impressions:%s", userID)
	entries, err := rdb.LRange(rctx, key, 0, -1).Result()
	if err != nil || len(entries) == 0 {
		return
	}

	for _, entry := range entries {
		parts := strings.Split(entry, "|")
		if len(parts) < 6 {
			continue
		}
		// Lowercase at the SOURCE so 'Comedy' and 'comedy' merge into one bucket.
		// Otherwise mixed-case records split: each partial count is gated
		// independently by minCategoryImpressions, and both buckets write to the
		// same lowercase CategoryAffinity key — the second read-after-write applies
		// decay/boost TWICE in one cycle. Canonical lowercase is the key everywhere.
		category := strings.ToLower(parts[0])
		dwellMs, _ := strconv.Atoi(parts[1])
		creator := parts[3]

		// Mark the category fresh if this impression postdates the aggregator's
		// cursor. Only parse the timestamp when a cursor is set (full-window
		// callers pass sinceUnix<=0 and skip this).
		if sinceUnix > 0 {
			if ts, e := strconv.ParseInt(parts[2], 10, 64); e == nil && ts > sinceUnix {
				freshCategories[category] = true
			}
		}

		// Aggregate by category
		if byCategory[category] == nil {
			byCategory[category] = &ImpressionStats{}
		}
		s := byCategory[category]
		s.Count++
		s.TotalDwellMs += dwellMs
		if dwellMs < dwellBounceThresholdMs {
			s.BounceCount++
		} else if dwellMs < dwellCuriosityMs {
			s.CuriosityCount++
		} else if dwellMs > dwellInterestMs {
			s.InterestCount++
		}

		// Aggregate by creator
		if creator != "" {
			if byCreator[creator] == nil {
				byCreator[creator] = &ImpressionStats{}
			}
			cs := byCreator[creator]
			cs.Count++
			cs.TotalDwellMs += dwellMs
			if dwellMs < dwellBounceThresholdMs {
				cs.BounceCount++
			} else if dwellMs < dwellCuriosityMs {
				cs.CuriosityCount++ // mirror the category classification; was silently always 0
			} else if dwellMs > dwellInterestMs {
				cs.InterestCount++
			}
		}
	}
	return
}

// startImpressionAggregator launches a background worker that periodically
// aggregates raw impressions into user profile signals.
//
// WHY a worker, not real-time:
// Aggregating on every impression would mean updating the DB ~50x/minute per user.
// Batching every 5min reduces DB writes by ~250x while still being responsive
// enough that the algorithm adapts within a single session.
func startImpressionAggregator() {
	go func() {
		// Initial delay so server isn't aggregating during startup
		time.Sleep(90 * time.Second)

		ticker := time.NewTicker(impressionAggregationInterval)
		defer ticker.Stop()

		for range ticker.C {
			if err := aggregateAllUserImpressions(); err != nil {
				log.Printf("impression aggregation error: %v", err)
			}
		}
	}()
}

// aggregateAllUserImpressions finds all users with recent impressions and updates their profiles.
func aggregateAllUserImpressions() error {
	// Scan for impression keys
	var cursor uint64
	users := make(map[string]bool)
	for {
		keys, next, err := rdb.Scan(rctx, cursor, "impressions:*", 500).Result()
		if err != nil {
			return err
		}
		for _, k := range keys {
			if len(k) > len("impressions:") {
				users[k[len("impressions:"):]] = true
			}
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}

	// Process users concurrently with a bounded worker pool. The old serial loop
	// did O(users) sequential loadUserProfile+saveUserProfile round-trips in one
	// goroutine, which can't finish within the 5-min tick at scale. Each user's
	// update is independent and serialized per-user by profileKeyLocks, so
	// concurrency is safe; the pool is capped so we don't exhaust the DB
	// connection pool.
	const aggWorkers = 8
	jobs := make(chan string, len(users))
	var processed int64
	var wg sync.WaitGroup
	for w := 0; w < aggWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for userID := range jobs {
				if err := aggregateUserImpressions(userID); err != nil {
					log.Printf("aggregate user %s: %v", userID, err)
					continue
				}
				atomic.AddInt64(&processed, 1)
			}
		}()
	}
	for userID := range users {
		jobs <- userID
	}
	close(jobs)
	wg.Wait()

	if processed > 0 {
		log.Printf("impression aggregator: processed %d users", processed)
	}
	return nil
}

// aggregateUserImpressions converts a user's raw impressions into affinity adjustments.
// Categories with high bounce rate get decayed. Categories with high interest get boosted.
func aggregateUserImpressions(userID string) error {
	// Idempotency cursor: impressions are retained ~48h and never consumed, so
	// re-reading the whole list every 5-min cycle applied the fixed ±delta to the
	// same categories hundreds of times and saturated CategoryAffinity to 0/1
	// (#12). We compute rates over the FULL window (stable sample/rate) but only
	// nudge categories that got a NEW impression since the last successful run.
	cursorKey := fmt.Sprintf("impressions_aggregated_at:%s", userID)
	var cursor int64
	if rdb != nil {
		if s, e := rdb.Get(rctx, cursorKey).Result(); e == nil {
			cursor, _ = strconv.ParseInt(s, 10, 64)
		}
	}
	nowUnix := time.Now().Unix()

	byCategory, _, fresh := getImpressionStatsWithFresh(userID, cursor)
	if len(byCategory) == 0 {
		return nil
	}
	if len(fresh) == 0 {
		// No new impressions since the last cycle (or first run, cursor==0):
		// advance the cursor and skip — do NOT re-nudge over the same retained
		// window. This is the steady-state path for any user not currently active.
		if rdb != nil {
			_ = rdb.Set(rctx, cursorKey, nowUnix, impressionRedisTTL).Err()
		}
		return nil
	}

	// Serialize the profile RMW against the other profile writers (negative
	// miner, strategy-outcome, recompute) so this aggregator's affinity/avoided
	// changes merge instead of clobbering theirs.
	unlock := profileKeyLocks.lock(userID)
	defer unlock()
	// Load current profile to adjust
	profile, err := loadUserProfile(userID)
	if err != nil || profile == nil {
		return err
	}
	if profile.CategoryAffinity == nil {
		profile.CategoryAffinity = make(map[string]float64)
	}

	changed := false
	for category, stats := range byCategory {
		if stats.Count < minCategoryImpressions {
			continue // Not enough data to draw conclusion
		}
		// Only nudge categories with a NEW impression this cycle. The full-window
		// stats above give a stable rate/sample, but the ±delta must fire once per
		// genuine new signal, not every cycle over the same retained impressions
		// (#12 re-decay saturation). A persistently-disliked category still reaches
		// 0 — but over real continued exposure, not stale re-reads.
		if !fresh[category] {
			continue
		}
		// category is already canonical-lowercase (bucketed lowercase in
		// getImpressionStats), so CategoryAffinity/AvoidedCategories keys match the
		// miner / computeUserProfile / serve casing and buckets don't split.

		// Match the serve-time scorer's asymmetry so the two paths agree on what
		// counts as a dislike: the negative/decay/avoided decisions use the
		// SHRUNK rate (don't decay or blacklist a category on small-sample noise),
		// while the positive boost uses the RAW rate (gated on a real dwell signal).
		shrunkBounce := stats.ShrunkBounceRate()
		rawBounce := stats.BounceRate()
		current := profile.CategoryAffinity[category]

		if shrunkBounce > bounceRateNegativeThreshold {
			// User is consistently ignoring this category → decay affinity
			newVal := current - affinityDecayPerCycle
			if newVal < 0 {
				newVal = 0
			}
			profile.CategoryAffinity[category] = newVal
			changed = true

			// Extreme bounce rate (>85%) in large sample → add to avoided
			if shrunkBounce > 0.85 && stats.Count >= 15 {
				if !containsString(profile.AvoidedCategories, category) {
					profile.AvoidedCategories = append(profile.AvoidedCategories, category)
					changed = true
				}
			}
		} else if rawBounce < bounceRatePositiveThreshold && stats.InterestCount > 0 {
			// User pauses on this category → boost affinity
			newVal := current + affinityBoostPerCycle
			if newVal > 1.0 {
				newVal = 1.0
			}
			profile.CategoryAffinity[category] = newVal
			changed = true

			// Remove from avoided list if it was there (preferences changed)
			profile.AvoidedCategories = removeString(profile.AvoidedCategories, category)
		}
	}

	if changed {
		saveUserProfile(profile)
	}
	// Advance the cursor so this cycle's impressions aren't aggregated again next
	// time — even when nothing crossed a threshold (changed==false). Only reached
	// after a successful profile load, so a load failure retries the same window.
	if rdb != nil {
		_ = rdb.Set(rctx, cursorKey, nowUnix, impressionRedisTTL).Err()
	}
	return nil
}

// containsString is a small helper for []string membership.
func containsString(slice []string, s string) bool {
	for _, v := range slice {
		if v == s {
			return true
		}
	}
	return false
}

// removeString removes the first occurrence of s from slice.
func removeString(slice []string, s string) []string {
	for i, v := range slice {
		if v == s {
			return append(slice[:i], slice[i+1:]...)
		}
	}
	return slice
}
