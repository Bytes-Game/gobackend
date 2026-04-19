package main

import (
	"fmt"
	"log"
	"strconv"
	"strings"
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
	// Recompute resistance level from new bounce data
	state.ResistanceLevel = detectResistance(state)
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

// AvgDwellMs returns mean dwell time.
func (s ImpressionStats) AvgDwellMs() float64 {
	if s.Count == 0 {
		return 0
	}
	return float64(s.TotalDwellMs) / float64(s.Count)
}

// getImpressionStats reads raw impressions from Redis and aggregates by category.
func getImpressionStats(userID string) (byCategory map[string]*ImpressionStats, byCreator map[string]*ImpressionStats) {
	byCategory = make(map[string]*ImpressionStats)
	byCreator = make(map[string]*ImpressionStats)

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
		category := parts[0]
		dwellMs, _ := strconv.Atoi(parts[1])
		creator := parts[3]

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

	processed := 0
	for userID := range users {
		if err := aggregateUserImpressions(userID); err != nil {
			log.Printf("aggregate user %s: %v", userID, err)
			continue
		}
		processed++
	}
	if processed > 0 {
		log.Printf("impression aggregator: processed %d users", processed)
	}
	return nil
}

// aggregateUserImpressions converts a user's raw impressions into affinity adjustments.
// Categories with high bounce rate get decayed. Categories with high interest get boosted.
func aggregateUserImpressions(userID string) error {
	byCategory, _ := getImpressionStats(userID)
	if len(byCategory) == 0 {
		return nil
	}

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

		bounceRate := stats.BounceRate()
		current := profile.CategoryAffinity[category]

		if bounceRate > bounceRateNegativeThreshold {
			// User is consistently ignoring this category → decay affinity
			newVal := current - affinityDecayPerCycle
			if newVal < 0 {
				newVal = 0
			}
			profile.CategoryAffinity[category] = newVal
			changed = true

			// Extreme bounce rate (>85%) in large sample → add to avoided
			if bounceRate > 0.85 && stats.Count >= 15 {
				if !containsString(profile.AvoidedCategories, category) {
					profile.AvoidedCategories = append(profile.AvoidedCategories, category)
					changed = true
				}
			}
		} else if bounceRate < bounceRatePositiveThreshold && stats.InterestCount > 0 {
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
