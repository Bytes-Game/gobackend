package main

import (
	"context"
	"math"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// ─────────────────────────────────────────────────────────────────────────────
// COLD-START BOOTSTRAP
//
// A brand-new user has no embedding (zero vector), no LTR signal (model
// doesn't exercise their cohort), and an empty interaction history. Without
// help they get "average feed" — a damped blend of cohort defaults that
// is by construction nobody's favorite content.
//
// We solve this with a high-confidence "known-bangers" pool, ranked by
// Wilson-score lower-bound on engagement rate (so a 9/10 like-rate from
// 1000 impressions beats a 9/10 from 10 impressions). New users get this
// pool intermixed with their normal feed for the first ~20 events; the
// mix-in fraction decays as their interaction history grows.
//
// PERFORMANCE: the pool is computed in the background every 30 minutes and
// cached in Redis (`bootstrap:pool`). Per-request reads are a single ZSET
// range — cheap enough to call on every cold-user feed request.
// ─────────────────────────────────────────────────────────────────────────────

const (
	bootstrapPoolRedisKey   = "bootstrap:pool"
	bootstrapPoolSize       = 200
	bootstrapPoolRefreshInt = 30 * time.Minute
	bootstrapPoolTTL        = 2 * time.Hour
	// Cold threshold: users with this many or fewer recorded events are
	// treated as cold and receive bootstrap intermixing. Beyond this they
	// rely on personalized signals (embeddings, LTR, bandit).
	bootstrapColdEventThreshold = 20
	// Maximum fraction of feed slots we'll ever fill from the bootstrap pool.
	// At 0 events: 50% bootstrap, 50% personalized. At threshold: 0%.
	bootstrapMaxMixFraction = 0.50
)

// userBootstrapMix returns the fraction of bootstrap items to inject based
// on the user's total event count. Decays linearly to 0 at the threshold.
//
// Examples:
//   events=0  → 0.50 (half the head is known-bangers)
//   events=10 → 0.25
//   events=20 → 0.00 (graduated; trust personalized signals fully)
func userBootstrapMix(eventCount int) float64 {
	if eventCount <= 0 {
		return bootstrapMaxMixFraction
	}
	if eventCount >= bootstrapColdEventThreshold {
		return 0
	}
	frac := 1.0 - float64(eventCount)/float64(bootstrapColdEventThreshold)
	return frac * bootstrapMaxMixFraction
}

// wilsonLowerBound returns the Wilson score lower bound (95% confidence) of
// a positive proportion given p positives out of n trials. Standard formula
// — gives high-volume items credit for sample size.
//
// The result penalizes low-n items (e.g. 1/1 = 100% surface rate is
// downgraded to ~0.21) so trending floor scoring is stable.
func wilsonLowerBound(positives, trials float64) float64 {
	if trials <= 0 {
		return 0
	}
	const z = 1.96 // 95% confidence
	phat := positives / trials
	denom := 1 + z*z/trials
	center := phat + z*z/(2*trials)
	margin := z * math.Sqrt((phat*(1-phat)+z*z/(4*trials))/trials)
	return (center - margin) / denom
}

// computeBootstrapPool walks recent content (last 14 days) and ranks by
// Wilson lower bound on engagement-per-impression. Stored in Redis as a
// ZSET so cold-user feed calls can pull top-N in one round-trip.
//
// "Engagement" here = any positive event (like, share, complete, save).
// Impressions come from the impression aggregator's view counts.
func computeBootstrapPool(_ context.Context) {
	if db == nil || rdb == nil {
		return
	}
	rows, err := db.Query(`
		SELECT
			c.id::text AS content_id,
			'challenge' AS content_type,
			GREATEST(c.views, 1) AS impressions,
			(
				SELECT COUNT(*) FROM feed_events
				WHERE content_id = c.id::text
				  AND content_type = 'challenge'
				  AND event_type IN ('like','share','complete','save','rewatch','follow_from_content')
				  AND created_at > NOW() - INTERVAL '14 days'
			) AS positives
		FROM challenges c
		WHERE c.created_at > NOW() - INTERVAL '14 days'
		  AND c.visibility = 'arena'
		  AND c.status IN ('open','active','completed')
		UNION ALL
		SELECT
			p.id::text AS content_id,
			'post' AS content_type,
			GREATEST(p.views, 1) AS impressions,
			(
				SELECT COUNT(*) FROM feed_events
				WHERE content_id = p.id::text
				  AND content_type = 'post'
				  AND event_type IN ('like','share','complete','save','rewatch')
				  AND created_at > NOW() - INTERVAL '14 days'
			) AS positives
		FROM posts p
		WHERE p.created_at > NOW() - INTERVAL '14 days'
	`)
	if err != nil {
		return
	}
	defer rows.Close()

	type entry struct {
		Type, ID string
		Score    float64
	}
	pool := make([]entry, 0, bootstrapPoolSize*2)
	for rows.Next() {
		var contentID, contentType string
		var impressions, positives int64
		if err := rows.Scan(&contentID, &contentType, &impressions, &positives); err != nil {
			continue
		}
		score := wilsonLowerBound(float64(positives), float64(impressions))
		pool = append(pool, entry{Type: contentType, ID: contentID, Score: score})
	}

	// Partial sort: bubble the top bootstrapPoolSize to the front.
	for i := 0; i < bootstrapPoolSize && i < len(pool); i++ {
		best := i
		for j := i + 1; j < len(pool); j++ {
			if pool[j].Score > pool[best].Score {
				best = j
			}
		}
		pool[i], pool[best] = pool[best], pool[i]
	}
	if len(pool) > bootstrapPoolSize {
		pool = pool[:bootstrapPoolSize]
	}

	// Replace the Redis ZSET atomically: DEL then ZADD all members.
	pipe := rdb.TxPipeline()
	pipe.Del(rctx, bootstrapPoolRedisKey)
	members := make([]redis.Z, 0, len(pool))
	for _, e := range pool {
		members = append(members, redis.Z{Score: e.Score, Member: e.Type + ":" + e.ID})
	}
	if len(members) > 0 {
		pipe.ZAdd(rctx, bootstrapPoolRedisKey, members...)
	}
	pipe.Expire(rctx, bootstrapPoolRedisKey, bootstrapPoolTTL)
	_, _ = pipe.Exec(rctx)

	if metricBootstrapPool != nil {
		metricBootstrapPool.WithLabelValues("compute").Inc()
	}
}

// fetchBootstrapPool pulls the top-K (type, id) pairs by Wilson score.
// Caller is responsible for materializing them into HomeFeedItems.
func fetchBootstrapPool(limit int) []trendingRealtimeEntry {
	if rdb == nil || limit <= 0 {
		return nil
	}
	if limit > bootstrapPoolSize {
		limit = bootstrapPoolSize
	}
	res, err := rdb.ZRevRangeWithScores(rctx, bootstrapPoolRedisKey, 0, int64(limit-1)).Result()
	if err != nil || len(res) == 0 {
		return nil
	}
	out := make([]trendingRealtimeEntry, 0, len(res))
	for _, m := range res {
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

// applyBootstrapMixIfCold takes the ranker's primary feed result and, if the
// user is cold, intermixes bootstrap items into the first ~30% of slots.
// Positions of personalized items are preserved relative to each other; the
// bootstrap items are sprinkled in via a stride proportional to mix fraction.
//
// Called AFTER MMR + anti-loop so the personalized backbone is already
// stable; bootstrap is purely additive.
func applyBootstrapMixIfCold(userID string, primary []ScoredItem, eventCount int) []ScoredItem {
	mix := userBootstrapMix(eventCount)
	if mix <= 0 {
		return primary
	}
	pool := fetchBootstrapPool(bootstrapPoolSize)
	if len(pool) == 0 {
		return primary
	}

	// Build a quick excludeSet from the primary list so we don't reinsert
	// what's already there.
	exclude := make(map[string]bool, len(primary))
	for _, si := range primary {
		exclude[si.Item.Type+":"+getItemID(si.Item)] = true
	}

	// Filter the pool by exclude + self-content + materialize.
	bootstrap := make([]ScoredItem, 0, 16)
	for _, e := range pool {
		key := e.Type + ":" + e.ID
		if exclude[key] {
			continue
		}
		item, ok := loadHomeFeedItemByID(e.Type, e.ID)
		if !ok {
			continue
		}
		// Skip self-content.
		if item.Post != nil && item.Post.AuthorID == userID {
			continue
		}
		if item.Challenge != nil && item.Challenge.CreatorID == userID {
			continue
		}
		bootstrap = append(bootstrap, ScoredItem{
			Item:  item,
			Score: e.Score,
		})
		if len(bootstrap) >= 16 {
			break
		}
	}
	if len(bootstrap) == 0 {
		return primary
	}

	// Interleave: every Nth slot is a bootstrap item, where N ≈ 1/mix.
	// We only inject into the first 30% of the feed so deep scrolls stay
	// fully personalized.
	stride := int(math.Round(1.0 / mix))
	if stride < 2 {
		stride = 2
	}
	headLen := len(primary) * 3 / 10
	if headLen < len(bootstrap) {
		headLen = len(bootstrap) * stride
	}
	if headLen > len(primary) {
		headLen = len(primary)
	}

	out := make([]ScoredItem, 0, len(primary)+len(bootstrap))
	bsIdx := 0
	for i := 0; i < len(primary); i++ {
		if i < headLen && bsIdx < len(bootstrap) && (i+1)%stride == 0 {
			out = append(out, bootstrap[bsIdx])
			bsIdx++
		}
		out = append(out, primary[i])
	}

	if metricBootstrapPool != nil {
		metricBootstrapPool.WithLabelValues("inject").Add(float64(bsIdx))
	}
	return out
}

// startBootstrapPoolWorker periodically refreshes the cached known-bangers pool.
// Called once from main(). One iteration runs immediately on boot so a fresh
// deploy doesn't have an empty pool for the first 30 minutes.
func startBootstrapPoolWorker() {
	go func() {
		computeBootstrapPool(context.Background())
		t := time.NewTicker(bootstrapPoolRefreshInt)
		defer t.Stop()
		for range t.C {
			computeBootstrapPool(context.Background())
		}
	}()
}

// userEventCountCache caches per-user event counts for the cold-start gate.
// Counting on every feed request would be a hot SQL hit; a 5-min cache is
// fine because cold-status only changes when a user crosses the threshold.
var userEventCountCache = NewSignalCache[int](5 * time.Minute)

func getUserEventCount(userID string) int {
	if userID == "" || db == nil {
		return 0
	}
	if v, ok := userEventCountCache.Get(userID); ok {
		return v
	}
	var n int
	row := db.QueryRow(`SELECT COUNT(*) FROM feed_events WHERE user_id = $1`, userID)
	if err := row.Scan(&n); err != nil {
		return 0
	}
	userEventCountCache.Set(userID, n)
	return n
}
