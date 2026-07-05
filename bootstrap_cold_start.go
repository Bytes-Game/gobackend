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
	// Cold threshold: users with this many or fewer recorded events still get
	// bootstrap intermixing on the warm path. Set well ABOVE coldStartThreshold
	// (15) so the handoff is smooth: a user who just graduated from the pure-
	// popularity coldStartFeed (events >= 15) enters personalized scoring with a
	// still-meaningful bootstrap mix (~0.38 at 15) that decays to 0 by 60 —
	// instead of the old hard cliff (cold feed → 100% personalized at exactly 15,
	// with the warm-path mix only active in the dead [15,20) band).
	bootstrapColdEventThreshold = 60
	// Maximum fraction of feed slots we'll ever fill from the bootstrap pool.
	// Decays linearly with event count toward 0 at the threshold.
	bootstrapMaxMixFraction = 0.50
)

// userBootstrapMix returns the fraction of bootstrap items to inject based
// on the user's total event count. Decays linearly to 0 at the threshold.
//
// Examples (threshold 60):
//   events=0  → 0.50 (half the head is known-bangers; cold-feed path though)
//   events=15 → 0.375 (just graduated from coldStartFeed — smooth handoff)
//   events=30 → 0.25
//   events=60 → 0.00 (graduated; trust personalized signals fully)
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
	// Clamp the rate to [0,1]. Callers pass independent COUNT aggregates for
	// positives (likes+shares+comments) and trials (views), and one viewer can
	// like+comment+share a single view, so positives routinely EXCEEDS trials →
	// phat>1 → phat*(1-phat)<0 → sqrt of a negative → NaN. That NaN then poisons
	// finalScore and the sort comparator, corrupting the WHOLE feed ordering.
	// "More engagement than impressions" should saturate at rate 1.0, not NaN.
	if phat > 1 {
		phat = 1
	} else if phat < 0 {
		phat = 0
	}
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
				SELECT COUNT(DISTINCT user_id) FROM feed_events
				WHERE content_id = c.id::text
				  AND content_type = 'challenge'
				  AND event_type IN ('like','share','complete','save','rewatch','follow_from_content')
				  AND created_at > NOW() - INTERVAL '14 days'
			) AS positives
		FROM challenges c
		WHERE c.created_at > NOW() - INTERVAL '14 days'
		  AND c.visibility = 'arena'
		  AND c.status IN ('open','active','completed')
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

	// Also consult the user's CROSS-PAGE seen set. The bootstrap pool is fetched
	// independently from Redis and never passes through the upstream seen filter
	// (filterUnseenScored), so a top-of-pool "banger" injected on page 1 is
	// stamped seen but still returned by fetchBootstrapPool on page 2+. It is
	// absent from `primary` (the seen filter already dropped it there), so the
	// per-page `exclude` above cannot catch it — the user would see the identical
	// bootstrap video on consecutive pages of one session. Skip anything already
	// shown, mirroring filterUnseenScored's dedup.
	seen := loadSeenSet(userID)

	// Filter the pool by exclude + seen + self-content + materialize.
	bootstrap := make([]ScoredItem, 0, 16)
	for _, e := range pool {
		key := e.Type + ":" + e.ID
		if exclude[key] {
			continue
		}
		if seen[seenMember(e.Type, e.ID)] {
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
		// NOTE on scale: e.Score is the Wilson lower bound on engagement rate,
		// a probability in [0,1] — NOT on the scoreForUser finalScore scale (which
		// is an unbounded weighted sum). It's carried here only for reference/debug.
		// The interleave below is POSITIONAL (stride-based), so these scores are
		// never compared against personalized finalScores. Do not re-sort the
		// returned slice by .Score without first renormalizing the two scales.
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
	// We inject only into the HEAD of the feed so deep scrolls stay personalized.
	// The head is the first 30%, but for the very coldest users (large mix) we
	// allow it to grow up to bootstrapMaxMixFraction (50%) so enough known-good
	// content lands early — NOT the whole feed, which the old
	// `headLen = len(bootstrap)*stride` override could reach (it ignored the cap
	// entirely once bootstrap exceeded 30% of primary).
	stride := int(math.Round(1.0 / mix))
	if stride < 2 {
		stride = 2
	}
	headLen := len(primary) * 3 / 10
	maxHead := int(float64(len(primary)) * bootstrapMaxMixFraction)
	if headLen < len(bootstrap)*stride {
		headLen = len(bootstrap) * stride
	}
	if headLen > maxHead {
		headLen = maxHead
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

// userLeagueCache caches the user's league for egoBonus, which is read once PER
// CANDIDATE in the scoring hot loop — the value is per-user (identical across all
// candidates in a request), so a per-candidate SELECT was N redundant queries.
var userLeagueCache = NewSignalCache[string](5 * time.Minute)

func getUserLeague(userID string) string {
	if userID == "" || db == nil {
		return ""
	}
	if v, ok := userLeagueCache.Get(userID); ok {
		return v
	}
	var league string
	if err := db.QueryRow(`SELECT league FROM users WHERE CAST(id AS TEXT) = $1`, userID).Scan(&league); err != nil {
		return ""
	}
	userLeagueCache.Set(userID, league)
	return league
}

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
