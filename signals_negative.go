package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// ════════════════════════════════════════════════════════════════════════════════
// NEGATIVE & SEARCH SIGNALS — real-time Redis capture, ranker-time lookup
// ════════════════════════════════════════════════════════════════════════════════
//
// These are high-confidence signals the old ranker ignored. Each is captured
// the moment the event fires (from event ingest or explicit handlers) and kept
// in Redis with an appropriate TTL. scoreForUser consults them to produce hard
// penalties (block/report/unfollow) or category boosts (recent search).
//
// KEY SHAPES:
//   blocked_creators:{user}  SET   creatorId values        TTL: none (persistent until unblocked)
//   unfollowed:{user}        ZSET  creatorId → unixTs      TTL: 7d auto-prune at lookup time
//   recent_bounces:{user}    ZSET  contentId → unixTs      TTL: 24h per entry (sliding)
//   recent_searches:{user}   LIST  normalized query        capped at 10, 24h key TTL
//   last_session_end:{user}  STRING unixTs                  TTL: 30d
//
// All write paths are best-effort: a Redis blip should never break the main
// user flow, so errors are logged and swallowed.

const (
	unfollowPenaltyWindow = 7 * 24 * time.Hour
	bounceWindow          = 24 * time.Hour
	searchWindow          = 24 * time.Hour
	recentSearchCap       = 10
	sessionEndTTL         = 30 * 24 * time.Hour
	bounceMaxMs           = 1000 // watch_duration_ms <= this = bounce
)

// ─────────────────────────────────────────────────────────────────────────────
// CAPTURE
// ─────────────────────────────────────────────────────────────────────────────

// MarkBlocked adds a creator to a user's persistent block list. Called from the
// block flow / high-severity report. Blocked creators are hard-penalized
// (effectively removed) from the feed until explicitly unblocked.
func MarkBlocked(userID, creatorID string) {
	if userID == "" || creatorID == "" {
		return
	}
	if err := rdb.SAdd(rctx, "blocked_creators:"+userID, creatorID).Err(); err != nil {
		// non-fatal: user will still be safe because the event is also in feed_events
	}
	if metricSignalCapture != nil {
		metricSignalCapture.WithLabelValues("block").Inc()
	}
}

// UnmarkBlocked reverses MarkBlocked.
func UnmarkBlocked(userID, creatorID string) {
	if userID == "" || creatorID == "" {
		return
	}
	_ = rdb.SRem(rctx, "blocked_creators:"+userID, creatorID).Err()
}

// MarkUnfollowed records a soft negative that decays over 7 days. Uses a ZSET
// keyed by unix timestamp so we can trim on read.
func MarkUnfollowed(userID, creatorID string) {
	if userID == "" || creatorID == "" {
		return
	}
	now := float64(time.Now().Unix())
	_ = rdb.ZAdd(rctx, "unfollowed:"+userID, redis.Z{Score: now, Member: creatorID}).Err()
	// Keep the key fresh for at least the penalty window.
	_ = rdb.Expire(rctx, "unfollowed:"+userID, unfollowPenaltyWindow+24*time.Hour).Err()
	if metricSignalCapture != nil {
		metricSignalCapture.WithLabelValues("unfollow").Inc()
	}
}

// MarkBounce records a <1s dismissal of a specific piece of content — a much
// stronger "no" than a normal skip. Keyed by (user, content).
func MarkBounce(userID, contentID string) {
	if userID == "" || contentID == "" {
		return
	}
	now := float64(time.Now().Unix())
	_ = rdb.ZAdd(rctx, "recent_bounces:"+userID, redis.Z{Score: now, Member: contentID}).Err()
	_ = rdb.Expire(rctx, "recent_bounces:"+userID, bounceWindow+time.Hour).Err()
	if metricSignalCapture != nil {
		metricSignalCapture.WithLabelValues("bounce").Inc()
	}
}

// RecordSearchQuery pushes a normalized query onto the user's recent-search
// list. The ranker biases feed categories toward these queries for 24h.
func RecordSearchQuery(userID, query string) {
	q := strings.ToLower(strings.TrimSpace(query))
	if userID == "" || q == "" {
		return
	}
	if len(q) > 64 {
		q = q[:64]
	}
	key := "recent_searches:" + userID
	_ = rdb.LPush(rctx, key, q).Err()
	_ = rdb.LTrim(rctx, key, 0, recentSearchCap-1).Err()
	_ = rdb.Expire(rctx, key, searchWindow).Err()
	if metricSignalCapture != nil {
		metricSignalCapture.WithLabelValues("search").Inc()
	}
}

// RecordSessionEnd stamps when the user last ended a session. Ranker reads
// this to decide whether to carry over session mood (short gap) or reset
// (long gap).
func RecordSessionEnd(userID string) {
	if userID == "" {
		return
	}
	_ = rdb.Set(rctx, "last_session_end:"+userID, fmt.Sprintf("%d", time.Now().Unix()), sessionEndTTL).Err()
	if metricSignalCapture != nil {
		metricSignalCapture.WithLabelValues("session_end").Inc()
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// LOOKUP (feed-request time, cached per-request via SignalCache)
// ─────────────────────────────────────────────────────────────────────────────

type negativeSignals struct {
	blocked       map[string]bool    // creatorId -> true
	unfollowed    map[string]float64 // creatorId -> unfollow unix time (for decaying penalty)
	bounces       map[string]bool    // contentKey (type:id) -> true
	recentQueries []string
	lastSessionEnd time.Time // zero if unknown
}

var negativeCache = NewSignalCache[*negativeSignals](2 * time.Minute)

// warmNegativeSignals loads every negative-signal key for the user into the
// request-local cache. Trims stale ZSET entries on the way in.
func warmNegativeSignals(userID string) {
	ns := &negativeSignals{
		blocked:    make(map[string]bool),
		unfollowed: make(map[string]float64),
		bounces:    make(map[string]bool),
	}

	// Blocked creators — persistent SET.
	if members, err := rdb.SMembers(rctx, "blocked_creators:"+userID).Result(); err == nil {
		for _, m := range members {
			ns.blocked[m] = true
		}
	}

	// Unfollowed — ZSET, trim anything older than window.
	cutoff := float64(time.Now().Add(-unfollowPenaltyWindow).Unix())
	_ = rdb.ZRemRangeByScore(rctx, "unfollowed:"+userID, "0", fmt.Sprintf("%f", cutoff)).Err()
	if zs, err := rdb.ZRangeWithScores(rctx, "unfollowed:"+userID, 0, -1).Result(); err == nil {
		for _, z := range zs {
			if s, ok := z.Member.(string); ok {
				ns.unfollowed[s] = z.Score
			}
		}
	}

	// Recent bounces — ZSET of contentKey (type:id) → ts, trim old.
	bounceCutoff := float64(time.Now().Add(-bounceWindow).Unix())
	_ = rdb.ZRemRangeByScore(rctx, "recent_bounces:"+userID, "0", fmt.Sprintf("%f", bounceCutoff)).Err()
	if members, err := rdb.ZRange(rctx, "recent_bounces:"+userID, 0, -1).Result(); err == nil {
		for _, m := range members {
			ns.bounces[m] = true
		}
	}

	// Recent searches — LIST.
	if items, err := rdb.LRange(rctx, "recent_searches:"+userID, 0, recentSearchCap-1).Result(); err == nil {
		ns.recentQueries = items
	}

	// Last session end.
	if s, err := rdb.Get(rctx, "last_session_end:"+userID).Result(); err == nil && s != "" {
		var ts int64
		_, _ = fmt.Sscanf(s, "%d", &ts)
		if ts > 0 {
			ns.lastSessionEnd = time.Unix(ts, 0)
		}
	}

	negativeCache.Set(userID, ns)
}

// getNegativeSignals returns the per-request bundle (may be nil if warm wasn't called).
func getNegativeSignals(userID string) *negativeSignals {
	ns, _ := negativeCache.Get(userID)
	return ns
}

// ─────────────────────────────────────────────────────────────────────────────
// SCORING HELPERS — used by scoreForUser
// ─────────────────────────────────────────────────────────────────────────────

// negativeCreatorPenalty returns a multiplicative factor <= 1.0 by which the
// creator's content should be attenuated. 0 = fully hidden (blocked).
func negativeCreatorPenalty(ns *negativeSignals, creatorID string) float64 {
	if ns == nil || creatorID == "" {
		return 1.0
	}
	if ns.blocked[creatorID] {
		return 0.0 // Hard block: contributes nothing.
	}
	if unfTs, ok := ns.unfollowed[creatorID]; ok {
		// Linear decay: full penalty at 0 days, none at 7 days.
		age := time.Since(time.Unix(int64(unfTs), 0))
		if age >= unfollowPenaltyWindow {
			return 1.0
		}
		decay := 1.0 - float64(age)/float64(unfollowPenaltyWindow)
		// Max 50% attenuation, easing as time passes.
		return 1.0 - 0.5*decay
	}
	return 1.0
}

// bouncePenalty returns 1.0 normally, 0.0 if the user bounced on this exact
// content in the last 24h (never re-serve a just-bounced item).
func bouncePenalty(ns *negativeSignals, contentType, contentID string) float64 {
	if ns == nil {
		return 1.0
	}
	if ns.bounces[contentType+":"+contentID] {
		return 0.0
	}
	return 1.0
}

// searchBoost returns a 0..1 multiplier reflecting how well the content matches
// the user's last 3 search queries. Rough substring match on category + caption.
func searchBoost(ns *negativeSignals, category, caption string) float64 {
	if ns == nil || len(ns.recentQueries) == 0 {
		return 0
	}
	target := strings.ToLower(category + " " + caption)
	best := 0.0
	// Only the most recent 3 queries count, decaying weight.
	for i, q := range ns.recentQueries {
		if i >= 3 || q == "" {
			break
		}
		if strings.Contains(target, q) {
			w := 1.0 - float64(i)*0.3 // 1.0, 0.7, 0.4
			if w > best {
				best = w
			}
		}
	}
	return best
}

// sessionContinuity returns 1.0 for a fresh-start user (long gap since last
// session) and a smaller value (down to 0.2) for users returning quickly. The
// ranker uses this to decide how aggressively to respect the last session's
// mood vs. give a clean slate.
func sessionContinuityFactor(ns *negativeSignals) float64 {
	if ns == nil || ns.lastSessionEnd.IsZero() {
		return 1.0 // No prior session → fresh start.
	}
	gap := time.Since(ns.lastSessionEnd)
	switch {
	case gap < 2*time.Hour:
		return 0.2 // Very recent — strong continuity (small factor = "don't shake things up")
	case gap < 6*time.Hour:
		return 0.4
	case gap < 24*time.Hour:
		return 0.7
	case gap < 72*time.Hour:
		return 0.9
	default:
		return 1.0
	}
}

