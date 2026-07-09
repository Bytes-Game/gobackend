package main

// cooccurrence.go — "users who engaged with X also engaged with Y".
//
// The most TikTok-like retrieval lane available without ML infra:
// pure engagement co-occurrence counts. When one user positively
// engages with item B shortly after item A (same session window), A
// and B co-occur; content that co-occurs with what YOU just engaged
// with is a strong candidate for your next page.
//
// WRITE PATH (event ingestion, positive events only):
//   lasteng:{userID}   — LIST of the user's last 5 engaged "type:id"
//                        keys, 2h TTL (the session-ish window).
//   cooc:{key}         — ZSET of co-engaged neighbor keys, symmetric
//                        (A→B and B→A), capped at coocMaxNeighbors,
//                        30d TTL. Increment-only ⇒ multi-replica safe.
//
// READ PATH (candidate source): take the user's last 3 engaged items
// as seeds, union their neighbor ZSETs (scores summed across seeds so
// an item co-occurring with SEVERAL of your recent engagements ranks
// above one that matches only one), drop the seeds themselves and the
// user's own uploads, materialize via loadHomeFeedItemByID.
//
// House rules apply: every Redis write is fire-and-forget, every read
// fails open to zero candidates (the source framework is fail-open).

import (
	"sort"
	"strings"
	"time"
)

const (
	coocLastEngagedKeyPrefix = "lasteng:"
	coocLastEngagedCap       = 5
	coocLastEngagedTTL       = 2 * time.Hour

	coocKeyPrefix    = "cooc:"
	coocMaxNeighbors = 100
	coocTTL          = 30 * 24 * time.Hour

	// coocPairWindow bounds how many of the user's previous engagements
	// each new engagement pairs with. 3 keeps the write fan-out small
	// (≤6 ZINCRBYs per positive event) while still linking across a
	// short attention window rather than only adjacent items.
	coocPairWindow = 3
)

func coocContentKey(contentType, contentID string) string {
	return contentType + ":" + contentID
}

// recordCoOccurrence links a fresh positive engagement with the user's
// recent ones. Call only for positive events (the caller gates on
// isPositiveEngagementForTrajectory — skips are noise, not preference).
func recordCoOccurrence(userID, contentType, contentID string) {
	if rdb == nil || userID == "" || contentID == "" {
		return
	}
	newKey := coocContentKey(contentType, contentID)
	listKey := coocLastEngagedKeyPrefix + userID

	prev, err := rdb.LRange(rctx, listKey, 0, coocPairWindow-1).Result()
	if err != nil {
		prev = nil
	}

	pipe := rdb.Pipeline()
	for _, p := range prev {
		if p == "" || p == newKey {
			continue
		}
		// Symmetric: engagement order carries little signal at this
		// granularity, and symmetry doubles the graph's density.
		pipe.ZIncrBy(rctx, coocKeyPrefix+p, 1, newKey)
		pipe.ZIncrBy(rctx, coocKeyPrefix+newKey, 1, p)
		pipe.Expire(rctx, coocKeyPrefix+p, coocTTL)
	}
	pipe.Expire(rctx, coocKeyPrefix+newKey, coocTTL)
	// Cap neighbor sets from the LOW-score end so established
	// co-occurrence survives and one-off noise is what falls out.
	pipe.ZRemRangeByRank(rctx, coocKeyPrefix+newKey, 0, -(coocMaxNeighbors + 1))
	pipe.LPush(rctx, listKey, newKey)
	pipe.LTrim(rctx, listKey, 0, coocLastEngagedCap-1)
	pipe.Expire(rctx, listKey, coocLastEngagedTTL)
	_, _ = pipe.Exec(rctx)
}

// sourceCoOccurrence is the candidate source: neighbors of the user's
// last engaged items, score-summed across seeds.
func sourceCoOccurrence(userID string, limit int) []HomeFeedItem {
	if rdb == nil || userID == "" || limit <= 0 {
		return nil
	}
	seeds, err := rdb.LRange(rctx, coocLastEngagedKeyPrefix+userID, 0, 2).Result()
	if err != nil || len(seeds) == 0 {
		return nil
	}
	seedSet := make(map[string]bool, len(seeds))
	for _, s := range seeds {
		seedSet[s] = true
	}

	scores := map[string]float64{}
	for _, seed := range seeds {
		neighbors, err := rdb.ZRevRangeWithScores(rctx, coocKeyPrefix+seed, 0, 20).Result()
		if err != nil {
			continue
		}
		for _, n := range neighbors {
			key, ok := n.Member.(string)
			if !ok || seedSet[key] {
				continue
			}
			scores[key] += n.Score
		}
	}
	if len(scores) == 0 {
		return nil
	}

	type cand struct {
		key   string
		score float64
	}
	ranked := make([]cand, 0, len(scores))
	for k, s := range scores {
		ranked = append(ranked, cand{k, s})
	}
	sort.Slice(ranked, func(i, j int) bool { return ranked[i].score > ranked[j].score })

	out := make([]HomeFeedItem, 0, limit)
	for _, c := range ranked {
		if len(out) >= limit {
			break
		}
		typ, id, ok := strings.Cut(c.key, ":")
		if !ok || id == "" {
			continue
		}
		item, found := loadHomeFeedItemByID(typ, id)
		if !found {
			continue
		}
		// Never recommend the user their own upload via this lane.
		if item.Challenge != nil && item.Challenge.CreatorID == userID {
			continue
		}
		out = append(out, item)
	}
	return out
}
