package main

import (
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// ─────────────────────────────────────────────────────────────────────────────
// SEEN-CONTENT FILTER
//
// The ranker MUST NOT re-serve content a user saw in the last ~12 hours.
// buildInteractedSet only covers content a user actively engaged with
// (liked/commented); plain impressions were invisible to dedup. This fills
// that gap with a bounded Redis sorted set per user, keyed by unix-ts score
// so we can evict old entries cheaply.
// ─────────────────────────────────────────────────────────────────────────────

const (
	seenKeyPrefix = "seen:"           // + userID
	seenTTL       = 12 * time.Hour    // window after which content may reappear
	seenMaxSize   = 2000              // hard cap to bound memory per user
)

func seenKey(userID string) string { return seenKeyPrefix + userID }

// seenMember returns the canonical member string we store in the set.
func seenMember(contentType, contentID string) string {
	return contentType + ":" + contentID
}

// markShown records that a user was served this item. Stamps the current
// unix-ts as the sorted-set score so TTL-based trimming is a single ZREMRANGEBYSCORE.
func markShown(userID, contentType, contentID string) {
	if rdb == nil || userID == "" || contentID == "" {
		return
	}
	key := seenKey(userID)
	now := time.Now().Unix()
	m := seenMember(contentType, contentID)
	// Add with timestamp score.
	_ = rdb.ZAdd(rctx, key, redis.Z{Score: float64(now), Member: m}).Err()
	// Trim anything older than the window.
	cutoff := fmt.Sprintf("%d", now-int64(seenTTL.Seconds()))
	_ = rdb.ZRemRangeByScore(rctx, key, "0", cutoff).Err()
	// Hard cap the set size: drop oldest until under seenMaxSize.
	if n, err := rdb.ZCard(rctx, key).Result(); err == nil && n > seenMaxSize {
		over := n - seenMaxSize
		_ = rdb.ZRemRangeByRank(rctx, key, 0, over-1).Err()
	}
	// Refresh TTL on the key itself.
	_ = rdb.Expire(rctx, key, 2*seenTTL).Err()
}

// markShownBatch is markShown for many items at once — used after a feed is
// composed so the next page cannot serve the same content.
func markShownBatch(userID string, items []HomeFeedItem) {
	if rdb == nil || userID == "" || len(items) == 0 {
		return
	}
	key := seenKey(userID)
	now := time.Now().Unix()
	members := make([]redis.Z, 0, len(items))
	for _, it := range items {
		id := getItemID(it)
		if id == "" {
			continue
		}
		members = append(members, redis.Z{Score: float64(now), Member: seenMember(it.Type, id)})
	}
	if len(members) == 0 {
		return
	}
	_ = rdb.ZAdd(rctx, key, members...).Err()
	cutoff := strconv.FormatInt(now-int64(seenTTL.Seconds()), 10)
	_ = rdb.ZRemRangeByScore(rctx, key, "0", cutoff).Err()
	if n, err := rdb.ZCard(rctx, key).Result(); err == nil && n > seenMaxSize {
		_ = rdb.ZRemRangeByRank(rctx, key, 0, n-seenMaxSize-1).Err()
	}
	_ = rdb.Expire(rctx, key, 2*seenTTL).Err()
	if metricSeenMarks != nil {
		metricSeenMarks.WithLabelValues("ok").Add(float64(len(members)))
	}
}

// loadSeenSet reads all members into a hash for O(1) membership checks.
// Small users stay small; capped users stay capped.
func loadSeenSet(userID string) map[string]bool {
	out := make(map[string]bool)
	if rdb == nil || userID == "" {
		return out
	}
	members, err := rdb.ZRange(rctx, seenKey(userID), 0, -1).Result()
	if err != nil {
		return out
	}
	for _, m := range members {
		out[m] = true
	}
	return out
}

// filterUnseen returns items the user has NOT already been shown in the
// current TTL window. Preserves input order.
func filterUnseen(userID string, items []HomeFeedItem) []HomeFeedItem {
	seen := loadSeenSet(userID)
	if len(seen) == 0 {
		return items
	}
	out := make([]HomeFeedItem, 0, len(items))
	dropped := 0
	for _, it := range items {
		id := getItemID(it)
		if id == "" {
			continue
		}
		if seen[seenMember(it.Type, id)] {
			dropped++
			continue
		}
		out = append(out, it)
	}
	if metricSeenFiltered != nil && dropped > 0 {
		metricSeenFiltered.Add(float64(dropped))
	}
	return out
}

// seenFilterMinKeep is the floor below which we stop dropping seen items.
// If filtering everything seen would leave the page nearly empty (sparse
// catalog, exhausted user, brand-new region), re-admit the best-scored
// seen items so the user sees SOMETHING. Re-watching popular content is
// normal behavior on Reels/TikTok — empty page is not.
const seenFilterMinKeep = 8

// filterUnseenScored is the ScoredItem variant used after ranking.
//
// Strategy:
//  1. Walk items, separating unseen from seen (preserving order = score).
//  2. If we have ≥ seenFilterMinKeep unseen, return them — same as before.
//  3. If we have fewer, top up from the seen pile (highest-score first)
//     so the user sees re-watches rather than an empty feed. The first
//     `len(unseen)` slots are still strictly fresh; only the tail gets
//     re-admitted seen content.
func filterUnseenScored(userID string, items []ScoredItem) []ScoredItem {
	seen := loadSeenSet(userID)
	if len(seen) == 0 {
		return items
	}
	unseen := make([]ScoredItem, 0, len(items))
	seenItems := make([]ScoredItem, 0, len(items))
	for _, si := range items {
		id := getItemID(si.Item)
		if id == "" {
			continue
		}
		if seen[seenMember(si.Item.Type, id)] {
			seenItems = append(seenItems, si)
		} else {
			unseen = append(unseen, si)
		}
	}
	// Healthy case: enough unseen items to fill a page.
	if len(unseen) >= seenFilterMinKeep {
		if metricSeenFiltered != nil && len(seenItems) > 0 {
			metricSeenFiltered.Add(float64(len(seenItems)))
		}
		return unseen
	}
	// Catalog exhausted (sparse data, power user, etc.): top up with
	// best-scored seen items so the user gets re-watches instead of nothing.
	out := make([]ScoredItem, 0, seenFilterMinKeep)
	out = append(out, unseen...)
	need := seenFilterMinKeep - len(unseen)
	if need > len(seenItems) {
		need = len(seenItems)
	}
	out = append(out, seenItems[:need]...)
	if metricSeenFiltered != nil {
		actuallyDropped := len(seenItems) - need
		if actuallyDropped > 0 {
			metricSeenFiltered.Add(float64(actuallyDropped))
		}
	}
	return out
}
