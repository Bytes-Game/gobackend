package main

import (
	"fmt"
	"sort"
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
//
// TWO TIERS, NOT ONE CUT
//
// "Never re-serve" is the right rule only while there is something else to
// serve. A seed catalog, a niche region, or a user who has watched everything
// their follows posted all reach the same state: nothing unseen is left. The
// filter therefore ranks in two tiers rather than making a single yes/no cut:
//
//	tier 1  unseen items, in the ranker's own order — always first
//	tier 2  already-watched items, appended ONLY to backfill the page the
//	        caller asked for, ordered by merit plus how long ago the user
//	        watched them
//
// A page with enough unseen content never sees tier 2 at all, so the 12h
// no-repeat guarantee is unchanged for a healthy catalog. An exhausted one
// degrades into re-watches at the tail instead of into a short page.
//
// History: this used to cap its own output at seenFilterMinKeep (8) whenever
// the unseen pool fell below 8 — a constant written as a FLOOR but applied as
// a CEILING. A client asking for 30 items got exactly 8, and the ~15 ranked
// items it had just thrown away were the ones the user was waiting to see.
// Because battles outscore shorts in every ranker we have, the surviving 8
// were nearly all battles, and the kind-spacing pass downstream cannot
// interleave a page that contains no shorts — so "the app has no shorts in
// it" was the visible symptom of a ceiling in this file.
// ─────────────────────────────────────────────────────────────────────────────

const (
	seenKeyPrefix = "seen:"        // + userID
	seenTTL       = 12 * time.Hour // window after which content may reappear
	seenMaxSize   = 2000           // hard cap to bound memory per user
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
	// Write the seen members SYNCHRONOUSLY: pagination dedup depends entirely on
	// this set, and the next page (often a prefetch fired immediately) reads it
	// synchronously — if this write were deferred, page 2 could re-serve page 1.
	// The non-correctness maintenance (window trim, size cap, TTL refresh) is
	// pushed to a goroutine so it never adds latency to the feed response.
	_ = rdb.ZAdd(rctx, key, members...).Err()
	if metricSeenMarks != nil {
		metricSeenMarks.WithLabelValues("ok").Add(float64(len(members)))
	}
	go func() {
		cutoff := strconv.FormatInt(now-int64(seenTTL.Seconds()), 10)
		_ = rdb.ZRemRangeByScore(rctx, key, "0", cutoff).Err()
		if n, err := rdb.ZCard(rctx, key).Result(); err == nil && n > seenMaxSize {
			_ = rdb.ZRemRangeByRank(rctx, key, 0, n-seenMaxSize-1).Err()
		}
		_ = rdb.Expire(rctx, key, 2*seenTTL).Err()
	}()
}

// loadSeenSet reads the user's seen ZSET into a map of member → unix
// timestamp of the LAST time we served it.
//
// The timestamp is the point of the map, not incidental: it is what lets the
// re-watch tier prefer something the user saw eleven hours ago over something
// they saw ninety seconds ago. A plain membership set cannot express that, and
// a feed that re-shows in merit order alone replays the same clip every pull.
// Small users stay small; capped users stay capped.
func loadSeenSet(userID string) map[string]int64 {
	out := make(map[string]int64)
	if rdb == nil || userID == "" {
		return out
	}
	members, err := rdb.ZRangeWithScores(rctx, seenKey(userID), 0, -1).Result()
	if err != nil {
		return out
	}
	for _, m := range members {
		name, ok := m.Member.(string)
		if !ok || name == "" {
			continue
		}
		out[name] = int64(m.Score)
	}
	return out
}

// seenBackfillFloor is the page size assumed when a caller does not say how
// many items it wants. It is a floor on the backfill target, never a cap on
// the result: a caller passing want=30 gets up to 30.
const seenBackfillFloor = 8

// seenRepeatMaxBonus is the largest re-rank nudge a re-watch can earn for
// being stale. Sized deliberately below a typical score spread: it reorders
// WITHIN the re-watch tier (where merit alone would replay the same clip on
// every pull) without ever being large enough to matter — the tier is
// appended wholesale after the unseen items regardless of what it contains.
const seenRepeatMaxBonus = 0.5

// seenRepeatBonus scales from 0 for something just watched to
// seenRepeatMaxBonus for something watched a full window ago.
func seenRepeatBonus(lastSeen, now int64) float64 {
	if lastSeen <= 0 || now <= lastSeen {
		return 0
	}
	frac := float64(now-lastSeen) / seenTTL.Seconds()
	if frac > 1 {
		frac = 1
	}
	return seenRepeatMaxBonus * frac
}

// splitSeenScored partitions items into unseen and already-watched, each
// keeping the caller's order. Items with no resolvable id (suggested-account
// cards and similar non-content entries) count as unseen: they can be neither
// marked nor deduped, and dropping them would silently delete them from every
// returning user's feed.
func splitSeenScored(items []ScoredItem, seen map[string]int64) (unseen, repeats []ScoredItem) {
	unseen = make([]ScoredItem, 0, len(items))
	repeats = make([]ScoredItem, 0, len(items))
	for _, si := range items {
		id := getItemID(si.Item)
		if id == "" {
			unseen = append(unseen, si)
			continue
		}
		if _, ok := seen[seenMember(si.Item.Type, id)]; ok {
			repeats = append(repeats, si)
		} else {
			unseen = append(unseen, si)
		}
	}
	return unseen, repeats
}

// filterUnseenScored is the ScoredItem variant used after ranking. `want` is
// the page size the handler is about to serve; pass the request's limit.
func filterUnseenScored(userID string, items []ScoredItem, want int) []ScoredItem {
	return filterUnseenScoredWith(items, loadSeenSet(userID), want)
}

// filterUnseenScoredWith is filterUnseenScored with a caller-supplied `seen`
// snapshot (the map loadSeenSet returns, keyed by seenMember). This lets a
// single feed request load seen:{user} ONCE and thread it into both this and
// applyBootstrapMixIfCold, eliminating a redundant ZRANGE (up to seenMaxSize
// members) on every cold feed request. A nil/empty map means "nothing seen"
// (fail-open), matching loadSeenSet's behaviour on rdb==nil / ZRANGE error.
//
//	want > len(unseen)  → unseen, then re-watches ranked by score + staleness,
//	                      appended until the page is `want` long
//	want <= len(unseen) → unseen only; no re-watch is served
func filterUnseenScoredWith(items []ScoredItem, seen map[string]int64, want int) []ScoredItem {
	if len(seen) == 0 {
		return items
	}
	if want <= 0 {
		want = seenBackfillFloor
	}
	unseen, repeats := splitSeenScored(items, seen)

	// Healthy case: enough unseen items to fill the page the caller asked
	// for. Nothing already watched is served — the 12h guarantee holds.
	if len(unseen) >= want {
		if metricSeenFiltered != nil && len(repeats) > 0 {
			metricSeenFiltered.Add(float64(len(repeats)))
		}
		return unseen
	}

	// Catalog exhausted for this user. Rank the re-watch tier by merit plus
	// how long ago they watched it, so the pull that follows a pull does not
	// replay the same clip in the same order.
	now := time.Now().Unix()
	rank := func(si ScoredItem) float64 {
		id := getItemID(si.Item)
		if id == "" {
			return si.Score
		}
		return si.Score + seenRepeatBonus(seen[seenMember(si.Item.Type, id)], now)
	}
	sort.SliceStable(repeats, func(i, j int) bool { return rank(repeats[i]) > rank(repeats[j]) })

	need := want - len(unseen)
	if need > len(repeats) {
		need = len(repeats)
	}
	out := make([]ScoredItem, 0, len(unseen)+need)
	out = append(out, unseen...)
	out = append(out, repeats[:need]...)
	if metricSeenFiltered != nil {
		if dropped := len(repeats) - need; dropped > 0 {
			metricSeenFiltered.Add(float64(dropped))
		}
	}
	return out
}

// sinkSeenItems reorders — never drops — so unseen items lead in the caller's
// existing order and already-watched ones follow, longest-ago-watched first.
//
// This is the unscored counterpart of the two-tier rule above, for surfaces
// whose contract is an order rather than a ranking (Following is
// chronological). The longest-ago-first tail is also what makes a pull feel
// alive on those surfaces: it changes on its own as the user watches things,
// which is why refresh no longer needs to delete the seen set to produce
// movement.
func sinkSeenItems(items []HomeFeedItem, seen map[string]int64) []HomeFeedItem {
	if len(items) < 2 || len(seen) == 0 {
		return items
	}
	unseen, repeats := splitSeen(items, seen)
	if len(repeats) == 0 {
		return items
	}
	return append(unseen, repeats...)
}

// splitSeen is splitSeenScored for plain items, with the repeats already
// ordered longest-ago-watched first.
func splitSeen(items []HomeFeedItem, seen map[string]int64) (unseen, repeats []HomeFeedItem) {
	type stamped struct {
		item HomeFeedItem
		at   int64
	}
	unseen = make([]HomeFeedItem, 0, len(items))
	watched := make([]stamped, 0, len(items))
	for _, it := range items {
		id := getItemID(it)
		if id == "" {
			unseen = append(unseen, it)
			continue
		}
		if at, ok := seen[seenMember(it.Type, id)]; ok {
			watched = append(watched, stamped{item: it, at: at})
		} else {
			unseen = append(unseen, it)
		}
	}
	sort.SliceStable(watched, func(i, j int) bool { return watched[i].at < watched[j].at })
	repeats = make([]HomeFeedItem, 0, len(watched))
	for _, w := range watched {
		repeats = append(repeats, w.item)
	}
	return unseen, repeats
}

// filterUnseen is the plain-item flavour of the two-tier rule: unseen first,
// then just enough longest-ago-watched items to reach `want`.
func filterUnseen(userID string, items []HomeFeedItem, want int) []HomeFeedItem {
	seen := loadSeenSet(userID)
	if len(seen) == 0 {
		return items
	}
	if want <= 0 {
		want = seenBackfillFloor
	}
	unseen, repeats := splitSeen(items, seen)
	if len(unseen) >= want {
		if metricSeenFiltered != nil && len(repeats) > 0 {
			metricSeenFiltered.Add(float64(len(repeats)))
		}
		return unseen
	}
	need := want - len(unseen)
	if need > len(repeats) {
		need = len(repeats)
	}
	if metricSeenFiltered != nil {
		if dropped := len(repeats) - need; dropped > 0 {
			metricSeenFiltered.Add(float64(dropped))
		}
	}
	return append(unseen, repeats[:need]...)
}
