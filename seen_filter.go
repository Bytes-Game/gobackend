package main

import (
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// ─────────────────────────────────────────────────────────────────────────────
// SEEN-CONTENT SIGNAL
//
// Whether a user has already watched something is a RANKING FEATURE, not a
// gate. This file owns the impression record (a bounded Redis sorted set per
// user, keyed by unix-ts so old entries evict cheaply) and turns it into a
// score penalty. It removes nothing, truncates nothing, and never reports
// that a feed has run out.
//
// WHY A PENALTY AND NOT A FILTER
//
// Deciding what to withhold is not the ranker's job. Its job is to order
// everything it was given; the client decides when to stop asking. Any rule
// of the form "hold this back until X" has to answer "and what if X never
// happens" — and every answer to that question so far has been a bug:
//
//	1. drop everything seen                → feed empties on a small catalog
//	2. drop seen, floor the page at 8      → a request for 30 returned 8, and
//	                                         because battles outscore shorts
//	                                         the 8 were all battles, so the
//	                                         app looked like it had no shorts
//	3. unseen tier, then a backfill tier   → a genuinely great video the user
//	                                         watched yesterday could never
//	                                         outrank a mediocre one they had
//	                                         not, and a page that filled by
//	                                         backfill reported hasMore=false,
//	                                         which ENDED the feed
//
// Each fix moved the cliff without removing it, because all three kept
// treating "seen" as a category rather than as evidence. As a penalty there
// is no cliff to move: a seen item sits lower than it otherwise would, by an
// amount that decays as the memory ages, and if it is good enough it still
// wins. Exhausting the unseen pool stops being an event the feed can hit.
//
// THE TWO CONSTANTS
//
// seenPenaltyMax is the handicap a just-watched item carries. It is sized to
// be decisive but finite — comfortably larger than the spread between
// typical candidates, so unseen content wins by default, yet small enough
// that an exceptional re-watch can still surface.
//
// seenCooldown answers the one case a smooth penalty handles badly: the item
// two swipes back. Its score is barely stale, so a smooth curve would let it
// return immediately if the alternatives were weak. Inside the cooldown an
// item carries an additional surcharge large enough to sink it beneath
// anything outside the cooldown. Note this is still a score: if EVERY
// candidate is in cooldown, they rank among themselves by staleness and the
// feed keeps serving. Nothing is withheld, so nothing can run out.
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
// The timestamp is the point of the map, not incidental: the penalty decays
// with it, so something watched eleven hours ago is handicapped far less than
// something watched ninety seconds ago. A plain membership set cannot express
// that, and a feed that re-shows in merit order alone replays the same clip
// every pull. Small users stay small; capped users stay capped.
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

// seenPenaltyMax is the handicap a just-watched item carries, decaying to 0
// across seenTTL. Decisive but finite: larger than the spread between typical
// candidates, so unseen content wins by default, yet small enough that an
// exceptional re-watch can still beat a weak unseen one.
const seenPenaltyMax = 0.6

// seenCooldown is the window in which a re-watch would read as a glitch
// rather than a choice — the video from two swipes ago coming straight back.
const seenCooldown = 30 * time.Minute

// seenCooldownSurcharge sinks an item inside the cooldown beneath everything
// outside it. Deliberately far larger than any real score so the ordering is
// unambiguous, and deliberately still a SCORE: when every candidate is in
// cooldown they simply rank among themselves and the feed keeps serving.
const seenCooldownSurcharge = 100.0

// seenPenalty is the score handicap for an item last served at lastSeen.
// Returns 0 only for never-seen (lastSeen <= 0) and for memories older than
// seenTTL — past the window we genuinely do not care.
//
// A future or equal timestamp clamps to age 0, i.e. the FULL handicap. It is
// tempting to write `now <= lastSeen → 0` as clock-skew defence, and that is
// backwards: seconds-per-timestamp granularity means an item served earlier in
// this same second reads as age 0, and that is the single most important item
// to hold back — the one a page-2 prefetch would otherwise serve straight back
// to a user who is still looking at it.
func seenPenalty(lastSeen, now int64) float64 {
	if lastSeen <= 0 {
		return 0
	}
	age := float64(now - lastSeen)
	if age < 0 {
		age = 0
	}
	window := seenTTL.Seconds()
	if age >= window {
		return 0
	}
	// Linear decay: full handicap at age 0, none at the window edge.
	penalty := seenPenaltyMax * (1 - age/window)
	if age < seenCooldown.Seconds() {
		penalty += seenCooldownSurcharge
	}
	return penalty
}

// applySeenPenalty ranks items with prior impressions handicapped rather than
// removed, and returns EVERY item it was given, newly sorted.
//
// The returned slice is a copy; input Scores are not mutated, so a caller that
// stashes scores for LTR sees what the ranker actually computed.
//
// `seen` is the snapshot loadSeenSet returns. A nil/empty map means "nothing
// seen" (fail-open), matching loadSeenSet's behaviour on rdb==nil or a ZRANGE
// error — in that case the caller's order is returned untouched. Items with no
// resolvable id (suggested-account cards) carry no penalty: they can be
// neither marked nor matched, and inventing one for them would push a card the
// user has never seen to the bottom of every page.
func applySeenPenalty(items []ScoredItem, seen map[string]int64) []ScoredItem {
	if len(items) < 2 || len(seen) == 0 {
		return items
	}
	now := time.Now().Unix()
	out := make([]ScoredItem, len(items))
	copy(out, items)

	penalties := make([]float64, len(out))
	handicapped := 0
	for i, si := range out {
		id := getItemID(si.Item)
		if id == "" {
			continue
		}
		p := seenPenalty(seen[seenMember(si.Item.Type, id)], now)
		penalties[i] = p
		if p > 0 {
			handicapped++
		}
	}

	// Rank on score-minus-penalty. Stable, so items with identical adjusted
	// scores keep the caller's order instead of shuffling between requests.
	idx := make([]int, len(out))
	for i := range idx {
		idx[i] = i
	}
	sort.SliceStable(idx, func(a, b int) bool {
		return out[idx[a]].Score-penalties[idx[a]] > out[idx[b]].Score-penalties[idx[b]]
	})
	ranked := make([]ScoredItem, 0, len(out))
	for _, i := range idx {
		ranked = append(ranked, out[i])
	}

	// The metric's meaning is "items the seen signal held back". It used to
	// count items dropped outright; nothing is dropped now, so it counts items
	// that took a handicap.
	if metricSeenFiltered != nil && handicapped > 0 {
		metricSeenFiltered.Add(float64(handicapped))
	}
	return ranked
}

// applySeenPenaltyFor is applySeenPenalty with the snapshot loaded for you.
func applySeenPenaltyFor(userID string, items []ScoredItem) []ScoredItem {
	return applySeenPenalty(items, loadSeenSet(userID))
}

// sinkSeenItems reorders — never drops — so unseen items lead in the caller's
// existing order and already-watched ones follow, longest-ago-watched first.
//
// This is the UNSCORED counterpart, and it stays a strict two-group ordering
// on purpose. Following is chronological: there are no scores to compare, so
// "let a good enough re-watch win" has nothing to evaluate. Newest-to-you
// first, then the rest oldest-watched first, is the whole available signal.
// Nothing is dropped, so this cannot end the feed either.
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

// splitSeen partitions plain items into unseen and already-watched, with the
// repeats ordered longest-ago-watched first. Items with no resolvable id
// count as unseen: they can be neither marked nor matched.
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
