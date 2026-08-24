package main

// watch_bloom.go — the long watch memory, sized for millions of people.
//
// ════════════════════════════════════════════════════════════════════════════
// WHY NOT JUST COPY THE TWELVE-HOUR LIST
// ════════════════════════════════════════════════════════════════════════════
//
// Because storing every video id somebody has watched does not fit. A heavy
// viewer gets through roughly 35,000 videos in ninety days; as Redis set
// members that is about 2 MB each.
//
//	     1 million viewers →  1.9 TB
//	     5 million viewers →  9.5 TB
//
// That is not a tuning problem, it is the wrong shape. The seen-set in
// seen_filter.go can afford to be exact because it caps at 2,000 entries and
// twelve hours; this one has to reach back months.
//
// So it does not store ids. It stores a bitmap that answers "have they
// probably watched this", in about 2 KB per week of viewing:
//
//	     5 million viewers → roughly 100-200 GB, depending on how heavy they are
//
// Two thousand videos a week fit in a bucket at a two percent error rate.
// Somebody who watches far more than that gets a higher error rate rather
// than a wrong answer of the other kind — see below for why that direction is
// the safe one.
//
// ════════════════════════════════════════════════════════════════════════════
// "PROBABLY" IS SAFE HERE, AND IT WOULD NOT BE IN A FILTER
// ════════════════════════════════════════════════════════════════════════════
//
// A structure like this can say yes to something it has never been told
// about. It can never say no to something it has.
//
// If watched content were being REMOVED from the feed, that would be
// unacceptable: a couple of videos in every hundred would silently vanish
// from the catalogue forever, and nobody would ever find out which.
//
// It is not removed. It is demoted, by an amount that fades with age
// (watch_history.go). So a false yes costs one video a small nudge down the
// ranking on one request. It still competes, it still appears, and the next
// request may well hash differently against a different bucket.
//
// The decision to penalise instead of filter was made for a different reason
// entirely — three earlier attempts at filtering emptied the feed, and the
// list is in seen_filter.go. It is what makes this affordable at scale.
//
// ════════════════════════════════════════════════════════════════════════════
// WEEK BUCKETS, BECAUSE THE HANDICAP FADES
// ════════════════════════════════════════════════════════════════════════════
//
// One bitmap per week, thirteen of them for the ninety-day window. A lookup
// walks them newest-first and the first hit gives the age, to the nearest
// week, which is all a curve that decays over three months needs.
//
// It also makes expiry free: an old week is a key with a TTL on it, not a
// scan for entries to delete.

import (
	"hash/fnv"
	"strconv"
	"time"
)

const (
	// One bucket per week; thirteen covers the ninety-day window that
	// watch_history.go decays across.
	watchBucketWeeks = 13

	// Bits per bucket, and how many hash positions each video sets.
	//
	// Sized for two thousand videos in a week at about a two percent error
	// rate: 16384 bits is 2 KB, and 6 positions is near the optimum for that
	// ratio. Somebody who watches more than that in a week pushes their own
	// error rate up and nobody else's, because the bucket is per viewer.
	watchBucketBits   = 16384
	watchBucketHashes = 6

	watchBucketPrefix = "wb:" // + userID + ":" + week index
)

// watchBucketIndex is the week number a moment falls in. Absolute rather than
// relative, so a bucket written today is still found tomorrow.
func watchBucketIndex(t time.Time) int64 {
	return t.Unix() / int64((7 * 24 * time.Hour).Seconds())
}

func watchBucketKey(userID string, week int64) string {
	return watchBucketPrefix + userID + ":" + strconv.FormatInt(week, 10)
}

// watchBitPositions is where one video lands in a bucket.
//
// Two hashes generate all six positions (h1 + i*h2), which is a standard and
// well-behaved substitute for six independent hash functions and costs one
// pass over the string instead of six.
func watchBitPositions(contentKey string) [watchBucketHashes]int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(contentKey))
	sum := h.Sum64()
	h1 := int64(sum & 0x7fffffff)
	h2 := int64((sum >> 32) & 0x7fffffff)
	if h2%2 == 0 {
		h2++ // odd stride visits more of the bitmap before repeating
	}
	var out [watchBucketHashes]int64
	for i := range out {
		out[i] = (h1 + int64(i)*h2) % watchBucketBits
	}
	return out
}

// noteWatched records that somebody watched something, now.
//
// Never returns an error and never blocks anything: losing one record costs a
// single video a slightly better ranking than it deserved, months from now.
func noteWatched(userID, contentKey string) {
	if rdb == nil || userID == "" || contentKey == "" {
		return
	}
	key := watchBucketKey(userID, watchBucketIndex(time.Now()))
	pipe := rdb.Pipeline()
	for _, bit := range watchBitPositions(contentKey) {
		pipe.SetBit(rctx, key, bit, 1)
	}
	// Expire a little past the window so the oldest bucket is still readable
	// right up to the moment it stops mattering.
	pipe.Expire(rctx, key, time.Duration(watchMemoryDays+7)*24*time.Hour)
	_, _ = pipe.Exec(rctx)
}

// watchedWeeksAgo answers, for a batch of candidates, how long ago each was
// probably watched — in weeks, or -1 for "no memory of this".
//
// Batched on purpose. Scoring a page asks about a hundred or so videos, and
// asking Redis about them one at a time would be a hundred round trips per
// feed request. This reads the thirteen buckets once and answers every
// candidate from them in process.
func watchedWeeksAgo(userID string, contentKeys []string) map[string]int {
	out := make(map[string]int, len(contentKeys))
	if rdb == nil || userID == "" || len(contentKeys) == 0 {
		return out
	}

	// Pull the buckets newest-first, as raw bitmaps.
	now := watchBucketIndex(time.Now())
	keys := make([]string, 0, watchBucketWeeks)
	for w := 0; w < watchBucketWeeks; w++ {
		keys = append(keys, watchBucketKey(userID, now-int64(w)))
	}
	vals, err := rdb.MGet(rctx, keys...).Result()
	if err != nil {
		return out
	}

	buckets := make([][]byte, len(vals))
	for i, v := range vals {
		if s, ok := v.(string); ok {
			buckets[i] = []byte(s)
		}
	}

	for _, ck := range contentKeys {
		pos := watchBitPositions(ck)
		for w, b := range buckets {
			if len(b) == 0 {
				continue
			}
			if bloomHas(b, pos) {
				out[ck] = w // w weeks ago; 0 is this week
				break       // newest hit wins — that is the age we want
			}
		}
	}
	return out
}

// bloomHas reports whether every position is set. Absent bytes count as zero,
// which is what a bucket shorter than the full bitmap means: Redis only
// allocates up to the highest bit actually set.
func bloomHas(bitmap []byte, positions [watchBucketHashes]int64) bool {
	for _, p := range positions {
		idx := p / 8
		if idx >= int64(len(bitmap)) {
			return false
		}
		// Redis SETBIT numbers bits from the most significant end of each
		// byte, so bit 0 is 0x80. Matching that is what lets this read a
		// bitmap Redis wrote.
		if bitmap[idx]&(1<<(7-uint(p%8))) == 0 {
			return false
		}
	}
	return true
}

// watchWorthRemembering decides whether an event earns a ninety-day memory.
//
// The line is "did they actually consume it", not "did it appear". Doing
// something deliberate — finishing, liking, sharing, saving, rewatching,
// commenting — is unambiguous. So is saying no: a skip or a not-interested
// means bringing it back in a month would be a mistake, which is the same
// conclusion for the opposite reason.
//
// A plain view counts only if enough of it was watched. Below that it is
// closer to a scroll-past than a viewing, and the twelve-hour seen-set
// already keeps it from returning immediately.
//
// Impressions never count. They never even reach here — recordFeedEvent
// routes them to the aggregator first — but the rule is written down anyway,
// because "it appeared on screen" earning a three-month handicap is exactly
// the mistake that would quietly shrink a catalogue.
func watchWorthRemembering(event FeedEvent) bool {
	if event.UserID == "" || event.ContentID == "" {
		return false
	}
	switch event.EventType {
	case "complete", "rewatch", "loop", "like", "save", "share", "comment",
		"skip", "not_interested":
		return true
	case "view":
		return event.CompletionRate >= watchViewThreshold
	default:
		return false
	}
}

// watchViewThreshold is how much of a video has to be watched before a plain
// view is remembered for months. Matches the completion floor the rest of the
// ranker treats as a real view rather than a scroll-past.
const watchViewThreshold = 0.3

// loadWatchBuckets pulls a viewer's weekly bitmaps, newest week first.
//
// One MGET for the whole ninety days. Thirteen keys of about 2 KB is a small
// enough read to do on every feed request, which is the entire point — the
// alternative it replaced was a Postgres scan over the viewer's whole history.
//
// Returns nil when there is nothing at all, which is what tells
// buildWatchHistory to fall back to the database.
func loadWatchBuckets(userID string) [][]byte {
	if rdb == nil || userID == "" {
		return nil
	}
	now := watchBucketIndex(time.Now())
	keys := make([]string, 0, watchBucketWeeks)
	for w := 0; w < watchBucketWeeks; w++ {
		keys = append(keys, watchBucketKey(userID, now-int64(w)))
	}
	vals, err := rdb.MGet(rctx, keys...).Result()
	if err != nil {
		return nil
	}
	buckets := make([][]byte, len(vals))
	any := false
	for i, v := range vals {
		if s, ok := v.(string); ok && s != "" {
			buckets[i] = []byte(s)
			any = true
		}
	}
	if !any {
		return nil
	}
	return buckets
}
