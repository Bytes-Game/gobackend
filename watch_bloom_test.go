package main

import (
	"fmt"
	"math"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// The scale path. Storing every watched video id exactly is 2 MB per heavy
// viewer, which is 9.5 TB at five million of them — so this stores bitmaps
// instead, about 26 KB for a whole viewer's ninety days.
//
// The trade is that it can say "probably watched" about something nobody
// watched. That is only tolerable because the answer feeds a HANDICAP and not
// a filter: a false yes costs one video a nudge down one ranking. If it were
// removing content, a couple of videos in every hundred would silently vanish
// from the catalogue and nobody would ever learn which.

func withRedis(t *testing.T) func() {
	t.Helper()
	addr := os.Getenv("TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("no TEST_REDIS_ADDR — skipping the Redis-backed checks")
	}
	prev := rdb
	rdb = redis.NewClient(&redis.Options{Addr: addr})
	if err := rdb.Ping(rctx).Err(); err != nil {
		rdb = prev
		t.Skipf("redis at %s not reachable: %v", addr, err)
	}
	rdb.FlushDB(rctx)
	return func() { rdb.FlushDB(rctx); _ = rdb.Close(); rdb = prev }
}

// ── The bit layout has to match what Redis writes ───────────────────────────

func TestBloom_ReadsBackWhatRedisWrote(t *testing.T) {
	// The single most breakable thing here. noteWatched sets bits with Redis
	// SETBIT, and the lookup reads the raw string and indexes into the bytes
	// itself. Redis numbers bits from the TOP of each byte, so getting that
	// backwards produces a memory that silently remembers nothing — every
	// lookup misses, no error anywhere, and the only symptom is repeats
	// coming back months later.
	defer withRedis(t)()

	noteWatched("u1", "challenge:v1")
	noteWatched("u1", "challenge:v2")

	h := buildWatchHistory("u1")
	if len(h.buckets) == 0 {
		t.Fatal("nothing came back from Redis after writing two videos")
	}
	for _, key := range []string{"challenge:v1", "challenge:v2"} {
		if !h.seen(key) {
			t.Errorf("%s was written and then not found — the bit order the "+
				"reader uses does not match the one Redis writes", key)
		}
	}
}

func TestBloom_DoesNotClaimEverything(t *testing.T) {
	// The other half: a bitmap that answers yes to everything would suppress
	// the entire catalogue, which looks like "the feed stopped showing me
	// anything new".
	defer withRedis(t)()
	noteWatched("u1", "challenge:v1")

	h := buildWatchHistory("u1")
	claimed := 0
	const probes = 2000
	for i := 0; i < probes; i++ {
		if h.seen(fmt.Sprintf("challenge:never-%d", i)) {
			claimed++
		}
	}
	// One video in the bucket should give essentially no false positives.
	if claimed > probes/100 {
		t.Errorf("claimed %d of %d unwatched videos as watched", claimed, probes)
	}
}

func TestBloom_ErrorRateStaysUsableAtDesignLoad(t *testing.T) {
	// Sized for two thousand videos in a week. Past a few percent the handicap
	// starts landing on content nobody watched often enough to be noticeable
	// in the ranking, so this pins the actual measured rate rather than the
	// arithmetic one.
	defer withRedis(t)()

	const load = 2000
	for i := 0; i < load; i++ {
		noteWatched("u1", fmt.Sprintf("challenge:seen-%d", i))
	}
	h := buildWatchHistory("u1")

	// Everything written must still be found. A bitmap can never forget.
	for i := 0; i < load; i += 97 {
		if !h.seen(fmt.Sprintf("challenge:seen-%d", i)) {
			t.Fatalf("seen-%d was written and then forgotten — a bitmap of this "+
				"kind cannot produce a false NO, so this is a real bug", i)
		}
	}

	false_ := 0
	const probes = 5000
	for i := 0; i < probes; i++ {
		if h.seen(fmt.Sprintf("challenge:fresh-%d", i)) {
			false_++
		}
	}
	rate := float64(false_) / probes
	if rate > 0.05 {
		t.Errorf("at its design load of %d videos a week the error rate is "+
			"%.1f%%, past the ~2%% it is sized for", load, rate*100)
	}
	t.Logf("measured error rate at design load: %.2f%% (%d of %d)",
		rate*100, false_, probes)
}

// ── Age, which is what the handicap decays against ──────────────────────────

func TestBloom_RecentWatchIsSuppressedHardest(t *testing.T) {
	defer withRedis(t)()

	// This week, and eight weeks ago, written directly into their buckets.
	now := time.Now()
	recent := watchBucketKey("u1", watchBucketIndex(now))
	old := watchBucketKey("u1", watchBucketIndex(now)-8)
	for _, b := range watchBitPositions("challenge:recent") {
		rdb.SetBit(rctx, recent, b, 1)
	}
	for _, b := range watchBitPositions("challenge:old") {
		rdb.SetBit(rctx, old, b, 1)
	}

	h := buildWatchHistory("u1")
	nowUnix := time.Now().Unix()
	r := h.suppression("challenge:recent", nowUnix)
	o := h.suppression("challenge:old", nowUnix)

	if r <= o {
		t.Errorf("this week scored %v and eight weeks ago %v — the handicap is "+
			"not fading with age", r, o)
	}
	if o <= 0 {
		t.Errorf("something watched eight weeks ago carries no handicap at all")
	}
	if r > rewatchPenaltyMax {
		t.Errorf("handicap %v exceeded its ceiling %v", r, rewatchPenaltyMax)
	}
}

func TestBloom_MemorySizeIsWhatWeClaim(t *testing.T) {
	// The number this whole design exists for. If the buckets grow, the
	// storage estimate that justified bitmaps over exact ids stops holding.
	perBucket := watchBucketBits / 8
	perViewer := perBucket * watchBucketWeeks
	if perViewer > 64*1024 {
		t.Errorf("a viewer's full memory is %d KB; at five million viewers "+
			"that is %.0f GB and the exact-storage alternative starts looking "+
			"comparable", perViewer/1024,
			float64(perViewer)*5e6/(1024*1024*1024))
	}
	t.Logf("per viewer: %d KB (%d weeks x %d bytes) — %.0f GB at 5M viewers",
		perViewer/1024, watchBucketWeeks, perBucket,
		float64(perViewer)*5e6/(1024*1024*1024))
}

// ── What is worth a ninety-day memory ───────────────────────────────────────

func TestWorthRemembering_DeliberateActionsAndRefusals(t *testing.T) {
	for _, ev := range []string{"complete", "like", "share", "save", "rewatch",
		"comment", "skip", "not_interested"} {
		e := FeedEvent{UserID: "u1", ContentID: "1", ContentType: "challenge", EventType: ev}
		if !watchWorthRemembering(e) {
			t.Errorf("%q was not remembered; finishing something and refusing "+
				"it are opposite feelings with the same conclusion — do not "+
				"bring it back", ev)
		}
	}
}

func TestWorthRemembering_AGlimpseIsNotAViewing(t *testing.T) {
	// An impression, or a video scrolled past in a second, must not earn a
	// three-month handicap. The twelve-hour seen-set already stops those
	// returning immediately; giving them ninety days would quietly shrink the
	// catalogue for content nobody actually watched.
	base := FeedEvent{UserID: "u1", ContentID: "1", ContentType: "challenge"}

	e := base
	e.EventType = "impression"
	if watchWorthRemembering(e) {
		t.Error("a bare impression earned a ninety-day memory")
	}

	e = base
	e.EventType = "view"
	e.CompletionRate = 0.05
	if watchWorthRemembering(e) {
		t.Error("a video watched for five percent earned a ninety-day memory")
	}

	e.CompletionRate = 0.8
	if !watchWorthRemembering(e) {
		t.Error("a video watched most of the way through was not remembered")
	}
}

func TestWorthRemembering_NeedsSomethingToRemember(t *testing.T) {
	if watchWorthRemembering(FeedEvent{EventType: "complete"}) {
		t.Error("an event with no user and no content was remembered")
	}
}

// ── Degrading without Redis ─────────────────────────────────────────────────

func TestBloom_NoRedisIsSilent(t *testing.T) {
	// rdb is nil under plain `go test`. Writing must not panic, and reading
	// must produce an empty memory rather than an error — the Postgres
	// fallback picks it up from there.
	prev := rdb
	rdb = nil
	defer func() { rdb = prev }()

	noteWatched("u1", "challenge:v1")
	if got := loadWatchBuckets("u1"); got != nil {
		t.Errorf("got %v with no Redis", got)
	}
}

func TestBitPositions_AreSpreadAndStable(t *testing.T) {
	// Stable, or a video moves between requests and the memory forgets it.
	a := watchBitPositions("challenge:v1")
	b := watchBitPositions("challenge:v1")
	if a != b {
		t.Fatalf("the same video hashed to %v then %v", a, b)
	}
	// Spread, or the six positions collapse onto each other and the error
	// rate is far worse than the arithmetic says.
	seen := map[int64]bool{}
	for _, p := range a {
		if p < 0 || p >= watchBucketBits {
			t.Errorf("position %d is outside the bitmap", p)
		}
		seen[p] = true
	}
	if len(seen) < watchBucketHashes-1 {
		t.Errorf("six hashes produced only %d distinct positions: %v", len(seen), a)
	}
	// Different videos land differently.
	if watchBitPositions("challenge:v1") == watchBitPositions("challenge:v2") {
		t.Error("two different videos hashed identically")
	}
}

func TestBloom_SizingMatchesTheArithmetic(t *testing.T) {
	// Guards the constants against each other. The optimal hash count for a
	// bitmap of m bits holding n items is (m/n)·ln2; drifting far from it
	// wastes either space or accuracy.
	const designLoad = 2000
	ideal := (float64(watchBucketBits) / designLoad) * math.Ln2
	if math.Abs(ideal-watchBucketHashes) > 2 {
		t.Errorf("%d hashes for %d bits at a design load of %d; the optimum is "+
			"%.1f", watchBucketHashes, watchBucketBits, designLoad, ideal)
	}
}
