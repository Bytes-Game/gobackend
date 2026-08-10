package main

import (
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestSeenFilter_EmptyUserIsNoop(t *testing.T) {
	items := []HomeFeedItem{{Type: "post", Post: &Post{ID: "p1"}}}
	out := filterUnseen("", items, 10)
	if len(out) != len(items) {
		t.Fatalf("empty userID should return items as-is, got %d", len(out))
	}
}

func TestSeenFilter_DropsAlreadySeen(t *testing.T) {
	resetRedis(t)
	u := "useen1"
	items := []HomeFeedItem{
		{Type: "post", Post: &Post{ID: "pA"}},
		{Type: "post", Post: &Post{ID: "pB"}},
		{Type: "post", Post: &Post{ID: "pC"}},
	}
	// Mark pA and pC as seen.
	markShown(u, "post", "pA")
	markShown(u, "post", "pC")

	// want=1: the single unseen item already fills the page, so nothing
	// already-watched is served.
	out := filterUnseen(u, items, 1)
	if len(out) != 1 {
		t.Fatalf("expected 1 unseen item, got %d", len(out))
	}
	if getItemID(out[0]) != "pB" {
		t.Fatalf("expected pB to survive, got %q", getItemID(out[0]))
	}
}

func TestSeenFilter_ScoredVariant_EnoughUnseen_ServesNoRepeats(t *testing.T) {
	// The page the caller asked for can be filled from unseen content alone,
	// so the re-watch tier is never reached and the 12h guarantee holds.
	resetRedis(t)
	u := "useen2"
	items := make([]ScoredItem, 0, 10)
	for i := 0; i < 10; i++ {
		items = append(items, ScoredItem{
			Item:  HomeFeedItem{Type: "post", Post: &Post{ID: "p" + string(rune('A'+i))}},
			Score: float64(10 - i),
		})
	}
	// Mark the first item as seen. 9 unseen remain.
	markShown(u, "post", "pA")

	out := filterUnseenScored(u, items, 9)
	if len(out) != 9 {
		t.Fatalf("expected the 9 unseen items, got %d", len(out))
	}
	for _, si := range out {
		if getItemID(si.Item) == "pA" {
			t.Fatalf("seen item pA leaked into a page that unseen content could fill")
		}
	}
}

func TestSeenFilter_ScoredVariant_BackfillsToRequestedLimit(t *testing.T) {
	// THE REGRESSION THIS FILE EXISTS FOR.
	//
	// The old implementation capped its own output at seenFilterMinKeep (8)
	// whenever the unseen pool fell below 8, ignoring the page size the
	// caller asked for. A client requesting 30 got exactly 8 and the ~15
	// ranked items in between were discarded. Backfill must reach `want`.
	resetRedis(t)
	u := "useen2c"
	items := make([]ScoredItem, 0, 23)
	for i := 0; i < 23; i++ {
		id := "q" + string(rune('A'+i))
		items = append(items, ScoredItem{
			Item:  HomeFeedItem{Type: "post", Post: &Post{ID: id}},
			Score: float64(23 - i),
		})
	}
	// Everything except the last two has been watched: 2 unseen, 21 repeats.
	for i := 0; i < 21; i++ {
		markShown(u, "post", "q"+string(rune('A'+i)))
	}

	out := filterUnseenScored(u, items, 30)
	if len(out) != 23 {
		t.Fatalf("want=30 with 23 candidates should serve all 23, got %d", len(out))
	}
	// The two unseen items must still lead.
	for i := 0; i < 2; i++ {
		if id := getItemID(out[i].Item); id != "qV" && id != "qW" {
			t.Fatalf("slot %d should hold an unseen item, got %q", i, id)
		}
	}
}

func TestSeenFilter_ScoredVariant_BackfillStopsAtWant(t *testing.T) {
	// Backfill fills the page and then stops — `want` is a target, not an
	// invitation to serve the whole re-watch pile.
	resetRedis(t)
	u := "useen2d"
	items := make([]ScoredItem, 0, 12)
	for i := 0; i < 12; i++ {
		id := "r" + string(rune('A'+i))
		items = append(items, ScoredItem{
			Item:  HomeFeedItem{Type: "post", Post: &Post{ID: id}},
			Score: float64(12 - i),
		})
		markShown(u, "post", id)
	}

	out := filterUnseenScored(u, items, 5)
	if len(out) != 5 {
		t.Fatalf("want=5 from an all-seen pool should serve exactly 5, got %d", len(out))
	}
}

func TestSeenFilter_ScoredVariant_UnseenLeadsRegardlessOfScore(t *testing.T) {
	// A low-scoring unseen item still outranks a high-scoring re-watch:
	// the tiers are concatenated, not merged. This is what "already seen
	// things come only when new content runs out" means positionally.
	resetRedis(t)
	u := "useen2b"
	items := []ScoredItem{
		{Item: HomeFeedItem{Type: "post", Post: &Post{ID: "p1"}}, Score: 5},
		{Item: HomeFeedItem{Type: "post", Post: &Post{ID: "p2"}}, Score: 4},
	}
	markShown(u, "post", "p1")

	out := filterUnseenScored(u, items, 8)
	if len(out) != 2 {
		t.Fatalf("backfill should keep both items, got %d", len(out))
	}
	if getItemID(out[0].Item) != "p2" {
		t.Fatalf("unseen item p2 should lead despite the lower score, got %q", getItemID(out[0].Item))
	}
}

func TestSeenFilter_ScoredVariant_IdlessItemsAreNeverDropped(t *testing.T) {
	// Suggested-account cards and similar non-content entries carry no id.
	// They can be neither marked nor deduped, so they must pass through:
	// treating them as "seen" would delete them from every returning user's
	// feed the moment that user had any watch history at all.
	resetRedis(t)
	u := "useen2e"
	markShown(u, "post", "anything")
	items := []ScoredItem{
		{Item: HomeFeedItem{Type: "suggestedAccounts"}, Score: 1},
	}
	out := filterUnseenScored(u, items, 8)
	if len(out) != 1 {
		t.Fatalf("id-less item should survive the seen filter, got %d items", len(out))
	}
}

func TestSeenFilter_RepeatTierPrefersLongestAgoWatched(t *testing.T) {
	// Within the re-watch tier, staleness reorders near-ties so a pull that
	// follows a pull doesn't replay the clip the user just finished. `stale`
	// is scored BELOW `fresh`, so merit alone would put fresh first.
	resetRedis(t)
	u := "useen4"
	now := time.Now().Unix()
	key := seenKey(u)
	if err := rdb.ZAdd(rctx, key,
		redis.Z{Score: float64(now - int64(11*time.Hour/time.Second)), Member: seenMember("post", "stale")},
		redis.Z{Score: float64(now - 30), Member: seenMember("post", "fresh")},
	).Err(); err != nil {
		t.Fatalf("seeding seen set: %v", err)
	}

	items := []ScoredItem{
		{Item: HomeFeedItem{Type: "post", Post: &Post{ID: "fresh"}}, Score: 1.0},
		{Item: HomeFeedItem{Type: "post", Post: &Post{ID: "stale"}}, Score: 0.8},
	}
	out := filterUnseenScored(u, items, 8)
	if len(out) != 2 {
		t.Fatalf("expected both re-watches, got %d", len(out))
	}
	if getItemID(out[0].Item) != "stale" {
		t.Fatalf("longest-ago-watched item should lead the re-watch tier, got %q", getItemID(out[0].Item))
	}
}

func TestSinkSeenItems_OrdersWatchedTailOldestFirst(t *testing.T) {
	// The Following tab's contract: nothing is dropped, unseen keeps its
	// chronological order, and the watched tail is longest-ago-first — which
	// is what makes a pull move on a feed the user has fully caught up on.
	resetRedis(t)
	u := "useen5"
	now := time.Now().Unix()
	if err := rdb.ZAdd(rctx, seenKey(u),
		redis.Z{Score: float64(now - 3600), Member: seenMember("challenge", "old")},
		redis.Z{Score: float64(now - 60), Member: seenMember("challenge", "recent")},
	).Err(); err != nil {
		t.Fatalf("seeding seen set: %v", err)
	}

	items := []HomeFeedItem{
		{Type: "challenge", Challenge: &Challenge{ID: "recent"}},
		{Type: "challenge", Challenge: &Challenge{ID: "new1"}},
		{Type: "challenge", Challenge: &Challenge{ID: "old"}},
		{Type: "challenge", Challenge: &Challenge{ID: "new2"}},
	}
	out := sinkSeenItems(items, loadSeenSet(u))
	if len(out) != len(items) {
		t.Fatalf("sink must not drop anything: got %d of %d", len(out), len(items))
	}
	got := []string{getItemID(out[0]), getItemID(out[1]), getItemID(out[2]), getItemID(out[3])}
	want := []string{"new1", "new2", "old", "recent"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("slot %d: want %q, got %q (full order %v)", i, want[i], got[i], got)
		}
	}
}

func TestSeenFilter_BatchMark(t *testing.T) {
	resetRedis(t)
	u := "useen3"
	items := []HomeFeedItem{
		{Type: "post", Post: &Post{ID: "b1"}},
		{Type: "post", Post: &Post{ID: "b2"}},
	}
	markShownBatch(u, items)
	seen := loadSeenSet(u)
	for _, m := range []string{seenMember("post", "b1"), seenMember("post", "b2")} {
		at, ok := seen[m]
		if !ok {
			t.Fatalf("batch mark should have recorded %q, got %v", m, seen)
		}
		if at <= 0 {
			t.Fatalf("batch mark should stamp a real timestamp on %q, got %d", m, at)
		}
	}
}

func TestApplyRefreshSignal_KeepsWatchHistory(t *testing.T) {
	// A refresh must not declare the user's watch history unseen. If it did,
	// the ranking pass that follows would be free to re-serve, at the head of
	// the feed, the clips the user just finished watching — which is the
	// opposite of what pulling to refresh asks for.
	resetRedis(t)
	u := "useen6"
	markShown(u, "post", "justWatched")

	applyRefreshSignal(u, "sess-1")

	seen := loadSeenSet(u)
	if _, ok := seen[seenMember("post", "justWatched")]; !ok {
		t.Fatalf("refresh must preserve the seen set, got %v", seen)
	}
}
