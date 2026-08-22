package main

import (
	"sort"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// seedSeen stamps `member` as last-shown `ago` in the past.
func seedSeen(t *testing.T, userID, contentType, contentID string, ago time.Duration) {
	t.Helper()
	at := time.Now().Add(-ago).Unix()
	err := rdb.ZAdd(rctx, seenKey(userID), redis.Z{
		Score:  float64(at),
		Member: seenMember(contentType, contentID),
	}).Err()
	if err != nil {
		t.Fatalf("seeding seen set: %v", err)
	}
}

func scoredPost(id string, score float64) ScoredItem {
	return ScoredItem{Item: HomeFeedItem{Type: "post", Post: &Post{ID: id}}, Score: score}
}

func rankedIDs(items []ScoredItem) []string {
	out := make([]string, 0, len(items))
	for _, si := range items {
		out = append(out, getItemID(si.Item))
	}
	return out
}

func TestSeenPenalty_NothingIsEverDropped(t *testing.T) {
	// The core contract. Every item handed in comes back out, however long
	// ago — or recently — the user watched it. A ranker that removes things
	// can run out; this one cannot.
	resetRedis(t)
	u := "upen1"
	items := make([]ScoredItem, 0, 12)
	for i := 0; i < 12; i++ {
		id := "p" + string(rune('A'+i))
		items = append(items, scoredPost(id, float64(12-i)))
		markShown(u, "post", id)
	}
	out := applySeenPenaltyFor(u, items)
	if len(out) != len(items) {
		t.Fatalf("all %d items must survive an all-seen pool, got %d", len(items), len(out))
	}
}

func TestSeenPenalty_UnseenBeatsSeenAtSimilarMerit(t *testing.T) {
	// Default behaviour: fresh content wins. The seen item is scored slightly
	// higher, so only the penalty can reorder them.
	resetRedis(t)
	u := "upen2"
	seedSeen(t, u, "post", "watched", 6*time.Hour)
	out := applySeenPenaltyFor(u, []ScoredItem{
		scoredPost("watched", 1.05),
		scoredPost("fresh", 1.00),
	})
	if got := rankedIDs(out); got[0] != "fresh" {
		t.Fatalf("unseen should lead at similar merit, got %v", got)
	}
}

func TestSeenPenalty_StrongSeenItemCanOutrankWeakUnseenItem(t *testing.T) {
	// THE BEHAVIOUR THIS REWRITE EXISTS FOR.
	//
	// The previous design concatenated tiers: unseen block, then seen block.
	// A genuinely great video watched hours ago could never beat a mediocre
	// one the user had not seen, no matter the gap in merit. The penalty is
	// finite, so merit can overcome it.
	resetRedis(t)
	u := "upen3"
	seedSeen(t, u, "post", "banger", 6*time.Hour)
	out := applySeenPenaltyFor(u, []ScoredItem{
		scoredPost("banger", 5.0),
		scoredPost("mediocre", 0.5),
	})
	if got := rankedIDs(out); got[0] != "banger" {
		t.Fatalf("a far better re-watch should be allowed to lead, got %v", got)
	}
}

func TestSeenPenalty_CooldownSinksTheItemFromTwoSwipesAgo(t *testing.T) {
	// The one case a smooth curve handles badly: something served seconds
	// ago is barely stale, so without a cooldown it would return immediately
	// whenever the alternatives were weak. Even a huge merit gap must not
	// bring it straight back.
	resetRedis(t)
	u := "upen4"
	seedSeen(t, u, "post", "justServed", 10*time.Second)
	out := applySeenPenaltyFor(u, []ScoredItem{
		scoredPost("justServed", 50.0),
		scoredPost("other", 0.1),
	})
	if got := rankedIDs(out); got[0] != "other" {
		t.Fatalf("an item inside the cooldown must not lead, got %v", got)
	}
}

func TestSeenPenalty_AllInCooldownStillRanksAndServes(t *testing.T) {
	// The cooldown is a score, not a gate. A user who has just burned through
	// the whole catalog still gets a ranked feed rather than an empty one.
	resetRedis(t)
	u := "upen5"
	seedSeen(t, u, "post", "a", 1*time.Minute)
	seedSeen(t, u, "post", "b", 2*time.Minute)
	items := []ScoredItem{scoredPost("a", 1.0), scoredPost("b", 2.0)}
	out := applySeenPenaltyFor(u, items)
	if len(out) != 2 {
		t.Fatalf("expected both items, got %d", len(out))
	}
	// Both carry the same surcharge, so merit decides between them.
	if got := rankedIDs(out); got[0] != "b" {
		t.Fatalf("within the cooldown, merit should still order items, got %v", got)
	}
}

func TestSeenPenalty_DecaysWithStaleness(t *testing.T) {
	// Two equally-good re-watches: the one watched longer ago should lead.
	resetRedis(t)
	u := "upen6"
	seedSeen(t, u, "post", "stale", 11*time.Hour)
	seedSeen(t, u, "post", "recent", 1*time.Hour)
	out := applySeenPenaltyFor(u, []ScoredItem{
		scoredPost("recent", 1.0),
		scoredPost("stale", 1.0),
	})
	if got := rankedIDs(out); got[0] != "stale" {
		t.Fatalf("longest-ago-watched should lead among equals, got %v", got)
	}
}

func TestSeenPenalty_SameSecondCountsAsJustServed(t *testing.T) {
	// Timestamps are whole seconds, so an item served earlier in THIS second
	// reads as age 0. That is the most important item to hold back — a page-2
	// prefetch fires milliseconds after page 1 — and an earlier version
	// returned 0 penalty for it, letting page 1 leak straight into page 2.
	now := time.Now().Unix()
	if p := seenPenalty(now, now); p <= seenPenaltyMax {
		t.Fatalf("same-second impression must carry the cooldown surcharge, got %f", p)
	}
	// A clock that ran backwards must not become a free pass either.
	if p := seenPenalty(now+5, now); p <= seenPenaltyMax {
		t.Fatalf("future timestamp must clamp to the full handicap, got %f", p)
	}
}

func TestSeenPenalty_PageTwoDoesNotReplayPageOne(t *testing.T) {
	// End-to-end shape of the same bug: mark a page as shown, then re-rank the
	// full pool as page 2 would. The just-served items must be at the bottom.
	resetRedis(t)
	u := "upen9"
	pageOne := []HomeFeedItem{
		{Type: "post", Post: &Post{ID: "hit1"}},
		{Type: "post", Post: &Post{ID: "hit2"}},
	}
	markShownBatch(u, pageOne)

	// The served items carry the HIGHEST merit — only the penalty can move them.
	out := applySeenPenaltyFor(u, []ScoredItem{
		scoredPost("hit1", 9.0),
		scoredPost("hit2", 8.0),
		scoredPost("rest1", 1.0),
		scoredPost("rest2", 0.9),
	})
	got := rankedIDs(out)
	if got[0] != "rest1" || got[1] != "rest2" {
		t.Fatalf("page 2 should lead with unserved content, got %v", got)
	}
	if len(got) != 4 {
		t.Fatalf("page 1's items must still be present, just last: %v", got)
	}
}

func TestSeenPenalty_SurvivesADownstreamResortByScore(t *testing.T) {
	// THE REGRESSION THIS FILE MISSED FOR A RELEASE.
	//
	// TestSeenPenalty_PageTwoDoesNotReplayPageOne above passes whether or not
	// the handicap reaches Score, because it only inspects the slice this
	// function returns. Every stage after it re-sorts by Score — MMR seeds
	// from the top Score in its head window, composeFeed re-buckets by Score
	// — so an ordering that Score does not back is discarded before anything
	// is served. Page 2 then re-ran the same ranking, arrived at the same
	// top-N, the client de-duplicated it to nothing, and the feed ended after
	// two pages.
	//
	// Re-sorting the output by Score here is the cheapest possible stand-in
	// for those stages: if the order survives it, the handicap is real.
	resetRedis(t)
	u := "upen10"
	markShownBatch(u, []HomeFeedItem{
		{Type: "post", Post: &Post{ID: "served1"}},
		{Type: "post", Post: &Post{ID: "served2"}},
	})

	out := applySeenPenaltyFor(u, []ScoredItem{
		scoredPost("served1", 9.0),
		scoredPost("served2", 8.0),
		scoredPost("unserved1", 1.0),
		scoredPost("unserved2", 0.9),
	})

	sort.SliceStable(out, func(i, j int) bool { return out[i].Score > out[j].Score })

	if got := rankedIDs(out); got[0] != "unserved1" || got[1] != "unserved2" {
		t.Fatalf("the handicap did not survive a re-sort by Score, so no "+
			"downstream stage would honour it; got %v", got)
	}
}

func TestSeenPenalty_LeavesTheBreakdownAlone(t *testing.T) {
	// ScoreBreakdown is what ltrStashBreakdownAll learns from. It has to keep
	// describing what the ranker computed — serving decisions like "we showed
	// this an hour ago" are not features of the content.
	resetRedis(t)
	u := "upen11"
	seedSeen(t, u, "post", "watched", 1*time.Hour)
	si := scoredPost("watched", 2.0)
	si.ScoreBreakdown = map[string]float64{"quality": 0.4}
	out := applySeenPenaltyFor(u, []ScoredItem{si, scoredPost("fresh", 1.0)})

	for _, r := range out {
		if getItemID(r.Item) != "watched" {
			continue
		}
		if len(r.ScoreBreakdown) != 1 || r.ScoreBreakdown["quality"] != 0.4 {
			t.Fatalf("breakdown was rewritten: %v", r.ScoreBreakdown)
		}
	}
}

func TestSeenPenalty_ExpiresPastTheWindow(t *testing.T) {
	// Beyond seenTTL we genuinely do not care that the user saw it.
	old := seenPenalty(time.Now().Add(-13*time.Hour).Unix(), time.Now().Unix())
	if old != 0 {
		t.Fatalf("a memory older than seenTTL should carry no penalty, got %f", old)
	}
	never := seenPenalty(0, time.Now().Unix())
	if never != 0 {
		t.Fatalf("never-seen should carry no penalty, got %f", never)
	}
}

func TestSeenPenalty_IdlessItemsAreNotHandicapped(t *testing.T) {
	// Suggested-account cards carry no id, so they can be neither marked nor
	// matched. Inventing a penalty for them would bury a card the user has
	// never seen at the bottom of every page.
	resetRedis(t)
	u := "upen7"
	markShown(u, "post", "anything")
	out := applySeenPenaltyFor(u, []ScoredItem{
		{Item: HomeFeedItem{Type: "suggestedAccounts"}, Score: 1.0},
		scoredPost("other", 0.5),
	})
	if len(out) != 2 {
		t.Fatalf("expected both items, got %d", len(out))
	}
	if getItemID(out[0].Item) != "" {
		t.Fatalf("the id-less card should keep its merit position, got %v", rankedIDs(out))
	}
}

func TestSeenPenalty_DoesNotMutateInputScores(t *testing.T) {
	// Handlers stash Score for LTR after ranking; it must be what the ranker
	// computed, not the penalized value used for ordering.
	resetRedis(t)
	u := "upen8"
	seedSeen(t, u, "post", "watched", 1*time.Hour)
	items := []ScoredItem{scoredPost("watched", 2.0), scoredPost("fresh", 1.0)}
	_ = applySeenPenaltyFor(u, items)
	if items[0].Score != 2.0 {
		t.Fatalf("input score was mutated: got %f, want 2.0", items[0].Score)
	}
}

func TestSeenPenalty_EmptySeenSetIsANoop(t *testing.T) {
	resetRedis(t)
	items := []ScoredItem{scoredPost("a", 1.0), scoredPost("b", 2.0)}
	out := applySeenPenalty(items, nil)
	if got := rankedIDs(out); got[0] != "a" || got[1] != "b" {
		t.Fatalf("with nothing seen the caller's order must be untouched, got %v", got)
	}
}

func TestSinkSeenItems_OrdersWatchedTailOldestFirst(t *testing.T) {
	// The Following tab's contract: nothing is dropped, unseen keeps its
	// chronological order, and the watched tail is longest-ago-first.
	resetRedis(t)
	u := "usink1"
	seedSeen(t, u, "challenge", "old", 1*time.Hour)
	seedSeen(t, u, "challenge", "recent", 1*time.Minute)

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
	want := []string{"new1", "new2", "old", "recent"}
	for i := range want {
		if got := getItemID(out[i]); got != want[i] {
			t.Fatalf("slot %d: want %q, got %q", i, want[i], got)
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
	// the feed, the clips the user just finished watching.
	resetRedis(t)
	u := "useen6"
	markShown(u, "post", "justWatched")

	applyRefreshSignal(u, "sess-1")

	seen := loadSeenSet(u)
	if _, ok := seen[seenMember("post", "justWatched")]; !ok {
		t.Fatalf("refresh must preserve the seen set, got %v", seen)
	}
}

// Marking a repeat must not remove it.
//
// The distinction is the whole design: the feed ranks seen content DOWN and
// still serves it, because the hard filter this replaced used to announce the
// feed had ended when it had not. Two changes went into undoing that, and a
// well-meaning edit here could quietly bring it back — the client would simply
// see shorter pages and nobody would know why.
func TestApplySeenPenalty_MarksRepeatsWithoutRemovingThem(t *testing.T) {
	now := time.Now().Unix()
	items := []ScoredItem{
		{Item: HomeFeedItem{Type: "challenge", Challenge: &Challenge{ID: "1"}}, Score: 1.0},
		{Item: HomeFeedItem{Type: "challenge", Challenge: &Challenge{ID: "2"}}, Score: 0.9},
		{Item: HomeFeedItem{Type: "challenge", Challenge: &Challenge{ID: "3"}}, Score: 0.8},
	}
	// Two of the three were shown an hour ago.
	seen := map[string]int64{
		seenMember("challenge", "1"): now - 3600,
		seenMember("challenge", "3"): now - 3600,
	}

	out := applySeenPenalty(items, seen)

	if len(out) != len(items) {
		t.Fatalf("got %d items back from %d — seen content must be ranked "+
			"down, never dropped", len(out), len(items))
	}

	marked := map[string]bool{}
	for _, si := range out {
		if si.Item.Challenge != nil && si.Item.Challenge.Repeat {
			marked[si.Item.Challenge.ID] = true
		}
	}
	for _, id := range []string{"1", "3"} {
		if !marked[id] {
			t.Errorf("challenge %s was seen but not marked as a repeat — the "+
				"client cannot tell a deliberate re-serve from a bug without it", id)
		}
	}
	if marked["2"] {
		t.Error("challenge 2 was never seen and must not be marked a repeat")
	}
}

// An unseen page must carry no marks at all, or "repeat" stops meaning
// anything and the client's page-ending rule fires on a healthy feed.
func TestApplySeenPenalty_LeavesAFreshPageUnmarked(t *testing.T) {
	items := []ScoredItem{
		{Item: HomeFeedItem{Type: "challenge", Challenge: &Challenge{ID: "10"}}, Score: 1.0},
		{Item: HomeFeedItem{Type: "challenge", Challenge: &Challenge{ID: "11"}}, Score: 0.9},
	}
	out := applySeenPenalty(items, map[string]int64{})
	for _, si := range out {
		if si.Item.Challenge != nil && si.Item.Challenge.Repeat {
			t.Errorf("challenge %s marked as a repeat on a page nothing was "+
				"seen on", si.Item.Challenge.ID)
		}
	}
}
