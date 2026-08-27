package main

import (
	"net/http"
	"strconv"
	"testing"
)

// The single-kind tabs. These cover the filter and nothing else — ranking,
// ordering and the battle/short mix are deliberately untouched by this file
// and have their own tests.

// kindPage builds a feed from a shape string: 'B' is a battle, 'S' a short.
// Battle-ness is read from TopResponseVideoUrl, the same field the client
// renders from and the same one itemIsBattle tests.
func kindPage(shape string) []HomeFeedItem {
	items := make([]HomeFeedItem, 0, len(shape))
	for i, c := range shape {
		ch := &Challenge{ID: strconv.Itoa(i)}
		if c == 'B' {
			ch.TopResponseVideoUrl = "https://cdn/opponent.mp4"
			ch.ResponseCount = 1
		}
		items = append(items, HomeFeedItem{Type: "challenge", Challenge: ch})
	}
	return items
}

func kindShape(items []HomeFeedItem) string {
	out := make([]byte, 0, len(items))
	for _, it := range items {
		if itemIsBattle(it) {
			out = append(out, 'B')
		} else {
			out = append(out, 'S')
		}
	}
	return string(out)
}

// ── What each tab returns ───────────────────────────────────────────────────

func TestShortsTab_DropsEveryBattle(t *testing.T) {
	if got := kindShape(filterFeedKind(kindPage("BSBSBBSS"), feedKindShorts)); got != "SSSS" {
		t.Errorf("Shorts tab returned %s, want shorts only", got)
	}
}

func TestBattlesTab_DropsEveryShort(t *testing.T) {
	if got := kindShape(filterFeedKind(kindPage("BSBSBBSS"), feedKindBattles)); got != "BBBB" {
		t.Errorf("Battles tab returned %s, want battles only", got)
	}
}

func TestKindFilter_TheMixedFeedIsUntouched(t *testing.T) {
	// The default path must be a no-op in every respect — same items, same
	// order. The whole point of reverting the ranking work is that the mixed
	// feed is exactly what it was.
	in := kindPage("BSBSBBSS")
	out := filterFeedKind(in, feedKindAll)
	if kindShape(out) != kindShape(in) {
		t.Errorf("the mixed feed changed from %s to %s", kindShape(in), kindShape(out))
	}
	for i := range in {
		if out[i].Challenge.ID != in[i].Challenge.ID {
			t.Fatalf("slot %d changed identity — this path must not reorder", i)
		}
	}
}

func TestKindFilter_PreservesOrderWithinTheKindItKeeps(t *testing.T) {
	// The filter removes; it must never reorder. Whatever the ranker decided
	// among the shorts is still what the Shorts tab shows.
	out := filterFeedKind(kindPage("SBSBSS"), feedKindShorts)
	want := []string{"0", "2", "4", "5"}
	for i, id := range want {
		if out[i].Challenge.ID != id {
			t.Errorf("slot %d holds id %s, want %s — the filter reordered the page",
				i, out[i].Challenge.ID, id)
		}
	}
}

func TestKindFilter_KeepsThingsThatAreNotVideos(t *testing.T) {
	// A suggested-accounts card is furniture, not content. Stripping it from a
	// single-kind tab would leave the client missing something it expects.
	in := []HomeFeedItem{
		kindPage("B")[0],
		{Type: "suggestedAccounts", SuggestedAccounts: &SuggestedAccountsCard{}},
		kindPage("S")[0],
	}
	for _, kind := range []string{feedKindShorts, feedKindBattles} {
		out := filterFeedKind(in, kind)
		found := false
		for _, it := range out {
			if it.Type == "suggestedAccounts" {
				found = true
			}
		}
		if !found {
			t.Errorf("the %s tab dropped the suggested-accounts card", kind)
		}
	}
}

func TestKindFilter_AnEmptyResultIsAllowed(t *testing.T) {
	// A page that happens to hold no battles gives the Battles tab nothing.
	// That must be an empty page, not a panic and not the unfiltered page.
	if got := filterFeedKind(kindPage("SSSS"), feedKindBattles); len(got) != 0 {
		t.Errorf("got %d items, want none", len(got))
	}
	if got := filterFeedKind(nil, feedKindShorts); len(got) != 0 {
		t.Errorf("a nil page produced %d items", len(got))
	}
}

// ── Reading the tab off the request ─────────────────────────────────────────

func TestKindFromRequest(t *testing.T) {
	cases := map[string]string{
		"/f":                feedKindAll,
		"/f?kind=":          feedKindAll,
		"/f?kind=shorts":    feedKindShorts,
		"/f?kind=battles":   feedKindBattles,
		"/f?kind=Shorts":    feedKindAll, // unknown → a normal feed, never a blank tab
		"/f?kind=nonsense":  feedKindAll,
		"/f?kind=challenge": feedKindAll,
	}
	for path, want := range cases {
		req, err := http.NewRequest("GET", path, nil)
		if err != nil {
			t.Fatal(err)
		}
		if got := feedKindFromRequest(req); got != want {
			t.Errorf("%s parsed as %q, want %q", path, got, want)
		}
	}
}

// ── Filling a filtered page ─────────────────────────────────────────────────

func TestFetchLimit_OverfetchesOnlyForASingleKindTab(t *testing.T) {
	// Filtering happens after ranking, so a page of twenty comes back smaller.
	// Asking for more up front is what stops a single-kind tab looking like it
	// ran out of content on every page.
	if got := feedKindFetchLimit(20, feedKindAll); got != 20 {
		t.Errorf("the mixed feed over-fetched (%d) — it must be untouched", got)
	}
	for _, kind := range []string{feedKindShorts, feedKindBattles} {
		if got := feedKindFetchLimit(20, kind); got <= 20 {
			t.Errorf("the %s tab fetched %d for a 20-item page — after the drop it "+
				"would serve a short page every time", kind, got)
		}
	}
}

func TestTrim_CutsBackToWhatTheClientAskedFor(t *testing.T) {
	page := make([]ScoredItem, 40)
	if got := len(trimFilteredPage(page, 20, feedKindShorts)); got != 20 {
		t.Errorf("an over-fetched page came back with %d items, want 20", got)
	}
	if got := len(trimFilteredPage(page[:5], 20, feedKindShorts)); got != 5 {
		t.Errorf("a page smaller than the limit was changed to %d items", got)
	}
	if got := len(trimFilteredPage(page, 0, feedKindShorts)); got != 40 {
		t.Errorf("a zero limit trimmed to %d — it should mean no trim", got)
	}
}

func TestTrim_NeverTouchesTheMixedFeed(t *testing.T) {
	// The normal feed is allowed to come back longer than the requested limit:
	// the audition floor and the suggested-accounts card are both inserted
	// after composition and nothing downstream cuts them off. Trimming here
	// would silently delete whichever landed last — a change to the ordinary
	// feed made while adding a tab.
	page := make([]ScoredItem, 22)
	if got := len(trimFilteredPage(page, 20, feedKindAll)); got != 22 {
		t.Errorf("the mixed feed was trimmed from 22 items to %d — injections that "+
			"land past the limit must survive", got)
	}
	plain := make([]HomeFeedItem, 22)
	if got := len(trimFilteredPagePlain(plain, 20, feedKindAll)); got != 22 {
		t.Errorf("the mixed feed was trimmed from 22 items to %d", got)
	}
}

// ── The two flavours agree ──────────────────────────────────────────────────

func TestBothFlavoursOfTheFilterAgree(t *testing.T) {
	// Two surfaces use the plain slice and two use the scored one. If they
	// diverged, the Shorts tab would mean something different on Explore than
	// it does on For You.
	for _, shape := range []string{"BSBSBBSS", "SSSS", "BBBB", "BS"} {
		for _, kind := range []string{feedKindShorts, feedKindBattles, feedKindAll} {
			plain := kindShape(filterFeedKind(kindPage(shape), kind))

			scored := make([]ScoredItem, 0)
			for _, it := range kindPage(shape) {
				scored = append(scored, ScoredItem{Item: it})
			}
			filtered := filterFeedKindScored(scored, kind)
			asPlain := make([]HomeFeedItem, len(filtered))
			for i, s := range filtered {
				asPlain[i] = s.Item
			}

			if got := kindShape(asPlain); got != plain {
				t.Errorf("%s/%s: plain gave %s, scored gave %s", shape, kind, plain, got)
			}
		}
	}
}

// ════════════════════════════════════════════════════════════════════════════
// "Is there another page?" — the bug that made the tabs feel empty
// ════════════════════════════════════════════════════════════════════════════
//
// A live log showed this exactly:
//
//	feed shorts page 1: 20 items  raw=20/20  more=false
//
// Twenty of twenty delivered and the tab stopped. Nothing had run out — the
// question "is there more" was being answered with the over-fetch number
// instead of the page the client asked for.

func TestHasMore_AFullPageIsNeverTheEnd(t *testing.T) {
	// The exact shape from the log: the client asked for 20, got 20, and the
	// pool behind it was short of the 100 we fetched with. There is no
	// evidence of an end anywhere in that, and saying so stopped the tab.
	if !feedKindHasMore(60, 20, 20, 100) {
		t.Error("a full page of 20 reported the feed was over — this is the " +
			"live bug: the pool not filling 100 says nothing about whether " +
			"the viewer's page of 20 was the last one")
	}
}

func TestHasMore_ShortPageButThePoolWasTheLimit(t *testing.T) {
	// 100 candidates came back — the fetch was the constraint, not the
	// catalogue. Only 8 of them were battles, but the ones that did not fit
	// in this pool are still out there.
	if !feedKindHasMore(100, 8, 20, 100) {
		t.Error("a short page off a FULL pool reported the end; the pool " +
			"was the limit, so there is more of this kind further down")
	}
}

func TestHasMore_ShortPageAndTheRankerHadNothingLeft(t *testing.T) {
	// The genuine end: the ranker offered 42 when asked for 100, so it gave
	// everything it had, and 8 of those were battles. Claiming more here
	// sends the client after a page that cannot exist.
	if feedKindHasMore(42, 8, 20, 100) {
		t.Error("the catalogue was exhausted and this still claimed more")
	}
}

func TestHasMore_NothingServedIsAlwaysTheEnd(t *testing.T) {
	// A client that stops only on an empty result would spin forever.
	if feedKindHasMore(100, 0, 20, 100) {
		t.Error("an empty page claimed there was more")
	}
}

// ── Fetching deep enough to actually fill a page ────────────────────────────

func TestFetchLimit_SizedForTheScarceKind(t *testing.T) {
	// Battles ran at about a quarter of the feed in a live sample (1, 8 and 6
	// across three pages of twenty). At 2x the tab asked for 40, found 8, and
	// visibly ran out while plenty of battles remained.
	got := feedKindFetchLimit(20, feedKindBattles)
	if got < 80 {
		t.Errorf("a page of 20 battles fetches %d candidates; at roughly a "+
			"quarter battles that yields ~%d — not a page", got, got/4)
	}
}

func TestFetchLimit_MixedFeedFetchesExactlyWhatItWasAsked(t *testing.T) {
	// The one that must not move. Over-fetching the mixed feed would change
	// what every downstream stage sees for every user on the normal feed.
	for _, n := range []int{1, 20, 50, 500} {
		if got := feedKindFetchLimit(n, feedKindAll); got != n {
			t.Errorf("mixed feed asked for %d, fetch limit became %d", n, got)
		}
	}
}

func TestFetchLimit_IsBounded(t *testing.T) {
	// Scoring is the expensive part of a feed request and cost is close to
	// linear in candidates. A tab that comes back a little short beats a tab
	// that takes five seconds.
	if got := feedKindFetchLimit(500, feedKindBattles); got > feedKindMaxFetch {
		t.Errorf("a page of 500 asked the ranker for %d candidates", got)
	}
	if got := feedKindFetchLimit(0, feedKindBattles); got != 0 {
		t.Errorf("a zero-item page asked for %d", got)
	}
}

// ════════════════════════════════════════════════════════════════════════════
// NARROWING THE POOL: WHAT IT IS ACTUALLY FOR
// ════════════════════════════════════════════════════════════════════════════
//
// A live Battles page scored 250 candidates to serve 9 items. The waste was
// the obvious half; the ceiling was the half that mattered. composeFeed counts
// its 3-per-creator limit against the MIXED page, so a creator whose slots go
// to shorts contributes almost nothing to the Battles tab and their other
// battles are never looked at.
//
// These tests pin both halves, because the fix is easy to undo by moving one
// line back below composition and nothing would fail.

func kindItem(id, creator string, battle bool) HomeFeedItem {
	ch := &Challenge{ID: id, CreatorID: creator}
	if battle {
		ch.TopResponseVideoUrl = "https://x/" + id + ".mp4"
		ch.ResponseCount = 1
	}
	return HomeFeedItem{Type: "challenge", Challenge: ch}
}

// composeCeiling is the arithmetic composeFeed actually applies: no more than
// maxItemsPerCreator from any one creator, counted over whatever it is given.
func composeCeiling(items []HomeFeedItem) []HomeFeedItem {
	perCreator := map[string]int{}
	out := make([]HomeFeedItem, 0, len(items))
	for _, it := range items {
		c := it.Challenge.CreatorID
		if perCreator[c] >= maxItemsPerCreator {
			continue
		}
		perCreator[c]++
		out = append(out, it)
	}
	return out
}

func TestNarrow_TheCreatorCapCountsTheKindBeingServed(t *testing.T) {
	// Three creators, each with 3 shorts and 3 battles. Shorts rank first,
	// which is the ordinary case — a challenge is born a short, so the newest
	// and most numerous things in any pool are shorts.
	var pool []HomeFeedItem
	for _, c := range []string{"alice", "bob", "carol"} {
		for i := 0; i < 3; i++ {
			pool = append(pool, kindItem(c+"-s"+string(rune('0'+i)), c, false))
		}
	}
	for _, c := range []string{"alice", "bob", "carol"} {
		for i := 0; i < 3; i++ {
			pool = append(pool, kindItem(c+"-b"+string(rune('0'+i)), c, true))
		}
	}

	// OLD ORDER: compose the mixed page first, then drop the shorts.
	oldWay := filterFeedKind(composeCeiling(pool), feedKindBattles)

	// NEW ORDER: drop what the tab cannot show, then compose.
	newWay := composeCeiling(filterFeedKind(pool, feedKindBattles))

	t.Logf("9 battles available — filtering after composition serves %d, "+
		"filtering before serves %d", len(oldWay), len(newWay))

	if len(oldWay) != 0 {
		t.Errorf("expected the old order to serve nothing here (every creator "+
			"spends all three slots on shorts), got %d", len(oldWay))
	}
	if len(newWay) != 9 {
		t.Errorf("filtering first should reach all 9 battles, got %d", len(newWay))
	}
}

func TestNarrow_MixedFeedIsUntouchedAndPaysNothing(t *testing.T) {
	// The ordinary feed must not change at all, and must not pay for the
	// enrichment query the tabs need. Identity of the returned slice is the
	// check: narrowing returns the caller's own slice, having done nothing.
	pool := []HomeFeedItem{
		kindItem("1", "alice", false),
		kindItem("2", "bob", true),
	}
	got := narrowCandidatesToKind(pool, feedKindAll)
	if len(got) != len(pool) {
		t.Fatalf("the mixed feed lost items: %d → %d", len(pool), len(got))
	}
	for i := range pool {
		if got[i].Challenge != pool[i].Challenge {
			t.Errorf("item %d was rebuilt; the mixed feed should be returned as-is", i)
		}
	}
	// Empty in, empty out, and no database touched.
	if got := narrowCandidatesToKind(nil, feedKindBattles); len(got) != 0 {
		t.Errorf("got %d items from an empty pool", len(got))
	}
}

func TestNarrow_KeepsWhatTheTabWantsAndDropsTheRest(t *testing.T) {
	pool := []HomeFeedItem{
		kindItem("1", "alice", false),
		kindItem("2", "bob", true),
		kindItem("3", "carol", false),
		// Furniture is not content and belongs on every tab.
		{Type: "suggestedAccounts", SuggestedAccounts: &SuggestedAccountsCard{}},
	}
	answered := map[string]bool{"2": true}

	battles := keepKind(pool, answered, feedKindBattles)
	if len(battles) != 2 {
		t.Errorf("battles tab kept %d items, expected the 1 battle + the card", len(battles))
	}
	shorts := keepKind(pool, answered, feedKindShorts)
	if len(shorts) != 3 {
		t.Errorf("shorts tab kept %d items, expected the 2 shorts + the card", len(shorts))
	}
}

func TestNarrow_AnUnanswerableQuestionChangesNothing(t *testing.T) {
	// db is nil here, which is the same shape as a database blip in
	// production. The wrong move is to guess: guessing "nothing is answered"
	// empties the Battles tab and lets battles onto the Shorts tab.
	//
	// Correctness lives in the filter that runs after composition, on an
	// enriched page. This step only buys headroom, so when it cannot tell it
	// hands the pool back exactly as it found it.
	pool := []HomeFeedItem{
		kindItem("1", "alice", false),
		kindItem("2", "bob", true),
	}
	for _, kind := range []string{feedKindBattles, feedKindShorts} {
		got := narrowCandidatesToKind(pool, kind)
		if len(got) != len(pool) {
			t.Errorf("%s tab: with no way to tell battles from shorts the pool "+
				"went %d → %d. It has to be left alone.", kind, len(pool), len(got))
		}
	}
	if _, ok := answeredChallengeIDs(pool); ok {
		t.Error("claimed to have answered the question with no database")
	}
}

func TestNarrow_TheOverfetchIsNotCappedByTheRequestCap(t *testing.T) {
	// Both handlers used to apply maxPageSize AFTER the over-fetch, which cut
	// a 5x working pool back to 50 and made feedKindMaxFetch unreachable. The
	// two caps mean different things and the order is what keeps them apart.
	const asked = 20
	if asked > maxPageSize {
		t.Skip("the default page is larger than the request cap; rewrite this")
	}
	fetch := feedKindFetchLimit(asked, feedKindBattles)
	if fetch <= maxPageSize {
		t.Errorf("a tab asking for %d works with a pool of %d, which the %d "+
			"request cap would swallow — the over-fetch has been capped by the "+
			"wrong number again", asked, fetch, maxPageSize)
	}
	if fetch != asked*feedKindOverfetch {
		t.Errorf("expected the full %dx over-fetch (%d), got %d",
			feedKindOverfetch, asked*feedKindOverfetch, fetch)
	}
}
