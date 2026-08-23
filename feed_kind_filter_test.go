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
