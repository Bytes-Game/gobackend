package main

// feed_kind_filter.go — the Shorts tab.
//
// Every feed surface serves both kinds: battles (two people head to head)
// and shorts (a challenge nobody has answered yet). The tabs at the top of
// the app pick a RANKING — For You, Following, Explore — and all three hand
// back a mix.
//
// A Shorts tab is a different question. It does not want a different
// ranking; it wants the same ranking with the battles taken out. So this is
// a filter over the finished page rather than another feed algorithm, which
// also means it stays correct on its own as the ranking changes underneath.
//
// WHERE IT RUNS
//
// After finalizeFeedItems, for the same reason spaceOutFeedKinds runs there:
// whether an item is a battle is decided by TopResponseVideoUrl, which is
// the field the client keys its own battle rendering off, and which only
// exists once finalize has filled it in. Filter before that and the answer
// comes from candidate-source metadata that several lanes leave unset — the
// tab would drop the wrong things, and would drop different wrong things
// depending on which lane found the item.
//
// WHY THE SERVER DOES THIS AND NOT THE APP
//
// The app could ask for a normal feed and simply not render the battles. It
// must not, and the reason is not bandwidth — it is that the server records
// what it served BEFORE the phone shows anything. markShownBatch stamps every
// item into the user's seen set at the moment the page is composed.
//
// So a battle the app quietly hid would still be written down as watched.
// Later pages would push it down or skip it as already-seen, and the profile
// the whole feed learns from would fill up with videos the person never laid
// eyes on. The tab would slowly poison the thing it sits on top of.
//
// The lesser reasons all point the same way: a page of twenty arriving as six
// makes hasMore meaningless and costs extra round trips; the phone would
// prefetch video for items it is about to throw away; and what counts as a
// battle is a server-side idea that would then have to be duplicated in every
// shipped app version and kept in step forever.
//
// WHY IT DOES NOT FILTER THE DATABASE INSTEAD
//
// It would be faster, and it would be wrong in a way that is hard to see.
// The ranker's whole job is to pick the best items for this person; asking
// it for the best twenty and then removing the battles gives a genuinely
// well-ranked page of shorts. Filtering at retrieval instead would change
// which candidates every downstream stage sees — the seen-penalty, the
// audition floor, diversity, the learned heads — and the tab would quietly
// become a different algorithm rather than a view of the same one.
//
// The cost is that a filtered page comes back shorter than asked for. That
// is what feedKindOverfetch is for.

import "net/http"

// Feed kind filters, as accepted in the ?kind= query parameter.
const (
	feedKindAll     = ""        // no filter — the normal mixed feed
	feedKindShorts  = "shorts"  // only challenges nobody has answered
	feedKindBattles = "battles" // only head-to-head battles
)

// feedKindOverfetch is how much extra to ask for when a kind filter is on.
//
// Sized off the SCARCE kind, not the common one. Battles run at roughly a
// quarter of the feed — a live sample gave 1, 8 and 6 battles across three
// pages of twenty — so filling a page of 20 battles means looking at about
// 80 items. At 2x it looked at 40 and returned 8 or 9, and the Battles tab
// visibly ran out after two short pages while plenty of battles remained.
//
// 5x is that quarter-share plus room for the seen-penalty to have demoted
// some of them. The common kind is unaffected: a shorts page fills long
// before the fetch limit and the extra is never scored.
//
// The extra IS real work — every candidate gets scored, and scoring is the
// expensive part of a feed request — which is why this is bounded below and
// why it only ever applies to a single-kind tab.
const feedKindOverfetch = 5

// feedKindMaxFetch caps the absolute number of candidates a filtered tab may
// ask for, however large a page the client requests.
//
// Without a ceiling a client asking for 100 would quietly ask the ranker for
// 500, and the cost of a feed request is close to linear in candidates
// scored. A tab that comes back a little short is a much smaller problem
// than a tab that takes five seconds.
const feedKindMaxFetch = 120

// feedKindFromRequest reads the ?kind= filter, ignoring anything it does not
// recognise.
//
// Unknown values fall back to the unfiltered feed rather than erroring. A
// typo in a query string should give somebody a normal feed, not a broken
// tab.
func feedKindFromRequest(r *http.Request) string {
	switch r.URL.Query().Get("kind") {
	case feedKindShorts:
		return feedKindShorts
	case feedKindBattles:
		return feedKindBattles
	default:
		return feedKindAll
	}
}

// feedKindWantsItem reports whether one item belongs in a filtered feed.
//
// Anything that is not a challenge — a suggested-accounts card, say — is
// kept regardless. Those are not content and removing them would strip the
// tab of furniture the client expects to be there.
func feedKindWantsItem(it HomeFeedItem, kind string) bool {
	if kind == feedKindAll || it.Challenge == nil {
		return true
	}
	if kind == feedKindBattles {
		return itemIsBattle(it)
	}
	return !itemIsBattle(it)
}

// filterFeedKind drops the items this tab does not want.
func filterFeedKind(items []HomeFeedItem, kind string) []HomeFeedItem {
	if kind == feedKindAll {
		return items
	}
	out := make([]HomeFeedItem, 0, len(items))
	for _, it := range items {
		if feedKindWantsItem(it, kind) {
			out = append(out, it)
		}
	}
	return out
}

// filterFeedKindScored is the ScoredItem flavour.
func filterFeedKindScored(items []ScoredItem, kind string) []ScoredItem {
	if kind == feedKindAll {
		return items
	}
	out := make([]ScoredItem, 0, len(items))
	for _, it := range items {
		if feedKindWantsItem(it.Item, kind) {
			out = append(out, it)
		}
	}
	return out
}

// feedKindFetchLimit is how many items to ask the pipeline for, given what
// the client asked for and which tab it is.
func feedKindFetchLimit(limit int, kind string) int {
	if kind == feedKindAll || limit <= 0 {
		return limit
	}
	n := limit * feedKindOverfetch
	if n > feedKindMaxFetch {
		n = feedKindMaxFetch
	}
	return n
}

// feedKindHasMore answers "is it worth asking for another page" for a
// single-kind tab.
//
// ════════════════════════════════════════════════════════════════════════════
// WHY THIS EXISTS AT ALL
// ════════════════════════════════════════════════════════════════════════════
//
// Because the obvious thing is wrong, and it shipped. Every feed handler ends
// with some version of "a full page means ask again", measured against the
// limit it fetched with. On a filtered tab that limit is the OVER-FETCH — so
// the question being answered was "did the mixed pool fill 100?" when the
// client had asked for 20.
//
// The result showed up in a live log as a full page of twenty shorts returned
// alongside "the feed is over":
//
//	feed shorts page 1: 20 items  raw=20/20  more=false
//
// Twenty of twenty delivered, and the tab stopped. Not because anything ran
// out — because a number that was never about the client's page was used to
// answer a question about the client's page.
//
// ════════════════════════════════════════════════════════════════════════════
// THE THREE CASES
// ════════════════════════════════════════════════════════════════════════════
//
//	FILLED THE PAGE      → yes. We gave them everything they asked for; there
//	                       is no evidence of an end anywhere.
//	SHORT, POOL WAS FULL → yes. The fetch was the constraint, not the
//	                       catalogue. This kind is scarce in the ranking, and
//	                       what did not fit in this pool is still out there.
//	SHORT, POOL WAS NOT  → no. The ranker offered everything it had and this
//	                       is what there was of this kind. Genuinely the end.
func feedKindHasMore(rawCount, filteredCount, clientLimit, fetchLimit int) bool {
	if filteredCount == 0 {
		// Nothing served. Another page can only be empty too, and a client
		// that stops only on an empty result would spin.
		return false
	}
	if filteredCount >= clientLimit {
		return true
	}
	return rawCount >= fetchLimit
}

// trimFilteredPage cuts an over-fetched single-kind page back to the size the
// client asked for.
//
// THE MIXED FEED IS NEVER TRIMMED, and that is not a detail. The normal feed
// is allowed to come back longer than the requested limit: the audition floor
// and the suggested-accounts card are both INSERTED after composition, on
// purpose, and nothing downstream cuts them off. Trimming here would silently
// delete whichever of them landed last — a change to the ordinary feed made
// while adding a tab, which is exactly the kind of accident that is hard to
// spot afterwards.
//
// So the kind is a parameter rather than something the caller remembers to
// check. There is no way to call this and shorten a page that was not
// filtered.
func trimFilteredPage(items []ScoredItem, limit int, kind string) []ScoredItem {
	if kind == feedKindAll || limit <= 0 || len(items) <= limit {
		return items
	}
	return items[:limit]
}

// trimFilteredPagePlain is trimFilteredPage for the surfaces that carry plain
// items rather than scored ones.
func trimFilteredPagePlain(items []HomeFeedItem, limit int, kind string) []HomeFeedItem {
	if kind == feedKindAll || limit <= 0 || len(items) <= limit {
		return items
	}
	return items[:limit]
}
