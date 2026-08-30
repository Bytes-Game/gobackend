package main

// ─────────────────────────────────────────────────────────────────────────────
// "IS THERE ANOTHER PAGE?"
//
// The For You handler used to answer this with `len(composed) >= limit`:
// a full page means ask again, a short page means the feed is over.
//
// That reads as common sense and is wrong, because a For You page can come
// back short for reasons that have nothing to do with running out of
// content. composeFeed enforces maxItemsPerCreator, so the page length is
// bounded by (distinct eligible creators × 3) no matter how deep the
// catalog is. Slot buckets can also empty out before the pattern does.
// Neither is a statement about supply.
//
// The observed failure: a catalog of 28 challenges from 6 creators, served
// to one of those 6. Own content is excluded, leaving 23 items from 5
// creators, and 5 × 3 = 15. The client asks for 20, gets 15, and is told
// hasMore=false — so the remaining 8 are unreachable. Not on the next page,
// not ever: the ceiling is structural, so no page can reach 20 and hasMore
// is false on page 1 of every session. Adding content does not help. A
// thousand videos from those 5 creators still ends at 15.
//
// The question worth asking is not "did the page fill" but "was anything
// left over". That is what [feedHasMore] answers, and it is the same
// position seen_filter.go already takes for the ranker — nothing is
// withheld, nothing reports that the feed has run out, and the client
// decides when to stop asking.
//
// WHAT STOPS THE CLIENT
//
// Handing paging authority to the client only works if the client can
// actually stop, and its stopping condition is an EMPTY page: it drops
// duplicates within a single response and treats a page that parsed to
// nothing as the end, whatever the server claimed. It does not de-duplicate
// against what it already has on screen, deliberately — see the long note in
// the app's _loadNextPage — so a repeat the server chose to send is rendered
// rather than silently deleted.
//
// That is what makes the feed able to loop rather than end. The pair of rules
// is: this function keeps saying yes while there is anything to serve, and the
// client stops when a page comes back with nothing in it.
// ─────────────────────────────────────────────────────────────────────────────

// feedHasMore reports whether the client should request another page.
//
// candidates is the size of the scored pool the page was composed FROM;
// composed is what actually made it onto the page; limit is what the client
// asked for.
//
// It used to take a fourth number, how many of the composed items were unseen,
// and answer false when that was zero. That made the feed announce an ending
// as soon as a viewer had worked through the catalogue once. Removed — see the
// note on the repeat case below for why looping is both safe and wanted.
func feedHasMore(candidates, composed, limit int) bool {
	// Nothing was served. There is no next page to ask for, and claiming
	// otherwise would spin a client that stops only on an empty result.
	if composed == 0 {
		return false
	}
	// A page of nothing but repeats used to end the feed here. It no longer
	// does, because "you have seen all of these" is not the same claim as
	// "there is nothing left to show you".
	//
	// ════════════════════════════════════════════════════════════════════════
	// WHY LOOPING IS SAFE, AND WHY IT SERVES SOMETHING DIFFERENT EACH TIME
	// ════════════════════════════════════════════════════════════════════════
	//
	// The worry with never ending is a feed that hands back the same twenty
	// videos forever. It does not, and the reason is that the seen record is a
	// TIMESTAMP, restamped by markShownBatch every time an item is served,
	// and seenPenalty is largest when that timestamp is newest.
	//
	// So serving page 1 is what pushes page 1's items to the back. Page 2 is
	// then composed from what has been waiting longest, page 3 from what has
	// been waiting longest after that, and the feed walks the catalogue in
	// least-recently-seen order instead of stopping at the end of it.
	//
	// Two properties fall out of that, both wanted:
	//
	//   - Unseen content still leads. It carries no handicap at all, so as
	//     soon as anything new is uploaded it outranks the whole backlog.
	//   - A strong video can still come back early. The handicap is a score
	//     penalty, not a filter, so something the ranker rates highly enough
	//     can beat an unwatched item — which is the behaviour on every feed
	//     worth copying.
	//
	// The catalogue is 108 videos today, so a viewer reaches the end of it in
	// one sitting. That is a fact about the library, not a reason to stop.
	//
	// What still ends the feed is composed == 0 above: nothing was served, so
	// there is genuinely nothing to ask for. That is the honest terminator and
	// it is the only one.
	// Composition left candidates on the table. Whatever the reason —
	// per-creator cap, an exhausted slot bucket — those items exist and
	// the next page's fresh cap can reach them.
	if candidates > composed {
		return true
	}
	// Everything scored was used. A full page still means "ask again":
	// the candidate pool is itself a bounded fetch, so landing exactly on
	// the page size says the fetch was the constraint, not the catalog.
	// Short of a full page, the pool really is what there was.
	return composed >= limit
}
