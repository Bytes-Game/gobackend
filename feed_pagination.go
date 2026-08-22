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
// actually stop, so this is deliberately paired with two things it already
// does. It de-duplicates by content id and treats a page that yielded zero
// NEW items as the end, whatever the server claimed; and it stops the
// moment hasMore is false. So the terminating case is a page of pure
// repeats, which is exactly what the catalog produces once every unseen
// item has been served — the seen penalty sinks them but never hides them,
// so they come back as repeats rather than as an empty page.
// ─────────────────────────────────────────────────────────────────────────────

// feedHasMore reports whether the client should request another page.
//
// candidates is the size of the scored pool the page was composed FROM;
// composed is what actually made it onto the page; limit is what the
// client asked for.
// feedHasMore answers "is it worth asking for another page".
//
// fresh is how many of the composed items the viewer has NOT already been
// shown. It is the difference between a feed with more to give and a feed that
// has started going round in circles, and without it this function cannot tell
// those apart — a page of twenty repeats looks exactly like a page of twenty
// new videos from in here.
//
// That gap had a cost. A device run walked six pages: the first three were
// almost entirely fresh, the fourth yielded seven new items out of twenty-one,
// the fifth sixteen, and the sixth nothing at all — and this function said
// "keep going" every time, because the pages were full. The client asked again
// and again for content that was not there.
func feedHasMore(candidates, composed, fresh, limit int) bool {
	// Nothing was served. There is no next page to ask for, and claiming
	// otherwise would spin a client that stops only on an empty result.
	if composed == 0 {
		return false
	}
	// Everything on this page has been seen before. The catalogue is exhausted
	// for this viewer right now, and another page can only bring more of the
	// same — so say so instead of sending them after it.
	//
	// This is NOT the old hard filter coming back. Repeats are still served;
	// the viewer keeps scrolling through them and the seen-handicap fades over
	// twelve hours so they return properly later. What stops is the pretence
	// that there is something new one page further on.
	if fresh == 0 {
		return false
	}
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
