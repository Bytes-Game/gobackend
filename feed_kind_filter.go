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
// The retrieval queries stay kind-blind. Teaching eight candidate-source
// queries about tabs is eight chances to miss one, and a missed lane fails
// silently — that source simply stops contributing to the tab, with no error
// anywhere to say so. The same argument is written out at
// populateHLSManifestURLs, which was built as one enrichment hop for exactly
// this reason.
//
// THE POOL IS STILL NARROWED, THOUGH — SEE narrowCandidatesToKind
//
// "Do not filter in the retrieval SQL" was once read here as "do not filter
// until the page is composed", and those are not the same thing. The second
// reading cost the Battles tab most of what it could show: composeFeed counts
// its per-creator cap against the MIXED page, so a creator whose slots went to
// shorts contributed nothing to the tab and their battles were never looked
// at. Two hundred and fifty candidates were scored to serve nine items.
//
// So the pool is narrowed once, after retrieval and before anything expensive,
// in a single place. Every ranking stage still runs, in the same order, on the
// same kind of item — the difference is that the diversity rule now counts the
// thing the viewer is actually being shown.
//
// The filter below still runs after composition as well, and that is not
// redundancy: the audition floor and the suggested-accounts card are injected
// AFTER the page is composed, so something the tab does not want can still
// arrive late. One filter per place an item can enter the page.
//
// The remaining cost is that a filtered page can come back shorter than asked
// for. That is what feedKindOverfetch is for.

import (
	"log"
	"net/http"

	"github.com/lib/pq"
)

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

// ════════════════════════════════════════════════════════════════════════════
// NARROWING THE POOL BEFORE THE EXPENSIVE PART
// ════════════════════════════════════════════════════════════════════════════
//
// The file above explains why the tab filter runs AFTER ranking rather than
// inside the retrieval SQL, and that argument still holds: the ranker's job is
// to pick the best items for this person, and rewriting eight candidate
// queries to know about tabs would be eight chances to get it wrong silently.
//
// It does NOT follow that the ranker has to score things the tab will throw
// away. A live Battles page:
//
//	app asks for                        20
//	candidates fetched and SCORED      250
//	composed into a mixed page          ~45   (3 per creator)
//	shorts thrown away                  ~36
//	served                                9
//
// Two hundred and fifty scored to deliver nine, and scoring is the expensive
// part of a feed request.
//
// Worse than the waste is the ceiling. composeFeed will not take more than
// maxItemsPerCreator from one creator, and it counts that against the MIXED
// page. So a creator whose three slots go to two shorts and one battle
// contributes one battle to the Battles tab — and their other battles are
// never looked at. The tab could only ever show
//
//	(creators × maxItemsPerCreator) × (this kind's share of the feed)
//
// which is roughly a quarter of what the rule intends. Raising the over-fetch
// cannot move that number, because the fetch is not what caps it. It was
// raised from 2x to 5x for exactly this symptom and the tab still returned
// nine.
//
// Narrowing here fixes both at once, in one place instead of eight. Every
// ranking stage still runs, in the same order, over the same kind of item —
// what changes is that the diversity rule now counts the thing the viewer is
// actually being shown.
//
// WHAT THIS DELIBERATELY DOES NOT DO
//
// It does not remove the filter that runs after composition. That one still
// earns its place: the audition floor and the suggested-accounts card are both
// injected AFTER the page is composed, so something the tab does not want can
// still arrive late. Two filters is not redundancy here, it is one filter per
// place an item can enter the page.

// narrowCandidatesToKind drops the candidates a single-kind tab cannot show.
//
// The mixed feed is returned untouched, as the caller's own slice, and pays
// nothing — not even a query.
//
// ════════════════════════════════════════════════════════════════════════════
// WHY THIS ASKS ITS OWN QUESTION INSTEAD OF CALLING populateTopResponses
// ════════════════════════════════════════════════════════════════════════════
//
// populateTopResponses is the obvious tool: it is what fills in
// TopResponseVideoUrl, which is what itemIsBattle reads. Using it here would
// be wrong twice over.
//
// It is far too much. It joins users, pulls the opponent's video, thumbnail,
// variants, manifest, username and league, and counts responses — everything
// needed to RENDER a battle. All this stage needs is a yes or no per id, and
// it needs it for the whole candidate pool rather than a page.
//
// And it would run twice. finalizeFeedItems calls populateTopResponses on the
// composed page regardless, which is where enrichment belongs — the page is
// twenty items, not up to eighteen hundred. Enriching the pool first would pay
// the expensive query on the big set and then pay it again on the small one.
//
// So: one column, one array parameter, one pass. The real enrichment still
// happens exactly where it always did.
// ════════════════════════════════════════════════════════════════════════════
// IF IT CANNOT TELL, IT CHANGES NOTHING
// ════════════════════════════════════════════════════════════════════════════
//
// This step is an optimisation and a ceiling fix. It is NOT what makes the tab
// correct — the filter after composition is, and it runs on an enriched page
// where battle-ness is known for certain.
//
// That ordering decides what to do when the question cannot be answered: a
// database blip, or no database at all under test. Guessing "nothing has been
// answered" would empty the Battles tab completely and let battles through
// onto the Shorts tab. Guessing the other way is no better.
//
// So it does not guess. An unanswerable question returns the pool untouched,
// the page composes exactly as it did before this function existed, and the
// filter downstream still serves the right kind. The tab loses its extra
// headroom for one request and nothing else.
func narrowCandidatesToKind(items []HomeFeedItem, kind string) []HomeFeedItem {
	if kind == feedKindAll || len(items) == 0 {
		return items
	}
	answered, ok := answeredChallengeIDs(items)
	if !ok {
		return items
	}
	return keepKind(items, answered, kind)
}

// keepKind is the filtering itself, split out so it can be exercised against a
// known answer instead of a database.
func keepKind(items []HomeFeedItem, answered map[string]bool, kind string) []HomeFeedItem {
	out := make([]HomeFeedItem, 0, len(items))
	for _, it := range items {
		// Anything that is not a challenge is furniture — a suggested-accounts
		// card — and belongs on every tab. Same rule as feedKindWantsItem.
		if it.Challenge == nil {
			out = append(out, it)
			continue
		}
		if answered[it.Challenge.ID] == (kind == feedKindBattles) {
			out = append(out, it)
		}
	}
	return out
}

// answeredChallengeIDs asks which of these challenges somebody has answered.
//
// "Answered" is the definition of a battle, and it is read from the responses
// table rather than from Challenge.ResponseCount because candidate sources
// populate that field inconsistently — several lanes leave it at zero on a
// challenge with responses, which would file a genuine battle as a short. The
// same reasoning is written out at itemIsBattle.
//
// An array parameter rather than a placeholder per id: the pool here can run
// to four figures, and building `IN ($1,$2,…$1800)` for that is a query plan
// nobody wants. warmContentAggregates already takes this shape.
//
// The second return is whether the question could be answered at all, and the
// caller leans on it — see narrowCandidatesToKind. False means no database, or
// a query that failed; an empty map with true means the real answer is that
// nobody has answered any of them.
func answeredChallengeIDs(items []HomeFeedItem) (map[string]bool, bool) {
	if db == nil {
		return nil, false
	}
	ids := make([]string, 0, len(items))
	for _, it := range items {
		if it.Challenge != nil && it.Challenge.ID != "" {
			ids = append(ids, it.Challenge.ID)
		}
	}
	if len(ids) == 0 {
		return map[string]bool{}, true
	}
	rows, err := db.Query(`
		SELECT DISTINCT CAST(challenge_id AS TEXT)
		  FROM challenge_responses
		 WHERE CAST(challenge_id AS TEXT) = ANY($1)`, pq.Array(ids))
	if err != nil {
		log.Printf("narrowCandidatesToKind: could not tell battles from shorts: %v", err)
		return nil, false
	}
	defer rows.Close()
	answered := make(map[string]bool, len(ids))
	for rows.Next() {
		var id string
		if rows.Scan(&id) == nil {
			answered[id] = true
		}
	}
	if err := rows.Err(); err != nil {
		// A partial read is the dangerous one: it looks like a complete answer
		// in which the rows that did not arrive were never answered, which
		// files real battles as shorts.
		log.Printf("narrowCandidatesToKind: partial read, leaving the pool alone: %v", err)
		return nil, false
	}
	return answered, true
}
