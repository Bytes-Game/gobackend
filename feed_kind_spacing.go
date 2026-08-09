package main

// Positional spacing of battles and shorts within an already-ranked page.
//
// THE PROBLEM THIS SOLVES
//
// Every feed surface picks a healthy mix of the two content kinds — the
// cold-start pool is budgeted 70/30 battles/shorts, and the warm path's
// candidate sources draw from both. What no surface did was control WHERE
// each kind landed once the ranker had sorted the page by merit.
//
// Merit is correlated with kind, so the mix clumped. Observed in
// production: a For You page holding 14 battles and 8 shorts served all
// eight shorts first, because every seeded short carried today's timestamp
// and every battle was a day or more old, and freshness is a scoring term.
// The user swiped past eight single videos and concluded the app had no
// battles in it. The battles were there — starting at swipe nine.
//
// THE FIX
//
// One pass, applied after ranking: PROPORTIONAL SLOTTING. Each kind is
// paid out across the page in proportion to how much of it the page
// holds, drawing from the top of that kind's merit order every time. A
// page of 14 battles and 8 shorts therefore reads roughly B B S B B S B S
// — both kinds present from the first screen and spread the whole way
// down — instead of all 8 shorts and then all 14 battles.
//
// The obvious cheaper rule, "take the highest-merit item unless it would
// make a run of three", was tried first and is not good enough: it drains
// the front-loaded kind at the cap rate and still leaves an eleven-battle
// tail (SSBSSBSSBSS + BBBBBBBBBBB). It fixes the headline symptom while
// leaving the first half of the page lopsided.
//
// Properties worth stating, because they are the whole point:
//
//   - Merit still decides WHICH items make the page and their order
//     within each kind: the best battle is still the first battle shown,
//     the best short the first short. What changes is only the pattern
//     the two are woven into. This is slotting, not re-ranking, and it
//     does not touch the cold-start exploration budget or the candidate
//     mix — those already do their job; nothing about them is wasted.
//   - When one kind runs out, the remainder of the other simply follows.
//     So a page that is mostly shorts still ends with its battles rather
//     than dropping them — "show them at the end when the rest is
//     finished" is a free consequence, not a special case.
//   - maxKindRun then sits on top as a safety net for the mixes where
//     proportional payout alone would still stack three of a kind.
//   - Single-kind pages and pages under three items are no-ops.
//
// WHERE IT RUNS
//
// After finalizeFeedItems, never before. Whether an item is a battle is
// decided by TopResponseVideoUrl — the exact field the client keys its own
// battle rendering off (isBattle => opponentVideoUrl.isNotEmpty). That
// field is populated by populateTopResponses inside finalize; run this
// pass earlier and it would classify by candidate-source metadata that
// several lanes leave unset, i.e. it would space out the wrong things.

// maxKindRun is how many battles, or shorts, may appear back to back
// before the other kind is forced in.
//
// 2 rather than 1: strict alternation is not what the big feeds do and it
// reads as mechanical, plus it would override merit on nearly every slot.
// 2 guarantees a battle inside the first three reels of any page that has
// one, which is what "the user sees what this app is" requires, while
// leaving most of the ranker's ordering untouched.
const maxKindRun = 2

// itemIsBattle reports whether a feed item will render as a battle on the
// client. Mirrors the client's own test rather than trusting
// ResponseCount, which individual candidate sources populate
// inconsistently; populateTopResponses self-heals both, and this field is
// the one it fills from the responses table itself.
func itemIsBattle(it HomeFeedItem) bool {
	return it.Challenge != nil && it.Challenge.TopResponseVideoUrl != ""
}

// spaceOutFeedKinds returns items reordered so neither battles nor shorts
// run longer than maxKindRun consecutively, preserving merit order within
// each kind. Safe on any input: pages shorter than 3, pages of a single
// kind, and nil all come back untouched.
func spaceOutFeedKinds(items []HomeFeedItem) []HomeFeedItem {
	if len(items) < 3 {
		return items
	}

	// Split into two queues, each holding indices into items so relative
	// merit order is preserved for free and the comparison below can tell
	// which queue head the ranker actually placed higher.
	battles := make([]int, 0, len(items))
	shorts := make([]int, 0, len(items))
	for i, it := range items {
		if itemIsBattle(it) {
			battles = append(battles, i)
		} else {
			shorts = append(shorts, i)
		}
	}
	// Nothing to interleave — one kind owns the whole page.
	if len(battles) == 0 || len(shorts) == 0 {
		return items
	}

	out := make([]HomeFeedItem, 0, len(items))
	bi, si := 0, 0
	// runIsBattle/runLen track the tail of `out` so the cap can be applied
	// without re-scanning it.
	runIsBattle := false
	runLen := 0

	for bi < len(battles) || si < len(shorts) {
		var takeBattle bool
		switch {
		case bi >= len(battles):
			takeBattle = false
		case si >= len(shorts):
			takeBattle = true
		default:
			// Both available: pay out whichever kind is further behind
			// its proportional share of the page.
			//
			// (bi+0.5)/len(battles) is how far through the battle queue
			// we are, measured at the midpoint of the item about to be
			// served; same for shorts. Serving whichever fraction is
			// SMALLER means serving whichever kind has been served least
			// relative to how much of it there is — which spreads both
			// kinds evenly from the first slot to the last, for any
			// ratio, with no tuning constants. (This is the standard
			// largest-remainder interleave; the halves are what keep it
			// symmetric rather than biased toward whichever kind is
			// tested first.)
			bFrac := (float64(bi) + 0.5) / float64(len(battles))
			sFrac := (float64(si) + 0.5) / float64(len(shorts))
			takeBattle = bFrac <= sFrac
			// Safety net: never extend an at-cap run. The other queue is
			// known non-empty in this branch, so the swap always has
			// something to swap in.
			if runLen >= maxKindRun && takeBattle == runIsBattle {
				takeBattle = !takeBattle
			}
		}

		var idx int
		if takeBattle {
			idx = battles[bi]
			bi++
		} else {
			idx = shorts[si]
			si++
		}
		out = append(out, items[idx])

		if len(out) == 1 || takeBattle != runIsBattle {
			runIsBattle = takeBattle
			runLen = 1
		} else {
			runLen++
		}
	}
	return out
}

// spaceOutFeedKindsScored is the ScoredItem-slice flavour, for the feed
// paths that carry scoring metadata through to the response.
func spaceOutFeedKindsScored(items []ScoredItem) []ScoredItem {
	if len(items) < 3 {
		return items
	}
	plain := make([]HomeFeedItem, len(items))
	for i, s := range items {
		plain[i] = s.Item
	}
	ordered := spaceOutFeedKinds(plain)

	// Re-attach each item's own ScoredItem wrapper. Matching by identity
	// (position in the original slice) rather than by id keeps this
	// correct when a page legitimately carries the same challenge twice.
	used := make([]bool, len(items))
	out := make([]ScoredItem, 0, len(items))
	for _, want := range ordered {
		for i, s := range items {
			if used[i] {
				continue
			}
			if sameFeedItem(s.Item, want) {
				out = append(out, s)
				used[i] = true
				break
			}
		}
	}
	// Defensive: if anything failed to match, keep the original page
	// rather than serving a short one. Losing an item to a reordering
	// helper would be a far worse bug than an imperfectly spaced page.
	if len(out) != len(items) {
		return items
	}
	return out
}

func sameFeedItem(a, b HomeFeedItem) bool {
	if a.Type != b.Type {
		return false
	}
	if a.Challenge == nil || b.Challenge == nil {
		return a.Challenge == b.Challenge
	}
	return a.Challenge == b.Challenge
}
