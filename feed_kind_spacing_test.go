package main

import (
	"strconv"
	"strings"
	"testing"
)

// shape renders a page as a string of B (battle) and S (short) so a test
// failure shows the actual layout a user would have thumbed through.
func shape(items []HomeFeedItem) string {
	var sb strings.Builder
	for _, it := range items {
		if itemIsBattle(it) {
			sb.WriteByte('B')
		} else {
			sb.WriteByte('S')
		}
	}
	return sb.String()
}

// page builds a feed from a shape string. Ids are the position, so a test
// can assert that merit order survived inside each kind.
func page(shape string) []HomeFeedItem {
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

func ids(items []HomeFeedItem) []string {
	out := make([]string, len(items))
	for i, it := range items {
		out[i] = it.Challenge.ID
	}
	return out
}

// worstContestedRun returns the longest streak of a single kind that
// occurred while the OTHER kind still had items left to serve.
//
// The trailing streak is deliberately exempt: once one kind is exhausted
// there is nothing left to break the run with, and serving the remainder
// is the intended behaviour ("the battles still show up at the end").
// Measuring the raw longest run would fail every realistic page for the
// one reason that is not a defect.
func worstContestedRun(items []HomeFeedItem) int {
	remaining := map[bool]int{}
	for _, it := range items {
		remaining[itemIsBattle(it)]++
	}
	best, run := 0, 0
	var prev bool
	for i, it := range items {
		b := itemIsBattle(it)
		remaining[b]--
		if i == 0 || b != prev {
			run = 1
		} else {
			run++
		}
		prev = b
		if remaining[!b] > 0 && run > best {
			best = run
		}
	}
	return best
}

// frontHalfMix reports how many of each kind appear in the first half of
// a page — the part a user actually reaches in one sitting.
func frontHalfMix(items []HomeFeedItem) (battles, shorts int) {
	for _, it := range items[:len(items)/2] {
		if itemIsBattle(it) {
			battles++
		} else {
			shorts++
		}
	}
	return
}

// The production case. A For You page held 14 battles and 8 shorts and
// served every short first, because the shorts all carried today's
// timestamp and freshness is a scoring term. Eight swipes before the first
// battle is how a user concludes the app has no battles in it.
func TestSpaceOutFeedKinds_TheProductionClump(t *testing.T) {
	in := page("SSSSSSSSBBBBBBBBBBBBBB")

	out := spaceOutFeedKinds(in)

	if got := worstContestedRun(out); got > maxKindRun {
		t.Fatalf("run of %d in %s, want <= %d", got, shape(out), maxKindRun)
	}
	// Not just "a battle appears early" — both kinds must be spread
	// across the page. The front half of a 14/8 page should hold roughly
	// half of each kind, not all of one.
	b, sh := frontHalfMix(out)
	if b < 5 || sh < 2 {
		t.Fatalf("front half of %s holds %d battles / %d shorts; a "+
			"proportional payout should put about half of each kind there",
			shape(out), b, sh)
	}
	if len(out) != len(in) {
		t.Fatalf("page went from %d items to %d — spacing must not drop or "+
			"duplicate content", len(in), len(out))
	}
	// The point of the whole exercise: a battle is visible immediately,
	// not after eight swipes.
	first := strings.Index(shape(out), "B")
	if first > maxKindRun {
		t.Fatalf("first battle at position %d in %s; a user should see what "+
			"this app is within the first few reels", first, shape(out))
	}
}

func TestSpaceOutFeedKinds_NeverExceedsTheRunCap(t *testing.T) {
	cases := []string{
		"SSSSSSSSBBBBBBBBBBBBBB",
		"BBBBBBBBBBBBBBSSSSSSSS",
		"SBSBSBSBSB",
		"BBBSBBBSBBBS",
		"SSSSSSSSSSB",
		"BSSSSSSSSSS",
		"SSBBSSBBSSBB",
	}
	for _, c := range cases {
		out := spaceOutFeedKinds(page(c))
		if got := worstContestedRun(out); got > maxKindRun {
			t.Errorf("%s -> %s: run of %d, want <= %d",
				c, shape(out), got, maxKindRun)
		}
	}
}

// Spacing reorders; it must never invent, drop, or duplicate an item.
func TestSpaceOutFeedKinds_PreservesTheExactSetOfItems(t *testing.T) {
	in := page("SSSSSSSSBBBBBBBBBBBBBB")
	out := spaceOutFeedKinds(in)

	seen := map[string]int{}
	for _, id := range ids(out) {
		seen[id]++
	}
	if len(seen) != len(in) {
		t.Fatalf("got %d distinct ids from a %d-item page", len(seen), len(in))
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("id %s appears %d times", id, n)
		}
	}
}

// Merit still decides the order WITHIN each kind. The pass may only move
// an item across the other kind's items, never past one of its own.
func TestSpaceOutFeedKinds_KeepsMeritOrderInsideEachKind(t *testing.T) {
	out := spaceOutFeedKinds(page("SSSSSSSSBBBBBBBBBBBBBB"))

	var lastB, lastS = -1, -1
	for _, it := range out {
		n, _ := strconv.Atoi(it.Challenge.ID)
		if itemIsBattle(it) {
			if n < lastB {
				t.Fatalf("battle %d served after battle %d — spacing must not "+
					"re-rank within a kind, only interleave the two", n, lastB)
			}
			lastB = n
		} else {
			if n < lastS {
				t.Fatalf("short %d served after short %d", n, lastS)
			}
			lastS = n
		}
	}
}

// When one kind runs out the rest of the other simply follows. This is
// what makes "the battles still show up at the end" work without a
// special case.
func TestSpaceOutFeedKinds_TailIsWhicheverKindRemains(t *testing.T) {
	out := shape(spaceOutFeedKinds(page("SSBBBBBBBBBB")))

	if !strings.HasSuffix(out, "BBBBBB") {
		t.Fatalf("got %s; once the shorts are exhausted the remaining "+
			"battles must continue rather than being dropped", out)
	}
}

// A page that is ALREADY laid out the way proportional slotting would lay
// it out comes back byte-for-byte identical — the pass converges, it does
// not keep shuffling a page that is already right.
func TestSpaceOutFeedKinds_IsIdempotent(t *testing.T) {
	once := spaceOutFeedKinds(page("SSSSSSSSBBBBBBBBBBBBBB"))
	twice := spaceOutFeedKinds(once)

	for i, id := range ids(twice) {
		if id != ids(once)[i] {
			t.Fatalf("position %d moved on a second pass (%s -> %s); a "+
				"settled page must be a fixed point",
				i, shape(once), shape(twice))
		}
	}
}

// An evenly-mixed page is left alone: equal counts pay out one-for-one,
// which is what it already is.
func TestSpaceOutFeedKinds_LeavesAnAlternatingPageAlone(t *testing.T) {
	in := page("BSBSBSBSBS")
	before := ids(in)

	out := spaceOutFeedKinds(in)

	for i, id := range ids(out) {
		if id != before[i] {
			t.Fatalf("position %d changed from %s to %s: %s -> %s",
				i, before[i], id, shape(in), shape(out))
		}
	}
}

func TestSpaceOutFeedKinds_NoOpsOnDegenerateInput(t *testing.T) {
	if got := spaceOutFeedKinds(nil); got != nil {
		t.Fatalf("nil page = %v, want nil", got)
	}
	for _, c := range []string{"B", "SS", "BB", "BBBBB", "SSSSSS"} {
		in := page(c)
		if got := shape(spaceOutFeedKinds(in)); got != c {
			t.Fatalf("%s -> %s, want unchanged: a page with only one kind "+
				"has nothing to interleave", c, got)
		}
	}
}

// Classification must key off TopResponseVideoUrl, the field the client
// renders from — not ResponseCount, which several candidate sources leave
// at zero on genuine battles.
func TestItemIsBattle_UsesTheFieldTheClientRendersFrom(t *testing.T) {
	unfilled := HomeFeedItem{Type: "challenge",
		Challenge: &Challenge{ID: "1", ResponseCount: 3}}
	if itemIsBattle(unfilled) {
		t.Fatal("a challenge with no opponent url renders as a short on the " +
			"client no matter what ResponseCount claims; spacing it as a " +
			"battle would space the wrong things")
	}

	filled := HomeFeedItem{Type: "challenge",
		Challenge: &Challenge{ID: "2", TopResponseVideoUrl: "https://cdn/o.mp4"}}
	if !itemIsBattle(filled) {
		t.Fatal("an opponent url is what makes the client render a battle")
	}
}

func TestSpaceOutFeedKindsScored_MatchesThePlainFlavour(t *testing.T) {
	plain := page("SSSSSSSSBBBBBBBBBBBBBB")
	scored := make([]ScoredItem, len(plain))
	for i, it := range plain {
		scored[i] = ScoredItem{Item: it, Score: float64(len(plain) - i)}
	}

	out := spaceOutFeedKindsScored(scored)

	if len(out) != len(scored) {
		t.Fatalf("got %d items, want %d", len(out), len(scored))
	}
	flat := make([]HomeFeedItem, len(out))
	for i, s := range out {
		flat[i] = s.Item
	}
	if got := worstContestedRun(flat); got > maxKindRun {
		t.Fatalf("run of %d in %s", got, shape(flat))
	}
	// Each item must still carry ITS OWN score, not the score of whatever
	// used to sit at that position.
	for _, s := range out {
		n, _ := strconv.Atoi(s.Item.Challenge.ID)
		if want := float64(len(plain) - n); s.Score != want {
			t.Fatalf("item %s carries score %v, want %v — the wrapper was "+
				"re-attached to the wrong item", s.Item.Challenge.ID, s.Score, want)
		}
	}
}
