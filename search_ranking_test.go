package main

import (
	"sort"
	"strings"
	"testing"
)

// ════════════════════════════════════════════════════════════════════════════
// What this ranking has to do, which is two things at once
// ════════════════════════════════════════════════════════════════════════════
//
//  1. Show people what everyone is watching. Popular videos being easy to
//     find is most of why anybody enjoys a short-video app, and holding them
//     back to protect a relevance score makes a worse product.
//
//  2. Never let being popular turn a video about something else into an
//     answer. Searching "bee" must not lead with a tree house because the
//     tree house has more views.
//
// A single blended score cannot do both — it has to pick one exchange rate
// between "is about this" and "is loved", and every rate is wrong somewhere.
// Classes do both: popularity runs free inside a class and cannot cross one.

type ranked struct {
	id         string
	tier       int
	popularity float64
	rel        float64
}

// order mirrors the real sort: class first, then how well it is doing.
func order(in []ranked) []string {
	out := append([]ranked(nil), in...)
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].tier != out[j].tier {
			return out[i].tier < out[j].tier
		}
		return out[i].popularity+out[i].rel*searchRelevanceTiebreak >
			out[j].popularity+out[j].rel*searchRelevanceTiebreak
	})
	ids := make([]string, len(out))
	for i, r := range out {
		ids[i] = r.id
	}
	return ids
}

// ── What the user asked for ─────────────────────────────────────────────────

func TestSearchRank_PopularityDecidesAmongEquallyGoodMatches(t *testing.T) {
	got := order([]ranked{
		{id: "obscure", tier: matchTierAbout, popularity: 0.05, rel: 1.0},
		{id: "loved", tier: matchTierAbout, popularity: 0.90, rel: 0.9},
	})
	if got[0] != "loved" {
		t.Errorf("got %v; among videos that are all about the thing, the one "+
			"people actually watch should lead — that is most of the point of "+
			"a short-video app", got)
	}
}

// There is no ceiling inside a class. A hugely popular video should be able to
// climb past a slightly better match by any margin.
func TestSearchRank_PopularityIsNotHeldBackInsideAClass(t *testing.T) {
	got := order([]ranked{
		{id: "best-match", tier: matchTierAbout, popularity: 0.0, rel: 1.0},
		{id: "runaway-hit", tier: matchTierAbout, popularity: 5.0, rel: 0.2},
	})
	if got[0] != "runaway-hit" {
		t.Errorf("got %v; inside one class popularity is deliberately "+
			"uncapped, so a runaway hit should be able to lead", got)
	}
}

// ── The line it must never cross ────────────────────────────────────────────

func TestSearchRank_APopularNearMissNeverBeatsARealMatch(t *testing.T) {
	got := order([]ranked{
		// A tree house. Shares only the word "nature" with a search for bees.
		{id: "treehouse", tier: matchTierRelated, popularity: 100.0, rel: 0.2},
		// The actual bee video, with nothing going for it but being right.
		{id: "bees", tier: matchTierAbout, popularity: 0.0, rel: 1.0},
	})
	if got[0] != "bees" {
		t.Fatalf("got %v; a merely-related video reached the top on views "+
			"alone. This is the whole thing the classes exist to prevent.", got)
	}
}

func TestSearchRank_ClassesHoldAtEveryLevel(t *testing.T) {
	got := order([]ranked{
		{id: "related-huge", tier: matchTierRelated, popularity: 999},
		{id: "partial-big", tier: matchTierPartial, popularity: 50},
		{id: "about-tiny", tier: matchTierAbout, popularity: 0.01},
	})
	want := []string{"about-tiny", "partial-big", "related-huge"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v — classes must hold however lopsided "+
				"the popularity is", got, want)
		}
	}
}

// The case from the live server that started this.
func TestSearchRank_TheLiveCaseComesOutRight(t *testing.T) {
	// Searching "pollination".
	got := order([]ranked{
		// tree house, forest, moss, nature — shares only "nature"
		{id: "120-treehouse", tier: matchTierRelated, popularity: 0.80},
		// plant, flower, petals, nature — shares "flower", a real topic match
		{id: "119-flowers", tier: matchTierPartial, popularity: 0.10},
		// bee, flower, pollination, thistle — the actual answer
		{id: "117-bees", tier: matchTierAbout, popularity: 0.50},
	})
	want := []string{"117-bees", "119-flowers", "120-treehouse"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v. The bee video answers the question, "+
				"the flower video is closer than the tree house, and the tree "+
				"house should not outrank either on views.", got, want)
		}
	}
}

// ── Tiebreak ────────────────────────────────────────────────────────────────

func TestSearchRank_ABetterMatchBreaksATie(t *testing.T) {
	got := order([]ranked{
		{id: "weaker", tier: matchTierAbout, popularity: 0.5, rel: 0.3},
		{id: "stronger", tier: matchTierAbout, popularity: 0.5, rel: 1.0},
	})
	if got[0] != "stronger" {
		t.Errorf("got %v; with popularity level, the better match should edge "+
			"ahead", got)
	}
}

func TestSearchRank_TheTiebreakStaysATiebreak(t *testing.T) {
	if searchRelevanceTiebreak <= 0 || searchRelevanceTiebreak > 0.2 {
		t.Fatalf("the tiebreak is %v. Above a small fraction it stops being a "+
			"tiebreak and starts holding popular videos back again, which is "+
			"the thing the classes were introduced to stop needing.",
			searchRelevanceTiebreak)
	}
}

// ── The classes themselves ──────────────────────────────────────────────────

func TestSearchTier_TheQueryBeingOneOfItsWordsIsTheTopClass(t *testing.T) {
	cases := []struct {
		name string
		ch   Challenge
		doc  searchDoc
		want int
	}{
		{"exact topic", Challenge{Subject: "x"},
			searchDoc{Topics: []string{"bee", "pollination"}}, matchTierAbout},
		{"word in title", Challenge{Subject: "the bee and the thistle"},
			searchDoc{}, matchTierAbout},
		{"exact tag", Challenge{Subject: "x"},
			searchDoc{Tags: []string{"bee"}}, matchTierAbout},
		{"part of a topic", Challenge{Subject: "x"},
			searchDoc{Topics: []string{"beekeeping"}}, matchTierPartial},
		{"only said aloud", Challenge{Subject: "x"},
			searchDoc{Spoken: "look at that bee go"}, matchTierRelated},
		{"nothing at all", Challenge{Subject: "x"}, searchDoc{}, matchTierNone},
	}
	for _, c := range cases {
		_, got := searchRelevanceDetail(c.ch, c.doc, "bee", nil)
		if got != c.want {
			t.Errorf("%s: class %d, want %d", c.name, got, c.want)
		}
	}
}

func TestSearchTier_NoMatchIsNeverAResult(t *testing.T) {
	score, tier := searchRelevanceDetail(
		Challenge{Subject: "something else"}, searchDoc{}, "bee", nil)
	if score != 0 || tier != matchTierNone {
		t.Errorf("a video with none of the query scored %v in class %d; it "+
			"must be no result at all", score, tier)
	}
}

// The plain scorer must keep working — plenty of code and tests still use it.
func TestSearchTier_ThePlainScorerStillAgrees(t *testing.T) {
	ch := Challenge{Subject: "x"}
	doc := searchDoc{Topics: []string{"bee"}}
	plain := searchRelevance(ch, doc, "bee", nil)
	detailed, _ := searchRelevanceDetail(ch, doc, "bee", nil)
	if plain != detailed {
		t.Errorf("the two scorers disagree: %v vs %v", plain, detailed)
	}
}

// ── Guard on the real code ──────────────────────────────────────────────────

func TestSearchRank_TheRealCodeStillSortsByClassFirst(t *testing.T) {
	// Only the code. This file and search.go both explain the old formulas in
	// comments, and a guard matching its own explanation would fire forever.
	src := codeOnly(funcBody(readSourceFile(t, "search.go"), "func rankSearchChallenges("))

	if strings.Contains(src, "lex + 0.20*eng") {
		t.Error("the old sum is back: relevance is a term in it again, so a " +
			"few thousand views will once more decide every search")
	}
	if strings.Contains(src, "math.Exp(-float64(i) / 8.0)") {
		t.Error("ranking is back to decaying a result's POSITION instead of " +
			"using how relevant it actually is")
	}
	if !strings.Contains(src, "out[i].Tier != out[j].Tier") {
		t.Error("results are no longer sorted by match class first. Without " +
			"that, popularity can promote a merely-related video above one " +
			"that actually answers the query.")
	}
	if !strings.Contains(src, "searchRelevanceDetail(") {
		t.Error("the ranker no longer asks which class a match is in")
	}
}

func codeOnly(src string) string {
	var b strings.Builder
	for _, line := range strings.Split(src, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "//") {
			continue
		}
		b.WriteString(line)
		b.WriteString("\n")
	}
	return b.String()
}

// funcBody returns just one function, so a guard about how challenges are
// ranked cannot be tripped by how accounts are.
func funcBody(src, decl string) string {
	i := strings.Index(src, decl)
	if i < 0 {
		return ""
	}
	rest := src[i:]
	if end := strings.Index(rest, "\n}\n"); end > 0 {
		return rest[:end]
	}
	return rest
}
