package main

import (
	"math"
	"strings"
	"testing"
)

// ════════════════════════════════════════════════════════════════════════════
// The bug this file exists for
// ════════════════════════════════════════════════════════════════════════════
//
// Searching "pollination" on the live server returned a tree house, a caption
// contest and a prop challenge — the same three videos as searching "thistle",
// or "bee", or anything else that did not match a title.
//
// It looked like relevance was broken. It was not. Relevance was one term in a
// sum, and it had already been flattened into a position before it got there,
// so a couple of thousand seeded views beat it every time.

// searchScore mirrors the shape of the final ranking: relevance decides, and
// everything else is a capped multiplier on it.
func searchScore(rel, boost float64) float64 {
	if boost > searchBoostCeiling {
		boost = searchBoostCeiling
	}
	if boost < 0 {
		boost = 0
	}
	return rel * (1 + boost)
}

// The headline: being popular must not turn a video about something else into
// an answer.
func TestSearchRank_PopularityCannotBeatBeingAboutIt(t *testing.T) {
	// The video that is actually about the query, with no engagement at all.
	onTopic := searchScore(1.0, 0)
	// A seeded favourite with thousands of views, fresh, and personally apt —
	// but only faintly related to what was typed.
	popular := searchScore(0.25, 10.0) // boost far past the ceiling

	if popular >= onTopic {
		t.Fatalf("a barely-related video with every boost going scored %.3f "+
			"and the video actually about the query scored %.3f. This is the "+
			"live bug: searching \"pollination\" returned a tree house.",
			popular, onTopic)
	}
}

// A video with nothing of the query in it scores nothing, whatever else is
// true about it. This is what multiplying buys that adding never could.
func TestSearchRank_NoRelevanceIsNoResult(t *testing.T) {
	if got := searchScore(0, 10.0); got != 0 {
		t.Errorf("a video with no relevance scored %.3f. Under the old sum, "+
			"engagement and recency alone were worth ~0.4, which is how "+
			"unrelated videos reached the top of every search.", got)
	}
}

// Boosts still have a job: separating results that are about equally relevant.
func TestSearchRank_PopularityStillBreaksTiesAmongRelevantResults(t *testing.T) {
	plain := searchScore(1.0, 0)
	loved := searchScore(1.0, 0.4)
	if loved <= plain {
		t.Error("between two equally relevant results, the more popular and " +
			"fresher one should win — that is what these signals are for")
	}
}

// The ceiling is the line between "reorders relevant results" and "is the
// ranking".
func TestSearchRank_TheCeilingHoldsTheLine(t *testing.T) {
	// Half as relevant, with everything going for it, must still lose.
	if searchScore(0.5, 100) >= searchScore(1.0, 0) {
		t.Error("a video half as relevant as another climbed past it on " +
			"boosts alone. The ceiling is not holding.")
	}
	// A near-tie on relevance should be winnable.
	if searchScore(0.9, 0.5) <= searchScore(1.0, 0) {
		t.Error("a result almost as relevant, and much more popular, could " +
			"not overtake — boosts have stopped mattering at all")
	}
	if searchBoostCeiling <= 0 || searchBoostCeiling >= 1 {
		t.Fatalf("the ceiling is %v; outside 0..1 it is either switched off "+
			"or lets popularity double a score", searchBoostCeiling)
	}
}

// Relevance is normalised against the best match in the set, so the ordering
// depends on how results compare to each other rather than on the raw weights
// happening to land in any particular range.
func TestSearchRank_NormalisingKeepsTheOrder(t *testing.T) {
	raw := []float64{120, 55, 12, 3}
	maxRel := raw[0]
	var prev float64 = math.Inf(1)
	for i, r := range raw {
		got := searchScore(r/maxRel, 0)
		if got > prev {
			t.Errorf("normalising reordered results at %d: %.3f after %.3f",
				i, got, prev)
		}
		prev = got
	}
}

// A guard on the shape of the thing, so the sum cannot quietly come back.
func TestSearchRank_RelevanceIsNotJustAnotherTerm(t *testing.T) {
	// Under the old sum, a zero-relevance video still scored ~0.4 from
	// engagement and recency alone. Under the current shape it scores 0.
	oldStyle := 0.0 + 0.20*1.0 + 0.20*1.0 // lex=0, full eng, full recency
	if got := searchScore(0, 0.4); got >= oldStyle {
		t.Errorf("an irrelevant video scores %.3f, which is no better than "+
			"the %.3f it got under the sum this replaced", got, oldStyle)
	}
}

// The real code, not a mirror of it. The mirror above can drift; this cannot.
func TestSearchRank_TheRealCodeStillMultiplies(t *testing.T) {
	// Only the code. This file explains the old formula in a comment, and a
	// guard that matched its own explanation would fire forever.
	src := codeOnly(funcBody(readSourceFile(t, "search.go"), "func rankSearchChallenges("))

	if strings.Contains(src, "lex + 0.20*eng") {
		t.Error("the old sum is back in rankSearchChallenges. Relevance is a " +
			"term in it again, so a few thousand views will once more decide " +
			"every search regardless of what was typed.")
	}
	if strings.Contains(src, "math.Exp(-float64(i) / 8.0)") {
		t.Error("ranking is back to decaying a result's POSITION instead of " +
			"using how relevant it actually is. That throws away the " +
			"difference between an exact match and a vague one.")
	}
	if !strings.Contains(src, "rel * (1 + boost)") {
		t.Error("rankSearchChallenges no longer multiplies relevance by its " +
			"boosts, so something with no relevance can score above zero again")
	}
	if !strings.Contains(src, "searchBoostCeiling") {
		t.Error("the boost ceiling is gone from rankSearchChallenges")
	}
}

// codeOnly strips whole-line comments so a guard checks what runs, not what is
// written about what used to run.
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
