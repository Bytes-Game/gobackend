package main

import (
	"os"
	"strings"
	"testing"
	"time"
)

// ════════════════════════════════════════════════════════════════════════════
// WHICH SUBJECTS GO TOGETHER
// ════════════════════════════════════════════════════════════════════════════
//
// Searching "aquarium" used to find nothing, on a platform with a jellyfish
// video that the model had described as being about an aquarium. The word was
// in the database and unreachable.
//
// The graph below is built from the real shape of this catalogue — these are
// the actual relationships it produces today.

// catalogueGraph builds the graph from topic sets directly, so these tests do
// not need a database.
func catalogueGraph(t *testing.T, sets [][]string) *topicGraph {
	t.Helper()
	g := &topicGraph{related: map[string]map[string]float64{}, builtAt: time.Now()}
	freq := map[string]int{}
	pair := map[string]map[string]int{}
	for _, ws := range sets {
		for _, w := range ws {
			freq[w]++
		}
		for i, a := range ws {
			for _, b := range ws[i+1:] {
				if pair[a] == nil {
					pair[a] = map[string]int{}
				}
				if pair[b] == nil {
					pair[b] = map[string]int{}
				}
				pair[a][b]++
				pair[b][a]++
			}
		}
	}
	for a, others := range pair {
		row := map[string]float64{}
		for b, n := range others {
			if n < topicGraphMinPairs {
				continue
			}
			s := float64(n) / float64(freq[a])
			if s > 1 {
				s = 1
			}
			row[b] = s
		}
		if len(row) > 0 {
			g.related[a] = row
		}
	}
	return g
}

func seaAndGarden() [][]string {
	return [][]string{
		{"jellyfish", "aquarium", "marine life", "underwater"},
		{"jellyfish", "aquarium", "marine life", "deep sea"},
		{"jellyfish", "aquarium", "underwater", "deep sea"},
		{"bee", "flower", "pollination", "thistle", "nature"},
		{"bee", "flower", "pollination", "thistle", "nature"},
		{"butterfly", "flower", "nature", "insect"},
		{"bee", "flower", "pollination", "nature"},
		{"leaves", "nature", "plant"},
		{"leaves", "nature", "plant"},
	}
}

func TestGraph_FindsTheSubjectNobodyWroteDown(t *testing.T) {
	// The whole point. Nobody ever recorded that aquariums relate to
	// jellyfish; the catalogue said so by putting them on the same videos.
	g := catalogueGraph(t, seaAndGarden())
	got := g.relatedTopics("aquarium", 4)
	found := false
	for _, w := range got {
		if w == "jellyfish" {
			found = true
		}
	}
	if !found {
		t.Errorf("searching aquarium does not reach jellyfish; got %v.\n\n"+
			"This is the search that returned nothing before: the word "+
			"aquarium is in no title and is not one of the eighteen "+
			"categories, so only the videos it describes can supply it.", got)
	}
}

func TestGraph_KeepsUnrelatedThingsApart(t *testing.T) {
	// Widening a search must not turn it into "here is the whole catalogue".
	g := catalogueGraph(t, seaAndGarden())
	for _, w := range g.relatedTopics("jellyfish", 6) {
		if w == "flower" || w == "bee" || w == "thistle" {
			t.Errorf("jellyfish widened to %q — the sea and the garden share "+
				"no video and must not be pulled together", w)
		}
	}
}

func TestGraph_IsAsymmetricOnPurpose(t *testing.T) {
	// Nearly every thistle video is also about nature. Hardly any nature video
	// is about thistles. Collapsing that into one number would make every
	// nature video a thistle video, which is the coarse behaviour this exists
	// to escape.
	g := catalogueGraph(t, seaAndGarden())
	thistleToNature := g.related["thistle"]["nature"]
	natureToThistle := g.related["nature"]["thistle"]
	if thistleToNature <= natureToThistle {
		t.Errorf("thistle→nature is %.2f and nature→thistle is %.2f. The "+
			"narrow subject should imply the broad one far more strongly "+
			"than the reverse.", thistleToNature, natureToThistle)
	}
}

func TestGraph_OneSharedVideoIsACoincidence(t *testing.T) {
	// Two subjects meeting once is not a relationship. Without a floor, every
	// pair of words that ever shared an upload becomes a search expansion.
	g := catalogueGraph(t, [][]string{
		{"cricket", "unrelated thing"},
		{"cricket", "bat"},
		{"cricket", "bat"},
	})
	for _, w := range g.relatedTopics("cricket", 5) {
		if w == "unrelated thing" {
			t.Error("a single shared video counted as a relationship")
		}
	}
	if len(g.relatedTopics("cricket", 5)) == 0 {
		t.Error("the genuinely repeated pair was dropped too")
	}
}

// ════════════════════════════════════════════════════════════════════════════
// WIDENING A SEARCH
// ════════════════════════════════════════════════════════════════════════════

func TestExpand_NeverLosesWhatWasAskedFor(t *testing.T) {
	// Widening ADDS. A search that quietly replaced the term with something
	// related would answer a question nobody asked.
	got := expandQuery("jellyfish")
	if len(got) == 0 || got[0] != "jellyfish" {
		t.Errorf("got %v; the original term must come first and never be "+
			"dropped, however the graph is feeling", got)
	}
}

func TestExpand_HandlesAQueryTheCatalogueHasNeverSeen(t *testing.T) {
	// Most searches on a small platform are for things it does not have. That
	// must return the term alone, not nothing — returning nothing would make
	// the search match everything or nothing depending on the caller.
	got := expandQuery("something nobody has ever uploaded")
	if len(got) != 1 {
		t.Errorf("got %v, want just the query itself", got)
	}
}

func TestExpand_IgnoresAnEmptyOrPunctuationQuery(t *testing.T) {
	for _, q := range []string{"", "   ", "!!!", "#"} {
		if got := expandQuery(q); len(got) != 0 {
			t.Errorf("expandQuery(%q) = %v, want nothing — a query that folds "+
				"to empty would otherwise match the whole catalogue", q, got)
		}
	}
}

func TestRelated_DoesNotSuggestWhatWasJustTyped(t *testing.T) {
	// "You searched jellyfish. Did you mean jellyfish?"
	for _, w := range relatedSearches("jellyfish", 5) {
		if w == "jellyfish" {
			t.Error("the query was offered back as its own suggestion")
		}
	}
}

func TestGraph_StaysUsableWithNoDatabase(t *testing.T) {
	// Search must degrade to a narrower search, never to an error. buildTopicGraph
	// is called on a cold cache and db is nil in tests.
	g := buildTopicGraph()
	if g == nil {
		t.Fatal("buildTopicGraph returned nil; every caller would panic")
	}
	if got := g.relatedTopics("anything", 3); got != nil {
		t.Errorf("got %v from an empty graph, want nothing", got)
	}
}

func TestGraph_BoundsHowFarAQueryIsWidened(t *testing.T) {
	// Past a handful the extra subjects are weakly related and start pulling
	// in everything, which is the same as not searching.
	if topicGraphMaxRelated <= 0 || topicGraphMaxRelated > 10 {
		t.Errorf("topicGraphMaxRelated is %d; unbounded widening turns every "+
			"search into the whole catalogue", topicGraphMaxRelated)
	}
	g := catalogueGraph(t, seaAndGarden())
	if got := g.relatedTopics("jellyfish", 2); len(got) > 2 {
		t.Errorf("asked for 2, got %d", len(got))
	}
}

func TestGraph_SuggestionsDoNotShuffle(t *testing.T) {
	// Equal scores must break ties consistently, or the chips under the search
	// box reorder themselves on every keystroke.
	g := catalogueGraph(t, seaAndGarden())
	first := g.relatedTopics("jellyfish", 4)
	if len(first) == 0 {
		t.Fatal("no suggestions at all, so this proves nothing about ordering")
	}
	for i := 0; i < 5; i++ {
		again := g.relatedTopics("jellyfish", 4)
		// Compared as whole slices rather than element by element: a shorter
		// or longer answer is just as much a change as a reordered one, and
		// indexing one against the other would read past the end of it.
		if strings.Join(again, "|") != strings.Join(first, "|") {
			t.Fatalf("order changed between calls: %v then %v", first, again)
		}
	}
}

// ════════════════════════════════════════════════════════════════════════════
// A COMMON SUBJECT TELLS YOU ALMOST NOTHING
// ════════════════════════════════════════════════════════════════════════════
//
// Searching "thistle" returned a video about a TREE HOUSE. Both are described
// as being about "nature", the graph linked them, and the search followed the
// link. The link is real and carries almost no information: "nature" sits on
// a large share of the catalogue and "pollination" sits on almost none.
//
// This also replaced the reason the co-occurrence floor was set to two. That
// was justified in the code as "the platform is small", which is a rule fitted
// to today's data — it would have needed revisiting at every scale. Specificity
// is a RATIO and behaves identically on a hundred videos and a hundred million.

func specGraph(t *testing.T, sets [][]string) *topicGraph {
	t.Helper()
	g := catalogueGraph(t, sets)
	g.freq = map[string]int{}
	for _, ws := range sets {
		for _, w := range ws {
			g.freq[w]++
		}
	}
	g.docs = len(sets)
	return g
}

func TestSpecificity_ACommonSubjectCountsForLess(t *testing.T) {
	// "nature" on most videos, "pollination" on two.
	sets := [][]string{
		{"bee", "pollination", "thistle", "nature"},
		{"bee", "pollination", "thistle", "nature"},
		{"tree house", "forest", "nature"},
		{"jellyfish", "aquarium", "nature"},
		{"cricket", "stadium", "nature"},
		{"biryani", "rice", "nature"},
	}
	g := specGraph(t, sets)

	common := g.topicSpecificity("nature")
	rare := g.topicSpecificity("pollination")
	if rare <= common {
		t.Errorf("pollination scored %.2f and nature %.2f. A subject on almost "+
			"nothing has to tell you more than one on almost everything, or "+
			"searching thistle keeps returning tree houses.", rare, common)
	}
	if common < 0 || rare > 1 {
		t.Errorf("outside 0..1: nature %.2f, pollination %.2f", common, rare)
	}
}

func TestSpecificity_TheSpecificRelatedSubjectLeads(t *testing.T) {
	// The visible effect. Measured on the real catalogue before and after:
	//   thistle -> pollination, nature, insect, flower, bee
	//   thistle -> pollination, bee, insect, flower, nature
	sets := [][]string{
		{"bee", "pollination", "thistle", "nature"},
		{"bee", "pollination", "thistle", "nature"},
		{"tree house", "forest", "nature"},
		{"jellyfish", "aquarium", "nature"},
		{"cricket", "stadium", "nature"},
	}
	g := specGraph(t, sets)
	got := g.relatedTopics("thistle", 5)
	if len(got) == 0 {
		t.Fatal("no related subjects at all")
	}
	posOf := func(w string) int {
		for i, v := range got {
			if v == w {
				return i
			}
		}
		return -1
	}
	p, n := posOf("pollination"), posOf("nature")
	if p < 0 {
		t.Fatalf("pollination is missing from %v", got)
	}
	if n >= 0 && p > n {
		t.Errorf("got %v — nature outranks pollination, so the vaguest link "+
			"leads and the search follows it into unrelated videos", got)
	}
}

func TestSpecificity_OnEverythingIsWorthNothing(t *testing.T) {
	// A subject every video carries separates nothing from anything.
	sets := [][]string{
		{"video", "a"}, {"video", "b"}, {"video", "c"},
	}
	g := specGraph(t, sets)
	if got := g.topicSpecificity("video"); got != 0 {
		t.Errorf("a subject on every video scored %.2f, want 0", got)
	}
}

func TestSpecificity_UnknownSubjectsAreNotSilentlyDowngraded(t *testing.T) {
	// A word outside the graph, or a graph that has not been built, must
	// behave as it did before this existed — not quietly weaken every match.
	var empty *topicGraph
	if got := empty.topicSpecificity("anything"); got != 1 {
		t.Errorf("nil graph gave %.2f, want 1", got)
	}
	g := specGraph(t, [][]string{{"a", "b"}, {"a", "c"}})
	if got := g.topicSpecificity("never seen"); got != 1 {
		t.Errorf("unknown subject gave %.2f, want 1", got)
	}
}

func TestGraph_ThresholdIsNotFittedToCatalogueSize(t *testing.T) {
	// The floor was set to two and justified in the comment as "the platform
	// is small". That is a rule tuned to today's data: it would need
	// revisiting at every scale, and nobody would notice when it should have
	// been.
	//
	// Two is now defended as the least evidence from which ANY pattern can be
	// inferred — one shared video is a single coincidence — and the work of
	// staying honest at scale is done by specificity, which is a ratio.
	src, err := os.ReadFile("topic_graph.go")
	if err != nil {
		t.Fatalf("read topic_graph.go: %v", err)
	}
	s := string(src)
	if strings.Contains(s, "deliberately low because the") {
		t.Error("the co-occurrence floor is still justified by how small this " +
			"platform is, which is a rule fitted to the current catalogue")
	}
	if !strings.Contains(s, "topicGraphMinStrength") {
		t.Error("there is no proportional strength floor, so relatedness rests " +
			"on an absolute count — which means something different at every " +
			"scale")
	}
}
