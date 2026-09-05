package main

import (
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
	for i := 0; i < 5; i++ {
		again := g.relatedTopics("jellyfish", 4)
		for j := range first {
			if first[j] != again[j] {
				t.Fatalf("order changed between calls: %v then %v", first, again)
			}
		}
	}
}
