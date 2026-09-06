package main

// topic_graph.go — which subjects go together, learned from the catalogue.
//
// ════════════════════════════════════════════════════════════════════════════
// WHY SEARCH NEEDED THIS
// ════════════════════════════════════════════════════════════════════════════
//
// Searching "jellyfish" matched the literal word and nothing else. The app
// knows perfectly well that its jellyfish video is also about an aquarium,
// marine life and the deep sea — the model wrote those words down — but a
// search for "aquarium" found nothing, because no title contains it and
// "aquarium" is not one of the eighteen categories.
//
// The fix is not a thesaurus, and it is not a language model. Two subjects
// that keep turning up on the SAME videos are related, and the catalogue says
// which those are without anybody writing a list. That is what this builds:
// a small graph of "when a video is about X, it also tends to be about Y",
// derived from what the videos actually are.
//
// It has the property the eighteen categories never had — it grows on its
// own. A platform that starts getting cooking videos learns that biryani goes
// with rice and restaurant without a single line changing.
//
// ════════════════════════════════════════════════════════════════════════════
// WHAT IT IS USED FOR
// ════════════════════════════════════════════════════════════════════════════
//
//	SEARCH        — a query is widened to the subjects that go with it, so
//	                "aquarium" finds the jellyfish video.
//	SUGGESTIONS   — "people also look for", from the same graph.
//	EMPTY RESULTS — a query with no matches falls back to near subjects
//	                before it falls back to whatever is trending, which is
//	                the difference between a related answer and a shrug.

import (
	"log"
	"math"
	"sort"
	"strings"
	"sync"
	"time"
)

// topicGraphTTL is how long a built graph is trusted.
//
// Long, because the shape of a catalogue moves in days not seconds, and every
// rebuild is a full table read. Short enough that a burst of uploads about
// something new is reachable the same afternoon.
const topicGraphTTL = 30 * time.Minute

// topicGraphMinPairs is the least evidence from which any relationship can be
// inferred at all.
//
// Two, and it is not a tuning knob. One shared video is a single coincidence
// and says nothing; two is the smallest number that can show a pattern. Making
// it larger would not make the graph safer — it would make it silent on a new
// platform and no more correct on a large one, because a subject pair sharing
// five videos out of ten million is still noise.
//
// What actually keeps this honest at any size is not a count. It is
// topicSpecificity below: a subject that appears on half the catalogue cannot
// tell you much about anything, however many videos it co-occurs with, and one
// that appears on a hundredth of it tells you a great deal. That ratio behaves
// identically on a hundred videos and on a hundred million, which a threshold
// picked for "our catalogue is small" never could.
const topicGraphMinPairs = 2

// topicGraphMinStrength is how much of a subject's videos must also be about
// the other one before they count as related.
//
// A proportion rather than a count, so it means the same thing at every scale.
// Below this the two things merely brush past each other.
const topicGraphMinStrength = 0.15

// topicGraphMaxRelated bounds how far a query is widened. Past a handful the
// extra subjects are weakly related and start pulling in everything.
const topicGraphMaxRelated = 6

type topicGraph struct {
	// How many videos each subject appears on, and how many were counted.
	// Kept so specificity can be measured — see topicSpecificity.
	freq map[string]int
	docs int

	// related[a][b] is how strongly b follows from a, 0..1. Asymmetric on
	// purpose: nearly every video about "thistle" is also about "nature", but
	// hardly any video about "nature" is about thistles, and collapsing that
	// into one number would make every nature video a thistle video.
	related map[string]map[string]float64
	builtAt time.Time
}

var (
	topicGraphMu    sync.RWMutex
	topicGraphCache *topicGraph
)

// getTopicGraph returns the current graph, rebuilding it when stale.
//
// Never nil, and never an error: a search that cannot widen its query is a
// narrower search, not a broken one.
func getTopicGraph() *topicGraph {
	topicGraphMu.RLock()
	g := topicGraphCache
	topicGraphMu.RUnlock()
	if g != nil && time.Since(g.builtAt) < topicGraphTTL {
		return g
	}

	built := buildTopicGraph()
	topicGraphMu.Lock()
	topicGraphCache = built
	topicGraphMu.Unlock()
	return built
}

// buildTopicGraph reads every described video and counts which subjects share
// one.
func buildTopicGraph() *topicGraph {
	g := &topicGraph{
		related: map[string]map[string]float64{},
		freq:    map[string]int{},
		builtAt: time.Now(),
	}
	if db == nil {
		return g
	}
	rows, err := db.Query(`
		SELECT COALESCE(content_topics::text, '[]')
		  FROM challenges
		 WHERE content_topics IS NOT NULL
		   AND content_topics <> '[]'::jsonb`)
	if err != nil {
		log.Printf("topic graph: %v", err)
		return g
	}
	defer rows.Close()

	freq := g.freq
	pair := map[string]map[string]int{}
	sets := 0
	for rows.Next() {
		var raw string
		if rows.Scan(&raw) != nil {
			continue
		}
		words := normalizeTags(jsonStrings(raw))
		if len(words) < 2 {
			// A video described by one word says nothing about what goes with
			// what. Its frequency still counts, so the word exists in the
			// graph even if nothing links to it yet.
			for _, w := range words {
				freq[w]++
			}
			continue
		}
		sets++
		for _, w := range words {
			freq[w]++
		}
		for i, a := range words {
			for _, b := range words[i+1:] {
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

	// Divide by the frequency of the SUBJECT BEING ASKED ABOUT, not by the
	// pair's total. That is what makes it asymmetric and what makes it useful:
	// "of the videos about thistle, how many are also about nature" is close
	// to 1, while the reverse is small — so searching thistle widens to nature
	// and searching nature does not narrow to thistle.
	for a, others := range pair {
		fa := freq[a]
		if fa == 0 {
			continue
		}
		row := map[string]float64{}
		for b, n := range others {
			if n < topicGraphMinPairs {
				continue
			}
			s := float64(n) / float64(fa)
			if s > 1 {
				s = 1
			}
			if s < topicGraphMinStrength {
				continue
			}
			row[b] = s
		}
		if len(row) > 0 {
			g.related[a] = row
		}
	}

	g.docs = sets
	log.Printf("topic graph: %d subjects from %d described videos", len(g.related), sets)
	return g
}

// topicSpecificity is how much knowing a subject actually tells you, from 0
// (on everything) to 1 (on almost nothing).
//
// ════════════════════════════════════════════════════════════════════════════
// WHY THIS EXISTS
// ════════════════════════════════════════════════════════════════════════════
//
// Searching "thistle" returned a video about a TREE HOUSE. Both are described
// as being about "nature", so the graph linked them and the search followed
// the link. The link is real; it just carries almost no information, because
// "nature" sits on a large share of the catalogue and "pollination" sits on
// almost none.
//
// This is the standard measure for that — the rarer a term, the more a match
// on it means — and its great virtue here is that it is a RATIO. It behaves
// the same way on a hundred videos and on a hundred million, so nothing about
// it needs revisiting as the app grows. That is the opposite of a threshold
// chosen because the catalogue happens to be small today.
//
// Unknown subjects score 1: on a graph that has not been built, or a word from
// outside it, treating a match as fully informative is the same behaviour as
// before this existed rather than a silent downgrade of everything.
func (g *topicGraph) topicSpecificity(word string) float64 {
	if g == nil || g.docs <= 1 {
		return 1
	}
	df := g.freq[word]
	if df <= 0 {
		return 1
	}
	if df >= g.docs {
		return 0 // on everything: knowing it separates nothing from anything
	}
	// log(N/df) over log(N) puts the rarest subject at 1 and the commonest
	// near 0, with the curve that matters: the drop from "on 1%" to "on 10%"
	// is far larger than from "on 40%" to "on 50%".
	return math.Log(float64(g.docs)/float64(df)) / math.Log(float64(g.docs))
}

// relatedTopics returns the subjects that go with this one, strongest first.
//
// NOTE ON DUPLICATES. The catalogue currently holds many copies of the same
// video, so a pair of subjects appearing on twelve identical uploads counts
// twelve times. That inflates confidence but not the ORDER, because every copy
// carries the same words — the graph says the same thing, more loudly. Worth
// knowing before reading a score here as a real measurement.
func (g *topicGraph) relatedTopics(word string, n int) []string {
	if g == nil {
		return nil
	}
	w := normalizeOneTag(word)
	row := g.related[w]
	if len(row) == 0 {
		return nil
	}
	type kv struct {
		k string
		v float64
	}
	all := make([]kv, 0, len(row))
	for k, v := range row {
		// Weighted by how much the related subject actually tells you.
		//
		// Without this, "thistle" leads with "nature" — a real link that
		// carries almost no information, because nature sits on a large share
		// of the catalogue. "pollination" sits on almost none and is what
		// somebody searching thistle actually meant.
		all = append(all, kv{k, v * g.topicSpecificity(k)})
	}
	sort.Slice(all, func(i, j int) bool {
		if all[i].v != all[j].v {
			return all[i].v > all[j].v
		}
		return all[i].k < all[j].k // stable, so suggestions do not shuffle
	})
	if n > len(all) {
		n = len(all)
	}
	out := make([]string, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, all[i].k)
	}
	return out
}

// expandQuery returns the search term plus the subjects that go with it.
//
// The original always comes first and is never dropped: widening a search must
// add to what somebody asked for, never replace it.
func expandQuery(query string) []string {
	q := normalizeOneTag(query)
	if q == "" {
		return nil
	}
	out := []string{q}
	// Only a query that IS a subject gets widened. A multi-word phrase is
	// somebody describing what they want in their own words, and the graph has
	// nothing to say about it — matching it as a whole is more honest than
	// widening on whichever word happens to be in the graph.
	for _, r := range getTopicGraph().relatedTopics(q, topicGraphMaxRelated) {
		if r != q {
			out = append(out, r)
		}
	}
	return out
}

// relatedSearches is what to offer somebody after they search.
//
// Same graph, presented rather than used: the subjects that go with what they
// asked for, which on this catalogue means searching "jellyfish" suggests
// aquarium and marine life.
func relatedSearches(query string, n int) []string {
	q := normalizeOneTag(query)
	if q == "" {
		return nil
	}
	out := getTopicGraph().relatedTopics(q, n)
	// A suggestion identical to what they just typed is not a suggestion.
	filtered := out[:0]
	for _, r := range out {
		if r != q && !strings.EqualFold(r, query) {
			filtered = append(filtered, r)
		}
	}
	return filtered
}

// relatedSearchCap is how many "people also look for" chips to offer. Enough
// to be useful, few enough to fit one row on a phone without scrolling.
const relatedSearchCap = 5

// searchNearbySubjects finds videos about the subjects that go with a query
// that itself found nothing.
//
// This sits between "no results" and "here is what is trending". A search for
// a word the catalogue has never heard of deserves trending — there is nothing
// better to say. But a search for "aquarium" on a platform with a jellyfish
// video deserves the jellyfish video, and before this it got whatever happened
// to be popular, which reads as the app not understanding the question.
func searchNearbySubjects(query string) []Challenge {
	near := relatedSearches(query, topicGraphMaxRelated)
	if len(near) == 0 {
		return nil
	}
	ids := map[string]bool{}
	for _, w := range near {
		for id := range challengeIDsAboutTopic(w) {
			ids[id] = true
		}
	}
	if len(ids) == 0 {
		return nil
	}
	var out []Challenge
	for _, c := range GetSearchableChallenges() {
		if ids[c.ID] {
			out = append(out, c)
			if len(out) >= searchBattleCap+searchShortCap {
				break
			}
		}
	}
	return out
}
