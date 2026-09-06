package main

// search_relevance.go — how well a video answers what somebody typed.
//
// ════════════════════════════════════════════════════════════════════════════
// WHAT WAS WRONG
// ════════════════════════════════════════════════════════════════════════════
//
// The reranker's "lexical relevance" was this:
//
//	lex := math.Exp(-float64(i) / 8.0)   // i = position in the list
//
// A decay over POSITION, which is only meaningful if something already sorted
// the list by relevance. Meilisearch does that. Meilisearch is not configured
// on the deployed server, so the Postgres path serves every search — and it
// returned whatever matched a title, in date order.
//
// So a video whose title IS the query and a video that mentions the word once
// in passing scored the same, and the newer one won.
//
// This scores the match itself, by WHERE the word was found. That ordering is
// the whole difference between a search that feels professional and one that
// feels random:
//
//	title            somebody named the video this
//	subject/topic    the video is ABOUT this
//	tag              somebody labelled it this
//	creator          the person is called this
//	spoken           the word was said once, somewhere in it
//
// ════════════════════════════════════════════════════════════════════════════
// AND WHY DUPLICATES ARE COLLAPSED
// ════════════════════════════════════════════════════════════════════════════
//
// This catalogue holds twelve copies of some videos. A search for their
// subject returned twelve identical results and nothing else, filling the
// entire page with one clip. No real search does that, so near-identical
// results are folded into their best-scoring copy.

import (
	"math"
	"sort"
	"strings"
	"sync"
	"time"
)

// Where a match was found, most meaningful first. These are ratios rather than
// absolutes: what matters is that a title beats a topic beats a passing
// mention, by enough that no amount of the weaker kind adds up to the stronger.
const (
	matchTitleExact   = 120.0 // the whole title is the query
	matchTitleWord    = 60.0  // a word of the title is the query
	matchTitlePart    = 30.0  // the title contains it
	matchTopicExact   = 55.0  // the video is ABOUT this
	matchTopicPart    = 25.0  // "food" inside "street food"
	matchTagExact     = 35.0
	matchTagPart      = 15.0
	matchCreator      = 40.0 // searching a person's name
	matchSpoken       = 8.0  // said once, somewhere in it
	matchScreenText   = 6.0
	matchRelatedTopic = 12.0 // a subject that GOES WITH the query
)

// searchDoc is everything about one video that a query can match against.
type searchDoc struct {
	Topics []string
	Tags   []string
	Spoken string // what was said, lowercased
	Screen string // what was written on screen, lowercased
}

// searchIndexTTL is how long the text index is trusted. Short enough that a
// freshly analysed video becomes findable within minutes, long enough that a
// burst of searches costs one query rather than hundreds.
const searchIndexTTL = 5 * time.Minute

var (
	searchIndexMu    sync.RWMutex
	searchIndexCache map[string]searchDoc
	searchIndexAt    time.Time
)

// searchTextIndex returns what every video is about, by id.
//
// ONE query for the whole catalogue rather than one per candidate. Scoring
// touches every video for every search, and a per-candidate lookup would turn
// one search into a hundred round trips.
func searchTextIndex() map[string]searchDoc {
	searchIndexMu.RLock()
	c, at := searchIndexCache, searchIndexAt
	searchIndexMu.RUnlock()
	if c != nil && time.Since(at) < searchIndexTTL {
		return c
	}

	out := map[string]searchDoc{}
	if db == nil {
		return out
	}
	rows, err := db.Query(`
		SELECT CAST(id AS TEXT),
		       COALESCE(content_topics::text, '[]'),
		       COALESCE(auto_tags::text, '[]'),
		       COALESCE(custom_tags::text, '[]'),
		       video_analysis
		  FROM challenges
		 WHERE visibility = 'arena'`)
	if err != nil {
		return out
	}
	defer rows.Close()
	for rows.Next() {
		var id, topicsJSON, autoJSON, tagsJSON string
		var analysisJSON []byte
		if rows.Scan(&id, &topicsJSON, &autoJSON, &tagsJSON, &analysisJSON) != nil {
			continue
		}
		doc := searchDoc{
			Topics: normalizeTags(jsonStrings(topicsJSON)),
			Tags:   normalizeTags(append(jsonStrings(autoJSON), jsonStrings(tagsJSON)...)),
		}
		if len(analysisJSON) > 0 {
			var a VideoAnalysis
			if jsonUnmarshalQuiet(analysisJSON, &a) {
				doc.Spoken = strings.ToLower(a.Speech)
				doc.Screen = strings.ToLower(a.ScreenText)
			}
		}
		out[id] = doc
	}

	searchIndexMu.Lock()
	searchIndexCache, searchIndexAt = out, time.Now()
	searchIndexMu.Unlock()
	return out
}

// searchRelevance scores how well one video answers the query, and says which
// kind of match it was so the caller can explain itself.
//
// Zero means no match. Popularity and recency are the CALLER's business —
// mixing them in here would let a popular video match a word it has nothing to
// do with, which is exactly the bug that made every search return the same ten
// accounts.
func searchRelevance(ch Challenge, doc searchDoc, q string, related []string) float64 {
	q = strings.ToLower(strings.TrimSpace(q))
	if q == "" {
		return 0
	}
	title := strings.ToLower(strings.TrimSpace(ch.Prefix + " " + ch.Subject))
	var score float64

	// ── TITLE ──
	switch {
	case title == q:
		score += matchTitleExact
	case containsWord(title, q):
		score += matchTitleWord
	case strings.Contains(title, q):
		score += matchTitlePart
	}

	// ── WHAT IT IS ABOUT ──
	// Only the best topic match counts. A video whose topics are "jellyfish,
	// jellyfish tank, jellyfish glow" is not three times more about jellyfish
	// than one that says it once.
	best := 0.0
	for _, t := range doc.Topics {
		switch {
		case t == q:
			best = math.Max(best, matchTopicExact)
		case strings.Contains(t, q) || strings.Contains(q, t):
			best = math.Max(best, matchTopicPart)
		}
	}
	score += best

	bestTag := 0.0
	for _, t := range doc.Tags {
		switch {
		case t == q:
			bestTag = math.Max(bestTag, matchTagExact)
		case strings.Contains(t, q):
			bestTag = math.Max(bestTag, matchTagPart)
		}
	}
	score += bestTag

	// ── WHO MADE IT ──
	if u := strings.ToLower(ch.CreatorUsername); u != "" && strings.Contains(u, q) {
		score += matchCreator
	}

	// ── WHAT WAS SAID ──
	// Deliberately the weakest. A word said once in a minute of talking is a
	// real signal and a poor one; without this it was no signal at all, and
	// with it weighted any higher every search returns whatever has the
	// longest transcript.
	if doc.Spoken != "" && strings.Contains(doc.Spoken, q) {
		score += matchSpoken
	}
	if doc.Screen != "" && strings.Contains(doc.Screen, q) {
		score += matchScreenText
	}

	// ── SUBJECTS THAT GO WITH IT ──
	// Below every direct match on purpose: "aquarium" should find the
	// jellyfish video, and should never rank it above a video actually
	// called aquarium.
	// Weighted by how much the related subject tells you. Matching on
	// "pollination" is near proof; matching on "nature" is barely a hint, and
	// treating them the same is why searching "thistle" returned a tree house.
	//
	// The strongest available related match counts, not the sum, so a video
	// sharing three vague subjects cannot outscore one sharing a precise one.
	if len(related) > 0 && best == 0 {
		g := getTopicGraph()
		bestRelated := 0.0
		for _, r := range related {
			for _, t := range doc.Topics {
				if t == r {
					if w := matchRelatedTopic * g.topicSpecificity(r); w > bestRelated {
						bestRelated = w
					}
				}
			}
		}
		score += bestRelated
	}

	return score
}

// containsWord reports whether q appears in text as a whole word, so
// searching "art" does not count as a match on "started".
func containsWord(text, q string) bool {
	for _, w := range strings.Fields(text) {
		if strings.Trim(w, ".,!?;:'\"()[]") == q {
			return true
		}
	}
	return false
}

// diversifySearchResults spreads similar videos out instead of removing them.
//
// ════════════════════════════════════════════════════════════════════════════
// WHY NOTHING IS EVER HIDDEN
// ════════════════════════════════════════════════════════════════════════════
//
// The first version of this DELETED near-identical results, keeping one copy.
// It was written because this app's catalogue happens to hold twelve copies of
// some clips — which is a fact about today's test data, not about video
// search, and building a rule around it was a mistake.
//
// At any real scale it is actively wrong. A trend is thousands of people doing
// the same thing, described in the same words, and somebody searching for it
// wants to see them all. Two genuinely different biryani recipes share almost
// every topic — rice, spices, chicken, cooking — and one of them would have
// been hidden permanently, invisibly, with no way for anybody to notice.
//
// A search result page has one job: do not lose things. So similarity costs a
// video POSITION, never its place in the results. Ten near-identical clips
// still all appear; they just stop occupying the whole first screen.
//
// Two kinds of sameness get damped, because they are the two ways a page
// becomes monotonous:
//
//	SUBJECT   ten videos about the same thing in a row
//	CREATOR   ten videos by the same person in a row
//
// Both damp multiplicatively, so the first repeat costs a little and the
// fifth costs a lot — which is the shape of how tiring repetition actually
// feels.
func diversifySearchResults(scored []scoredHit, index map[string]searchDoc, limit int) []challengeHit {
	if len(scored) == 0 {
		return nil
	}
	taken := make([]bool, len(scored))
	fps := make([][]string, len(scored))
	for i, s := range scored {
		d := index[s.hit.Ch.ID]
		fps[i] = contentFingerprint(d.Topics, d.Tags, "")
	}

	chosenFPs := make([][]string, 0, limit)
	seenCreator := map[string]int{}
	out := make([]challengeHit, 0, limit)

	for len(out) < limit && len(out) < len(scored) {
		bestIdx, bestVal := -1, -1.0
		for i, s := range scored {
			if taken[i] {
				continue
			}
			// Start from how well it answered the query, then charge it for
			// everything already on the page that it resembles.
			v := s.score
			if len(fps[i]) > 0 {
				for _, cf := range chosenFPs {
					if topicOverlap(fps[i], cf) >= searchNearIdentical {
						v *= searchRepeatDamping
					}
				}
			}
			for n := seenCreator[s.hit.Ch.CreatorID]; n > 0; n-- {
				v *= searchCreatorDamping
			}
			if v > bestVal {
				bestIdx, bestVal = i, v
			}
		}
		if bestIdx < 0 {
			break
		}
		taken[bestIdx] = true
		chosenFPs = append(chosenFPs, fps[bestIdx])
		if id := scored[bestIdx].hit.Ch.CreatorID; id != "" {
			seenCreator[id]++
		}
		h := scored[bestIdx].hit
		h.Rank = len(out) // the caller decays over position; gaps would demote unfairly
		out = append(out, h)
	}
	return out
}

const (
	// searchNearIdentical is how alike two videos must be before one counts as
	// a repeat of the other. Deliberately high: this only costs position, but
	// pushing genuinely different videos down is still a real cost.
	searchNearIdentical = 0.8
	// searchRepeatDamping is what each earlier near-identical result costs.
	// One repeat barely moves a strong match; five bury it — while still
	// leaving every one of them on the page.
	searchRepeatDamping = 0.55
	// searchCreatorDamping is the same idea for one person dominating. Gentler,
	// because searching a creator's subject and getting their videos is often
	// exactly right.
	searchCreatorDamping = 0.75
)

// scoredHit is a candidate and how well it answered the query.
type scoredHit struct {
	hit   challengeHit
	score float64
}

// rankByRelevance scores the whole searchable catalogue against a query and
// returns the best, most relevant first.
//
// This replaced a substring scan over titles that returned matches in date
// order. That is why the reranker's position decay meant nothing: nothing had
// ordered the list by how well anything matched.
func rankByRelevance(query string, limit int) []challengeHit {
	q := strings.ToLower(strings.TrimSpace(query))
	if q == "" {
		return nil
	}
	index := searchTextIndex()
	related := relatedSearches(q, topicGraphMaxRelated)

	scored := make([]scoredHit, 0, 32)
	for _, ch := range GetSearchableChallenges() {
		s := searchRelevance(ch, index[ch.ID], q, related)
		if s <= 0 {
			continue
		}
		scored = append(scored, scoredHit{challengeHit{Ch: ch}, s})
	}
	sort.SliceStable(scored, func(i, j int) bool { return scored[i].score > scored[j].score })
	return diversifySearchResults(scored, index, limit)
}
