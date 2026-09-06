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
	if len(related) > 0 && best == 0 {
		for _, r := range related {
			for _, t := range doc.Topics {
				if t == r {
					score += matchRelatedTopic
					goto doneRelated
				}
			}
		}
	}
doneRelated:

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

// dedupeSearchResults folds near-identical videos into their best copy.
//
// This catalogue holds twelve copies of some clips. Without this a search for
// their subject fills the entire page with one video repeated, which no real
// search does and which makes a small catalogue look even smaller.
//
// Judged on what the videos are ABOUT, using the same overlap the feed uses to
// decide two things are alike — not on titles, which the duplicates also share
// but which real distinct videos can share too.
func dedupeSearchResults(hits []challengeHit, index map[string]searchDoc) []challengeHit {
	out := make([]challengeHit, 0, len(hits))
	kept := make([][]string, 0, len(hits))
	for _, h := range hits {
		fp := contentFingerprint(index[h.Ch.ID].Topics, index[h.Ch.ID].Tags, "")
		dup := false
		if len(fp) > 0 {
			for _, k := range kept {
				if topicOverlap(fp, k) >= searchDuplicateOverlap {
					dup = true
					break
				}
			}
		}
		if dup {
			continue
		}
		kept = append(kept, fp)
		out = append(out, h)
	}
	// Ranks must be renumbered: the caller decays relevance over position, and
	// gaps left by removed items would silently penalise everything after the
	// first duplicate.
	for i := range out {
		out[i].Rank = i
	}
	return out
}

// searchDuplicateOverlap is how alike two videos must be to count as the same
// result. High, because folding two genuinely different videos together hides
// one of them completely — a worse failure than showing a near-duplicate.
const searchDuplicateOverlap = 0.8

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

	type sc struct {
		hit   challengeHit
		score float64
	}
	scored := make([]sc, 0, 32)
	for _, ch := range GetSearchableChallenges() {
		s := searchRelevance(ch, index[ch.ID], q, related)
		if s <= 0 {
			continue
		}
		scored = append(scored, sc{challengeHit{Ch: ch}, s})
	}
	sort.SliceStable(scored, func(i, j int) bool { return scored[i].score > scored[j].score })

	out := make([]challengeHit, 0, len(scored))
	for i, s := range scored {
		s.hit.Rank = i
		out = append(out, s.hit)
	}
	out = dedupeSearchResults(out, index)
	if len(out) > limit {
		out = out[:limit]
	}
	return out
}
