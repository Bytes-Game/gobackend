package main

import (
	"os"
	"strings"
	"testing"
)

// ════════════════════════════════════════════════════════════════════════════
// WHERE THE WORD WAS FOUND IS THE WHOLE ANSWER
// ════════════════════════════════════════════════════════════════════════════
//
// The reranker decayed relevance over POSITION IN THE LIST — meaningful only
// if something already sorted by relevance. Meilisearch does that;
// Meilisearch is not configured on the deployed server, so the Postgres path
// serves every search and it returned title matches in date order.
//
// A video called "jellyfish" and a video that says the word once in a minute
// of talking scored identically, and the newer one won.

func doc(topics, tags []string, spoken string) searchDoc {
	return searchDoc{Topics: topics, Tags: tags, Spoken: spoken}
}

func TestRelevance_TitleBeatsAPassingMention(t *testing.T) {
	named := Challenge{Subject: "jellyfish"}
	mentioned := Challenge{Subject: "my aquarium visit"}

	a := searchRelevance(named, doc(nil, nil, ""), "jellyfish", nil)
	b := searchRelevance(mentioned, doc(nil, nil,
		"so we walked past the jellyfish and then went for lunch"), "jellyfish", nil)

	if a <= b {
		t.Errorf("a video CALLED jellyfish scored %.0f and one that says the "+
			"word once scored %.0f. Somebody searching jellyfish wants the "+
			"first one.", a, b)
	}
	if b <= 0 {
		t.Error("the spoken word counts for nothing; a transcript match is a " +
			"weak signal, not an absent one")
	}
}

func TestRelevance_BeingAboutItBeatsMentioningIt(t *testing.T) {
	about := Challenge{Subject: "a day at the coast"}
	mentions := Challenge{Subject: "my holiday"}

	a := searchRelevance(about, doc([]string{"jellyfish", "aquarium"}, nil, ""), "jellyfish", nil)
	b := searchRelevance(mentions, doc(nil, nil, "there was a jellyfish"), "jellyfish", nil)
	if a <= b {
		t.Errorf("a video ABOUT jellyfish scored %.0f against %.0f for one "+
			"that merely mentions them", a, b)
	}
}

func TestRelevance_RepeatingATopicIsNotBeingMoreAboutIt(t *testing.T) {
	// Only the best topic match counts. Otherwise a video described as
	// "jellyfish, jellyfish tank, jellyfish glow" outranks one whose whole
	// title is the word, on repetition alone.
	repeated := doc([]string{"jellyfish", "jellyfish tank", "jellyfish glow"}, nil, "")
	once := doc([]string{"jellyfish"}, nil, "")
	a := searchRelevance(Challenge{Subject: "x"}, repeated, "jellyfish", nil)
	b := searchRelevance(Challenge{Subject: "x"}, once, "jellyfish", nil)
	if a != b {
		t.Errorf("repeated topics scored %.0f against %.0f for one mention — "+
			"saying it three ways is not being three times more about it", a, b)
	}
}

func TestRelevance_RelatedNeverOutranksDirect(t *testing.T) {
	// "aquarium" should find the jellyfish video. It must never rank that
	// above a video actually about aquariums.
	direct := searchRelevance(Challenge{Subject: "x"},
		doc([]string{"aquarium"}, nil, ""), "aquarium", []string{"jellyfish"})
	viaGraph := searchRelevance(Challenge{Subject: "x"},
		doc([]string{"jellyfish"}, nil, ""), "aquarium", []string{"jellyfish"})

	if viaGraph <= 0 {
		t.Error("a related subject does not match at all, so searching " +
			"aquarium cannot reach the jellyfish video")
	}
	if viaGraph >= direct {
		t.Errorf("a related match scored %.0f and a direct one %.0f — the "+
			"video actually about the word has to come first", viaGraph, direct)
	}
}

func TestRelevance_WholeWordsNotFragments(t *testing.T) {
	// Searching "art" must not match "started". Substring matching on titles
	// is how a search starts returning things with no visible connection to
	// what was typed.
	started := searchRelevance(Challenge{Subject: "we started early"}, searchDoc{}, "art", nil)
	real := searchRelevance(Challenge{Subject: "art class"}, searchDoc{}, "art", nil)
	if started >= real {
		t.Errorf(`"started" scored %.0f for the query "art" against %.0f for `+
			`"art class"`, started, real)
	}
}

func TestRelevance_NoMatchIsZero(t *testing.T) {
	// The caller keeps anything above zero. Anything that leaks a base score
	// makes every video a result for every query — the exact bug that made
	// every search return the same ten accounts.
	got := searchRelevance(
		Challenge{Subject: "cooking biryani", CreatorUsername: "chef", Views: 99999, Likes: 5000},
		doc([]string{"rice", "recipe"}, []string{"food"}, "we add the spices now"),
		"jellyfish", nil)
	if got != 0 {
		t.Errorf("an unrelated video scored %.0f. Popularity and length must "+
			"never create a match — that is how a search starts returning the "+
			"same popular items for every word.", got)
	}
}

func TestRelevance_EmptyQueryMatchesNothing(t *testing.T) {
	for _, q := range []string{"", "   "} {
		if got := searchRelevance(Challenge{Subject: "anything"}, searchDoc{}, q, nil); got != 0 {
			t.Errorf("blank query %q scored %.0f", q, got)
		}
	}
}

// ════════════════════════════════════════════════════════════════════════════
// ONE VIDEO SHOULD APPEAR ONCE
// ════════════════════════════════════════════════════════════════════════════

func TestDedupe_TwelveCopiesBecomeOneResult(t *testing.T) {
	// This catalogue really does hold twelve copies of some clips. Without
	// this, searching their subject fills the whole page with one video.
	index := map[string]searchDoc{}
	var hits []challengeHit
	for _, id := range []string{"1", "2", "3", "4", "5"} {
		index[id] = doc([]string{"jellyfish", "aquarium", "marine life"}, nil, "")
		hits = append(hits, challengeHit{Ch: Challenge{ID: id}})
	}
	index["9"] = doc([]string{"cricket", "bat", "stadium"}, nil, "")
	hits = append(hits, challengeHit{Ch: Challenge{ID: "9"}})

	got := dedupeSearchResults(hits, index)
	if len(got) != 2 {
		t.Fatalf("got %d results from five identical videos plus one "+
			"different, want 2", len(got))
	}
	if got[0].Ch.ID != "1" {
		t.Errorf("kept %q rather than the best-scoring copy, which is first",
			got[0].Ch.ID)
	}
}

func TestDedupe_DifferentVideosSurvive(t *testing.T) {
	// Folding two genuinely different videos together hides one completely —
	// a worse failure than showing a near-duplicate, which is why the
	// threshold is high.
	index := map[string]searchDoc{
		"1": doc([]string{"jellyfish", "aquarium"}, nil, ""),
		"2": doc([]string{"cricket", "stadium"}, nil, ""),
		"3": doc([]string{"biryani", "rice"}, nil, ""),
	}
	hits := []challengeHit{
		{Ch: Challenge{ID: "1"}}, {Ch: Challenge{ID: "2"}}, {Ch: Challenge{ID: "3"}},
	}
	if got := dedupeSearchResults(hits, index); len(got) != 3 {
		t.Errorf("got %d, want all 3 kept — these share nothing", len(got))
	}
}

func TestDedupe_VideosWithNoDescriptionAreNotAllTheSame(t *testing.T) {
	// Most of the catalogue has no topics yet. An empty description must not
	// match every other empty description, or one unanalysed video would hide
	// all the rest.
	index := map[string]searchDoc{"1": {}, "2": {}, "3": {}}
	hits := []challengeHit{
		{Ch: Challenge{ID: "1"}}, {Ch: Challenge{ID: "2"}}, {Ch: Challenge{ID: "3"}},
	}
	if got := dedupeSearchResults(hits, index); len(got) != 3 {
		t.Errorf("got %d, want 3 — knowing nothing about two videos does not "+
			"make them the same video", len(got))
	}
}

func TestDedupe_RanksAreRenumbered(t *testing.T) {
	// The caller decays relevance over position. Gaps left by removed
	// duplicates would silently penalise everything after the first one.
	index := map[string]searchDoc{
		"1": doc([]string{"a", "b", "c"}, nil, ""),
		"2": doc([]string{"a", "b", "c"}, nil, ""),
		"3": doc([]string{"x", "y", "z"}, nil, ""),
	}
	hits := []challengeHit{
		{Ch: Challenge{ID: "1"}, Rank: 0},
		{Ch: Challenge{ID: "2"}, Rank: 1},
		{Ch: Challenge{ID: "3"}, Rank: 2},
	}
	got := dedupeSearchResults(hits, index)
	for i, h := range got {
		if h.Rank != i {
			t.Errorf("result %d has rank %d; a gap here quietly demotes "+
				"everything after a removed duplicate", i, h.Rank)
		}
	}
}

// ════════════════════════════════════════════════════════════════════════════
// A SEARCH THAT FINDS NOTHING STILL SHOWS SOMETHING
// ════════════════════════════════════════════════════════════════════════════

func TestRescue_NeverAnEmptyPage(t *testing.T) {
	// The rescue's own comment promises "never render an empty search page",
	// and the promise was quietly broken. Trending is built from what people
	// watched recently, held in Redis. On a platform with no traffic that list
	// is empty — measured live: 96 videos, zero views, zero active users — so
	// a query matching nothing showed a blank screen.
	//
	// The newest uploads are a real answer to "we could not find that".
	src, err := os.ReadFile("search.go")
	if err != nil {
		t.Fatalf("read search.go: %v", err)
	}
	s := string(src)
	if !strings.Contains(s, "if newest := GetSearchableChallenges(); len(newest) > 0 {") {
		t.Error("a search that matches nothing and finds no trending content " +
			"shows a blank page. Trending is empty whenever nobody has " +
			"watched anything, which is the normal state before launch.")
	}
	if !strings.Contains(s, "searchNewestRescueCap") {
		t.Error("the last-resort fallback is unbounded, so a failed search " +
			"turns into the entire catalogue")
	}
}

func TestRescue_DoesNotCallNewVideosPopular(t *testing.T) {
	// "Trending" and "recent" are different claims. With no traffic every
	// rescue is really "here is what is new", and labelling those popular
	// invents an engagement signal that does not exist — the app renders this
	// label directly to the user.
	src, err := os.ReadFile("search.go")
	if err != nil {
		t.Fatalf("read search.go: %v", err)
	}
	s := string(src)
	if !strings.Contains(s, "func zeroResultRescueKind() string") {
		t.Fatal("the rescue kind is not computed, so recent uploads are " +
			"announced as trending")
	}
	if strings.Contains(s, `resp.RelatedKind = "trending"`) {
		t.Error("the trending label is still hardcoded; it has to depend on " +
			"whether a trending list actually exists")
	}
	if !strings.Contains(s, `return "recent"`) {
		t.Error("there is no honest label for the no-traffic case")
	}
}
