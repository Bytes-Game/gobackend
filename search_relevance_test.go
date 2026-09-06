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
// SIMILAR VIDEOS COST POSITION, NEVER THEIR PLACE
// ════════════════════════════════════════════════════════════════════════════
//
// The first version of this DELETED near-identical results. It was written
// around a fact about today's test catalogue — twelve copies of some clips —
// rather than around how video search works, which was the wrong instinct.
//
// At real scale it destroys results. A trend is thousands of people doing the
// same thing in the same words. Two different biryani recipes share rice,
// spices, chicken and cooking. Removing "duplicates" would hide one of them
// permanently, invisibly, with nobody able to tell.

func fakeHits(ids ...string) []scoredHit {
	out := make([]scoredHit, 0, len(ids))
	for i, id := range ids {
		out = append(out, scoredHit{
			hit:   challengeHit{Ch: Challenge{ID: id, CreatorID: "c" + id}},
			score: 100 - float64(i), // already in relevance order
		})
	}
	return out
}

func TestDiversity_NothingIsEverHidden(t *testing.T) {
	// The property that matters most. Ten near-identical videos must all still
	// be reachable — somebody searching a trend wants the trend.
	index := map[string]searchDoc{}
	ids := []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"}
	for _, id := range ids {
		index[id] = doc([]string{"jellyfish", "aquarium", "marine life"}, nil, "")
	}
	got := diversifySearchResults(fakeHits(ids...), index, 20)
	if len(got) != 10 {
		t.Fatalf("got %d of 10 near-identical videos. Similar is not the same "+
			"as duplicate: two different recipes share every topic, and "+
			"dropping one hides it from everybody, permanently and "+
			"invisibly.", len(got))
	}
}

func TestDiversity_RepetitionIsPushedDownNotOut(t *testing.T) {
	// Five copies of one subject and one different video. The different one
	// should climb — the page stops being one thing — but all six survive.
	index := map[string]searchDoc{}
	for _, id := range []string{"1", "2", "3", "4", "5"} {
		index[id] = doc([]string{"jellyfish", "aquarium", "marine life"}, nil, "")
	}
	index["9"] = doc([]string{"cricket", "bat", "stadium"}, nil, "")

	got := diversifySearchResults(fakeHits("1", "2", "3", "4", "5", "9"), index, 10)
	if len(got) != 6 {
		t.Fatalf("got %d, want all 6 kept", len(got))
	}
	if got[0].Ch.ID != "1" {
		t.Errorf("the best match is no longer first; it is %q", got[0].Ch.ID)
	}
	// "9" scored lowest of the six and should still have climbed past most of
	// the repeats.
	pos := -1
	for i, h := range got {
		if h.Ch.ID == "9" {
			pos = i
		}
	}
	if pos > 2 {
		t.Errorf("the one different video sits at position %d of 6. Repetition "+
			"is supposed to cost position, so a fresh subject should rise.", pos)
	}
}

func TestDiversity_OnePersonDoesNotOwnThePage(t *testing.T) {
	// The other way a page turns monotonous. Gentler than subject damping,
	// because searching a creator's subject and getting their videos is often
	// exactly right.
	index := map[string]searchDoc{}
	hits := make([]scoredHit, 0, 6)
	for i, id := range []string{"1", "2", "3", "4", "5"} {
		index[id] = doc([]string{"topic" + id}, nil, "") // all different subjects
		hits = append(hits, scoredHit{
			hit:   challengeHit{Ch: Challenge{ID: id, CreatorID: "same"}},
			score: 100 - float64(i),
		})
	}
	index["9"] = doc([]string{"other"}, nil, "")
	hits = append(hits, scoredHit{
		hit:   challengeHit{Ch: Challenge{ID: "9", CreatorID: "someone-else"}},
		score: 80,
	})

	got := diversifySearchResults(hits, index, 10)
	if len(got) != 6 {
		t.Fatalf("got %d, want all 6 — one creator dominating is a reason to "+
			"reorder, never to delete", len(got))
	}
	pos := -1
	for i, h := range got {
		if h.Ch.ID == "9" {
			pos = i
		}
	}
	if pos > 2 {
		t.Errorf("the only other creator sits at position %d; one person owns "+
			"the page", pos)
	}
}

func TestDiversity_TheBestMatchAlwaysLeads(t *testing.T) {
	// Diversity must never cost the top spot. Somebody typed a word; the thing
	// that best answers it goes first, whatever else is on the page.
	index := map[string]searchDoc{
		"1": doc([]string{"jellyfish"}, nil, ""),
		"2": doc([]string{"cricket"}, nil, ""),
	}
	got := diversifySearchResults(fakeHits("1", "2"), index, 10)
	if got[0].Ch.ID != "1" {
		t.Errorf("the best match is %q, not the top result", got[0].Ch.ID)
	}
}

func TestDiversity_VideosWithNoDescriptionAreNotAllAlike(t *testing.T) {
	// Most of the catalogue has no topics yet. An empty description must not
	// count as resembling every other empty one, or unanalysed videos would
	// damp each other for no reason.
	index := map[string]searchDoc{"1": {}, "2": {}, "3": {}}
	got := diversifySearchResults(fakeHits("1", "2", "3"), index, 10)
	if len(got) != 3 {
		t.Fatalf("got %d, want 3", len(got))
	}
	for i, h := range got {
		if h.Ch.ID != []string{"1", "2", "3"}[i] {
			t.Errorf("order changed to %v; knowing nothing about two videos "+
				"does not make them alike", []string{got[0].Ch.ID, got[1].Ch.ID, got[2].Ch.ID})
			break
		}
	}
}

func TestDiversity_RanksAreRenumbered(t *testing.T) {
	// The caller decays relevance over position, so a gap would silently
	// demote everything after it.
	index := map[string]searchDoc{
		"1": doc([]string{"a", "b", "c"}, nil, ""),
		"2": doc([]string{"a", "b", "c"}, nil, ""),
		"3": doc([]string{"x", "y", "z"}, nil, ""),
	}
	got := diversifySearchResults(fakeHits("1", "2", "3"), index, 10)
	for i, h := range got {
		if h.Rank != i {
			t.Errorf("result %d carries rank %d", i, h.Rank)
		}
	}
}

func TestDiversity_RespectsTheLimit(t *testing.T) {
	index := map[string]searchDoc{}
	ids := []string{"1", "2", "3", "4", "5"}
	for _, id := range ids {
		index[id] = doc([]string{"t" + id}, nil, "")
	}
	if got := diversifySearchResults(fakeHits(ids...), index, 3); len(got) != 3 {
		t.Errorf("got %d, want 3", len(got))
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
