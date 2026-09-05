package main

import (
	"math"
	"testing"
)

// ════════════════════════════════════════════════════════════════════════════
// WHAT A VIDEO IS LIKE, RATHER THAN WHICH BOX IT IS IN
// ════════════════════════════════════════════════════════════════════════════
//
// These are written against the app's real catalogue, because the failure that
// motivated the change is visible in it: videos 116, 118 and 126 are a bee on
// a thistle, a flower opening, and a butterfly — three nearly identical clips,
// filed by their creators under lifestyle, comedy and art.
//
// Under the old category signal those are three different things. Every test
// here is a way of saying they are not.

func TestSimilar_TheThreeNatureClipsAreAlike(t *testing.T) {
	bee := contentFingerprint(
		[]string{"bee", "flower", "pollination", "thistle", "insect", "nature"},
		[]string{"chill"}, "lifestyle")
	butterfly := contentFingerprint(
		[]string{"butterfly", "flower", "nature", "insect", "wildlife", "close up"},
		[]string{"chill", "satisfying"}, "art")

	got := topicOverlap(bee, butterfly)
	if got < 0.3 {
		t.Errorf("overlap is %.2f, want these read as clearly related.\n\n"+
			"They share flower, nature, insect and chill. Their CATEGORIES are "+
			"lifestyle and art, which is why the old signal saw two unrelated "+
			"videos and would happily show both in a row.", got)
	}
}

func TestSimilar_ADifferentSubjectIsNotAlike(t *testing.T) {
	butterfly := contentFingerprint(
		[]string{"butterfly", "flower", "nature", "insect"}, []string{"chill"}, "art")
	fantasy := contentFingerprint(
		[]string{"lone hunter", "dangerous quest", "gatekeepers", "dark fantasy"},
		[]string{"story", "horror"}, "story")
	if got := topicOverlap(butterfly, fantasy); got > 0.05 {
		t.Errorf("overlap is %.2f between a butterfly clip and a dark fantasy "+
			"scene; these share nothing and must not be pulled together", got)
	}
}

func TestSimilar_BeingWellDescribedIsNotPenalised(t *testing.T) {
	// Why the denominator is the SMALLER set and not the union.
	//
	// The model writes anywhere from one to six topics depending on how much it
	// could tell. Under plain Jaccard, a two-topic video entirely contained in a
	// six-topic one scores 2/6 — "barely related" — so similarity would depend
	// mostly on how talkative the model felt rather than on the videos.
	small := []string{"jellyfish", "aquarium"}
	big := []string{"jellyfish", "aquarium", "marine life", "underwater", "deep sea", "tank"}

	got := topicOverlap(small, big)
	if got < 0.99 {
		t.Errorf("overlap is %.2f, want 1.0. Everything the smaller video is "+
			"about is also true of the larger one — that is what alike means. "+
			"Jaccard would score this %.2f and call them unrelated.",
			got, 2.0/6.0)
	}
}

func TestSimilar_NothingKnownIsNotSimilarity(t *testing.T) {
	// Most of the catalogue has no topics yet. An empty fingerprint must score
	// zero against everything, not one — "we know nothing about either" is not
	// "these are identical", and treating it as a match would collapse the feed
	// onto the videos nobody has analysed.
	if got := topicOverlap(nil, []string{"thistle"}); got != 0 {
		t.Errorf("empty vs known = %.2f, want 0", got)
	}
	if got := topicOverlap(nil, nil); got != 0 {
		t.Errorf("two empties = %.2f, want 0 — knowing nothing about two "+
			"videos does not make them the same video", got)
	}
}

// ════════════════════════════════════════════════════════════════════════════
// THE FINGERPRINT
// ════════════════════════════════════════════════════════════════════════════

func TestFingerprint_DropsTheWordsThatMeanNothing(t *testing.T) {
	// "other" and "general" are what gets stored when nobody chose. They are
	// on a large share of the catalogue, so treating them as subjects would
	// make almost every video look related to almost every other one.
	fp := contentFingerprint([]string{"thistle"}, []string{"other"}, "general")
	for _, w := range fp {
		if w == "other" || w == "general" {
			t.Errorf("fingerprint kept %q. That word is on most of the "+
				"catalogue, so it would make everything look alike.", w)
		}
	}
	if len(fp) != 1 || fp[0] != "thistle" {
		t.Errorf("got %v, want just the real subject", fp)
	}
}

func TestFingerprint_CategoryIsJustAnotherWord(t *testing.T) {
	// The entire change, in one assertion. The category still counts — somebody
	// chose it, or a model concluded it — but it is worth exactly as much as
	// any other word describing the video, rather than being the only thing
	// that decides relevance.
	fp := contentFingerprint([]string{"thistle"}, []string{"chill"}, "lifestyle")
	found := false
	for _, w := range fp {
		if w == "lifestyle" {
			found = true
		}
	}
	if !found {
		t.Error("the category is gone entirely. It is a real signal and should " +
			"still count — just not more than everything else put together.")
	}
	if fp[0] != "thistle" {
		t.Errorf("fingerprint starts %q; topics come first because they are the "+
			"specific half", fp[0])
	}
}

func TestFingerprint_FoldsTheSameWayTagsAreFolded(t *testing.T) {
	// A topic stored as "Close-Up" and one stored as "close up" have to be one
	// word here, or two videos about the same thing never match.
	fp := contentFingerprint([]string{"Close-Up", "close up"}, nil, "")
	if len(fp) != 1 {
		t.Errorf("got %v, want one word — those are the same topic written two "+
			"ways, and if they stay separate the videos never match", fp)
	}
}

// ════════════════════════════════════════════════════════════════════════════
// HOW MUCH A VIEWER WANTS IT
// ════════════════════════════════════════════════════════════════════════════

func TestRelevance_AveragesRatherThanSums(t *testing.T) {
	// Summing would make a video's score depend on how many words happen to
	// describe it, so the model being chatty about one video would outrank the
	// viewer's actual taste.
	aff := map[string]float64{"nature": 0.8, "insect": 0.8, "flower": 0.8}
	two, _ := topicRelevance(aff, []string{"nature", "insect"})
	three, _ := topicRelevance(aff, []string{"nature", "insect", "flower"})
	if math.Abs(two-three) > 0.001 {
		t.Errorf("two words scored %.2f and three scored %.2f. Equal enthusiasm "+
			"about every word should score the same however many there are, or "+
			"a longer description beats a better match.", two, three)
	}
}

func TestRelevance_UnknownWordsDoNotDilute(t *testing.T) {
	// A video half of whose topics are brand new and half of which the viewer
	// loves should read as "quite relevant", not "half relevant". Almost every
	// video will contain words nobody has any history with, so counting those
	// as zero would drag every score toward nothing.
	aff := map[string]float64{"thistle": 1.0}
	got, matched := topicRelevance(aff, []string{"thistle", "brand new word", "another"})
	if got < 0.99 {
		t.Errorf("scored %.2f, want 1.0 — the viewer loves the one word we know "+
			"about; the words we know nothing about are not evidence against it", got)
	}
	if matched != 1 {
		t.Errorf("matched %d, want 1 — callers need this to tell a confident "+
			"score from one resting on a single word", matched)
	}
}

func TestRelevance_KeepsDislikesNegative(t *testing.T) {
	// The negative half has to survive. If a viewer has rejected a subject, a
	// video about it must score below zero and not merely low.
	aff := map[string]float64{"prank": -0.6}
	got, _ := topicRelevance(aff, []string{"prank", "unknown"})
	if got >= 0 {
		t.Errorf("scored %.2f for a subject the viewer rejects; a dislike that "+
			"reads as neutral shows them more of what they avoid", got)
	}
}

func TestRelevance_NoHistoryIsZeroNotNegative(t *testing.T) {
	// A brand-new viewer knows nothing, and every video must start level. A
	// negative default would bury the whole catalogue for everyone new.
	got, matched := topicRelevance(map[string]float64{}, []string{"thistle"})
	if got != 0 || matched != 0 {
		t.Errorf("got %.2f from %d matches, want 0 from 0", got, matched)
	}
}

func TestConfidence_OneCoincidentalWordIsNotAMatch(t *testing.T) {
	// "nature" turning up on a video that is really about something else must
	// not move it as far as a genuine match on everything.
	if topicConfidence(1) >= topicConfidence(3) {
		t.Error("one matched word counts as much as three; a coincidence then " +
			"ranks like a real match")
	}
	if topicConfidence(3) < 1 {
		t.Errorf("three matched words is only %.2f confident; the threshold has "+
			"to be reachable on a platform this small", topicConfidence(3))
	}
	if topicConfidence(0) != 0 {
		t.Error("no matched words is not zero confidence")
	}
}

// ════════════════════════════════════════════════════════════════════════════
// HAVING HAD ENOUGH OF SOMETHING
// ════════════════════════════════════════════════════════════════════════════

func TestSaturation_CatchesWhatCategoryFatigueMissed(t *testing.T) {
	// The concrete bug. Three nature clips whose CATEGORIES are lifestyle,
	// comedy and art: category fatigue counts one of each and sees a varied
	// feed. Counting words sees three of the same thing.
	seen := map[string]int{}
	for _, fp := range [][]string{
		{"bee", "flower", "nature", "insect"},
		{"plant", "flower", "nature", "growth"},
		{"butterfly", "flower", "nature", "insect"},
	} {
		rememberTopics(seen, fp)
	}
	fourth := []string{"moth", "flower", "nature", "insect"}
	if got := topicSaturation(seen, fourth); got <= 0 {
		t.Errorf("saturation is %.2f after three near-identical clips. This is "+
			"exactly what the old category fatigue missed: those three are "+
			"filed under three different categories, so it counted one of each "+
			"and called the feed varied.", got)
	}
}

func TestSaturation_TwoOfSomethingIsATasteNotARut(t *testing.T) {
	// Somebody who likes cricket should get cricket. The penalty is for the
	// third and beyond, not for having a preference at all.
	seen := map[string]int{}
	rememberTopics(seen, []string{"cricket"})
	rememberTopics(seen, []string{"cricket"})
	if got := topicSaturation(seen, []string{"cricket"}); got != 0 {
		t.Errorf("saturation %.2f after only two; punishing a preference is not "+
			"the same as breaking a rut", got)
	}
}

func TestSaturation_JudgesOnTheTiredestWord(t *testing.T) {
	// A video sharing one exhausted word with the session is repetitive even
	// if its other words are fresh. Averaging would let a well-described video
	// hide the repetition behind its unique words.
	seen := map[string]int{"cricket": 9}
	one := topicSaturation(seen, []string{"cricket"})
	many := topicSaturation(seen, []string{"cricket", "a", "b", "c", "d"})
	if many < one {
		t.Errorf("adding fresh words dropped saturation from %.2f to %.2f — a "+
			"ninth cricket video is still a ninth cricket video", one, many)
	}
}

func TestSaturation_UnseenSubjectIsFree(t *testing.T) {
	seen := map[string]int{"cricket": 9}
	if got := topicSaturation(seen, []string{"thistle"}); got != 0 {
		t.Errorf("an unrelated subject was penalised %.2f", got)
	}
}

// ════════════════════════════════════════════════════════════════════════════
// KEEPING A PROFILE LOADABLE
// ════════════════════════════════════════════════════════════════════════════

func TestTrim_KeepsStrongDislikesNotJustLikes(t *testing.T) {
	// The vocabulary is open, so a profile would grow forever; it is decoded on
	// every feed request. Trimming by how positive a feeling is would silently
	// delete the "stop showing me this" signal while looking healthy.
	m := map[string]float64{"hated": -0.95, "loved": 0.95}
	for i := 0; i < maxTopicAffinity; i++ {
		m[string(rune('a'+i%26))+string(rune('a'+i/26))+"x"] = 0.01
	}
	out := trimTopicAffinity(m)
	if len(out) != maxTopicAffinity {
		t.Fatalf("trimmed to %d, want %d", len(out), maxTopicAffinity)
	}
	if _, ok := out["hated"]; !ok {
		t.Error("a strong DISLIKE was dropped while faint likes survived. That " +
			"deletes the protection against subjects somebody has rejected, " +
			"and the profile still looks fine.")
	}
	if _, ok := out["loved"]; !ok {
		t.Error("a strong like was dropped")
	}
}

func TestTrim_LeavesSmallProfilesAlone(t *testing.T) {
	m := map[string]float64{"a": 0.1, "b": 0.2}
	if got := trimTopicAffinity(m); len(got) != 2 {
		t.Errorf("got %d entries, want both kept", len(got))
	}
}

// ════════════════════════════════════════════════════════════════════════════
// SEARCH
// ════════════════════════════════════════════════════════════════════════════

func TestSearch_FindsASubjectThatIsNotACategory(t *testing.T) {
	// Search could only match the eighteen category names, so "jellyfish"
	// matched nothing on a platform that has a jellyfish video.
	fp := contentFingerprint(
		[]string{"jellyfish", "aquarium", "marine life"}, []string{"chill"}, "other")
	if !topicMatchesQuery(fp, "jellyfish") {
		t.Error("searching jellyfish does not find the jellyfish video")
	}
	if !topicMatchesQuery(fp, "Jellyfish") {
		t.Error("search is case sensitive")
	}
	if topicMatchesQuery(fp, "cricket") {
		t.Error("an unrelated search matched")
	}
}

func TestSearch_MatchesInsidePhrases(t *testing.T) {
	// Topics are phrases. Somebody searching "food" should find "street food".
	fp := []string{"street food", "chai"}
	if !topicMatchesQuery(fp, "food") {
		t.Error(`searching "food" does not find a video whose topic is "street food"`)
	}
}

func TestSearch_EmptyQueryMatchesNothing(t *testing.T) {
	// A blank or punctuation-only query folds to "", and a substring test
	// against "" is true for everything — which would match the whole
	// catalogue.
	fp := []string{"thistle"}
	for _, q := range []string{"", "   ", "!!!"} {
		if topicMatchesQuery(fp, q) {
			t.Errorf("query %q matched everything", q)
		}
	}
}
