package main

import (
	"reflect"
	"strings"
	"testing"
)

// The pure half of "the other video in a battle counts too". No database, so
// these run everywhere; the SQL half lives in response_words_db_test.go and
// needs real Postgres.

func TestAnswerWordsFrom_FoldsWhatWasStoredRaw(t *testing.T) {
	got := answerWordsFrom(
		[]byte(`["Street Food","street food"]`),
		[]byte(`["Food"]`),
		[]byte(`["#Chaat"]`),
		[]byte(`["Funny"]`),
	)
	if !reflect.DeepEqual(got.Topics, []string{"street food"}) {
		t.Errorf("topics %v — two spellings of one subject are one subject, "+
			"and an unfolded one matches nothing", got.Topics)
	}
	// Responder first, model second.
	if !reflect.DeepEqual(got.Tags, []string{"chaat", "food"}) {
		t.Errorf("tags %v, want the responder's word then the model's", got.Tags)
	}
	if !reflect.DeepEqual(got.Emotions, []string{"funny"}) {
		t.Errorf("moods %v", got.Emotions)
	}
}

func TestAnswerWordsFrom_SurvivesAColumnItCannotRead(t *testing.T) {
	// A JSONB column that will not parse must cost the words in it and
	// nothing else. Failing the whole battle over one bad row would take the
	// challenge's own description down with it.
	got := answerWordsFrom([]byte(`not json`), []byte(`["food"]`), []byte(`[]`), []byte(`[]`))
	if len(got.Topics) != 0 {
		t.Errorf("unreadable topics produced %v", got.Topics)
	}
	if !reflect.DeepEqual(got.Tags, []string{"food"}) {
		t.Errorf("the readable column was lost too: %v", got.Tags)
	}
}

func TestFoldInAnswer_TheChallengeLeads(t *testing.T) {
	topics, tags := foldInAnswer(
		[]string{"street food"}, []string{"food"},
		answerWords{Topics: []string{"night market"}, Tags: []string{"chaat"}},
	)
	if len(topics) == 0 || topics[0] != "street food" {
		t.Errorf("the answer displaced the challenge: %v", topics)
	}
	if !reflect.DeepEqual(topics, []string{"street food", "night market"}) {
		t.Errorf("topics %v", topics)
	}
	if !reflect.DeepEqual(tags, []string{"food", "chaat"}) {
		t.Errorf("tags %v", tags)
	}
}

func TestFoldInAnswer_NothingToAddChangesNothing(t *testing.T) {
	inT, inG := []string{"street food"}, []string{"food"}
	topics, tags := foldInAnswer(inT, inG, answerWords{})
	if !reflect.DeepEqual(topics, inT) || !reflect.DeepEqual(tags, inG) {
		t.Errorf("a short's own words were changed: %v / %v", topics, tags)
	}
}

// A battle must not be allowed to carry more words than a short. Otherwise
// every similarity comparison quietly favours battles, for no reason but that
// they have two videos to be described by.
func TestFoldInAnswer_StaysUnderTheSameCapAsAShort(t *testing.T) {
	many := []string{"a1", "b2", "c3", "d4", "e5", "f6", "g7", "h8", "i9", "j10", "k11", "l12"}
	topics, tags := foldInAnswer(many[:8], many[:8],
		answerWords{Topics: many[8:], Tags: many[8:]})
	if len(topics) > maxTagsPerItem {
		t.Errorf("a battle carries %d topics; the cap is %d", len(topics), maxTagsPerItem)
	}
	if len(tags) > maxTagsPerItem {
		t.Errorf("a battle carries %d tags; the cap is %d", len(tags), maxTagsPerItem)
	}
	// And the challenge's own words are the ones that survive the cap.
	if len(topics) == 0 || topics[0] != "a1" {
		t.Errorf("the cap evicted the challenge's own first topic: %v", topics)
	}
}

// ════════════════════════════════════════════════════════════════════════════
// WHAT AN ANSWER IS CALLED
// ════════════════════════════════════════════════════════════════════════════

func TestCategoryForResponse_TheResponderOutranksEveryoneElse(t *testing.T) {
	got := categoryForResponse("comedy", []string{"dance"}, "dance", "some caption")
	if got != "comedy" {
		t.Errorf("got %q; the person who made the video said comedy", got)
	}
}

func TestCategoryForResponse_TheirTagsOutrankTheChallenge(t *testing.T) {
	got := categoryForResponse("", []string{"comedy"}, "dance", "")
	if got != "comedy" {
		t.Errorf("got %q; the responder tagged it comedy", got)
	}
}

// The step that makes the column worth having. Without it an answer has
// almost nothing to be judged on — no prefix, no subject, usually no caption —
// and nearly every one in the app would be filed as "general".
func TestCategoryForResponse_FallsBackToTheChallengeItAnswers(t *testing.T) {
	got := categoryForResponse("", nil, "dance", "")
	if got != "dance" {
		t.Errorf("got %q; an answer to a dance challenge is a dance video", got)
	}
}

// 'other' and 'general' are what gets stored when nobody chose. Treating
// either as a claim turns "nobody said" into "somebody said nothing", and the
// fallback below it would never run.
func TestCategoryForResponse_ANonAnswerIsNotAnAnswer(t *testing.T) {
	if got := categoryForResponse("other", nil, "dance", ""); got != "dance" {
		t.Errorf("an explicit 'other' blocked the challenge fallback: %q", got)
	}
	if got := categoryForResponse("general", nil, "dance", ""); got != "dance" {
		t.Errorf("an explicit 'general' blocked the challenge fallback: %q", got)
	}
	if got := categoryForResponse("", nil, "other", "funny prank"); got == "other" {
		t.Error("a challenge with no category of its own was treated as one")
	}
}

func TestCategoryForResponse_LastResortIsTheCaption(t *testing.T) {
	got := categoryForResponse("", nil, "", "this dance is unreal")
	if got == "" {
		t.Error("returned nothing at all; every video needs a category")
	}
}

// ════════════════════════════════════════════════════════════════════════════
// THE CALL SITE
// ════════════════════════════════════════════════════════════════════════════
//
// The bug this repo keeps shipping is not a wrong function, it is a right
// function nothing calls. The database test proves the fold happens; this
// pins the two conditions around it that a database test cannot distinguish —
// that it is gated on the row's own response count rather than run for every
// short, and that the category is worked out from the challenge alone.
func TestContentScore_FoldsTheAnswerInOnlyForBattles(t *testing.T) {
	src := readSourceFile(t, "feed_engine.go")
	if !strings.Contains(src, "answer = loadAnswerWords(cid)") ||
		!strings.Contains(src, "foldInAnswer(cs.Topics, cs.Tags, answer)") {
		t.Fatal("nothing folds the answer's words into the battle, so half " +
			"of what a viewer watches describes nothing")
	}
	// And its mood, which is a separate wire: the tags go into cs.Tags, the
	// moods into the emotion vector, and cutting either one leaves the other
	// working.
	if !strings.Contains(src, "emotions = append(emotions, answer.Emotions...)") {
		t.Error("the answer's mood never reaches the battle's emotion vector")
	}
	// The gate. Without it every short in the catalogue pays for a query
	// that can only ever come back empty.
	idx := strings.Index(src, "answer = loadAnswerWords(cid)")
	before := src[max(0, idx-400):idx]
	if !strings.Contains(before, "if respCount > 0 {") {
		t.Error("the answer lookup is not gated on the response count already " +
			"read from the same row — every short now runs an extra query")
	}
	// The category must be decided before the fold, from the challenge alone.
	verdict := strings.Index(src, "catVerdict := categoryFromEvidence(")
	if verdict < idx {
		t.Error("the category is worked out after the answer's words are " +
			"folded in, so one off-topic reply can refile the battle")
	}
}
