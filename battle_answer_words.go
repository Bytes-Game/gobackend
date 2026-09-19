package main

// battle_answer_words.go — what the OTHER half of a battle is about.
//
// ════════════════════════════════════════════════════════════════════════════
// THE HOLE THIS FILLS
// ════════════════════════════════════════════════════════════════════════════
//
// A battle is two videos. The viewer watches both — the challenge, then the
// answer on a flip — and votes on the pair.
//
// Everything the ranker knew about that battle came from one of them. Topics,
// tags, mood: all read off the challenge row, because the challenge row is
// what a feed item points at. The answer was a URL and nothing else, even
// though the worker had already read it, listened to it and looked at it, and
// even though it is half of what the viewer's time is spent on.
//
// So a dance challenge answered with a comedy bit was filed, ranked and
// learned-from as pure dance, and the half of the screen-time that was comedy
// taught nobody anything.
//
// ════════════════════════════════════════════════════════════════════════════
// WHY IT FOLLOWS THE SAME ORDER AS populateTopResponses
// ════════════════════════════════════════════════════════════════════════════
//
// A battle has one opponent on screen, not all of them. populateTopResponses
// picks it with ORDER BY created_at DESC — the newest answer — and that is the
// video the viewer actually gets.
//
// This reads the same one, the same way, on purpose. Describing a different
// answer would be worse than describing none: the ranker would be confident
// about a video nobody is going to see, and nothing in a log would look wrong.
//
// ════════════════════════════════════════════════════════════════════════════
// WHAT IT DELIBERATELY DOES NOT TOUCH
// ════════════════════════════════════════════════════════════════════════════
//
// The category. A battle's category belongs to the challenge, because the
// challenge is the question and the person who asked it chose the terms. If an
// answer could move it, one off-topic reply would pull a battle out of the
// category a viewer explicitly asked for — and the app already has a gentler
// way to say "this answer does not fit", in relevance_score and the off-topic
// flags.
//
// Topics and tags are additive and describe; a category excludes. Only the
// first kind is safe to take from somebody else's video.

import (
	"database/sql"
	"encoding/json"
	"errors"
	"log"
)

// answerWords is what the shown answer to a battle is about.
//
// Empty is the ordinary case, not a failure: a short has no answers, and an
// answer posted before the worker analysed it has nothing to say yet.
type answerWords struct {
	// Topics are the model's open-vocabulary description — "street food",
	// "dark fantasy".
	Topics []string
	// Tags are the responder's own words followed by the model's, in that
	// order, the same way mergeTags orders a challenge's.
	Tags []string
	// Emotions is how the answer feels. Folded into the battle's mood rather
	// than replacing it: EmotionVector is a set of flags, so a calm challenge
	// answered with something frantic is honestly both, and the viewer sat
	// through both.
	Emotions []string
}

// empty reports that there was nothing to add. Saves every caller writing the
// same two length checks before deciding whether to bother.
func (a answerWords) empty() bool {
	return len(a.Topics) == 0 && len(a.Tags) == 0 && len(a.Emotions) == 0
}

// loadAnswerWords reads the shown answer's topics and tags for one challenge.
//
// Never returns an error. A battle whose answer cannot be read is ranked on
// the challenge alone, which is exactly where it was before this existed — and
// failing a feed request over a description would be a far worse trade.
func loadAnswerWords(challengeID int) answerWords {
	if db == nil || challengeID <= 0 {
		return answerWords{}
	}
	var topicsJSON, autoJSON, mineJSON, emotionJSON []byte
	err := db.QueryRow(`
		SELECT COALESCE(content_topics::text, '[]'),
		       COALESCE(auto_tags::text, '[]'),
		       COALESCE(custom_tags::text, '[]'),
		       COALESCE(emotion_tags::text, '[]')
		  FROM challenge_responses
		 WHERE challenge_id = $1
		 ORDER BY created_at DESC
		 LIMIT 1`, challengeID).Scan(&topicsJSON, &autoJSON, &mineJSON, &emotionJSON)
	if err != nil {
		// No answers at all is the common case for a short, and is not worth
		// a line in the log.
		if !errors.Is(err, sql.ErrNoRows) {
			log.Printf("battle answer words: challenge %d: %v", challengeID, err)
		}
		return answerWords{}
	}
	return answerWordsFrom(topicsJSON, autoJSON, mineJSON, emotionJSON)
}

// answerWordsFrom is the decoding, split out so the shape can be tested
// without a database — and so the folding rule lives in one place whether the
// columns arrive from this file's query or from the profile rebuild's join.
//
// Folded on read as well as on write. The worker folds before storing, but
// rows written before that existed hold raw strings, and a tag that is not
// folded matches nothing — which looks exactly like a video with no tags.
func answerWordsFrom(topicsJSON, autoJSON, mineJSON, emotionJSON []byte) answerWords {
	var topics, auto, mine, emotions []string
	_ = json.Unmarshal(topicsJSON, &topics)
	_ = json.Unmarshal(autoJSON, &auto)
	_ = json.Unmarshal(mineJSON, &mine)
	_ = json.Unmarshal(emotionJSON, &emotions)
	return answerWords{
		Topics: normalizeTags(topics),
		// Responder first, model second — the same order mergeTags puts a
		// challenge's two sets in, so the two halves of a battle are described
		// in one convention rather than two.
		Tags:     mergeTags(normalizeTags(mine), normalizeTags(auto)),
		Emotions: normalizeTags(emotions),
	}
}

// foldInAnswer appends the answer's words to the challenge's own.
//
// The challenge leads, always. Both lists are capped at maxTagsPerItem by
// normalizeTags, so a battle never carries more words than a short — which
// means a challenge that already filled its ten keeps all ten and the answer
// adds nothing. That is the right way round: the question is what the item IS,
// and the answer is context.
func foldInAnswer(topics, tags []string, a answerWords) (outTopics, outTags []string) {
	if a.empty() {
		return topics, tags
	}
	return normalizeTags(append(append([]string{}, topics...), a.Topics...)),
		normalizeTags(append(append([]string{}, tags...), a.Tags...))
}
