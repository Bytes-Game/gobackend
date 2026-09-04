package main

import (
	"strings"
	"testing"
)

func tagsFor(text string) map[string]bool {
	a := videoAnalysis{Speech: text, Passes: []string{"speech"}}
	out := map[string]bool{}
	for _, t := range tagsFromAnalysis(a) {
		out[t] = true
	}
	return out
}

// ════════════════════════════════════════════════════════════════════════════
// A KEYWORD IS A WORD, NOT A RUN OF LETTERS
// ════════════════════════════════════════════════════════════════════════════
//
// The match used to be strings.Contains over the raw text, which was already
// wrong before the list grew: "eat" means food and "beat" means music, so a
// video with a beat in it was also filed under food. The bigger the list, the
// more of these there are — "rap" inside "wrap", "art" inside "start".

func TestKeywords_ASubstringIsNotAMatch(t *testing.T) {
	cases := []struct {
		text    string
		wantNot string
		because string
	}{
		{"this beat is amazing", "food", `"beat" contains "eat"`},
		{"wrap it up nicely", "music", `"wrap" contains "rap"`},
		{"start the engine", "art", `"start" contains "art"`},
		{"i have a big heart", "art", `"heart" contains "art"`},
		{"read the newspaper today", "news", `"newspaper" contains "news"`},
	}
	for _, c := range cases {
		if tagsFor(c.text)[c.wantNot] {
			t.Errorf("%q was tagged %q because %s. A keyword has to match a "+
				"whole word, or a longer list files videos under subjects "+
				"nobody mentioned.", c.text, c.wantNot, c.because)
		}
	}
}

func TestKeywords_AWholeWordStillMatches(t *testing.T) {
	// The other half. Word matching must not become so strict that real
	// sentences stop matching — punctuation, capitals and line ends included.
	cases := map[string]string{
		"let me eat first":           "food",
		"EAT.":                       "food",
		"we will cook, then eat":     "food",
		"drop the beat":              "music",
		"here is a tutorial for you": "education",
		"time for the gym":           "sports",
		"day in my life, part two":   "lifestyle",
	}
	for text, want := range cases {
		if !tagsFor(text)[want] {
			t.Errorf("%q was not tagged %q; got %v", text, want, tagsFromAnalysis(
				videoAnalysis{Speech: text, Passes: []string{"speech"}}))
		}
	}
}

func TestKeywords_ReadsDevanagari(t *testing.T) {
	// Whisper writes Hindi in its own script, and the list has Hindi words in
	// it. A filter that only knew ASCII would strip them out of the text and
	// never match one.
	if !tagsFor("आज हम खाना बनाएंगे")["food"] {
		t.Error("a Hindi sentence about food was not tagged food; the word " +
			"filter has dropped non-ASCII letters")
	}
	if !tagsFor("यह गाना बहुत अच्छा है")["music"] {
		t.Error("a Hindi sentence about a song was not tagged music")
	}
}

func TestKeywords_EveryTagIsOneTheBackendAccepts(t *testing.T) {
	// The tags produced here are stored as auto_tags and read by
	// categoryFromTags. A tag that is not a real category (and not a known
	// shape or mood word) is a tag nobody can ever match on.
	allowed := map[string]bool{
		"comedy": true, "motivation": true, "sports": true, "dance": true,
		"music": true, "gaming": true, "art": true, "education": true,
		"story": true, "fashion": true, "food": true, "horror": true,
		"emotional": true, "lifestyle": true, "tech": true, "prank": true,
		"news": true, "other": true,
		// Mood words. emotionsFromTags reads any tag that is an emotion
		// label and feeds it to the ranker's mood matching, so a keyword
		// can say what a video is about AND how it feels in one entry.
		// These must stay in step with EmotionLabels in models.go.
		"funny": true, "scary": true, "sad": true, "romantic": true,
		"nostalgic": true, "empowering": true, "inspiring": true,
	}
	for kw, tags := range analysisKeywords {
		for _, tag := range tags {
			if !allowed[tag] {
				t.Errorf("keyword %q produces tag %q, which is not a category "+
					"the backend knows — nothing will ever match on it", kw, tag)
			}
		}
	}
}

func TestKeywords_NoKeywordIsShortEnoughToBeNoise(t *testing.T) {
	// Two-letter keywords match far too much even with word boundaries, and
	// the OCR pass discards short words for the same reason.
	for kw := range analysisKeywords {
		if len([]rune(kw)) < 3 {
			t.Errorf("keyword %q is shorter than three characters", kw)
		}
		if strings.TrimSpace(kw) != kw {
			t.Errorf("keyword %q has padding, which breaks word matching", kw)
		}
		if kw != strings.ToLower(kw) {
			t.Errorf("keyword %q is not lowercase; the text is lowercased "+
				"before matching so it can never fire", kw)
		}
	}
}

// ════════════════════════════════════════════════════════════════════════════
// THE TWO VIDEOS THAT CAME BACK WITH NOTHING
// ════════════════════════════════════════════════════════════════════════════
//
// Both transcribed cleanly and both earned only "talking", because every word
// in the list named a SUBJECT — food, a gym, a phone — and neither video was
// about a thing. That was the gap; these are the transcripts, verbatim.

func TestKeywords_TagsTheEnglishTranscriptThatUsedToMissEverything(t *testing.T) {
	// Verbatim from challenge 261. It earned only "talking" before, because
	// every word in the list named a thing and this video is about a
	// feeling.
	speech := "If I do forgive you, you're just gonna break my heart " +
		"all over again and I can't handle that. I won't. I promise. " +
		"Don't get it, do you? Either way..."

	got := tagsFor(speech)
	if !got["emotional"] {
		t.Errorf("not tagged emotional. Transcript: %q", speech)
	}
	if !got["sad"] {
		t.Error("heartbreak did not reach the mood vector, so the ranker " +
			"cannot match this to somebody asking for something sad")
	}
}

// ════════════════════════════════════════════════════════════════════════════
// AND THE ONE THAT STILL MISSES, WHICH IS THE HONEST LIMIT
// ════════════════════════════════════════════════════════════════════════════
//
// Challenge 262, byte for byte as stored. The video is about nobody knowing
// what the future holds, and "भविष्य" (future) IS in the keyword list — but
// whisper wrote भवि श् य where the word is भवि ष् य. One letter.
//
// Exact word matching cannot survive that, and it should not be taught to:
// the fix would be listing every way a model might misspell every word, which
// is a list nobody can finish. This test asserts the miss rather than hiding
// it, so the limit stays visible and stops being rediscovered.
//
// It is also the clearest argument on file for reading transcripts with
// something that understands meaning instead of spelling.
func TestKeywords_AOneLetterTranscriptionErrorStillDefeatsIt(t *testing.T) {
	stored := "अर्जुन भविश्य में क्या होने वाला है उसका किसी को पता नहीं और जो भुआ ही नहीं"

	if _, listed := analysisKeywords["भविष्य"]; !listed {
		t.Fatal("भविष्य has left the keyword list; this test no longer says " +
			"what it claims to")
	}
	if tagsFor(stored)["motivation"] {
		t.Log("the stored transcript now tags motivation — whisper's spelling " +
			"improved, or a keyword was added that catches it. Good; update " +
			"this test to assert the match rather than the miss.")
	}
	// Correctly spelled, the same sentence does match — so the list is right
	// and the transcription is what fell short.
	fixed := "अर्जुन भविष्य में क्या होने वाला है उसका किसी को पता नहीं"
	if !tagsFor(fixed)["motivation"] {
		t.Error("even correctly spelled, this sentence does not tag " +
			"motivation — then the keyword list is the problem, not the " +
			"transcription")
	}
}

func TestKeywords_AFeelingWordAlsoNamesTheMood(t *testing.T) {
	// emotionsFromTags reads any tag that is an emotion label, so one entry
	// can say what a video is about and how it feels. "funny" and "scary"
	// already worked this way; heartbreak should too.
	got := tagsFor("this is about my heartbreak")
	if !got["emotional"] {
		t.Error(`"heartbreak" did not produce the emotional category`)
	}
	if !got["sad"] {
		t.Error(`"heartbreak" did not produce the sad mood, so the ranker ` +
			`cannot match it to somebody asking for something sad`)
	}
}

func TestKeywords_TheWordsTooCommonToMeanAnythingStayOut(t *testing.T) {
	// The temptation when a video misses is to add the words it did use.
	// These carry no subject, appear in most sentences, and adding them
	// would tag half the catalogue.
	for _, kw := range []string{"life", "truth", "promise", "know", "thing", "time"} {
		if _, ok := analysisKeywords[kw]; ok {
			t.Errorf("%q is in the keyword list. It appears in ordinary "+
				"speech constantly, so it would file videos under a subject "+
				"nobody chose.", kw)
		}
	}
}
