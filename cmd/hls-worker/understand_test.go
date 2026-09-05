package main

import (
	"os"
	"strings"
	"testing"
)

// ════════════════════════════════════════════════════════════════════════════
// WHAT THE MODEL IS ALLOWED TO SAY
// ════════════════════════════════════════════════════════════════════════════
//
// The model runs on a runner, so nothing here starts it. What IS testable, and
// is the part that can quietly ruin the feed, is the filter between its answer
// and the database.
//
// A language model asked to pick from a list will sometimes pick something
// else. A tag no downstream stage knows is worse than no tag at all: it looks
// like the feature working, and it ranks nothing, forever, silently. So every
// word it returns is checked, and these say so.

func TestUnderstood_KeepsAGoodAnswer(t *testing.T) {
	got := understoodTags(`{"categories": ["emotional"], "feelings": ["sad", "intense"]}`)
	want := map[string]bool{"emotional": true, "sad": true, "intense": true}
	if len(got) != len(want) {
		t.Fatalf("got %v, want the three tags in %v", got, want)
	}
	for _, g := range got {
		if !want[g] {
			t.Errorf("unexpected tag %q in %v", g, got)
		}
	}
}

func TestUnderstood_DropsATagTheBackendNeverHeardOf(t *testing.T) {
	// The failure this exists for. "spiritual" and "philosophy" are perfectly
	// sensible words and neither is a category this app has, so neither can
	// ever be matched on.
	got := understoodTags(
		`{"categories": ["spiritual", "emotional"], "feelings": ["philosophical", "sad"]}`)
	for _, bad := range []string{"spiritual", "philosophical"} {
		for _, g := range got {
			if g == bad {
				t.Errorf("kept %q, which no part of the backend knows. A tag "+
					"nothing can match on looks exactly like a working "+
					"feature and ranks nothing.", bad)
			}
		}
	}
	// And the real ones survive, or the filter is just breaking the answer.
	if len(got) != 2 {
		t.Errorf("got %v, want the two real tags kept", got)
	}
}

func TestUnderstood_DropsOtherRatherThanStoringIt(t *testing.T) {
	// "other" is the model saying it could not tell. That is worth having as
	// an ANSWER and must not become a TAG: "other" is a real category name,
	// so storing it files the video under a subject the ranker then matches
	// people to.
	//
	// Seen in testing on a real transcript — asked for feelings it could not
	// judge, the model answered "other", which is not even a feeling.
	if got := understoodTags(`{"categories": ["other"], "feelings": ["other"]}`); len(got) != 0 {
		t.Errorf("got %v, want nothing stored", got)
	}
}

func TestUnderstood_SurvivesTheModelTalkingAroundTheAnswer(t *testing.T) {
	// Small models add a sentence either side of what they were asked for.
	// The answer still has to come out.
	raw := `Sure! Here is the label for this video:
{"categories": ["motivation"], "feelings": ["inspiring"]}
Let me know if you would like anything else.`
	got := understoodTags(raw)
	if len(got) != 2 {
		t.Fatalf("got %v, want motivation + inspiring out of the chatter", got)
	}
}

func TestUnderstood_TakesTheLastObjectNotTheFirst(t *testing.T) {
	// The instructions contain an example object, and depending on the build
	// the CLI can echo parts of the prompt. Taking the FIRST match would
	// label every video with whatever the example says.
	raw := `{"categories": ["..."], "feelings": ["..."]}
{"categories": ["food"], "feelings": ["happy"]}`
	got := understoodTags(raw)
	if len(got) != 2 || got[0] != "food" && got[1] != "food" {
		t.Errorf("got %v, want the real answer (food) and not the template", got)
	}
}

func TestUnderstood_GarbageIsNoTagsRatherThanBadTags(t *testing.T) {
	for _, raw := range []string{
		"",
		"I'm sorry, I can't help with that.",
		`{"categories": [`,
		`{"categories": "emotional"}`, // a string where a list belongs
	} {
		if got := understoodTags(raw); len(got) != 0 {
			t.Errorf("understoodTags(%q) = %v, want nothing", raw, got)
		}
	}
}

// ════════════════════════════════════════════════════════════════════════════
// THE PROMPT IS PART OF THE PROGRAM
// ════════════════════════════════════════════════════════════════════════════
//
// Each of these lines was added because the model got something specific
// wrong without it, measured on this app's own transcripts. They read as
// prose, so nothing else in the repo would notice them being edited away.

func TestPrompt_TellsTheModelWhatTheCategoriesMean(t *testing.T) {
	// Given bare names, the model has to guess what this app means by each
	// one, and it guesses badly: "having ideas is easy, the important thing
	// is how well you execute" came back as COMEDY. With the descriptions it
	// came back as motivation.
	p := buildUnderstandPrompt("anything")
	for _, want := range []string{
		"motivation — inspirational, discipline, success, hustle",
		"story — vlogs, storytime, personal experiences",
	} {
		if !strings.Contains(p, want) {
			t.Errorf("the prompt no longer explains what the categories mean.\n"+
				"Missing: %q\n\nWithout this the model scores markedly worse "+
				"— it is the single change that fixed the most cases.", want)
		}
	}
}

func TestPrompt_WarnsThatBracketsAreSoundsNotSpeech(t *testing.T) {
	// Whisper writes sound effects in brackets. One whole stored transcript
	// is "(music playing) (door squeaking) (music playing) (upbeat music)" —
	// nobody speaks in that video at all. Without this line the model read
	// the word "music" and filed it under MUSIC.
	p := buildUnderstandPrompt("anything")
	if !strings.Contains(p, "(music playing)") {
		t.Error("the prompt no longer warns that bracketed text is the " +
			"transcriber describing sound. A video where nobody speaks then " +
			"gets tagged from the sound effects.")
	}
}

func TestPrompt_SaysAWrongAnswerIsWorseThanNoAnswer(t *testing.T) {
	// This is the whole disposition of the feature. Without it the model
	// reaches for a category on transcripts that say nothing, and a wrong
	// tag shows the video to people who asked for something else.
	p := buildUnderstandPrompt("anything")
	if !strings.Contains(p, "worse than") {
		t.Error(`the prompt no longer tells the model which way to err`)
	}
}

func TestPrompt_CapsAStuckTranscript(t *testing.T) {
	// Short or noisy audio makes whisper repeat itself — one stored
	// transcript is "I don't know why I'm doing this" over and over. Sending
	// all of it costs prompt-processing time and says nothing the first lines
	// did not, and a long enough one would overflow the context.
	long := strings.Repeat("यह एक लंबा वाक्य है। ", 4000)
	p := buildUnderstandPrompt(long)
	if len([]rune(p)) > understandMaxSpeechChars+4000 {
		t.Errorf("the transcript is not being capped: prompt is %d runes",
			len([]rune(p)))
	}
	// Cut on a rune boundary, not a byte one. Byte slicing would hand the
	// model half a Devanagari character and make a Hindi video harder to read
	// than an English one for no reason.
	if !strings.ContainsRune(p, 'ए') {
		t.Error("the Hindi text did not survive truncation intact")
	}
}

func TestUnderstand_StaysOffUnlessBothVariablesAreSet(t *testing.T) {
	// Same contract as the speech pass: no binary means the pass does not
	// run, and the worker falls back to the keyword list rather than failing
	// the video. Half-configured must count as off — a binary with no model
	// would start and then fail on every single video.
	for _, c := range []struct{ bin, model string }{
		{"", ""},
		{"/nonexistent/llama-cli", ""},
		{"", "/nonexistent/model.gguf"},
	} {
		t.Setenv(understandBinEnv, c.bin)
		t.Setenv(understandModelEnv, c.model)
		if _, ran := understandContent(t.Context(), videoAnalysis{
			Speech: "this is a long enough sentence to be worth judging",
		}); ran {
			t.Errorf("claimed to run with bin=%q model=%q", c.bin, c.model)
		}
	}
}

func TestUnderstand_DoesNotStartTheModelForAVideoThatSaidNothing(t *testing.T) {
	// Most of the catalogue is silent — 79 of 114 videos produce no
	// transcript. Starting a language model to be told there is nothing to
	// read would be the single biggest cost in the pass, spent on the
	// majority of videos, to learn what the word count already says.
	t.Setenv(understandBinEnv, "/definitely/not/here/llama-cli")
	t.Setenv(understandModelEnv, "/definitely/not/here/model.gguf")
	if _, ran := understandContent(t.Context(), videoAnalysis{Speech: "(music playing)"}); ran {
		t.Error("ran the model on a transcript with nothing in it")
	}
}

func TestUnderstand_CategoriesMatchTheOnesTheBackendDefines(t *testing.T) {
	// The worker is a separate program and cannot import models.go, so this
	// list is a copy — and a copy that drifts is a model being asked for
	// categories the app no longer has, or never offered.
	backend, err := os.ReadFile("../../models.go")
	if err != nil {
		t.Skipf("cannot read models.go: %v", err)
	}
	src := string(backend)
	for _, c := range understandCategories {
		if !strings.Contains(src, `"`+c.Name+`",`) {
			t.Errorf("this pass can answer %q, which is not in "+
				"ContentCategories any more. The model would be told to "+
				"choose a category nothing downstream knows.", c.Name)
		}
	}
	for _, e := range understandEmotions {
		if !strings.Contains(src, `"`+e+`",`) {
			t.Errorf("this pass can answer the feeling %q, which is not in "+
				"EmotionLabels any more", e)
		}
	}
}

// ════════════════════════════════════════════════════════════════════════════
// AND THE ONE THAT IS NOT OPTIONAL
// ════════════════════════════════════════════════════════════════════════════

func TestUnderstand_AlwaysBoundsTheContext(t *testing.T) {
	// Qwen3-VL's own context is enormous and llama.cpp reserves memory for
	// all of it up front. Without -c the process is killed outright before it
	// reads a word:
	//
	//	llama-cli ... -p "Reply with exactly: OK"
	//	Killed                                       (exit 137)
	//
	// Nothing about that says "context". It is an out-of-memory kill with no
	// message, which on a runner looks exactly like the feature being off —
	// so this is worth a test rather than a comment.
	src, err := os.ReadFile("understand.go")
	if err != nil {
		t.Fatalf("read understand.go: %v", err)
	}
	if !strings.Contains(string(src), `"-c", strconv.Itoa(understandContextTokens)`) {
		t.Error("the context size is no longer passed to the model. Without " +
			"it the process is out-of-memory killed on every video, silently.")
	}
	if understandContextTokens > 16384 {
		t.Errorf("context is %d tokens; large values are what caused the "+
			"out-of-memory kill this bound exists to prevent",
			understandContextTokens)
	}
}

// TestUnderstand_EndToEndAgainstARealModel drives the whole pass with a real
// binary and real weights.
//
// Skipped unless UNDERSTAND_BIN and UNDERSTAND_MODEL are set, because the
// weights are 2.4GB and CI has no reason to hold them. It is here because
// everything above tests the pieces, and the pieces passing is not the same
// as the model actually answering: the flags could be wrong, the binary could
// refuse them, the JSON could come back in a shape the parser misses. Run it
// once on a machine that has the model and that whole question is settled.
//
//	UNDERSTAND_BIN=~/llama/llama-cli UNDERSTAND_MODEL=~/llama/model.gguf \
//	  go test ./cmd/hls-worker/ -run EndToEnd -v
func TestUnderstand_EndToEndAgainstARealModel(t *testing.T) {
	if os.Getenv(understandBinEnv) == "" || os.Getenv(understandModelEnv) == "" {
		t.Skip("no local model; set " + understandBinEnv + " and " +
			understandModelEnv + " to run this")
	}
	// Challenge 261, verbatim. Unmistakably about heartbreak, so a model that
	// is working at all gets it — which makes a failure here a failure of the
	// plumbing rather than a judgement call.
	a := videoAnalysis{Speech: "If I do forgive you, you're just gonna break " +
		"my heart all over again and I can't handle that. I won't. I promise."}

	tags, ran := understandContent(t.Context(), a)
	if !ran {
		t.Fatal("the pass reported it did not run, with both variables set")
	}
	if len(tags) == 0 {
		t.Fatal("the model ran and produced no tags for an unambiguous transcript")
	}
	t.Logf("model answered: %v", tags)
	var sawCategory bool
	for _, tag := range tags {
		for _, c := range understandCategories {
			if tag == c.Name {
				sawCategory = true
			}
		}
	}
	if !sawCategory {
		t.Errorf("got %v — feelings but no category, so nothing decides where "+
			"this video is filed", tags)
	}
}
