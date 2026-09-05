package main

import (
	"os"
	"regexp"
	"strings"
	"testing"
	"time"
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

	got, ran := understandContent(t.Context(), a)
	if !ran {
		t.Fatal("the pass reported it did not run, with both variables set")
	}
	if len(got.Tags) == 0 {
		t.Fatal("the model ran and produced no tags for an unambiguous transcript")
	}
	t.Logf("model answered: tags=%v topics=%v", got.Tags, got.Topics)
	var sawCategory bool
	for _, tag := range got.Tags {
		for _, c := range understandCategories {
			if tag == c.Name {
				sawCategory = true
			}
		}
	}
	if !sawCategory {
		t.Errorf("got %v — feelings but no category, so nothing decides where "+
			"this video is filed", got.Tags)
	}
}

// ════════════════════════════════════════════════════════════════════════════
// THE BUDGETS THAT KEEP THE PASSES OUT OF EACH OTHER'S WAY
// ════════════════════════════════════════════════════════════════════════════
//
// Found in production, on the two videos re-queued to prove an unrelated fix:
//
//	probeHasAudio: source.mp4 hasAudio=true
//	... 6m35s later ...
//	analyze: speech pass failed: signal: killed
//
// Challenge 260 HAS sound and had been transcribed before — 41 words — and
// lost its transcript entirely because it was processed at the same moment as
// another video. Two causes, and both are pinned below, because both failures
// are silent: the stored analysis of a killed transcription is identical to
// that of a genuinely quiet video.

func TestSpeech_AsksForHalfTheMachineNotAllOfIt(t *testing.T) {
	// The root cause. The workflow runs TWO workers on a four-core runner and
	// whisper.cpp takes four threads when nobody says otherwise, so both
	// workers together wanted eight — while also encoding video. Not "a bit
	// slower": slow enough to still be running when the deadline expired.
	src, err := os.ReadFile("analyze.go")
	if err != nil {
		t.Fatalf("read analyze.go: %v", err)
	}
	if !strings.Contains(string(src), `"-t", strconv.Itoa(speechThreads)`) {
		t.Error("whisper is no longer told how many threads to use. With two " +
			"workers on a four-core runner it will take eight, and " +
			"transcripts start disappearing on whichever videos happen to " +
			"be processed together.")
	}
	if speechThreads > 2 {
		t.Errorf("speechThreads is %d; two workers would ask for %d threads "+
			"on a four-core runner", speechThreads, speechThreads*2)
	}
	// Both models that run on this box share the machine the same way, and a
	// change to one without the other puts it straight back into overcommit.
	if speechThreads != understandThreads {
		t.Errorf("speech uses %d threads and understanding uses %d. Both run "+
			"two-up on the same four cores, so they should agree.",
			speechThreads, understandThreads)
	}
}

func TestAnalyze_NoSinglePassCanSpendTheWholeBudget(t *testing.T) {
	// The structural half. The passes run in sequence against one deadline,
	// so a slow one spends the time belonging to those behind it — and the
	// slowest pass sits directly in front of the one that depends on its
	// output. When speech ran to the full six minutes, understanding never
	// started at all.
	for _, b := range []struct {
		name   string
		budget time.Duration
	}{
		{"shape", shapeBudget},
		{"screen text", screenTextBudget},
		{"speech", speechBudget},
		{"understand", understandBudget},
	} {
		if b.budget <= 0 {
			t.Errorf("the %s pass has no budget of its own", b.name)
		}
		if b.budget >= analyzeTimeout {
			t.Errorf("the %s pass may run for %v, which is the whole %v "+
				"analysis budget — it can still starve every pass after it",
				b.name, b.budget, analyzeTimeout)
		}
	}
	// Understanding must have room left after speech has had its worst case,
	// or the fix does not actually buy anything.
	if speechBudget+understandBudget > analyzeTimeout {
		t.Errorf("speech (%v) plus understanding (%v) is more than the %v "+
			"ceiling, so a slow transcription still leaves nothing to read it",
			speechBudget, understandBudget, analyzeTimeout)
	}
}

func TestAnalyze_TheCeilingStillFitsInsideOneJob(t *testing.T) {
	// analyzeTimeout is nested inside the worker's -job-timeout, which also
	// has to cover downloading and transcoding the video. Raising the analysis
	// ceiling to buy headroom is the obvious move and the wrong one: it would
	// push whole JOBS past their limit, and a job killed mid-transcode leaves
	// a row PENDING for the reaper.
	if analyzeTimeout > 6*time.Minute {
		t.Errorf("analyzeTimeout is %v. It shares -job-timeout with the "+
			"download, the transcode and the upload; give the passes room by "+
			"making them faster, not by raising this.", analyzeTimeout)
	}
}

// ════════════════════════════════════════════════════════════════════════════
// AND THE HALF THAT LOOKS INSTEAD OF READING
// ════════════════════════════════════════════════════════════════════════════
//
// 79 of 114 videos produce no transcript, and 62 of those have no readable
// text on screen either. Nothing that only reads has ever been able to help
// them. This half is why the model chosen was a vision one.

func TestFrames_StayOffUnlessEveryPieceIsPresent(t *testing.T) {
	// Three things are needed: the multimodal binary, the weights, and the
	// vision projector. Any one missing must count as off — a run started
	// without the projector would fail on every silent video, which is most
	// of them.
	full := map[string]string{
		framesBinEnv:       "/nonexistent/llama-mtmd-cli",
		understandModelEnv: "/nonexistent/model.gguf",
		framesProjectorEnv: "/nonexistent/mmproj.gguf",
	}
	for missing := range full {
		for k, v := range full {
			if k == missing {
				t.Setenv(k, "")
			} else {
				t.Setenv(k, v)
			}
		}
		if _, ran := understandContentFromFrames(t.Context(), "/tmp/nope.mp4", 10); ran {
			t.Errorf("claimed to run with %s unset", missing)
		}
	}
}

func TestFrames_AskTheSameQuestionAsTheReadingHalf(t *testing.T) {
	// Both halves feed the same auto_tags column and the same ranker. If they
	// offered different categories, which pass happened to run would change
	// what a video could possibly be filed under.
	p := buildFramesPrompt()
	for _, c := range understandCategories {
		if !strings.Contains(p, c.Name+" — "+c.Means) {
			t.Errorf("the frames prompt is missing %q, so the two halves no "+
				"longer choose from the same list", c.Name)
		}
	}
	// And err the same way. A picture invites guessing far more than a
	// transcript does — there is always SOMETHING in a frame — so this line
	// matters more here, not less.
	if !strings.Contains(p, "worse than") {
		t.Error("the frames prompt no longer tells the model which way to err")
	}
	// The specific over-guess this is written against: every video has a
	// person or a room in it, and neither is what a video is about.
	if !strings.Contains(p, "not a subject") {
		t.Error("the frames prompt no longer warns that an ordinary scene is " +
			"not a subject, which is the easiest way for it to invent a tag")
	}
}

func TestFrames_AskForTopicsToo(t *testing.T) {
	// This was missing, and it mattered more here than anywhere else.
	//
	// Reading declines on a video with no words, so LOOKING is the only pass
	// that ever runs on a silent video — and 66 of the app's 96 analysed
	// videos say nothing at all. A frames prompt that asks only for a category
	// leaves exactly those videos with one of eighteen words and no
	// description, which is the situation topics were added to fix.
	p := buildFramesPrompt()
	if !strings.Contains(p, "TOPICS") {
		t.Fatal("the frames prompt does not ask for topics, so a silent video " +
			"can never be described — only filed under one of eighteen words")
	}
	if !strings.Contains(p, `"topics"`) {
		t.Error("the answer format does not include topics, so anything the " +
			"model says about them is dropped when the JSON is parsed")
	}
	// Same vocabulary as the reading half. Two different example lists would
	// make the same video described differently depending on whether it
	// happened to have a soundtrack.
	for _, want := range understandTopicExamples[:3] {
		if !strings.Contains(p, want) {
			t.Errorf("the frames prompt does not share the reading half's topic "+
				"examples (missing %q), so the two passes would name the same "+
				"thing differently", want)
		}
	}
}

func TestFrames_AnswersGoThroughTheSameFilter(t *testing.T) {
	// Same validation as the reading half, so a model looking at a picture
	// can no more invent a category than one reading a sentence. Shared code
	// rather than a second copy — see understoodTags.
	src, err := os.ReadFile("understand_frames.go")
	if err != nil {
		t.Fatalf("read understand_frames.go: %v", err)
	}
	if !strings.Contains(string(src), "understoodTags(") {
		t.Error("the frames pass no longer validates its answer through " +
			"understoodTags. It could then store a tag nothing downstream knows.")
	}
	// Topics come from the same answer and are deliberately NOT validated —
	// see the topics note. Both halves must read both, or a video categorised
	// from pictures would carry no description at all.
	if !strings.Contains(string(src), "understoodTopics(") {
		t.Error("the frames pass does not read topics, so a video with no " +
			"words gets a category and no description of what it is about")
	}
	if !strings.Contains(string(src), `"-c", strconv.Itoa(understandFramesContextTokens)`) {
		t.Error("the frames pass does not bound its context. The pictures take " +
			"context too, so this is the same silent out-of-memory kill as the " +
			"reading pass, only easier to hit.")
	}
}

func TestFrames_TheSilentVideoPathFitsInTheBudget(t *testing.T) {
	// This pass exists for videos with no sound. Such a video costs almost
	// nothing before it: with no audio track the speech pass returns
	// immediately, and the reading pass declines without starting a model. So
	// the arithmetic that has to hold is shape + text + frames.
	if shapeBudget+screenTextBudget+framesBudget > analyzeTimeout {
		t.Errorf("a silent video's passes need %v but the ceiling is %v, so "+
			"the one pass that could categorise it is the one that gets cut",
			shapeBudget+screenTextBudget+framesBudget, analyzeTimeout)
	}
	if framesBudget <= 0 || framesBudget >= analyzeTimeout {
		t.Errorf("framesBudget is %v, which is not a slice of %v",
			framesBudget, analyzeTimeout)
	}
}

func TestFrames_OnlyRunWhenReadingFoundNothing(t *testing.T) {
	// Words beat pictures: somebody saying what they are doing is better
	// evidence than a model inferring it from four stills. Running both on
	// every video would double the cost of the videos that were already
	// answered, which are the ones that needed help least.
	src, err := os.ReadFile("analyze.go")
	if err != nil {
		t.Fatalf("read analyze.go: %v", err)
	}
	// Matched on the gate itself rather than an exact line, so a rename does
	// not fail this for the wrong reason — the property is that looking is
	// conditional on reading having produced no tags.
	gate := regexp.MustCompile(`if len\([A-Za-z.]*[Tt]ags\) == 0 \{`)
	if !gate.Match(src) {
		t.Error("the frames pass is no longer gated on the reading pass " +
			"finding nothing, so every video now pays for both")
	}
}

// ════════════════════════════════════════════════════════════════════════════
// TOPICS — WHAT THE VIDEO IS ABOUT, IN THE MODEL'S OWN WORDS
// ════════════════════════════════════════════════════════════════════════════
//
// Eighteen categories cannot describe a video. A clip about chanting the
// Hanuman Chalisa when a ghost frightens somebody is spiritual, devotional,
// paranormal and a bit comic, and the category list offers it "comedy" or
// "other". Topics are the answer to that, and they are deliberately the
// OPPOSITE of tags: open where tags are closed, unranked where tags are
// ranked. These say so, because the natural instinct on reading the code is
// to "fix" the missing validation.

func TestTopics_AreNotFilteredAgainstAnyList(t *testing.T) {
	// The whole feature. None of these is a category, a feeling, or anything
	// the backend has ever heard of — and every one of them describes a video
	// better than "comedy" does.
	got := understoodTopics(
		`{"categories": ["comedy"], "feelings": ["funny"],` +
			` "topics": ["hanuman chalisa", "ghost", "temple", "negative energy"]}`)
	if len(got) != 4 {
		t.Fatalf("got %v, want all four kept. Topics are not validated against "+
			"a vocabulary — that is the point of them.", got)
	}
	if got[0] != "hanuman chalisa" {
		t.Errorf("got %v; the model's own ordering must survive, because it "+
			"puts the main subject first and a sort would throw that away", got)
	}
}

func TestTopics_AreShapedEvenThoughTheyAreNotFiltered(t *testing.T) {
	// Not filtered is not the same as not cleaned. The same subject has to
	// read the same way twice or nothing can ever group by it.
	got := understoodTopics(
		`{"categories": ["food"], "topics": ["  Street   Food ", "STREET FOOD", "chai"]}`)
	if len(got) != 2 {
		t.Fatalf("got %v, want street food (once) and chai", got)
	}
	if got[0] != "street food" {
		t.Errorf("got %q, want %q — lowercased with inner spacing collapsed",
			got[0], "street food")
	}
}

func TestTopics_DropTheModelSayingItHasNothingToSay(t *testing.T) {
	// A model with no answer writes it into the field rather than leaving the
	// field out. None of these is a subject, and "other" as a TOPIC is
	// especially misleading — it is a real category name.
	for _, raw := range []string{
		`{"topics": ["other"]}`,
		`{"topics": ["none"]}`,
		`{"topics": ["unknown"]}`,
		`{"topics": ["..."]}`,
		`{"topics": [""]}`,
	} {
		if got := understoodTopics(`{"categories": ["other"], ` + raw[1:]); len(got) != 0 {
			t.Errorf("%s produced %v, want nothing", raw, got)
		}
	}
}

func TestTopics_AreCappedPerVideoNotInVocabulary(t *testing.T) {
	// The cap is on how many one video carries, not on which words exist. A
	// model asked for everything starts naming what is in shot rather than
	// what the video is about.
	many := `{"categories": ["food"], "topics": ["a1","b2","c3","d4","e5","f6","g7","h8","i9"]}`
	if got := understoodTopics(many); len(got) != understandMaxTopics {
		t.Errorf("got %d topics, want the cap of %d", len(got), understandMaxTopics)
	}
	// And a sentence written into the field is dropped rather than stored.
	long := `{"categories": ["food"], "topics": ["` +
		strings.Repeat("very long ", 20) + `"]}`
	if got := understoodTopics(long); len(got) != 0 {
		t.Errorf("got %v, want the over-long entry dropped", got)
	}
}

func TestTopics_DoNotReachTheRankedTags(t *testing.T) {
	// The safety property. Tags decide who a video is shown to; topics do not.
	// If a topic ever leaked into AutoTags it would be an unvalidated string
	// in the column the ranker reads — exactly what understoodTags exists to
	// prevent.
	answer := `{"categories": ["horror"], "feelings": ["scary"],` +
		` "topics": ["ghost", "hanuman chalisa"]}`
	tags := understoodTags(answer)
	for _, tag := range tags {
		if tag == "ghost" || tag == "hanuman chalisa" {
			t.Errorf("topic %q reached the ranked tags: %v", tag, tags)
		}
	}
	if len(tags) != 2 {
		t.Errorf("got %v, want just horror and scary", tags)
	}
}

func TestPrompt_AsksForTopicsWithoutGivingAList(t *testing.T) {
	p := buildUnderstandPrompt("anything")
	if !strings.Contains(p, "NOT a list to choose from") {
		t.Error("the prompt no longer tells the model topics are open. Given a " +
			"list it will pick from it, and the whole value here is the words " +
			"nobody thought to include.")
	}
	// Examples steer vocabulary without constraining it — an unguided model
	// drifts between "food", "cooking" and "making dinner" for one idea.
	if !strings.Contains(p, "hanuman chalisa") {
		t.Error("the steering examples are gone from the prompt")
	}
	// English topics for non-English videos, or the same subject reads two
	// ways and nothing can group by it.
	if !strings.Contains(p, "Write topics in English") {
		t.Error("the prompt no longer asks for topics in one language")
	}
}
