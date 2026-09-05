package main

// understand.go — working out what a video is ABOUT by reading its words with
// a language model, instead of looking them up in a list.
//
// ════════════════════════════════════════════════════════════════════════════
// WHY A MODEL AND NOT A LONGER LIST
// ════════════════════════════════════════════════════════════════════════════
//
// The keyword list in analyze.go does not understand anything. It looks for
// spellings. Two failures in production, both the same fault wearing different
// clothes:
//
//   - Challenge 262 says nobody knows what the future holds. "भविष्य" (future)
//     is in the list, but whisper wrote भवि-श्-य where the word is भवि-ष्-य.
//     One letter, and the video earns nothing. Anyone reading the sentence can
//     see what it is about.
//
//   - Challenge 260 is about reciting the Hanuman Chalisa when something
//     frightens you. It was tagged "dance", because with the vowel marks
//     stripped "हनूमान चालिशा" contains the same consonants as "नाच". Two
//     unrelated phrases, one string.
//
// Neither is fixable by adding words. The list will always need the exact
// spelling of a thing nobody promised to say, and it will always be blind to
// meaning. A model reads a sentence the way a person does: a misspelling does
// not stop it understanding, and a coincidence of letters does not fool it.
//
// ════════════════════════════════════════════════════════════════════════════
// WHY THIS ONE, AND WHY IT COSTS NOTHING
// ════════════════════════════════════════════════════════════════════════════
//
// Qwen3-VL-4B-Instruct, run locally through llama.cpp. Apache 2.0, so it is
// free to use commercially, and it runs on the same free GitHub runner that
// already builds and caches whisper.cpp — the pattern this copies exactly. No
// API key, no bill, no rate limit, no company able to change the terms.
//
// It is a VISION model, which is deliberate even though this pass only reads
// words. Most of the catalogue says nothing at all — of 114 videos, 79 produce
// no transcript — and no amount of reading helps a video with no words in it.
// The same model file can look at frames, so the half that is silent is a
// second pass with the same download rather than a second model. This file is
// the first half.
//
// ════════════════════════════════════════════════════════════════════════════
// WHAT IT IS ALLOWED TO SAY
// ════════════════════════════════════════════════════════════════════════════
//
// Nothing the backend does not already understand. A model asked for a
// category will happily invent one, and a tag no downstream stage knows is a
// tag nobody can ever match on — it looks like a working feature and ranks
// nothing. So every word it returns is checked against the lists below and
// dropped if it is not there. See understoodTags.

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
)

const (
	understandBinEnv   = "UNDERSTAND_BIN"
	understandModelEnv = "UNDERSTAND_MODEL"
)

// understandContextTokens is how much room the model is given.
//
// IT HAS TO BE SET, and it has to be set small. Qwen3-VL's own context is
// enormous, and llama.cpp reserves memory for the whole of it up front —
// asking for the default killed the process outright on a 16 GB machine
// before it read a single word:
//
//	llama-cli ... -p "Reply with exactly: OK"
//	Killed                                          (exit 137)
//
// The same command with -c 2048 answers in a few seconds. Nothing about that
// failure says "context" — it is an out-of-memory kill with no message, which
// on a runner looks exactly like the feature being switched off.
//
// 4096 leaves room for the instructions (~400 tokens) plus a long transcript,
// and truncateForModel below keeps the transcript inside it.
const understandContextTokens = 4096

// understandMaxOutput bounds the answer. It is one line of JSON; anything
// longer means the model is rambling and the parse will take the last object
// it finds anyway.
const understandMaxOutput = 96

// understandThreads is HALF the runner's cores, not all of them.
//
// The workflow runs two workers side by side on a four-core runner — see the
// "TWO parallel workers" note in hls-worker.yml. Asking for four threads in
// each means eight threads fighting over four cores whenever both workers
// reach this pass together, which is slower than two threads each, not
// faster. Both are also encoding video at the time.
const understandThreads = 2

// understandMaxSpeechChars caps how much of a transcript is sent.
//
// A minute of continuous speech is comfortably under this. The cap is for the
// pathological case — a stuck transcription repeating one phrase hundreds of
// times, which happens on short or noisy audio and which we have seen (one
// stored transcript is "I don't know why I'm doing this" over and over). That
// costs prompt-processing time and says nothing the first few lines did not.
const understandMaxSpeechChars = 2000

// understandCategories are the ONLY categories this pass may produce, with the
// descriptions the model is given to tell them apart.
//
// Names and meanings come straight from ContentCategories in models.go. Kept
// here rather than imported because the worker is a separate program; if that
// list changes, this one has to change with it, the same way the tag allowlist
// in the keyword tests does.
//
// The descriptions are not decoration. Given bare names the model has to guess
// what the app means by each one, and it guesses badly: asked to label "having
// ideas is easy, the important thing is how well you execute", it answered
// comedy. The descriptions are what make the names mean what this app means.
var understandCategories = []struct{ Name, Means string }{
	{"comedy", "funny, humour, roasts, pranks, memes"},
	{"motivation", "inspirational, discipline, success, hustle, advice about life"},
	{"sports", "athletic skills, sports challenges, fitness"},
	{"dance", "choreography, dance battles, freestyle"},
	{"music", "singing, instruments, beatbox, rap"},
	{"gaming", "gameplay, esports, game challenges"},
	{"art", "drawing, painting, creative crafts"},
	{"education", "tutorials, how-to, explaining or teaching something"},
	{"story", "vlogs, storytime, personal experiences, scenes and dialogue"},
	{"fashion", "style, beauty, outfit challenges"},
	{"food", "cooking, food challenges, recipes"},
	{"horror", "scary, thriller, creepy"},
	{"emotional", "sad, heartfelt, deep emotional content, relationships"},
	{"lifestyle", "day in the life, routines, wellness"},
	{"tech", "technology, coding, gadgets"},
	{"prank", "pranks, social experiments"},
	{"news", "commentary, opinions, current events"},
	{"other", "anything that does not clearly fit above"},
}

// understandEmotions are the only feelings this pass may produce. Same source
// and same rule as the categories — these are EmotionLabels from models.go,
// and the ranker matches them against what a viewer is in the mood for.
var understandEmotions = []string{
	"happy", "sad", "intense", "chill", "inspiring", "scary", "funny",
	"serious", "aggressive", "romantic", "nostalgic", "satisfying", "cringe",
	"wholesome", "suspenseful", "empowering",
}

// understandPrompt is the whole instruction. Every paragraph in it is there
// because of something that went wrong without it — see the notes on
// understandCategories, and the bracket rule below.
const understandPrompt = `You label short videos for a social video app. You are given a transcript of what is said in the video, and you decide what the video is about.

CATEGORIES — pick one, or at most two if the video genuinely spans both:
%s

FEELINGS — pick up to two, or none:
%s

How to judge:
- The transcript may be in any language and WILL contain transcription mistakes. Judge what the speaker means, not how words are spelled.
- Text in brackets like (music playing), (dramatic music), (door squeaking) is the transcriber describing SOUND, not somebody speaking. A transcript that is only bracketed sounds means nobody said anything: answer "other".
- A few words, or words with no subject, means you cannot tell. Answer "other".
- "other" is a correct and useful answer. A wrong category is worse than "other", because the app will show this video to people who asked for something else.

Answer with one line of JSON and nothing else:
{"categories": ["..."], "feelings": ["..."]}

Transcript:
%s
`

// understandReply is the shape the model is asked for. Anything else it says
// is ignored.
type understandReply struct {
	Categories []string `json:"categories"`
	Feelings   []string `json:"feelings"`
}

// lastJSONObject finds the final {...} containing "categories". The CLI prints
// its own banner and, depending on build, may echo parts of the prompt — which
// itself contains an example object — so the LAST match is the answer.
var lastJSONObject = regexp.MustCompile(`(?s)\{[^{}]*"categories"[^{}]*\}`)

// understandContent reads what the video said and returns the tags a model
// makes of it.
//
// The second return says whether the model actually RAN, for the same reason
// transcribeSpeech reports it: "the model read this and could not tell" and
// "there is no model on this machine" both come back as no tags, and only one
// of them means the feature is off. Callers use it to decide whether to fall
// back to the keyword list.
func understandContent(ctx context.Context, a videoAnalysis) ([]string, bool) {
	bin := strings.TrimSpace(os.Getenv(understandBinEnv))
	model := strings.TrimSpace(os.Getenv(understandModelEnv))
	if bin == "" || model == "" {
		return nil, false
	}
	if _, err := exec.LookPath(bin); err != nil {
		log.Printf("analyze: %s is set to %q but that will not run: %v",
			understandBinEnv, bin, err)
		return nil, false
	}

	// Both sources of words, the same pair the keyword list reads. A caption
	// burned onto the screen is often the clearest statement of what a video
	// is, and plenty of videos have one and no speech.
	said := strings.TrimSpace(a.ScreenText + "\n" + a.Speech)
	if len(strings.Fields(said)) < understandMinWords {
		// Not enough to judge. Saying so here rather than asking the model
		// saves a run per silent video, which is most of them.
		return nil, false
	}

	prompt := buildUnderstandPrompt(said)
	out, err := exec.CommandContext(ctx, bin,
		"-m", model,
		// See understandContextTokens — without this the process is killed.
		"-c", strconv.Itoa(understandContextTokens),
		"-p", prompt,
		"-n", strconv.Itoa(understandMaxOutput),
		"-t", strconv.Itoa(understandThreads),
		"--temp", "0", // same transcript, same answer, every run
		"-st",                 // answer once and exit, no chat session
		"--no-warmup",         // nothing to warm for a single answer
		"--no-display-prompt", // keep the instructions out of stdout
	).Output()
	if err != nil {
		log.Printf("analyze: understand pass failed: %v", err)
		return nil, false
	}

	tags := understoodTags(string(out))
	return tags, true
}

// understandMinWords is the floor below which the model is not asked.
//
// Whisper emits sound effects in brackets for videos where nobody speaks —
// "(music playing) (door squeaking)" is a whole stored transcript. Those are
// only a handful of words and contain no statement about anything, so there is
// nothing to understand and a run would be spent finding that out.
const understandMinWords = 6

// buildUnderstandPrompt assembles the instruction, with the transcript capped.
func buildUnderstandPrompt(said string) string {
	var cats strings.Builder
	for _, c := range understandCategories {
		cats.WriteString("  " + c.Name + " — " + c.Means + "\n")
	}
	return fmt.Sprintf(understandPrompt,
		strings.TrimRight(cats.String(), "\n"),
		strings.Join(understandEmotions, ", "),
		truncateForModel(said, understandMaxSpeechChars))
}

// truncateForModel caps the transcript at a rune boundary.
//
// Byte slicing would cut a Devanagari or emoji character in half and hand the
// model a broken rune, which is a silly way to make a Hindi video harder to
// read than an English one.
func truncateForModel(s string, max int) string {
	r := []rune(s)
	if len(r) <= max {
		return s
	}
	return string(r[:max])
}

// understoodTags turns the model's answer into tags this app can use, and
// drops everything else.
//
// EVERY word is checked against the lists above. A model asked for a category
// will invent one when it is unsure — and a tag no downstream stage knows is
// worse than no tag, because it looks like the feature working while ranking
// nothing. Seen in testing: asked for feelings it could not judge, the model
// answered "other", which is a category name and not a feeling at all.
//
// "other" is dropped rather than stored. It is the model saying it could not
// tell, which is worth knowing and is not a subject; storing it would file the
// video under a category the ranker then matches people to.
func understoodTags(raw string) []string {
	m := lastJSONObject.FindAllString(raw, -1)
	if len(m) == 0 {
		return nil
	}
	var reply understandReply
	if err := json.Unmarshal([]byte(m[len(m)-1]), &reply); err != nil {
		return nil
	}

	known := make(map[string]bool, len(understandCategories)+len(understandEmotions))
	for _, c := range understandCategories {
		known[c.Name] = true
	}
	for _, e := range understandEmotions {
		known[e] = true
	}

	var out []string
	for _, t := range append(reply.Categories, reply.Feelings...) {
		t = strings.ToLower(strings.TrimSpace(t))
		if t == "" || t == "other" || !known[t] {
			continue
		}
		out = append(out, t)
	}
	return dedupeSorted(out)
}
