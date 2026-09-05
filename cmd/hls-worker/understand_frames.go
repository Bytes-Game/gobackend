package main

// understand_frames.go — working out what a video is about by LOOKING at it,
// for the videos that never say anything.
//
// ════════════════════════════════════════════════════════════════════════════
// WHY THIS HALF EXISTS
// ════════════════════════════════════════════════════════════════════════════
//
// Reading the words was only ever half the problem, and the smaller half. Of
// 114 videos in the catalogue, 79 produce no transcript at all — and of those,
// 62 have no readable text on screen either. Nothing that only reads can help
// them. They are not badly categorised; they are uncategorised, and always
// were, and a better word list or a better reader changes none of it.
//
// A picture is the only evidence those videos offer. So the same model that
// reads the transcript in understand.go is shown a few frames instead.
//
// The same model, deliberately, and it is why Qwen3-VL was chosen over a
// text-only one that would have scored the same on transcripts. The weights
// are already downloaded and already cached; this half adds one companion
// file (the vision projector, ~450MB) rather than a second model, a second
// cache and a second thing to keep in step.
//
// ════════════════════════════════════════════════════════════════════════════
// WHAT IT IS AND IS NOT ASKED
// ════════════════════════════════════════════════════════════════════════════
//
// It is asked the same question, against the same categories, and its answer
// goes through the same validation — understoodTags — so it can no more invent
// a tag than the reading pass can.
//
// It is NOT asked to describe the video. A description would be a new kind of
// data that nothing downstream knows how to use, and the temptation would then
// be to keyword-match the description, which is the exact mistake this whole
// line of work exists to undo.

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

const (
	// The multimodal CLI, which is a different binary from the one that reads
	// text — llama-mtmd-cli rather than llama-cli. Kept as its own variable so
	// a runner that built one and not the other switches this half off rather
	// than failing every video.
	framesBinEnv = "UNDERSTAND_FRAMES_BIN"
	// The vision projector: the companion file that turns pixels into
	// something the language model can read. Same repository as the weights.
	framesProjectorEnv = "UNDERSTAND_PROJECTOR"
)

// understandFrames is how many stills are shown.
//
// Four rather than the six the OCR pass takes. Each frame costs real time to
// look at, and unlike OCR — where more frames means more chances to catch a
// caption that appears once — the question here is "what IS this", which four
// evenly spread stills answer about as well as six.
const understandFrames = 4

// framesWidth is what each still is scaled to before the model sees it.
//
// ════════════════════════════════════════════════════════════════════════════
// THIS NUMBER IS THE WHOLE COST OF THE PASS
// ════════════════════════════════════════════════════════════════════════════
//
// Turning a picture into something the model can read is far and away the
// expensive part, and it does not get more expensive smoothly. Measured on the
// same image, same machine, four threads:
//
//	512 wide    18,675 ms
//	256 wide     1,549 ms
//
// Twelve times cheaper for four times fewer pixels — the encoder tiles large
// images, and 512 crosses into more tiles. At 512 a four-frame pass would
// spend over a minute on encoding alone and blow its budget; at 256 it spends
// about six seconds.
//
// And it costs nothing that matters. The 256-wide version of a title card was
// still read correctly, and more plainly than the large one:
//
//	512: "a title card for a guide on how to cook Butter Chicken at home,
//	      set against a dark background with contrasting white and yellow text"
//	256: "This video is about how to cook butter chicken at home."
//
// If frames ever need to be bigger, this is the knob — but measure the
// encoding time again before raising it, because the cost is a cliff rather
// than a slope.
//
// Height follows the source, so nothing is squashed. A reel is a tall video,
// and stretching one to a square would be a strange thing to ask a model to
// interpret.
const framesWidth = 256

// understandFramesPrompt asks the same question of a picture that
// understandPrompt asks of a transcript, and errs the same way.
//
// Including the topics question, which it originally left out. That omission
// meant the videos that most need describing could never be described: reading
// declines on a silent video, so looking is the ONLY pass that ever runs on
// it, and 66 of the app's 96 analysed videos say nothing at all. Eighteen
// categories were never going to be enough for them — that is the whole reason
// topics exist — and picking one of eighteen was all this pass could do.
//
// A picture is in fact good evidence for a topic. What a video shows is often
// more concrete than what somebody says about it: a plate of biryani, a temple
// doorway, a cricket bat. The instructions differ from the reading version
// only where seeing differs from hearing.
const understandFramesPrompt = `You label short videos for a social video app. You are shown a few frames taken from one video, spread across its length. Decide what the video is about.

CATEGORIES — pick one, or at most two if the video genuinely spans both:
%s

FEELINGS — pick up to two, or none:
%s

TOPICS — up to %d short phrases saying what the video is actually about.
Unlike the categories, these are NOT a list to choose from. Write whatever
fits, in two or three words each. Name what you can SEE: the object, the food,
the place, the activity. For example: %s.

How to judge:
- These frames are from ONE video. Judge the video as a whole, not each frame separately.
- Nobody speaks in this video, or nothing they said could be made out. The pictures are all the evidence there is.
- If the frames are too dark, too blurred, or too ordinary to tell what the video is about, answer "other" with no topics. A person, a room or a street on its own is not a subject.
- "other" is a correct and useful answer for the CATEGORY. A wrong category is worse than "other", because the app will show this video to people who asked for something else. Topics are different: nothing is filed by them, so name anything you can genuinely see.
- Write topics in English, so the same subject reads the same way across the app.

Answer with one line of JSON and nothing else:
{"categories": ["..."], "feelings": ["..."], "topics": ["..."]}
`

// understandContentFromFrames looks at a video and returns the tags a model
// makes of what it sees.
//
// Same second-return contract as every other pass: it says whether the model
// RAN, so "looked and could not tell" stays distinguishable from "there was
// nothing here to look with".
func understandContentFromFrames(ctx context.Context, src string, dur float64) (understood, bool) {
	bin := strings.TrimSpace(os.Getenv(framesBinEnv))
	model := strings.TrimSpace(os.Getenv(understandModelEnv))
	proj := strings.TrimSpace(os.Getenv(framesProjectorEnv))
	if bin == "" || model == "" || proj == "" {
		return understood{}, false
	}
	if _, err := exec.LookPath(bin); err != nil {
		log.Printf("analyze: %s is set to %q but that will not run: %v",
			framesBinEnv, bin, err)
		return understood{}, false
	}

	frames, cleanup, err := extractFrames(ctx, src, dur)
	if err != nil || len(frames) == 0 {
		return understood{}, false
	}
	defer cleanup()

	args := []string{
		"-m", model,
		"--mmproj", proj,
		// Same trap as the reading pass, and worse here: the pictures take
		// context too. Without an explicit bound the process is killed before
		// it looks at anything. See understandContextTokens.
		"-c", strconv.Itoa(understandFramesContextTokens),
		"-n", strconv.Itoa(understandMaxOutput),
		"-t", strconv.Itoa(understandThreads),
		"--temp", "0",
	}
	for _, f := range frames {
		args = append(args, "--image", f)
	}
	args = append(args, "-p", buildFramesPrompt())

	out, err := exec.CommandContext(ctx, bin, args...).Output()
	if err != nil {
		log.Printf("analyze: frames pass failed: %v", err)
		return understood{}, false
	}
	answer := string(out)
	return understood{Tags: understoodTags(answer), Topics: understoodTopics(answer)}, true
}

// understandFramesContextTokens is bigger than the reading pass's bound
// because the frames themselves occupy context — each still becomes a few
// hundred tokens once the projector has turned it into something the model
// can read, and there are four of them.
//
// Still explicitly bounded, for the same reason and with the same failure if
// it is not: an out-of-memory kill with no message, which looks exactly like
// the feature being switched off.
const understandFramesContextTokens = 8192

// buildFramesPrompt assembles the instruction. Shares the category list and
// descriptions with the reading pass so the two halves cannot drift into
// answering different questions.
func buildFramesPrompt() string {
	var cats strings.Builder
	for _, c := range understandCategories {
		cats.WriteString("  " + c.Name + " — " + c.Means + "\n")
	}
	return fmt.Sprintf(understandFramesPrompt,
		strings.TrimRight(cats.String(), "\n"),
		strings.Join(understandEmotions, ", "),
		understandMaxTopics,
		strings.Join(understandTopicExamples, ", "))
}

// extractFrames pulls evenly spread stills and returns their paths plus a
// cleanup function.
//
// Same approach as readScreenText, and for the same reason: taking the first
// N frames of a video gets four near-identical stills of its opening moment,
// which is the least informative thing a video contains — a title card, a
// logo, or somebody drawing breath.
func extractFrames(ctx context.Context, src string, dur float64) ([]string, func(), error) {
	dir, err := os.MkdirTemp("", "vis")
	if err != nil {
		return nil, func() {}, err
	}
	cleanup := func() { os.RemoveAll(dir) }

	fps := "1"
	if dur > 0 {
		fps = strconv.FormatFloat(float64(understandFrames)/dur, 'f', 6, 64)
	}
	pattern := filepath.Join(dir, "f%03d.png")
	extract := exec.CommandContext(ctx, "ffmpeg",
		"-hide_banner", "-nostats", "-loglevel", "error",
		"-i", src,
		"-vf", "fps="+fps+",scale="+strconv.Itoa(framesWidth)+":-1",
		"-frames:v", strconv.Itoa(understandFrames),
		pattern,
	)
	if err := extract.Run(); err != nil {
		cleanup()
		return nil, func() {}, err
	}

	frames, _ := filepath.Glob(filepath.Join(dir, "*.png"))
	sort.Strings(frames) // oldest moment first, so the model sees them in order
	if len(frames) == 0 {
		cleanup()
		return nil, func() {}, errNoFrames
	}
	return frames, cleanup, nil
}

// errNoFrames is what a video ffmpeg produced no stills from looks like —
// a zero-length or unreadable file, which the transcode would already have
// failed on, so it is a genuinely unexpected case rather than a routine one.
var errNoFrames = errors.New("no frames could be extracted")
