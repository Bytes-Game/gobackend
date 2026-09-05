package main

// analyze.go — working out what a video IS, by looking at the video.
//
// ════════════════════════════════════════════════════════════════════════════
// WHY THIS RUNS HERE
// ════════════════════════════════════════════════════════════════════════════
//
// The backend already knows two things about a video's subject: what the
// creator typed, and what its audience did afterwards. Both are useful and
// both have a hole in the middle — a video posted a minute ago with a blank
// caption is invisible to the first and unknown to the second.
//
// This worker is the one place with the actual file in hand, and it already
// has ffmpeg. So it is the only place that can look.
//
// ════════════════════════════════════════════════════════════════════════════
// THREE PASSES, IN ORDER OF WHAT THEY COST
// ════════════════════════════════════════════════════════════════════════════
//
//	SHAPE   — ffmpeg alone. How fast it cuts, how loud, how much motion, how
//	          bright. Costs one pass over an already-downloaded file and needs
//	          nothing that is not already installed.
//	TEXT    — words burned into the picture, via tesseract on a handful of
//	          frames. The single best description of a short video is very
//	          often the caption its creator typed onto it.
//	SPEECH  — what is said out loud, via whisper.
//
// EVERY PASS IS OPTIONAL AND FAILS SILENT. If tesseract is not installed the
// text pass is skipped; if whisper is not installed the speech pass is
// skipped; if ffmpeg's filters error the shape pass returns nothing. A video
// still transcodes, still uploads, still appears in the feed. Analysis is a
// bonus signal, never a gate — a worker that refused to finish a job because
// an optional binary was missing would take the whole upload path down for a
// feature nobody asked to be blocking.
//
// Silent is not the same as invisible. Passes records which passes RAN, and
// logAnalysis prints the same thing at the end of every job, so "the video had
// nothing to say" can be told apart from "nothing listened to it" — from the
// stored analysis and from the workflow log, without guessing.
//
// ════════════════════════════════════════════════════════════════════════════
// WHICH PASSES ARE ON RIGHT NOW
// ════════════════════════════════════════════════════════════════════════════
//
// All three. Speech was the last one on and the workflow installs whisper.cpp
// and sets its two environment variables; the "check whisper works" step there
// transcribes a second of silence on every run, so a broken install fails the
// job instead of quietly turning the feature off.
//
// This worker runs in GitHub Actions, from .github/workflows/hls-worker.yml —
// every 30 minutes on a schedule, and immediately after an upload because the
// backend pokes it (transcode_wakeup.go). That workflow is where tools are
// installed and where environment variables are set. cmd/hls-worker/Dockerfile
// is the container version of the same worker and is NOT what production uses;
// installing something there alone does nothing to the live app.
//
// To turn speech on: put a whisper.cpp binary and a model wherever the worker
// runs, and set WHISPER_BIN and WHISPER_MODEL. Nothing else changes.
//
// It must be whisper.cpp, NOT OpenAI's Python program of the same name. The
// call below passes -m, -f, -nt, -np and -l, which is whisper.cpp's command
// line; the Python one takes different arguments, so pointing WHISPER_BIN at
// it makes every video fail silently — indistinguishable from the feature
// being switched off.

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"
)

// videoAnalysis is what one video's inspection produced. Every field is
// optional; a zero value means "not measured", which readers must treat
// differently from "measured as zero".
type videoAnalysis struct {
	// Shape — how the video is put together.
	CutsPerMinute float64 `json:"cutsPerMinute,omitempty"` // scene changes per minute
	MotionScore   float64 `json:"motionScore,omitempty"`   // 0 still, 1 constant movement
	Loudness      float64 `json:"loudness,omitempty"`      // integrated LUFS, negative
	SpeechRatio   float64 `json:"speechRatio,omitempty"`   // share of the video that is not silence
	Brightness    float64 `json:"brightness,omitempty"`    // 0 dark, 1 bright

	// Words — what the video says, on screen and out loud.
	ScreenText string `json:"screenText,omitempty"`
	Speech     string `json:"speech,omitempty"`

	// Tags derived from all of the above, already folded into the same shape
	// the backend stores creator tags in.
	AutoTags []string `json:"autoTags,omitempty"`

	// What the video is about, in the model's own words — "hanuman chalisa",
	// "street food", "long distance relationship". Free-form on purpose and
	// separate from AutoTags on purpose: nothing ranks on these, so they can
	// describe a video far more precisely than eighteen categories ever
	// could. See the topics note in understand.go.
	Topics []string `json:"topics,omitempty"`

	// Which passes actually ran, so the backend can tell "quiet video" from
	// "we never listened".
	Passes []string `json:"passes,omitempty"`
}

// analyzeTimeout bounds the whole inspection.
//
// Generous next to the transcode it runs beside, and hard: a wedged ffmpeg
// filter or a whisper run on a pathological file must never hold a worker
// slot open, because the queue behind it is the upload path for every
// creator on the app.
//
// Six rather than three, because the speech model got bigger. Measured on a
// real job with the `small` model, all three passes together took 47-56
// seconds on a short clip; `medium` is roughly 2.5x that model, and a reel
// can run to 60 seconds. Three minutes left no room for the worst of those,
// and running out here is silent — analyzeVideo never fails, so a timeout
// just loses the transcript with nothing saying why.
//
// It has to stay under the worker's -job-timeout, which the workflow moved
// to 10m for the same reason. See the budget sum in hls-worker.yml.
const analyzeTimeout = 6 * time.Minute

// Each pass also gets its OWN slice of that budget.
//
// ════════════════════════════════════════════════════════════════════════════
// WHY ONE SHARED POT WAS NOT ENOUGH
// ════════════════════════════════════════════════════════════════════════════
//
// The passes run one after another against a single deadline, so whichever
// one is slow spends the time belonging to the ones behind it. That is not
// hypothetical — the speech pass ran to the full six minutes on challenge 260
// and was killed, and by then the clock was gone, so the pass after it never
// started at all. Two features lost to one slow model.
//
// The order matters here too. Speech feeds understanding: a transcript that
// arrives is read, and a transcript that is cut off leaves the video with
// nothing to read. So the pass most likely to overrun sits directly in front
// of the pass that depends on it, which is the worst possible arrangement
// under a shared clock.
//
// Per-pass budgets fix that. A wedged whisper now costs its own pass and
// nothing else, and the sum deliberately exceeds analyzeTimeout — these bound
// each pass individually, while analyzeTimeout stays the hard ceiling on all
// of them together.
const (
	shapeBudget      = 1 * time.Minute
	screenTextBudget = 1 * time.Minute
	speechBudget     = 3 * time.Minute
	understandBudget = 2 * time.Minute
	framesBudget     = 2 * time.Minute
)

// analyzeVideo inspects a local file and returns everything it could work
// out. It never returns an error: a failed pass is a missing field, not a
// failed job.
func analyzeVideo(ctx context.Context, src string) videoAnalysis {
	ctx, cancel := context.WithTimeout(ctx, analyzeTimeout)
	defer cancel()

	var a videoAnalysis
	dur := probeDuration(ctx, src)

	shapeCtx, cancelShape := context.WithTimeout(ctx, shapeBudget)
	ranShape := analyzeShape(shapeCtx, src, dur, &a)
	cancelShape()
	if ranShape {
		a.Passes = append(a.Passes, "shape")
	}

	// A pass is recorded when it RAN, not when it found something. Those are
	// different facts and the difference is the whole reason Passes exists:
	// a video with nobody talking and a worker with no whisper installed both
	// produce an empty transcript, and only one of them is a problem.
	textCtx, cancelText := context.WithTimeout(ctx, screenTextBudget)
	txt, ranText := readScreenText(textCtx, src, dur)
	cancelText()
	if ranText {
		a.ScreenText = txt
		a.Passes = append(a.Passes, "text")
	}

	speechCtx, cancelSpeech := context.WithTimeout(ctx, speechBudget)
	sp, ranSpeech := transcribeSpeech(speechCtx, src)
	cancelSpeech()
	if ranSpeech {
		a.Speech = sp
		a.Passes = append(a.Passes, "speech")
	}

	a.AutoTags = tagsFromAnalysis(a)

	// A fourth pass: read the words back and work out what the video is
	// ABOUT, rather than which spellings happen to appear in it. See
	// understand.go for why a word list could never do that.
	//
	// WHEN IT RUNS, IT WINS — including when it comes back with nothing.
	// The model saying "I cannot tell what this is" is a judgement, and it
	// is a better one than a keyword list's guess, which is how a video
	// about reciting the Hanuman Chalisa came to be filed under dance. A
	// wrong tag is worse than no tag: it shows the video to people who
	// asked for something else.
	//
	// Shape tags survive either way. Those are measurements of the file —
	// how often it cuts, whether anyone is talking — and are just as true
	// whoever read the words.
	understandCtx, cancelUnderstand := context.WithTimeout(ctx, understandBudget)
	read, ranUnderstand := understandContent(understandCtx, a)
	cancelUnderstand()
	if ranUnderstand {
		a.Passes = append(a.Passes, "understand")
		a.AutoTags = dedupeStable(append(read.Tags, shapeTags(a)...))
		// Topics are stored as they came, not merged into AutoTags. Nothing
		// ranks on them and that is the point — see the topics note in
		// understand.go.
		a.Topics = read.Topics
	}

	// And the other half: for a video that said nothing, LOOK at it.
	//
	// Only when reading came back with nothing, which is the whole point.
	// Words are better evidence than pictures — somebody saying what they are
	// doing beats a model inferring it from four stills — so when there are
	// words worth reading, this does not run and does not cost anything. It
	// exists for the 79 videos of 114 that produce no transcript at all, which
	// no amount of reading has ever been able to help.
	//
	// That gate is also what keeps the budget honest. A silent video reaches
	// here having spent almost nothing: with no audio track the speech pass
	// returns immediately, and the reading pass declines before starting a
	// model. So the pass that needs the time is the one that gets it.
	if len(read.Tags) == 0 {
		framesCtx, cancelFrames := context.WithTimeout(ctx, framesBudget)
		seen, ranFrames := understandContentFromFrames(framesCtx, src, dur)
		cancelFrames()
		if ranFrames {
			a.Passes = append(a.Passes, "frames")
			a.AutoTags = dedupeStable(append(seen.Tags, shapeTags(a)...))
			if len(seen.Topics) > 0 {
				a.Topics = seen.Topics
			}
		}
	}

	logAnalysis(src, a)
	return a
}

// logAnalysis prints one line saying what the inspection actually did.
//
// It exists because the worker used to be silent about this, and silence is
// indistinguishable from "it did not run". Somebody uploads a video, wants to
// know whether the app listened to it, and the only evidence in the whole
// workflow log is how long the job took.
//
// The words themselves are deliberately NOT printed. A transcript is whatever
// somebody said into their phone, and workflow logs on this repo are readable
// by anyone who can see the Actions tab. Counts answer the question — "we
// listened and heard eleven words" — without publishing the sentence.
func logAnalysis(src string, a videoAnalysis) {
	passes := "none"
	if len(a.Passes) > 0 {
		passes = strings.Join(a.Passes, "+")
	}
	log.Printf("analyze: %s passes=%s speechWords=%d screenTextChars=%d tags=%v",
		filepath.Base(src), passes,
		len(strings.Fields(a.Speech)), len(a.ScreenText), a.AutoTags)
}

// ════════════════════════════════════════════════════════════════════════════
// SHAPE — ffmpeg only
// ════════════════════════════════════════════════════════════════════════════

func probeDuration(ctx context.Context, src string) float64 {
	out, err := exec.CommandContext(ctx, "ffprobe",
		"-v", "error",
		"-show_entries", "format=duration",
		"-of", "default=noprint_wrappers=1:nokey=1",
		src).Output()
	if err != nil {
		return 0
	}
	d, _ := strconv.ParseFloat(strings.TrimSpace(string(out)), 64)
	return d
}

// hasAudioStream reports whether the file has any audio at all.
//
// Worth a separate probe because plenty of short video has none — a clip
// filmed with the phone muted, an export that dropped the track, anything
// screen-recorded. Handing ffmpeg a filter graph that reads [0:a] on one of
// those does not degrade gracefully: the whole command fails, and the picture
// measurements are lost along with the sound ones, for a video where the
// picture is all there was to measure.
func hasAudioStream(ctx context.Context, src string) bool {
	out, err := exec.CommandContext(ctx, "ffprobe",
		"-v", "error",
		"-select_streams", "a",
		"-show_entries", "stream=index",
		"-of", "csv=p=0",
		src).Output()
	if err != nil {
		return false
	}
	return strings.TrimSpace(string(out)) != ""
}

// What ffmpeg actually prints. Every one of these was checked against real
// ffmpeg output rather than written from memory, because three earlier
// versions of them looked plausible and matched nothing — see the note on
// analyzeShape about why that failure is invisible.
var (
	// scdet prints a score for EVERY frame and a `time` only for frames it
	// judges to be a cut. Counting scores would count movement, not cuts: on
	// continuously-moving footage nearly every frame scores above zero, which
	// on one 11-second test clip turned 0 real cuts into 327.
	//
	// Metadata form (`=`) only. scdet also logs its own line for the same cut
	// in `key: value` form, and matching both would double every count.
	reSceneCut = regexp.MustCompile(`lavfi\.scd\.time=`)

	// signalstats namespaces its keys and separates with `=`, not `:`.
	reYAVG = regexp.MustCompile(`lavfi\.signalstats\.YAVG=([0-9.]+)`)

	// ebur128 prints one indented summary block when the stream ends:
	//
	//	Integrated loudness:
	//	  I:         -15.4 LUFS
	//
	// The `"input_i": "-15.4"` JSON shape belongs to the loudnorm filter,
	// which is a different filter that this pass does not run.
	reLoudness = regexp.MustCompile(`(?m)^\s*I:\s+(-?[0-9.]+)\s+LUFS`)

	// silencedetect prints this at the END of each silent stretch.
	reSilenceDur = regexp.MustCompile(`silence_duration:\s*([0-9.]+)`)
)

// analyzeShape runs one ffmpeg pass that measures several things at once.
//
// One pass rather than four, because decoding the video is the expensive part
// and the filters themselves are nearly free. The filters write to stderr as
// metadata lines, which is why the output is scraped rather than parsed —
// ffmpeg has no structured output for this.
func analyzeShape(ctx context.Context, src string, dur float64, a *videoAnalysis) bool {
	// scdet   — marks scene changes, so cuts can be counted
	// signalstats — YAVG is average luma, i.e. how bright the picture is
	// ebur128 — integrated loudness of the audio
	// silencedetect — how much of the audio is silence
	//
	// The audio half is added only when there is audio to read. ffmpeg treats
	// a filter graph referencing a stream that does not exist as a hard error
	// and gives up on the whole command, so on a silent clip asking for it
	// would cost the picture measurements too.
	filter := "[0:v]scdet=threshold=10,signalstats,metadata=print[v]"
	args := []string{"-hide_banner", "-nostats", "-i", src}
	maps := []string{"-map", "[v]"}
	audio := hasAudioStream(ctx, src)
	if audio {
		filter += ";[0:a]ebur128=metadata=1,silencedetect=n=-30dB:d=0.5[a]"
		maps = append(maps, "-map", "[a]")
	} else {
		// No audio is a real reading, not a missing one: a video with no
		// sound is silent, which is exactly what SpeechRatio 0 means.
		a.SpeechRatio = 0
	}
	args = append(args, "-filter_complex", filter)
	args = append(args, maps...)
	args = append(args, "-f", "null", "-")

	cmd := exec.CommandContext(ctx, "ffmpeg", args...)
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return false
	}
	if err := cmd.Start(); err != nil {
		return false
	}

	var cuts int
	var lumaSum float64
	var lumaN int
	var silence float64
	var loudness float64
	var sawLoudness bool

	sc := bufio.NewScanner(stderr)
	sc.Buffer(make([]byte, 64*1024), 1024*1024)
	for sc.Scan() {
		line := sc.Text()
		if reSceneCut.MatchString(line) {
			cuts++
		}
		if m := reYAVG.FindStringSubmatch(line); m != nil {
			if v, e := strconv.ParseFloat(m[1], 64); e == nil {
				lumaSum += v
				lumaN++
			}
		}
		if m := reSilenceDur.FindStringSubmatch(line); m != nil {
			if v, e := strconv.ParseFloat(m[1], 64); e == nil {
				silence += v
			}
		}
		if m := reLoudness.FindStringSubmatch(line); m != nil {
			if v, e := strconv.ParseFloat(m[1], 64); e == nil {
				loudness, sawLoudness = v, true
			}
		}
	}
	_ = cmd.Wait()

	// Did we understand the output at all?
	//
	// signalstats reports on EVERY frame, so a video that decoded produces
	// hundreds of luma readings. Zero of them means ffmpeg printed something
	// this code cannot read — a changed key name, a filter that refused to
	// load — and every number below would be a zero dressed up as a
	// measurement.
	//
	// That distinction is the whole point. Reporting the pass as successful
	// with zeros in it tells the ranker "this video has no cuts and is pitch
	// black", which it will believe. Reporting failure tells it "not
	// measured", which it already knows how to handle. This exact bug shipped
	// once: three of the four patterns above matched nothing, and because the
	// pass still declared success on duration alone, every analysed video was
	// silently recorded as having zero cuts.
	if lumaN == 0 {
		return false
	}

	a.Brightness = clamp01(lumaSum / float64(lumaN) / 255)
	got := true

	if dur > 0 && cuts > 0 {
		a.CutsPerMinute = float64(cuts) / (dur / 60)
	}
	if sawLoudness {
		a.Loudness = loudness
	}
	if dur > 0 && audio {
		// What is left once the silence is taken out. Not speech as such —
		// music counts — but it separates a video with a soundtrack from one
		// recorded in a quiet room, which is most of the value.
		//
		// Only when there IS audio: with no track at all, silencedetect never
		// ran, so `silence` is 0 and this would compute a ratio of 1 — reading
		// a silent video as wall-to-wall sound, the exact opposite of the
		// truth. The no-audio case is set to 0 up where it is detected.
		a.SpeechRatio = clamp01((dur - silence) / dur)
	}
	// Motion is inferred from cut rate rather than measured directly: a real
	// motion-vector pass costs another decode for a number that mostly tracks
	// this one. Honest about being a proxy.
	if a.CutsPerMinute > 0 {
		a.MotionScore = clamp01(a.CutsPerMinute / 60)
	}
	return got
}

func clamp01(v float64) float64 {
	if v < 0 {
		return 0
	}
	if v > 1 {
		return 1
	}
	return v
}

// ════════════════════════════════════════════════════════════════════════════
// TEXT — words burned into the picture
// ════════════════════════════════════════════════════════════════════════════

// screenTextFrames is how many stills to read.
//
// Six is enough to catch a caption that is on screen for any meaningful part
// of the video, and few enough that the OCR cost stays under a second or two.
// Reading every frame would find the same words dozens of times.
const screenTextFrames = 6

// readScreenText pulls a few frames and runs OCR over them.
//
// The second return says whether the pass RAN. False means there was no
// tesseract to run, or extracting the frames failed — nothing was read, so
// nothing is known. True with an empty string means the opposite and is a real
// answer: we looked at six frames and there were no words on them.
func readScreenText(ctx context.Context, src string, dur float64) (string, bool) {
	if _, err := exec.LookPath("tesseract"); err != nil {
		return "", false
	}
	dir, err := os.MkdirTemp("", "ocr")
	if err != nil {
		return "", false
	}
	defer os.RemoveAll(dir)

	// Spread the frames across the video rather than taking the first N, so a
	// caption that only appears halfway through is still found.
	fps := "1"
	if dur > 0 {
		fps = fmt.Sprintf("%f", float64(screenTextFrames)/dur)
	}
	pattern := filepath.Join(dir, "f%03d.png")
	extract := exec.CommandContext(ctx, "ffmpeg",
		"-hide_banner", "-nostats", "-loglevel", "error",
		"-i", src,
		"-vf", "fps="+fps+",scale=640:-1",
		"-frames:v", strconv.Itoa(screenTextFrames),
		pattern,
	)
	if err := extract.Run(); err != nil {
		return "", false
	}

	frames, _ := filepath.Glob(filepath.Join(dir, "*.png"))
	sort.Strings(frames)

	seen := make(map[string]bool)
	var words []string
	for _, f := range frames {
		// tsv rather than plain text, for the confidence column. See
		// screenTextMinConfidence.
		out, err := exec.CommandContext(ctx, "tesseract", f, "stdout",
			"--psm", "11", "tsv").Output()
		if err != nil {
			continue
		}
		// The same caption sits on many frames, so dedupe by word. Without
		// this the text is one phrase repeated six times, which skews every
		// keyword count that reads it.
		for _, w := range confidentWords(string(out)) {
			key := strings.ToLower(w)
			if seen[key] {
				continue
			}
			seen[key] = true
			words = append(words, w)
		}
	}
	return strings.TrimSpace(strings.Join(words, " ")), true
}

// screenTextMinConfidence is how sure tesseract has to be about a word before
// we keep it.
//
// ════════════════════════════════════════════════════════════════════════════
// WHY THIS EXISTS
// ════════════════════════════════════════════════════════════════════════════
//
// Without it this pass produced almost pure noise. Real stored readings:
//
//	"—_ Vale 7 SCs Pda gy? Ww | C 0 — "A My ¥ ROMAN waN Sf" THE NEV Mima"
//	"Rees 43 "s malas — — < " \. f = & p« Bay SSS 4 "~"
//
// -psm 11 is "find as much text as anywhere in this image", which is right
// for a caption that could be anywhere on the frame and also means grain,
// hair and foliage get read as letters. Plain text output gives no way to
// tell those from a real caption: both arrive as words.
//
// tsv output carries a per-word confidence, and the two populations separate
// cleanly. Measured on rendered frames here:
//
//	a readable caption              88-96
//	noise read off a grainy frame   mostly under 50, highest seen 62
//
// 70 sits in that gap with room on both sides. 60 was the first choice and
// was wrong: it is level with the noise, so the worst of it still came
// through. It is a threshold on the OCR's own certainty, not on what the
// words say, so it cannot decide that some real caption is not interesting
// enough.
//
// The frames are still scaled to 640 first, which is worth keeping and was
// worth checking: the same caption at 1080 read as nothing at all above this
// threshold, and at 640 read perfectly. Downscaling averages the grain away
// faster than it costs letter detail.
const screenTextMinConfidence = 70

// confidentWords pulls the words tesseract was sure about out of its tsv.
//
// The format is one row per word: 12 tab-separated columns, confidence in
// column 11 and the text in column 12, with a header row and rows for page,
// block, paragraph and line structure that carry no text.
func confidentWords(tsv string) []string {
	var out []string
	for i, line := range strings.Split(tsv, "\n") {
		if i == 0 {
			continue // header
		}
		cols := strings.Split(line, "\t")
		if len(cols) < 12 {
			continue
		}
		word := strings.TrimSpace(cols[11])
		if len(word) < 3 {
			// Under three characters is not worth the risk: single letters
			// and pairs are what grain most often reads as, and no keyword
			// in the vocabulary is that short.
			continue
		}
		conf, err := strconv.ParseFloat(strings.TrimSpace(cols[10]), 64)
		if err != nil || conf < screenTextMinConfidence {
			continue
		}
		out = append(out, word)
	}
	return out
}

// ════════════════════════════════════════════════════════════════════════════
// SPEECH — what is said out loud
// ════════════════════════════════════════════════════════════════════════════

// whisperBinEnv and whisperModelEnv name the environment variables that turn
// the speech pass on.
//
// Off unless both are set, deliberately. Whisper is a few hundred megabytes
// of model and real CPU time per video; whether that is worth it depends on
// the host and the volume, and that is an operator's decision rather than a
// default.
const (
	whisperBinEnv   = "WHISPER_BIN"
	whisperModelEnv = "WHISPER_MODEL"
)

// speechThreads is how many cores one whisper run may use.
//
// Half the runner, because the workflow runs two workers at once — the same
// arithmetic as understandThreads. See the long note at the -t flag below for
// what going without this cost.
const speechThreads = 2

// transcribeSpeech extracts the audio and runs whisper over it.
//
// The second return says whether whisper actually ran. That distinction is the
// point of this function's signature: "we listened and nobody spoke" and "there
// was no whisper on this machine" both come back as an empty transcript, and
// only one of them means the feature is switched off. Without the flag the
// difference is invisible in the stored analysis and in the logs, which is
// exactly the question somebody asks after uploading a video.
//
// False for: no binary, no model, a binary that will not start, no audio
// track, a conversion failure, a timeout.
func transcribeSpeech(ctx context.Context, src string) (string, bool) {
	bin := strings.TrimSpace(os.Getenv(whisperBinEnv))
	model := strings.TrimSpace(os.Getenv(whisperModelEnv))
	if bin == "" || model == "" {
		return "", false
	}
	if _, err := exec.LookPath(bin); err != nil {
		log.Printf("analyze: %s is set to %q but that will not run: %v",
			whisperBinEnv, bin, err)
		return "", false
	}

	dir, err := os.MkdirTemp("", "stt")
	if err != nil {
		return "", false
	}
	defer os.RemoveAll(dir)

	// whisper.cpp wants 16kHz mono PCM. Converting here rather than handing it
	// the mp4 keeps the model's input predictable.
	wav := filepath.Join(dir, "audio.wav")
	conv := exec.CommandContext(ctx, "ffmpeg",
		"-hide_banner", "-nostats", "-loglevel", "error",
		"-i", src, "-vn", "-ac", "1", "-ar", "16000", "-f", "wav", wav)
	if err := conv.Run(); err != nil {
		// A silent video has no audio stream, so this is the ordinary path for
		// one — not worth a log line every time.
		return "", false
	}

	out, err := exec.CommandContext(ctx, bin,
		"-m", model,
		"-f", wav,
		"-nt", // no timestamps — we want the words, not a subtitle file
		"-np", // no progress spam on stderr
		// HALF the runner's cores, not all of them.
		//
		// This line is why transcripts were going missing. The workflow runs
		// TWO workers side by side on a four-core runner, and whisper.cpp
		// helps itself to four threads when nobody tells it otherwise. So
		// whenever both workers reached this pass together there were eight
		// threads fighting over four cores — while those same two workers were
		// also each encoding video.
		//
		// The result was not "a bit slower". It was whisper still running when
		// the analysis deadline expired, and being killed outright:
		//
		//   probeHasAudio: source.mp4 hasAudio=true
		//   ... 6m35s later ...
		//   analyze: speech pass failed: signal: killed
		//
		// That is challenge 260, a video WITH sound that had been transcribed
		// successfully before — 41 words — losing its transcript entirely
		// because it happened to be processed at the same moment as another.
		// It reads in the stored analysis exactly like a silent video, which
		// is how "79 of 114 videos have no words" turned out to be partly
		// untrue.
		//
		// Two threads each means the two workers add up to the machine instead
		// of overcommitting it.
		"-t", strconv.Itoa(speechThreads),
		// Work out the language from the audio rather than assuming one.
		//
		// This line was here before the model could honour it. The workflow
		// ran base.en, and an English-only model ignores -l entirely, so
		// every video was heard as though it were English whatever was
		// actually said. It showed: of 190 videos, 126 had no audio and were
		// correctly skipped, and every one of the remaining 64 came back with
		// something — but usually one, two or four words. Not people saying
		// little; an English listener catching fragments of speech it does
		// not know.
		//
		// The workflow now installs a multilingual model, so this does what
		// it says. See WHISPER_MODEL_NAME in .github/workflows/hls-worker.yml
		// — putting a ".en" model back there switches this off again without
		// touching a line of Go.
		"-l", "auto",
	).Output()
	if err != nil {
		log.Printf("analyze: speech pass failed: %v", err)
		return "", false
	}
	return strings.TrimSpace(string(out)), true
}

// ════════════════════════════════════════════════════════════════════════════
// TURNING ALL OF IT INTO TAGS
// ════════════════════════════════════════════════════════════════════════════
//
// The point of every pass above is to end up in the same place the creator's
// own tags end up, because that is what the ranker already knows how to use.
// A separate "machine signals" pathway would need every downstream stage
// taught about it; tags need nothing taught at all.

// analysisKeywords maps a word that might appear in a video's on-screen text
// or speech onto the tag it implies.
//
// Deliberately small and concrete. This is not trying to understand language
// — it is picking out the handful of words that reliably mean something about
// what a video is, in the vocabulary this app already uses.
var analysisKeywords = map[string][]string{
	// Food
	"recipe": {"food"}, "cook": {"food"}, "cooking": {"food"}, "kitchen": {"food"},
	"eat": {"food"}, "eating": {"food"}, "food": {"food"}, "dish": {"food"},
	"chef": {"food"}, "bake": {"food"}, "baking": {"food"}, "tasty": {"food"},
	"delicious": {"food"}, "breakfast": {"food"}, "lunch": {"food"},
	"dinner recipe": {"food"}, "snack": {"food"}, "curry": {"food"},
	"masala": {"food"}, "paratha": {"food"}, "biryani": {"food"},
	"khana": {"food"}, "खाना": {"food"}, "रेसिपी": {"food"}, "रसोई": {"food"},

	// Sports and fitness
	"workout": {"sports"}, "gym": {"sports"}, "reps": {"sports"},
	"fitness": {"sports"}, "exercise": {"sports"}, "training": {"sports"},
	"cricket": {"sports"}, "football": {"sports"}, "match": {"sports"},
	"pushups": {"sports"}, "squats": {"sports"}, "athlete": {"sports"},
	"क्रिकेट": {"sports"}, "जिम": {"sports"}, "खेल": {"sports"},

	// Dance
	"dance": {"dance"}, "dancing": {"dance"}, "choreo": {"dance"},
	"choreography": {"dance"}, "steps": {"dance"}, "moves": {"dance"},
	"नाच": {"dance"}, "डांस": {"dance"},

	// Music
	"sing": {"music"}, "singing": {"music"}, "song": {"music"},
	"lyrics": {"music"}, "beat": {"music"}, "rap": {"music"},
	"guitar": {"music"}, "piano": {"music"}, "melody": {"music"},
	"गाना": {"music"}, "संगीत": {"music"},

	// Education
	"tutorial": {"education"}, "how to": {"education"}, "step 1": {"education"},
	"learn": {"education"}, "learning": {"education"}, "teach": {"education"},
	"lesson": {"education"}, "explain": {"education"}, "guide": {"education"},
	"सीखो": {"education"}, "पढ़ाई": {"education"},

	// Motivation
	"motivation": {"motivation"}, "discipline": {"motivation"},
	"hustle": {"motivation"}, "grind": {"motivation"}, "success": {"motivation"},
	"mindset": {"motivation"}, "never give up": {"motivation"},
	"doubt you": {"motivation"}, "advice": {"motivation"}, "believe": {"motivation"},
	"मेहनत": {"motivation"}, "सफलता": {"motivation"},

	// Comedy and pranks
	"prank": {"prank"}, "reaction": {"comedy"}, "funny": {"comedy", "funny"},
	"joke": {"comedy"}, "comedy": {"comedy"}, "hilarious": {"comedy"},
	"मज़ाक": {"comedy"}, "हंसी": {"comedy"},

	// Tech
	"code": {"tech"}, "coding": {"tech"}, "app": {"tech"}, "phone": {"tech"},
	"laptop": {"tech"}, "gadget": {"tech"}, "software": {"tech"},
	"मोबाइल": {"tech"},

	// Gaming
	"gameplay": {"gaming"}, "gaming": {"gaming"}, "esports": {"gaming"},
	"noob": {"gaming"}, "respawn": {"gaming"},

	// Art
	"drawing": {"art"}, "painting": {"art"}, "sketch": {"art"},
	"artwork": {"art"}, "craft": {"art"},

	// Fashion
	"outfit": {"fashion"}, "makeup": {"fashion"}, "style": {"fashion"},
	"fashion": {"fashion"}, "hairstyle": {"fashion"},
	"कपड़े": {"fashion"},

	// Horror
	"scary": {"horror", "scary"}, "haunted": {"horror"}, "ghost": {"horror"},
	"creepy": {"horror"}, "horror": {"horror"},
	"भूत": {"horror"},

	// Story and lifestyle
	"story": {"story"}, "storytime": {"story"}, "day in my life": {"lifestyle"},
	"vlog": {"story"}, "routine": {"lifestyle"}, "morning routine": {"lifestyle"},
	"कहानी": {"story"},

	// News and commentary
	"news": {"news"}, "opinion": {"news"}, "breaking": {"news"},

	// ══════════════════════════════════════════════════════════════════════
	// FEELINGS AND THE ABSTRACT
	// ══════════════════════════════════════════════════════════════════════
	//
	// Everything above this line names a SUBJECT — food, a gym, a phone. The
	// list was entirely that shape, and it had no word at all for the
	// "emotional" category, which is why two videos with clean transcripts
	// came back tagged only "talking":
	//
	//	"If I do forgive you, you're just gonna break my heart all over
	//	 again and I can't handle that."
	//	"अर्जुन भविष्य में क्या होने वाला है उसका किसी को पता नहीं"
	//
	// Both obviously about something. Neither about a THING.
	//
	// Some of these also name a mood. emotionsFromTags reads any tag that is
	// an emotion label and feeds it to the ranker's mood matching, the way
	// "funny" and "scary" already did — so a word about heartbreak says both
	// what the video is about and how it feels, in one entry.
	//
	// Deliberately NOT here: "life", "truth", "promise", "know". They carry
	// no subject on their own, appear in every other sentence, and the whole
	// point of word matching is that a match should mean something.

	// Emotional — hurt, love, loss, repair
	"heartbreak": {"emotional", "sad"}, "heartbroken": {"emotional", "sad"},
	"break my heart": {"emotional", "sad"}, "broke my heart": {"emotional", "sad"},
	"forgive": {"emotional"}, "forgiveness": {"emotional"},
	"betrayed": {"emotional"}, "betrayal": {"emotional"},
	"lonely": {"emotional", "sad"}, "alone": {"emotional"},
	"crying": {"emotional", "sad"}, "tears": {"emotional", "sad"},
	"breakup": {"emotional", "sad"}, "break up": {"emotional", "sad"},
	"miss you": {"emotional", "nostalgic"}, "missing you": {"emotional", "nostalgic"},
	"love you": {"emotional", "romantic"}, "in love": {"emotional", "romantic"},
	"goodbye": {"emotional", "sad"}, "regret": {"emotional"},
	"grief": {"emotional", "sad"}, "healing": {"emotional"},
	"closure": {"emotional"}, "relationship": {"emotional"},
	"apology": {"emotional"}, "trust": {"emotional"},
	"दिल": {"emotional"}, "प्यार": {"emotional", "romantic"},
	"मोहब्बत": {"emotional", "romantic"}, "दर्द": {"emotional"},
	"आँसू": {"emotional", "sad"}, "माफ़": {"emotional"},
	"अकेला": {"emotional", "sad"}, "रिश्ता": {"emotional"},
	"जुदाई": {"emotional", "sad"},

	// Motivation — the abstract half, which the list had almost none of
	"future": {"motivation"}, "destiny": {"motivation"}, "fate": {"motivation"},
	"purpose": {"motivation"}, "journey": {"motivation"},
	"struggle": {"motivation"}, "failure": {"motivation"},
	"courage": {"motivation", "empowering"}, "patience": {"motivation"},
	"dream": {"motivation"}, "dreams": {"motivation"},
	"goals": {"motivation"}, "comeback": {"motivation"},
	"confidence": {"motivation", "empowering"}, "self love": {"motivation", "empowering"},
	"keep going": {"motivation", "inspiring"}, "growth": {"motivation"},
	"भविष्य": {"motivation"}, "ज़िंदगी": {"motivation"},
	"किस्मत": {"motivation"}, "हिम्मत": {"motivation", "empowering"},
	"कोशिश": {"motivation"}, "सपना": {"motivation"}, "सपने": {"motivation"},
	"संघर्ष": {"motivation"},

	// Story — a thing that happened, rather than a thing
	"experience": {"story"}, "true story": {"story"},
	"childhood": {"story", "nostalgic"}, "growing up": {"story"},
	"बचपन": {"story", "nostalgic"}, "यादें": {"story", "nostalgic"},
}

// tagsFromAnalysis turns everything measured into tags.
//
// Two sources: words found in the video, and the shape of the video itself.
// The shape tags are coarse on purpose — "fast cuts" and "talking" are real,
// checkable properties, where anything finer would be the analysis pretending
// to more certainty than ffmpeg can give it.
func tagsFromAnalysis(a videoAnalysis) []string {
	out := append(keywordTags(a), shapeTags(a)...)
	return dedupeSorted(out)
}

// keywordTags is the "what is this video about" half: the words the video
// said, matched against the list above.
//
// Split out from shapeTags because the two answer different questions from
// different evidence, and only this half has a replacement. What a video is
// ABOUT is a question about meaning, which a word list is a poor tool for —
// see understandContent. How a video is PUT TOGETHER is a measurement, and a
// measurement does not need a language model.
func keywordTags(a videoAnalysis) []string {
	text := wordSearchable(a.ScreenText + " " + a.Speech)
	if strings.TrimSpace(text) == "" {
		return nil
	}
	var out []string
	for kw, tags := range analysisKeywords {
		// wordSearchable pads both ends, so the keyword arrives already
		// bounded by spaces and must not be padded again.
		if strings.Contains(text, wordSearchable(kw)) {
			out = append(out, tags...)
		}
	}
	return out
}

// shapeTags is the "how is this video put together" half — cuts, talking,
// silence. Measured from the file itself, so it is true whatever the video is
// about and whoever is reading the words.
//
// Only emitted when the shape pass actually ran. Otherwise a video with no
// measurement would be tagged "still", which is a claim nobody made.
func shapeTags(a videoAnalysis) []string {
	if !hasPass(a, "shape") {
		return nil
	}
	var out []string
	switch {
	case a.CutsPerMinute >= 30:
		out = append(out, "fast cuts")
	case a.CutsPerMinute > 0 && a.CutsPerMinute < 6:
		out = append(out, "single take")
	}
	if a.SpeechRatio >= 0.8 {
		out = append(out, "talking")
	}
	if a.SpeechRatio > 0 && a.SpeechRatio < 0.2 {
		out = append(out, "silent")
	}
	return out
}

// dedupeSorted drops repeats and sorts, so the stored value does not churn
// between runs that found the same things in a different order.
//
// Only for tags whose order means nothing — keyword matches. Never for a
// model's answer: see dedupeStable.
func dedupeSorted(tags []string) []string {
	out := dedupeStable(tags)
	sort.Strings(out)
	return out
}

// dedupeStable drops repeats and KEEPS THE ORDER IT WAS GIVEN.
//
// ════════════════════════════════════════════════════════════════════════════
// WHY THE ORDER IS NOT DECORATION
// ════════════════════════════════════════════════════════════════════════════
//
// The model is told "pick one category, or at most two if the video genuinely
// spans both". When it names two, the first is its answer and the second is
// the qualifier. And the backend picks a video's category by walking the tag
// list and taking the FIRST one it recognises.
//
// So sorting the answer alphabetically silently replaces the model's choice
// with whichever of its choices happens to start with an earlier letter.
//
// This was not hypothetical. Video 108 is a dark-fantasy scene — "What brings
// you to the land of the gatekeepers? I'm searching for someone." Run against
// that transcript the model answers:
//
//	{"categories": ["story", "horror"], ...}
//
// Story first, which is also exactly what its creator chose. Sorted, that
// became ["horror", "intense", "scary", "story", "talking"], the backend took
// "horror", and the video was recorded as a case of the machine OVERRULING its
// creator — with its category boost damped for a disagreement that never
// happened.
//
// That second cost is the worse one. The whole point of recording both answers
// was to measure how often the machine and the creator really disagree, and a
// sort was manufacturing disagreements into that measurement.
func dedupeStable(tags []string) []string {
	seen := make(map[string]bool, len(tags))
	var out []string
	for _, t := range tags {
		if t != "" && !seen[t] {
			seen[t] = true
			out = append(out, t)
		}
	}
	return out
}

// wordSearchable lowercases text and reduces everything that is not a letter
// or a digit to a single space, padded at both ends.
//
// ════════════════════════════════════════════════════════════════════════════
// WHY MATCHING ON WORDS AND NOT ON SUBSTRINGS
// ════════════════════════════════════════════════════════════════════════════
//
// The match used to be strings.Contains over the raw text, and the keyword
// list was small enough to hide what that costs. It does not survive a bigger
// list, and it was already wrong: "eat" means food and "beat" means music, so
// every video with a beat in it was also tagged food. "rap" is inside "wrap",
// "art" is inside "start" and "heart", "news" is inside "newspaper" — the
// longer the list, the more of these there are, and each one is a video filed
// under a subject nobody mentioned.
//
// Padding both the text and the keyword with spaces makes " eat " miss
// "beat" and still match "eat" at either end of a sentence. Multi-word
// keywords like "how to" keep working, because the separator is a space on
// both sides.
//
// unicode.IsLetter rather than a-z, so Devanagari survives. Whisper writes
// Hindi in its own script and the keyword list has Hindi words in it; a
// filter that only knew ASCII would strip them out of the text and never
// match one.
//
// ════════════════════════════════════════════════════════════════════════════
// WHY THE VOWEL MARKS HAVE TO BE KEPT
// ════════════════════════════════════════════════════════════════════════════
//
// Letters alone are not enough, and leaving them alone was a real bug. In
// Hindi the vowels are written as small strokes attached to the consonants —
// the ा in नाच, the ि and ् in भविष्य. Go does not count those strokes as
// letters (they are "marks"), so a filter that keeps only letters throws
// every vowel away and leaves a row of bare consonants.
//
// That is not a smaller version of the word. It is a different word, and it
// collides with other words:
//
//	नाच          ("dance")            → न च
//	हनुमान चालीसा ("Hanuman Chalisa")  → हन म न च ल स   ← contains "न च"
//
// So a devotional video about reciting the Hanuman Chalisa was tagged
// "dance". Seen in production on challenge 260. Every Hindi keyword had the
// same problem: stripped to consonants they are short, and short things
// match by accident.
//
// Worse, it made the word-boundary padding above useless for Hindi. Its
// whole job is to stop one word matching inside another, and it cannot do
// that on fragments that were never words.
//
// Keeping the marks costs nothing anywhere else — English has none.
func wordSearchable(s string) string {
	var b strings.Builder
	b.WriteByte(' ')
	lastSpace := true
	for _, r := range strings.ToLower(s) {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || isCombiningMark(r) {
			b.WriteRune(r)
			lastSpace = false
			continue
		}
		if !lastSpace {
			b.WriteByte(' ')
			lastSpace = true
		}
	}
	if !lastSpace {
		b.WriteByte(' ')
	}
	return b.String()
}

// isCombiningMark reports whether r is a mark that attaches to the letter
// before it rather than standing on its own — the vowel strokes and nasal
// dots of Devanagari and of most other Indic scripts. Mn is a mark that
// takes no width of its own (ि, ं); Mc is one that does (ा, ी).
func isCombiningMark(r rune) bool {
	return unicode.In(r, unicode.Mn, unicode.Mc)
}

func hasPass(a videoAnalysis, name string) bool {
	for _, p := range a.Passes {
		if p == name {
			return true
		}
	}
	return false
}

// analysisJSON serializes an analysis for the completion callback, or returns
// nil when there is nothing worth sending.
func analysisJSON(a videoAnalysis) json.RawMessage {
	if len(a.Passes) == 0 {
		return nil
	}
	b, err := json.Marshal(a)
	if err != nil {
		return nil
	}
	return b
}
