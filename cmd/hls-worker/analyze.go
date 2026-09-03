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
const analyzeTimeout = 3 * time.Minute

// analyzeVideo inspects a local file and returns everything it could work
// out. It never returns an error: a failed pass is a missing field, not a
// failed job.
func analyzeVideo(ctx context.Context, src string) videoAnalysis {
	ctx, cancel := context.WithTimeout(ctx, analyzeTimeout)
	defer cancel()

	var a videoAnalysis
	dur := probeDuration(ctx, src)

	if analyzeShape(ctx, src, dur, &a) {
		a.Passes = append(a.Passes, "shape")
	}
	// A pass is recorded when it RAN, not when it found something. Those are
	// different facts and the difference is the whole reason Passes exists:
	// a video with nobody talking and a worker with no whisper installed both
	// produce an empty transcript, and only one of them is a problem.
	if txt, ran := readScreenText(ctx, src, dur); ran {
		a.ScreenText = txt
		a.Passes = append(a.Passes, "text")
	}
	if sp, ran := transcribeSpeech(ctx, src); ran {
		a.Speech = sp
		a.Passes = append(a.Passes, "speech")
	}

	a.AutoTags = tagsFromAnalysis(a)
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
		out, err := exec.CommandContext(ctx, "tesseract", f, "stdout", "--psm", "11").Output()
		if err != nil {
			continue
		}
		// The same caption sits on many frames, so dedupe by line. Without
		// this the text is one phrase repeated six times, which skews every
		// keyword count that reads it.
		for _, line := range strings.Split(string(out), "\n") {
			line = strings.TrimSpace(line)
			if len(line) < 3 || seen[strings.ToLower(line)] {
				continue
			}
			seen[strings.ToLower(line)] = true
			words = append(words, line)
		}
	}
	return strings.TrimSpace(strings.Join(words, " ")), true
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
	"recipe": {"food"}, "cook": {"food"}, "kitchen": {"food"}, "eat": {"food"},
	"workout": {"sports"}, "gym": {"sports"}, "reps": {"sports"}, "goal": {"sports"},
	"dance": {"dance"}, "choreo": {"dance"}, "beat": {"music"},
	"sing": {"music"}, "song": {"music"}, "lyrics": {"music"},
	"tutorial": {"education"}, "how to": {"education"}, "step 1": {"education"},
	"learn": {"education"}, "tip": {"education"},
	"prank": {"prank"}, "reaction": {"comedy"}, "funny": {"comedy", "funny"},
	"code": {"tech"}, "app": {"tech"}, "phone": {"tech"},
	"outfit": {"fashion"}, "makeup": {"fashion"},
	"scary": {"horror", "scary"}, "haunted": {"horror"},
	"story": {"story"}, "day in my life": {"story"},
}

// tagsFromAnalysis turns everything measured into tags.
//
// Two sources: words found in the video, and the shape of the video itself.
// The shape tags are coarse on purpose — "fast cuts" and "talking" are real,
// checkable properties, where anything finer would be the analysis pretending
// to more certainty than ffmpeg can give it.
func tagsFromAnalysis(a videoAnalysis) []string {
	seen := map[string]bool{}
	var out []string
	add := func(t string) {
		if t != "" && !seen[t] {
			seen[t] = true
			out = append(out, t)
		}
	}

	text := strings.ToLower(a.ScreenText + " " + a.Speech)
	if strings.TrimSpace(text) != "" {
		for kw, tags := range analysisKeywords {
			if strings.Contains(text, kw) {
				for _, t := range tags {
					add(t)
				}
			}
		}
	}

	// Shape tags. Only emitted when the shape pass actually ran — otherwise a
	// video with no measurement would be tagged "still", which is a claim
	// nobody made.
	if hasPass(a, "shape") {
		switch {
		case a.CutsPerMinute >= 30:
			add("fast cuts")
		case a.CutsPerMinute > 0 && a.CutsPerMinute < 6:
			add("single take")
		}
		if a.SpeechRatio >= 0.8 {
			add("talking")
		}
		if a.SpeechRatio > 0 && a.SpeechRatio < 0.2 {
			add("silent")
		}
	}

	sort.Strings(out) // stable order so the stored value does not churn
	return out
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
