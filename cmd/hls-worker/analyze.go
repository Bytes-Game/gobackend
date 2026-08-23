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
// The Dockerfile installs tesseract. Whisper is left to the operator: it is a
// few hundred megabytes of model and a real CPU cost per video, and it is
// worth turning on deliberately rather than by default.

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
	if txt := readScreenText(ctx, src, dur); txt != "" {
		a.ScreenText = txt
		a.Passes = append(a.Passes, "text")
	}
	if sp := transcribeSpeech(ctx, src); sp != "" {
		a.Speech = sp
		a.Passes = append(a.Passes, "speech")
	}

	a.AutoTags = tagsFromAnalysis(a)
	return a
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

var (
	reSceneScore = regexp.MustCompile(`lavfi\.scene_score=([0-9.]+)`)
	reLoudness   = regexp.MustCompile(`"input_i"\s*:\s*"(-?[0-9.]+)"`)
	reSilenceDur = regexp.MustCompile(`silence_duration:\s*([0-9.]+)`)
	reYAVG       = regexp.MustCompile(`YAVG:([0-9.]+)`)
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
	cmd := exec.CommandContext(ctx, "ffmpeg",
		"-hide_banner", "-nostats",
		"-i", src,
		"-filter_complex",
		"[0:v]scdet=threshold=10,signalstats,metadata=print[v];"+
			"[0:a]ebur128=metadata=1,silencedetect=n=-30dB:d=0.5[a]",
		"-map", "[v]", "-map", "[a]",
		"-f", "null", "-",
	)
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
		if m := reSceneScore.FindStringSubmatch(line); m != nil {
			if v, e := strconv.ParseFloat(m[1], 64); e == nil && v > 0 {
				cuts++
			}
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

	got := false
	if dur > 0 && cuts > 0 {
		a.CutsPerMinute = float64(cuts) / (dur / 60)
		got = true
	}
	if lumaN > 0 {
		a.Brightness = clamp01(lumaSum / float64(lumaN) / 255)
		got = true
	}
	if sawLoudness {
		a.Loudness = loudness
		got = true
	}
	if dur > 0 {
		// What is left once the silence is taken out. Not speech as such —
		// music counts — but it separates a video with a soundtrack from one
		// recorded in a quiet room, which is most of the value.
		a.SpeechRatio = clamp01((dur - silence) / dur)
		got = true
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
// Returns "" when tesseract is not installed, which is the normal case on a
// worker nobody has configured for it. The caller treats that as "no text
// found" and moves on.
func readScreenText(ctx context.Context, src string, dur float64) string {
	if _, err := exec.LookPath("tesseract"); err != nil {
		return ""
	}
	dir, err := os.MkdirTemp("", "ocr")
	if err != nil {
		return ""
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
		return ""
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
	return strings.TrimSpace(strings.Join(words, " "))
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
// Returns "" whenever anything is missing or fails — no binary, no model, no
// audio track, a timeout. Same rule as every other pass.
func transcribeSpeech(ctx context.Context, src string) string {
	bin := strings.TrimSpace(os.Getenv(whisperBinEnv))
	model := strings.TrimSpace(os.Getenv(whisperModelEnv))
	if bin == "" || model == "" {
		return ""
	}
	if _, err := exec.LookPath(bin); err != nil {
		return ""
	}

	dir, err := os.MkdirTemp("", "stt")
	if err != nil {
		return ""
	}
	defer os.RemoveAll(dir)

	// whisper.cpp wants 16kHz mono PCM. Converting here rather than handing it
	// the mp4 keeps the model's input predictable.
	wav := filepath.Join(dir, "audio.wav")
	conv := exec.CommandContext(ctx, "ffmpeg",
		"-hide_banner", "-nostats", "-loglevel", "error",
		"-i", src, "-vn", "-ac", "1", "-ar", "16000", "-f", "wav", wav)
	if err := conv.Run(); err != nil {
		return ""
	}

	out, err := exec.CommandContext(ctx, bin,
		"-m", model,
		"-f", wav,
		"-nt",    // no timestamps — we want the words, not a subtitle file
		"-np",    // no progress spam on stderr
		"-l", "auto",
	).Output()
	if err != nil {
		log.Printf("analyze: speech pass failed: %v", err)
		return ""
	}
	return strings.TrimSpace(string(out))
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
