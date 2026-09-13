package main

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// Cutting video that runs past the limit.
//
// The upload gate refuses anything over three minutes, so in the normal case
// nothing here fires. It is for the video the gate cannot cover: four clips
// already in the feed at 183 seconds, three seconds over a limit that did not
// exist when they were posted; a file whose length the gate could not measure
// and so let through; an app old enough not to report one.
//
// The limit is sent WITH THE JOB rather than configured on this side. The
// worker is a separate program, and the last time this limit lived in two
// places one of the two was missing entirely.

// clipSeconds reads a file's running time back with ffprobe.
func clipSeconds(t *testing.T, path string) float64 {
	t.Helper()
	out, err := exec.Command("ffprobe", "-v", "error",
		"-show_entries", "format=duration",
		"-of", "default=nw=1:nk=1", path).Output()
	if err != nil {
		t.Fatalf("ffprobe %s: %v", path, err)
	}
	d, err := strconv.ParseFloat(strings.TrimSpace(string(out)), 64)
	if err != nil {
		t.Fatalf("could not read a duration out of %q: %v", out, err)
	}
	return d
}

// makeLongSource builds a clip of a given length, above every bitrate ceiling
// so the encoder does not decide to leave it alone.
func makeLongSource(t *testing.T, dir, name string, seconds int) string {
	t.Helper()
	needFFmpeg(t)
	path := filepath.Join(dir, name)
	target := itoa(aboveEveryCeiling()/1000) + "k"
	out, err := exec.Command("ffmpeg", "-y",
		"-f", "lavfi", "-i",
		"color=c=black:s=640x360:r=30:d="+itoa(seconds)+",noise=alls=80:allf=t+u",
		"-f", "lavfi", "-i", "sine=frequency=440:duration="+itoa(seconds),
		"-c:v", "libx264", "-preset", "ultrafast", "-pix_fmt", "yuv420p",
		"-b:v", target, "-maxrate", target, "-bufsize", target,
		"-c:a", "aac", "-shortest",
		path,
	).CombinedOutput()
	if err != nil {
		t.Skipf("could not build a %ds source: %v: %s", seconds, err, lastLine(string(out)))
	}
	return path
}

func TestCutSeconds_UsesWhatTheBackendAsksFor(t *testing.T) {
	if got := cutSeconds(180); got != 180 {
		t.Errorf("cut = %ds, want the 180 the backend asked for", got)
	}
	if got := durationCutArgs(180); len(got) != 2 || got[0] != "-t" || got[1] != "180" {
		t.Errorf("args = %v, want [-t 180]", got)
	}
}

func TestCutSeconds_NeverCutsBelowTheProductLimit(t *testing.T) {
	// This is the bug that was found here: the worker carried a flat 90, set
	// when reels were capped at 60 seconds, and it outlived that cap. Every
	// video over 90 seconds was served as a full-length MP4 and a 90-second
	// HLS stream, and nothing said so.
	//
	// The runner bound exists to stop one job wedging the machine. A bound
	// BELOW what the product allows is not a safety net, it is a second
	// limit nobody wrote down.
	const productLimit = 180
	if runnerCapSeconds <= productLimit {
		t.Fatalf("the runner bound is %ds and the product allows %ds — "+
			"every longer video would be silently cut in half",
			runnerCapSeconds, productLimit)
	}
	if got := cutSeconds(productLimit); got != productLimit {
		t.Errorf("a %ds reel is cut to %ds", productLimit, got)
	}
}

func TestCutSeconds_StillBoundsOneJob(t *testing.T) {
	// A backend that asks for something absurd, or an older one that asks
	// for nothing, still gets bounded — the reason the bound exists does not
	// go away when the backend does.
	if got := cutSeconds(0); got != runnerCapSeconds {
		t.Errorf("no limit sent -> %ds, want the runner bound %ds", got, runnerCapSeconds)
	}
	if got := cutSeconds(-5); got != runnerCapSeconds {
		t.Errorf("a negative limit -> %ds, want the runner bound %ds", got, runnerCapSeconds)
	}
	if got := cutSeconds(60 * 60); got != runnerCapSeconds {
		t.Errorf("an hour -> %ds, want the runner bound %ds", got, runnerCapSeconds)
	}
}

// There must be exactly one -t. Two is how the old flat 90 won: it sat later
// in the argument list than anything else, so it quietly overrode it.
func TestOnlyOneDurationCapReachesFFmpeg(t *testing.T) {
	src, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	if n := strings.Count(string(src), `"-t",`); n != 0 {
		t.Errorf("main.go still sets -t directly %d time(s) — the last one "+
			"wins, and that is how a stale cap survived", n)
	}
}

func TestProgressive_CutsVideoPastTheLimit(t *testing.T) {
	needFFmpeg(t)
	dir := t.TempDir()
	// Six seconds against a four-second limit. Same shape as 183 seconds
	// against 180, without making the test suite wait three minutes.
	src := makeLongSource(t, dir, "long.mp4", 6)

	made := buildProgressiveMP4s(context.Background(), src, dir, 4)
	if len(made) == 0 {
		t.Fatal("nothing was encoded")
	}
	for label, path := range made {
		if d := clipSeconds(t, path); d > 4.3 {
			t.Errorf("%s came out %.2fs long against a 4s limit — the cut "+
				"never reached the encoder", label, d)
		}
	}
}

func TestProgressive_LeavesShorterVideoAlone(t *testing.T) {
	needFFmpeg(t)
	dir := t.TempDir()
	src := makeLongSource(t, dir, "short.mp4", 3)

	made := buildProgressiveMP4s(context.Background(), src, dir, 180)
	if len(made) == 0 {
		t.Fatal("nothing was encoded")
	}
	for label, path := range made {
		if d := clipSeconds(t, path); d < 2.5 {
			t.Errorf("%s came out %.2fs — a three-second clip under a "+
				"three-minute limit must not be touched", label, d)
		}
	}
}

func TestHLS_CutsVideoPastTheLimit(t *testing.T) {
	needFFmpeg(t)
	dir := t.TempDir()
	src := makeLongSource(t, dir, "long.mp4", 6)
	outDir := filepath.Join(dir, "hls")
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		t.Fatal(err)
	}

	if err := transcodeHLS(context.Background(), src, outDir, 4); err != nil {
		t.Skipf("the ladder would not build here: %v", err)
	}

	// The two ladders have to agree. A reel whose HLS stream is six seconds
	// and whose MP4 is four is one reel with two lengths.
	var total float64
	for _, m := range mustGlob(t, filepath.Join(outDir, "*", "index.m3u8")) {
		body, err := os.ReadFile(m)
		if err != nil {
			t.Fatal(err)
		}
		var secs float64
		for _, line := range strings.Split(string(body), "\n") {
			if !strings.HasPrefix(line, "#EXTINF:") {
				continue
			}
			v := strings.TrimSuffix(strings.TrimPrefix(line, "#EXTINF:"), ",")
			f, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
			if err == nil {
				secs += f
			}
		}
		if secs > 4.5 {
			t.Errorf("%s adds up to %.2fs against a 4s limit", filepath.Base(filepath.Dir(m)), secs)
		}
		total += secs
	}
	if total == 0 {
		t.Fatal("no segment durations were found, so nothing was checked")
	}
}

func mustGlob(t *testing.T, pattern string) []string {
	t.Helper()
	m, err := filepath.Glob(pattern)
	if err != nil {
		t.Fatal(err)
	}
	if len(m) == 0 {
		t.Skipf("no files matched %s", pattern)
	}
	return m
}

// ════════════════════════════════════════════════════════════════════════════
// THE LIMIT TRAVELS WITH THE JOB
// ════════════════════════════════════════════════════════════════════════════

// The worker must read the limit off the job, not carry its own copy. A copy
// over here is a copy that drifts, which is exactly how the challenge upload
// path ended up with no limit at all.
func TestTheLimitComesFromTheJob(t *testing.T) {
	var job pendingJob
	if err := json.Unmarshal([]byte(`{
		"challengeId":"27","sourceUrl":"https://x/y.mp4",
		"kind":"challenge","publicBaseUrl":"https://cdn","maxSeconds":180
	}`), &job); err != nil {
		t.Fatal(err)
	}
	if job.MaxSeconds != 180 {
		t.Errorf("maxSeconds = %d, want 180 — the wire name does not match", job.MaxSeconds)
	}

	// A backend too old to send it leaves zero, which cuts nothing.
	var old pendingJob
	if err := json.Unmarshal([]byte(`{"challengeId":"1","sourceUrl":"https://x/y.mp4"}`), &old); err != nil {
		t.Fatal(err)
	}
	if old.MaxSeconds != 0 {
		t.Errorf("an older backend must leave the limit unset, got %d", old.MaxSeconds)
	}
	if got := cutSeconds(old.MaxSeconds); got != runnerCapSeconds {
		t.Errorf("an older backend leaves the limit unset, which must fall "+
			"back to the runner bound %ds, got %ds", runnerCapSeconds, got)
	}
}

// Both encoders have to be handed it. One of the two ladders honouring the
// limit is the same reel at two different lengths.
func TestBothEncodersAreGivenTheLimit(t *testing.T) {
	src, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"transcodeHLS(ctx, srcPath, outDir, job.MaxSeconds)",
		"buildProgressiveMP4s(ctx, srcPath, outDir, job.MaxSeconds)",
	} {
		if !strings.Contains(string(src), want) {
			t.Errorf("missing %q — that encoder runs without a limit", want)
		}
	}
}
