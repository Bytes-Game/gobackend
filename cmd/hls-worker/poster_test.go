package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// The poster is the only thing a viewer sees for the first moment of every
// reel, so these run ffmpeg for real rather than checking the arguments. An
// argument list that looks right and produces a black frame, a sideways
// picture or an empty file is the failure that matters, and none of those are
// visible in the command line.

// makeTestVideo writes a short clip: one second of red, then two of blue.
// The colour change is what lets a test say WHICH frame was taken.
func makeTestVideo(t *testing.T, dir string) string {
	t.Helper()
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		t.Skip("ffmpeg not installed")
	}
	path := filepath.Join(dir, "src.mp4")
	err := exec.Command("ffmpeg", "-hide_banner", "-loglevel", "error",
		"-f", "lavfi", "-i", "color=c=red:s=640x360:d=1",
		"-f", "lavfi", "-i", "color=c=blue:s=640x360:d=2",
		"-filter_complex", "[0:v][1:v]concat=n=2:v=1:a=0",
		"-c:v", "libx264", "-pix_fmt", "yuv420p", "-y", path,
	).Run()
	if err != nil {
		t.Skipf("could not build a test video: %v", err)
	}
	return path
}

func TestMakePoster_WritesARealPicture(t *testing.T) {
	dir := t.TempDir()
	src := makeTestVideo(t, dir)

	out, err := makePoster(context.Background(), src, dir)
	if err != nil {
		t.Fatalf("makePoster: %v", err)
	}
	st, err := os.Stat(out)
	if err != nil {
		t.Fatalf("no file at %s: %v", out, err)
	}
	if st.Size() == 0 {
		t.Fatal("the poster is an empty file, which the app would wait on " +
			"forever rather than paint")
	}
	// It has to be small enough to arrive BEFORE the video it stands in for,
	// or it is competing with the thing it exists to cover.
	const budget = 300 * 1024
	if st.Size() > budget {
		t.Errorf("the poster is %d bytes, over the %d budget — at that size "+
			"it costs about as much as the video's opening slice, which is "+
			"what it is meant to save", st.Size(), budget)
	}
}

func TestMakePoster_SkipsTheOpeningFrame(t *testing.T) {
	// The reason posterAtSeconds is not zero: videos fade in, and a poster of
	// black is no better than no poster. The test clip is red for its first
	// second and blue afterwards, so the colour says which frame was taken.
	dir := t.TempDir()
	src := makeTestVideo(t, dir)

	out, err := makePoster(context.Background(), src, dir)
	if err != nil {
		t.Fatalf("makePoster: %v", err)
	}
	rgb := averageColour(t, out)
	if rgb[2] <= rgb[0] {
		t.Errorf("the poster is from the first second of the video "+
			"(avg rgb %v looks red, not blue).\n\nFrame zero is a fade from "+
			"black on a great many real videos.", rgb)
	}
}

func TestMakePoster_ARealPictureIsNotSideways(t *testing.T) {
	// A poster that ignores rotation is shown on its side under a video that
	// is not, which is more jarring than no poster at all.
	dir := t.TempDir()
	src := makeTestVideo(t, dir)
	out, err := makePoster(context.Background(), src, dir)
	if err != nil {
		t.Fatalf("makePoster: %v", err)
	}
	w, h := imageSize(t, out)
	if w <= h {
		t.Errorf("a 640x360 landscape source produced a %dx%d poster", w, h)
	}
	if w > posterMaxWidth {
		t.Errorf("the poster is %d wide, over the %d cap", w, posterMaxWidth)
	}
}

func TestMakePoster_SaysSoRatherThanWritingNothing(t *testing.T) {
	// A missing file, or a video shorter than the seek point, must come back
	// as an error the caller can log — not as a path to a file that is not
	// there, and not as a zero-byte poster the app would wait on.
	dir := t.TempDir()
	if _, err := makePoster(context.Background(), filepath.Join(dir, "nope.mp4"), dir); err == nil {
		t.Error("a source that does not exist reported success")
	}
	if _, err := os.Stat(filepath.Join(dir, "poster.jpg")); err == nil {
		t.Error("a failed grab left a file behind for the app to try to paint")
	}
}

func TestPoster_IsGrabbedPastTheOpeningFrameButNotIntoAnotherShot(t *testing.T) {
	if posterAtSeconds <= 0 {
		t.Errorf("the poster is taken at %.2fs, which is frame zero — on any "+
			"video that fades in, the still is black", posterAtSeconds)
	}
	if posterAtSeconds > 3 {
		t.Errorf("the poster is taken at %.2fs, far enough in to show "+
			"something other than the video's opening shot", posterAtSeconds)
	}
}

// averageColour reads the mean R, G and B of an image, via ffmpeg so the test
// needs no image-decoding dependency.
func averageColour(t *testing.T, path string) [3]float64 {
	t.Helper()
	raw := filepath.Join(t.TempDir(), "one.rgb")
	if err := exec.Command("ffmpeg", "-hide_banner", "-loglevel", "error",
		"-i", path, "-vf", "scale=1:1", "-f", "rawvideo",
		"-pix_fmt", "rgb24", "-y", raw).Run(); err != nil {
		t.Fatalf("read colour: %v", err)
	}
	b, err := os.ReadFile(raw)
	if err != nil || len(b) < 3 {
		t.Fatalf("read colour: %v (%d bytes)", err, len(b))
	}
	return [3]float64{float64(b[0]), float64(b[1]), float64(b[2])}
}

// imageSize reads an image's pixel dimensions with ffprobe.
func imageSize(t *testing.T, path string) (int, int) {
	t.Helper()
	if _, err := exec.LookPath("ffprobe"); err != nil {
		t.Skip("ffprobe not installed")
	}
	out, err := exec.Command("ffprobe", "-v", "error",
		"-select_streams", "v:0", "-show_entries", "stream=width,height",
		"-of", "csv=p=0:s=x", path).Output()
	if err != nil {
		t.Fatalf("ffprobe: %v", err)
	}
	var w, h int
	if _, err := fmt.Sscanf(strings.TrimSpace(string(out)), "%dx%d", &w, &h); err != nil {
		t.Fatalf("parse %q: %v", out, err)
	}
	return w, h
}
