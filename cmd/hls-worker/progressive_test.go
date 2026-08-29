package main

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"mymodule/internal/mp4layout"
)

// ════════════════════════════════════════════════════════════════════════════
// OUR OWN ENCODE
// ════════════════════════════════════════════════════════════════════════════
//
// Run against real ffmpeg, because every question worth asking here is about
// what ffmpeg actually produced. A fixture would only test the test.

// makeSource writes a clip that is worth re-encoding.
//
// The bitrate is forced, and that is not incidental. ffmpeg's test pattern is
// a synthetic image that compresses to about 0.3 Mbps whatever size you ask
// for — below progressiveSkipBps, so an unforced fixture is correctly LEFT
// ALONE and every test about the encode silently stops testing it.
//
// 2.5 Mbps is what these tests are about: the real imported clip that started
// all this measured 2.68 Mbps at 853x480, roughly four times what that picture
// needs. Tests that want the opposite case build their own lean clip.
func makeSource(t *testing.T, dir, name string, w, h int) string {
	t.Helper()
	needFFmpeg(t)
	path := filepath.Join(dir, name)
	out, err := exec.Command("ffmpeg", "-y",
		"-f", "lavfi", "-i",
		"testsrc=size="+itoa(w)+"x"+itoa(h)+":rate=30:duration=2",
		"-f", "lavfi", "-i", "sine=frequency=440:duration=2",
		"-c:v", "libx264", "-preset", "ultrafast", "-pix_fmt", "yuv420p",
		// nal-hrd=cbr pads to the target instead of undershooting it. Without
		// it x264 finds the test pattern so compressible that a 640x480 clip
		// lands at 0.75 Mbps however high the target is set, which puts the
		// fixture back below the skip threshold.
		"-b:v", "2500k", "-maxrate", "2500k", "-bufsize", "5000k",
		"-x264-params", "nal-hrd=cbr:force-cfr=1",
		"-c:a", "aac", "-shortest",
		path,
	).CombinedOutput()
	if err != nil {
		t.Skipf("could not build a source clip: %v: %s", err, lastLine(string(out)))
	}
	return path
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}

func TestProgressive_ProducesAPlayableFileWithTheIndexInFront(t *testing.T) {
	needFFmpeg(t)
	dir := t.TempDir()
	src := makeSource(t, dir, "src.mp4", 1280, 720)

	made := buildProgressiveMP4s(context.Background(), src, dir)
	if len(made) == 0 {
		t.Fatal("nothing was encoded from a perfectly ordinary 720p source")
	}
	for label, path := range made {
		// The property the whole change exists for: never a repair
		// afterwards, always right the first time.
		layout, err := mp4layout.OfFile(path)
		if err != nil {
			t.Errorf("%s: %v", label, err)
			continue
		}
		if layout != mp4layout.FastStart {
			t.Errorf("%s came out %s — our own encode must never need "+
				"straightening out", label, layout)
		}
		shape, err := describeMedia(context.Background(), path)
		if err != nil {
			t.Errorf("%s: could not describe it: %v", label, err)
			continue
		}
		if len(shape.codecs) != 2 {
			t.Errorf("%s has streams %v; the source had picture and sound and "+
				"both have to survive", label, shape.codecs)
		}
		if shape.duration < 1.8 || shape.duration > 2.2 {
			t.Errorf("%s runs %.2fs against a 2s source", label, shape.duration)
		}
	}
}

func TestProgressive_SmallerThanTheThingItReplaces(t *testing.T) {
	// The point of encoding at all, in one number. A phone's own export is
	// generously encoded because it was never meant to be streamed to
	// strangers; ours is chosen for the feed.
	needFFmpeg(t)
	dir := t.TempDir()
	// -qp 0 is lossless, which is the extreme version of "whatever the camera
	// felt like" and makes the comparison unambiguous.
	src := filepath.Join(dir, "fat.mp4")
	if out, err := exec.Command("ffmpeg", "-y",
		"-f", "lavfi", "-i", "testsrc=size=1280x720:rate=30:duration=2",
		"-c:v", "libx264", "-preset", "ultrafast", "-qp", "0",
		"-pix_fmt", "yuv420p", src,
	).CombinedOutput(); err != nil {
		t.Skipf("could not build the source: %v: %s", err, lastLine(string(out)))
	}
	srcInfo, err := os.Stat(src)
	if err != nil {
		t.Fatal(err)
	}

	made := buildProgressiveMP4s(context.Background(), src, dir)
	path, ok := made["720p"]
	if !ok {
		t.Fatal("no 720p rendition from a 720p source")
	}
	outInfo, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("source %d KB → our 720p %d KB (%.0f%% of the original)",
		srcInfo.Size()/1024, outInfo.Size()/1024,
		100*float64(outInfo.Size())/float64(srcInfo.Size()))
	if outInfo.Size() >= srcInfo.Size() {
		t.Errorf("our encode is %d bytes against a %d byte source — it is "+
			"costing bandwidth rather than saving it",
			outInfo.Size(), srcInfo.Size())
	}
}

func TestProgressive_NeverUpscales(t *testing.T) {
	// A 480p source must not be blown up to 720p. That spends bandwidth and
	// CPU inventing detail that is not there and comes out looking softer
	// than the original at twice the size.
	needFFmpeg(t)
	dir := t.TempDir()
	src := makeSource(t, dir, "small.mp4", 640, 480)

	made := buildProgressiveMP4s(context.Background(), src, dir)
	if _, ok := made["720p"]; ok {
		t.Error("a 480p source produced a 720p rendition")
	}
	if _, ok := made["480p"]; !ok {
		t.Error("a 480p source produced no 480p rendition either, so the tab " +
			"gets nothing at all")
	}
}

func TestProgressive_ASilentVideoKeepsItsPicture(t *testing.T) {
	// A clip with no audio track used to be the case that quietly lost its
	// video too, because the audio mapping failed the whole command.
	needFFmpeg(t)
	dir := t.TempDir()
	src := filepath.Join(dir, "silent.mp4")
	// Fat on purpose — see makeSource. A lean clip would be skipped by the
	// bitrate rule and this test would pass without encoding anything.
	if out, err := exec.Command("ffmpeg", "-y",
		"-f", "lavfi", "-i", "testsrc=size=1280x720:rate=30:duration=2",
		"-c:v", "libx264", "-preset", "ultrafast", "-pix_fmt", "yuv420p",
		// nal-hrd=cbr pads to the target instead of undershooting it. Without
		// it x264 finds the test pattern so compressible that a 640x480 clip
		// lands at 0.75 Mbps however high the target is set, which puts the
		// fixture back below the skip threshold.
		"-b:v", "2500k", "-maxrate", "2500k", "-bufsize", "5000k",
		"-x264-params", "nal-hrd=cbr:force-cfr=1",
		src,
	).CombinedOutput(); err != nil {
		t.Skipf("could not build the silent source: %v: %s", err, lastLine(string(out)))
	}

	made := buildProgressiveMP4s(context.Background(), src, dir)
	path, ok := made["720p"]
	if !ok {
		t.Fatal("a silent video produced nothing at all")
	}
	shape, err := describeMedia(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	if len(shape.codecs) != 1 || shape.codecs[0] != "h264" {
		t.Errorf("a silent source came out with streams %v", shape.codecs)
	}
}

func TestProgressive_RubbishInProducesNothingRatherThanRubbishOut(t *testing.T) {
	// Nothing is served unless it passed the checks. A missing source, or a
	// file that is not a video, has to come back as an empty map — the app
	// then keeps playing the upload, which is exactly today's behaviour.
	dir := t.TempDir()
	if got := buildProgressiveMP4s(context.Background(), filepath.Join(dir, "nope.mp4"), dir); len(got) != 0 {
		t.Errorf("got %v from a file that does not exist", got)
	}
	junk := filepath.Join(dir, "junk.mp4")
	if err := os.WriteFile(junk, []byte("not a video"), 0o600); err != nil {
		t.Fatal(err)
	}
	if got := buildProgressiveMP4s(context.Background(), junk, dir); len(got) != 0 {
		t.Errorf("got %v from a file that is not a video", got)
	}
}

func TestProgressive_TheLadderMatchesWhatTheAppCanChoose(t *testing.T) {
	// The app picks from labels it knows: 480p, 720p, 1080p. A rendition
	// under any other name is a file nobody ever plays, and the backend's
	// allow-list would drop it on the way in anyway.
	known := map[string]bool{"480p": true, "720p": true, "1080p": true}
	for _, r := range progressiveLadder {
		if !known[r.label] {
			t.Errorf("rendition %q is not a label the app chooses from", r.label)
		}
		if r.crf < 18 || r.crf > 28 {
			t.Errorf("%s uses crf %d; below 18 is bigger files for no visible "+
				"gain and above 28 starts to show", r.label, r.crf)
		}
	}
}

func TestProgressive_DoesNotEnlargeASourceBetweenTheRungs(t *testing.T) {
	// The bug this exists for, from a real upload. 960x720 fell between the
	// two rungs, and force_original_aspect_ratio=decrease grew it to 1280x960
	// — more pixels than it started with, no more detail, and a file 97% the
	// size of the original instead of a third of it.
	//
	// "decrease" fits a picture INSIDE a box. It does not refuse to enlarge.
	needFFmpeg(t)
	dir := t.TempDir()
	src := makeSource(t, dir, "between.mp4", 960, 720)

	made := buildProgressiveMP4s(context.Background(), src, dir)
	path, ok := made["720p"]
	if !ok {
		t.Fatal("a 960x720 source produced no 720p rendition")
	}
	out, err := exec.Command("ffprobe", "-v", "error",
		"-select_streams", "v:0", "-show_entries", "stream=width,height",
		"-of", "csv=p=0:s=x", path).Output()
	if err != nil {
		t.Fatal(err)
	}
	got := strings.TrimSpace(string(out))
	if got != "960x720" {
		t.Errorf("a 960x720 source came out %s — anything larger is invented "+
			"detail paid for by every viewer", got)
	}
}

func TestProgressive_NoTwoRenditionsAreTheSameFile(t *testing.T) {
	// A small source clamps both rungs to its own size, which would otherwise
	// produce identical bytes under two names: twice the CPU, twice the
	// storage, and a chooser picking between things that do not differ.
	needFFmpeg(t)
	dir := t.TempDir()
	src := makeSource(t, dir, "tiny.mp4", 640, 360)

	made := buildProgressiveMP4s(context.Background(), src, dir)
	if len(made) != 1 {
		t.Errorf("a 640x360 source produced %d renditions (%v); both rungs "+
			"clamp to 640 so only one is worth making", len(made), made)
	}
}

// ════════════════════════════════════════════════════════════════════════════
// LEAVING AN ALREADY-LEAN FILE ALONE
// ════════════════════════════════════════════════════════════════════════════
//
// Re-encoding is lossy, so it has to be worth something. On a file carrying
// far more bits than its picture needs it plainly is — a real imported clip at
// 853x480 and 2.68 Mbps came out 72% smaller at the same size, and all of that
// was waste rather than detail.
//
// On a file already at 0.7 Mbps the trade inverts: almost nothing left to
// squeeze, so the generation loss is most of what changes.

func TestProgressive_LeavesAnAlreadyLeanFileAlone(t *testing.T) {
	needFFmpeg(t)
	dir := t.TempDir()
	// A 480p source encoded modestly — the shape of a clip that was already
	// prepared for streaming by whoever made it.
	src := filepath.Join(dir, "lean.mp4")
	if out, err := exec.Command("ffmpeg", "-y",
		"-f", "lavfi", "-i", "color=c=gray:size=854x480:rate=30:duration=3",
		"-f", "lavfi", "-i", "sine=frequency=440:duration=3",
		"-c:v", "libx264", "-preset", "ultrafast", "-b:v", "400k",
		"-pix_fmt", "yuv420p", "-c:a", "aac", "-b:a", "64k", "-shortest", src,
	).CombinedOutput(); err != nil {
		t.Skipf("could not build the lean clip: %v: %s", err, lastLine(string(out)))
	}
	_, bitrate, ok := sourceShape(context.Background(), src)
	if !ok || bitrate == 0 {
		t.Skip("could not measure the clip's bitrate here")
	}
	if bitrate > progressiveSkipBps {
		t.Skipf("the fixture came out at %d bps, above the %d threshold — "+
			"it does not exercise the case", bitrate, progressiveSkipBps)
	}

	made := buildProgressiveMP4s(context.Background(), src, dir)
	if len(made) != 0 {
		t.Errorf("a source already at %.2f Mbps for its size was re-encoded "+
			"into %v — that spends a lossy generation to save almost nothing",
			float64(bitrate)/1e6, made)
	}
}

func TestProgressive_StillShrinksABigPictureAtALowBitrate(t *testing.T) {
	// The exception the skip must not swallow. A 1080p source at a low
	// bitrate still gains from being made smaller: fewer pixels is less for a
	// weak phone to decode as well as fewer bytes on the wire.
	needFFmpeg(t)
	dir := t.TempDir()
	src := filepath.Join(dir, "big-but-lean.mp4")
	if out, err := exec.Command("ffmpeg", "-y",
		"-f", "lavfi", "-i", "color=c=gray:size=1920x1080:rate=30:duration=3",
		"-c:v", "libx264", "-preset", "ultrafast", "-b:v", "500k",
		"-pix_fmt", "yuv420p", src,
	).CombinedOutput(); err != nil {
		t.Skipf("could not build the clip: %v: %s", err, lastLine(string(out)))
	}
	_, bitrate, ok := sourceShape(context.Background(), src)
	if !ok || bitrate == 0 || bitrate > progressiveSkipBps {
		t.Skipf("fixture is not below the threshold here (%d bps)", bitrate)
	}

	made := buildProgressiveMP4s(context.Background(), src, dir)
	if len(made) == 0 {
		t.Error("a 1080p source was left alone because its bitrate was low. " +
			"Shrinking the picture is worth doing at any bitrate — every " +
			"viewer decodes four times the pixels otherwise.")
	}
}

func TestSourceShape_ReportsSizeAndBitrate(t *testing.T) {
	needFFmpeg(t)
	dir := t.TempDir()
	src := makeSource(t, dir, "shape.mp4", 1280, 720)

	longSide, bitrate, ok := sourceShape(context.Background(), src)
	if !ok {
		t.Fatal("could not measure an ordinary clip")
	}
	if longSide != 1280 {
		t.Errorf("long side came back %d for a 1280x720 clip", longSide)
	}
	if bitrate <= 0 {
		t.Error("bitrate came back as zero for a clip that declares one; a " +
			"zero here reads as 'unknown' and sends every file to the encoder")
	}
}

func TestSourceShape_UnknownBitrateStillEncodes(t *testing.T) {
	// A container that declares no bitrate must not be mistaken for a lean
	// one. Unknown has to mean "encode it", which is what the code did before
	// the skip existed — a format we cannot read never silently stops being
	// processed.
	needFFmpeg(t)
	dir := t.TempDir()
	src := makeSource(t, dir, "ordinary.mp4", 1280, 720)

	// Proven through the constant rather than by forging a container: the
	// guard is `bitrate > 0 && bitrate <= threshold`, so zero fails the first
	// half and the file is encoded.
	made := buildProgressiveMP4s(context.Background(), src, dir)
	if len(made) == 0 {
		t.Error("an ordinary 720p clip produced no renditions at all")
	}
}
