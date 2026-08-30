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

// aboveEveryCeiling is the bitrate makeSource aims for: half again as much as
// the most generous rung on the ladder.
//
// Derived rather than written down, because a hard-coded number here has now
// disarmed these tests twice. The fixture only exercises the encoder if the
// encoder judges it worth encoding, and that judgement is "is this above the
// ceiling for its size" — so a fixture pinned at 2.5 Mbps tested the code
// while the 720p ceiling was 2 Mbps and quietly stopped the day it became
// 3.5 Mbps. Every affected test still passed on the way past, because
// "produced nothing" only fails the ones that check for output.
//
// Reading the ladder means raising a ceiling drags the fixture up with it.
func aboveEveryCeiling() int {
	highest := 0
	for _, r := range progressiveLadder {
		if r.maxBps > highest {
			highest = r.maxBps
		}
	}
	return highest + highest/2
}

// makeSource writes a clip that is worth re-encoding.
//
// The picture is NOISE rather than ffmpeg's test pattern. The pattern is a
// flat synthetic image that x264 compresses to a fraction of whatever target
// it is given — a 640x480 clip landed at 0.75 Mbps however high the target was
// set — so hitting a realistic bitrate with it meant forcing constant bitrate
// and padding the file with nothing. Noise has no structure to predict, so it
// costs what it says it costs, which is also what a real camera upload does.
func makeSource(t *testing.T, dir, name string, w, h int) string {
	t.Helper()
	needFFmpeg(t)
	path := filepath.Join(dir, name)
	target := itoa(aboveEveryCeiling()/1000) + "k"
	out, err := exec.Command("ffmpeg", "-y",
		"-f", "lavfi", "-i",
		"color=c=black:s="+itoa(w)+"x"+itoa(h)+
			":r=30:d=2,noise=alls=80:allf=t+u",
		"-f", "lavfi", "-i", "sine=frequency=440:duration=2",
		"-c:v", "libx264", "-preset", "ultrafast", "-pix_fmt", "yuv420p",
		"-b:v", target, "-maxrate", target, "-bufsize", target,
		"-c:a", "aac", "-shortest",
		path,
	).CombinedOutput()
	if err != nil {
		t.Skipf("could not build a source clip: %v: %s", err, lastLine(string(out)))
	}
	// The fixture is only useful above the ceiling; below it the encoder is
	// right to leave it alone and the test would be asserting on a file
	// nobody made. Say so here rather than in each caller's failure.
	if _, bps, ok := sourceShape(context.Background(), path); ok && bps > 0 {
		if smallest := smallestCeiling(); bps <= smallest {
			t.Skipf("fixture came out at %d bps, at or under the lowest "+
				"ceiling (%d) — it would correctly be served as-is",
				bps, smallest)
		}
	}
	return path
}

// smallestCeiling is the lowest rung's ceiling — the bar a fixture has to
// clear before any rung would re-encode it.
func smallestCeiling() int {
	lowest := 0
	for _, r := range progressiveLadder {
		if lowest == 0 || r.maxBps < lowest {
			lowest = r.maxBps
		}
	}
	return lowest
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
	// Noise, not testsrc. testsrc is a flat synthetic pattern that compresses
	// so well it comes out at 1.9 Mbps even at -qp 0 — under the 720p ceiling,
	// so the encoder would rightly decline to touch it and this test would be
	// asserting on a file nobody made. Noise has no structure to predict, so
	// it lands where a real generously-encoded upload lands: far above.
	src := filepath.Join(dir, "fat.mp4")
	if out, err := exec.Command("ffmpeg", "-y",
		"-f", "lavfi", "-i",
		"color=c=black:s=1280x720:r=30:d=2,noise=alls=80:allf=t+u",
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
	// Noise, and a target read off the ladder — see makeSource, which this is
	// the audio-less twin of. A clip under the ceiling would be served as-is
	// and this test would pass without encoding anything.
	target := itoa(aboveEveryCeiling()/1000) + "k"
	if out, err := exec.Command("ffmpeg", "-y",
		"-f", "lavfi", "-i",
		"color=c=black:s=1280x720:r=30:d=2,noise=alls=80:allf=t+u",
		"-c:v", "libx264", "-preset", "ultrafast", "-pix_fmt", "yuv420p",
		"-b:v", target, "-maxrate", target, "-bufsize", target,
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
	// The app picks from labels it knows. A rendition under any other name is
	// a file nobody ever plays: the backend's allow-list drops it on the way
	// in, and even past that the app's chooser has no rank or speed
	// requirement for it and would never select it.
	//
	// So adding a rung is a change in three places, not one. This list is the
	// third, and it fails loudly rather than letting a rung be encoded,
	// uploaded, and silently ignored.
	//
	// Keep in step with:
	//   - videoVariantLabels in hls_worker_api.go   (what may be stored)
	//   - bitrateNeededFor in the app's NetworkQualityService (what it needs)
	known := map[string]bool{
		"480p": true, "720p": true, "720p_hq": true, "1080p": true,
	}
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
	r, ok := rungForSize(854)
	if !ok {
		t.Fatal("no ladder rung covers an 854-wide source")
	}
	if bitrate > r.maxBps {
		t.Skipf("the fixture came out at %d bps, above the %s ceiling of %d — "+
			"it does not exercise the case", bitrate, r.label, r.maxBps)
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
	if !ok || bitrate == 0 {
		t.Skip("could not measure the clip's bitrate here")
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

// TestProgressive_HoldsTheCeilingOnHardContent is the regression for the bug
// that made this ceiling necessary.
//
// The encoder ran on production video with -crf and no -maxrate. On easy
// content that behaved; on hard content it did not, and hard content is what
// imported clips are. Files it produced and served, measured:
//
//	video 251   720p   4.48 Mbps
//	video 248   720p   4.64 Mbps
//	video 247   720p   4.14 Mbps
//
// The app downloads 768 KB before playing. At 4.5 Mbps that is 1.4 seconds,
// so the player was streaming live almost immediately — worse than the
// untouched sources this was supposed to fix.
//
// The fixture is noise, which is the hardest thing to encode there is: it has
// no structure to predict, so every frame costs almost as much as a still
// image. A crf-only encode of it runs far past any sane rate. If this test
// fails, the ceiling has stopped being enforced.
func TestProgressive_HoldsTheCeilingOnHardContent(t *testing.T) {
	needFFmpeg(t)
	dir := t.TempDir()
	src := filepath.Join(dir, "hard.mp4")
	// High-motion noise at 720p, encoded near-losslessly so the SOURCE is
	// well above every ceiling and the skip rule cannot claim it.
	if out, err := exec.Command("ffmpeg", "-y",
		"-f", "lavfi", "-i",
		"color=c=black:s=1280x720:r=30:d=3,noise=alls=100:allf=t+u",
		"-c:v", "libx264", "-preset", "ultrafast", "-qp", "10",
		"-pix_fmt", "yuv420p", src,
	).CombinedOutput(); err != nil {
		t.Skipf("could not build the hard clip: %v: %s", err, lastLine(string(out)))
	}

	_, srcBps, ok := sourceShape(context.Background(), src)
	if !ok {
		t.Skip("could not measure the fixture here")
	}
	t.Logf("source is %.2f Mbps", float64(srcBps)/1e6)

	made := buildProgressiveMP4s(context.Background(), src, dir)
	if len(made) == 0 {
		t.Fatal("nothing was encoded, so the ceiling was never exercised")
	}

	for label, path := range made {
		var want int
		for _, r := range progressiveLadder {
			if r.label == label {
				want = r.maxBps
			}
		}
		if want == 0 {
			t.Fatalf("%s is not a ladder rung", label)
		}
		_, got, ok := sourceShape(context.Background(), path)
		if !ok || got == 0 {
			t.Errorf("%s: could not measure the output", label)
			continue
		}
		t.Logf("%s came out at %.2f Mbps (ceiling %.2f)",
			label, float64(got)/1e6, float64(want)/1e6)
		// Whole-file rate, so the audio track and container overhead ride on
		// top of the video ceiling. A little headroom, not a lot.
		if limit := want + want/4; got > limit {
			t.Errorf("%s came out at %.2f Mbps against a ceiling of %.2f.\n\n"+
				"The 768 KB the app pre-downloads now covers %.1f seconds "+
				"instead of %.1f, which is what makes a reel stall.",
				label, float64(got)/1e6, float64(want)/1e6,
				768*1024*8/float64(got), 768*1024*8/float64(want))
		}
	}
}

// TestProgressive_TwoRungsShareASizeAtDifferentRates is the point of the
// fast-connection rung, and the thing the de-duplication has to stop
// swallowing.
//
// The ladder now carries two 1280-wide entries on purpose: the same picture
// at 2.5 and 3.5 Mbps, so a viewer's own measured connection decides which
// they get rather than one global number decided from somebody else's link.
// De-duplication used to key on picture size alone, which would silently drop
// the second — the encode would simply not happen and nothing would say so.
func TestProgressive_TwoRungsShareASizeAtDifferentRates(t *testing.T) {
	needFFmpeg(t)
	dir := t.TempDir()
	src := makeSource(t, dir, "wide.mp4", 1280, 720)

	made := buildProgressiveMP4s(context.Background(), src, dir)

	for _, want := range []string{"480p", "720p", "720p_hq"} {
		if _, ok := made[want]; !ok {
			t.Fatalf("a 1280-wide source produced no %s; got %v", want, made)
		}
	}

	// Same picture size, different bitrate. That is the whole distinction.
	loSide, loBps, ok1 := sourceShape(context.Background(), made["720p"])
	hiSide, hiBps, ok2 := sourceShape(context.Background(), made["720p_hq"])
	if !ok1 || !ok2 {
		t.Fatal("could not measure the two renditions")
	}
	if loSide != hiSide {
		t.Errorf("the two 720p rungs came out %d and %d wide; they are meant "+
			"to differ in bitrate, not size", loSide, hiSide)
	}
	t.Logf("720p %.2f Mbps, 720p_hq %.2f Mbps",
		float64(loBps)/1e6, float64(hiBps)/1e6)
	if hiBps <= loBps {
		t.Errorf("720p_hq came out at %d bps against 720p's %d. The higher "+
			"rung is supposed to spend more bits; if it does not, it is a "+
			"second copy of the same file and should not be on the ladder.",
			hiBps, loBps)
	}

	// ...different files. If the ceiling is not actually binding, these are
	// the same video stored twice and the extra encode buys nothing.
	loInfo, err := os.Stat(made["720p"])
	if err != nil {
		t.Fatal(err)
	}
	hiInfo, err := os.Stat(made["720p_hq"])
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("720p %d KB, 720p_hq %d KB", loInfo.Size()/1024, hiInfo.Size()/1024)
	if hiInfo.Size() <= loInfo.Size() {
		t.Errorf("720p_hq is %d bytes against 720p's %d. The higher rung is "+
			"supposed to spend more bits; if it does not, it is a second copy "+
			"of the same file and should not be on the ladder.",
			hiInfo.Size(), loInfo.Size())
	}
}

// TestProgressive_ASmallSourceStillGetsOneRendition guards the other side of
// that de-duplication change.
//
// When a source is smaller than a rung, that rung clamps down to the source
// size — and then several rungs land on the same picture with nothing to tell
// them apart, because a source with few bits to begin with cannot spend the
// higher ceiling. Keying on size AND ceiling would make three copies of one
// video.
func TestProgressive_ASmallSourceStillGetsOneRendition(t *testing.T) {
	needFFmpeg(t)
	dir := t.TempDir()
	src := makeSource(t, dir, "small.mp4", 640, 360)

	made := buildProgressiveMP4s(context.Background(), src, dir)
	if len(made) != 1 {
		t.Errorf("a 640-wide source produced %d renditions (%v); every rung "+
			"clamps to 640 there, so they would be the same video stored "+
			"repeatedly", len(made), made)
	}
}
