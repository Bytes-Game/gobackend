package main

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"mymodule/internal/mp4layout"
)

// ════════════════════════════════════════════════════════════════════════════
// WHAT GETS OVERWRITTEN
// ════════════════════════════════════════════════════════════════════════════
//
// bucketKeyFromURL decides which object in the bucket this worker is allowed
// to replace. Everything else in this file is about not corrupting a video;
// this is about not writing to the wrong one in the first place, so it is
// deliberately strict and these cases pin that.

func TestBucketKey_AcceptsAnAppUpload(t *testing.T) {
	key, ok := bucketKeyFromURL(
		"https://pub-abc123.r2.dev/u/1/7f52a9a9c68252492657c00d8ced5413/720p.mp4")
	if !ok {
		t.Fatal("a normal upload URL was not recognised, so nothing would ever be fixed")
	}
	if key != "u/1/7f52a9a9c68252492657c00d8ced5413/720p.mp4" {
		t.Errorf("got key %q", key)
	}
}

func TestBucketKey_IgnoresQueryAndFragment(t *testing.T) {
	// A cache-busting suffix must not end up inside the key, or the upload
	// would create a second object rather than replacing the first.
	for _, u := range []string{
		"https://pub-abc123.r2.dev/u/1/abc/720p.mp4?v=2",
		"https://pub-abc123.r2.dev/u/1/abc/720p.mp4#t=3",
	} {
		key, ok := bucketKeyFromURL(u)
		if !ok || key != "u/1/abc/720p.mp4" {
			t.Errorf("%s → %q, %v", u, key, ok)
		}
	}
}

func TestBucketKey_RefusesAnythingItDoesNotRecognise(t *testing.T) {
	// Each of these would name an object other than the upload, or no object
	// at all. Overwriting is the operation being guarded, so the only safe
	// answer to an unfamiliar shape is no.
	for _, u := range []string{
		"",
		"https://cdn.plyr.io/static/demo/trailer-720p.mp4", // not our bucket's shape
		"https://pub-abc123.r2.dev/hls/256/abcd/master.m3u8",
		"https://pub-abc123.r2.dev/u/1/720p.mp4",     // too few parts
		"https://pub-abc123.r2.dev/u/1/a/b/720p.mp4", // too many
		"https://pub-abc123.r2.dev/u//abc/720p.mp4",  // empty user
		"https://pub-abc123.r2.dev/u/1//720p.mp4",    // empty upload id
		"https://pub-abc123.r2.dev/u/1/../720p.mp4",  // climbing out
		"https://pub-abc123.r2.dev/x/1/abc/720p.mp4", // wrong root
	} {
		if key, ok := bucketKeyFromURL(u); ok {
			t.Errorf("%q was accepted as key %q", u, key)
		}
	}
}

// ════════════════════════════════════════════════════════════════════════════
// THE REMUX, AGAINST A REAL FILE
// ════════════════════════════════════════════════════════════════════════════
//
// Built with ffmpeg rather than a fixture, because the thing under test is
// whether ffmpeg did what was asked — a hand-written byte fixture would only
// test the parser, which internal/mp4layout already covers.

func needFFmpeg(t *testing.T) {
	t.Helper()
	for _, bin := range []string{"ffmpeg", "ffprobe"} {
		if _, err := exec.LookPath(bin); err != nil {
			t.Skipf("no %s here", bin)
		}
	}
}

// makeClip writes a two-second clip with picture and sound. moovAtEnd picks
// which way round the boxes go.
func makeClip(t *testing.T, dir, name string, moovAtEnd bool) string {
	t.Helper()
	path := filepath.Join(dir, name)
	args := []string{"-y",
		"-f", "lavfi", "-i", "testsrc=size=320x240:rate=15:duration=2",
		"-f", "lavfi", "-i", "sine=frequency=440:duration=2",
		"-c:v", "libx264", "-preset", "ultrafast", "-c:a", "aac", "-shortest",
	}
	if !moovAtEnd {
		args = append(args, "-movflags", "+faststart")
	}
	args = append(args, path)
	if out, err := exec.Command("ffmpeg", args...).CombinedOutput(); err != nil {
		t.Skipf("could not build a test clip: %v: %s", err, lastLine(string(out)))
	}
	return path
}

func TestRemux_MovesTheIndexAndKeepsTheVideo(t *testing.T) {
	needFFmpeg(t)
	dir := t.TempDir()
	src := makeClip(t, dir, "phone.mp4", true)

	// The premise: a phone-shaped file really does have its index at the end.
	// If this ever stops holding, the rest of the test proves nothing.
	if got, _ := mp4layout.OfFile(src); got != mp4layout.MoovAtEnd {
		t.Fatalf("the test clip came out %s, not moov-at-end", got)
	}

	fixed := filepath.Join(dir, "fixed.mp4")
	out, err := exec.Command("ffmpeg", "-y", "-i", src,
		"-c", "copy", "-movflags", "+faststart", fixed).CombinedOutput()
	if err != nil {
		t.Fatalf("remux failed: %v: %s", err, lastLine(string(out)))
	}

	if got, _ := mp4layout.OfFile(fixed); got != mp4layout.FastStart {
		t.Errorf("after the remux the file is still %s", got)
	}
	if err := sameVideoInside(context.Background(), src, fixed); err != nil {
		t.Errorf("the remux was rejected as different from the original: %v", err)
	}
}

func TestVerify_CatchesADroppedAudioTrack(t *testing.T) {
	// The failure that would otherwise sail through every other check: the
	// video looks perfect and has no sound. Nobody notices until a creator
	// asks why their upload went silent.
	needFFmpeg(t)
	dir := t.TempDir()
	src := makeClip(t, dir, "with-sound.mp4", true)

	silent := filepath.Join(dir, "silent.mp4")
	if out, err := exec.Command("ffmpeg", "-y", "-i", src,
		"-an", "-c:v", "copy", "-movflags", "+faststart", silent).CombinedOutput(); err != nil {
		t.Skipf("could not build the silent clip: %v: %s", err, lastLine(string(out)))
	}

	if err := sameVideoInside(context.Background(), src, silent); err == nil {
		t.Error("a remux that dropped the audio track was accepted, and would " +
			"have replaced the creator's upload with a silent one")
	}
}

func TestVerify_CatchesATruncatedRemux(t *testing.T) {
	// Half the video, still a valid MP4, still faststart. Length is the only
	// thing that gives it away.
	needFFmpeg(t)
	dir := t.TempDir()
	src := makeClip(t, dir, "full.mp4", true)

	short := filepath.Join(dir, "short.mp4")
	if out, err := exec.Command("ffmpeg", "-y", "-i", src, "-t", "1",
		"-c", "copy", "-movflags", "+faststart", short).CombinedOutput(); err != nil {
		t.Skipf("could not build the short clip: %v: %s", err, lastLine(string(out)))
	}

	if err := sameVideoInside(context.Background(), src, short); err == nil {
		t.Error("a remux missing half the video was accepted")
	}
}

func TestVerify_RejectsAFileThatIsStillMoovAtEnd(t *testing.T) {
	// The pointless case, and the one worth being loud about: if ffmpeg ever
	// stops honouring +faststart, this must not report success and upload the
	// same broken layout back over the original.
	needFFmpeg(t)
	dir := t.TempDir()
	src := makeClip(t, dir, "a.mp4", true)

	if err := sameVideoInside(context.Background(), src, src); err == nil {
		t.Error("a file that still has its index at the end was accepted as fixed")
	}
}

func TestVerify_RejectsGarbage(t *testing.T) {
	needFFmpeg(t)
	dir := t.TempDir()
	src := makeClip(t, dir, "a.mp4", true)
	junk := filepath.Join(dir, "junk.mp4")
	if err := os.WriteFile(junk, []byte("this is not a video"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := sameVideoInside(context.Background(), src, junk); err == nil {
		t.Error("a file that is not a video at all was accepted as the fixed version")
	}
}

func TestFixSource_LeavesForeignURLsAlone(t *testing.T) {
	// No bucket credentials in this process, so the check is that it returns
	// without trying to upload anything rather than what it uploads. A URL we
	// did not write is not ours to rewrite.
	fixSourceFastStart(context.Background(), &workerConfig{}, pendingJob{
		SourceURL: "https://cdn.plyr.io/static/demo/trailer-720p.mp4",
	}, "nonexistent.mp4")
}
