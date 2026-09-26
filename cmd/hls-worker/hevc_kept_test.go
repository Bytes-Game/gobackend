package main

import (
	"context"
	"os/exec"
	"strings"
	"testing"
)

// The H.265 rungs shipped, and not one H.265 file was ever kept: the check
// that stands between "ffmpeg exited zero" and "serve this" asked every file
// for an h264 stream, and dropped every hevc one as "came out wrong". The
// tests that would have seen it all run a real encode and skip without
// ffmpeg, and CI did not install ffmpeg. See checkVideoStream.

func rung(label string) progressiveRendition {
	for _, r := range progressiveLadder {
		if r.label == label {
			return r
		}
	}
	panic("no rung " + label)
}

func TestCheckVideoStream_AnHevcRungWantsHevc(t *testing.T) {
	good := mediaShape{codecs: []string{"hevc", "aac"}, tags: []string{"hvc1", "mp4a"}}
	if err := checkVideoStream(good, rung("720p_hevc")); err != nil {
		t.Errorf("a proper H.265 file was refused: %v — which is every H.265 "+
			"file the worker has ever made", err)
	}
	if err := checkVideoStream(mediaShape{codecs: []string{"h264", "aac"},
		tags: []string{"avc1", "mp4a"}}, rung("720p_hevc")); err == nil {
		t.Error("an H.264 file was accepted as the H.265 rung, so a phone " +
			"told it is getting the small file would get the big one")
	}
}

func TestCheckVideoStream_AnH264RungStillWantsH264(t *testing.T) {
	// The presence half: the fix must not have loosened the old check.
	if err := checkVideoStream(mediaShape{codecs: []string{"h264", "aac"},
		tags: []string{"avc1", "mp4a"}}, rung("720p")); err != nil {
		t.Errorf("a proper H.264 file was refused: %v", err)
	}
	for _, bad := range []mediaShape{
		{codecs: []string{"hevc"}, tags: []string{"hvc1"}},
		{codecs: []string{"aac"}, tags: []string{"mp4a"}},
		{},
	} {
		if err := checkVideoStream(bad, rung("720p")); err == nil {
			t.Errorf("the 720p rung accepted %v, which is not an H.264 video", bad.codecs)
		}
	}
}

func TestCheckVideoStream_AppleNeedsTheHvc1Label(t *testing.T) {
	hev1 := mediaShape{codecs: []string{"hevc", "aac"}, tags: []string{"hev1", "mp4a"}}
	err := checkVideoStream(hev1, rung("480p_hevc"))
	if err == nil {
		t.Fatal("an H.265 file labelled hev1 was accepted. It plays on " +
			"Android and opens on every iPhone to a black screen with sound.")
	}
	if !strings.Contains(err.Error(), "hvc1") {
		t.Errorf("the refusal does not say what was wrong: %v", err)
	}
}

func TestProgressive_ABigSourceKeepsItsHevcCopies(t *testing.T) {
	// The real thing, end to end: encode, check, keep.
	needFFmpeg(t)
	if out, err := exec.Command("ffmpeg", "-hide_banner", "-encoders").Output(); err != nil ||
		!strings.Contains(string(out), "libx265") {
		t.Skip("this ffmpeg has no H.265 encoder")
	}
	dir := t.TempDir()
	src := makeSource(t, dir, "big.mp4", 1920, 1080)

	made := buildProgressiveMP4s(context.Background(), src, dir, 0)
	for _, label := range []string{"480p_hevc", "720p_hevc"} {
		path, ok := made[label]
		if !ok {
			t.Errorf("a 1920x1080 source kept no %s (made %v). Every H.265 "+
				"file used to be dropped at this point.", label, keysOf(made))
			continue
		}
		shape, err := describeMedia(context.Background(), path)
		if err != nil {
			t.Fatal(err)
		}
		if err := checkVideoStream(shape, rung(label)); err != nil {
			t.Errorf("%s was kept but is not what it claims: %v", label, err)
		}
	}
	// And the H.264 fallbacks are all still there for phones that cannot
	// decode H.265.
	for _, label := range []string{"360p", "480p", "720p"} {
		if _, ok := made[label]; !ok {
			t.Errorf("no %s — a phone without H.265 has nothing at that size", label)
		}
	}
}

func keysOf(m map[string]string) []string {
	var out []string
	for k := range m {
		out = append(out, k)
	}
	return out
}

func TestProgressiveLooksRight_ReadsTheFileItself(t *testing.T) {
	// The unit tests above hand checkVideoStream a description by hand. This
	// is the gate the worker actually calls, on a real file: an H.264 clip
	// is right for an H.264 rung and wrong for an H.265 one.
	needFFmpeg(t)
	clip := makeClip(t, t.TempDir(), "clip.mp4", false)
	if err := progressiveLooksRight(context.Background(), clip, rung("720p")); err != nil {
		t.Fatalf("a good H.264 clip was refused for the 720p rung: %v", err)
	}
	if err := progressiveLooksRight(context.Background(), clip, rung("720p_hevc")); err == nil {
		t.Error("an H.264 clip was accepted as the 720p_hevc rung — the gate " +
			"is not looking at what the file actually holds")
	}
}
