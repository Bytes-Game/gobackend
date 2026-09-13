package main

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// What a cover may COST.
//
// The poster used to be a fixed quality and whatever size that produced.
// Across the reels in this feed that was 3 KB to 109 KB — a thirty-fold
// spread, decided by how busy the picture happened to be. The heavy end is
// most of a video's opening download spent on a still image, in front of the
// video it is delaying.
//
// The thing that has to be true is not "quality 6". It is that the cover
// reaches the screen well before the video does. So that is what is fixed
// now, and the quality moves to meet it.
//
// These run ffmpeg for real. A budget that is enforced in the arguments and
// not in the bytes is the failure that matters, and it is invisible in a
// command line.

// makeBusyVideo writes a clip full of noise, which is what makes a JPEG big.
// A flat colour compresses to nothing and would clear any budget on the first
// rung, testing the loop by never entering it.
func makeBusyVideo(t *testing.T, dir string, w, h int) string {
	t.Helper()
	needFFmpeg(t)
	path := filepath.Join(dir, "busy.mp4")
	out, err := exec.Command("ffmpeg", "-hide_banner", "-loglevel", "error",
		"-f", "lavfi", "-i",
		"color=c=gray:s="+strconv.Itoa(w)+"x"+strconv.Itoa(h)+
			":r=30:d=3,noise=alls=100:allf=t+u",
		"-c:v", "libx264", "-preset", "ultrafast", "-crf", "12",
		"-pix_fmt", "yuv420p", "-y", path,
	).CombinedOutput()
	if err != nil {
		t.Skipf("could not build a busy clip: %v: %s", err, lastLine(string(out)))
	}
	return path
}

func TestPoster_BusyPictureIsBroughtInsideTheBudget(t *testing.T) {
	dir := t.TempDir()
	src := makeBusyVideo(t, dir, 1080, 1920)

	got, err := makePoster(context.Background(), src, dir)
	if err != nil {
		t.Fatalf("makePoster: %v", err)
	}
	st, err := os.Stat(got)
	if err != nil {
		t.Fatal(err)
	}
	if st.Size() > posterMaxBytes {
		t.Errorf("cover is %d bytes against a %d budget — the ladder never "+
			"walked, so a busy reel still pays for a still image in front of "+
			"the video it is delaying", st.Size(), posterMaxBytes)
	}
	if st.Size() == 0 {
		t.Error("cover is empty")
	}
}

// makeModerateVideo writes a clip with enough detail to tell quality levels
// apart, but not so much that it blows the budget on the best rung.
//
// A flat colour will not do, which is how the first version of the test
// below passed with the budget check deleted: flat blue is about the same
// number of bytes at every quality, so "did the ladder walk" was
// unanswerable from the size. Measured on this fixture: 11.6 KB at the best
// rung, 2.1 KB at the worst.
func makeModerateVideo(t *testing.T, dir string) string {
	t.Helper()
	needFFmpeg(t)
	path := filepath.Join(dir, "moderate.mp4")
	out, err := exec.Command("ffmpeg", "-hide_banner", "-loglevel", "error",
		"-f", "lavfi", "-i", "color=c=gray:s=640x360:r=30:d=3,noise=alls=28:allf=t+u",
		"-c:v", "libx264", "-preset", "ultrafast", "-crf", "14",
		"-pix_fmt", "yuv420p", "-y", path,
	).CombinedOutput()
	if err != nil {
		t.Skipf("could not build a moderate clip: %v: %s", err, lastLine(string(out)))
	}
	return path
}

func TestPoster_AnEasyPictureKeepsTheBestQuality(t *testing.T) {
	// Most reels clear the budget on the first rung. Those must not be
	// quietly degraded to pay for the ones that do not.
	dir := t.TempDir()
	src := makeModerateVideo(t, dir)

	got, err := makePoster(context.Background(), src, dir)
	if err != nil {
		t.Fatalf("makePoster: %v", err)
	}

	// Encode the same frame at the ladder's best rung by hand. If the real
	// poster is bigger than that it means a HIGHER quality was used, which is
	// impossible; if it is smaller it means the ladder walked when it did not
	// need to.
	ref := filepath.Join(dir, "ref.jpg")
	if err := exec.Command("ffmpeg", "-hide_banner", "-loglevel", "error",
		"-ss", "1.00", "-i", src, "-frames:v", "1",
		"-vf", posterScaleFilter(),
		"-q:v", strconv.Itoa(posterQualityLadder[0]),
		"-y", ref).Run(); err != nil {
		t.Skipf("could not build the reference: %v", err)
	}
	a, err := os.Stat(got)
	if err != nil {
		t.Fatalf("stat %s: %v", got, err)
	}
	b, err := os.Stat(ref)
	if err != nil {
		t.Fatalf("stat %s: %v", ref, err)
	}
	if a.Size() != b.Size() {
		t.Errorf("cover is %d bytes, the best rung is %d — a picture that "+
			"already fits the budget was degraded for no reason",
			a.Size(), b.Size())
	}
	// The fixture has to be able to tell the rungs apart, or this test
	// passes with the budget check deleted. Guard the guard.
	if b.Size() > posterMaxBytes {
		t.Fatalf("fixture is %d bytes at the best rung, over the %d budget — "+
			"it cannot show that an easy picture is left alone", b.Size(), posterMaxBytes)
	}
	worst := filepath.Join(dir, "worst.jpg")
	if err := exec.Command("ffmpeg", "-hide_banner", "-loglevel", "error",
		"-ss", "1.00", "-i", src, "-frames:v", "1",
		"-vf", posterScaleFilter(),
		"-q:v", strconv.Itoa(posterQualityLadder[len(posterQualityLadder)-1]),
		"-y", worst).Run(); err == nil {
		if w, err := os.Stat(worst); err == nil && w.Size() == b.Size() {
			t.Fatal("the fixture is the same size at the best and worst rung, " +
				"so this test cannot tell whether the ladder walked")
		}
	}
}

func TestPosterLadder_IsOrderedBestFirst(t *testing.T) {
	// Walked in order and stopped at the first fit, so an unordered ladder
	// would hand out a worse picture than the budget allows.
	if len(posterQualityLadder) < 2 {
		t.Fatal("a ladder of one rung cannot adapt to anything")
	}
	for i := 1; i < len(posterQualityLadder); i++ {
		// On ffmpeg's scale a HIGHER number is worse.
		if posterQualityLadder[i] <= posterQualityLadder[i-1] {
			t.Errorf("rung %d (%d) is not worse than rung %d (%d)",
				i, posterQualityLadder[i], i-1, posterQualityLadder[i-1])
		}
	}
	if posterQualityLadder[0] > 6 {
		t.Errorf("the best rung is quality %d — reels that already fit the "+
			"budget would come out softer than before this existed",
			posterQualityLadder[0])
	}
}

func TestPoster_ArrivesFastEnoughToReadAsInstant(t *testing.T) {
	// The point of the number, and it is a TIME. An earlier version of this
	// compared the cover to the video's bytes, which is a fraction of the
	// wrong thing: what matters is how long the person waits, not how the
	// cover measures up against the video.
	//
	// 3 Mbps is the slowest link the device logs actually show.
	const slowestBps = 3000000
	seconds := float64(posterMaxBytes*8) / slowestBps

	if seconds > 0.25 {
		t.Errorf("a cover takes %.2fs on the slowest link this app sees — "+
			"past about a fifth of a second it stops reading as instant and "+
			"starts reading as a wait", seconds)
	}
	if seconds < 0.05 {
		t.Errorf("a cover may only take %.3fs, which forces every one of "+
			"them to the bottom of the ladder — that trades a black screen "+
			"for a blurry one", seconds)
	}
}

// A narrower picture at higher quality beats a wider one at lower quality for
// the same bytes — measured, not assumed:
//
//	at ~41 KB   720 wide q18  SSIM 0.845
//	            540 wide q10  SSIM 0.873
//	            480 wide q8   SSIM 0.874
//
// 480 gives up more on landscape reels than it saves, so 540 is the turn.
func TestPoster_BoundsTheShortSideNotTheWidth(t *testing.T) {
	// Capping WIDTH gives a landscape reel 540x304 — only 304 pixels tall.
	// Every surface that shows a cover is portrait, so a 304-tall picture is
	// upscaled about 1.6x to fill the crop, and it looks it. The short side
	// is what decides how much upscaling a portrait crop has to do.
	src, err := os.ReadFile("poster.go")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(src), `scale='min(`) {
		t.Error("the poster is scaled by width again, which halves the " +
			"height of every landscape reel")
	}
	if posterMaxShortSide != 540 {
		t.Errorf("short side is %d; 540 keeps portrait where it was and "+
			"gives landscape 960x540 instead of 540x304", posterMaxShortSide)
	}
}

func TestPoster_LandscapeKeepsItsHeight(t *testing.T) {
	needFFmpeg(t)
	dir := t.TempDir()
	src := makeBusyVideo(t, dir, 1280, 720) // landscape

	got, err := makePoster(context.Background(), src, dir)
	if err != nil {
		t.Fatalf("makePoster: %v", err)
	}
	_, h := imageSize(t, got)
	if h < posterMaxShortSide {
		t.Errorf("landscape cover is %d tall, want %d — a shorter one is "+
			"upscaled into every portrait tile that shows it",
			h, posterMaxShortSide)
	}
}

func TestPoster_PortraitIsUnchangedByThat(t *testing.T) {
	needFFmpeg(t)
	dir := t.TempDir()
	src := makeBusyVideo(t, dir, 1080, 1920) // portrait

	got, err := makePoster(context.Background(), src, dir)
	if err != nil {
		t.Fatalf("makePoster: %v", err)
	}
	w, h := imageSize(t, got)
	if w != posterMaxShortSide {
		t.Errorf("portrait cover is %dx%d, want %d wide", w, h, posterMaxShortSide)
	}
	if h <= w {
		t.Errorf("portrait cover came out %dx%d — it is not portrait", w, h)
	}
}

func TestPoster_ASmallSourceIsNotBlownUp(t *testing.T) {
	needFFmpeg(t)
	dir := t.TempDir()
	src := makeBusyVideo(t, dir, 320, 240) // smaller than the cap either way

	got, err := makePoster(context.Background(), src, dir)
	if err != nil {
		t.Fatalf("makePoster: %v", err)
	}
	w, h := imageSize(t, got)
	if w > 320 || h > 240 {
		t.Errorf("cover is %dx%d from a 320x240 source — upscaling adds "+
			"bytes and no detail", w, h)
	}
}

// The whole mechanism must still fail soft. A cover is a nicety; a video with
// none shows what it always showed.
func TestPoster_StillGivesUpQuietlyOnAVideoItCannotRead(t *testing.T) {
	dir := t.TempDir()
	junk := filepath.Join(dir, "notavideo.mp4")
	if err := os.WriteFile(junk, []byte("this is not an mp4"), 0o644); err != nil {
		t.Fatal(err)
	}
	got, err := makePoster(context.Background(), junk, dir)
	if err == nil {
		t.Errorf("got %q and no error from a file that is not a video", got)
	}
	if got != "" {
		t.Errorf("returned a path (%q) for a poster that was never written", got)
	}
}

// A failure that is about the video, not the size, must not be retried six
// times — every rung fails the same way and the job budget is not free.
func TestPoster_DoesNotWalkTheLadderOnAVideoError(t *testing.T) {
	src := File("poster.go")
	if !strings.Contains(src, "return \"\", fmt.Errorf(\"ffmpeg: %v: %s\", err, trimOutput(out))") {
		t.Error("an ffmpeg failure no longer returns immediately, so a " +
			"broken video is decoded once per rung")
	}
}

func File(name string) string {
	b, err := os.ReadFile(name)
	if err != nil {
		return ""
	}
	return string(b)
}
