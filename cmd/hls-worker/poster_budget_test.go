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
		"-vf", "scale='min("+strconv.Itoa(posterMaxWidth)+",iw)':-2",
		"-q:v", strconv.Itoa(posterQualityLadder[0]),
		"-y", ref).Run(); err != nil {
		t.Skipf("could not build the reference: %v", err)
	}
	a, _ := os.Stat(got)
	b, _ := os.Stat(ref)
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
		"-vf", "scale='min("+strconv.Itoa(posterMaxWidth)+",iw)':-2",
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

func TestPoster_BudgetIsSmallNextToWhatAVideoCosts(t *testing.T) {
	// The point of the number. A reel becomes playable at roughly 390 KB for
	// a short one; the cover has to be up long before that, not alongside it.
	const videoReadyBytes = 390 * 1024
	if posterMaxBytes*8 > videoReadyBytes {
		t.Errorf("a cover may cost %d bytes against a video's %d — that is "+
			"not a head start, it is a queue", posterMaxBytes, videoReadyBytes)
	}
	if posterMaxBytes < 16*1024 {
		t.Errorf("a %d byte budget forces every cover to the bottom of the "+
			"ladder, which trades the black screen for a blurry one",
			posterMaxBytes)
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
func TestPoster_WidthCameDownSoQualityCouldStayUp(t *testing.T) {
	if posterMaxWidth != 540 {
		t.Errorf("width is %d; 540 is where the measurements put the best "+
			"picture per byte", posterMaxWidth)
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
