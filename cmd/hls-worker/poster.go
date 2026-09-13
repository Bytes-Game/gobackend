package main

// poster.go — one still frame per video, so a reel has something to show
// before it can play.
//
// ════════════════════════════════════════════════════════════════════════════
// WHY
// ════════════════════════════════════════════════════════════════════════════
//
// The app already knows how to show a poster. The feed sends a thumbnailUrl,
// the reel tile paints it while the video opens, and the comments in that
// code talk about the poster "holding the face until the first frame lands".
//
// Nothing ever made one. Measured on production: 42 videos in the arena, ONE
// with a thumbnail — and that one came from the import script. The word
// "thumbnail" did not appear anywhere in this worker.
//
// So every reel showed a black rectangle until its first video frame
// decoded, which on a cold open is a download plus a decoder start. That is
// the "black screen before every video" people see. The machinery to avoid
// it was all there with nothing to put in it.
//
// ════════════════════════════════════════════════════════════════════════════
// WHY IT IS ALMOST FREE
// ════════════════════════════════════════════════════════════════════════════
//
// The expensive parts of a job — fetching the source and encoding five
// renditions — are already paid for by the time this runs. The file is on
// local disk. Pulling one frame out of it costs about a second, against the
// two to eight minutes the rest of the job takes.
//
// A poster is also tiny next to what it saves. It is a few tens of kilobytes
// against the 768 KB of video the app must otherwise fetch before it can put
// anything on screen at all.

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"time"
)

// posterAtSeconds is how far into the video to grab the frame.
//
// Not frame zero. Videos routinely open on a fade from black, a slate, or a
// hand still moving away from the camera, and a poster of black is no better
// than no poster. One second in is past the fade on almost everything while
// still being the same shot the video opens on — a later grab risks showing
// something from the middle that does not match what starts playing.
const posterAtSeconds = 1.0

// posterMaxShortSide bounds the picture by its SHORT side.
//
// ══════════════════════════════════════════════════════════════════════════
// WHY THE SHORT SIDE AND NOT THE WIDTH
// ══════════════════════════════════════════════════════════════════════════
//
// This was a width cap, and it was wrong for half the catalog.
//
// Capping WIDTH at 540 gives a portrait reel 540x960, which is right, and a
// landscape one 540x304 — only 304 pixels tall. Every surface that shows a
// cover is portrait: the reel is full-screen portrait, and the search grid
// is three columns of portrait tiles. A 304-tall picture cropped to fill a
// portrait tile is upscaled about 1.6x, and it looks it.
//
// Measured on the real posters after the width cap shipped:
//
//	landscape reel   540x304   was 720x406   noticeably softer
//	portrait reel    540x960   was 720x1280  no visible change
//
// The short side is the one that decides how much upscaling a portrait crop
// has to do, so that is the side to bound. The same idea the upload gate
// already uses with LongSide(), so portrait and landscape are judged the
// same way — applied here too late.
//
// 540 keeps portrait exactly where it was and gives landscape 960x540
// instead of 540x304.
const posterMaxShortSide = 540

// posterMaxBytes is what a cover may cost.
//
// ══════════════════════════════════════════════════════════════════════════
// A TIME BUDGET, WRITTEN IN BYTES
// ══════════════════════════════════════════════════════════════════════════
//
// This used to be a fixed quality of 6 and whatever size that produced.
// Across this feed that was 3 KB to 109 KB — a thirty-fold spread, decided
// by how busy the picture happened to be. The heavy end is most of a video's
// opening download spent on a still image, in front of the video it is
// delaying.
//
// The thing that has to be true is not "quality 6". It is that the cover is
// on screen fast enough to read as instant, which is about a fifth of a
// second. On the slowest link this app actually measures — 3 Mbps, from the
// device logs — that is:
//
//	40 KB   0.11s
//	64 KB   0.17s
//	109 KB  0.29s   (what the old fixed quality produced at worst)
//
// So 64 KB. An earlier version of this said 40 KB, derived as a tenth of
// what a video costs to become playable. That was a fraction of the wrong
// thing: what matters is how long the person waits, not how the cover
// compares to the video. It was also set while the width cap was quietly
// halving landscape posters, so the pictures it was sized against were
// smaller than they should have been.
const posterMaxBytes = 64 * 1024

// posterQualityLadder is tried best-first until one fits [posterMaxBytes].
//
// Most reels clear the budget on the first rung, so the common case is the
// quality this always produced and one ffmpeg call, same as before. Only a
// busy picture walks down, and only as far as it has to.
//
// On ffmpeg's scale 2 is best and 31 is worst. The last rung is the floor:
// if even that will not fit, the cover is kept anyway. A large cover is
// worth more than none — a reel with no still is the black screen this whole
// mechanism exists to remove.
var posterQualityLadder = []int{6, 8, 10, 12, 16, 20}

// posterTimeout bounds the grab. It is one frame from a local file, so a
// second is generous; the cap exists because this runs inside a job budget
// that the transcode has already spent most of, and a wedged ffmpeg here
// would cost the video its whole attempt for the sake of a still image.
const posterTimeout = 45 * time.Second

// posterScaleFilter bounds the SHORT side, whichever it is.
//
// -2 asks for whatever the other side works out to, rounded even, which the
// JPEG encoder requires. min() keeps a picture already smaller than the cap
// at its own size rather than blowing it up: upscaling adds bytes and no
// detail.
//
// Its own function so a test can build the exact same picture to compare
// against, rather than a near-enough one.
func posterScaleFilter() string {
	return fmt.Sprintf(
		"scale='if(gt(iw,ih),-2,min(%[1]d,iw))':'if(gt(iw,ih),min(%[1]d,ih),-2)'",
		posterMaxShortSide)
}

// makePoster writes a JPEG next to the source and returns its path.
//
// Every failure returns an empty path and no error. A poster is a nicety: a
// video with no still shows what it always showed, and failing the job over
// one would trade a working video for a black rectangle. The caller logs the
// reason so a systematic failure is still visible.
func makePoster(ctx context.Context, srcPath, outDir string) (string, error) {
	dst := filepath.Join(outDir, "poster.jpg")

	ctx, cancel := context.WithTimeout(ctx, posterTimeout)
	defer cancel()

	// -ss before -i seeks by keyframe, which is what makes this fast: ffmpeg
	// jumps straight there instead of decoding a second of video to reach it.
	//
	// scale keeps the aspect ratio: -2 asks for whatever height matches the
	// width, rounded to an even number, which the JPEG encoder requires. A
	// video already narrower than the cap is left alone rather than blown up.
	//
	// Walked best-first until one fits the budget. Each attempt overwrites
	// the last, so what is left on disk is always the best rung that fit —
	// or, if none did, the smallest one there is.
	for _, q := range posterQualityLadder {
		args := []string{
			"-hide_banner", "-loglevel", "error",
			"-ss", strconv.FormatFloat(posterAtSeconds, 'f', 2, 64),
			"-i", srcPath,
			"-frames:v", "1",
			// Bound the SHORT side, whichever it is. -2 asks for whatever
			// the other side works out to, rounded even, which the JPEG
			// encoder requires. min() keeps a picture already smaller than
			// the cap at its own size rather than blowing it up.
			"-vf", posterScaleFilter(),
			"-q:v", strconv.Itoa(q),
			"-y", dst,
		}
		out, err := exec.CommandContext(ctx, "ffmpeg", args...).CombinedOutput()
		if err != nil {
			// Not worth walking the ladder for a failure that is about the
			// video rather than the size — the next rung fails the same way.
			return "", fmt.Errorf("ffmpeg: %v: %s", err, trimOutput(out))
		}
		// A file we cannot measure is one the check below will report on.
		// Walking further would only overwrite it with another we cannot
		// measure either.
		st, err := os.Stat(dst)
		if err != nil || st.Size() <= posterMaxBytes {
			break
		}
	}

	// A video shorter than the seek point produces no frame, and ffmpeg can
	// report that as success with an empty file. Serving a zero-byte poster
	// would be worse than serving none: the app would wait on an image that
	// can never paint.
	st, err := os.Stat(dst)
	if err != nil {
		return "", fmt.Errorf("no poster written: %w", err)
	}
	if st.Size() == 0 {
		_ = os.Remove(dst)
		return "", fmt.Errorf("poster came out empty (video shorter than %.0fs?)", posterAtSeconds)
	}
	return dst, nil
}

// trimOutput keeps a command's complaint short enough to log.
func trimOutput(b []byte) string {
	const max = 300
	s := string(b)
	if len(s) > max {
		return s[:max] + "…"
	}
	return s
}
