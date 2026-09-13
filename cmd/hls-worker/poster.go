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

// posterMaxWidth bounds the picture.
//
// 540, down from 720. That is not a guess — it is where the measurements
// landed. Encoding the same three reels at a range of widths and qualities
// and scoring each against the native frame:
//
//	at ~41 KB    720 wide, quality 18   SSIM 0.845
//	             540 wide, quality 10   SSIM 0.873
//	             480 wide, quality 8    SSIM 0.874
//
// For any given number of BYTES, a narrower picture at higher quality beats
// a wider one at lower quality. 540 is the turn: 480 gives up more on
// landscape reels than it saves. So the width comes down and the quality
// stays up, which is the opposite of the obvious move.
const posterMaxWidth = 540

// posterMaxBytes is what a cover may cost.
//
// ══════════════════════════════════════════════════════════════════════════
// A SIZE BUDGET, NOT A QUALITY NUMBER
// ══════════════════════════════════════════════════════════════════════════
//
// This used to be a fixed quality of 6 and whatever size that produced.
// Across the reels in this feed that was 3 KB to 109 KB — a THIRTY-FOLD
// spread, decided by how busy the picture happened to be. The heavy end is
// most of a video's opening download spent on a still image, in front of the
// video it is delaying.
//
// The thing that has to be true is not "quality 6". It is that the cover is
// on screen well before the video is. So that is what is fixed, and the
// quality moves to meet it.
//
// Derived, not chosen: the app can start a reel once it holds the file's
// index plus about two seconds of video — roughly 390 KB for a short reel,
// 570 KB for a three-minute one (see prefixReadyBytesFor in the app). A
// tenth of the smaller figure arrives in a tenth of the time, which is the
// margin that makes the cover feel instant rather than merely early.
const posterMaxBytes = 40 * 1024

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
			"-vf", fmt.Sprintf("scale='min(%d,iw)':-2", posterMaxWidth),
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
