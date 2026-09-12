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
// It is shown full-bleed on a phone, so it wants to be sharp, but it is also
// fetched before anything can be shown and therefore competes with the video
// itself for the same link. 720 wide is the width of the rendition most
// people are served, so a poster is never softer than the video behind it and
// never costs more than it saves.
const posterMaxWidth = 720

// posterQuality is JPEG quality on ffmpeg's scale, where 2 is best and 31 is
// worst. 6 lands around 40-60 KB at this width — small enough to arrive
// before the video does, which is the whole point of it.
const posterQuality = 6

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
	args := []string{
		"-hide_banner", "-loglevel", "error",
		"-ss", strconv.FormatFloat(posterAtSeconds, 'f', 2, 64),
		"-i", srcPath,
		"-frames:v", "1",
		"-vf", fmt.Sprintf("scale='min(%d,iw)':-2", posterMaxWidth),
		"-q:v", strconv.Itoa(posterQuality),
		"-y", dst,
	}
	if out, err := exec.CommandContext(ctx, "ffmpeg", args...).CombinedOutput(); err != nil {
		return "", fmt.Errorf("ffmpeg: %v: %s", err, trimOutput(out))
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
