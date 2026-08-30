package main

// progressive.go — our own MP4, made to our rules instead of the camera's.
//
// ════════════════════════════════════════════════════════════════════════════
// WHY THIS EXISTS
// ════════════════════════════════════════════════════════════════════════════
//
// The file people actually watch has never been touched by us.
//
// The app takes what the camera produced, copies it, names the copy "720p"
// and uploads it — deliberately, because re-encoding on the phone silently
// dropped the audio track on some Android chips (see the note in the app's
// video_processor_service). The server then builds an HLS ladder, which the
// app uses only as a fallback: it plays the MP4 because HLS needs three round
// trips before a frame appears where an MP4 needs one, and on a sub-90-second
// reel there is no time for adaptive bitrate to earn those trips back.
//
// So every viewer gets whatever a stranger's phone happened to write. That
// means we do not control:
//
//	how big it is        an 11 MB clip that should be 2 MB, paid for by every
//	                     viewer and by us
//	where the index is   at the back, on every Android upload — see faststart.go
//	keyframe spacing     which is what makes looping and scrubbing feel tight
//	the pixel format     and the profile, which is what old phones can decode
//
// This makes our own MP4 instead. Same one-file, one-request playback the app
// already does — a file we chose, rather than one we inherited.
//
// ════════════════════════════════════════════════════════════════════════════
// IT ADDS FILES, IT NEVER REPLACES ONE
// ════════════════════════════════════════════════════════════════════════════
//
// The renditions land next to the HLS output, under the same random
// per-transcode prefix, so every key is new and nothing anybody uploaded is
// touched. If this fails, the upload is exactly where it was and the app keeps
// playing it.
//
// That is a deliberate difference from faststart.go, which does overwrite. A
// container rewrite is provably the same video; a re-encode is a judgement
// call about quality, and a judgement call should not be able to destroy the
// only copy of somebody's video.
//
// ════════════════════════════════════════════════════════════════════════════
// THE APP NEEDS NO CHANGES
// ════════════════════════════════════════════════════════════════════════════
//
// It already reads a videoVariants map keyed by "480p"/"720p" and picks one
// from the network and the phone's memory — that path predates this and has
// simply had nothing to pick from. Filling it in is enough.

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"

	"mymodule/internal/mp4layout"
)

// progressiveLadder is what we serve as plain MP4.
//
// Two rungs, not four. The HLS ladder can afford 240p and 360p because a
// player switches between them mid-video; a progressive file is chosen once,
// before playback, so a rung nobody would deliberately pick is a rung nobody
// picks. The app's own chooser only knows 480p, 720p and 1080p.
//
// 1080p is absent for the same reason it is absent from the HLS ladder: on a
// phone, full-screen, under ninety seconds, it is not distinguishable from
// 720p, and device logs showed 1080p decoder sessions stalling for seconds on
// a mid-range chip.
var progressiveLadder = []progressiveRendition{
	// Cellular and older phones. Small enough to arrive before somebody
	// gives up, large enough not to look broken.
	{label: "480p", maxLongSide: 854, crf: 24, maxBps: 1_500_000, audioBps: 96_000},
	// The default almost everybody gets.
	{label: "720p", maxLongSide: 1280, crf: 22, maxBps: 2_500_000, audioBps: 128_000},
}

// ════════════════════════════════════════════════════════════════════════════
// pickingTheCeiling — where the numbers above come from
// ════════════════════════════════════════════════════════════════════════════
//
// The first ceiling was 2 Mbps at 720p, and it was too low. Measured on a real
// feed video (a 4.14 Mbps source), re-encoding at each ceiling and comparing
// the picture against that source with SSIM:
//
//	ceiling      SSIM      768 KB covers
//	2.0 Mbps     0.970     3.1s
//	2.5 Mbps     0.978     2.5s
//	3.0 Mbps     0.985     2.1s
//	3.5 Mbps     0.990     1.8s
//
// SSIM around 0.97 is where compression starts being visible on motion, and
// it was: the change was reported as the picture getting noticeably worse.
// 0.99 is close enough to the source that there is nothing to see.
//
// The awkward part is the right-hand column. The app pre-downloads a fixed
// number of BYTES before playing, so a better picture buys a shorter head
// start and the two goals fight — at a fixed prefix, every gain on one side is
// a loss on the other.
//
// So the prefix moved too, from 768 KB to 2 MB, in the app. That is what makes
// 3.5 Mbps affordable: 2 MB at 3.5 Mbps is 4.8 seconds of runway, better than
// the 3.1 seconds the 2 Mbps ceiling bought AND better than the 1.5 seconds
// the uncapped files gave. Both numbers have to move together; changing this
// ceiling without looking at VideoCacheService.prefixBytes will trade one
// complaint for the other.
//
// A slower x264 preset was measured too, since it would have been free
// quality. It is not worth it here: preset slow scored 0.9851 against medium's
// 0.9847 at the same ceiling, for 50% more encode time.
//
// ════════════════════════════════════════════════════════════════════════════
// WHY 720p CAME BACK DOWN TO 2.5
// ════════════════════════════════════════════════════════════════════════════
//
// 3.5 Mbps was chosen on picture quality alone, against the source. It looked
// right and it was, on that axis. What it left out is whether a phone can
// actually pull 3.5 Mbps for the length of a reel.
//
// The app now measures its own downloads and reports the answer. On the
// connection this was tuned for:
//
//	link=6.1 Mbps    link=5.5    link=5.2    link=3.5
//
// Typically fine, and it drops to 3.5. A 3.5 Mbps file on a 3.5 Mbps link has
// nothing to spare, and these are progressive MP4s: a reel that is already
// playing cannot switch down when the link dips, the way an adaptive stream
// would. It just stops. The device log carried 296 of those stops across 110
// reels.
//
// So the ceiling has to fit under the WORST the connection does, not the
// typical. At 2.5 Mbps a reel needs about 3.25 Mbps to stream comfortably,
// which fits under that 3.5 floor with room left.
//
// The cost is real and was weighed: 0.978 SSIM against the source, where 3.5
// scored 0.990. The alternative was letting the app fall back to 480p on every
// dip, and 480p is 854 pixels wide against 1280 — a much bigger visible loss
// than the step from 0.990 to 0.978, which is why this is the better trade
// rather than simply a more cautious one.
//
// If the app ever gains adaptive switching mid-reel, this can go back up:
// the constraint here is the inability to change quality once playback has
// started, not the picture.
//
// ════════════════════════════════════════════════════════════════════════════
// WHEN NOT TO ENCODE AT ALL
// ════════════════════════════════════════════════════════════════════════════
//
// Re-encoding is lossy. It is worth it on a file carrying far more bits than
// its picture needs, because nearly everything it strips out is waste. Run the
// same pass over a file that is already lean and the trade inverts: there is
// almost nothing left to strip, so the generation loss is most of what
// changes. Better to serve the original.
//
// The line is each rung's OWN ceiling — see maxBps on progressiveRendition —
// not one number for the whole ladder. A single global threshold was the first
// attempt and it was incoherent: with the 720p ceiling at 2 Mbps, a 1.8 Mbps
// source is already where we would put it, and encoding it anyway spent a
// lossy generation to arrive back at 1.8 Mbps.
//
// Keying the skip to the ceiling makes the two rules one rule — "get every
// video to at most this rate, and do nothing to one that is already there" —
// so they cannot drift apart when a ceiling is next tuned.

type progressiveRendition struct {
	label       string
	maxLongSide int
	// crf is constant-quality encoding: pick a quality, let the bitrate land
	// wherever the content needs it. See encodeProgressive for why this is
	// not a fixed bitrate — and why it is not crf on its own either.
	crf int
	// maxBps is the ceiling crf is NOT allowed to spend past. See
	// encodeProgressive for how it is enforced, and pickingTheCeiling below
	// for how these particular numbers were chosen.
	maxBps   int
	audioBps int
}

// rungForSize returns the rung a source of this size naturally sits at: the
// smallest one whose box would not shrink the picture.
//
// It reports false when the source is larger than every rung — a 1080p upload,
// say. That is not "no opinion", it is the answer: something that big is going
// to be scaled down whatever its bitrate, because every viewer decoding four
// times the pixels is a cost of its own.
func rungForSize(longSide int) (progressiveRendition, bool) {
	best, found := progressiveRendition{}, false
	for _, r := range progressiveLadder {
		if r.maxLongSide < longSide {
			continue
		}
		if !found || r.maxLongSide < best.maxLongSide {
			best, found = r, true
		}
	}
	return best, found
}

// buildProgressiveMP4s encodes our renditions and returns label → local path.
//
// Never fatal, and never partial in a way the caller has to reason about: a
// rendition that fails is left out of the map, and the caller serves whatever
// did work — down to nothing at all, which is exactly today's behaviour.
func buildProgressiveMP4s(ctx context.Context, src, outDir string) map[string]string {
	made := map[string]string{}
	longSide, bitrate, ok := sourceShape(ctx, src)
	if !ok {
		log.Printf("progressive: could not measure %s, skipping our own encode", src)
		return made
	}
	hasAudio := probeHasAudio(src)

	// ════════════════════════════════════════════════════════════════════════
	// LEAVE A SOURCE THAT IS ALREADY RIGHT COMPLETELY ALONE
	// ════════════════════════════════════════════════════════════════════════
	//
	// All or nothing, deliberately. Skipping rung by rung looks tidier and is
	// a trap: a 1280x720 source already at 1.5 Mbps would have the 720p rung
	// skipped (it is under that ceiling) while the 480p rung still ran, so the
	// only rendition on offer would be the 480p one — and the app, which picks
	// the best label present, would serve 480p to everybody on good wifi. A
	// downgrade, caused by the source being GOOD.
	//
	// So when no rung would shrink the picture and the source is already at or
	// under the ceiling for its size, we produce nothing at all. An empty map
	// leaves video_variants untouched and the app keeps playing the upload
	// itself, which in this case is exactly the file we would have made.
	if r, ok := rungForSize(longSide); ok && bitrate > 0 && bitrate <= r.maxBps {
		log.Printf("progressive: %s is %d-tall-side at %.2f Mbps, already "+
			"inside the %s ceiling of %.2f Mbps — serving it as it is",
			filepath.Base(src), longSide, float64(bitrate)/1e6,
			r.label, float64(r.maxBps)/1e6)
		return made
	}

	// Boxes already encoded, so a rung that would repeat one is skipped. See
	// the loop below.
	done := map[int]bool{}

	for _, r := range progressiveLadder {
		// ════════════════════════════════════════════════════════════════════
		// NEVER ENLARGE THE PICTURE
		// ════════════════════════════════════════════════════════════════════
		//
		// ffmpeg's force_original_aspect_ratio=decrease does NOT mean "only
		// shrink". It means "fit inside the box, preserving aspect" — and it
		// will happily grow a smaller source to reach the box. A real upload,
		// 960x720, came out of the 720p rung at 1280x960: more pixels than it
		// started with, no more detail, and a file 97% the size of the
		// original instead of a third of it.
		//
		// So the box is clamped to the source. A 960-wide video asked for at
		// "720p" gets a 960 box and is re-encoded at its own size; a
		// 1920-wide one gets 1280 and is genuinely scaled down.
		box := r.maxLongSide
		if longSide < box {
			box = longSide
		}
		// Two rungs that landed on the same box would be the same file under
		// two names — identical bytes, twice the CPU, twice the storage, and
		// a chooser picking between things that do not differ.
		if done[box] {
			continue
		}

		done[box] = true

		out := filepath.Join(outDir, r.label+".mp4")
		if err := encodeProgressive(ctx, src, out, r, box, hasAudio); err != nil {
			log.Printf("progressive: %s failed, carrying on without it: %v", r.label, err)
			_ = os.Remove(out)
			continue
		}
		if err := progressiveLooksRight(ctx, out); err != nil {
			log.Printf("progressive: %s came out wrong, dropping it: %v", r.label, err)
			_ = os.Remove(out)
			continue
		}
		made[r.label] = out
	}
	return made
}

// encodeProgressive writes one rendition.
//
// ════════════════════════════════════════════════════════════════════════════
// WHY CONSTANT QUALITY AND NOT A FIXED BITRATE
// ════════════════════════════════════════════════════════════════════════════
//
// The HLS ladder uses fixed bitrates because it has to: a player switching
// between rungs mid-video needs to know in advance what each one costs.
//
// Nothing here switches. So the better trade is to name a QUALITY and let the
// size land where the content puts it. A person talking to camera against a
// wall might come out at 400 kbps and look perfect; the same 2500 kbps we
// would have forced on it is wasted on both of us. A confetti cannon in a
// nightclub needs every bit of 4 Mbps, and a fixed 2500 would have turned it
// to mush.
//
// CRF 22 at 720p is the ordinary "looks like the original" setting for this
// kind of footage. 24 at 480p, slightly looser, because a smaller picture
// hides more and that rung exists for people on bad connections.
//
// -preset medium rather than veryfast. The ladder uses veryfast because it
// encodes four rungs and lives inside a job timeout; this encodes two, and
// medium buys roughly 20% smaller files at the same quality for CPU we have.
func encodeProgressive(ctx context.Context, src, out string, r progressiveRendition, box int, hasAudio bool) error {
	args := []string{"-y", "-i", src,
		"-map", "0:v:0",
	}
	if hasAudio {
		args = append(args, "-map", "0:a:0")
	}
	args = append(args,
		"-c:v", "libx264",
		"-crf", fmt.Sprintf("%d", r.crf),
		// ════════════════════════════════════════════════════════════════════
		// THE CEILING, AND WHY CRF ALONE WAS NOT ENOUGH
		// ════════════════════════════════════════════════════════════════════
		//
		// -crf says "hold this quality, spend whatever it takes". On easy
		// video that is exactly right and the file comes out small. On hard
		// video — fast cuts, grain, heavy motion — "whatever it takes" is a
		// lot, and nothing was stopping it.
		//
		// Measured on files this encoder actually produced and served:
		//
		//	video 251   720p   4.48 Mbps
		//	video 248   720p   4.64 Mbps
		//	video 247   720p   4.14 Mbps
		//	video 241   720p   0.97 Mbps   ← easy content, crf behaving
		//
		// The app downloads 768 KB before it starts playing. At 4.5 Mbps that
		// is 1.4 seconds of video; the player then has to keep pace with a
		// 4.5 Mbps stream live, and any dip in the connection is a stall.
		// That was worse than the untouched sources this was meant to fix.
		//
		// -maxrate with -bufsize is the standard pairing for this: quality
		// stays the target, but a stretch of hard video cannot buy its way
		// past the ceiling.
		//
		// bufsize is the window the limit is measured over, and it is one
		// second's worth rather than the more usual two. Measured by
		// re-encoding the 4.14 Mbps file this bug shipped:
		//
		//	bufsize 2s   2.15 Mbps    768 KB covers 2.9s
		//	bufsize 1s   2.04 Mbps    768 KB covers 3.1s
		//	bufsize 0.5s 1.72 Mbps    768 KB covers 3.7s
		//
		// A wider window lets the opening seconds run over the ceiling and
		// pay it back later. Normally that is a good trade. Here it is the
		// worst place to spend it: the opening is precisely the part the app
		// pre-downloads, so an overspend there is a shorter head start, which
		// is the whole problem. Half a second holds the rate tighter still,
		// at a real cost to quality on scene cuts — reels are all scene cuts.
		"-maxrate", fmt.Sprintf("%d", r.maxBps),
		"-bufsize", fmt.Sprintf("%d", r.maxBps),
		"-preset", "medium",
		// A square box, so the same setting works on a portrait reel and a
		// landscape one — whichever side is longer is the one that meets the
		// limit. The box is pre-clamped to the source by the caller, because
		// "decrease" alone will enlarge a small video to fill it.
		//
		// force_divisible_by=2 — libx264 refuses odd dimensions, and a
		// portrait source scaled by aspect lands on them constantly. This
		// failed every upload once already; see transcodeHLS.
		"-vf", fmt.Sprintf(
			"scale=w=%d:h=%d:force_original_aspect_ratio=decrease:force_divisible_by=2,fps=30",
			box, box),
		// Keyframe every second. Twice as often as the HLS ladder, and worth
		// it here: a reel loops, and every loop is a seek back to the start.
		// Sparse keyframes are what makes a loop stutter before it catches.
		"-g", "30",
		"-keyint_min", "30",
		// yuv420p and High@4.0 — the combination every phone shipped in the
		// last decade can decode in hardware. A source in 4:2:2 or 10-bit
		// (some newer phones, some editing apps) would otherwise produce a
		// file that plays on the encoder's machine and nowhere else.
		"-pix_fmt", "yuv420p",
		"-profile:v", "high",
		"-level", "4.0",
		// The index at the FRONT. This is the whole point of faststart.go,
		// applied here by construction instead of as a repair.
		"-movflags", "+faststart",
	)
	if hasAudio {
		args = append(args,
			"-c:a", "aac",
			"-b:a", fmt.Sprintf("%dk", r.audioBps/1000),
			"-ac", "2",
			// 48kHz: MediaTek decoders handle it more reliably than the
			// 44100 some Android cameras emit. Same reasoning as the ladder.
			"-ar", "48000",
		)
	}
	args = append(args, out)

	cmd := exec.CommandContext(ctx, "ffmpeg", args...)
	if o, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("%w: %s", err, lastLine(string(o)))
	}
	return nil
}

// progressiveLooksRight is the gate between "ffmpeg exited zero" and "we are
// willing to serve this".
//
// ffmpeg exits zero on plenty of files that are not what was asked for. The
// checks are cheap and each one stands for a way a reel has actually broken
// before: a file with no duration plays as a black screen; an index at the
// back is the bug this whole change exists to stop; a missing video stream is
// a silent audio-only "video".
func progressiveLooksRight(ctx context.Context, path string) error {
	st, err := os.Stat(path)
	if err != nil {
		return err
	}
	if st.Size() == 0 {
		return fmt.Errorf("empty file")
	}
	shape, err := describeMedia(ctx, path)
	if err != nil {
		return fmt.Errorf("could not describe it: %w", err)
	}
	if shape.duration <= 0 {
		return fmt.Errorf("no duration")
	}
	if len(shape.codecs) == 0 {
		return fmt.Errorf("no streams")
	}
	hasVideo := false
	for _, c := range shape.codecs {
		if c == "h264" {
			hasVideo = true
		}
	}
	if !hasVideo {
		return fmt.Errorf("no h264 video stream, got %v", shape.codecs)
	}
	switch layout, err := mp4layout.OfFile(path); {
	case err != nil:
		return fmt.Errorf("could not read its layout: %w", err)
	case layout != mp4layout.FastStart:
		return fmt.Errorf("came out %s despite +faststart", layout)
	}
	return nil
}

// sourceShape measures the two things the ladder decides from: how big the
// picture is, so a rung never enlarges it, and how many bits it is spending,
// so an already-lean file can be left alone.
//
// The bitrate comes back as 0 when the container does not declare one. That
// reads as "not known" rather than "zero", and the caller treats an unknown
// bitrate as worth encoding — the same direction the code took before this
// existed, so a container we cannot read never silently stops being processed.
func sourceShape(ctx context.Context, src string) (longSide int, bitrate int, ok bool) {
	out, err := exec.CommandContext(ctx, "ffprobe",
		"-v", "error",
		"-select_streams", "v:0",
		"-show_entries", "stream=width,height",
		"-of", "csv=p=0:s=x", src,
	).Output()
	if err != nil {
		return 0, 0, false
	}
	var w, h int
	if _, err := fmt.Sscanf(strings.TrimSpace(string(out)), "%dx%d", &w, &h); err != nil {
		return 0, 0, false
	}
	if w <= 0 || h <= 0 {
		return 0, 0, false
	}
	longSide = w
	if h > w {
		longSide = h
	}

	// Whole-file bitrate rather than the video stream's, because the whole
	// file is what a viewer downloads — the audio track is bytes on the wire
	// too. ffprobe leaves it empty on some containers; that is the 0 case.
	br, err := exec.CommandContext(ctx, "ffprobe",
		"-v", "error",
		"-show_entries", "format=bit_rate",
		"-of", "csv=p=0", src,
	).Output()
	if err == nil {
		bitrate, _ = strconv.Atoi(strings.TrimSpace(string(br)))
	}
	return longSide, bitrate, true
}
