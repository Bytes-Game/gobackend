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
	{label: "480p", maxLongSide: 854, crf: 24, audioBps: 96_000},
	// The default almost everybody gets.
	{label: "720p", maxLongSide: 1280, crf: 22, audioBps: 128_000},
}

// progressiveSkipBps is the bitrate at or below which a source is already
// lean enough to leave alone.
//
// ════════════════════════════════════════════════════════════════════════════
// WHY SKIP AT ALL
// ════════════════════════════════════════════════════════════════════════════
//
// Re-encoding is lossy. It is worth it on a file that is carrying far more
// bits than its picture needs — an imported clip measured at 853x480 and
// 2.68 Mbps, which is roughly four times what that size wants, came out of
// our encoder at 0.74 Mbps and 72% smaller with the same picture. All of that
// saving was waste, not detail.
//
// Run the same pass over a file that is ALREADY at 0.7 Mbps and the trade
// inverts: there is almost nothing left to squeeze out, so the generation
// loss is most of what changes. Better to serve the original.
//
// One megabit is the line. Comfortably above what 480p needs and below what
// an over-encoded file carries, so the two cases land on opposite sides of it
// without needing to guess per video.
const progressiveSkipBps = 1_000_000

type progressiveRendition struct {
	label       string
	maxLongSide int
	// crf is constant-quality encoding: pick a quality, let the bitrate land
	// wherever the content needs it. See encodeProgressive for why this is
	// not a fixed bitrate.
	crf      int
	audioBps int
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
	// A source already at or under the threshold is left alone unless a rung
	// would genuinely shrink its picture — see progressiveSkipBps and the
	// check inside the loop.
	alreadyLean := bitrate > 0 && bitrate <= progressiveSkipBps

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

		// Already lean, and this rung would not make the picture smaller
		// either — so there is nothing left for an encode to win, and it
		// would spend a lossy generation to find that out. See
		// progressiveSkipBps.
		//
		// A rung that DOES shrink the picture still earns its place at any
		// bitrate: fewer pixels is less to decode on a weak phone as well as
		// fewer bytes on the wire.
		if alreadyLean && box >= longSide {
			log.Printf("progressive: %s already at %.2f Mbps for its size, "+
				"leaving %s alone", filepath.Base(src),
				float64(bitrate)/1e6, r.label)
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
