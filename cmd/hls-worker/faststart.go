package main

// faststart.go — moving a video's index to the front, once, for everybody.
//
// ════════════════════════════════════════════════════════════════════════════
// THE BUG THIS EXISTS FOR
// ════════════════════════════════════════════════════════════════════════════
//
// Somebody uploads a video from their phone and it does not start cleanly.
// Not on their phone, and not on anybody else's either — a second account
// scrolling past the same video in For You sees the same hitch. That last
// part is the tell: a problem that follows the FILE rather than the viewer is
// in the file.
//
// It is. An MP4 is a flat list of boxes, and two of them matter here. `moov`
// is the index — durations, sample tables, codec setup, everything a player
// needs before it can decode a single frame. `mdat` is the video itself.
// Their order is not fixed by the format:
//
//	ftyp moov mdat   the index arrives first. A player starts as soon as the
//	                 opening bytes land.
//	ftyp mdat moov   the index is at the END. A player has to reach the last
//	                 bytes of the file before it can show anything.
//
// Android's recorder writes the second kind, and the app uploads what the
// camera produced without touching it (see the phone's video_processor_service:
// the "720p" variant is a straight file copy, on purpose). So every video
// uploaded from an Android phone lands in the bucket with its index at the
// end, and stays that way forever.
//
// Measured on two real uploads:
//
//	u/1/…/720p.mp4    2.3 MB, moov 4.5 KB starting at byte 2,344,508
//	u/39/…/720p.mp4   11 MB,  moov after 11,027,119 bytes of mdat
//
// The app warms the opening 768 KB of every reel into a local cache and plays
// from there. For these files that slice cannot answer a single question the
// player has: it reads it, finds no index, and goes back to the network for
// the tail before the first frame. Bigger file, longer wait, every viewer,
// every time.
//
// ════════════════════════════════════════════════════════════════════════════
// WHY THE SERVER, AND WHY HERE
// ════════════════════════════════════════════════════════════════════════════
//
// The phone could export it the right way round, and that would be the ideal
// root fix. It also ships in an app version, reaches people whenever they
// update, and does nothing at all for the videos already uploaded.
//
// This worker already has the file on local disk, already has the bucket
// credentials, and already runs about a minute after the upload. Fixing it
// here fixes every video, for every app version, including the ones already
// in the feed the next time they pass through.
//
// cmd/mediaimport does the same thing for imported clips and for the same
// reason. The two share internal/mp4layout.
//
// ════════════════════════════════════════════════════════════════════════════
// IT OVERWRITES THE UPLOAD, SO IT VERIFIES FIRST
// ════════════════════════════════════════════════════════════════════════════
//
// The fixed file is written back over the key it came from. That is what
// makes it work with no database change and no second URL — every reader,
// present and future, just gets a file that starts.
//
// It is also a replace, and the bytes it replaces are somebody's video. So
// nothing is uploaded until the remux has been checked against the original:
// same duration, same streams, same codecs, and the index actually moved. Any
// doubt at all and the original is left exactly where it is. A video that
// starts a little slowly is a far better outcome than a video that is gone.
//
// The remux itself does not re-encode. `-c copy` reads the container and
// writes the same compressed frames back with the boxes in the other order —
// no quality change, about a second on a reel.

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"os"
	"os/exec"
	"strconv"
	"strings"

	"mymodule/internal/mp4layout"
)

// fixSourceFastStart rewrites the uploaded file so it starts immediately, and
// puts the rewritten version back where it came from.
//
// Never fatal. This runs beside a transcode that is the actual job; a video
// that could not be straightened out still transcodes, still uploads, still
// appears in the feed. Every failure path leaves the original untouched and
// says why.
func fixSourceFastStart(ctx context.Context, cfg *workerConfig, job pendingJob, srcPath string) {
	key, ok := bucketKeyFromURL(job.SourceURL)
	if !ok {
		// A source we did not put in our own bucket — a seeded demo clip on
		// somebody else's CDN, say. Not ours to rewrite.
		return
	}
	if !strings.HasSuffix(strings.ToLower(key), ".mp4") {
		return
	}

	before, err := mp4layout.OfFile(srcPath)
	if err != nil {
		log.Printf("faststart: could not read %s: %v", key, err)
		return
	}
	if before != mp4layout.MoovAtEnd {
		// Already fine, or a read we could not interpret. Both mean leave it
		// alone: rewriting on the strength of a misread would churn healthy
		// files and change every object's bytes for nothing.
		return
	}

	fixed := srcPath + ".faststart.mp4"
	defer os.Remove(fixed)
	cmd := exec.CommandContext(ctx, "ffmpeg", "-y",
		"-i", srcPath,
		"-c", "copy", // container-level fix; re-encoding here would cost quality
		"-movflags", "+faststart",
		fixed,
	)
	if out, err := cmd.CombinedOutput(); err != nil {
		log.Printf("faststart: remux of %s failed, leaving it alone: %v: %s",
			key, err, lastLine(string(out)))
		return
	}

	if err := sameVideoInside(ctx, srcPath, fixed); err != nil {
		log.Printf("faststart: refusing to replace %s — %v", key, err)
		return
	}

	if err := uploadFile(ctx, cfg, fixed, key); err != nil {
		log.Printf("faststart: could not upload the fixed %s: %v", key, err)
		return
	}
	log.Printf("faststart: %s had its index at the end; moved it to the front", key)
}

// sameVideoInside checks that the remux carries the same video as the
// original, and that it actually moved the index.
//
// ffmpeg exits zero on plenty of outputs that are not what was asked for. An
// unverified remux would replace somebody's upload with whatever came out,
// while logging that it had fixed it — the same shape of silent wrongness the
// whole file is here to remove. Returns nil only when everything lines up.
func sameVideoInside(ctx context.Context, original, remuxed string) error {
	after, err := mp4layout.OfFile(remuxed)
	if err != nil {
		return fmt.Errorf("could not read the remuxed file: %w", err)
	}
	if after != mp4layout.FastStart {
		return fmt.Errorf("the remux came out %s, so it fixed nothing", after)
	}

	a, err := describeMedia(ctx, original)
	if err != nil {
		return fmt.Errorf("could not describe the original: %w", err)
	}
	b, err := describeMedia(ctx, remuxed)
	if err != nil {
		return fmt.Errorf("could not describe the remux: %w", err)
	}

	// Streams first, because a dropped audio track is the failure that would
	// otherwise pass every other check — the video would look perfectly fine
	// and be silent.
	if len(a.codecs) != len(b.codecs) {
		return fmt.Errorf("the original has %d streams and the remux has %d",
			len(a.codecs), len(b.codecs))
	}
	for i := range a.codecs {
		if a.codecs[i] != b.codecs[i] {
			return fmt.Errorf("stream %d changed from %s to %s",
				i, a.codecs[i], b.codecs[i])
		}
	}
	// A tenth of a second of slack. Rewriting the container can nudge the
	// reported duration by a frame; anything past that means content is
	// missing.
	if math.Abs(a.duration-b.duration) > 0.1 {
		return fmt.Errorf("the original runs %.2fs and the remux runs %.2fs",
			a.duration, b.duration)
	}
	if b.duration <= 0 {
		return fmt.Errorf("the remux has no duration")
	}
	return nil
}

type mediaShape struct {
	duration float64
	codecs   []string // one per stream, in file order
}

func describeMedia(ctx context.Context, path string) (mediaShape, error) {
	out, err := exec.CommandContext(ctx, "ffprobe",
		"-v", "error",
		"-show_entries", "format=duration:stream=codec_name",
		"-of", "json", path,
	).Output()
	if err != nil {
		return mediaShape{}, err
	}
	var probed struct {
		Format struct {
			Duration string `json:"duration"`
		} `json:"format"`
		Streams []struct {
			CodecName string `json:"codec_name"`
		} `json:"streams"`
	}
	if err := json.Unmarshal(out, &probed); err != nil {
		return mediaShape{}, err
	}
	shape := mediaShape{}
	shape.duration, _ = strconv.ParseFloat(strings.TrimSpace(probed.Format.Duration), 64)
	for _, s := range probed.Streams {
		shape.codecs = append(shape.codecs, s.CodecName)
	}
	return shape, nil
}

// bucketKeyFromURL turns a public video URL back into the object key it was
// uploaded under.
//
// Uploads from the app live under `u/<userId>/<uploadId>/<variant>.mp4` — see
// the backend's media_storage.go, which builds exactly that shape. Only that
// shape is accepted, which is the point: this function decides what gets
// OVERWRITTEN, so anything it does not recognise with certainty has to come
// back false. A permissive parse here would let an oddly-shaped URL name some
// other object in the bucket.
func bucketKeyFromURL(rawURL string) (string, bool) {
	s := strings.TrimSpace(rawURL)
	if s == "" {
		return "", false
	}
	// Strip the scheme and host, then anything after the path.
	for _, prefix := range []string{"https://", "http://"} {
		s = strings.TrimPrefix(s, prefix)
	}
	if i := strings.IndexAny(s, "?#"); i >= 0 {
		s = s[:i]
	}
	slash := strings.Index(s, "/")
	if slash < 0 {
		return "", false
	}
	key := strings.TrimPrefix(s[slash:], "/")

	// u / <userId> / <uploadId> / <file>. Four parts, none empty, and the
	// ids have to look like ids rather than path tricks.
	parts := strings.Split(key, "/")
	if len(parts) != 4 || parts[0] != "u" {
		return "", false
	}
	for _, p := range parts[1:] {
		if p == "" || p == "." || p == ".." {
			return "", false
		}
	}
	return key, true
}

func lastLine(s string) string {
	lines := strings.Split(strings.TrimSpace(s), "\n")
	return lines[len(lines)-1]
}
