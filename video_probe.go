package main

// video_probe.go — find out how big an uploaded video actually is.
//
// ════════════════════════════════════════════════════════════════════════════════
// WHY THIS EXISTS
// ════════════════════════════════════════════════════════════════════════════════
//
// The phone uploads video straight to object storage using a signed URL. The
// bytes never pass through this server, which is the whole reason the video
// pipeline is affordable — but it also means nothing here has ever known what
// was actually uploaded.
//
// The app shrinks video to 720p before sending it. That is a promise made by
// code running on someone else's phone. It holds for the normal path and it
// does not hold for anything else: an old build, a modified client, a bug in
// the shrink step, or content seeded by a script. A profile run on a real
// device found exactly that — files named "720p.mp4" that decoded at 1920x1080,
// one of them at 60fps, because whatever put them there skipped the shrink.
//
// That matters because a cheap phone can only decode so much at once. Serving
// it a 4K file is how a feed freezes on a device we never tested on.
//
// So: after the client says "here is my video", read a few kilobytes back out
// of storage and find out the truth. No transcode, no full download — an MP4
// carries its dimensions in a small header, and we only need that header.
//
// ════════════════════════════════════════════════════════════════════════════════
// WHAT IT DOES NOT DO
// ════════════════════════════════════════════════════════════════════════════════
//
// It does not re-encode anything. This server runs on a free tier with no CPU
// budget for FFmpeg — that is what the separate transcode worker is for. The
// probe's job is to REFUSE what is too big, not to fix it.
//
// It also fails open. A probe that cannot reach storage, or cannot make sense
// of the file, allows the upload. Refusing on "I could not check" would turn
// every storage hiccup into a user who cannot post. We reject only what we
// have positively measured as too large.

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"
)

// maxUploadLongSide is the ceiling, measured on the LONGER of the two sides so
// it reads the same for portrait and landscape video.
//
// 1920 admits ordinary 1080p (1920x1080 landscape, 1080x1920 portrait) and
// refuses 4K. It is deliberately NOT 1280: the app targets 720p, but a phone
// that recorded 1080p and uploaded before the shrink step is a real user with
// real content, and rejecting them outright is worse than serving one large
// file until the transcode worker produces a ladder for it.
//
// Override with MAX_UPLOAD_LONG_SIDE to tighten or loosen without a deploy.
const maxUploadLongSide = 1920

// probeHeadBytes is how much of the file's start we read looking for the
// header. An MP4 written for streaming puts its header first, and that header
// is small — a few KB for a short clip. 256 KB is generous enough to clear the
// `ftyp` box and any `free` padding an encoder left behind.
const probeHeadBytes = 256 * 1024

// probeTailBytes is the fallback read for files whose header sits at the END.
// Plenty of encoders write it there because the size is not known until the
// last frame. Those are the same files the client has to warm from both ends.
const probeTailBytes = 512 * 1024

// probeTimeout bounds the whole probe. Creating a challenge should not hang
// because storage is slow; on timeout we fail open and let the upload through.
const probeTimeout = 8 * time.Second

// errNoDimensions means the bytes we read did not contain a usable track
// header. Treated as "could not check", never as "too big".
var errNoDimensions = errors.New("no video track dimensions found")

// videoDimensions is what a successful probe learned.
type videoDimensions struct {
	Width  int
	Height int
}

// LongSide is the dimension the ceiling is compared against, so portrait and
// landscape video are judged the same way.
func (d videoDimensions) LongSide() int {
	if d.Width > d.Height {
		return d.Width
	}
	return d.Height
}

func (d videoDimensions) String() string {
	return fmt.Sprintf("%dx%d", d.Width, d.Height)
}

// uploadLongSideLimit returns the configured ceiling.
func uploadLongSideLimit() int {
	if v := os.Getenv("MAX_UPLOAD_LONG_SIDE"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return maxUploadLongSide
}

// ════════════════════════════════════════════════════════════════════════════════
// MP4 PARSING
// ════════════════════════════════════════════════════════════════════════════════
//
// An MP4 is a tree of boxes. Each box is [4-byte size][4-byte name][payload],
// and the size counts the 8-byte header. We only care about one leaf:
//
//	moov              the header for the whole file
//	 └─ trak          one per track (video, audio, subtitles...)
//	     └─ tkhd      that track's header — carries its display size
//
// Audio tracks carry 0x0, so the video track is simply the one with a size.
//
// Deliberately hand-rolled rather than pulled in as a dependency: this is one
// well-specified walk over four box names, and the alternative is a library
// that parses the entire format to answer a question about 8 bytes of it.

// parseMP4Dimensions walks the boxes in buf and returns the largest track it
// can find dimensions for.
//
// "Largest" rather than "first" because a file can carry more than one video
// track, and the one that decides decode cost is the biggest.
//
// Pure — no I/O — so every branch below is testable from a byte slice.
func parseMP4Dimensions(buf []byte) (videoDimensions, error) {
	best := videoDimensions{}
	walkMP4Boxes(buf, func(name string, payload []byte) bool {
		if name != "tkhd" {
			return true // keep walking; containers are descended into below
		}
		if d, ok := parseTkhd(payload); ok {
			if d.LongSide() > best.LongSide() {
				best = d
			}
		}
		return true
	})
	if best.Width == 0 || best.Height == 0 {
		return videoDimensions{}, errNoDimensions
	}
	return best, nil
}

// mp4Containers are the boxes we descend into. Everything else is skipped
// whole, which is what keeps this cheap: we never read sample data.
var mp4Containers = map[string]bool{
	"moov": true,
	"trak": true,
	"mdia": true,
	"edts": true,
}

// walkMP4Boxes calls fn for every box it finds, descending into containers.
// Stops early if fn returns false. Tolerant of truncation — a buffer that ends
// mid-box simply ends the walk, because the head of a file legitimately does.
func walkMP4Boxes(buf []byte, fn func(name string, payload []byte) bool) {
	for off := 0; off+8 <= len(buf); {
		size := int(binary.BigEndian.Uint32(buf[off : off+4]))
		name := string(buf[off+4 : off+8])

		switch {
		case size == 0:
			// "To end of file." Nothing after it, so treat the rest as payload.
			size = len(buf) - off
		case size == 1:
			// 64-bit size in the 8 bytes after the name.
			if off+16 > len(buf) {
				return
			}
			s64 := binary.BigEndian.Uint64(buf[off+8 : off+16])
			// A box larger than the buffer is normal (mdat). Skipping past the
			// end just ends the walk.
			if s64 > uint64(len(buf)-off) {
				return
			}
			size = int(s64)
		case size < 8:
			return // malformed: a box cannot be smaller than its own header
		}

		end := off + size
		if end > len(buf) {
			// Truncated — usual for the head of a file. Hand over what we have
			// so a container split across the boundary still gets descended.
			end = len(buf)
		}
		payload := buf[off+8 : end]

		if mp4Containers[name] {
			walkMP4Boxes(payload, fn)
		} else if !fn(name, payload) {
			return
		}

		if size <= 0 {
			return // defensive: never loop forever on a zero-width box
		}
		off = end
	}
}

// parseTkhd reads a track header's display width and height.
//
// Layout after the box header, per ISO/IEC 14496-12:
//
//	version(1) flags(3)
//	v0: creation(4) modified(4) trackID(4) reserved(4) duration(4)   = 20
//	v1: creation(8) modified(8) trackID(4) reserved(4) duration(8)   = 32
//	reserved(8) layer(2) altGroup(2) volume(2) reserved(2) matrix(36)
//	width(4) height(4)          ← both 16.16 fixed point
//
// Width and height are the DISPLAY size, already accounting for a rotation
// matrix, which is the right number here: it is what a player has to put on
// screen. A portrait clip recorded on a rotated sensor reports 1080x1920, not
// 1920x1080, and either way LongSide() gives the same answer.
func parseTkhd(payload []byte) (videoDimensions, bool) {
	if len(payload) < 4 {
		return videoDimensions{}, false
	}
	version := payload[0]

	var offset int
	switch version {
	case 0:
		offset = 4 + 20 + 8 + 8 + 36
	case 1:
		offset = 4 + 32 + 8 + 8 + 36
	default:
		return videoDimensions{}, false
	}
	if len(payload) < offset+8 {
		return videoDimensions{}, false
	}

	// 16.16 fixed point: the whole-number part is the top 16 bits.
	w := int(binary.BigEndian.Uint32(payload[offset:offset+4]) >> 16)
	h := int(binary.BigEndian.Uint32(payload[offset+4:offset+8]) >> 16)
	if w <= 0 || h <= 0 {
		// Audio and subtitle tracks are 0x0. Not an error, just not video.
		return videoDimensions{}, false
	}
	return videoDimensions{Width: w, Height: h}, true
}

// ════════════════════════════════════════════════════════════════════════════════
// FETCHING
// ════════════════════════════════════════════════════════════════════════════════

// probeHTTPClient is separate from any shared client so a slow storage origin
// can never tie up a connection pool the request path depends on.
var probeHTTPClient = &http.Client{Timeout: probeTimeout}

// probeVideoDimensions reads enough of the video at url to learn its size.
//
// Tries the start of the file first, because a file prepared for streaming
// puts its header there. Falls back to the END for the files that do not —
// plenty of encoders write the header last, since the size is not known until
// the final frame. That is the same split the client copes with when it warms
// a reel from both ends.
func probeVideoDimensions(ctx context.Context, url string) (videoDimensions, error) {
	if url == "" {
		return videoDimensions{}, errors.New("empty url")
	}

	head, err := fetchRange(ctx, url, fmt.Sprintf("bytes=0-%d", probeHeadBytes-1))
	if err != nil {
		return videoDimensions{}, err
	}
	if d, err := parseMP4Dimensions(head); err == nil {
		return d, nil
	}

	// Header is not at the front. Ask for the last stretch instead.
	tail, err := fetchRange(ctx, url, fmt.Sprintf("bytes=-%d", probeTailBytes))
	if err != nil {
		return videoDimensions{}, err
	}
	return parseMP4Dimensions(tail)
}

// fetchRange GETs one byte range. Storage that ignores Range and returns the
// whole file is handled by capping how much we read, so a 4K upload cannot
// pull hundreds of megabytes through this server.
func fetchRange(ctx context.Context, url, rangeHeader string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Range", rangeHeader)

	resp, err := probeHTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20)); _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusPartialContent && resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("probe fetch: unexpected status %d", resp.StatusCode)
	}
	// Cap regardless of what the server chose to send us.
	limit := int64(probeHeadBytes)
	if probeTailBytes > probeHeadBytes {
		limit = int64(probeTailBytes)
	}
	return io.ReadAll(io.LimitReader(resp.Body, limit))
}

// ════════════════════════════════════════════════════════════════════════════════
// THE GATE
// ════════════════════════════════════════════════════════════════════════════════

// checkUploadWithinLimits probes the video and reports whether it may be
// published.
//
// Returns the measured dimensions when it managed to measure them, so the
// caller can store them — knowing a video is 1080p is what later lets the feed
// keep it away from a phone that cannot decode it.
//
// FAILS OPEN on every "could not check" path: unreachable storage, an
// unparseable file, a timeout. Only a positive measurement above the ceiling
// is a refusal. A user who cannot post because object storage was briefly slow
// is a worse outcome than one oversized video reaching the feed.
func checkUploadWithinLimits(videoURL string) (dims videoDimensions, ok bool, measured bool) {
	if videoURL == "" {
		return videoDimensions{}, true, false
	}
	ctx, cancel := context.WithTimeout(context.Background(), probeTimeout)
	defer cancel()

	d, err := probeVideoDimensions(ctx, videoURL)
	if err != nil {
		// Could not check. Allow, and say why in the log so a storage problem
		// that silently disables the gate is visible rather than invisible.
		return videoDimensions{}, true, false
	}
	return d, d.LongSide() <= uploadLongSideLimit(), true
}

// ════════════════════════════════════════════════════════════════════════════════
// STORING WHAT WE MEASURED
// ════════════════════════════════════════════════════════════════════════════════

// recordVideoDimensions saves a measurement against a challenge or a response.
//
// Metadata, not correctness: the row is already published and useful without
// it, so a failure here is logged and dropped rather than surfaced. What it
// buys is the feed being able to keep a large file away from a phone that
// cannot decode it — see feedMaxLongSide.
//
// kind is "challenge" or "response"; anything else is ignored rather than
// interpolated into SQL.
func recordVideoDimensions(kind, id string, d videoDimensions) {
	if db == nil || id == "" || d.Width <= 0 || d.Height <= 0 {
		return
	}
	var table string
	switch kind {
	case "challenge":
		table = "challenges"
	case "response":
		table = "challenge_responses"
	default:
		return
	}
	// Table name comes from the switch above, never from a caller's string.
	q := "UPDATE " + table + " SET video_width=$1, video_height=$2 WHERE id=$3"
	if _, err := db.Exec(q, d.Width, d.Height, id); err != nil {
		log.Printf("could not record %s %s dimensions (%s): %v", kind, id, d, err)
	}
}

// gateUpload is the one call both upload paths make.
//
// Returns a message to refuse with, or "" to allow. On allow it also hands
// back what it measured, so the caller can store it.
//
// The refusal text names the actual size and the ceiling, because "your video
// is too large" with no numbers is a support ticket.
func gateUpload(videoURL string) (refusal string, dims videoDimensions, measured bool) {
	d, ok, measured := checkUploadWithinLimits(videoURL)
	if !measured {
		// Could not check. Allowed by design — see checkUploadWithinLimits.
		// Logged so a storage problem that silently disables the gate shows up
		// as a pattern in the logs rather than as nothing at all.
		log.Printf("upload size gate: could not measure %s — allowing", videoURL)
		return "", videoDimensions{}, false
	}
	if !ok {
		return fmt.Sprintf(
			"video is %s, which is larger than this app supports (longest side must be %d or less)",
			d, uploadLongSideLimit()), d, true
	}
	return "", d, true
}
