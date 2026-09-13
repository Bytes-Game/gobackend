package main

import (
	"encoding/binary"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

// Refusing video that is too LONG for a short-video feed.
//
// The limit was written down once, in challenge_validation.go, and it covered
// battle answers only. The challenges those answers reply to had no check at
// all — not a loose one, none — so a ten-minute upload walked in through the
// front door while a ten-minute answer to it was refused at the side door.
//
// It cost more than a slot in the feed. An MP4 keeps an index in front of the
// video that grows with running time: 12 KB for a ten-second clip, 653 KB for
// the ten-minute one. Everything in the app that warms reels was built for the
// small end of that, so the file stalled on every play, on every account, on
// every page, until it was deleted by hand.
//
// These build real MP4 bytes rather than mocking the parser, because the
// failure being guarded against is misreading a byte offset — which a mock
// cannot catch.

// mvhdPayload builds a movie header claiming a running time. version selects
// the 32-bit (0) or 64-bit (1) layout.
func mvhdPayload(version byte, timescale, duration uint64) []byte {
	switch version {
	case 0:
		p := make([]byte, 20)
		p[0] = 0
		binary.BigEndian.PutUint32(p[12:16], uint32(timescale))
		binary.BigEndian.PutUint32(p[16:20], uint32(duration))
		return p
	default:
		p := make([]byte, 32)
		p[0] = 1
		binary.BigEndian.PutUint32(p[20:24], uint32(timescale))
		binary.BigEndian.PutUint64(p[24:32], duration)
		return p
	}
}

// mp4WithDuration builds ftyp + moov{mvhd, trak{tkhd}} — a real faststart
// layout with both the boxes the probe reads.
func mp4WithDuration(version byte, timescale, duration uint64, w, h int) []byte {
	inner := append(box("mvhd", mvhdPayload(version, timescale, duration)),
		box("trak", box("tkhd", tkhdPayload(0, w, h)))...)
	return append(box("ftyp", make([]byte, 16)), box("moov", inner)...)
}

func TestParseMvhd_ReadsRunningTime(t *testing.T) {
	cases := []struct {
		name      string
		version   byte
		timescale uint64
		duration  uint64
		want      time.Duration
	}{
		// The file that had to be deleted: 596.48 seconds at the timescale
		// ffmpeg writes.
		{"the ten-minute upload", 0, 1000, 596480, 596*time.Second + 480*time.Millisecond},
		{"an ordinary reel", 0, 1000, 10100, 10100 * time.Millisecond},
		{"a 90kHz timescale", 0, 90000, 90000 * 30, 30 * time.Second},
		{"64-bit layout", 1, 1000, 45000, 45 * time.Second},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, ok := parseMvhd(mvhdPayload(c.version, c.timescale, c.duration))
			if !ok {
				t.Fatal("could not read a running time out of a valid header")
			}
			if got != c.want {
				t.Errorf("got %s, want %s", got, c.want)
			}
		})
	}
}

func TestParseMvhd_SaysNothingRatherThanGuessing(t *testing.T) {
	cases := []struct {
		name    string
		payload []byte
	}{
		{"empty", nil},
		{"truncated v0", mvhdPayload(0, 1000, 5000)[:12]},
		{"truncated v1", mvhdPayload(1, 1000, 5000)[:20]},
		{"a timescale of zero would divide by zero", mvhdPayload(0, 0, 5000)},
		{"a duration of zero is not a length", mvhdPayload(0, 1000, 0)},
		{"the all-ones 'unknown' some encoders write", mvhdPayload(0, 1000, 0xFFFFFFFF)},
		{"a version nobody has defined", []byte{9, 0, 0, 0, 0, 0, 0, 0}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if d, ok := parseMvhd(c.payload); ok {
				t.Errorf("claimed a running time of %s from %s", d, c.name)
			}
		})
	}
}

func TestParseMP4Facts_ReadsBothInOneWalk(t *testing.T) {
	f := parseMP4Facts(mp4WithDuration(0, 1000, 30000, 1080, 1920))
	if !f.haveDuration() || f.Duration != 30*time.Second {
		t.Errorf("duration = %s, want 30s", f.Duration)
	}
	if !f.haveDimensions() || f.Width != 1080 || f.Height != 1920 {
		t.Errorf("size = %s, want 1080x1920", f.videoDimensions)
	}
}

func TestParseMP4Facts_HalfAnAnswerIsStillAnAnswer(t *testing.T) {
	// A movie header with no track header. Real files do this; the length is
	// still usable and must not be thrown away with the missing size.
	only := append(box("ftyp", make([]byte, 16)),
		box("moov", box("mvhd", mvhdPayload(0, 1000, 400000)))...)
	f := parseMP4Facts(only)
	if !f.haveDuration() {
		t.Fatal("a file that states its length must be believed about it")
	}
	if f.haveDimensions() {
		t.Error("no track header was present, so no size may be claimed")
	}
}

// ════════════════════════════════════════════════════════════════════════════
// THE GATE
// ════════════════════════════════════════════════════════════════════════════

// serveBytes stands up a range-capable origin for one file.
func serveBytes(t *testing.T, body []byte) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.ServeContent(w, r, "v.mp4", time.Time{}, strings.NewReader(string(body)))
	}))
	t.Cleanup(srv.Close)
	return srv.URL + "/v.mp4"
}

func TestGateUpload_RefusesVideoLongerThanTheLimit(t *testing.T) {
	// 596 seconds — the length of the upload that had to be deleted.
	url := serveBytes(t, mp4WithDuration(0, 1000, 596480, 852, 480))

	refusal, _, _ := gateUpload(url)
	if refusal == "" {
		t.Fatal("a ten-minute video was allowed into a short-video feed")
	}
	if !strings.Contains(refusal, "longer than this app takes") {
		t.Errorf("refusal should say the video is too long, got %q", refusal)
	}
	// A refusal with no numbers in it is a support ticket. Both the length
	// and the limit have to be in it, in a form a person can read.
	if !strings.Contains(refusal, "9m56s") {
		t.Errorf("refusal should name how long the video actually is, got %q", refusal)
	}
	if !strings.Contains(refusal, "3m0s") {
		t.Errorf("refusal should name the limit, got %q", refusal)
	}
}

func TestGateUpload_AllowsVideoInsideTheLimit(t *testing.T) {
	// Three minutes exactly. The limit is a ceiling, not an exclusion.
	url := serveBytes(t, mp4WithDuration(0, 1000, 180000, 1080, 1920))
	if refusal, _, _ := gateUpload(url); refusal != "" {
		t.Errorf("a video exactly at the limit was refused: %q", refusal)
	}
}

func TestGateUpload_StillRefusesOversizePictures(t *testing.T) {
	// The size gate must keep working now that it shares the code path.
	url := serveBytes(t, mp4WithDuration(0, 1000, 10000, 3840, 2160))
	refusal, _, _ := gateUpload(url)
	if refusal == "" {
		t.Fatal("a 4K upload was allowed")
	}
	if strings.Contains(refusal, "longer than this app takes") {
		t.Errorf("this is too BIG, not too long: %q", refusal)
	}
	if !strings.Contains(refusal, "3840x2160") {
		t.Errorf("refusal should name the size, got %q", refusal)
	}
}

func TestGateUpload_LengthIsCheckedBeforeSize(t *testing.T) {
	// A file that breaks both rules. The length is the one a person hits by
	// accident, so it is the one worth telling them about.
	url := serveBytes(t, mp4WithDuration(0, 1000, 600000, 3840, 2160))
	refusal, _, _ := gateUpload(url)
	if !strings.Contains(refusal, "long") {
		t.Errorf("want the length named first, got %q", refusal)
	}
}

func TestGateUpload_AFileThatDoesNotSayIsAllowed(t *testing.T) {
	// No movie header at all. Fail open — refusing on "could not check" turns
	// an encoder quirk into a user who cannot post.
	url := serveBytes(t, mp4WithTracks(0, [2]int{1080, 1920}))
	if refusal, _, _ := gateUpload(url); refusal != "" {
		t.Errorf("a file that does not state its length must pass, got %q", refusal)
	}
}

func TestUploadDurationLimit_EnvOverride(t *testing.T) {
	t.Setenv("MAX_UPLOAD_SECONDS", "60")
	if got := uploadDurationLimit(); got != time.Minute {
		t.Errorf("limit = %s, want 1m from the override", got)
	}
	t.Setenv("MAX_UPLOAD_SECONDS", "nonsense")
	if got := uploadDurationLimit(); got != maxUploadDuration {
		t.Errorf("limit = %s, want the default %s when the override is junk", got, maxUploadDuration)
	}
	os.Unsetenv("MAX_UPLOAD_SECONDS")
}

// ════════════════════════════════════════════════════════════════════════════
// ONE LIMIT, NOT TWO
// ════════════════════════════════════════════════════════════════════════════

// The bug was not that the number was wrong. It was that there were two
// places a video's length could be judged and only one of them had a number
// in it. These must stay the same limit.
func TestTheLengthLimitIsOneNumber(t *testing.T) {
	if got := time.Duration(maxVideoDurationMs) * time.Millisecond; got != maxUploadDuration {
		t.Errorf("the response check allows %s and the upload gate allows %s — "+
			"they have drifted apart, which is the whole bug", got, maxUploadDuration)
	}
	if maxUploadDuration != 3*time.Minute {
		t.Errorf("limit is %s, want the three minutes this feed is built for", maxUploadDuration)
	}
}

// Both upload paths must actually run the gate. A perfect gate nothing calls
// is what the challenge path had.
func TestBothUploadPathsRunTheGate(t *testing.T) {
	src, err := os.ReadFile("challenge_handler.go")
	if err != nil {
		t.Fatal(err)
	}
	if n := strings.Count(string(src), "gateUpload(payload.VideoURL)"); n != 2 {
		t.Errorf("gateUpload is called %d times in challenge_handler.go, want 2 — "+
			"one for creating a challenge and one for answering it", n)
	}
	if !strings.Contains(string(src),
		fmt.Sprintf("payload.DurationMs > %s", "maxVideoDurationMs")) {
		t.Error("creating a challenge no longer checks the length the app reports, " +
			"so an over-long upload is only caught after the server fetches it")
	}
}
