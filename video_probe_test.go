package main

import (
	"encoding/binary"
	"testing"
)

// Building real MP4 bytes rather than mocking the parser, because the failure
// this guards against is misreading a byte offset — which a mock cannot catch.

// box assembles one MP4 box: [4-byte size][4-byte name][payload].
func box(name string, payload []byte) []byte {
	out := make([]byte, 8, 8+len(payload))
	binary.BigEndian.PutUint32(out[0:4], uint32(8+len(payload)))
	copy(out[4:8], name)
	return append(out, payload...)
}

// tkhdPayload builds a track header carrying the given display size.
// version selects the 32-bit (0) or 64-bit (1) time field layout.
func tkhdPayload(version byte, width, height int) []byte {
	var timeFields int
	switch version {
	case 0:
		timeFields = 20 // creation(4) modified(4) trackID(4) reserved(4) duration(4)
	case 1:
		timeFields = 32 // creation(8) modified(8) trackID(4) reserved(4) duration(8)
	}
	// version+flags, times, reserved(8), layer/altGroup/volume/reserved(8),
	// matrix(36), then width+height.
	p := make([]byte, 4+timeFields+8+8+36+8)
	p[0] = version
	off := 4 + timeFields + 8 + 8 + 36
	binary.BigEndian.PutUint32(p[off:off+4], uint32(width)<<16)
	binary.BigEndian.PutUint32(p[off+4:off+8], uint32(height)<<16)
	return p
}

// mp4WithTracks builds moov > trak > tkhd for each size given.
func mp4WithTracks(version byte, sizes ...[2]int) []byte {
	var traks []byte
	for _, s := range sizes {
		traks = append(traks, box("trak", box("tkhd", tkhdPayload(version, s[0], s[1])))...)
	}
	// A realistic file opens with ftyp, which the walk must step over.
	return append(box("ftyp", make([]byte, 16)), box("moov", traks)...)
}

func TestParseMP4Dimensions_ReadsDisplaySize(t *testing.T) {
	cases := []struct {
		name    string
		version byte
		w, h    int
	}{
		{"1080p landscape, v0", 0, 1920, 1080},
		{"1080p portrait, v0", 0, 1080, 1920},
		{"720p landscape, v0", 0, 1280, 720},
		{"4K, v0", 0, 3840, 2160},
		{"1080p, v1 (64-bit times)", 1, 1920, 1080},
		{"portrait 720, v1", 1, 720, 1280},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			d, err := parseMP4Dimensions(mp4WithTracks(c.version, [2]int{c.w, c.h}))
			if err != nil {
				t.Fatalf("parseMP4Dimensions: %v", err)
			}
			if d.Width != c.w || d.Height != c.h {
				t.Errorf("got %dx%d, want %dx%d", d.Width, d.Height, c.w, c.h)
			}
		})
	}
}

// The ceiling is compared on the long side so a portrait and a landscape file
// of the same size are judged identically. If this used width alone, every
// portrait video — which is most of them on a phone — would sail past it.
func TestVideoDimensions_LongSide(t *testing.T) {
	cases := []struct {
		w, h, want int
	}{
		{1920, 1080, 1920},
		{1080, 1920, 1920},
		{1280, 720, 1280},
		{720, 720, 720},
	}
	for _, c := range cases {
		got := videoDimensions{Width: c.w, Height: c.h}.LongSide()
		if got != c.want {
			t.Errorf("%dx%d long side = %d, want %d", c.w, c.h, got, c.want)
		}
	}
}

// An audio track reports 0x0. It must be skipped, not treated as a video track
// of no size — otherwise a file whose audio track sorts first reads as 0x0 and
// the gate silently passes everything.
func TestParseMP4Dimensions_IgnoresZeroSizedTracks(t *testing.T) {
	data := mp4WithTracks(0, [2]int{0, 0}, [2]int{1920, 1080})
	d, err := parseMP4Dimensions(data)
	if err != nil {
		t.Fatalf("parseMP4Dimensions: %v", err)
	}
	if d.Width != 1920 || d.Height != 1080 {
		t.Errorf("got %s, want 1920x1080 — the audio track was not skipped", d)
	}
}

// More than one video track: the one that decides decode cost is the biggest,
// so that is the one the ceiling must be applied to.
func TestParseMP4Dimensions_PicksLargestTrack(t *testing.T) {
	data := mp4WithTracks(0, [2]int{640, 360}, [2]int{3840, 2160}, [2]int{1280, 720})
	d, err := parseMP4Dimensions(data)
	if err != nil {
		t.Fatalf("parseMP4Dimensions: %v", err)
	}
	if d.LongSide() != 3840 {
		t.Errorf("got %s, want the 3840x2160 track", d)
	}
}

// A file with no video at all must report "could not measure", NOT 0x0 — the
// two are different, and only the second would wrongly pass the gate.
func TestParseMP4Dimensions_NoVideoTrackIsAnError(t *testing.T) {
	if _, err := parseMP4Dimensions(mp4WithTracks(0, [2]int{0, 0})); err == nil {
		t.Error("expected an error for a file with no sized track")
	}
	if _, err := parseMP4Dimensions([]byte("not an mp4 at all")); err == nil {
		t.Error("expected an error for non-MP4 bytes")
	}
	if _, err := parseMP4Dimensions(nil); err == nil {
		t.Error("expected an error for empty input")
	}
}

// We only ever read the FIRST slice of a file, so the buffer routinely ends
// in the middle of a box. That must end the walk quietly rather than panic or
// loop — this is the normal case, not an edge case.
func TestParseMP4Dimensions_SurvivesTruncation(t *testing.T) {
	full := mp4WithTracks(0, [2]int{1920, 1080})
	for cut := 1; cut < len(full); cut++ {
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("panic on %d-byte truncation: %v", cut, r)
				}
			}()
			_, _ = parseMP4Dimensions(full[:cut])
		}()
	}
}

// A box claiming to be huge (mdat, usually) must not send the walk past the
// end of the buffer, and a nonsense size must not spin.
func TestWalkMP4Boxes_HandlesHostileSizes(t *testing.T) {
	// A box declaring a size larger than the data present.
	huge := make([]byte, 16)
	binary.BigEndian.PutUint32(huge[0:4], 0xFFFFFFF0)
	copy(huge[4:8], "mdat")

	// A box declaring a size smaller than its own header.
	tiny := make([]byte, 16)
	binary.BigEndian.PutUint32(tiny[0:4], 2)
	copy(tiny[4:8], "junk")

	// A 64-bit size box whose extended size overruns the buffer.
	big64 := make([]byte, 16)
	binary.BigEndian.PutUint32(big64[0:4], 1)
	copy(big64[4:8], "mdat")
	binary.BigEndian.PutUint64(big64[8:16], 1<<62)

	for name, data := range map[string][]byte{
		"oversized 32-bit": huge,
		"undersized":       tiny,
		"oversized 64-bit": big64,
	} {
		t.Run(name, func(t *testing.T) {
			done := make(chan struct{})
			go func() {
				defer close(done)
				defer func() { _ = recover() }()
				walkMP4Boxes(data, func(string, []byte) bool { return true })
			}()
			<-done // a hang here is the failure this guards against
		})
	}
}

// The gate itself: what passes, what does not.
func TestUploadLimit_AcceptsNormalPhoneVideoAndRefusesOversize(t *testing.T) {
	limit := uploadLongSideLimit()

	allowed := []videoDimensions{
		{1280, 720},  // what the app is supposed to produce
		{720, 1280},  // the same, portrait
		{1920, 1080}, // a phone that uploaded before the shrink step
		{1080, 1920},
	}
	for _, d := range allowed {
		if d.LongSide() > limit {
			t.Errorf("%s should be allowed under a %d ceiling", d, limit)
		}
	}

	refused := []videoDimensions{
		{3840, 2160}, // 4K
		{2160, 3840},
		{7680, 4320}, // 8K
	}
	for _, d := range refused {
		if d.LongSide() <= limit {
			t.Errorf("%s should be refused under a %d ceiling", d, limit)
		}
	}
}

func TestUploadLongSideLimit_EnvOverride(t *testing.T) {
	t.Setenv("MAX_UPLOAD_LONG_SIDE", "1280")
	if got := uploadLongSideLimit(); got != 1280 {
		t.Errorf("limit = %d, want 1280", got)
	}
	// Nonsense values must fall back to the built-in rather than disabling
	// the gate by setting it to zero.
	t.Setenv("MAX_UPLOAD_LONG_SIDE", "not-a-number")
	if got := uploadLongSideLimit(); got != maxUploadLongSide {
		t.Errorf("limit = %d, want the default %d", got, maxUploadLongSide)
	}
	t.Setenv("MAX_UPLOAD_LONG_SIDE", "0")
	if got := uploadLongSideLimit(); got != maxUploadLongSide {
		t.Errorf("limit = %d on a zero override, want the default %d", got, maxUploadLongSide)
	}
}

// An empty URL is "nothing to check", which must allow rather than refuse —
// a challenge can legitimately be created before its video URL is known.
func TestCheckUploadWithinLimits_EmptyURLAllows(t *testing.T) {
	_, ok, measured := checkUploadWithinLimits("")
	if !ok {
		t.Error("an empty URL must not block creation")
	}
	if measured {
		t.Error("nothing was measured, so measured must be false")
	}
}

// The whole safety posture in one test: when the probe cannot reach storage,
// the upload goes through. Refusing on "could not check" would turn a storage
// blip into users who cannot post.
func TestCheckUploadWithinLimits_FailsOpenWhenUnreachable(t *testing.T) {
	// Port 0 on loopback is never listening, so this fails fast.
	_, ok, measured := checkUploadWithinLimits("http://127.0.0.1:0/nope.mp4")
	if !ok {
		t.Error("an unreachable probe must fail OPEN, not block the upload")
	}
	if measured {
		t.Error("nothing was measured, so measured must be false")
	}
}
