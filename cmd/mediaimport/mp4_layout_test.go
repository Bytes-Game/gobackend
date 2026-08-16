package main

import (
	"encoding/binary"
	"testing"
)

// box builds a top-level MP4 box header: 4-byte big-endian size, 4-byte
// ASCII type, then payload bytes of filler. Only headers are ever walked,
// so the filler's contents are irrelevant — its LENGTH is what moves the
// cursor, and getting that wrong is the bug these tests exist for.
func box(boxType string, payload int, declaredSize ...int) []byte {
	size := 8 + payload
	if len(declaredSize) > 0 {
		size = declaredSize[0]
	}
	b := make([]byte, 0, 8+payload)
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(size))
	b = append(b, hdr[:]...)
	b = append(b, boxType...)
	return append(b, make([]byte, payload)...)
}

func join(parts ...[]byte) []byte {
	var out []byte
	for _, p := range parts {
		out = append(out, p...)
	}
	return out
}

func TestReadMP4Layout(t *testing.T) {
	// A 64-bit size lives in the 8 bytes AFTER the type, and the cursor
	// must advance by that value — not by 1, and not by a fixed 16.
	large64 := func(size uint64, boxType string, payload int) []byte {
		b := []byte{0, 0, 0, 1}
		b = append(b, boxType...)
		var sz [8]byte
		binary.BigEndian.PutUint64(sz[:], size)
		b = append(b, sz[:]...)
		return append(b, make([]byte, payload)...)
	}

	cases := []struct {
		name string
		head []byte
		want mp4Layout
		why  string
	}{
		{
			name: "moov before mdat is faststart",
			head: join(box("ftyp", 24), box("moov", 900), box("mdat", 64)),
			want: mp4LayoutFastStart,
		},
		{
			name: "mdat before moov is not",
			// The live shape being detected. mdat's declared size runs far
			// past the probe, which is why the verdict has to come from the
			// header rather than from finding moov.
			head: join(box("ftyp", 24), box("mdat", 64, 4*1024*1024)),
			want: mp4LayoutMoovAtEnd,
			why:  "this is the clip that has to be remuxed",
		},
		{
			name: "padding boxes before moov are skipped, not tripped over",
			// free/skip/wide legitimately sit between ftyp and moov, often
			// precisely because a faststart rewrite left a gap behind.
			head: join(box("ftyp", 24), box("free", 512), box("skip", 8),
				box("wide", 8), box("moov", 100)),
			want: mp4LayoutFastStart,
		},
		{
			name: "neither box in range decides nothing",
			head: join(box("ftyp", 24), box("free", 0)),
			want: mp4LayoutUnknown,
			why: "must not read as moov-at-end: that would remux a file " +
				"on the strength of a short read",
		},
		{
			name: "an empty read decides nothing",
			head: nil,
			want: mp4LayoutUnknown,
		},
		{
			name: "a truncated header decides nothing",
			head: []byte{0, 0, 0, 32, 0x66},
			want: mp4LayoutUnknown,
		},
		{
			name: "bytes that are not an mp4 decide nothing",
			head: func() []byte {
				b := make([]byte, 64)
				for i := range b {
					b[i] = 0xff
				}
				return b
			}(),
			want: mp4LayoutUnknown,
			why:  "a 0xffffffff size walks past the buffer and stops there",
		},
		{
			name: "size 0 means the box runs to EOF, so nothing follows",
			head: join(box("ftyp", 24, 0), box("moov", 8)),
			want: mp4LayoutUnknown,
		},
		{
			name: "a 64-bit size is read from the right place",
			head: join(large64(32, "free", 16), box("moov", 8)),
			want: mp4LayoutFastStart,
		},
		{
			name: "a 64-bit size too large to be a reel decides nothing",
			head: large64(1<<32, "free", 0),
			want: mp4LayoutUnknown,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := readMP4Layout(c.head)
			if got != c.want {
				msg := "readMP4Layout(...) = " + got.String() +
					", want " + c.want.String()
				if c.why != "" {
					msg += " — " + c.why
				}
				t.Error(msg)
			}
		})
	}
}

// A box that claims to be smaller than its own header cannot advance the
// cursor past the header that declared it. Without the floor in
// readMP4Layout the walk never terminates, so this test hanging IS the
// failure — there is no return value to assert on.
func TestReadMP4LayoutTerminatesOnABoxSmallerThanItsHeader(t *testing.T) {
	for _, bad := range []int{0, 1, 2, 7} {
		head := join(box("ftyp", 24, bad), box("moov", 8))
		if got := readMP4Layout(head); got == mp4LayoutFastStart {
			t.Errorf("a box declaring size %d was walked through as if valid; "+
				"its declared size cannot be trusted to move the cursor", bad)
		}
	}
}

// The probe budget has to clear a realistic amount of leading padding. Set
// it below that and every file reads as unknown, the remux never fires,
// and the detection is silently dead.
func TestProbeBudgetClearsRealisticPadding(t *testing.T) {
	if mp4LayoutProbeBytes < 1024 {
		t.Errorf("mp4LayoutProbeBytes = %d, too small to reach moov past "+
			"ordinary ftyp and padding boxes", mp4LayoutProbeBytes)
	}

	padded := join(box("ftyp", 32), box("free", 800), box("moov", 64))
	if len(padded) > mp4LayoutProbeBytes {
		t.Fatalf("fixture is %d bytes, larger than the probe", len(padded))
	}
	if got := readMP4Layout(padded); got != mp4LayoutFastStart {
		t.Errorf("a file with 800 bytes of padding read as %s", got)
	}
}
