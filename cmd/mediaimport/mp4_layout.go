package main

// Where an MP4 keeps its index, read from the opening bytes.
//
// An MP4 is a flat list of boxes. Two matter here: moov is the index —
// sample tables, durations, codec configuration, everything a player needs
// before it can decode a frame — and mdat is the media itself. Their ORDER
// is not fixed by the format, and it decides whether a video can start
// playing before it has finished downloading.
//
//	ftyp moov mdat  → "faststart". The index arrives first, so a player can
//	                  begin as soon as the opening bytes land.
//	ftyp mdat moov  → the index is at the END. A player must reach the last
//	                  bytes of the file before it can start.
//
// This tool used to upload whatever it downloaded, byte for byte, so a
// source encoded the second way stayed that way in our bucket for good.
// One clip in the catalog is exactly that, and it cost more than a slow
// start: the app warms the opening 768 KB of every reel and serves it from
// a local proxy, which for that file is 768 KB that cannot answer a single
// question the player has. It reads the slice, finds no index, and goes
// back to the network for the tail before showing a frame — while being
// counted as a cache hit, so the slowest reel in the feed was logged among
// the fastest.
//
// Fixing it at import is the right place. The remux is lossless and takes
// about a second, the alternative is every client working around every
// badly exported source forever, and a source we do not control is exactly
// the kind of input that should be normalised on the way in.
type mp4Layout int

const (
	// Neither box appeared in the bytes given. Not a verdict: treat it as
	// fastStart, because remuxing on the strength of a short read would
	// rewrite files that are already fine.
	mp4LayoutUnknown mp4Layout = iota

	// moov comes before mdat. Ready to stream as-is.
	mp4LayoutFastStart

	// mdat comes before moov. Needs the faststart remux.
	mp4LayoutMoovAtEnd
)

func (l mp4Layout) String() string {
	switch l {
	case mp4LayoutFastStart:
		return "faststart"
	case mp4LayoutMoovAtEnd:
		return "moov-at-end"
	default:
		return "unknown"
	}
}

// Every box header is at least a 4-byte size and a 4-byte type.
const mp4HeaderBytes = 8

// mp4LayoutProbeBytes is how much of a file's opening readMP4Layout needs.
//
// ftyp is a few dozen bytes and any padding before moov is small, so the
// box that settles the question is normally inside the first hundred.
const mp4LayoutProbeBytes = 4096

// readMP4Layout reports the top-level box order in head, the opening bytes
// of a file.
//
// Only box headers are walked, never their contents. Returns
// mp4LayoutUnknown rather than erroring on anything it does not understand
// — a truncated read, a nonsense size, bytes that are not an MP4 — so a
// misread can only ever leave a file exactly as it arrived.
func readMP4Layout(head []byte) mp4Layout {
	for offset := 0; offset+mp4HeaderBytes <= len(head); {
		declared := int(be32(head[offset:]))
		switch string(head[offset+4 : offset+8]) {
		case "moov":
			return mp4LayoutFastStart
		case "mdat":
			return mp4LayoutMoovAtEnd
		}

		var size int
		switch declared {
		case 1:
			// 64-bit size, in the 8 bytes after the type. A high word that
			// is set means a file far larger than any reel, which is not
			// something to reason further about.
			if offset+16 > len(head) || be32(head[offset+8:]) != 0 {
				return mp4LayoutUnknown
			}
			size = int(be32(head[offset+12:]))
		case 0:
			// "Extends to end of file", so nothing follows it.
			return mp4LayoutUnknown
		default:
			size = declared
		}

		// A box cannot be smaller than its own header. One that claims to
		// be would leave the cursor where it was, and the loop would never
		// end.
		if size < mp4HeaderBytes {
			return mp4LayoutUnknown
		}
		offset += size
	}
	return mp4LayoutUnknown
}

func be32(b []byte) uint32 {
	return uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3])
}
