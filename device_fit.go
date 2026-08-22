package main

// device_fit.go — serve each phone video it can actually decode.
//
// ════════════════════════════════════════════════════════════════════════════════
// WHY THIS EXISTS
// ════════════════════════════════════════════════════════════════════════════════
//
// A phone can only run a small, fixed number of video decoders at once, and how
// large a frame it can decode is a property of the chip rather than of memory.
// The app already respects that on its side: it caps how many videos it holds
// open, and it holds fewer on a phone with less RAM.
//
// What it cannot do is change the video it was sent. If the feed hands a 2 GB
// phone a 1080p60 file, the phone's only options are to struggle or to fail,
// and a decoder that fails mid-playback is the frozen reel the whole caching
// effort was built to prevent.
//
// Until now nothing here could have known, because nothing measured what was
// uploaded. video_probe.go changed that. This file spends the measurement.
//
// ════════════════════════════════════════════════════════════════════════════════
// FIX BEFORE DROPPING
// ════════════════════════════════════════════════════════════════════════════════
//
// The obvious move — drop anything too large for the caller — is the wrong
// first move. It shrinks the feed for exactly the people whose feed is already
// hardest to fill, and a short feed was the problem a whole release went into
// solving.
//
// So the order is:
//
//	1. Adaptive streaming available?  Serve it. The player picks its own rung,
//	   which is a better decision than we could make from here.
//	2. A smaller pre-made rung available?  Point at that one.
//	3. Measured too large, with nothing smaller to offer?  Only then drop it.
//
// Steps 1 and 2 keep the item AND make it playable. Step 3 is the last resort,
// and on a healthy catalogue it should almost never fire.
//
// ════════════════════════════════════════════════════════════════════════════════
// UNKNOWN IS NOT SMALL
// ════════════════════════════════════════════════════════════════════════════════
//
// Every row uploaded before the probe existed has no measurement. Treating
// "unknown" as "small" is how an unmeasured 4K file reaches the phone least
// able to play it; treating it as "large" would empty the feed for older
// content that is almost certainly fine.
//
// We treat unknown as PLAYABLE — keep it — because the transcode worker
// eventually produces a ladder for everything, and the client already falls
// back gracefully. The gate at upload time is what stops new oversized content
// arriving; this is about routing what is already here.

import (
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"
)

// deviceMaxLongSideParam is the query parameter the app sends to say what it
// can decode, in pixels on the longer side. Absent means "no constraint",
// which is what every older client sends and must keep working.
const deviceMaxLongSideParam = "maxVideoSide"

// minDeviceLongSide floors whatever a client asks for. A caller claiming it can
// only handle 100px would filter its own feed down to nothing; far more likely
// a bug than a real device. 480 is the smallest rung anything is encoded at.
const minDeviceLongSide = 480

// parseDeviceMaxLongSide reads the caller's decode ceiling.
//
// Returns 0 for "did not say", which every caller before this parameter
// existed does, and which must mean no filtering at all.
func parseDeviceMaxLongSide(raw string) int {
	if raw == "" {
		return 0
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		return 0
	}
	if n < minDeviceLongSide {
		return minDeviceLongSide
	}
	return n
}

// variantLongSide reads the pixel height out of a variant label ("720p" → 720).
//
// The labels name the SHORT side of a landscape video, which for phone content
// is the number that matters: a "720p" rung is 1280x720 landscape or 720x1280
// portrait, and either way the decoder is doing 720p work. Comparing that to a
// long-side budget is slightly conservative, which is the right direction to be
// wrong in.
func variantLongSide(label string) int {
	n, err := strconv.Atoi(strings.TrimSuffix(strings.TrimSpace(label), "p"))
	if err != nil || n <= 0 {
		return 0
	}
	return n
}

// bestVariantWithin picks the largest rung that fits the budget.
//
// Largest rather than smallest: the point is the best picture the phone can
// actually handle, not the safest possible one. Returns "" when nothing fits or
// there are no variants, which sends the caller on to the next step.
func bestVariantWithin(variants VideoVariants, maxLongSide int) (label, url string) {
	if len(variants) == 0 || maxLongSide <= 0 {
		return "", ""
	}
	labels := make([]string, 0, len(variants))
	for l := range variants {
		labels = append(labels, l)
	}
	// Deterministic order so the same request always resolves the same way —
	// map iteration order would otherwise make this flap between rungs.
	sort.Slice(labels, func(i, j int) bool {
		return variantLongSide(labels[i]) > variantLongSide(labels[j])
	})
	for _, l := range labels {
		size := variantLongSide(l)
		if size > 0 && size <= maxLongSide && variants[l] != "" {
			return l, variants[l]
		}
	}
	return "", ""
}

// videoFit is what fitChallengeToDevice decided, so callers can log it and
// tests can assert on the reason rather than only the outcome.
type videoFit int

const (
	fitUnconstrained videoFit = iota // caller named no ceiling
	fitAdaptive                      // has HLS; the player chooses its own rung
	fitAlreadySmall                  // measured, and within budget
	fitDownshifted                   // swapped to a smaller pre-made rung
	fitUnknownSize                   // never measured — kept, see file header
	fitTooLarge                      // measured over budget, nothing smaller
)

func (f videoFit) String() string {
	switch f {
	case fitUnconstrained:
		return "unconstrained"
	case fitAdaptive:
		return "adaptive"
	case fitAlreadySmall:
		return "already-small"
	case fitDownshifted:
		return "downshifted"
	case fitUnknownSize:
		return "unknown-size"
	case fitTooLarge:
		return "too-large"
	}
	return "?"
}

// fitChallengeToDevice makes one challenge playable on a phone with the given
// decode ceiling, or reports that it cannot.
//
// Mutates c in place when it downshifts, because the caller is about to encode
// it and the whole point is to change what gets sent.
//
// Returns keep=false ONLY for step 3 — measured too large with nothing smaller
// to offer.
func fitChallengeToDevice(c *Challenge, maxLongSide int, measured videoDimensions) (fit videoFit, keep bool) {
	if maxLongSide <= 0 {
		return fitUnconstrained, true
	}

	// 1. Adaptive streaming beats anything we could pick from here: the player
	//    measures the actual connection and switches mid-stream.
	if c.HLSManifestURL != "" {
		return fitAdaptive, true
	}

	// 2. Already small enough — nothing to do.
	if measured.Width > 0 && measured.LongSide() <= maxLongSide {
		return fitAlreadySmall, true
	}

	// 3. Too big, but a smaller rung exists. Point at it and keep the item.
	if measured.Width > 0 {
		if _, url := bestVariantWithin(c.VideoVariants, maxLongSide); url != "" {
			c.VideoURL = url
			return fitDownshifted, true
		}
		// Measured over budget with nothing smaller. This is the only drop.
		return fitTooLarge, false
	}

	// 4. Never measured. Keep it — see the file header for why unknown is not
	//    treated as too large.
	return fitUnknownSize, true
}

// ════════════════════════════════════════════════════════════════════════════════
// THE FEED BOUNDARY
// ════════════════════════════════════════════════════════════════════════════════

// loadVideoDimensions batch-reads what the probe measured for a page of items.
//
// One query per page, matching how every other enrichment on this path works.
// Rows with no measurement are simply absent from the result, which the caller
// reads as "unknown" — see the file header for why that is not "too large".
func loadVideoDimensions(items []HomeFeedItem) map[string]videoDimensions {
	out := map[string]videoDimensions{}
	if db == nil || len(items) == 0 {
		return out
	}
	wantIDs := make([]int, 0, len(items))
	for _, it := range items {
		if it.Type != "challenge" || it.Challenge == nil {
			continue
		}
		if cid, err := strconv.Atoi(it.Challenge.ID); err == nil {
			wantIDs = append(wantIDs, cid)
		}
	}
	if len(wantIDs) == 0 {
		return out
	}
	placeholders := make([]string, len(wantIDs))
	args := make([]interface{}, len(wantIDs))
	for i, id := range wantIDs {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
		args[i] = id
	}
	rows, err := db.Query(`
		SELECT id, video_width, video_height
		FROM challenges
		WHERE id IN (`+strings.Join(placeholders, ",")+`)
		  AND video_width IS NOT NULL
		  AND video_height IS NOT NULL`, args...)
	if err != nil {
		// Older databases have not run migration 002 yet. Unknown everywhere
		// means no filtering, which is the pre-existing behaviour.
		log.Printf("loadVideoDimensions query error (treating all as unmeasured): %v", err)
		return out
	}
	defer rows.Close()
	for rows.Next() {
		var cid, w, h int
		if err := rows.Scan(&cid, &w, &h); err != nil {
			continue
		}
		out[strconv.Itoa(cid)] = videoDimensions{Width: w, Height: h}
	}
	return out
}

// applyDeviceFit makes a page playable on the phone that asked for it.
//
// Call at the feed boundary, AFTER the HLS URLs are filled in — step 1 of the
// decision reads HLSManifestURL, and an item enriched afterwards would be
// downshifted for no reason.
//
// Returns the page unchanged when the caller named no ceiling, which is what
// every client built before this parameter existed does.
func applyDeviceFit(items []HomeFeedItem, maxLongSide int) []HomeFeedItem {
	if maxLongSide <= 0 || len(items) == 0 {
		return items
	}
	dims := loadVideoDimensions(items)

	kept := make([]HomeFeedItem, 0, len(items))
	var dropped, downshifted int
	for _, it := range items {
		if it.Type != "challenge" || it.Challenge == nil {
			kept = append(kept, it) // cards and posts have no video to fit
			continue
		}
		fit, keep := fitChallengeToDevice(it.Challenge, maxLongSide, dims[it.Challenge.ID])
		switch fit {
		case fitDownshifted:
			downshifted++
		case fitTooLarge:
			dropped++
		}
		if keep {
			kept = append(kept, it)
		}
	}
	if dropped > 0 || downshifted > 0 {
		// Worth a line: a device whose feed is being visibly reshaped is
		// something to notice, and a rising drop count means the transcode
		// worker is falling behind rather than that phones got weaker.
		log.Printf("device fit (max %dpx): %d downshifted, %d dropped of %d",
			maxLongSide, downshifted, dropped, len(items))
	}
	return kept
}

// applyDeviceFitScored is applyDeviceFit for the ScoredItem slices the smart
// and explore pipelines carry.
//
// The Challenge pointers inside each ScoredItem.Item are shared with the
// wrappers built here, so a downshift lands in the caller's slice without a
// copy-back. Dropping, though, has to rebuild the slice — hence the second
// pass rather than reusing applyDeviceFit wholesale.
func applyDeviceFitScored(items []ScoredItem, maxLongSide int) []ScoredItem {
	if maxLongSide <= 0 || len(items) == 0 {
		return items
	}
	plain := make([]HomeFeedItem, len(items))
	for i, si := range items {
		plain[i] = si.Item
	}
	dims := loadVideoDimensions(plain)

	kept := make([]ScoredItem, 0, len(items))
	var dropped, downshifted int
	for _, si := range items {
		if si.Item.Type != "challenge" || si.Item.Challenge == nil {
			kept = append(kept, si)
			continue
		}
		fit, keep := fitChallengeToDevice(si.Item.Challenge, maxLongSide, dims[si.Item.Challenge.ID])
		switch fit {
		case fitDownshifted:
			downshifted++
		case fitTooLarge:
			dropped++
		}
		if keep {
			kept = append(kept, si)
		}
	}
	if dropped > 0 || downshifted > 0 {
		log.Printf("device fit (max %dpx): %d downshifted, %d dropped of %d",
			maxLongSide, downshifted, dropped, len(items))
	}
	return kept
}
