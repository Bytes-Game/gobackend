package main

import (
	"sort"
	"strings"
	"testing"
)

// ════════════════════════════════════════════════════════════════════════════
// WHICH FILES A SOURCE GETS, DECIDED WITHOUT AN ENCODER
// ════════════════════════════════════════════════════════════════════════════
//
// Every bug this ladder has had was a DECISION bug: a rung silently dropped, a
// picture blown up to more pixels than it started with, two names for one
// file. None of them needed ffmpeg to find, and all of them shipped, because
// the only tests that could see them called ffmpeg and were skipped wherever
// it was not installed.
//
// planProgressive is that decision on its own, so these run everywhere.

// labelsOf is the plan as a plain list of names, in the order it was made.
func labelsOf(plan []plannedRendition) []string {
	out := make([]string, 0, len(plan))
	for _, p := range plan {
		out = append(out, p.rung.label)
	}
	return out
}

func planLabels(longSide, bitrate int) []string {
	return labelsOf(planProgressive(longSide, bitrate))
}

func has(labels []string, want string) bool {
	for _, l := range labels {
		if l == want {
			return true
		}
	}
	return false
}

func boxOf(plan []plannedRendition, label string) (int, bool) {
	for _, p := range plan {
		if p.rung.label == label {
			return p.box, true
		}
	}
	return 0, false
}

func ceilingOf(plan []plannedRendition, label string) (int, bool) {
	for _, p := range plan {
		if p.rung.label == label {
			return p.rung.maxBps, true
		}
	}
	return 0, false
}

// fatEnough is a bitrate above every ceiling on the ladder, so nothing is
// clamped by the source and each rung runs at its own number.
func fatEnough() int { return aboveEveryCeiling() }

func TestPlan_AFullSizeSourceGetsEveryRung(t *testing.T) {
	// A normal phone upload: 1920 on the long side, more bits than any rung
	// spends. Every rung has something to do.
	got := planLabels(1920, fatEnough())
	for _, want := range []string{"360p", "480p", "720p", "720p_hq"} {
		if !has(got, want) {
			t.Errorf("a 1920-wide source produced no %s; got %v", want, got)
		}
	}
	if len(got) != len(progressiveLadder) {
		t.Errorf("a source bigger and fatter than every rung should get all "+
			"%d of them; got %d: %v", len(progressiveLadder), len(got), got)
	}
}

func TestProgressive_ASmallSourceGetsBothItsRungs(t *testing.T) {
	// ══════════════════════════════════════════════════════════════════════
	// THE TRAP IN ADDING A SMALLER RUNG
	// ══════════════════════════════════════════════════════════════════════
	//
	// 640 is exactly 360p's picture size, and it is inside every other rung's
	// box as well. The planner lets the first rung to claim a size own it, so
	// if 360p is listed first it claims 640 — and the upload's ONLY rendition
	// comes out capped at 600k, where it used to get 1.5 Mbps.
	//
	// Nothing would have said so. The file exists, it plays, it is just
	// quietly much softer than it was before the rung was added.
	//
	// What we want instead: the good file at 1.5 Mbps, AND 360p underneath it
	// for a connection that cannot carry that.
	const small = 640
	plan := planProgressive(small, fatEnough())
	got := labelsOf(plan)

	if !has(got, "480p") {
		t.Errorf("a %d-wide source produced no 480p — its best rendition is "+
			"missing; got %v", small, got)
	}
	if !has(got, "360p") {
		t.Errorf("a %d-wide source produced no 360p — a slow link has nothing "+
			"to drop to; got %v", small, got)
	}
	// And the 480p really is the fuller one, not a second copy of the cheap
	// file under a bigger name.
	lo, okLo := ceilingOf(plan, "360p")
	hi, okHi := ceilingOf(plan, "480p")
	if okLo && okHi && hi <= lo {
		t.Errorf("480p was planned at %d bps against 360p's %d. They are "+
			"meant to be a real choice, not the same file twice.", hi, lo)
	}
}

func TestPlan_ASourceSmallerThanEveryRungStillGetsACheapOne(t *testing.T) {
	// ══════════════════════════════════════════════════════════════════════
	// THE HOLE UNDERNEATH THE SMALLEST RUNG
	// ══════════════════════════════════════════════════════════════════════
	//
	// 500 pixels is smaller than 360p's own box, so EVERY rung is reaching
	// down to it and every one of them lands on the same 500-wide picture.
	//
	// The old rule was "one rendition per picture size, first rung wins".
	// Under it this source got exactly one file — 480p, at 1.5 Mbps — and a
	// viewer on mobile data had nothing to fall back to, even though the
	// video is tiny. The picture being small does not make the file cheap.
	//
	// The rule now is "cheaper is worth making, dearer is not", so 360p is
	// added underneath. Reverting to first-wins turns this red.
	const tiny = 500
	got := planLabels(tiny, fatEnough())
	if !has(got, "360p") {
		t.Errorf("a %d-wide source produced no 360p, so a slow link has "+
			"nothing it can play; got %v", tiny, got)
	}
	if !has(got, "480p") {
		t.Errorf("a %d-wide source lost its fuller rendition; got %v", tiny, got)
	}

	// And the rule really is about price, not count: a source already leaner
	// than 360p's ceiling gets ONE file, because a second would be the same
	// bytes again.
	lean := planLabels(tiny, 400_000)
	if len(lean) != 1 {
		t.Errorf("a %d-wide source at 400 kbps got %d renditions (%v). "+
			"Clamped to its own rate they are all the same file.",
			tiny, len(lean), lean)
	}
}

func TestPlan_NeverPlansAPictureBiggerThanTheSource(t *testing.T) {
	// ffmpeg's "fit inside this box" does NOT mean "only shrink" — it will
	// happily grow a smaller source to fill the box. A real upload, 960x720,
	// came out of the 720p rung at 1280x960: more pixels than it started
	// with, no more detail, and a file 97% the size of the original instead
	// of a third of it.
	for _, longSide := range []int{320, 480, 500, 640, 720, 854, 960, 1080, 1280, 1920} {
		for _, bitrate := range []int{0, 400_000, 1_200_000, fatEnough()} {
			plan := planProgressive(longSide, bitrate)
			if len(plan) == 0 {
				t.Errorf("%dx source at %d bps got no renditions at all",
					longSide, bitrate)
			}
			bySize := map[int][]string{}
			for _, p := range plan {
				if p.box > longSide {
					t.Errorf("%dx source: %s was planned at a %d box, which "+
						"is bigger than the source",
						longSide, p.rung.label, p.box)
				}
				bySize[p.box] = append(bySize[p.box], p.rung.label)
			}
			for box, labels := range bySize {
				if len(labels) < 2 {
					continue
				}
				sort.Strings(labels)
				t.Logf("%dx @%d: box %d holds %v", longSide, bitrate, box,
					strings.Join(labels, ","))
			}
		}
	}
}

func TestPlan_ALabelMeansThePictureSizeItSays(t *testing.T) {
	// ══════════════════════════════════════════════════════════════════════
	// A NAME IS READ AS A SIZE, NOT JUST A BITRATE
	// ══════════════════════════════════════════════════════════════════════
	//
	// The app reads a rendition's name as BOTH what it costs and how many
	// pixels it has — see _labelRank and decodeRank in the app's
	// NetworkQualityService. A 640-wide file called "720p" is two lies at
	// once: it tells a low-RAM phone to skip a file it could easily have
	// decoded, and it tells the picker the file costs 2.5 Mbps when it does
	// not.
	//
	// For a source smaller than a rung there is no way round it — the picture
	// is the size it is, and the file has to be called something. But for a
	// source at least as big as every rung, which is what a phone actually
	// uploads, each name must mean exactly the size it says.
	plan := planProgressive(1920, fatEnough())
	for _, r := range progressiveLadder {
		box, ok := boxOf(plan, r.label)
		if !ok {
			t.Errorf("a 1920-wide source produced no %s at all", r.label)
			continue
		}
		if box != r.maxLongSide {
			t.Errorf("%s came out %d pixels wide from a source big enough for "+
				"its full %d. The app reads that name as %d pixels, so it "+
				"would judge this file by a size it does not have.",
				r.label, box, r.maxLongSide, r.maxLongSide)
		}
	}
}

func TestPlan_ALeanSourceIsNotStoredTwice(t *testing.T) {
	// A source leaner than several rungs' ceilings gets clamped to its own
	// rate at each of them — which makes those rungs the same file under
	// different names. They must collapse to one.
	//
	// 1280 wide at 400 kbps: 720p and 720p_hq are the same box, and once both
	// are clamped to 400k they are the same ceiling too.
	plan := planProgressive(1280, 400_000)
	got := labelsOf(plan)
	if has(got, "720p") && has(got, "720p_hq") {
		t.Errorf("a lean 1280-wide source produced both 720p and 720p_hq. "+
			"Clamped to its own rate they are byte-for-byte the same video; "+
			"got %v", got)
	}
	// Every ceiling in the plan is distinct-per-box, which is the same rule
	// stated as an invariant.
	seen := map[[2]int]string{}
	for _, p := range plan {
		k := [2]int{p.box, p.rung.maxBps}
		if prev, dup := seen[k]; dup {
			t.Errorf("%s and %s are both a %d box at %d bps — one file, two "+
				"names", prev, p.rung.label, p.box, p.rung.maxBps)
		}
		seen[k] = p.rung.label
	}
}

func TestPlan_AnUnknownBitrateClampsNothing(t *testing.T) {
	// 0 means the container did not declare a bitrate. That is "unknown", not
	// "zero bits", so no rung may be pulled down by it.
	plan := planProgressive(1920, 0)
	for _, p := range plan {
		for _, r := range progressiveLadder {
			if r.label != p.rung.label {
				continue
			}
			if p.rung.maxBps != r.maxBps {
				t.Errorf("%s ran at %d bps instead of its own %d, on a source "+
					"whose bitrate we do not know", r.label, p.rung.maxBps, r.maxBps)
			}
			if p.rung.crf != r.crf {
				t.Errorf("%s used the preserve quality target on a source "+
					"whose bitrate we do not know", r.label)
			}
		}
	}
}

func TestPlan_TheSmallestRungIsSomethingMobileDataCanCarry(t *testing.T) {
	// The whole reason 360p exists. The app will not choose a rendition
	// unless the link is a third again faster than the file, and it holds
	// 1 Mbps back to fetch the next reel. So the smallest rung has to fit
	// inside what is left on a real mobile connection.
	//
	// Field measurement: mobile data runs at 2–3 Mbps and dips below 2.
	const mobileBps = 2_600_000
	const readAheadReserveBps = 1_048_576 // NetworkQualityService's figure
	const headroom = 1.3                  // NetworkQualityService.bitrateHeadroom

	smallest := 0
	for _, r := range progressiveLadder {
		if smallest == 0 || r.maxBps < smallest {
			smallest = r.maxBps
		}
	}
	forPicture := mobileBps - readAheadReserveBps
	if float64(smallest)*headroom > float64(forPicture) {
		t.Errorf("the smallest rung costs %d bps, which needs %.0f bps of "+
			"link once the app's headroom is applied. A %d bps connection "+
			"has %d left for the picture after read-ahead, so there would be "+
			"nothing on this ladder it could play.",
			smallest, float64(smallest)*headroom, mobileBps, forPicture)
	}
}

func TestPlan_EveryRungIsReachable(t *testing.T) {
	// A rung nothing can ever produce is dead weight: encode time budgeted
	// for it, a name in the app's tables, and no file behind it. For each
	// rung there has to be SOME source shape that produces it.
	reachable := map[string]bool{}
	for _, longSide := range []int{320, 480, 640, 720, 854, 960, 1080, 1280, 1920, 3840} {
		for _, bitrate := range []int{0, 300_000, 700_000, 1_200_000, 2_000_000, 3_000_000, fatEnough()} {
			for _, l := range planLabels(longSide, bitrate) {
				reachable[l] = true
			}
		}
	}
	for _, r := range progressiveLadder {
		if !reachable[r.label] {
			t.Errorf("no source shape produces %s — it is on the ladder and "+
				"nothing will ever be encoded to it", r.label)
		}
	}
}
