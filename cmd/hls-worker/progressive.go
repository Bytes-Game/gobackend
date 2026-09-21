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
// It already reads a videoVariants map and picks one from the network and the
// phone's memory — that path predates this and had simply had nothing to pick
// from. Filling it in is enough.
//
// The one exception is a NEW label. The app's chooser has to know a label's
// rank and what speed it needs, so adding a rung here means adding it there
// too — see NetworkQualityService.bitrateNeededFor. A label the app does not
// know is a file nobody ever plays.

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
// Four rungs. The HLS ladder has a 240p one below these because a player
// switches between them mid-video and can drop to it for a few seconds; a
// progressive file is chosen once, before playback, so a rung nobody would
// deliberately pick for a whole video is a rung nobody picks.
//
// The four cover the range of connections rather than the range of screen
// sizes, because that is what actually varies here — every viewer is holding
// a phone, and their links are not alike. Two of them are the same 1280-wide
// picture at different bitrates for exactly that reason.
//
// 1080p is absent: on a phone, full-screen, under ninety seconds, it is not
// distinguishable from 720p, and device logs showed 1080p decoder sessions
// stalling for seconds on a mid-range chip.
// preserveCRF is the quality target for a rung whose ceiling has been pulled
// down to the source's own rate — see the clamp in buildProgressiveMP4s.
//
// Lower means "spend more to stay closer to what you were given", and 18 is
// the usual point at which an x264 re-encode stops being tellable from its
// input by eye. It is deliberately far below the ladder's own 22/24: those
// exist to bring a fat source DOWN, and reusing them on a lean one is the
// double-squeeze this whole path is built to avoid.
//
// In practice the ceiling does most of the work — held at the source's own
// rate, an encode at this quality target spends right up to it — so this is
// less "aim for CRF 18" than "do not throw anything away that the ceiling
// would have paid for".
const preserveCRF = 18

var progressiveLadder = []progressiveRendition{
	// Cellular and older phones. Small enough to arrive before somebody
	// gives up, large enough not to look broken.
	{label: "480p", maxLongSide: 854, crf: 24, maxBps: 1_500_000, audioBps: 96_000},
	// The default almost everybody gets. Full picture size at a rate a
	// middling connection can actually sustain.
	{label: "720p", maxLongSide: 1280, crf: 22, maxBps: 2_500_000, audioBps: 128_000},
	// ════════════════════════════════════════════════════════════════════════
	// SAME PICTURE SIZE, MORE BITS, FOR CONNECTIONS THAT CAN TAKE THEM
	// ════════════════════════════════════════════════════════════════════════
	//
	// The rung above exists because one measured phone dipped to 3.5 Mbps and
	// a 3.5 Mbps file has nothing spare there. That was the right fix for that
	// phone and the wrong way to decide it for everybody: the encoding ceiling
	// is a single global number, so tuning it to one link hands everyone else
	// the same compromise whether they need it or not.
	//
	// A ladder is how that gets decided per viewer instead. This is the same
	// 1280-wide picture with the bits a good connection can carry — 0.990
	// against the source where 2.5 Mbps scores 0.978 — and the app serves it
	// only to links it has measured as fast enough. A slow link never sees it
	// and a fast one is no longer capped by somebody else's dip.
	//
	// It is deliberately NOT 1080p. On a phone, full-screen, under ninety
	// seconds, the extra pixels are not visible and cost roughly double the
	// decode; device logs showed 1920x1080 sessions with render intervals in
	// the seconds. What a fast link is short of here is bits, not pixels.
	{label: "720p_hq", maxLongSide: 1280, crf: 20, maxBps: 3_500_000, audioBps: 128_000},
	// ════════════════════════════════════════════════════════════════════════
	// WHY THE SMALLEST RUNG IS LISTED LAST
	// ════════════════════════════════════════════════════════════════════════
	//
	// planProgressive walks this list in order, and the first rung to claim a
	// picture size owns it — see cheapestAtBox. So where a rung sits in this
	// list decides which NAME a small upload's file gets.
	//
	// A 640-wide upload is inside 360p's box and inside every other rung's box
	// too. Listed first, 360p would claim it and the upload's only rendition
	// would be capped at 600k. Listed last, the 480p rung claims it at 1.5 Mbps
	// and 360p is then added underneath as the cheap option — which is what we
	// wanted: the good file AND something a slow link can still play.
	//
	// TestProgressive_ASmallSourceGetsBothItsRungs pins that outcome, so
	// re-sorting this list turns it red rather than quietly halving the
	// quality of every small upload.
	//
	// ════════════════════════════════════════════════════════════════════════
	// THE RUNG FOR A REAL MOBILE CONNECTION
	// ════════════════════════════════════════════════════════════════════════
	//
	// For a long time the bottom of this ladder was 480p, and 480p costs
	// 1.5 Mbps of video plus 96k of audio — about 1.6 Mbps. The app will not
	// choose a rendition unless the link is a third again faster than the
	// file (see bitrateHeadroom), so 480p really asks for about 2.1 Mbps, and
	// it asks for it BEFORE anything is set aside to fetch the next reel.
	//
	// Mobile data in the field runs at 2–3 Mbps and dips below 2 routinely.
	// At 2.6 Mbps the 480p rung swallowed nearly the whole link; at 1.5 Mbps
	// it could not play at all — and there was nothing underneath it to fall
	// back to. The app's own picker said as much: its floor was the string
	// "480p", so on a very slow link it chose a file the link could not carry
	// and the video stopped.
	//
	// The reasoning in that picker was right — "a soft picture that plays
	// still beats a sharp one that stops" — and the ladder simply did not
	// give it anything soft enough to keep the promise.
	//
	// So: 640x360 at 600k video + 64k audio, about 0.66 Mbps. With headroom
	// that is roughly 0.9 Mbps for the picture, which leaves real room on a
	// 1.5 Mbps link instead of overrunning it.
	//
	// 640x360 and 600k are the HLS ladder's own 360p numbers (main.go), so
	// the two halves of this app agree about what "360p" looks like and what
	// it costs. The audio is 64k here rather than that ladder's 96k: 32 kbps
	// is 5% of this rung's whole budget, which is worth more to the picture
	// than it is to speech at this size, and it is what the 240p rung below
	// it already uses.
	//
	// Nobody on wifi ever sees this. It is a floor, not a change of default.
	{label: "360p", maxLongSide: 640, crf: 26, maxBps: 600_000, audioBps: 64_000},
	// ════════════════════════════════════════════════════════════════════════
	// THE SAME TWO PICTURES, IN THE NEWER LANGUAGE
	// ════════════════════════════════════════════════════════════════════════
	//
	// These are not extra sizes. They are the 480p and 720p pictures above,
	// described in H.265 instead of H.264, which takes about a third fewer
	// bits for the same thing.
	//
	//	480p   1.5 Mbps  ->  480p_hevc   0.9 Mbps
	//	720p   2.5 Mbps  ->  720p_hevc   1.5 Mbps
	//
	// Read the second line again: a phone that can decode H.265 gets the FULL
	// 720p picture for less than 480p costs today. That is the win, and it is
	// the one thing on this whole page that makes a video smaller without
	// making it look worse.
	//
	// WHY ONLY TWO
	//
	// Encoding H.265 is slower, and the worker already runs out of its twelve
	// minutes with a queue left over. So these go where they buy the most:
	// the two picture sizes people actually watch on a phone.
	//
	// 360p is left alone because it is already tiny — a third off 600k saves
	// 200k, and it has to stay H.264 anyway, because it is the floor every
	// device must be able to play. 720p_hq is left alone because it is the
	// rung for links with bandwidth to spare, and saving bits there is
	// solving a problem that rung does not have.
	//
	// WHY THEY ARE LAST, AND WHY THAT IS NOT WHAT KEEPS THEM SAFE
	//
	// A rung reaching down to a small source is only made when it is cheaper
	// than what is already planned at that size (see planProgressive). An
	// H.265 rung is ALWAYS cheaper than its H.264 twin — that is the entire
	// point — so listing these first would have let each one push its own
	// fallback off the ladder, leaving older phones with nothing at that size.
	//
	// What actually prevents it is that planProgressive plans all the H.264
	// rungs BEFORE any H.265 one, in an order it builds for itself rather
	// than reading off this list. By the time an H.265 rung is considered,
	// the fallback it sits beside is already made and cannot be displaced.
	//
	// The cheaper-wins rule is applied per codec too, which is the right
	// model and stops the two families interfering in either direction. But
	// it is not what is load-bearing: cutting it leaves the tests green,
	// because the ordering has already done the work. Cutting the ordering
	// turns them red. Said plainly, because a comment naming the wrong guard
	// is worse than no comment.
	{label: "480p_hevc", maxLongSide: 854, crf: 28, maxBps: 900_000, audioBps: 96_000, hevc: true},
	{label: "720p_hevc", maxLongSide: 1280, crf: 26, maxBps: 1_500_000, audioBps: 128_000, hevc: true},
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
// A LEAN SOURCE IS HELD, NOT SQUEEZED — AND NOT SKIPPED
// ════════════════════════════════════════════════════════════════════════════
//
// Re-encoding is lossy, so a file already carrying no more bits than its
// picture needs has nothing left to strip: squeezing it to a rung's ceiling
// spends a generation to arrive back where it started. That much was right,
// and this used to answer it by encoding nothing at all and serving the
// upload as it arrived.
//
// Serving the upload turned out to be the expensive half of that trade. What
// comes off a phone is whatever its camera chose — any frame rate, any
// profile, any structure — and "small enough" is not the same question as
// "cheap to play". A file can sit well inside a ceiling and still be harder
// for a mid-range phone to decode than anything this encoder produces. On top
// of that a skipped video had no rungs at all, so a viewer whose connection
// dipped had nothing to drop to, and the one file on offer was one nobody
// here had ever inspected.
//
// So: encode every video, and let a lean one keep its own rate.
//
//	ceiling = min(rung ceiling, what the source already spends)
//
// Where that clamp bites, the quality target goes up too (preserveCRF), which
// turns the pass from "squeeze this to the ceiling" into "hold this picture at
// the size it already is". A 1.6 Mbps upload comes back about 1.6 Mbps.
//
// The generation loss that argument was built on is real but small here, and
// it is paid against a gain: x264 at -preset medium is a better encoder than
// the fixed-function one in a phone, so the same bitrate buys a bit more
// picture. Held at the source's own rate, the two roughly cancel.
//
// The clamp also settles which rungs are worth making without a second rule.
// Nothing above what the source already spends can add information back, so
// two rungs that share a picture size collapse to one for a lean source — the
// dedup below sees the same box at the same (clamped) ceiling and drops the
// repeat. A fat source is untouched by any of this and still gets the full
// ladder at each rung's own ceiling.

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
	// hevc picks H.265 instead of H.264 for this rung.
	//
	// ════════════════════════════════════════════════════════════════════
	// THE SAME PICTURE FOR A THIRD FEWER BITS
	// ════════════════════════════════════════════════════════════════════
	//
	// H.264 is from 2003. Every device made since understands it, which is
	// why it is what we serve. H.265 is the newer way of describing the
	// same pictures, and it needs roughly 35% fewer bits to describe them
	// equally well.
	//
	// That is the whole point for this app. Fewer bits is a video that
	// starts sooner and does not stop on mobile data, WITHOUT the picture
	// getting worse — which is the trade every other lever here forces.
	//
	// The catch is that older phones cannot decode it. So these rungs are
	// added ALONGSIDE the H.264 ones, never instead of them: a phone that
	// can decode H.265 is served the small file, and a phone that cannot
	// is served exactly what it gets today. Nobody is left without
	// something to play. That is what TikTok does, and it is only possible
	// because the app's chooser already asks "can this device handle this
	// file" before every pick.
	hevc bool
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
		// Smallest picture that still does not shrink this source, and among
		// rungs of that size the LOWEST ceiling.
		//
		// The lowest, because this answers "is the original already as lean as
		// anything we would make". If a source is under even our smallest
		// option at its size, every rung would just re-encode it to roughly
		// what it already is and lose a generation doing it. Comparing against
		// the highest ceiling instead would leave fat sources untouched.
		if !found ||
			r.maxLongSide < best.maxLongSide ||
			(r.maxLongSide == best.maxLongSide && r.maxBps < best.maxBps) {
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
// runnerCapSeconds bounds ONE job's encode so a single video cannot outlive
// the runner window, whatever the backend asks for.
//
// This is a different concern from the product limit and deliberately a
// different number. The product limit is how long a reel may be, and it
// arrives with the job. This is how long a transcode may take, and it belongs
// to whatever machine the worker happens to be on.
//
// Four minutes, from the numbers that bound it: one job has an eight-minute
// ceiling (-job-timeout), and the five-rung ladder costs roughly 1.3x the
// running time, so four minutes of video is about five and a half of encode.
//
// It MUST stay above the product limit. This value used to be 90 seconds,
// chosen when reels were capped at 60 — and when the product limit became
// three minutes, that 90 quietly cut every longer video in half without
// anything saying so. A cap below the limit is not a safety net, it is a
// second limit nobody wrote down.
const runnerCapSeconds = 240

// cutSeconds is the -t value for one encode: the product limit the backend
// sent, bounded by what one job may spend on the runner.
//
// A limit of zero means the backend did not send one — an older deploy — and
// falls back to the runner bound alone rather than cutting nothing, because
// the reason the bound exists does not go away when the backend is old.
func cutSeconds(maxSeconds int) int {
	if maxSeconds <= 0 || maxSeconds > runnerCapSeconds {
		return runnerCapSeconds
	}
	return maxSeconds
}

// durationCutArgs returns the ffmpeg arguments that stop an encode at
// [cutSeconds].
//
// The upload gate refuses over-long video, so in the normal case this cuts
// nothing — the file is already inside the limit and -t never fires. It is
// for the cases the gate cannot cover: video that predates it, a file whose
// length it could not measure and therefore let through, and an app old
// enough not to report one.
//
// Applied on the ENCODE rather than as a separate trim pass. Both encoders
// re-encode from the source anyway, so this costs nothing and lands on the
// exact second. Cutting first with a stream copy would be free but would land
// on the nearest keyframe instead, which is a second or two out in either
// direction — and "either direction" includes over the limit.
func durationCutArgs(maxSeconds int) []string {
	return []string{"-t", strconv.Itoa(cutSeconds(maxSeconds))}
}

// plannedRendition is one file we have decided to make: the rung, already
// adjusted for this source, and the picture box it will be encoded into.
type plannedRendition struct {
	rung progressiveRendition
	box  int
}

// planProgressive decides which renditions a source of this shape should get,
// before any encoding happens.
//
// Split out from the encoder on purpose. Every bug this ladder has had was a
// decision bug — a rung silently dropped, a picture blown up, two names for
// one file — and none of them needed ffmpeg to find. A pure function can be
// checked on every push; a function that shells out to an encoder only gets
// checked where ffmpeg happens to be installed.
//
// longSide is the source's larger side in pixels. bitrate is what the source
// already spends, or 0 when the container did not say.
func planProgressive(longSide, bitrate int) []plannedRendition {
	// Exactly this picture size at exactly this ceiling, in this codec.
	//
	// The codec is part of the key because an H.265 rung is a different file
	// from an H.264 one even at identical numbers — half the devices can play
	// one and not the other.
	type rendition struct {
		box    int
		maxBps int
		hevc   bool
	}
	madeExact := map[rendition]bool{}
	// The CHEAPEST ceiling already planned at each picture size.
	//
	// It used to be a plain yes/no, which was fine while every rung at a
	// given size cost about the same. It stopped being fine when 360p joined
	// the ladder: a 640-wide upload would have had its one and only rendition
	// capped at 600k, because the first rung to claim the size owned it
	// outright and nothing dearer could follow.
	//
	// Cheapest, because a cheaper option at a size we already cover is
	// exactly what a slow connection needs, and a DEARER one at that size is
	// what we do not want — it would be the same picture under a bigger
	// rung's name, which the app reads as both more bits AND more pixels.
	// Keyed by codec as well as size. An H.265 rung is always cheaper than
	// its H.264 twin — that is what H.265 is FOR — so letting them share a
	// bucket would let each new rung delete the fallback it was meant to sit
	// beside, and older phones would be left with nothing at that size.
	//
	// Per codec, they cannot touch each other: H.264 competes with H.264,
	// H.265 with H.265.
	//
	// This is the right model rather than the active guard — the ordering
	// below already makes sure a fallback exists before any H.265 rung is
	// considered. Keep both: the ordering is what holds today, and this is
	// what keeps holding if the ordering is ever rearranged.
	type boxCodec struct {
		box  int
		hevc bool
	}
	cheapestAtBox := map[boxCodec]int{}

	// ════════════════════════════════════════════════════════════════════════
	// H.264 FIRST, THEN H.265 — AND NOT BY ACCIDENT OF THE LIST
	// ════════════════════════════════════════════════════════════════════════
	//
	// An H.265 rung is only worth making when it comes out SMALLER than the
	// H.264 file at the same picture size. That answer does not exist until
	// the H.264 rungs have been planned, so they have to go first.
	//
	// Built here rather than trusted to the order of the ladder literal. A
	// rule that lives in how a list happens to be sorted is a rule the next
	// person deletes by tidying, and this one decides whether older phones
	// have anything to play.
	ordered := make([]progressiveRendition, 0, len(progressiveLadder))
	for _, r := range progressiveLadder {
		if !r.hevc {
			ordered = append(ordered, r)
		}
	}
	for _, r := range progressiveLadder {
		if r.hevc {
			ordered = append(ordered, r)
		}
	}

	plan := make([]plannedRendition, 0, len(progressiveLadder))
	for _, r := range ordered {
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
		// ════════════════════════════════════════════════════════════════════
		// NEVER AIM ABOVE WHAT THE SOURCE ALREADY SPENDS
		// ════════════════════════════════════════════════════════════════════
		//
		// Encoding a 1.6 Mbps upload with a 3.5 Mbps ceiling does not make it
		// a 3.5 Mbps video. The detail is not there to find; the encoder just
		// spends longer describing the same picture. So the ceiling is the
		// lower of what this rung allows and what the source actually has.
		//
		// And where the clamp bites, the quality target rises with it. The
		// rung's own crf is chosen to squeeze a fat source DOWN to its
		// ceiling; applied to a source already at that rate it would squeeze
		// again, which is the generation loss the old skip existed to avoid.
		// preserveCRF asks the encoder to hold what is there instead, and the
		// ceiling stops it running long on hard content.
		if bitrate > 0 && bitrate < r.maxBps {
			r.maxBps = bitrate
			r.crf = preserveCRF
		}
		// Two rungs that landed on the same box AND the same ceiling would be
		// the same file under two names — identical bytes, twice the CPU,
		// twice the storage, and a chooser picking between things that do not
		// differ. Same box at a different ceiling is a real choice and is
		// kept.
		//
		// Read AFTER the clamp above, so two rungs sharing a picture size
		// collapse into one whenever the source is leaner than both of their
		// ceilings — they would otherwise be the same file twice.
		key := rendition{box: box, maxBps: r.maxBps, hevc: r.hevc}
		bc := boxCodec{box: box, hevc: r.hevc}

		// ════════════════════════════════════════════════════════════════
		// AN H.265 RUNG THAT SAVES NOTHING IS NOT WORTH ENCODING
		// ════════════════════════════════════════════════════════════════
		//
		// H.265 exists here to make a big file smaller. When the source is
		// already leaner than the H.264 ceiling, both rungs get clamped to
		// the source's own rate and the H.265 one saves nothing at all —
		// it is a second file, the same size, that half the devices cannot
		// play, for an encode that costs more than the H.264 one did.
		//
		// A 500-wide upload at 400 kbps is the case: 480p and 480p_hevc
		// both land at 400 kbps. There is nothing to shrink.
		//
		// So the test is the honest one — is this actually smaller than
		// what we are already serving at this size?
		if r.hevc {
			if h264, ok := cheapestAtBox[boxCodec{box: box, hevc: false}]; ok && r.maxBps >= h264 {
				continue
			}
		}
		if box < r.maxLongSide {
			// Clamped: this rung is reaching DOWN to a source smaller than
			// itself, so its name already promises more picture than there
			// is. Worth doing only if it is CHEAPER than anything else we
			// are making at this size — a smaller file of the same picture,
			// for a connection that cannot carry the one we have.
			//
			// Anything at or above what is already planned here is refused.
			// It would be the same picture again, no better, under a name
			// that claims more pixels than the file has.
			if cheap, ok := cheapestAtBox[bc]; ok && r.maxBps >= cheap {
				continue
			}
		} else if madeExact[key] {
			// Not clamped, and something already made this exact size at this
			// exact ceiling. Same file under two names.
			continue
		}

		madeExact[key] = true
		if cheap, ok := cheapestAtBox[bc]; !ok || r.maxBps < cheap {
			cheapestAtBox[bc] = r.maxBps
		}
		plan = append(plan, plannedRendition{rung: r, box: box})
	}
	return plan
}

// buildProgressiveMP4s encodes our renditions and returns label → local path.
//
// Never fatal, and never partial in a way the caller has to reason about: a
// rendition that fails is left out of the map, and the caller serves whatever
// did work — down to nothing at all, which is exactly today's behaviour.
func buildProgressiveMP4s(ctx context.Context, src, outDir string, maxSeconds int) map[string]string {
	made := map[string]string{}
	longSide, bitrate, ok := sourceShape(ctx, src)
	if !ok {
		log.Printf("progressive: could not measure %s, skipping our own encode", src)
		return made
	}
	hasAudio := probeHasAudio(src)

	// ════════════════════════════════════════════════════════════════════════
	// HOW LEAN THE SOURCE ALREADY IS
	// ════════════════════════════════════════════════════════════════════════
	//
	// A rung must never aim above this, because bits the source never spent
	// cannot be recovered by spending them now. Zero means the container did
	// not declare a bitrate, which is not "zero bits" — it is "unknown", so no
	// rung gets clamped and every one runs at its own ceiling.
	//
	// The old rule read the same measurement and returned here with nothing
	// encoded. See the section comment above for why serving the upload
	// untouched turned out to cost more than the generation it saved.
	if r, ok := rungForSize(longSide); ok && bitrate > 0 && bitrate <= r.maxBps {
		log.Printf("progressive: %s is %d-tall-side at %.2f Mbps, already "+
			"inside the %s ceiling of %.2f Mbps — holding it at its own rate",
			filepath.Base(src), longSide, float64(bitrate)/1e6,
			r.label, float64(r.maxBps)/1e6)
	}

	for _, p := range planProgressive(longSide, bitrate) {
		r := p.rung
		out := filepath.Join(outDir, r.label+".mp4")
		if err := encodeProgressive(ctx, src, out, r, p.box, hasAudio, maxSeconds); err != nil {
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
// hevcVideoArgs is the H.265 half of encodeProgressive.
//
// ════════════════════════════════════════════════════════════════════════════
// THE ONE LINE THAT DECIDES WHETHER APPLE PLAYS THIS AT ALL
// ════════════════════════════════════════════════════════════════════════════
//
//	-tag:v hvc1
//
// H.265 inside an MP4 can be labelled two ways: hev1 or hvc1. They describe
// the same video. ffmpeg writes hev1 by default. Apple plays ONLY hvc1 — an
// hev1 file opens on an iPhone to a black screen, usually with the sound
// still playing.
//
// It is the classic way to ship broken H.265, and it fails in the worst shape
// this repo knows: the encode succeeds, the file uploads, the worker logs
// success, the storage bill goes up, Android plays it perfectly, and only
// Apple users see a black rectangle. Nothing anywhere says why.
//
// TestProgressive_HevcIsTaggedForApple reads the built command and fails
// without it.
//
// ════════════════════════════════════════════════════════════════════════════
// THE REST IS ABOUT BEING PLAYABLE, NOT ABOUT BEING SMALL
// ════════════════════════════════════════════════════════════════════════════
//
// profile main (8-bit): Main10 carries more colour depth, and a decoder built
// for Main refuses it outright. Phones that do H.265 at all nearly always do
// Main; Main10 is a narrower set. The source is 8-bit video off a phone
// camera, so there is nothing here to gain and devices to lose.
//
// preset fast, not medium: x265 is slower than x264, and the worker already
// runs out of its twelve minutes with a queue left over. fast still
// compresses far better than x264 at any preset. medium would buy a few
// percent for encode time this worker does not have.
//
// The CRF numbers are NOT x264's scale. x265 lands about four to six higher
// for the same visible quality, which is why these read 28 and 26 beside the
// H.264 rungs' 24 and 22. Same picture quality, different unit.
func hevcVideoArgs(r progressiveRendition) []string {
	return []string{
		"-c:v", "libx265",
		"-crf", fmt.Sprintf("%d", r.crf),
		"-maxrate", fmt.Sprintf("%d", r.maxBps),
		"-bufsize", fmt.Sprintf("%d", r.maxBps),
		"-preset", "fast",
		"-profile:v", "main",
		// Apple plays hvc1 and not hev1. See above. Do not remove.
		"-tag:v", "hvc1",
		// x265 prints a banner and per-frame stats to stderr, which buries the
		// real ffmpeg error on the rare occasion there is one.
		"-x265-params", "log-level=error",
	}
}

// scaleFilter fits the picture inside a square box without ever enlarging it.
//
// Shared by both codecs on purpose. It carries two fixes that were each paid
// for once — see the H.264 branch below for the full account — and a copy of
// it that drifted would re-introduce them for one codec only, which is the
// hardest kind of bug to notice.
func scaleFilter(box int) string {
	return fmt.Sprintf(
		"scale=w=%d:h=%d:force_original_aspect_ratio=decrease:force_divisible_by=2,fps=30",
		box, box)
}

// progressiveArgs builds the ffmpeg command for one rendition.
//
// Split out from running it so the command can be CHECKED without ffmpeg
// installed. Two of the things that matter most about it — the hvc1 tag that
// decides whether Apple plays H.265 at all, and the shape settings the two
// codecs must keep in step — are invisible from the outside: a wrong one
// produces a file that encodes, uploads and plays on the machine that made
// it. Only some viewers see the failure, and nothing logs it.
//
// So they are read off this list on every push, by tests that need no encoder.
func progressiveArgs(src, out string, r progressiveRendition, box int, hasAudio bool, maxSeconds int) []string {
	args := []string{"-y", "-i", src}
	args = append(args, durationCutArgs(maxSeconds)...)
	args = append(args, "-map", "0:v:0")
	if hasAudio {
		args = append(args, "-map", "0:a:0")
	}
	if r.hevc {
		args = append(args, hevcVideoArgs(r)...)
		args = append(args,
			"-vf", scaleFilter(box),
			// Same keyframe spacing, pixel format and index placement as the
			// H.264 rungs, and for the same reasons — see the block below.
			// TestProgressive_BothCodecsAreShapedTheSame keeps them together.
			"-g", "30",
			"-keyint_min", "30",
			"-pix_fmt", "yuv420p",
			"-movflags", "+faststart",
		)
	} else {
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
			"-vf", scaleFilter(box),
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
	}
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
	return args
}

func encodeProgressive(ctx context.Context, src, out string, r progressiveRendition, box int, hasAudio bool, maxSeconds int) error {
	args := progressiveArgs(src, out, r, box, hasAudio, maxSeconds)
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
