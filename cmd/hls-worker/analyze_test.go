package main

import "testing"

// These tests exist because of a bug that shipped.
//
// Three of the four patterns this file checks were written from memory, looked
// entirely plausible, and matched nothing at all. Every video analysed in that
// period was recorded as having zero cuts, zero brightness and zero loudness —
// and nothing anywhere said so, because the pass still reported success and
// zero is a perfectly ordinary-looking number.
//
// So the fixtures below are REAL ffmpeg output, copied from an actual run
// rather than typed out. A test written from the same memory as the bug would
// have agreed with the bug.
//
// If ffmpeg ever changes these key names, these tests fail. That is the point.

// Captured from: ffmpeg -i in.mp4 -filter_complex
//   "[0:v]scdet=threshold=10,signalstats,metadata=print[v];
//    [0:a]ebur128=metadata=1,silencedetect=n=-30dB:d=0.5[a]"
//   -map "[v]" -map "[a]" -f null -
const (
	realCutMetadata   = `[Parsed_metadata_2 @ 0x55cc41f8a900] lavfi.scd.time=2`
	realCutLog        = `[scdet @ 0x55cc41f8a600] lavfi.scd.score: 75.781, lavfi.scd.time: 2`
	realNonCutFrame   = `[Parsed_metadata_2 @ 0x559285699b00] lavfi.scd.score=0.058`
	realLumaLine      = `[Parsed_metadata_2 @ 0x559285699b00] lavfi.signalstats.YAVG=122.09`
	realLoudnessLine  = `    I:         -15.4 LUFS`
	realLoudnessRange = `    LRA low:   -18.9 LUFS`
	realSilenceLine   = `[silencedetect @ 0x55d266ec0500] silence_end: 2.85719 | silence_duration: 0.570875`
	realSilenceStart  = `[silencedetect @ 0x55d266ec0500] silence_start: 2.28631`
)

func TestSceneCut_CountsCutsAndNotMovement(t *testing.T) {
	if !reSceneCut.MatchString(realCutMetadata) {
		t.Error("the metadata line marking a real cut was not recognised — " +
			"every video would read as having zero cuts")
	}
	// The one that caused the bug's twin. scdet scores EVERY frame, and on
	// continuously-moving footage nearly every score is above zero. Treating a
	// score as a cut turned a clip with no cuts at all into 327 of them.
	if reSceneCut.MatchString(realNonCutFrame) {
		t.Error("a per-frame score was counted as a cut — this reads ordinary " +
			"movement as rapid editing, on nearly every real video")
	}
}

func TestSceneCut_DoesNotCountTheSameCutTwice(t *testing.T) {
	// scdet logs its own line for each cut AND the metadata filter prints one.
	// Matching both doubles every count, which would quietly halve the
	// threshold at which a video reads as fast-cut.
	if reSceneCut.MatchString(realCutLog) {
		t.Errorf("scdet's own log line also matched:\n  %s\nBoth lines describe "+
			"ONE cut, so counting both doubles every reading", realCutLog)
	}
}

func TestYAVG_ReadsTheNamespacedKey(t *testing.T) {
	m := reYAVG.FindStringSubmatch(realLumaLine)
	if m == nil {
		t.Fatal("signalstats output was not recognised — brightness would be " +
			"zero for every video, and zero means pitch black")
	}
	if m[1] != "122.09" {
		t.Errorf("captured %q, want 122.09", m[1])
	}
}

func TestLoudness_ReadsTheEbur128Summary(t *testing.T) {
	m := reLoudness.FindStringSubmatch(realLoudnessLine)
	if m == nil {
		t.Fatal("ebur128's summary was not recognised. The old pattern looked " +
			"for loudnorm's JSON, which is a different filter this pass never runs")
	}
	if m[1] != "-15.4" {
		t.Errorf("captured %q, want -15.4", m[1])
	}
	// The summary block has several LUFS numbers in it. Only the integrated
	// one is the loudness of the video; picking up a range bound instead would
	// be wrong by a quiet but meaningful amount.
	if reLoudness.MatchString(realLoudnessRange) {
		t.Errorf("a loudness-range line also matched:\n  %s", realLoudnessRange)
	}
}

func TestSilence_ReadsTheDurationNotTheStart(t *testing.T) {
	m := reSilenceDur.FindStringSubmatch(realSilenceLine)
	if m == nil {
		t.Fatal("silencedetect output was not recognised")
	}
	if m[1] != "0.570875" {
		t.Errorf("captured %q, want 0.570875", m[1])
	}
	// silence_start carries a timestamp, not a length. Adding those up as if
	// they were durations would make a video look more silent the later its
	// quiet moments happen to fall.
	if reSilenceDur.MatchString(realSilenceStart) {
		t.Error("a silence_start timestamp was read as a duration")
	}
}

// ── Turning readings into tags ──────────────────────────────────────────────

func TestTagsFromAnalysis_DescribesWhatWasMeasured(t *testing.T) {
	fast := tagsFromAnalysis(videoAnalysis{
		Passes: []string{"shape"}, CutsPerMinute: 50, SpeechRatio: 0.9,
	})
	if !hasTag(fast, "fast cuts") {
		t.Errorf("50 cuts a minute produced %v, with nothing about the pace", fast)
	}
	if !hasTag(fast, "talking") {
		t.Errorf("a video that is 90%% not-silence produced %v", fast)
	}
}

func TestTagsFromAnalysis_SaysNothingAboutWhatItDidNotMeasure(t *testing.T) {
	// The property the whole file rests on: absent is not zero. A video nobody
	// looked at must not come back described as slow, silent and still.
	if got := tagsFromAnalysis(videoAnalysis{}); len(got) != 0 {
		t.Errorf("an unmeasured video was tagged %v", got)
	}
}

func hasTag(tags []string, want string) bool {
	for _, t := range tags {
		if t == want {
			return true
		}
	}
	return false
}
