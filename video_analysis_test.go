package main

import (
	"encoding/json"
	"strings"
	"testing"
)

// The video analysis is optional at every step: a worker with no OCR binary
// sends no screen text, a worker predating it sends nothing at all, and an
// upload from before any of it exists has NULL in the column. So most of what
// matters here is that ABSENT and ZERO stay different — a video nobody looked
// at is not a silent, still, dark video, and treating it as one would push
// every un-analysed upload toward the same corner of the feed.

func TestMeasured_AbsentIsNotZero(t *testing.T) {
	if (&VideoAnalysis{}).Measured() {
		t.Error("an analysis with no passes reported itself as measured")
	}
	if (*VideoAnalysis)(nil).Measured() {
		t.Error("a nil analysis reported itself as measured")
	}
	// A pass that ran and genuinely found silence and stillness IS measured.
	a := &VideoAnalysis{Passes: []string{"shape"}}
	if !a.Measured() {
		t.Error("a shape pass that ran was not counted as measured")
	}
}

func TestAnalysisEnergy_OnlyAnswersWhenTheShapePassRan(t *testing.T) {
	// The energy value is built from cut rate and silence. Without the shape
	// pass both are zero, and returning 0 would say "this video is calm"
	// about a video nobody measured.
	if _, ok := analysisEnergy(nil); ok {
		t.Error("a nil analysis produced an energy value")
	}
	if _, ok := analysisEnergy(&VideoAnalysis{}); ok {
		t.Error("an empty analysis produced an energy value")
	}
	if _, ok := analysisEnergy(&VideoAnalysis{Passes: []string{"text"}}); ok {
		t.Error("an OCR-only analysis produced an energy value — it measured " +
			"no cuts and no audio, so it has nothing to say about energy")
	}
	if _, ok := analysisEnergy(&VideoAnalysis{Passes: []string{"shape"}}); !ok {
		t.Error("a shape pass produced no energy value")
	}
}

func TestAnalysisEnergy_FastAndLoudScoresHigherThanStillAndSilent(t *testing.T) {
	still, _ := analysisEnergy(&VideoAnalysis{
		Passes: []string{"shape"}, CutsPerMinute: 1, SpeechRatio: 0.05,
	})
	busy, _ := analysisEnergy(&VideoAnalysis{
		Passes: []string{"shape"}, CutsPerMinute: 40, SpeechRatio: 0.95,
	})
	if busy <= still {
		t.Errorf("a fast-cut noisy video scored %v and a still silent one %v", busy, still)
	}
	if busy > 1 || still < 0 {
		t.Errorf("energy escaped 0..1: still=%v busy=%v", still, busy)
	}
}

func TestAnalysisEnergy_CutsMatterMoreThanSound(t *testing.T) {
	// A static shot with wall-to-wall music is calm; a rapid edit is busy even
	// in silence. If sound dominated, every video with a backing track would
	// read as high energy.
	loudStill, _ := analysisEnergy(&VideoAnalysis{
		Passes: []string{"shape"}, CutsPerMinute: 0, SpeechRatio: 1,
	})
	quietFast, _ := analysisEnergy(&VideoAnalysis{
		Passes: []string{"shape"}, CutsPerMinute: 30, SpeechRatio: 0,
	})
	if quietFast <= loudStill {
		t.Errorf("a silent fast-cut video scored %v, no more than a loud static "+
			"one at %v", quietFast, loudStill)
	}
}

// ── Merging the two sets of tags ────────────────────────────────────────────

func TestMergeTags_CreatorLeads(t *testing.T) {
	// Order carries meaning: categoryFromTags takes the first tag that names a
	// category, so the creator being first is what keeps their choice winning
	// over a machine guess.
	got := mergeTags([]string{"music"}, []string{"sports", "fast cuts"})
	if len(got) == 0 || got[0] != "music" {
		t.Errorf("got %v — the creator's tag must come first", got)
	}
	if categoryFromTags(got) != "music" {
		t.Errorf("the merged list resolved to %q, want music", categoryFromTags(got))
	}
}

func TestMergeTags_NoDuplicatesWhenBothAgree(t *testing.T) {
	got := mergeTags([]string{"comedy", "prank"}, []string{"comedy", "talking"})
	seen := map[string]int{}
	for _, t := range got {
		seen[t]++
	}
	for tag, n := range seen {
		if n > 1 {
			t.Errorf("%q appears %d times in %v", tag, n, got)
		}
	}
	if len(got) != 3 {
		t.Errorf("got %v, want three distinct tags", got)
	}
}

func TestMergeTags_EitherSideEmpty(t *testing.T) {
	if got := mergeTags([]string{"a"}, nil); len(got) != 1 || got[0] != "a" {
		t.Errorf("no machine tags changed the creator's list to %v", got)
	}
	if got := mergeTags(nil, []string{"b"}); len(got) != 1 || got[0] != "b" {
		t.Errorf("no creator tags dropped the machine's: %v", got)
	}
	if got := mergeTags(nil, nil); len(got) != 0 {
		t.Errorf("two empty lists produced %v", got)
	}
}

// ── The text the matchers read ──────────────────────────────────────────────

func TestAnalysisText_JoinsWhatWasSeenAndHeard(t *testing.T) {
	a := &VideoAnalysis{ScreenText: "day 1 of", Speech: "welcome back everyone"}
	got := analysisText(a)
	if !strings.Contains(got, "day 1 of") || !strings.Contains(got, "welcome back") {
		t.Errorf("got %q, want both the screen text and the speech", got)
	}
}

func TestAnalysisText_HandlesEitherSideMissing(t *testing.T) {
	// The common case by far: OCR installed, speech not.
	if got := analysisText(&VideoAnalysis{ScreenText: "recipe"}); got != "recipe" {
		t.Errorf("screen text only gave %q", got)
	}
	if got := analysisText(&VideoAnalysis{Speech: "hello"}); got != "hello" {
		t.Errorf("speech only gave %q", got)
	}
	if got := analysisText(&VideoAnalysis{}); got != "" {
		t.Errorf("nothing found gave %q", got)
	}
	if got := analysisText(nil); got != "" {
		t.Errorf("a nil analysis gave %q", got)
	}
}

func TestAnalysisText_ReachesTheCategoryMatcher(t *testing.T) {
	// The point of the whole pipeline: a video whose only description is
	// burned into the picture should still land in the right category.
	a := &VideoAnalysis{Passes: []string{"text"}, ScreenText: "easy pasta recipe"}
	got := categoryForContent("", nil, "", "", analysisText(a))
	if got == "general" || got == "" {
		t.Errorf("a video captioned %q was categorised as %q — the text found on "+
			"screen never reached the matcher", a.ScreenText, got)
	}
}

// ── Storing ─────────────────────────────────────────────────────────────────

func TestStoreVideoAnalysis_IgnoresNothingAndNonsense(t *testing.T) {
	// Must not panic and must not touch the database. db is nil under test,
	// so reaching a query at all would fail loudly.
	storeVideoAnalysis("challenges", 1, nil)
	storeVideoAnalysis("challenges", 1, json.RawMessage(`not json`))
	storeVideoAnalysis("challenges", 1, json.RawMessage(`{}`))
	storeVideoAnalysis("challenges", 1, json.RawMessage(`{"passes":[]}`))
}

func TestVideoAnalysis_SurvivesTheRoundTrip(t *testing.T) {
	// The worker and the backend declare this struct separately on purpose —
	// they deploy independently — so the wire format is the only thing holding
	// them together and it has to survive being written and read back.
	in := VideoAnalysis{
		CutsPerMinute: 24, MotionScore: 0.4, Loudness: -14.2,
		SpeechRatio: 0.83, Brightness: 0.61,
		ScreenText: "day 3", Speech: "let us begin",
		AutoTags: []string{"talking"}, Passes: []string{"shape", "text"},
	}
	b, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	var out VideoAnalysis
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	if out.CutsPerMinute != in.CutsPerMinute || out.SpeechRatio != in.SpeechRatio ||
		out.ScreenText != in.ScreenText || out.Speech != in.Speech ||
		len(out.Passes) != len(in.Passes) {
		t.Errorf("round trip changed the reading:\n got %+v\nwant %+v", out, in)
	}
}
