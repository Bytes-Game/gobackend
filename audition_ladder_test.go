package main

import (
	"strconv"
	"strings"
	"testing"
)

// The ladder decides whether somebody's upload gets a bigger audience or stops
// here, so the parts worth pinning are the ones where being wrong is silent:
// the arithmetic of a rung, what happens when the ladder has nothing to say,
// and the hand-built SQL. All of these run without a database.

// ── The shape of the ladder ─────────────────────────────────────────────────

func TestLadderTotalMatchesTheViewTarget(t *testing.T) {
	// Plenty of code outside the ladder still asks "has this video been
	// measured yet" by comparing against auditionViewTarget. If the rungs stop
	// adding up to it, those two ideas quietly drift apart and nothing
	// complains.
	if got := auditionLadderTotal(); got != auditionViewTarget {
		t.Errorf("the rungs add up to %d views but auditionViewTarget is %d — "+
			"one of them was changed without the other", got, auditionViewTarget)
	}
}

func TestLadderStartsSmall(t *testing.T) {
	// The whole saving comes from the first rung being cheap. A ladder whose
	// first rung is most of the budget saves nothing, which would be a real
	// regression that still builds, still passes every other test, and shows up
	// only as a backlog months later.
	if len(auditionLadder) < 2 {
		t.Fatal("a ladder with fewer than two rungs is just the old flat rule")
	}
	first := auditionLadder[0].views
	if first >= auditionLadderTotal()/2 {
		t.Errorf("the first rung is %d of %d views — too big to save anything, "+
			"since every video pays it whether it is any good or not",
			first, auditionLadderTotal())
	}
}

func TestAuditionRungFor(t *testing.T) {
	if _, ok := auditionRungFor(0); !ok {
		t.Error("rung 0 should exist")
	}
	if _, ok := auditionRungFor(len(auditionLadder)); ok {
		t.Error("a stage past the end of the ladder means the video finished it, " +
			"and must not report as a real rung")
	}
	if _, ok := auditionRungFor(-1); ok {
		t.Error("a negative stage is corrupt data and must not report as a rung")
	}
}

// ── How far through a rung a video is ───────────────────────────────────────

func TestRungProgress_CountsOnlyTheViewsThisRungEarned(t *testing.T) {
	// A video promoted to rung 1 already has 60 views. Those belong to the rung
	// it passed. If they counted again, a promoted video would look finished
	// the moment it arrived and would get no push at all on the audience it
	// just earned.
	stage := 1
	rung, ok := auditionRungFor(stage)
	if !ok {
		t.Skip("ladder has only one rung")
	}
	start := auditionLadder[0].views

	p, under := auditionRungProgress(stage, start, start)
	if !under || p != 0 {
		t.Errorf("a rung that just began = progress %v, under audition %v; want 0, true", p, under)
	}

	half := start + rung.views/2
	p, under = auditionRungProgress(stage, half, start)
	if !under {
		t.Error("halfway through a rung is still under audition")
	}
	if p < 0.4 || p > 0.6 {
		t.Errorf("halfway through a rung reported progress %v, want about 0.5", p)
	}
}

func TestRungProgress_AFullRungIsWaitingForItsVerdict(t *testing.T) {
	rung := auditionLadder[0]
	_, under := auditionRungProgress(0, rung.views, 0)
	if under {
		t.Error("a video that finished its rung is waiting for a verdict, not " +
			"still gathering views — it must stop drawing the new-video push")
	}
}

func TestRungProgress_SurvivesAViewCountGoingBackwards(t *testing.T) {
	// Views are recounted from two places and a correction can move the number
	// down. That must not produce a negative push, which would actively bury
	// the video it was meant to help.
	p, under := auditionRungProgress(0, 5, 50)
	if !under {
		t.Error("a video whose view count dipped is still under audition")
	}
	if p < 0 {
		t.Errorf("progress %v is negative, which would invert the new-video push", p)
	}
}

// ── What the feed sees ──────────────────────────────────────────────────────

// withRoster installs a roster for one test and puts back whatever was there.
func withRoster(t *testing.T, r map[string]auditionRosterEntry) {
	t.Helper()
	old := auditionRoster.Load()
	auditionRoster.Store(&r)
	t.Cleanup(func() { auditionRoster.Store(old) })
}

func TestStanding_FallsBackToTheOldRuleWhenNothingIsLoaded(t *testing.T) {
	// This is the safety net for the seconds after a boot and for any database
	// blip. Getting it wrong the other way — reporting "not under audition" —
	// would switch new-video promotion off across the whole app without a
	// single error in the log.
	old := auditionRoster.Load()
	auditionRoster.Store(nil)
	t.Cleanup(func() { auditionRoster.Store(old) })

	if _, under := auditionStanding("challenge", "1", 10); !under {
		t.Error("with no roster loaded, a 10-view video must still count as under audition")
	}
	if _, under := auditionStanding("challenge", "1", auditionViewTarget); under {
		t.Error("with no roster loaded, a video past the view target must not count as under audition")
	}
}

func TestStanding_ContentThatIsNotAChallengeKeepsTheOldRule(t *testing.T) {
	// Only challenges ride the ladder. Everything else must behave exactly as
	// it did before this file existed.
	withRoster(t, map[string]auditionRosterEntry{})
	if _, under := auditionStanding("post", "1", 10); !under {
		t.Error("a 10-view post must still count as under audition")
	}
}

func TestStanding_AVideoMissingFromTheRosterIsDone(t *testing.T) {
	// Absent from a loaded roster means graduated or retired. Either way it has
	// had its answer and must stop taking free slots, even though its view
	// count is still under the old flat target.
	withRoster(t, map[string]auditionRosterEntry{"other": {stage: 0}})
	if _, under := auditionStanding("challenge", "retired-one", 10); under {
		t.Error("a video the ladder has finished with must not keep drawing " +
			"audition slots just because its view count is low")
	}
}

func TestStanding_PushRestartsWhenAVideoIsPromoted(t *testing.T) {
	if len(auditionLadder) < 2 {
		t.Skip("ladder has only one rung")
	}
	start := auditionLadder[0].views
	withRoster(t, map[string]auditionRosterEntry{
		"promoted": {stage: 1, stageViews: start},
	})
	p, under := auditionStanding("challenge", "promoted", start)
	if !under {
		t.Fatal("a just-promoted video is under audition")
	}
	if p != 0 {
		t.Errorf("a just-promoted video reported progress %v, want 0 — it earned "+
			"a bigger audience and should be pushed to go and find it", p)
	}
}

// ── The verdict ─────────────────────────────────────────────────────────────

func TestVerdict_NobodyIsCutBeforeThereIsABar(t *testing.T) {
	// The most important behaviour in the file. On a young app there are not
	// enough scored videos to say what "good enough" means, so nothing is cut.
	// If this ever flipped, a brand-new app would start retiring uploads on a
	// standard invented from four videos.
	state, next := auditionVerdict(0, 60, 0, 0.0001, 0.9, false)
	if state != auditionStateActive {
		t.Errorf("with no bar set, a very low score got %q, want %q", state, auditionStateActive)
	}
	if next != 1 {
		t.Errorf("next rung = %d, want 1", next)
	}
}

func TestVerdict_BelowTheBarStopsHere(t *testing.T) {
	state, next := auditionVerdict(0, 60, 0, 0.2, 0.35, true)
	if state != auditionStateRetired {
		t.Errorf("a score under the bar got %q, want %q", state, auditionStateRetired)
	}
	if next != 0 {
		t.Errorf("a retired video moved to rung %d — it must stay on the rung it "+
			"was judged on, so a second chance later starts from the bottom", next)
	}
}

func TestVerdict_AboveTheBarMovesUp(t *testing.T) {
	state, next := auditionVerdict(0, 60, 0, 0.9, 0.35, true)
	if state != auditionStateActive {
		t.Errorf("a score over the bar got %q, want %q", state, auditionStateActive)
	}
	if next != 1 {
		t.Errorf("next rung = %d, want 1", next)
	}
}

func TestVerdict_TheTopOfTheLadderIsTheEnd(t *testing.T) {
	last := len(auditionLadder) - 1
	state, _ := auditionVerdict(last, auditionViewTarget, 0, 0.9, 0.35, true)
	if state != auditionStateGraduated {
		t.Errorf("finishing the last rung got %q, want %q", state, auditionStateGraduated)
	}
}

func TestVerdict_AVideoThatAlreadyHadABigAudienceIsDone(t *testing.T) {
	// The bug a first run against a real database found: a 5,600-view video was
	// promoted onto the next rung, which reset its new-video push to full
	// strength. It would have kept the strongest push in the app while being
	// one of the most-seen videos in it.
	state, _ := auditionVerdict(0, 5600, 0, 0.9, 0.35, true)
	if state != auditionStateGraduated {
		t.Errorf("a video with 5600 views got %q, want %q — it has already been "+
			"seen by far more people than the ladder was ever going to give it",
			state, auditionStateGraduated)
	}
}

func TestVerdict_ASecondChanceCountsFromWhereItRestarted(t *testing.T) {
	// A revived video has a big view count by definition — that is why it was
	// retired in the first place. Its second run must be measured from the
	// restart, or the rule above would graduate it instantly and the second
	// chance would be worth nothing.
	state, next := auditionVerdict(0, 5060, 5000, 0.9, 0.35, true)
	if state != auditionStateActive {
		t.Errorf("a video on its second run got %q, want %q", state, auditionStateActive)
	}
	if next != 1 {
		t.Errorf("next rung = %d, want 1", next)
	}
}

func TestRunSpent_SurvivesAViewCountGoingBackwards(t *testing.T) {
	if got := auditionRunSpent(10, 50); got != 0 {
		t.Errorf("auditionRunSpent(10, 50) = %d, want 0 — a negative spend would "+
			"read as an unlimited remaining budget", got)
	}
}

// ── The hand-built SQL ──────────────────────────────────────────────────────

func TestDueWhere_NumbersItsPlaceholdersFromTheOffset(t *testing.T) {
	// This fragment is pasted after other parameters, so its numbering has to
	// continue from where they stopped. Getting it wrong compares a view count
	// against a status string, which Postgres rejects at runtime — inside a
	// background worker, where the only symptom is that no video is ever
	// judged again.
	where, args := auditionDueWhere(1)
	if len(args) != len(auditionLadder) {
		t.Fatalf("got %d arguments for %d rungs", len(args), len(auditionLadder))
	}
	// Every placeholder is written as "$N)" by the fragment, so matching on the
	// closing bracket keeps "$1" from matching inside "$12".
	for i := range auditionLadder {
		want := "$" + strconv.Itoa(i+2) + ")"
		if !strings.Contains(where, want) {
			t.Errorf("rung %d has no placeholder %s in %q", i, want, where)
		}
	}
	if strings.Contains(where, "$1)") {
		t.Errorf("$1 belongs to the caller but the fragment claimed it: %q", where)
	}
}

func TestDueWhere_AsksAboutEveryRung(t *testing.T) {
	where, args := auditionDueWhere(0)
	for i, rung := range auditionLadder {
		if args[i] != rung.views {
			t.Errorf("rung %d passes %v as its audience, want %d", i, args[i], rung.views)
		}
		if !strings.Contains(where, "audition_stage = "+strconv.Itoa(i)) {
			t.Errorf("no test for rung %d in %q", i, where)
		}
	}
}

// ── Second chances ──────────────────────────────────────────────────────────

func TestRetriesAreCapped(t *testing.T) {
	// Unlimited second chances put the queue back where it started: old videos
	// endlessly re-spending the audience that new uploads are waiting for.
	if auditionMaxRetries < 1 {
		t.Error("no second chance at all is harsh — a video posted at a bad " +
			"moment never gets another look")
	}
	if auditionMaxRetries > 2 {
		t.Errorf("auditionMaxRetries = %d — enough re-runs to crowd out the "+
			"new uploads the queue exists for", auditionMaxRetries)
	}
}

func TestTheBarNeedsRealEvidence(t *testing.T) {
	// Below this the answer is always "promote". A bar drawn from a handful of
	// videos is an accident, and retiring somebody's upload on an accident is
	// the exact failure the ladder exists to prevent.
	if auditionBarMinSamples < 20 {
		t.Errorf("auditionBarMinSamples = %d — too few scores to call a bar fair",
			auditionBarMinSamples)
	}
	if auditionPromoteFraction <= 0 || auditionPromoteFraction >= 1 {
		t.Errorf("auditionPromoteFraction = %v; at 0 nothing is ever promoted and "+
			"at 1 nothing is ever saved", auditionPromoteFraction)
	}
}
