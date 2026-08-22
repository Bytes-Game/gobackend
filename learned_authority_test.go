package main

import "testing"

// These two curves decide how much say the learned parts of the ranker get, so
// the properties worth pinning are the ones that would be silent if broken: a
// model that has learned nothing must not be handed influence, and a model that
// has learned a lot must be able to take it.

// ── Evidence ────────────────────────────────────────────────────────────────

func TestEvidence_NothingBeforeWarmup(t *testing.T) {
	for _, n := range []int{0, 1, 19, 20} {
		if got := learnedEvidence(n, 20); got != 0 {
			t.Errorf("learnedEvidence(%d, 20) = %v, want 0 — a model that has not "+
				"reached its warmup line has no business moving anybody's feed", n, got)
		}
	}
}

func TestEvidence_NoCliffAtWarmup(t *testing.T) {
	// The bug this curve replaced: influence jumped from nothing to everything
	// on the single sample that crossed the line, so one person's engagement
	// event visibly changed what a whole cohort saw next.
	justOver := learnedEvidence(21, 20)
	if justOver <= 0 {
		t.Fatal("one sample past warmup earns nothing at all")
	}
	if justOver > 0.01 {
		t.Errorf("one sample past warmup earns %v of full authority — that is a "+
			"cliff, which is what this curve exists to remove", justOver)
	}
}

func TestEvidence_RisesAndNeverExceedsOne(t *testing.T) {
	prev := 0.0
	for _, n := range []int{21, 100, 1000, 5000, 50_000, 5_000_000} {
		got := learnedEvidence(n, 20)
		if got <= prev {
			t.Errorf("evidence at %d samples (%v) did not rise above the previous "+
				"step (%v) — more data must always mean more say", n, got, prev)
		}
		if got > 1 {
			t.Errorf("evidence at %d samples = %v, which breaks the ceiling", n, got)
		}
		prev = got
	}
}

func TestEvidence_HalfwayIsWhereItSaysItIs(t *testing.T) {
	got := learnedEvidence(20+int(learnedAuthorityHalfLife), 20)
	if got < 0.49 || got > 0.51 {
		t.Errorf("at the half-life the curve reads %v, want about 0.5 — the "+
			"constant is documented as the halfway point and has to mean it", got)
	}
}

// ── Skill ───────────────────────────────────────────────────────────────────

func TestSkill_ACoinFlipEarnsNothing(t *testing.T) {
	// A head that guesses at random on a fifty-fifty outcome misses by 0.5 on
	// average, so its mean squared error is 0.25. It has told us nothing and
	// must earn nothing, however many samples it has behind it.
	if got := learnedSkill(0, learnedNoSkillError); got != 0 {
		t.Errorf("a coin-flip head earned %v of full authority, want 0", got)
	}
	if got := learnedSkill(0, 0.9); got != 0 {
		t.Errorf("a head worse than a coin flip earned %v, want 0", got)
	}
}

func TestSkill_APerfectHeadEarnsEverything(t *testing.T) {
	if got := learnedSkill(0, 0); got != 1 {
		t.Errorf("a head that is never wrong earned %v of full authority, want 1", got)
	}
}

func TestSkill_ABiasedHeadIsPenalised(t *testing.T) {
	// A head can be wrong two ways: leaning consistently in one direction, or
	// being all over the place. Only counting the spread would let a head that
	// always guesses far too high look flawless.
	steady := learnedSkill(0, 0.05)
	leaning := learnedSkill(0.3, 0.05)
	if leaning >= steady {
		t.Errorf("a head that is consistently 0.3 too high earned %v, no less than "+
			"an unbiased one on %v — a steady lean is still being wrong",
			leaning, steady)
	}
}

func TestSkill_StaysInRange(t *testing.T) {
	for _, c := range []struct{ mean, variance float64 }{
		{0, 0}, {0, 0.1}, {0.5, 0.5}, {-2, 0}, {0, -1},
	} {
		got := learnedSkill(c.mean, c.variance)
		if got < 0 || got > 1 {
			t.Errorf("learnedSkill(%v, %v) = %v, outside 0..1 — this number scales "+
				"a bonus, so out of range means the bonus is inverted or unbounded",
				c.mean, c.variance, got)
		}
	}
}

// ── The two together ────────────────────────────────────────────────────────

func TestGainByVolume_IsCappedBelowTheFullGain(t *testing.T) {
	// The watch-time head has no accuracy record, so it earns on volume alone.
	// That is the weaker test and must buy less.
	huge := learnedGainByVolume(50_000_000, 30)
	if huge > learnedAuthorityMaxGain*0.5+1e-9 {
		t.Errorf("a head nobody is checking earned a gain of %v, above the %v it "+
			"is meant to be capped at", huge, learnedAuthorityMaxGain*0.5)
	}
	if huge < learnedAuthorityMaxGain*0.45 {
		t.Errorf("a head with fifty million samples earned only %v — the cap is "+
			"meant to be reachable", huge)
	}
}

func TestGainByVolume_NothingBeforeWarmup(t *testing.T) {
	if got := learnedGainByVolume(29, 30); got != 0 {
		t.Errorf("below warmup the watch-time head earned %v, want 0", got)
	}
}

func TestMaxGain_IsWorthHaving(t *testing.T) {
	// At 1.0 this whole file would be a no-op dressed up as a change: the ramp
	// would exist but a fully-trained model would end up exactly where the old
	// fixed cap left it, still unable to outweigh rules written before launch.
	if learnedAuthorityMaxGain <= 1.0 {
		t.Errorf("learnedAuthorityMaxGain = %v — at or below 1 a model can never "+
			"earn more than the fixed cap it replaced", learnedAuthorityMaxGain)
	}
}
