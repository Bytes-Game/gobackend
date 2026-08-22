package main

// learned_authority.go — let the learned parts of the ranker earn their say.
//
// ════════════════════════════════════════════════════════════════════════════════
// THE PROBLEM
// ════════════════════════════════════════════════════════════════════════════════
//
// The feed score is a big sum of hand-written rules — is this fresh, is this a
// friend, does the category match, and about twenty more — plus three parts that
// LEARN from what people actually did:
//
//	the LTR head        guesses whether this person will engage
//	the calibrated head turns that guess into a probability
//	the watch-time head guesses how much of the video they will watch
//
// Every one of those three worked the same way, and it was the wrong way. Each
// sat at exactly zero until its cohort crossed a sample count, and then jumped
// straight to full strength on the very next sample. After that it never grew
// again, no matter how much it learned. A model with thirty samples and a model
// with thirty million had precisely the same say.
//
// Both halves of that are wrong, in opposite directions:
//
//	Thirty samples is nothing. A model fitted on thirty examples of thirty-odd
//	features has memorised noise, and on the sample that crossed the line it
//	was handed its full budget with no warning.
//
//	Thirty million samples is a model that knows this audience far better than
//	any rule anybody typed by hand — and it was still held to the same small
//	correction, permanently outvoted by guesses made before launch.
//
// That second half is the real difference between this ranker and the ones it
// is modelled on. Those are learned-first: the model does the ranking and the
// hand-written rules are guardrails around it. Here the hand-written rules did
// the ranking and the model was allowed to nudge.
//
// ════════════════════════════════════════════════════════════════════════════════
// WHAT REPLACES IT
// ════════════════════════════════════════════════════════════════════════════════
//
// A learned head's influence now grows, and it has to earn the growth twice:
//
//	EVIDENCE — how much has it actually seen? Rises smoothly from nothing, so
//	           there is no cliff at the warmup line and no sudden change in
//	           anybody's feed.
//
//	SKILL    — has it been RIGHT? The ranker already records the gap between
//	           what each head predicted and what the person then did. A head
//	           whose guesses are no better than a coin flip earns nothing,
//	           however many samples it has.
//
// Multiply the two and you get how much of its budget the head has earned.
// Age alone buys nothing. Being right about a handful of people buys nothing.
// Being right about a lot of people, repeatedly, buys the ability to overrule
// a rule somebody guessed at before launch — which is the whole point.
//
// ════════════════════════════════════════════════════════════════════════════════
// WHAT THIS DOES TODAY
// ════════════════════════════════════════════════════════════════════════════════
//
// Nothing. With no traffic there are no samples, so every head earns zero and
// contributes zero, exactly as before. This is the shape the ranker needs to
// already have when real traffic arrives, not something to bolt on afterwards
// once the feed has been tuned around a permanently-hobbled model.

import (
	"sync"
	"time"
)

// learnedAuthorityHalfLife is how many samples past its warmup a head needs to
// reach half of its full say.
//
// Two thousand is a few days of real traffic, not months. The number to have in
// mind is that a head at half authority is already stronger than the fixed cap
// it used to live under — so this is not a long wait before the learned parts
// matter, it is a short ramp instead of a step.
const learnedAuthorityHalfLife = 2000.0

// learnedAuthorityMaxGain is how much stronger a fully-earned head is than the
// fixed cap it replaced.
//
// This is the dial. At 1.0 nothing changes from the old behaviour except the
// ramp. At 2.0 a head that has seen a lot and been right about it can move an
// item by roughly as much as the entire hand-written base score can — which is
// what "the model does the ranking" actually means in numbers.
//
// Higher than that is not obviously better and is much harder to reason about,
// so if the feed ever needs the learned parts to matter more than this, the
// honest change is to cut hand-written rules rather than to keep raising this.
const learnedAuthorityMaxGain = 2.0

// learnedNoSkillError is the error a head that knows nothing would post.
//
// A head predicting a coin flip on a fifty-fifty outcome is wrong by 0.5 on
// average, so its mean squared error is 0.25. Anything at or above that has
// told us nothing we did not already know and earns no say at all.
//
// On a lopsided cohort — one where almost everybody skips — a know-nothing head
// would actually score better than 0.25, so measuring against a flat 0.25 gives
// such a head slightly more credit than it deserves. The error is small and it
// is in the direction of caution in the place that matters: a head is never
// denied authority it earned, only occasionally granted a little it did not.
const learnedNoSkillError = 0.25

// learnedEvidence is the share of authority a head has earned by volume, from
// 0 at its warmup line rising smoothly toward 1.
//
// Pure, so the curve can be read and tested without any model state.
func learnedEvidence(samples, warmup int) float64 {
	if samples <= warmup {
		return 0
	}
	n := float64(samples - warmup)
	return n / (n + learnedAuthorityHalfLife)
}

// learnedSkill is the share of authority a head has earned by being right,
// from 0 for a head no better than a coin flip up to 1 for a perfect one.
//
// meanError is the average signed gap between prediction and outcome, so it
// catches a head that is consistently too high or too low. variance is the
// spread around that. Squaring the first and adding gives the mean squared
// error, which is the whole of what "wrong" means here: a head can be wrong by
// leaning the wrong way, or by being all over the place, and both must count.
func learnedSkill(meanError, variance float64) float64 {
	mse := meanError*meanError + variance
	if mse >= learnedNoSkillError {
		return 0
	}
	if mse < 0 {
		return 1 // not reachable from real numbers; refuse to return nonsense
	}
	return 1 - mse/learnedNoSkillError
}

// learnedAuthoritySkillTTL bounds how stale a skill reading may get.
//
// The reading moves over hours and is shared by every candidate on every page,
// so taking the lock that guards it once per item would be many thousands of
// identical answers a second. Thirty seconds of staleness cannot change a
// number that only scales a bonus.
const learnedAuthoritySkillTTL = 30 * time.Second

type learnedSkillEntry struct {
	value   float64
	fetched time.Time
}

var (
	learnedSkillMu    sync.Mutex
	learnedSkillCache = map[Cohort]learnedSkillEntry{}
)

// learnedSkillFor reports how right this cohort's learned heads have been.
//
// An unmeasured cohort scores 1 rather than 0. That looks generous but is not:
// nothing is measured until samples exist, and with no samples the evidence
// term is zero, so the product is zero either way. Returning 0 here instead
// would mean a head could never start at all — skill needs samples, and samples
// only arrive once the head is running.
func learnedSkillFor(cohort Cohort) float64 {
	learnedSkillMu.Lock()
	if e, ok := learnedSkillCache[cohort]; ok && time.Since(e.fetched) < learnedAuthoritySkillTTL {
		learnedSkillMu.Unlock()
		return e.value
	}
	learnedSkillMu.Unlock()

	mean, variance, ok := bayesianResidual(cohort)
	skill := 1.0
	if ok {
		skill = learnedSkill(mean, variance)
	}

	learnedSkillMu.Lock()
	learnedSkillCache[cohort] = learnedSkillEntry{value: skill, fetched: time.Now()}
	learnedSkillMu.Unlock()
	return skill
}

// learnedGain is the multiplier a learned head's bonus should be scaled by:
// 0 for one that has earned nothing, up to learnedAuthorityMaxGain for one that
// has seen a great deal and been right about it.
//
// A gain of 1 reproduces the fixed cap this replaced, so the old behaviour is a
// point on the new curve rather than something thrown away.
func learnedGain(cohort Cohort, samples, warmup int) float64 {
	e := learnedEvidence(samples, warmup)
	if e <= 0 {
		return 0
	}
	return learnedAuthorityMaxGain * e * learnedSkillFor(cohort)
}

// learnedGainByVolume is learnedGain for a head with no accuracy record of its
// own.
//
// The watch-time head is the only one in this position: the residual tracker
// follows the engagement prediction, not the watch-time one. Volume alone is
// the weaker of the two tests, so this deliberately tops out at half the gain —
// a head we cannot check does not get to overrule the rest on its own.
func learnedGainByVolume(samples, warmup int) float64 {
	return learnedAuthorityMaxGain * 0.5 * learnedEvidence(samples, warmup)
}
