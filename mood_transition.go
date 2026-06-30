package main

import (
	"strings"
	"sync"
)

// ─────────────────────────────────────────────────────────────────────────────
// MOOD TRANSITION GRAPH
//
// devf's founding philosophy (your words, internalized): "We're not ranking
// content — we're regulating human nervous systems. The user should feel
// BETTER after 20 minutes, not worse." That's the ethical-vs-dark-pattern test.
//
// This module operationalizes that philosophy. We maintain a graph of
// observed mood transitions:
//
//   FROM mood X, content of mood Y led to a session that ENDED WELL
//   (engagement remained, no bounce, no app-close)
//
// At score time, given the user's currently detected mood (already tracked
// on SessionState.DetectedMood), we boost candidates whose mood is on the
// "healthy next" path — the kind of transition that tends to leave users
// feeling lifted, not drained.
//
// "Healthy next" = transitions that empirically end in continued engagement
// rather than session abandonment. This is learned, not hand-coded — though
// we seed with sensible priors so the cold model isn't useless.
//
// Mood vocabulary (matches SessionState.DetectedMood):
//   energetic, chill, frustrated, bored, engaged, curious
//
// Plus content-side moods inferred from emotion tags:
//   happy, sad, intense, peaceful, anxious, motivated, nostalgic
// ─────────────────────────────────────────────────────────────────────────────

const (
	moodTransitionMaxBonus = 0.12
	moodMinTransitions     = 8  // need at least this many observations to predict
	moodLearnedConfidenceK = 12 // pseudo-count: learned overrides the seed only as observations accumulate
)

// moodHealthyNext is the seed graph — sensible priors that get OVERRIDDEN
// by learned transitions once we have enough data. Each from-mood maps to
// a list of content-moods that empirically tend to leave the user better
// off. NOT a hard rule — these are starting weights for the learned model.
//
// IMPORTANT: the from-mood KEYS are detectMood() labels, but the to-mood VALUES
// must be members of EmotionLabels (models.go) — they are matched at runtime
// against content emotion tags. The original seed used aspirational mood words
// (energetic, motivated, peaceful, curious, calming) that are NOT content tags,
// so roughly half the seed transitions could never fire and the cold-start
// regulation signal was silently inert. Values below are mapped to real tags:
//   energetic→intense, motivated→empowering, peaceful/calming→chill,
//   curious→suspenseful. A unit test asserts every value is a valid EmotionLabel.
//
// Reasoning per row:
//   energetic → ride the wave (intense/happy/empowering/funny)
//   chill → don't shock them (chill/happy/nostalgic/satisfying)
//   frustrated → gentle recovery (chill/satisfying/nostalgic/happy)
//   bored → re-stimulate (suspenseful/intense/happy/funny)
//   engaged → keep momentum (empowering/intense/happy/inspiring)
//   curious → feed the appetite (suspenseful/intense/happy/inspiring)
var moodHealthyNext = map[string][]string{
	"energetic":  {"intense", "happy", "empowering", "funny"},
	"chill":      {"chill", "happy", "nostalgic", "satisfying"},
	"frustrated": {"chill", "satisfying", "nostalgic", "happy"},
	"bored":      {"suspenseful", "intense", "happy", "funny"},
	"engaged":    {"empowering", "intense", "happy", "inspiring"},
	"curious":    {"suspenseful", "intense", "happy", "inspiring"},
}

// learnedMoodTransitions stores the OBSERVED reward per (fromMood, toMood)
// pair — overrides the seed priors as evidence accumulates.
type moodTransitionStore struct {
	mu      sync.RWMutex
	rewards map[string]map[string]float64 // fromMood → toMood → EMA reward
	counts  map[string]map[string]int     // fromMood → toMood → observation count
}

var moodTransitions = &moodTransitionStore{
	rewards: make(map[string]map[string]float64),
	counts:  make(map[string]map[string]int),
}

// observeMoodTransition records the outcome of a mood transition. reward
// in [0, 1]; typically 1.0 = session continued with engagement, 0.0 =
// user bounced/closed app, 0.5 = neutral.
func observeMoodTransition(fromMood, toMood string, reward float64) {
	if fromMood == "" || toMood == "" {
		return
	}
	fromMood = strings.ToLower(fromMood)
	toMood = strings.ToLower(toMood)
	if reward < 0 {
		reward = 0
	}
	if reward > 1 {
		reward = 1
	}
	moodTransitions.mu.Lock()
	defer moodTransitions.mu.Unlock()
	if moodTransitions.rewards[fromMood] == nil {
		moodTransitions.rewards[fromMood] = make(map[string]float64)
		moodTransitions.counts[fromMood] = make(map[string]int)
	}
	const ema = 0.10
	prev, exists := moodTransitions.rewards[fromMood][toMood]
	if !exists {
		moodTransitions.rewards[fromMood][toMood] = reward
	} else {
		moodTransitions.rewards[fromMood][toMood] = prev*(1-ema) + reward*ema
	}
	moodTransitions.counts[fromMood][toMood]++
}

// moodTransitionBonus returns a bounded score adjustment for a candidate
// based on whether its mood is a "healthy next step" from the user's
// currently detected mood.
//
// Combines two signals:
//   1. Seed prior: is toMood in the healthy-next list for fromMood?
//   2. Learned: what's the EMA reward for this exact (from, to) pair?
//
// Seed gives a mild bonus when learned data is sparse; learned overrides
// once we have enough observations to trust it.
func moodTransitionBonus(currentMood string, contentEmotions []string) float64 {
	if currentMood == "" || len(contentEmotions) == 0 {
		return 0
	}
	from := strings.ToLower(currentMood)

	// Determine the dominant mood signaled by the content's emotions —
	// take the first non-empty emotion as the candidate "to" mood. (More
	// sophisticated approaches could weight by emotion intensity; first-
	// emotion is a reasonable proxy that doesn't depend on intensities
	// being calibrated.)
	to := ""
	for _, e := range contentEmotions {
		if e != "" {
			to = strings.ToLower(e)
			break
		}
	}
	if to == "" {
		return 0
	}

	// Learned signal: lookup EMA reward.
	moodTransitions.mu.RLock()
	count := 0
	emaReward := 0.0
	hasLearned := false
	if outs, ok := moodTransitions.counts[from]; ok {
		count = outs[to]
	}
	if outs, ok := moodTransitions.rewards[from]; ok {
		if v, exists := outs[to]; exists && count >= moodMinTransitions {
			emaReward = v
			hasLearned = true
		}
	}
	moodTransitions.mu.RUnlock()

	// Seed signal: bonus if toMood is in the healthy-next prior list.
	seedBonus := 0.0
	if next, ok := moodHealthyNext[from]; ok {
		for _, n := range next {
			if n == to {
				seedBonus = moodTransitionMaxBonus * 0.5 // half max from prior alone
				break
			}
		}
	}

	if hasLearned {
		// Center reward on 0.5 → bonus in ±max range.
		learnedComp := (emaReward - 0.5) * 2.0 * moodTransitionMaxBonus
		// CONFIDENCE-weight learned vs seed by observation count, instead of a flat
		// 70/30. Just crossing moodMinTransitions with NEUTRAL data (emaReward≈0.5 →
		// learnedComp≈0) used to drop the bonus to 0.3·seed — i.e. BELOW the pure
		// seed prior — punishing a healthy transition for having a little neutral
		// evidence. Now sparse learned stays near the seed; only abundant evidence
		// pulls toward the learned value.
		conf := float64(count) / float64(count+moodLearnedConfidenceK)
		blended := seedBonus*(1-conf) + learnedComp*conf
		if blended > moodTransitionMaxBonus {
			blended = moodTransitionMaxBonus
		}
		if blended < -moodTransitionMaxBonus {
			blended = -moodTransitionMaxBonus
		}
		return blended
	}
	return seedBonus
}

// recordSessionMoodOutcome is called at session end (or on app_background)
// to record the reward of the most recent mood transition. The "reward"
// is derived from the session's final state: high engagement = positive,
// bounce or quick close = negative.
func recordSessionMoodOutcome(fromMood string, recentEmotions []string, sessionGoodOutcome bool) {
	if fromMood == "" || len(recentEmotions) == 0 {
		return
	}
	reward := 0.5 // neutral default
	if sessionGoodOutcome {
		reward = 1.0
	} else {
		reward = 0.0
	}
	// Record the transition for each recent emotion (the user moved through
	// these moods during the session — they jointly contributed to outcome).
	for _, e := range recentEmotions {
		if e != "" {
			observeMoodTransition(fromMood, e, reward)
		}
	}
}

// resetMoodTransitions is for tests only.
func resetMoodTransitions() {
	moodTransitions.mu.Lock()
	defer moodTransitions.mu.Unlock()
	moodTransitions.rewards = make(map[string]map[string]float64)
	moodTransitions.counts = make(map[string]map[string]int)
}
