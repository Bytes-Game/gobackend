package main

// ─────────────────────────────────────────────────────────────────────────────
// ANTI-LOOP / STUCK-USER DETECTION
//
// Users quit when the feed gets monotonous ("all cooking" or "5 skips in a
// row" or "same creator again"). Detecting these patterns and deliberately
// switching strategy is often the difference between a 30-sec and a 30-min
// session.
//
// This module inspects the live SessionState and decides whether the ranker
// should override its default strategy for the *next* page.
//
// Signals we use:
//   1. SkipStreak  — consecutive skips (hard resistance)
//   2. LastCategories repetition — same category ≥ 3 of last 4 served
//   3. LastCreators repetition — same creator ≥ 3 of last 5
//   4. DopamineBudget collapse — depleted; user is fatigued
//
// Each fires a different remedy — the goal is not to panic but to pick a
// strategy the user hasn't been served recently (via TriedStrategies).
// ─────────────────────────────────────────────────────────────────────────────

// LoopDiagnosis summarises what (if anything) is stuck, and what strategy
// the ranker should switch to for the next page.
type LoopDiagnosis struct {
	Stuck           bool
	Reason          string // human-readable for metrics / debug payload
	SuggestedStrat  string // "" means no override
}

// detectLoop inspects the session and returns a diagnosis. Pure function —
// no I/O, no mutation. Caller decides whether to honour the suggestion.
func detectLoop(session *SessionState) LoopDiagnosis {
	if session == nil {
		return LoopDiagnosis{}
	}

	// Signal 1: Hard skip resistance.
	if session.SkipStreak >= 4 {
		return LoopDiagnosis{
			Stuck:          true,
			Reason:         "skip_streak",
			SuggestedStrat: pickUntriedStrategy(session, []string{strategyFreshBlood, strategyDiscovery, strategyCalming}),
		}
	}

	// Signal 2: Category monoculture — ≥3 of last 4 items in same bucket.
	if cat, n := dominantValue(session.LastCategories, 4); n >= 3 {
		_ = cat
		return LoopDiagnosis{
			Stuck:          true,
			Reason:         "category_monoculture",
			SuggestedStrat: pickUntriedStrategy(session, []string{strategyDiscovery, strategyFreshBlood, strategyMoodMatch}),
		}
	}

	// Signal 3: Creator flood — ≥3 of last 5 items from same creator.
	if _, n := dominantValue(session.LastCreators, 5); n >= 3 {
		return LoopDiagnosis{
			Stuck:          true,
			Reason:         "creator_flood",
			SuggestedStrat: pickUntriedStrategy(session, []string{strategyDiscovery, strategyFreshBlood}),
		}
	}

	// Signal 4: Dopamine collapse — user is fatigued, feed is failing them.
	// No `> 0` lower guard: the budget is clamped to a floor of 0 at every drain
	// site and initialized to 1.0, so 0.0 means MAXIMALLY fatigued, not
	// "uninitialized" — excluding it denied the remedy to exactly the users (rock
	// bottom) it exists for.
	if session.DopamineBudget < 0.15 {
		return LoopDiagnosis{
			Stuck:          true,
			Reason:         "dopamine_collapse",
			SuggestedStrat: pickUntriedStrategy(session, []string{strategyCalming, strategyNostalgic, strategyMoodMatch}),
		}
	}

	return LoopDiagnosis{}
}

// dominantValue returns the most common non-empty value in the last `tailN`
// entries of `xs`, and its count. Empty input → ("", 0).
func dominantValue(xs []string, tailN int) (string, int) {
	if len(xs) == 0 || tailN <= 0 {
		return "", 0
	}
	from := len(xs) - tailN
	if from < 0 {
		from = 0
	}
	counts := make(map[string]int, tailN)
	for _, v := range xs[from:] {
		if v == "" {
			continue
		}
		counts[v]++
	}
	var bestKey string
	var bestN int
	for k, n := range counts {
		if n > bestN {
			bestKey = k
			bestN = n
		}
	}
	return bestKey, bestN
}

// pickUntriedStrategy returns the first preferred strategy the session hasn't
// tried yet. Falls back to the first preference if all have been tried.
func pickUntriedStrategy(session *SessionState, preferred []string) string {
	tried := make(map[string]bool, len(session.TriedStrategies))
	for _, s := range session.TriedStrategies {
		tried[s] = true
	}
	for _, p := range preferred {
		if !tried[p] {
			return p
		}
	}
	if len(preferred) > 0 {
		return preferred[0]
	}
	return ""
}
