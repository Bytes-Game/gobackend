package main

import (
	"strings"
	"sync"
)

// ─────────────────────────────────────────────────────────────────────────────
// SESSION-TRAJECTORY PREDICTOR
//
// The ranker currently looks at the LAST ITEM (sequence penalty) but treats
// the rest of the session as flat. Real consumption is sequential: after
// 3 comedy → 1 fitness → 2 emotional, what comes next?
//
// This module learns a Markov-like transition model over CATEGORY × ENERGY
// pairs from observed positive-engagement sequences. At score time it
// returns a small bonus for candidates whose (category, energy) matches the
// "what usually comes next after the current trajectory" prediction.
//
// Why CATEGORY × ENERGY (not category alone): captures intent transitions.
// "comedy at low energy" → "comedy at high energy" is one path; "comedy at
// low energy" → "music at low energy" is another. Both predict differently.
//
// State storage:
//   trans[from][to] += 1 on observed pair
//   from = "comedy:low", "fitness:high", etc.
//
// At score time:
//   1. Inspect last 3 positively-engaged items in session
//   2. Build the from-state (last item's category × energy bucket)
//   3. For each candidate, lookup trans[from][candidate.bucket]
//   4. Normalize across all candidates' lookups, return as a small bonus
//
// SAFETY:
//   - Bonus capped at trajectoryMaxBonus
//   - Bucket count below threshold → return 0 (cold model)
//   - Per-cohort transitions to avoid one user's pattern shaping everyone's
// ─────────────────────────────────────────────────────────────────────────────

const (
	trajectoryMaxBonus  = 0.10
	trajectoryMinCounts = 5  // need at least this many observations of `from` state to predict
)

// trajectoryState is the aggregate transition table for one cohort.
// Keys are "<categoryLowercase>:<energyBucket>" pairs; values are counts.
type trajectoryState struct {
	mu          sync.RWMutex
	transitions map[string]map[string]int // from → to → count
	fromTotals  map[string]int             // from → total outgoing
}

func newTrajectoryState() *trajectoryState {
	return &trajectoryState{
		transitions: make(map[string]map[string]int),
		fromTotals:  make(map[string]int),
	}
}

type trajectoryStore struct {
	mu     sync.RWMutex
	byCoh  map[Cohort]*trajectoryState
}

var sessionTrajectories = &trajectoryStore{
	byCoh: make(map[Cohort]*trajectoryState),
}

func trajectoryEnergyBucket(energy float64) string {
	switch {
	case energy < 0.33:
		return "low"
	case energy < 0.67:
		return "med"
	}
	return "high"
}

func trajectoryStateKey(category string, energy float64) string {
	if category == "" {
		return ""
	}
	return strings.ToLower(category) + ":" + trajectoryEnergyBucket(energy)
}

// noteSessionTransition records one observed (from → to) transition for
// a cohort. Called when a user positively engages with item B after
// engaging with item A.
func noteSessionTransition(cohort Cohort, fromKey, toKey string) {
	if fromKey == "" || toKey == "" {
		return
	}
	sessionTrajectories.mu.Lock()
	st, ok := sessionTrajectories.byCoh[cohort]
	if !ok {
		st = newTrajectoryState()
		sessionTrajectories.byCoh[cohort] = st
	}
	sessionTrajectories.mu.Unlock()

	st.mu.Lock()
	defer st.mu.Unlock()
	if st.transitions[fromKey] == nil {
		st.transitions[fromKey] = make(map[string]int)
	}
	st.transitions[fromKey][toKey]++
	st.fromTotals[fromKey]++
}

// trajectoryBonus returns a bounded score adjustment for a candidate based
// on its likelihood of being the natural next step after the user's most
// recent positively-engaged item.
//
// fromKey is the bucket of the user's last positive engagement; can be ""
// when the session is fresh (returns 0).
//
// candidateKey is the bucket of the candidate being scored.
func trajectoryBonus(cohort Cohort, fromKey, candidateKey string) float64 {
	if fromKey == "" || candidateKey == "" {
		return 0
	}
	sessionTrajectories.mu.RLock()
	st, ok := sessionTrajectories.byCoh[cohort]
	sessionTrajectories.mu.RUnlock()
	if !ok {
		return 0
	}
	st.mu.RLock()
	total := st.fromTotals[fromKey]
	if total < trajectoryMinCounts {
		st.mu.RUnlock()
		return 0
	}
	count := 0
	if outs, ok := st.transitions[fromKey]; ok {
		count = outs[candidateKey]
	}
	st.mu.RUnlock()

	// Probability this candidate's bucket is the natural next step.
	prob := float64(count) / float64(total)
	// Map prob to a bounded bonus. We center on the uniform expectation
	// (1/12 = 0.083 for 4 categories × 3 energy buckets) so a candidate
	// with strictly random transition probability gets 0; clearly above-
	// expected gets +bonus, below gets a small negative.
	const uniformExpected = 1.0 / 12.0
	delta := prob - uniformExpected
	bonus := delta * 1.5 // scale so a 30%-probable transition gives ~0.36 → clamped to cap
	if bonus > trajectoryMaxBonus {
		bonus = trajectoryMaxBonus
	}
	if bonus < -trajectoryMaxBonus*0.5 {
		bonus = -trajectoryMaxBonus * 0.5
	}
	return bonus
}

// recordSessionTrajectoryFromEvent is the wrapper called from the event
// handler. Looks up the user's session, finds the last positively-engaged
// item, and records the transition with the current event as `to`.
//
// We only record on positive events (label >= 0.5 in LTR terms) — skip
// transitions are noise.
func recordSessionTrajectoryFromEvent(cohort Cohort, session *SessionState, eventCategory string, eventEnergy float64, isPositive bool) {
	if !isPositive || session == nil {
		return
	}
	toKey := trajectoryStateKey(eventCategory, eventEnergy)
	if toKey == "" {
		return
	}
	// `from` is the most-recent prior category in the session.
	fromKey := ""
	if n := len(session.LastCategories); n > 0 {
		// We don't have the energy of the prior item handy; default to "med"
		// which is a reasonable fallback. Production could stash energies
		// alongside categories in the session state — small upgrade.
		fromKey = strings.ToLower(session.LastCategories[n-1]) + ":med"
	}
	if fromKey == "" {
		return
	}
	noteSessionTransition(cohort, fromKey, toKey)
}

// isPositiveEngagementForTrajectory decides whether an event is a useful
// "positive transition" signal. Stricter than ltrLabelForEvent's positive:
// we only count strong signals (like, save, share, complete, scroll_back)
// and watch-completion above 70%, not every "view."
func isPositiveEngagementForTrajectory(eventType string, completionRate float64) bool {
	switch eventType {
	case "like", "save", "share", "complete", "rewatch", "scroll_back", "loop", "follow_from_content":
		return true
	case "view":
		return completionRate >= 0.70
	}
	return false
}

// resetSessionTrajectories clears all transitions — only used by tests.
func resetSessionTrajectories() {
	sessionTrajectories.mu.Lock()
	defer sessionTrajectories.mu.Unlock()
	sessionTrajectories.byCoh = make(map[Cohort]*trajectoryState)
}
