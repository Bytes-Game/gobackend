package main

// ════════════════════════════════════════════════════════════════════════════════
// COHORT CLASSIFIER — different users want different things from the ranker
// ════════════════════════════════════════════════════════════════════════════════
//
// A one-size-fits-all scoring formula is a compromise: new users get too much
// personalization (garbage, because there's no data), power users get too
// little novelty, at-risk users get the same feed that's been failing them.
//
// We bucket users into 5 cohorts and apply a per-cohort weight multiplier map
// on top of the base scoring. Cohort is decided cheaply at request time from
// fields the profile already has.

type Cohort string

const (
	CohortColdStart Cohort = "cold_start"
	CohortNew       Cohort = "new"        // <50 events, past cold-start
	CohortEngaged   Cohort = "engaged"    // healthy usage
	CohortPower     Cohort = "power"      // many sessions, high avg watch
	CohortAtRisk    Cohort = "at_risk"    // declining usage signals
)

// cohortWeights holds per-feature multipliers applied to the base scoring terms.
// A value of 1.0 means "use as-is". >1.0 amplifies, <1.0 dampens.
type cohortWeights struct {
	Social    float64
	Freshness float64
	EnergyFit float64
	Relevance float64
	Quality   float64
	Novelty   float64
	Tie       float64
	Affinity  float64
	Search    float64
}

// cohortWeightTable encodes each cohort's preference shape.
//
//   cold_start: quality and freshness dominate, personalization is zero-noise.
//   new:        quality still strong, relevance starts to matter.
//   engaged:    balanced — the default.
//   power:      novelty + tie-strength + affinity — keep them surprised.
//   at_risk:    heavy quality + affinity (safe favorites), low novelty/energy.
var cohortWeightTable = map[Cohort]cohortWeights{
	CohortColdStart: {Social: 0.5, Freshness: 1.2, EnergyFit: 0.8, Relevance: 0.2, Quality: 1.6, Novelty: 1.4, Tie: 0.0, Affinity: 0.0, Search: 0.5},
	CohortNew:       {Social: 0.8, Freshness: 1.1, EnergyFit: 1.0, Relevance: 0.7, Quality: 1.3, Novelty: 1.2, Tie: 0.5, Affinity: 0.5, Search: 0.8},
	CohortEngaged:   {Social: 1.0, Freshness: 1.0, EnergyFit: 1.0, Relevance: 1.0, Quality: 1.0, Novelty: 1.0, Tie: 1.0, Affinity: 1.0, Search: 1.0},
	CohortPower:     {Social: 1.1, Freshness: 1.0, EnergyFit: 1.0, Relevance: 1.1, Quality: 0.9, Novelty: 1.3, Tie: 1.2, Affinity: 1.2, Search: 1.1},
	CohortAtRisk:    {Social: 1.2, Freshness: 0.9, EnergyFit: 0.8, Relevance: 1.3, Quality: 1.2, Novelty: 0.6, Tie: 1.4, Affinity: 1.5, Search: 1.4},
}

// classifyCohort places a user into one of the five buckets based on profile
// metrics. Deliberately cheap — runs once per feed request.
func classifyCohort(p *UserProfile) Cohort {
	if p == nil {
		return CohortColdStart
	}

	// 1) Cold-start has the highest priority — unambiguous.
	if p.EventCount < coldStartThreshold {
		return CohortColdStart
	}

	// 2) New: past cold-start but still thin data.
	if p.EventCount < 200 {
		return CohortNew
	}

	// 3) At-risk: declining behavior. We infer this from:
	//    - high skip rate AND
	//    - avg completion below 0.4 AND
	//    - avg session below half the population baseline (~180s).
	if p.AvgSkipRate > 0.5 && p.AvgCompletionRate < 0.4 && p.AvgSessionSec < 90 {
		return CohortAtRisk
	}

	// 4) Power user: lots of sessions, high watch engagement.
	if p.TotalSessions > 30 && p.AvgCompletionRate > 0.6 && p.AvgSessionSec > 240 {
		return CohortPower
	}

	// 5) Default — healthy engaged user.
	return CohortEngaged
}

// weightsFor returns the cohort's weight table, defaulting to Engaged if
// something is wrong.
func weightsFor(c Cohort) cohortWeights {
	if w, ok := cohortWeightTable[c]; ok {
		return w
	}
	return cohortWeightTable[CohortEngaged]
}
