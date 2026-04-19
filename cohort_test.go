package main

import "testing"

func TestClassifyCohort_ColdStart(t *testing.T) {
	p := &UserProfile{EventCount: 5}
	if got := classifyCohort(p); got != CohortColdStart {
		t.Errorf("expected cold_start, got %s", got)
	}
}

func TestClassifyCohort_New(t *testing.T) {
	p := &UserProfile{EventCount: 100}
	if got := classifyCohort(p); got != CohortNew {
		t.Errorf("expected new, got %s", got)
	}
}

func TestClassifyCohort_AtRisk(t *testing.T) {
	p := &UserProfile{
		EventCount:        500,
		AvgSkipRate:       0.7,
		AvgCompletionRate: 0.3,
		AvgSessionSec:     60,
	}
	if got := classifyCohort(p); got != CohortAtRisk {
		t.Errorf("expected at_risk, got %s", got)
	}
}

func TestClassifyCohort_Power(t *testing.T) {
	p := &UserProfile{
		EventCount:        1000,
		TotalSessions:     60,
		AvgCompletionRate: 0.8,
		AvgSessionSec:     300,
		AvgSkipRate:       0.1,
	}
	if got := classifyCohort(p); got != CohortPower {
		t.Errorf("expected power, got %s", got)
	}
}

func TestClassifyCohort_EngagedDefault(t *testing.T) {
	p := &UserProfile{
		EventCount:        500,
		TotalSessions:     10,
		AvgCompletionRate: 0.55,
		AvgSessionSec:     200,
		AvgSkipRate:       0.25,
	}
	if got := classifyCohort(p); got != CohortEngaged {
		t.Errorf("expected engaged, got %s", got)
	}
}

func TestClassifyCohort_NilProfile(t *testing.T) {
	if got := classifyCohort(nil); got != CohortColdStart {
		t.Errorf("nil profile must be cold_start, got %s", got)
	}
}

func TestWeightsFor_KnownCohorts(t *testing.T) {
	cases := []Cohort{CohortColdStart, CohortNew, CohortEngaged, CohortPower, CohortAtRisk}
	for _, c := range cases {
		w := weightsFor(c)
		if w.Social == 0 && w.Quality == 0 && w.Novelty == 0 {
			t.Errorf("cohort %s returned zero weights", c)
		}
	}
}

func TestWeightsFor_UnknownFallsBackToEngaged(t *testing.T) {
	w := weightsFor(Cohort("nonexistent"))
	engaged := cohortWeightTable[CohortEngaged]
	if w != engaged {
		t.Errorf("unknown cohort should fall back to engaged, got %+v", w)
	}
}

func TestCohortOrdinal_Stable(t *testing.T) {
	// Just ensure every cohort maps to some ordinal without panic.
	for _, c := range []Cohort{CohortColdStart, CohortNew, CohortEngaged, CohortPower, CohortAtRisk, Cohort("weird")} {
		_ = cohortOrdinal(c)
	}
}
