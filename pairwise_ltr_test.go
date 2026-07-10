package main

import "testing"

// ltrUpdates reads a cohort's update counter (test helper).
func ltrUpdates(cohort Cohort) int {
	ltr.mu.RLock()
	defer ltr.mu.RUnlock()
	if m, ok := ltr.byCoh[cohort]; ok {
		return m.Updates
	}
	return 0
}

// TestPairwiseLTR_OrdersPairs: after BPR steps on (engaged, skipped)
// pairs differing in one feature, the model must rank the engaged
// feature profile above the skipped one.
func TestPairwiseLTR_OrdersPairs(t *testing.T) {
	resetRedis(t)
	resetLTR()

	pos := map[string]float64{"quality": 0.9, "freshness": 0.5}
	neg := map[string]float64{"quality": 0.1, "freshness": 0.5}
	for i := 0; i < 60; i++ {
		ltrObservePairwise(CohortEngaged, pos, neg, 1.0)
	}
	dPos := ltrScoreDelta(CohortEngaged, pos)
	dNeg := ltrScoreDelta(CohortEngaged, neg)
	if dPos <= dNeg {
		t.Fatalf("pairwise training must rank pos above neg: %v vs %v", dPos, dNeg)
	}
}

// TestPairwiseLTR_NegPoolRoundtrip: stash → pop pairs within the same
// user + cohort; cross-cohort pairs are dropped without a model update.
func TestPairwiseLTR_NegPoolRoundtrip(t *testing.T) {
	resetRedis(t)
	resetLTR()

	ltrStashNegative("u1", string(CohortEngaged), map[string]float64{"quality": 0.1})
	before := ltrUpdates(CohortEngaged)
	ltrPairwiseFromPool("u1", CohortEngaged, map[string]float64{"quality": 0.9}, 1.0)
	if ltrUpdates(CohortEngaged) != before+1 {
		t.Fatal("same-cohort pair must apply exactly one pairwise update")
	}

	// Pool is consumed — a second positive finds nothing.
	ltrPairwiseFromPool("u1", CohortEngaged, map[string]float64{"quality": 0.9}, 1.0)
	if ltrUpdates(CohortEngaged) != before+1 {
		t.Fatal("empty pool must not produce an update")
	}

	// Cross-cohort: stash under power, pop as engaged → dropped.
	ltrStashNegative("u1", string(CohortPower), map[string]float64{"quality": 0.1})
	ltrPairwiseFromPool("u1", CohortEngaged, map[string]float64{"quality": 0.9}, 1.0)
	if ltrUpdates(CohortEngaged) != before+1 {
		t.Fatal("cross-cohort pair must be dropped")
	}
}
