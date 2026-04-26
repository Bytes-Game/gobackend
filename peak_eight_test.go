package main

import (
	"math/rand"
	"strings"
	"testing"
	"time"
)

// ─────────────────────────────────────────────────────────────────────────────
// PEAK-INTELLIGENCE EIGHT — focused safety-net tests
//
// One test (sometimes two) per upgrade. Each pins a contract that, if
// broken in the future, would silently degrade the algorithm.
// ─────────────────────────────────────────────────────────────────────────────

// ── Peak #1: Trained two-tower ──────────────────────────────────────────────

func TestPeak1_TrainedTwoTowerInitFromPrior(t *testing.T) {
	resetRedis(t)
	cs := &ContentScore{
		ContentID:    "tt-1",
		ContentType:  "post",
		Category:     "music",
		CreatorID:    "creatorX",
		QualityScore: 0.6,
		EnergyLevel:  0.5,
		CreatedAt:    time.Now(),
	}
	emotions := []string{"happy"}
	tt := loadOrInitTrainedContent(cs, emotions)
	if tt == nil {
		t.Fatal("expected init; got nil")
	}
	if tt.Updates != 0 {
		t.Errorf("fresh init must have zero updates, got %d", tt.Updates)
	}
	// At init, trained == prior (modulo float).
	for i := range tt.Trained {
		if abs8(tt.Trained[i]-tt.Prior[i]) > 1e-9 {
			t.Errorf("trained should match prior at init, slot %d: %v vs %v", i, tt.Trained[i], tt.Prior[i])
		}
	}
}

func TestPeak1_TrainedTwoTowerDriftsTowardUserOnPositive(t *testing.T) {
	resetRedis(t)
	cs := &ContentScore{
		ContentID:    "tt-2",
		ContentType:  "post",
		Category:     "music",
		CreatorID:    "creatorY",
		QualityScore: 0.6,
		EnergyLevel:  0.5,
		CreatedAt:    time.Now(),
	}
	emotions := []string{"chill"}
	// Create a synthetic user vector pointing strongly at slot 5.
	uv := make([]float64, embedDim)
	uv[5] = 1.0
	uv = l2norm(uv)

	// Apply many positive updates.
	for i := 0; i < 30; i++ {
		updateTrainedContentEmbedding(cs, emotions, uv, 1.0)
	}
	tt := loadOrInitTrainedContent(cs, emotions)
	if tt.Updates < 30 {
		t.Errorf("expected at least 30 updates, got %d", tt.Updates)
	}
	// Drift: the new trained vector should have moved away from prior
	// toward the user vector at slot 5.
	if !(tt.Trained[5] > tt.Prior[5]) {
		t.Errorf("slot 5 should have grown toward user vec; trained=%v prior=%v", tt.Trained[5], tt.Prior[5])
	}
}

// ── Peak #2: Bayesian uncertainty bonus ─────────────────────────────────────

func TestPeak2_BayesianBonusZeroBeforeWarmup(t *testing.T) {
	resetRedis(t)
	resetBayesianStore()
	rnd := rand.New(rand.NewSource(1))
	if b := bayesianUncertaintyBonus(CohortEngaged, 0, rnd); b != 0 {
		t.Errorf("bonus must be 0 before warmup, got %v", b)
	}
}

func TestPeak2_BayesianBonusBoundedAfterWarmup(t *testing.T) {
	resetRedis(t)
	resetBayesianStore()
	// Drive enough samples to clear the warmup gate. Use small noise so
	// the variance is non-zero but bounded.
	for i := 0; i < 60; i++ {
		bayesianRecord(CohortEngaged, 0.5+0.05*float64(i%5)/5.0, 0.5)
	}
	rnd := rand.New(rand.NewSource(7))
	for i := 0; i < 50; i++ {
		b := bayesianUncertaintyBonus(CohortEngaged, 0, rnd)
		if b > bayesianMaxBonus+1e-9 || b < -bayesianMaxBonus-1e-9 {
			t.Errorf("bonus exceeded bound: got %v cap=%v", b, bayesianMaxBonus)
		}
	}
}

// ── Peak #3: Negative-feedback profile mining ───────────────────────────────

func TestPeak3_NegativeFeedbackDropsCategoryAffinity(t *testing.T) {
	profile := &UserProfile{
		UserID:           "u3",
		CategoryAffinity: map[string]float64{"comedy": 0.5},
		EmotionPreference: map[string]float64{},
		EnergyPreference: 0.5,
	}
	cs := &ContentScore{
		Category:    "comedy",
		EnergyLevel: 0.9,
	}
	applyNegativeFeedbackToProfile(profile, "block", cs, []string{"loud"})
	if profile.CategoryAffinity["comedy"] >= 0.5 {
		t.Errorf("block should have dropped comedy affinity; got %v", profile.CategoryAffinity["comedy"])
	}
}

func TestPeak3_RepeatNegativesAddToAvoidedCategories(t *testing.T) {
	profile := &UserProfile{
		UserID:           "u3b",
		CategoryAffinity: map[string]float64{"fitness": 0.0},
	}
	cs := &ContentScore{Category: "fitness", EnergyLevel: 0.7}
	// Five blocks should drive affinity below -0.3 and add to AvoidedCategories.
	for i := 0; i < 5; i++ {
		applyNegativeFeedbackToProfile(profile, "block", cs, nil)
	}
	if !containsCI(profile.AvoidedCategories, "fitness") {
		t.Errorf("expected fitness in AvoidedCategories, got %v", profile.AvoidedCategories)
	}
}

// ── Peak #4: Engagement latency weighting ───────────────────────────────────

func TestPeak4_LatencyMonotonicAndBounded(t *testing.T) {
	fast := engagementLatencyWeight(500)
	mid := engagementLatencyWeight(5000)
	slow := engagementLatencyWeight(20000)
	if !(fast > mid && mid > slow) {
		t.Errorf("latency weight should decrease with delay: fast=%v mid=%v slow=%v", fast, mid, slow)
	}
	for _, lat := range []int{0, 100, 5000, 30000, 60000} {
		w := engagementLatencyWeight(lat)
		if w < engLatencyMinWeight-1e-9 || w > engLatencyMaxWeight+1e-9 {
			t.Errorf("weight out of bound at %dms: %v", lat, w)
		}
	}
}

func TestPeak4_ZeroLatencyNeutral(t *testing.T) {
	if w := engagementLatencyWeight(0); w != 1.0 {
		t.Errorf("unknown latency should map to neutral 1.0, got %v", w)
	}
}

// ── Peak #5: Per-creator residual calibration ───────────────────────────────

func TestPeak5_CreatorResidualWarmupGate(t *testing.T) {
	resetRedis(t)
	if adj := creatorResidualAdjustment("warmup-creator"); adj != 0 {
		t.Errorf("expected 0 before warmup, got %v", adj)
	}
}

func TestPeak5_CreatorResidualCorrectsOverServed(t *testing.T) {
	resetRedis(t)
	// Simulate a creator who's predicted to engage 0.8 but actually 0.2 — over-served.
	for i := 0; i < creatorResidualMinSamples+5; i++ {
		observeCreatorResidual("over-served", 0.8, 0.2)
	}
	adj := creatorResidualAdjustment("over-served")
	if !(adj < 0) {
		t.Errorf("over-served creator should get a negative score adjustment, got %v", adj)
	}
	if adj < -creatorResidualMaxAdjust-1e-9 {
		t.Errorf("adjustment exceeded bound, got %v", adj)
	}
}

func TestPeak5_CreatorResidualBoostsUnderServed(t *testing.T) {
	resetRedis(t)
	for i := 0; i < creatorResidualMinSamples+5; i++ {
		observeCreatorResidual("under-served", 0.2, 0.8)
	}
	adj := creatorResidualAdjustment("under-served")
	if !(adj > 0) {
		t.Errorf("under-served creator should get a positive score adjustment, got %v", adj)
	}
}

// ── Peak #6: Session trajectory predictor ───────────────────────────────────

func TestPeak6_TrajectoryRequiresWarmup(t *testing.T) {
	resetSessionTrajectories()
	bonus := trajectoryBonus(CohortEngaged, "music:med", "music:med")
	if bonus != 0 {
		t.Errorf("cold trajectory should give 0 bonus, got %v", bonus)
	}
}

func TestPeak6_TrajectoryLearnsTransitions(t *testing.T) {
	resetSessionTrajectories()
	// Drive 10 transitions: comedy → music dominates.
	for i := 0; i < 10; i++ {
		noteSessionTransition(CohortEngaged, "comedy:med", "music:high")
	}
	// One off-pattern transition.
	noteSessionTransition(CohortEngaged, "comedy:med", "fitness:high")
	bonusMatching := trajectoryBonus(CohortEngaged, "comedy:med", "music:high")
	bonusOff := trajectoryBonus(CohortEngaged, "comedy:med", "fitness:high")
	if !(bonusMatching > bonusOff) {
		t.Errorf("learned transition should boost more than off-pattern: match=%v off=%v", bonusMatching, bonusOff)
	}
}

// ── Peak #7: Mood transition graph ──────────────────────────────────────────

func TestPeak7_MoodSeedPriorBoostsHealthyNext(t *testing.T) {
	resetMoodTransitions()
	// "frustrated" → "peaceful" is in the seed prior.
	bonus := moodTransitionBonus("frustrated", []string{"peaceful"})
	if !(bonus > 0) {
		t.Errorf("seed prior should boost healthy-next mood, got %v", bonus)
	}
}

func TestPeak7_MoodLearnedOverridesSeed(t *testing.T) {
	resetMoodTransitions()
	// Train: frustrated → intense was BAD outcome, repeatedly.
	for i := 0; i < 20; i++ {
		observeMoodTransition("frustrated", "intense", 0.0)
	}
	bonusBad := moodTransitionBonus("frustrated", []string{"intense"})
	if !(bonusBad < 0) {
		t.Errorf("learned-bad transition should yield negative bonus, got %v", bonusBad)
	}
}

// ── Peak #8: Per-cohort source blending ─────────────────────────────────────

func TestPeak8_SourceWeightsSumToOne(t *testing.T) {
	resetRedis(t)
	resetCohortBlend()
	for _, c := range []Cohort{CohortColdStart, CohortNew, CohortEngaged, CohortPower, CohortAtRisk} {
		w := effectiveSourceWeights(c)
		var sum float64
		for _, v := range w {
			sum += v
			if v < cohortBlendMinWeight-1e-9 {
				t.Errorf("cohort %s source weight below floor: %v", c, v)
			}
		}
		if abs8(sum-1.0) > 1e-9 {
			t.Errorf("cohort %s weights do not sum to 1: got %v", c, sum)
		}
	}
}

func TestPeak8_SourceRewardsShiftsWeights(t *testing.T) {
	resetRedis(t)
	resetCohortBlend()
	// Reward "embedding" hard in the engaged cohort.
	for i := 0; i < 200; i++ {
		observeSourceReward(CohortEngaged, "embedding", cohortBlendRewardPositive)
	}
	w := effectiveSourceWeights(CohortEngaged)
	if w["embedding"] <= defaultSourceWeights["embedding"] {
		t.Errorf("rewarded source should have grown beyond default; default=%v got=%v",
			defaultSourceWeights["embedting"], w["embedding"])
	}
}

// ── helpers ────────────────────────────────────────────────────────────────

func resetBayesianStore() {
	bayesianLTR.mu.Lock()
	defer bayesianLTR.mu.Unlock()
	bayesianLTR.byCoh = make(map[Cohort]*bayesianStats)
	bayesianLTR.loaded = false
}

func abs8(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}

// avoid unused warning on strings.
var _ = strings.ToLower
