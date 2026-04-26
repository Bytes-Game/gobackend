package main

import (
	"math/rand"
	"testing"
	"time"
)

// ─────────────────────────────────────────────────────────────────────────────
// SEVEN ALGORITHM IMPROVEMENTS — focused safety-net tests
// ─────────────────────────────────────────────────────────────────────────────

// ── #1 Bandit time-decay ────────────────────────────────────────────────────

func TestImpr1_BanditDecaysToPriorOverTime(t *testing.T) {
	resetRedis(t)
	b := newBandit()
	// Build up strong evidence on an arm.
	for i := 0; i < 50; i++ {
		b.updateArm("u-decay", "winner", 1.0)
	}
	armBefore := b.armOrDefault("winner")
	beforeAlpha := armBefore.alpha
	if beforeAlpha < 30 {
		t.Fatalf("setup: expected alpha to grow, got %v", beforeAlpha)
	}

	// Backdate the arm's last-update by 4 weeks (~2 half-lives).
	b.mu.Lock()
	armBefore.lastUpdate = time.Now().Add(-28 * 24 * time.Hour)
	b.mu.Unlock()

	// Trigger decay by recording one new (neutral-leaning) outcome.
	b.updateArm("u-decay", "winner", 0.5)
	armAfter := b.armOrDefault("winner")

	// After ~2 half-lives, the prior-distance should have shrunk by ~75%.
	// We just assert a meaningful drop, not the exact number, to stay
	// robust against future tuning.
	priorDistBefore := beforeAlpha - 1.0 // distance from (1,1) prior
	priorDistAfter := armAfter.alpha - 1.0
	if !(priorDistAfter < priorDistBefore*0.5) {
		t.Errorf("decay didn't shrink evidence enough: before=%.2f after=%.2f",
			priorDistBefore, priorDistAfter)
	}
}

func TestImpr1_FreshArmNotDecayed(t *testing.T) {
	resetRedis(t)
	b := newBandit()
	// New arm with no lastUpdate timestamp shouldn't be touched.
	a := b.armOrDefault("fresh")
	originalAlpha := a.alpha
	originalBeta := a.beta
	applyTimeDecay(a, time.Time{})
	if a.alpha != originalAlpha || a.beta != originalBeta {
		t.Errorf("zero timestamp must not decay arm: before=(%.2f,%.2f) after=(%.2f,%.2f)",
			originalAlpha, originalBeta, a.alpha, a.beta)
	}
}

// ── #2 Cohort-aware exploration floor ───────────────────────────────────────

func TestImpr2_CohortFloorOrder(t *testing.T) {
	// Cold > new > engaged; power < engaged; at_risk highest.
	cold := cohortExplorationFloor(CohortColdStart)
	new_ := cohortExplorationFloor(CohortNew)
	eng := cohortExplorationFloor(CohortEngaged)
	pwr := cohortExplorationFloor(CohortPower)
	atr := cohortExplorationFloor(CohortAtRisk)
	if !(cold > new_ && new_ > eng && eng > pwr) {
		t.Errorf("floor ordering broken: cold=%v new=%v eng=%v pwr=%v", cold, new_, eng, pwr)
	}
	if !(atr > eng) {
		t.Errorf("at_risk should explore more than engaged: atr=%v eng=%v", atr, eng)
	}
}

func TestImpr2_SoftMixForCohortEnforcesCohortFloor(t *testing.T) {
	resetRedis(t)
	b := newBandit()
	// Strong skew: one winner, several near-zero arms.
	for i := 0; i < 200; i++ {
		b.updateArm("u-cf", "winner", 1.0)
		b.updateArm("u-cf", "loser", 0.0)
	}
	rnd := rand.New(rand.NewSource(11))
	weights := b.softMixForCohort([]string{"winner", "loser", "scout"}, CohortColdStart, rnd)
	floor := cohortExplorationFloor(CohortColdStart)
	for k, v := range weights {
		if v < floor-1e-9 {
			t.Errorf("cold-start arm %q below cold floor %v: %v", k, floor, v)
		}
	}
}

// ── #3 Position-aware MMR lambda ────────────────────────────────────────────

func TestImpr3_PositionLambdaRamp(t *testing.T) {
	topK := 10
	headLam := positionLambda(0, topK)
	midLam := positionLambda(5, topK)
	tailLam := positionLambda(9, topK)

	if !(headLam < midLam && midLam < tailLam) {
		t.Errorf("lambda should ramp head→tail: head=%v mid=%v tail=%v", headLam, midLam, tailLam)
	}
	const eps = 1e-9
	if abs7(headLam-mmrLambdaHead) > eps {
		t.Errorf("head lambda should equal mmrLambdaHead %v, got %v", mmrLambdaHead, headLam)
	}
	if abs7(tailLam-mmrLambdaTail) > eps {
		t.Errorf("tail lambda should equal mmrLambdaTail %v, got %v", mmrLambdaTail, tailLam)
	}
}

func abs7(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}

// ── #4 Hour-of-day routing ──────────────────────────────────────────────────

func TestImpr4_CategoryHourBoostExactMatch(t *testing.T) {
	now := time.Date(2026, 4, 25, 21, 0, 0, 0, time.UTC) // 9pm
	profile := &UserProfile{
		CategoryByHour: map[int]string{21: "music"},
	}
	b := categoryHourBoost(profile, "music", now)
	if b != hourCategoryMaxBoost {
		t.Errorf("exact match should give full boost %v, got %v", hourCategoryMaxBoost, b)
	}
	mismatch := categoryHourBoost(profile, "fitness", now)
	if mismatch != 0 {
		t.Errorf("category mismatch should be 0, got %v", mismatch)
	}
}

func TestImpr4_CategoryHourBoostAdjacentMatch(t *testing.T) {
	now := time.Date(2026, 4, 25, 21, 0, 0, 0, time.UTC)
	profile := &UserProfile{
		CategoryByHour: map[int]string{20: "music"}, // 1 hour off
	}
	b := categoryHourBoost(profile, "music", now)
	want := hourCategoryMaxBoost * 0.5
	if b != want {
		t.Errorf("adjacent match should give %v, got %v", want, b)
	}
}

func TestImpr4_EnergyHourMatchClose(t *testing.T) {
	now := time.Date(2026, 4, 25, 21, 0, 0, 0, time.UTC)
	profile := &UserProfile{EnergyByHour: map[int]float64{21: 0.30}}
	close := energyHourMatch(profile, 0.32, now) // diff 0.02 → in tight band
	if close != hourEnergyMaxBoost {
		t.Errorf("close energy should give full boost %v, got %v", hourEnergyMaxBoost, close)
	}
	far := energyHourMatch(profile, 0.95, now) // diff 0.65 → past linear taper
	if far >= 0 {
		t.Errorf("far-energy match should yield negative pull, got %v", far)
	}
}

func TestImpr4_TimeContextStrategyHints(t *testing.T) {
	morning := time.Date(2026, 4, 25, 8, 0, 0, 0, time.UTC)
	night := time.Date(2026, 4, 26, 1, 0, 0, 0, time.UTC)
	mh := timeContextStrategyHints(morning)
	nh := timeContextStrategyHints(night)
	if len(mh) == 0 || len(nh) == 0 {
		t.Fatalf("strategy hints should be non-empty: morning=%v night=%v", mh, nh)
	}
	// Morning hints should not include night-specific calming/nostalgic.
	for _, s := range mh {
		if s == strategyCalming || s == strategyNostalgic {
			t.Errorf("morning hints should not include night strategy %q", s)
		}
	}
	// Night hints should include calming.
	hasCalm := false
	for _, s := range nh {
		if s == strategyCalming {
			hasCalm = true
		}
	}
	if !hasCalm {
		t.Errorf("night hints should include calming, got %v", nh)
	}
}

// ── #5 Engagement quality weighting ─────────────────────────────────────────

func TestImpr5_EngagementQualityClampsAndCaches(t *testing.T) {
	resetRedis(t)
	// With db nil, computeEngagementQuality returns 1.0 — verify caching path.
	q := userEngagementQuality("u-eq")
	if q != 1.0 {
		t.Errorf("expected neutral 1.0 with no DB, got %v", q)
	}
	// Verify cache populated.
	v, err := rdb.Get(rctx, engQualityRedisKey+"u-eq").Result()
	if err != nil || v == "" {
		t.Errorf("expected cache to be populated, got err=%v v=%q", err, v)
	}
}

func TestImpr5_TrendingUsesEngagementQuality(t *testing.T) {
	resetRedis(t)
	// Seed user multiplier directly in the cache to control the test.
	_ = rdb.Set(rctx, engQualityRedisKey+"u-trusted", "1.80", time.Hour).Err()
	_ = rdb.Set(rctx, engQualityRedisKey+"u-spammy", "0.30", time.Hour).Err()
	// Same event, different users → different score deltas.
	noteTrendingEventByUser("u-trusted", "post", "boost-A", "like", 0)
	noteTrendingEventByUser("u-spammy", "post", "boost-B", "like", 0)
	entries := fetchTrendingRealtime(10)
	scoreA, scoreB := 0.0, 0.0
	for _, e := range entries {
		if e.ID == "boost-A" {
			scoreA = e.Score
		}
		if e.ID == "boost-B" {
			scoreB = e.Score
		}
	}
	if !(scoreA > scoreB*2) {
		t.Errorf("trusted user's vote should weigh more than 2x spammy's: A=%v B=%v", scoreA, scoreB)
	}
}

// ── #6 Surprise injection ───────────────────────────────────────────────────

func TestImpr6_SurpriseInjectionSkippedAtRisk(t *testing.T) {
	items := makeStubScoredItems(15, "comedy", "creator1")
	profile := &UserProfile{UserID: "u-atrisk", CategoryAffinity: map[string]float64{"comedy": 0.9}}
	rnd := rand.New(rand.NewSource(1))
	out := applySurpriseInjection(items, profile, CohortAtRisk, rnd)
	if len(out) != len(items) {
		t.Errorf("at_risk users should be exempt; got %d items vs input %d", len(out), len(items))
	}
}

func TestImpr6_SurpriseInjectionRespectsProbability(t *testing.T) {
	// With a seed that produces float > 0.10, no injection should happen.
	items := makeStubScoredItems(15, "comedy", "creator1")
	profile := &UserProfile{UserID: "u-test", CategoryAffinity: map[string]float64{"comedy": 0.9}}
	skipped := 0
	for seed := int64(1); seed <= 100; seed++ {
		out := applySurpriseInjection(items, profile, CohortEngaged, rand.New(rand.NewSource(seed)))
		if len(out) == len(items) {
			skipped++
		}
	}
	// Roughly 90% of seeds should skip injection (10% probability).
	// Allow wide variance — bigger sample would tighten this.
	if skipped < 75 {
		t.Errorf("expected ~90 skips out of 100, got %d", skipped)
	}
}

func makeStubScoredItems(n int, category, creator string) []ScoredItem {
	out := make([]ScoredItem, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, ScoredItem{
			Item: HomeFeedItem{
				Type: "challenge",
				Challenge: &Challenge{
					ID: "stub-" + intStr(i), CreatorID: creator,
				},
			},
			Score: 1.0 - float64(i)*0.01,
		})
	}
	return out
}

func intStr(i int) string {
	s := ""
	if i == 0 {
		return "0"
	}
	for i > 0 {
		s = string(byte('0'+i%10)) + s
		i /= 10
	}
	return s
}

// ── #7 Session-level cross-page diversity ───────────────────────────────────

func TestImpr7_DiversityPenaltySuperlinear(t *testing.T) {
	cases := []struct {
		n     int
		minOK float64
		maxOK float64
	}{
		{1, 0, 0},                                // first appearance — free
		{2, sessionDiversityPenaltyBase, sessionDiversityPenaltyBase},
		{3, 4 * sessionDiversityPenaltyBase, 4 * sessionDiversityPenaltyBase},
		{4, 9 * sessionDiversityPenaltyBase, 9 * sessionDiversityPenaltyBase},
		{10, sessionDiversityMaxPenalty, sessionDiversityMaxPenalty},
	}
	for _, c := range cases {
		got := diversityPenaltyForCount(c.n)
		if got < c.minOK || got > c.maxOK {
			t.Errorf("diversityPenaltyForCount(%d) want in [%v,%v], got %v", c.n, c.minOK, c.maxOK, got)
		}
	}
}

func TestImpr7_NoteAndLoadSessionCategories(t *testing.T) {
	resetRedis(t)
	noteSessionCategories("sess-X", []string{"comedy", "comedy", "music", "skill"})
	counts := loadSessionCategoryCounts(rctx, "sess-X")
	if counts["comedy"] != 2 {
		t.Errorf("comedy count: want 2, got %d", counts["comedy"])
	}
	if counts["music"] != 1 || counts["skill"] != 1 {
		t.Errorf("non-comedy counts wrong: %v", counts)
	}
	// Adding more should accumulate.
	noteSessionCategories("sess-X", []string{"comedy"})
	counts = loadSessionCategoryCounts(rctx, "sess-X")
	if counts["comedy"] != 3 {
		t.Errorf("after second note, comedy should be 3, got %d", counts["comedy"])
	}
}
