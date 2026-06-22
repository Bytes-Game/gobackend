package main

import (
	"math/rand"
	"strconv"
	"testing"
	"time"
)

// ─────────────────────────────────────────────────────────────────────────────
// GAPS 1–6 — focused safety-net tests
//
// Each gap gets a small, isolated test so a future change that breaks the
// invariant fails loudly rather than silently degrading the algorithm.
// ─────────────────────────────────────────────────────────────────────────────

// ── Gap 1: Cold-start bootstrap mix ──────────────────────────────────────────

func TestGap1_BootstrapMixDecaysToZero(t *testing.T) {
	cases := []struct {
		events int
		want   float64
	}{
		// Threshold raised to 60 to smooth the cold→warm handoff (no hard cliff
		// at coldStartThreshold=15); decay is linear from 0.50 at 0 to 0 at 60.
		{0, 0.50},
		{15, 0.375},
		{30, 0.25},
		{45, 0.125},
		{60, 0},
		{100, 0},
		{-1, 0.50}, // negative input clamps to "fully cold"
	}
	for _, c := range cases {
		got := userBootstrapMix(c.events)
		if abs6(got-c.want) > 1e-9 {
			t.Errorf("userBootstrapMix(%d): got %v, want %v", c.events, got, c.want)
		}
	}
}

func TestGap1_WilsonLowerBoundPenalizesLowSampleSize(t *testing.T) {
	// 100% rate from 1 trial should score lower than 90% from 100 trials.
	low := wilsonLowerBound(1, 1)    // 1/1
	high := wilsonLowerBound(90, 100) // 90/100
	if !(high > low) {
		t.Errorf("Wilson should reward sample size: 90/100=%.3f vs 1/1=%.3f", high, low)
	}
	// 0 trials → 0 score (no division by zero).
	if z := wilsonLowerBound(0, 0); z != 0 {
		t.Errorf("0 trials should yield 0, got %v", z)
	}
}

// ── Gap 2: Realtime trending — cumulative weights, decay via prune ───────────

func TestGap2_TrendingHighEngagementOutranksLow(t *testing.T) {
	resetRedis(t)
	// "viral" gets a share (weight 5), "meh" gets a partial view (weight 0).
	// Viral should rank first.
	noteTrendingEvent("post", "viral", "share", 0)
	noteTrendingEvent("post", "meh", "view", 0.3) // bounce — weight 0
	noteTrendingEvent("post", "decent", "like", 0)

	entries := fetchTrendingRealtime(10)
	if len(entries) < 2 {
		t.Fatalf("expected at least 2 entries, got %d", len(entries))
	}
	if entries[0].ID != "viral" {
		t.Errorf("share should rank above like, got %q first", entries[0].ID)
	}
	// "meh" got weight 0 → never inserted → must not appear.
	for _, e := range entries {
		if e.ID == "meh" {
			t.Errorf("zero-weight events should not enter the ZSET")
		}
	}
}

func TestGap2_TrendingNegativeWeightsSuppress(t *testing.T) {
	resetRedis(t)
	// Like → +3, report → -8. Net = -5, well below the positive floor →
	// item should not surface in fetchTrendingRealtime.
	noteTrendingEvent("post", "spam-x", "like", 0)
	noteTrendingEvent("post", "spam-x", "report", 0)
	entries := fetchTrendingRealtime(10)
	for _, e := range entries {
		if e.ID == "spam-x" {
			t.Errorf("reported item should not surface as trending, but appeared: %v", e)
		}
	}
}

func TestGap2_PrunerAppliesDecay(t *testing.T) {
	resetRedis(t)
	noteTrendingEvent("post", "decay-test", "share", 0) // weight 5
	before := fetchTrendingRealtime(5)
	if len(before) == 0 {
		t.Fatal("expected entry in ZSET")
	}
	startScore := before[0].Score
	// Apply many prune ticks (~ several half-lives worth of decay).
	for i := 0; i < 20; i++ {
		pruneTrendingRealtime(nil)
	}
	after := fetchTrendingRealtime(5)
	if len(after) == 0 {
		// 20 prunes = 1 half-life → should still be above floor (~2.5).
		// If everything dropped, decay was too aggressive.
		t.Fatalf("after 20 prunes (1 half-life), entry should still survive")
	}
	if !(after[0].Score < startScore) {
		t.Errorf("score should decay over time: before=%v after=%v", startScore, after[0].Score)
	}
	// Sanity: ~1 half-life later the score should be roughly half (loose check).
	if !(after[0].Score < startScore*0.6) {
		t.Errorf("score should be ~halved after one half-life: before=%v after=%v", startScore, after[0].Score)
	}
}

// ── Gap 3: Watch-ratio prediction head ───────────────────────────────────────

func TestGap3_WatchRatioWarmupGate(t *testing.T) {
	resetRedis(t)
	resetWR()
	// Below the warmup threshold, predictions must be exactly 0.
	bonus := wrPredictBonus(CohortEngaged, map[string]float64{"quality": 0.9})
	if bonus != 0 {
		t.Errorf("expected 0 bonus before warmup, got %v", bonus)
	}
}

func TestGap3_WatchRatioMonotonicAfterTraining(t *testing.T) {
	resetRedis(t)
	resetWR()
	// Train with strong signal: high quality → high watch ratio, low → low.
	for i := 0; i < 80; i++ {
		wrObserve(CohortEngaged, map[string]float64{"quality": 1.0}, 0.95)
		wrObserve(CohortEngaged, map[string]float64{"quality": -1.0}, 0.10)
	}
	highQ := wrPredictBonus(CohortEngaged, map[string]float64{"quality": 1.0})
	lowQ := wrPredictBonus(CohortEngaged, map[string]float64{"quality": -1.0})
	if !(highQ > lowQ) {
		t.Errorf("after training, high-quality bonus must exceed low: highQ=%v lowQ=%v", highQ, lowQ)
	}
	// Bonus must be inside the safety bound.
	if highQ > wrMaxBonus+1e-6 || lowQ < -wrMaxBonus-1e-6 {
		t.Errorf("bonus exceeded ±%v cap: highQ=%v lowQ=%v", wrMaxBonus, highQ, lowQ)
	}
}

// ── Gap 4: MMR creator-level penalty ─────────────────────────────────────────

func TestGap4_MMRPenalizesRepeatCreator(t *testing.T) {
	// Three items, all near-identical embeddings (so embedding-based MMR
	// alone would just sort by score). Two share creator C1, one is by C2.
	// With creator penalty active, we expect C2 to be lifted ahead of the
	// second C1 item even though its raw score is lower.
	mkItem := func(id, creator string, score float64) ScoredItem {
		return ScoredItem{
			Item: HomeFeedItem{
				Type: "challenge",
				Challenge: &Challenge{ID: id, CreatorID: creator},
			},
			Score: score,
		}
	}
	items := []ScoredItem{
		mkItem("a1", "C1", 0.9),
		mkItem("a2", "C1", 0.85),
		mkItem("b1", "C2", 0.8),
	}
	// Inject a constant embedding so similarity penalty is a no-op
	// — this isolates the creator-penalty signal.
	embed := func(_ ScoredItem) []float64 {
		v := make([]float64, embedDim)
		v[0] = 1
		return v
	}
	out := applyMMRWithCreator(items, mmrLambda, len(items), embed, defaultCreatorOf)
	if out[0].Item.Challenge.ID != "a1" {
		t.Fatalf("highest-score item should still lead, got %q", out[0].Item.Challenge.ID)
	}
	// With penalty mmrCreatorPenalty=0.18 and items 0.85 vs 0.8:
	// a2 effective = 0.72*0.85 - 0.28*1 - 0.18 = -0.0480
	// b1 effective = 0.72*0.80 - 0.28*1 - 0   =  0.296
	// b1 wins by a wide margin.
	if out[1].Item.Challenge.ID != "b1" {
		t.Errorf("creator penalty should lift b1 ahead of second a-creator pick; got %q", out[1].Item.Challenge.ID)
	}
}

// ── Gap 5: Bandit exploration floor ──────────────────────────────────────────

func TestGap5_ExplorationFloorPreventsCollapse(t *testing.T) {
	// Construct softmax weights where one arm dominates badly.
	weights := map[string]float64{
		"winner":   0.99,
		"runnerup": 0.005,
		"loser":    0.005,
	}
	applyExplorationFloor(weights, banditExplorationFloor)
	// Every arm must be ≥ the floor.
	for k, v := range weights {
		if v < banditExplorationFloor-1e-9 {
			t.Errorf("arm %q below floor: %v < %v", k, v, banditExplorationFloor)
		}
	}
	// Total mass must still sum to 1.
	var sum float64
	for _, v := range weights {
		sum += v
	}
	if abs6(sum-1.0) > 1e-9 {
		t.Errorf("post-floor weights must sum to 1, got %v", sum)
	}
	// Winner must still be the largest.
	if weights["winner"] <= weights["runnerup"] || weights["winner"] <= weights["loser"] {
		t.Errorf("relative ordering broken after floor: %v", weights)
	}
}

func TestGap5_SoftMixEnforcesFloorIntegrated(t *testing.T) {
	resetRedis(t)
	b := newBandit()
	// Heavily skew one arm so its Beta posterior dominates.
	for i := 0; i < 500; i++ {
		b.updateArm("u-floor", "winner", 1.0)
		b.updateArm("u-floor", "loser", 0.0)
	}
	rnd := rand.New(rand.NewSource(7))
	weights := b.softMix([]string{"winner", "loser", "newcomer"}, rnd)
	for k, v := range weights {
		if v < banditExplorationFloor-1e-9 {
			t.Errorf("softMix arm %q below floor: %v", k, v)
		}
	}
}

// ── Gap 6: Embedding cache invalidation ──────────────────────────────────────

func TestGap6_InvalidateContentEmbeddingDeletesKey(t *testing.T) {
	resetRedis(t)
	cs := &ContentScore{
		ContentID:    "inv-1",
		Category:     "music",
		CreatorID:    "creator-x",
		QualityScore: 0.5,
		EnergyLevel:  0.4,
		CreatedAt:    time.Now(),
	}
	// Warm the cache.
	_ = getOrBuildContentEmbedding(cs, []string{"chill"})
	if rdb == nil {
		t.Skip("redis not available")
	}
	if v, _ := rdb.Get(rctx, contentEmbedRedisKey+"inv-1").Result(); v == "" {
		t.Fatal("expected cache to be populated")
	}
	invalidateContentEmbedding("inv-1")
	if _, err := rdb.Get(rctx, contentEmbedRedisKey+"inv-1").Result(); err == nil {
		t.Error("expected cache key to be deleted after invalidate")
	}
}

// ── helpers ──────────────────────────────────────────────────────────────────

func abs6(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}

// resetWR clears the watch-ratio store between tests.
func resetWR() {
	watchRatio.mu.Lock()
	watchRatio.byCoh = make(map[Cohort]*wrModel)
	watchRatio.dirty = make(map[Cohort]bool)
	watchRatio.loaded = false
	watchRatio.mu.Unlock()
}

// avoid the unused-import warning if a future edit drops strconv.
var _ = strconv.Itoa
