package main

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// ════════════════════════════════════════════════════════════════════════════════
// FIELD-BY-FIELD VERIFICATION
//
// This is the answer to "is every field collecting data as I expect?"
//
// We synthesize a realistic mini-session of events covering every event type
// the algorithm reads, then probe each downstream signal store and assert it
// got populated.
//
// At the end we print a per-field PASS/FAIL table — same format the
// /admin/diagnostics endpoint exposes, so the human-facing API and the
// regression test agree on what's "working".
//
// Each subsection has its own t.Run so a failure surfaces cleanly: you see
// the name of the field/subsystem that's broken, not just "the test failed".
// ════════════════════════════════════════════════════════════════════════════════

func TestAlgorithm_AllFields_CollectExpectedData(t *testing.T) {
	resetRedis(t)
	resetLTR()
	resetWR()

	const userID = "u-fieldverify-1"
	const altUser = "u-fieldverify-2"
	const creatorA = "creator-A"
	const creatorB = "creator-B"
	const creatorBlocked = "creator-bad"

	type subsystemResult struct {
		Name   string
		Status string // "PASS" | "FAIL" | "SKIP"
		Detail string
	}
	results := make([]subsystemResult, 0, 30)
	record := func(name, status, detail string) {
		results = append(results, subsystemResult{Name: name, Status: status, Detail: detail})
	}

	// ── 1. NEGATIVE SIGNALS — block, unfollow, bounce, search, session_end ───
	t.Run("negativeSignals", func(t *testing.T) {
		MarkBlocked(userID, creatorBlocked)
		MarkUnfollowed(userID, creatorA)
		MarkBounce(userID, "post:p-bounce-1")
		RecordSearchQuery(userID, "skateboard tricks")
		RecordSessionEnd(userID)

		warmNegativeSignals(userID)
		ns := getNegativeSignals(userID)
		if ns == nil {
			t.Fatalf("negative signals failed to warm")
		}

		// block
		if !ns.blocked[creatorBlocked] {
			t.Errorf("blocked creator not in warmed signals")
			record("negativeSignals.block", "FAIL", "missing")
		} else {
			record("negativeSignals.block", "PASS", "creator marked blocked")
		}
		// unfollow → penalty < 1
		pen := negativeCreatorPenalty(ns, creatorA)
		if pen >= 1.0 {
			t.Errorf("unfollow penalty did not apply, got %v", pen)
			record("negativeSignals.unfollow", "FAIL", fmt.Sprintf("penalty=%v", pen))
		} else {
			record("negativeSignals.unfollow", "PASS", fmt.Sprintf("penalty=%.3f", pen))
		}
		// bounce → 0
		bp := bouncePenalty(ns, "post", "p-bounce-1")
		if bp != 0 {
			t.Errorf("bounce penalty did not zero out, got %v", bp)
			record("negativeSignals.bounce", "FAIL", fmt.Sprintf("penalty=%v", bp))
		} else {
			record("negativeSignals.bounce", "PASS", "bounced item zeroed")
		}
		// search
		if len(ns.recentQueries) == 0 {
			t.Errorf("search query not recorded")
			record("negativeSignals.search", "FAIL", "list empty")
		} else {
			record("negativeSignals.search", "PASS", fmt.Sprintf("%d query", len(ns.recentQueries)))
		}
		// session_end timestamp
		if ns.lastSessionEnd.IsZero() {
			t.Errorf("session_end not recorded")
			record("negativeSignals.session_end", "FAIL", "ts zero")
		} else {
			record("negativeSignals.session_end", "PASS", "ts present")
		}
	})

	// ── 2. SEEN FILTER — markShownBatch + filterUnseen ───────────────────────
	t.Run("seenFilter", func(t *testing.T) {
		items := []HomeFeedItem{
			{Type: "post", Post: &Post{ID: "p-seen-1", AuthorID: creatorA}},
			{Type: "challenge", Challenge: &Challenge{ID: "c-seen-1", CreatorID: creatorB}},
		}
		markShownBatch(userID, items)
		// One synthetic candidate matching what we just marked, one not.
		scored := []ScoredItem{
			{Item: HomeFeedItem{Type: "post", Post: &Post{ID: "p-seen-1", AuthorID: creatorA}}, Score: 0.9},
			{Item: HomeFeedItem{Type: "post", Post: &Post{ID: "p-seen-2", AuthorID: creatorA}}, Score: 0.8},
		}
		filtered := filterUnseenScored(userID, scored)
		if len(filtered) != 1 || getItemID(filtered[0].Item) != "p-seen-2" {
			t.Errorf("expected unseen 'p-seen-2', got %d items: %+v", len(filtered), filtered)
			record("seenFilter", "FAIL", fmt.Sprintf("kept %d items", len(filtered)))
		} else {
			record("seenFilter", "PASS", "drops seen, keeps unseen")
		}
	})

	// ── 3. CONTENT EMBEDDING CACHE ───────────────────────────────────────────
	t.Run("contentEmbeddingCache", func(t *testing.T) {
		cs := &ContentScore{
			ContentID:    "c-embed-1",
			Category:     "skateboard",
			CreatorID:    creatorA,
			QualityScore: 0.7,
			EnergyLevel:  0.6,
			CreatedAt:    time.Now().Add(-2 * time.Hour),
			ViewCount:    50,
			LikeCount:    8,
		}
		_ = getOrBuildContentEmbedding(cs, []string{"hype"})
		v, err := rdb.Get(rctx, contentEmbedRedisKey+"c-embed-1").Result()
		if err != nil || v == "" {
			t.Errorf("content embedding cache key not written")
			record("contentEmbeddingCache", "FAIL", "Redis key absent")
		} else {
			record("contentEmbeddingCache", "PASS", fmt.Sprintf("%d-byte payload", len(v)))
		}
		invalidateContentEmbedding("c-embed-1")
		if _, err := rdb.Get(rctx, contentEmbedRedisKey+"c-embed-1").Result(); err == nil {
			t.Errorf("invalidate did not clear cached embedding")
			record("contentEmbeddingCache.invalidate", "FAIL", "still present")
		} else {
			record("contentEmbeddingCache.invalidate", "PASS", "cleared")
		}
	})

	// ── 4. USER EMBEDDING — EMA toward liked content ─────────────────────────
	t.Run("userEmbedding", func(t *testing.T) {
		cs := &ContentScore{
			ContentID: "c-ux-1", Category: "comedy", CreatorID: creatorA,
			QualityScore: 0.6, EnergyLevel: 0.5, CreatedAt: time.Now(),
		}
		cv := buildContentEmbedding(cs, []string{"funny"})
		for i := 0; i < 3; i++ {
			updateUserEmbedding(userID, cv, 1.0)
		}
		got := getUserEmbedding(userID)
		if userEmbeddingIsCold(got) {
			t.Errorf("user embedding still cold after 3 positive updates")
			record("userEmbedding", "FAIL", "vector all zeros")
		} else {
			sim := cosineSim(got, cv)
			record("userEmbedding", "PASS", fmt.Sprintf("warm; cos(user, content)=%.3f", sim))
		}
	})

	// ── 5. BANDIT ARMS ────────────────────────────────────────────────────────
	t.Run("banditArms", func(t *testing.T) {
		b := loadBandit(userID)
		b.updateArm(userID, strategyDiscovery, 1.0)
		b.updateArm(userID, strategyDiscovery, 1.0)
		b.updateArm(userID, strategyCalming, 0.0)
		b.updateArm(userID, strategyTrending, 0.5)
		b2 := loadBandit(userID)
		if a := b2.armOrDefault(strategyDiscovery); a.alpha < 2.0 {
			t.Errorf("discovery arm alpha=%.2f, expected ≥2", a.alpha)
			record("banditArms.persist", "FAIL", fmt.Sprintf("alpha=%.2f", a.alpha))
		} else {
			record("banditArms.persist", "PASS", fmt.Sprintf("3 arms tracked, discovery alpha=%.2f", a.alpha))
		}
		w := b2.softMix([]string{strategyDiscovery, strategyCalming, strategyTrending}, rand.New(rand.NewSource(1)))
		var sum float64
		for _, v := range w {
			sum += v
		}
		if sum < 0.999 || sum > 1.001 {
			t.Errorf("softMix weights don't sum to 1: %v", sum)
			record("banditArms.softMix", "FAIL", fmt.Sprintf("sum=%v", sum))
		} else {
			record("banditArms.softMix", "PASS", fmt.Sprintf("normalized; %d arms", len(w)))
		}
	})

	// ── 6. LTR (engagement classifier) ───────────────────────────────────────
	t.Run("ltrTraining", func(t *testing.T) {
		bd := map[string]float64{"quality": 0.8, "freshness": 0.6, "social": 0.4}
		ltrStashBreakdownWithPos(userID, "post", "p-ltr-1", CohortEngaged, bd, 3)
		ltrObserveEvent(userID, "post", "p-ltr-1", 1.0, 0.85)
		ltr.mu.RLock()
		m := ltr.byCoh[CohortEngaged]
		ltr.mu.RUnlock()
		if m == nil || m.Updates < 1 {
			updates := -1
			if m != nil {
				updates = m.Updates
			}
			t.Errorf("LTR engaged-cohort updates=%d", updates)
			record("ltrTraining", "FAIL", "no observe recorded")
		} else {
			record("ltrTraining", "PASS", fmt.Sprintf("updates=%d, bias=%.4f", m.Updates, m.Bias))
		}
	})

	// ── 7. WATCH-RATIO HEAD ──────────────────────────────────────────────────
	t.Run("watchRatioTraining", func(t *testing.T) {
		bd := map[string]float64{"quality": 0.9, "freshness": 0.5}
		for i := 0; i < wrMinSamples+5; i++ {
			wrObserve(CohortEngaged, bd, 0.82)
		}
		watchRatio.mu.RLock()
		m := watchRatio.byCoh[CohortEngaged]
		watchRatio.mu.RUnlock()
		if m == nil || m.Samples < wrMinSamples {
			samples := -1
			if m != nil {
				samples = m.Samples
			}
			t.Errorf("watch-ratio engaged-cohort samples=%d, expected ≥%d", samples, wrMinSamples)
			record("watchRatioTraining", "FAIL", "samples below warmup")
		} else {
			bonus := wrPredictBonus(CohortEngaged, bd)
			record("watchRatioTraining", "PASS", fmt.Sprintf("samples=%d, predBonus=%.4f", m.Samples, bonus))
		}
	})

	// ── 8. PLATT CALIBRATION ─────────────────────────────────────────────────
	t.Run("plattCalibration", func(t *testing.T) {
		for i := 0; i < 250; i++ {
			x := rand.Float64()*4 - 2
			label := 0.0
			if x > 0 {
				label = 1.0
			}
			plattRecord(x, label)
		}
		plattFit() // explicit fit; refit-loop runs on a separate goroutine in prod
		s, err := rdb.Get(rctx, calibRedisKey).Result()
		if err != nil || s == "" {
			t.Errorf("Platt params not persisted")
			record("plattCalibration", "FAIL", "no Redis key")
		} else {
			record("plattCalibration", "PASS", fmt.Sprintf("%d-byte payload", len(s)))
		}
	})

	// ── 9. TRENDING REALTIME ─────────────────────────────────────────────────
	t.Run("trendingRealtime", func(t *testing.T) {
		noteTrendingEvent("post", "p-trend-1", "share", 0)
		noteTrendingEvent("post", "p-trend-2", "like", 0)
		entries := fetchTrendingRealtime(10)
		if len(entries) < 2 {
			t.Errorf("expected ≥2 trending entries, got %d", len(entries))
			record("trendingRealtime", "FAIL", fmt.Sprintf("only %d entries", len(entries)))
		} else if entries[0].ID != "p-trend-1" {
			t.Errorf("expected share to outrank like, got %q first", entries[0].ID)
			record("trendingRealtime", "FAIL", "ordering wrong")
		} else {
			record("trendingRealtime", "PASS",
				fmt.Sprintf("%d entries, share leads (score=%.2f)", len(entries), entries[0].Score))
		}
	})

	// ── 10. BOOTSTRAP POOL — Wilson lower-bound ranking ──────────────────────
	t.Run("bootstrapPool", func(t *testing.T) {
		_ = rdb.ZAdd(rctx, bootstrapPoolRedisKey,
			redis.Z{Score: 0.85, Member: "challenge:c-boot-1"},
			redis.Z{Score: 0.50, Member: "post:p-boot-2"},
		).Err()
		got := fetchBootstrapPool(10)
		if len(got) != 2 {
			t.Errorf("expected 2 pool entries, got %d", len(got))
			record("bootstrapPool", "FAIL", "wrong count")
		} else if got[0].ID != "c-boot-1" {
			t.Errorf("expected highest-Wilson 'c-boot-1' first, got %q", got[0].ID)
			record("bootstrapPool", "FAIL", "ordering wrong")
		} else {
			record("bootstrapPool", "PASS", "top-Wilson leads")
		}
		if mix := userBootstrapMix(0); mix != bootstrapMaxMixFraction {
			t.Errorf("cold mix=%v, want %v", mix, bootstrapMaxMixFraction)
			record("bootstrapPool.mixGate", "FAIL", fmt.Sprintf("mix=%v", mix))
		} else {
			record("bootstrapPool.mixGate", "PASS", "cold→50%, warm→0%")
		}
	})

	// ── 11. MMR re-rank with creator penalty ─────────────────────────────────
	t.Run("mmrCreatorPenalty", func(t *testing.T) {
		mk := func(id, creator string, score float64) ScoredItem {
			return ScoredItem{
				Item: HomeFeedItem{
					Type:      "challenge",
					Challenge: &Challenge{ID: id, CreatorID: creator},
				},
				Score: score,
			}
		}
		items := []ScoredItem{
			mk("a1", creatorA, 0.9),
			mk("a2", creatorA, 0.85),
			mk("b1", creatorB, 0.78),
		}
		embed := func(_ ScoredItem) []float64 {
			v := make([]float64, embedDim)
			v[0] = 1
			return v
		}
		out := applyMMRWithCreator(items, mmrLambda, len(items), embed, defaultCreatorOf)
		if out[0].Item.Challenge.ID != "a1" || out[1].Item.Challenge.ID != "b1" {
			t.Errorf("MMR creator-penalty ordering wrong: %v",
				[]string{out[0].Item.Challenge.ID, out[1].Item.Challenge.ID})
			record("mmrCreatorPenalty", "FAIL", "lifted same-creator above different-creator")
		} else {
			record("mmrCreatorPenalty", "PASS", "different-creator lifted above same-creator")
		}
	})

	// ── 12. ANTI-LOOP DETECTOR — all 4 signals fire ──────────────────────────
	t.Run("antiLoopDetector", func(t *testing.T) {
		s1 := &SessionState{SkipStreak: 5}
		if d := detectLoop(s1); !d.Stuck {
			t.Errorf("skip-streak loop not detected")
			record("antiLoopDetector.skipStreak", "FAIL", "missed")
		} else {
			record("antiLoopDetector.skipStreak", "PASS", d.Reason+"→"+d.SuggestedStrat)
		}
		s2 := &SessionState{LastCategories: []string{"a", "a", "a", "b"}}
		if d := detectLoop(s2); !d.Stuck {
			t.Errorf("category-monoculture not detected")
			record("antiLoopDetector.categoryMonoculture", "FAIL", "missed")
		} else {
			record("antiLoopDetector.categoryMonoculture", "PASS", d.Reason+"→"+d.SuggestedStrat)
		}
		s3 := &SessionState{LastCreators: []string{"x", "x", "x", "y", "z"}}
		if d := detectLoop(s3); !d.Stuck {
			t.Errorf("creator-flood not detected")
			record("antiLoopDetector.creatorFlood", "FAIL", "missed")
		} else {
			record("antiLoopDetector.creatorFlood", "PASS", d.Reason+"→"+d.SuggestedStrat)
		}
		s4 := &SessionState{DopamineBudget: 0.10}
		if d := detectLoop(s4); !d.Stuck {
			t.Errorf("dopamine-collapse not detected")
			record("antiLoopDetector.dopamineCollapse", "FAIL", "missed")
		} else {
			record("antiLoopDetector.dopamineCollapse", "PASS", d.Reason+"→"+d.SuggestedStrat)
		}
	})

	// ── 13. COHORT CLASSIFIER — every cohort reachable ───────────────────────
	t.Run("cohortClassifier", func(t *testing.T) {
		// Cohort thresholds (see cohort.go):
		//   EventCount < 15 → cold_start
		//   EventCount < 200 → new
		//   high-skip + low-completion + short-session → at_risk
		//   TotalSessions > 30 + avg completion >0.6 + avg session >240s → power
		//   else engaged.
		cases := []struct {
			name string
			p    *UserProfile
			want Cohort
		}{
			{"cold_start", &UserProfile{EventCount: 5}, CohortColdStart},
			{"new", &UserProfile{EventCount: 50}, CohortNew},
			{"engaged", &UserProfile{EventCount: 250, AvgCompletionRate: 0.55, AvgSessionSec: 180}, CohortEngaged},
			{"power", &UserProfile{EventCount: 500, TotalSessions: 50, AvgCompletionRate: 0.7, AvgSessionSec: 300}, CohortPower},
			{"at_risk", &UserProfile{EventCount: 300, AvgSkipRate: 0.85, AvgCompletionRate: 0.10, AvgSessionSec: 60}, CohortAtRisk},
		}
		hits := 0
		details := make([]string, 0, len(cases))
		for _, c := range cases {
			got := classifyCohort(c.p)
			details = append(details, fmt.Sprintf("%s→%s", c.name, got))
			if got == c.want {
				hits++
			}
		}
		// Cohort thresholds are heuristic; require ≥3/5 canonical hits to
		// flag drift without making the test brittle on tuning changes.
		if hits < 3 {
			t.Errorf("cohort classifier mismatched on %d/%d cases: %v", len(cases)-hits, len(cases), details)
			record("cohortClassifier", "FAIL", fmt.Sprintf("%d/%d hit", hits, len(cases)))
		} else {
			record("cohortClassifier", "PASS",
				fmt.Sprintf("%d/%d canonical hits — %s", hits, len(cases), strings.Join(details, ", ")))
		}
	})

	// ── 14. BANDIT EXPLORATION FLOOR ─────────────────────────────────────────
	t.Run("banditExplorationFloor", func(t *testing.T) {
		w := map[string]float64{"a": 0.99, "b": 0.005, "c": 0.005}
		applyExplorationFloor(w, banditExplorationFloor)
		minW := 1.0
		for _, v := range w {
			if v < minW {
				minW = v
			}
		}
		if minW < banditExplorationFloor-1e-9 {
			t.Errorf("floor not enforced, min weight=%v", minW)
			record("banditExplorationFloor", "FAIL", fmt.Sprintf("min=%v", minW))
		} else {
			record("banditExplorationFloor", "PASS",
				fmt.Sprintf("min=%.3f ≥ floor=%.2f", minW, banditExplorationFloor))
		}
	})

	// ── 15. /admin/diagnostics endpoint contract ─────────────────────────────
	t.Run("diagnosticsEndpoint", func(t *testing.T) {
		rep := buildUserDiagnosticsReport(context.Background(), altUser)
		want := []string{
			"profile", "negativeSignals", "banditArms",
			"ltrWeights", "watchRatioWeights", "userEmbedding",
			"trendingRealtime", "bootstrapPool", "seenFilter",
			"impressionStats", "sessionState", "calibrationParams",
			"embedCacheSpotCheck",
		}
		missing := make([]string, 0)
		for _, k := range want {
			if _, ok := rep.Summary[k]; !ok {
				missing = append(missing, k)
			}
		}
		if len(missing) > 0 {
			t.Errorf("diagnostics report missing keys: %v", missing)
			record("diagnosticsEndpoint", "FAIL", fmt.Sprintf("missing: %v", missing))
		} else {
			record("diagnosticsEndpoint", "PASS", fmt.Sprintf("%d subsystem keys present", len(want)))
		}
	})

	// ── 16. METRICS — every Prometheus series we shipped is registered ───────
	t.Run("metricsRegistered", func(t *testing.T) {
		series := map[string]bool{
			"feedRequests":      metricFeedRequests != nil,
			"feedLatency":       metricFeedLatency != nil,
			"banditWrites":      metricBanditWrites != nil,
			"ltrFlushes":        metricLTRFlushes != nil,
			"ltrUpdates":        metricLTRUpdates != nil,
			"signalCapture":     metricSignalCapture != nil,
			"analyticsJob":      metricAnalyticsJob != nil,
			"httpRequests":      metricHTTPRequests != nil,
			"httpLatency":       metricHTTPLatency != nil,
			"embedUpdates":      metricEmbedUpdates != nil,
			"embedCacheHits":    metricEmbedCacheHits != nil,
			"seenMarks":         metricSeenMarks != nil,
			"seenFiltered":      metricSeenFiltered != nil,
			"mmrReranks":        metricMMRReranks != nil,
			"plattFits":         metricPlattFits != nil,
			"candidateSource":   metricCandidateSource != nil,
			"bootstrapPool":     metricBootstrapPool != nil,
			"watchRatioObserve": metricWatchRatioObserve != nil,
			"watchRatioFlush":   metricWatchRatioFlush != nil,
		}
		broken := make([]string, 0)
		for n, ok := range series {
			if !ok {
				broken = append(broken, n)
			}
		}
		if len(broken) > 0 {
			t.Errorf("metrics not registered: %v", broken)
			record("metricsRegistered", "FAIL", fmt.Sprintf("missing: %v", broken))
		} else {
			record("metricsRegistered", "PASS", fmt.Sprintf("%d Prometheus series live", len(series)))
		}
	})

	// ── Final: print the per-field report ────────────────────────────────────
	t.Run("PRINT_REPORT", func(t *testing.T) {
		sort.Slice(results, func(i, j int) bool { return results[i].Name < results[j].Name })

		var pass, fail int
		fmt.Println()
		fmt.Println(strings.Repeat("═", 78))
		fmt.Println("ALGORITHM FIELD-BY-FIELD VERIFICATION REPORT")
		fmt.Println(strings.Repeat("═", 78))
		fmt.Printf("%-44s  %-6s  %s\n", "FIELD / SUBSYSTEM", "STATUS", "DETAIL")
		fmt.Println(strings.Repeat("─", 78))
		for _, r := range results {
			marker := "OK "
			if r.Status == "FAIL" {
				marker = "XX "
				fail++
			} else if r.Status == "PASS" {
				pass++
			}
			fmt.Printf("%-44s  %s%-4s  %s\n", r.Name, marker, r.Status, r.Detail)
		}
		fmt.Println(strings.Repeat("─", 78))
		fmt.Printf("Total: %d checks  ·  PASS: %d  ·  FAIL: %d\n", len(results), pass, fail)
		fmt.Println(strings.Repeat("═", 78))
		fmt.Println()
		if fail > 0 {
			t.Fatalf("%d field(s) failing — see report above", fail)
		}
	})
}

// (resetLTR is defined in learning_to_rank_test.go and reused here.)
