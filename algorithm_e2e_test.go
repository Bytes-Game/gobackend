package main

import (
	"encoding/json"
	"math/rand"
	"strings"
	"testing"
	"time"
)

// This file is a single end-to-end sanity test: it exercises every component
// of the new recommendation algorithm and asserts each one produces the
// output/side-effect we expect. If any step stops working, this test fails
// with a clear message saying which subsystem is broken.
//
// It uses the miniredis instance from setup_test.go. It does NOT require a
// live Postgres connection — components that need SQL are exercised via
// their in-memory code paths (session state, embeddings, bandit, MMR,
// seen-filter, Platt, LTR, anti-loop).

func TestAlgorithm_EndToEnd_AllComponentsProduceExpectedOutput(t *testing.T) {
	resetRedis(t)

	const userID = "e2e_user"
	now := time.Now()

	// ───────────────────────── 1. Embeddings ─────────────────────────
	csA := &ContentScore{
		ContentID:    "c1",
		ContentType:  "post",
		Category:     "gaming",
		CreatorID:    "u_alice",
		EnergyLevel:  0.8,
		QualityScore: 0.7,
		CreatedAt:    now,
		ViewCount:    1000,
		LikeCount:    100,
	}
	csB := &ContentScore{
		ContentID:    "c2",
		ContentType:  "post",
		Category:     "gaming",
		CreatorID:    "u_alice",
		EnergyLevel:  0.8,
		QualityScore: 0.6,
		CreatedAt:    now.Add(-1 * time.Hour),
		ViewCount:    800,
		LikeCount:    90,
	}
	csC := &ContentScore{
		ContentID:    "c3",
		ContentType:  "post",
		Category:     "music",
		CreatorID:    "u_bob",
		EnergyLevel:  0.3,
		QualityScore: 0.9,
		CreatedAt:    now.Add(-2 * time.Hour),
		ViewCount:    2000,
		LikeCount:    400,
	}

	vA := buildContentEmbedding(csA, []string{"hype", "competitive"})
	vB := buildContentEmbedding(csB, []string{"hype", "competitive"})
	vC := buildContentEmbedding(csC, []string{"chill", "peaceful"})
	if len(vA) != embedDim || len(vB) != embedDim || len(vC) != embedDim {
		t.Fatalf("[embeddings] wrong dim: %d/%d/%d (want %d)", len(vA), len(vB), len(vC), embedDim)
	}
	simAB := cosineSim(vA, vB)
	simAC := cosineSim(vA, vC)
	if !(simAB > simAC) {
		t.Fatalf("[embeddings] expected cosine(A,B) > cosine(A,C) since A,B share category+creator; got %.3f vs %.3f", simAB, simAC)
	}

	// User EMA should move toward positively-engaged content.
	updateUserEmbedding(userID, vA, 1.0)
	updateUserEmbedding(userID, vA, 1.0)
	updateUserEmbedding(userID, vA, 1.0)
	userVec := getUserEmbedding(userID)
	if userEmbeddingIsCold(userVec) {
		t.Fatal("[embeddings] user vector should be warm after 3 positive updates")
	}
	// And the vector must be persisted in Redis at the known key.
	if _, err := rdb.Get(rctx, userEmbedRedisKey+userID).Result(); err != nil {
		t.Fatalf("[embeddings] user vector not persisted in redis: %v", err)
	}
	sim := cosineSim(userVec, vA)
	if sim < 0.1 {
		t.Fatalf("[embeddings] user vector should align with positively-engaged content, cosine=%.3f", sim)
	}

	// ───────────────────────── 2. Seen filter ─────────────────────────
	items := []HomeFeedItem{
		{Type: "post", Post: &Post{ID: "p1"}},
		{Type: "post", Post: &Post{ID: "p2"}},
		{Type: "post", Post: &Post{ID: "p3"}},
	}
	markShownBatch(userID, items)
	// Verify each member is present in the zset.
	seen := loadSeenSet(userID)
	for _, want := range []string{"post:p1", "post:p2", "post:p3"} {
		if !seen[want] {
			t.Fatalf("[seen_filter] expected %q in seen set, got %v", want, seen)
		}
	}
	// filterUnseen drops all 3 and lets a fresh one through.
	mix := []HomeFeedItem{
		{Type: "post", Post: &Post{ID: "p1"}},
		{Type: "post", Post: &Post{ID: "p_fresh"}},
	}
	out := filterUnseen(userID, mix)
	if len(out) != 1 || getItemID(out[0]) != "p_fresh" {
		t.Fatalf("[seen_filter] expected only p_fresh to pass, got %+v", out)
	}

	// ───────────────────────── 3. MMR re-ranker ─────────────────────────
	scored := []ScoredItem{
		{Item: HomeFeedItem{Type: "post", Post: &Post{ID: "c1"}}, Score: 1.00}, // gaming+alice
		{Item: HomeFeedItem{Type: "post", Post: &Post{ID: "c2"}}, Score: 0.98}, // gaming+alice (near-dup)
		{Item: HomeFeedItem{Type: "post", Post: &Post{ID: "c3"}}, Score: 0.90}, // music+bob (diverse)
	}
	embedLookup := func(si ScoredItem) []float64 {
		switch getItemID(si.Item) {
		case "c1":
			return vA
		case "c2":
			return vB
		case "c3":
			return vC
		}
		return make([]float64, embedDim)
	}
	reranked := applyMMR(scored, 0.3, 3, embedLookup) // low λ → favors diversity
	if getItemID(reranked[0].Item) != "c1" {
		t.Fatalf("[mmr] top item should still be c1 (highest score), got %q", getItemID(reranked[0].Item))
	}
	if getItemID(reranked[1].Item) != "c3" {
		t.Fatalf("[mmr] under low lambda, c3 (diverse) should beat c2 (near-dup) at slot 2; got %q", getItemID(reranked[1].Item))
	}

	// ───────────────────────── 4. Platt calibration ─────────────────────────
	// Reset calibrator to known unfitted state.
	platt.mu.Lock()
	platt.A, platt.B, platt.fitted, platt.samples = calibInitialA, calibInitialB, false, nil
	platt.mu.Unlock()

	// Feed synthetic labels: label=1 when score>0.
	for i := 0; i < 500; i++ {
		x := float64((i%20)-10) / 4.0 // span roughly -2.5..2.5
		y := 0.0
		if x > 0 {
			y = 1.0
		}
		plattRecord(x, y)
	}
	plattFit()
	platt.mu.RLock()
	fitted := platt.fitted
	platt.mu.RUnlock()
	if !fitted {
		t.Fatal("[platt] should be fitted after 500 samples")
	}
	pHigh := plattCalibrate(3)
	pLow := plattCalibrate(-3)
	if pHigh <= pLow {
		t.Fatalf("[platt] calibrator should map larger scores to higher p; got pHigh=%.3f pLow=%.3f", pHigh, pLow)
	}
	// Fit must be persisted to Redis at the known key.
	if raw, err := rdb.Get(rctx, calibRedisKey).Result(); err != nil {
		t.Fatalf("[platt] calibration not persisted: %v", err)
	} else {
		var m map[string]float64
		if err := json.Unmarshal([]byte(raw), &m); err != nil {
			t.Fatalf("[platt] persisted blob not JSON: %v", err)
		}
		if _, ok := m["a"]; !ok {
			t.Fatalf("[platt] persisted blob missing 'a': %v", m)
		}
	}

	// ───────────────────────── 5. LTR with position-bias correction ─────
	// Stash a breakdown for this (user, content) with a specific position.
	cohort := Cohort("engaged")
	breakdown := map[string]float64{
		"quality":     0.8,
		"freshness":   0.6,
		"social":      0.3,
		"energyFit":   0.5,
		"relevance":   0.7,
		"novelty":     0.4,
	}
	ltrStashBreakdownWithPos(userID, "post", "c1", cohort, breakdown, 5)
	// Verify the stashed payload has position field.
	stashKey := ltrBreakdownKey(userID, "post", "c1")
	raw, err := rdb.Get(rctx, stashKey).Result()
	if err != nil {
		t.Fatalf("[ltr] stashed breakdown missing: %v", err)
	}
	var pld struct {
		C string             `json:"c"`
		B map[string]float64 `json:"b"`
		P int                `json:"p"`
	}
	if err := json.Unmarshal([]byte(raw), &pld); err != nil {
		t.Fatalf("[ltr] stashed breakdown not JSON: %v", err)
	}
	if pld.P != 5 {
		t.Fatalf("[ltr] expected position=5 in payload, got %d", pld.P)
	}
	if pld.C != string(cohort) {
		t.Fatalf("[ltr] expected cohort=%q, got %q", cohort, pld.C)
	}
	if pld.B["quality"] != 0.8 {
		t.Fatalf("[ltr] breakdown not round-tripped through redis: got %+v", pld.B)
	}
	// Position propensity should be in (0,1] and decreasing in position.
	if p1, p10 := positionPropensity(1), positionPropensity(10); !(p1 == 1.0 && p10 < p1 && p10 > 0) {
		t.Fatalf("[ltr] position propensity misshapen: p(1)=%.3f p(10)=%.3f", p1, p10)
	}

	// ───────────────────────── 6. Anti-loop ─────────────────────────
	// Healthy session → no diagnosis.
	s := &SessionState{DopamineBudget: 0.8}
	if detectLoop(s).Stuck {
		t.Fatalf("[anti_loop] healthy session should not be flagged stuck")
	}
	// Skip streak → override suggested.
	if d := detectLoop(&SessionState{SkipStreak: 5}); !d.Stuck || d.Reason != "skip_streak" || d.SuggestedStrat == "" {
		t.Fatalf("[anti_loop] skip streak signal missing: %+v", d)
	}
	// Category monoculture.
	if d := detectLoop(&SessionState{LastCategories: []string{"g", "g", "g", "m"}}); !d.Stuck || d.Reason != "category_monoculture" {
		t.Fatalf("[anti_loop] category monoculture signal missing: %+v", d)
	}
	// Creator flood.
	if d := detectLoop(&SessionState{LastCreators: []string{"a", "a", "a", "b", "c"}}); !d.Stuck || d.Reason != "creator_flood" {
		t.Fatalf("[anti_loop] creator flood signal missing: %+v", d)
	}
	// Dopamine collapse.
	if d := detectLoop(&SessionState{DopamineBudget: 0.1}); !d.Stuck || d.Reason != "dopamine_collapse" {
		t.Fatalf("[anti_loop] dopamine collapse signal missing: %+v", d)
	}

	// ───────────────────────── 7. Bandit (soft-mix) ─────────────────────
	b := newBandit()
	b.arms["discovery"] = &banditArm{alpha: 40, beta: 10} // strong winner
	b.arms["calming"] = &banditArm{alpha: 10, beta: 40}   // weak
	b.arms["fresh_blood"] = &banditArm{alpha: 25, beta: 25}
	weights := b.softMix([]string{"discovery", "calming", "fresh_blood"}, rand.New(rand.NewSource(1)))
	var total float64
	for _, w := range weights {
		if w < 0 {
			t.Fatalf("[bandit] soft-mix produced negative weight: %+v", weights)
		}
		total += w
	}
	if total < 0.99 || total > 1.01 {
		t.Fatalf("[bandit] soft-mix weights must sum to ~1, got %.3f (%+v)", total, weights)
	}
	if weights["discovery"] <= weights["calming"] {
		t.Fatalf("[bandit] strong winner should get more weight than weak arm: %+v", weights)
	}

	// ───────────────────────── 8. Multi-source: weighted interleave ─────
	bySource := map[string][]HomeFeedItem{
		"recency":   {{Type: "post", Post: &Post{ID: "r1"}}, {Type: "post", Post: &Post{ID: "r2"}}},
		"trending":  {{Type: "post", Post: &Post{ID: "t1"}}},
		"follow":    {{Type: "post", Post: &Post{ID: "f1"}}},
		"collab":    {{Type: "post", Post: &Post{ID: "cb1"}}},
		"embedding": {{Type: "post", Post: &Post{ID: "e1"}}},
	}
	sources := buildDefaultSources()
	merged := interleaveBySource(bySource, sources, 10)
	if len(merged) == 0 {
		t.Fatalf("[multi_source] interleave produced zero items from non-empty sources")
	}
	// First slot should be a recency item (highest weight).
	if firstID := getItemID(merged[0]); !strings.HasPrefix(firstID, "r") {
		t.Fatalf("[multi_source] highest-weight source (recency) should lead the interleave, got %q", firstID)
	}
	// Every input item should appear exactly once (no dupes).
	haveIDs := map[string]int{}
	for _, it := range merged {
		haveIDs[getItemID(it)]++
	}
	for _, n := range haveIDs {
		if n != 1 {
			t.Fatalf("[multi_source] duplicate item in interleave output: %+v", haveIDs)
		}
	}

	// ───────────────────────── 9. Metrics are registered & live ─────────
	// Call each new metric at least once to ensure the registration is good.
	metricEmbedUpdates.WithLabelValues("pos").Inc()
	metricSeenMarks.WithLabelValues("ok").Inc()
	metricSeenFiltered.Inc()
	metricMMRReranks.Inc()
	metricPlattFits.Inc()
	metricCandidateSource.WithLabelValues("recency", "ok").Inc()
	// If any of these were nil we'd panic — reaching this line == pass.

	t.Logf("[OK] all 9 algorithm subsystems verified end-to-end")
}
