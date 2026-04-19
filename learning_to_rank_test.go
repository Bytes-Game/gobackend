package main

import (
	"encoding/json"
	"testing"
)

// Reset LTR state between tests — it's a package-level singleton.
func resetLTR() {
	ltr.mu.Lock()
	defer ltr.mu.Unlock()
	ltr.byCoh = make(map[Cohort]*ltrModel)
	ltr.dirty = make(map[Cohort]bool)
	ltr.loaded = false
}

func TestLTR_LabelForEvent(t *testing.T) {
	cases := []struct {
		event      string
		completion float64
		wantLabel  float64
		wantOk     bool
	}{
		{"complete", 0, 1, true},
		{"like", 0, 1, true},
		{"share", 0, 1, true},
		{"rewatch", 0, 1, true},
		{"loop", 0, 1, true},
		{"scroll_back", 0, 1, true},
		{"save", 0, 1, true},
		{"unmute", 0, 1, true},
		{"skip", 0, 0, true},
		{"not_interested", 0, 0, true},
		{"report", 0, 0, true},
		{"block", 0, 0, true},
		{"view", 0.9, 1, true},
		{"view", 0.1, 0, true},
		{"view", 0.5, 0, false},
		{"random_event", 0, 0, false},
	}
	for _, c := range cases {
		gotL, gotOk := ltrLabelForEvent(c.event, c.completion)
		if gotOk != c.wantOk || gotL != c.wantLabel {
			t.Errorf("event=%q comp=%v → label=%v ok=%v, want %v/%v",
				c.event, c.completion, gotL, gotOk, c.wantLabel, c.wantOk)
		}
	}
}

func TestLTR_NoDataReturnsZeroDelta(t *testing.T) {
	resetRedis(t)
	resetLTR()
	bd := map[string]float64{"quality": 0.5, "novelty": 0.3}
	if got := ltrScoreDelta(CohortEngaged, bd); got != 0 {
		t.Errorf("expected 0 with no training data, got %v", got)
	}
}

func TestLTR_DeltaBounded(t *testing.T) {
	resetRedis(t)
	resetLTR()
	// Force a big positive model manually — delta must still be bounded.
	ltrEnsureLoaded()
	ltr.mu.Lock()
	ltr.byCoh[CohortEngaged] = &ltrModel{
		Weights: map[string]float64{"quality": 100},
		Bias:    50,
		Updates: 500,
	}
	ltr.mu.Unlock()
	bd := map[string]float64{"quality": 10}
	got := ltrScoreDelta(CohortEngaged, bd)
	if got > ltrMaxDelta+1e-9 || got < -ltrMaxDelta-1e-9 {
		t.Errorf("delta must be bounded to ±%v, got %v", ltrMaxDelta, got)
	}
	// Also that the bound is approached (very large logit → near max).
	if got < ltrMaxDelta*0.9 {
		t.Errorf("large positive logit should approach max delta, got %v", got)
	}
}

func TestLTR_ObserveMovesTowardLabel(t *testing.T) {
	resetRedis(t)
	resetLTR()
	bd := map[string]float64{
		"quality":  0.8,
		"novelty":  0.5,
		"freshness": 0.7,
	}
	// Hammer label=1 and check the delta becomes positive.
	for i := 0; i < 200; i++ {
		ltrObserve(CohortEngaged, bd, 1.0)
	}
	delta := ltrScoreDelta(CohortEngaged, bd)
	if delta <= 0 {
		t.Errorf("after 200 positive observations, delta should be >0, got %v", delta)
	}

	// Now flip to label=0 and check it moves back.
	for i := 0; i < 400; i++ {
		ltrObserve(CohortEngaged, bd, 0.0)
	}
	delta2 := ltrScoreDelta(CohortEngaged, bd)
	if delta2 >= delta {
		t.Errorf("after negative observations, delta should decrease; %v → %v", delta, delta2)
	}
}

func TestLTR_StashAndObserveEventRoundTrip(t *testing.T) {
	resetRedis(t)
	resetLTR()

	bd := map[string]float64{
		"quality": 0.9,
		"novelty": 0.2,
	}
	ltrStashBreakdown("uLTR", "post", "p42", CohortPower, bd)

	// Fetch directly to verify persistence shape.
	js, err := rdb.Get(rctx, ltrBreakdownKey("uLTR", "post", "p42")).Result()
	if err != nil {
		t.Fatalf("stash not persisted: %v", err)
	}
	var payload struct {
		C string             `json:"c"`
		B map[string]float64 `json:"b"`
	}
	if err := json.Unmarshal([]byte(js), &payload); err != nil {
		t.Fatalf("stash payload unparseable: %v", err)
	}
	if payload.C != string(CohortPower) {
		t.Errorf("cohort mismatch: %s", payload.C)
	}
	if payload.B["quality"] != 0.9 {
		t.Errorf("breakdown mismatch: %v", payload.B)
	}

	// Observe a positive outcome — LTR should train on the stashed breakdown.
	ltrObserveEvent("uLTR", "post", "p42", 1.0)
	ltr.mu.RLock()
	m := ltr.byCoh[CohortPower]
	ltr.mu.RUnlock()
	if m == nil || m.Updates != 1 {
		t.Errorf("expected 1 update on power cohort, got %+v", m)
	}

	// And the stash must be deleted so it can't double-train.
	if _, err := rdb.Get(rctx, ltrBreakdownKey("uLTR", "post", "p42")).Result(); err == nil {
		t.Error("stash should have been deleted after observe")
	}

	// Second observe should be a no-op (no stash left).
	ltrObserveEvent("uLTR", "post", "p42", 1.0)
	ltr.mu.RLock()
	m2 := ltr.byCoh[CohortPower]
	ltr.mu.RUnlock()
	if m2.Updates != 1 {
		t.Errorf("double-train prevention failed, updates=%d", m2.Updates)
	}
}

func TestLTR_FlushPersistsWeights(t *testing.T) {
	resetRedis(t)
	resetLTR()

	bd := map[string]float64{"quality": 0.5}
	for i := 0; i < 30; i++ {
		ltrObserve(CohortAtRisk, bd, 1.0)
	}
	ltrFlush()

	s, err := rdb.Get(rctx, ltrRedisKey+string(CohortAtRisk)).Result()
	if err != nil || s == "" {
		t.Fatalf("flush did not persist weights: err=%v s=%q", err, s)
	}
	var m ltrModel
	if err := json.Unmarshal([]byte(s), &m); err != nil {
		t.Fatalf("persisted weights unparseable: %v", err)
	}
	if m.Updates != 30 {
		t.Errorf("persisted updates mismatch: got %d want 30", m.Updates)
	}
}

func TestLTR_EnsureLoadedReadsExistingWeights(t *testing.T) {
	resetRedis(t)
	resetLTR()

	// Plant a fake model in Redis first.
	planted := ltrModel{
		Weights: map[string]float64{"quality": 0.42},
		Bias:    0.1,
		Updates: 77,
	}
	js, _ := json.Marshal(planted)
	_ = rdb.Set(rctx, ltrRedisKey+string(CohortEngaged), js, 0).Err()

	ltrEnsureLoaded()
	ltr.mu.RLock()
	m := ltr.byCoh[CohortEngaged]
	ltr.mu.RUnlock()
	if m == nil || m.Updates != 77 || m.Weights["quality"] != 0.42 {
		t.Errorf("expected planted weights to be loaded, got %+v", m)
	}
}

func TestLTR_ObserveEventWithoutStashIsNoop(t *testing.T) {
	resetRedis(t)
	resetLTR()
	ltrObserveEvent("noone", "post", "nope", 1.0)
	ltr.mu.RLock()
	for c, m := range ltr.byCoh {
		if m != nil && m.Updates > 0 {
			ltr.mu.RUnlock()
			t.Fatalf("stashless observe should not train, got updates on %s", c)
		}
	}
	ltr.mu.RUnlock()
}
