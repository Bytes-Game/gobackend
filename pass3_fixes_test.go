package main

import (
	"math"
	"testing"
)

// TestDopamineViewDrainCalibration locks the proportional view-drain calibration
// (pass-3 #1). The constants must reproduce the old flat regime's fatigue timing:
// a low/mid-completion view drains, a full watch refills, neutral at 0.8 — so a
// ~30-item partial-watch session reaches the fatigue gates (budget ≈ 0.1). This
// was previously untested (the simulation test uses a hand-rolled dopamine model),
// which let the earlier 0.6-neutral/0.08-slope mis-tuning ship.
func TestDopamineViewDrainCalibration(t *testing.T) {
	delta := func(completion float64) float64 {
		return (completion - dopamineNeutralCompletion) * dopamineViewSlope
	}
	cases := []struct {
		val, expect float64
	}{
		{0.5, -0.03}, // typical partial view drains like the old flat rate
		{0.8, 0.0},   // neutral at the old refill/drain boundary
		{1.0, 0.02},  // full watch refills like the old +0.02
	}
	for _, c := range cases {
		if got := delta(c.val); math.Abs(got-c.expect) > 1e-9 {
			t.Errorf("view-drain delta(%.2f) = %.4f, want %.4f", c.val, got, c.expect)
		}
	}
	// A ~30-item partial-watch (0.5) session must reach the fatigue zone.
	budget := 1.0
	for i := 0; i < 30; i++ {
		budget = math.Max(0, budget+delta(0.5))
	}
	if budget > 0.15 {
		t.Errorf("after 30 partial-watch views budget=%.3f, expected <=0.15 (fatigued)", budget)
	}
}

// TestRepresentativeEmotion guards the wellbeing per-item emotion collapse
// (negative-priority): a multi-tagged item must reduce to its negative tag so the
// spiral detector counts negative ITEMS, not tags.
func TestRepresentativeEmotion(t *testing.T) {
	cases := []struct {
		in   []string
		want string
	}{
		{[]string{"happy", "sad"}, "sad"},          // negative wins over an earlier positive
		{[]string{"funny", "happy"}, "funny"},      // no negative → first tag
		{[]string{"scary"}, "scary"},               // single negative
		{nil, ""},                                  // empty → ""
		{[]string{"intense", "wholesome"}, "intense"}, // 'intense' is not in the wellbeing negative set → first tag
	}
	for _, c := range cases {
		if got := representativeEmotion(c.in); got != c.want {
			t.Errorf("representativeEmotion(%v) = %q, want %q", c.in, got, c.want)
		}
	}
}
