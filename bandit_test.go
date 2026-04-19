package main

import (
	"math/rand"
	"testing"
)

func TestBandit_SampleBestWithNoData(t *testing.T) {
	b := newBandit()
	rnd := rand.New(rand.NewSource(42))
	got := b.sampleBest([]string{"a", "b", "c"}, rnd)
	if got == "" {
		t.Error("sampleBest returned empty string")
	}
}

func TestBandit_SampleBestEmptyCandidates(t *testing.T) {
	b := newBandit()
	rnd := rand.New(rand.NewSource(42))
	if b.sampleBest(nil, rnd) != "" {
		t.Error("empty candidates should return empty string")
	}
}

// A strategy with overwhelmingly more wins should win the majority of samples.
func TestBandit_SampleBestPrefersWinners(t *testing.T) {
	b := newBandit()
	b.arms["good"] = &banditArm{alpha: 90, beta: 10}
	b.arms["bad"] = &banditArm{alpha: 10, beta: 90}

	rnd := rand.New(rand.NewSource(1))
	goodCount := 0
	n := 500
	for i := 0; i < n; i++ {
		if b.sampleBest([]string{"good", "bad"}, rnd) == "good" {
			goodCount++
		}
	}
	// Expect at least 80% preference for the clear winner.
	if goodCount < 400 {
		t.Errorf("expected strong preference for 'good', got %d/%d", goodCount, n)
	}
}

func TestBandit_UpdateArmPersists(t *testing.T) {
	resetRedis(t)
	b := loadBandit("ubandit1")
	b.updateArm("ubandit1", "strategy_a", 1.0)
	b.updateArm("ubandit1", "strategy_a", 1.0)
	b.updateArm("ubandit1", "strategy_a", 0.0)

	// Reload from Redis — should reflect 2 wins, 1 loss on top of (1,1) prior.
	b2 := loadBandit("ubandit1")
	arm := b2.armOrDefault("strategy_a")
	if arm.alpha < 2.9 || arm.alpha > 3.1 {
		t.Errorf("expected alpha ~3, got %v", arm.alpha)
	}
	if arm.beta < 1.9 || arm.beta > 2.1 {
		t.Errorf("expected beta ~2, got %v", arm.beta)
	}
}

func TestBandit_UpdateArmEmptyInputsAreNoop(t *testing.T) {
	resetRedis(t)
	b := newBandit()
	b.updateArm("", "strat", 1.0)
	b.updateArm("u1", "", 1.0)
	if len(mr.Keys()) != 0 {
		t.Errorf("empty inputs should not write, keys=%v", mr.Keys())
	}
}

func TestBandit_UpdateArmRewardClamped(t *testing.T) {
	resetRedis(t)
	b := loadBandit("u2")
	b.updateArm("u2", "s", 5.0)   // clamped to 1
	b.updateArm("u2", "s", -2.0)  // clamped to 0
	b2 := loadBandit("u2")
	arm := b2.armOrDefault("s")
	// 1 win, 1 loss on top of (1,1) → (2,2)
	if arm.alpha < 1.9 || arm.alpha > 2.1 {
		t.Errorf("alpha expected ~2, got %v", arm.alpha)
	}
	if arm.beta < 1.9 || arm.beta > 2.1 {
		t.Errorf("beta expected ~2, got %v", arm.beta)
	}
}

func TestBandit_UpdateArmCapTriggers(t *testing.T) {
	resetRedis(t)
	b := loadBandit("u3")
	for i := 0; i < 250; i++ {
		b.updateArm("u3", "s", 1.0)
	}
	arm := b.armOrDefault("s")
	total := arm.alpha + arm.beta
	if total > 201 {
		t.Errorf("total observations should cap at 200, got %v", total)
	}
}

// betaSample must always return a value in [0,1].
func TestBetaSample_Range(t *testing.T) {
	rnd := rand.New(rand.NewSource(7))
	for i := 0; i < 2000; i++ {
		v := betaSample(float64(1+i%10), float64(1+i%7), rnd)
		if v < 0 || v > 1 {
			t.Fatalf("betaSample out of range: %v", v)
		}
	}
}

func TestGammaSample_Positive(t *testing.T) {
	rnd := rand.New(rand.NewSource(11))
	// shape < 1 triggers the boost path
	for _, shape := range []float64{0.3, 0.7, 1.0, 2.0, 5.0, 50.0} {
		for i := 0; i < 50; i++ {
			v := gammaSample(shape, rnd)
			if v < 0 {
				t.Fatalf("gammaSample produced negative: shape=%v val=%v", shape, v)
			}
		}
	}
}

func TestLoadBandit_EmptyUserIsSafe(t *testing.T) {
	b := loadBandit("")
	if b == nil {
		t.Fatal("loadBandit(\"\") returned nil")
	}
	if len(b.arms) != 0 {
		t.Errorf("expected empty arms, got %v", b.arms)
	}
}

func TestLoadBandit_MissingKeyReturnsDefaults(t *testing.T) {
	resetRedis(t)
	b := loadBandit("neveruser")
	arm := b.armOrDefault("anything")
	if arm.alpha != 1.0 || arm.beta != 1.0 {
		t.Errorf("default arm should be (1,1), got (%v,%v)", arm.alpha, arm.beta)
	}
}
