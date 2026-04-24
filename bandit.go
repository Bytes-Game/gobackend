package main

import (
	"fmt"
	"math"
	"math/rand"
	"sync"
	"time"
)

// ════════════════════════════════════════════════════════════════════════════════
// THOMPSON-SAMPLING BANDIT — chooses the next strategy when the current one
// fails, using a beta distribution posterior on each strategy's success rate.
// ════════════════════════════════════════════════════════════════════════════════
//
// The pre-existing pickAlternateStrategy is rule-based (mood→strategy). This
// is a complement: when that heuristic is ambiguous or keeps suggesting the
// same strategy twice, the bandit steps in to explore.
//
// Each strategy keeps two counters per-user: alpha (wins) and beta (losses).
// We draw a sample from Beta(alpha, beta) for each strategy and pick the max.
// This naturally balances exploration (uncertain strategies get wider draws)
// with exploitation (well-performing strategies get tighter, higher draws).
//
// STATE: per-user bandit state is stored in Redis hash bandit:{userId}.
//
//   HGET bandit:{u} {strategy}_a   → alpha (float string)
//   HGET bandit:{u} {strategy}_b   → beta
//
// We prefer Redis over in-memory because a user may land on any server
// instance; a shared store keeps learning consistent.

type banditArm struct {
	alpha float64
	beta  float64
}

// banditDefaults: a tiny prior so brand-new arms still get sampled.
func banditDefaults() banditArm {
	return banditArm{alpha: 1.0, beta: 1.0}
}

// bandit is the in-memory mirror of one user's arms, loaded on demand.
type bandit struct {
	mu   sync.Mutex
	arms map[string]*banditArm
}

func newBandit() *bandit {
	return &bandit{arms: map[string]*banditArm{}}
}

// loadBandit reads the user's arms from Redis. Non-fatal on failure — caller
// just gets a fresh bandit with default priors.
func loadBandit(userID string) *bandit {
	b := newBandit()
	if userID == "" {
		return b
	}
	m, err := rdb.HGetAll(rctx, "bandit:"+userID).Result()
	if err != nil || len(m) == 0 {
		return b
	}
	for k, v := range m {
		var strat, which string
		var val float64
		// Format: {strategy}_a or {strategy}_b
		if n := len(k); n >= 2 && (k[n-2] == '_') {
			strat = k[:n-2]
			which = k[n-1:]
		} else {
			continue
		}
		if _, err := fmt.Sscanf(v, "%f", &val); err != nil {
			continue
		}
		arm, ok := b.arms[strat]
		if !ok {
			d := banditDefaults()
			arm = &d
			b.arms[strat] = arm
		}
		switch which {
		case "a":
			arm.alpha = val
		case "b":
			arm.beta = val
		}
	}
	return b
}

// armOrDefault returns the arm for a strategy, creating a default if missing.
func (b *bandit) armOrDefault(strat string) *banditArm {
	b.mu.Lock()
	defer b.mu.Unlock()
	if a, ok := b.arms[strat]; ok {
		return a
	}
	d := banditDefaults()
	b.arms[strat] = &d
	return &d
}

// sampleBest returns the strategy (from `candidates`) with the highest random
// draw from its Beta posterior. Thompson sampling — no epsilon tuning needed.
func (b *bandit) sampleBest(candidates []string, rnd *rand.Rand) string {
	if len(candidates) == 0 {
		return ""
	}
	bestStrat := candidates[0]
	bestVal := -1.0
	for _, c := range candidates {
		a := b.armOrDefault(c)
		v := betaSample(a.alpha, a.beta, rnd)
		if v > bestVal {
			bestVal = v
			bestStrat = c
		}
	}
	return bestStrat
}

// banditExplorationFloor is the minimum probability mass any candidate gets
// in softMix even if its Thompson draw is overwhelmingly small. Without a
// floor, a strategy that lost a few times early can collapse to ~0 weight
// forever, starving the bandit of new evidence. 5% per arm guarantees we
// keep retrying every option occasionally — this is the "explore" leg of
// the exploration/exploitation tradeoff that a pure Thompson loop loses
// once the prior gets sharply skewed.
const banditExplorationFloor = 0.05

// softMix returns per-strategy weights summing to 1.0, derived from Thompson
// draws. Unlike sampleBest (which picks one winner), softMix lets the ranker
// blend strategy-specific score bonuses proportionally — useful when two
// strategies are near-tied and "picking one" loses nuance.
//
// The default "winner" always has the largest weight; the softmax temperature
// is tuned so a clear winner still dominates while close runners-up keep
// meaningful influence. After the softmax we apply an exploration floor so
// no arm ever falls below banditExplorationFloor — this keeps the bandit
// learning indefinitely instead of locking onto an early winner.
func (b *bandit) softMix(candidates []string, rnd *rand.Rand) map[string]float64 {
	out := make(map[string]float64, len(candidates))
	if len(candidates) == 0 {
		return out
	}
	// Draw one Thompson sample per candidate.
	raw := make([]float64, len(candidates))
	for i, c := range candidates {
		a := b.armOrDefault(c)
		raw[i] = betaSample(a.alpha, a.beta, rnd)
	}
	// Softmax with temperature 4 — sharp but not degenerate.
	const temp = 4.0
	var maxV float64 = -1e18
	for _, v := range raw {
		if v > maxV {
			maxV = v
		}
	}
	var sum float64
	exps := make([]float64, len(raw))
	for i, v := range raw {
		exps[i] = expSafe(temp * (v - maxV))
		sum += exps[i]
	}
	if sum == 0 {
		// Uniform fallback (shouldn't happen, but guard).
		u := 1.0 / float64(len(candidates))
		for _, c := range candidates {
			out[c] = u
		}
		return out
	}
	for i, c := range candidates {
		out[c] = exps[i] / sum
	}

	// Exploration floor: lift every weight to at least banditExplorationFloor,
	// then re-normalize so the total stays 1.0. The total mass donated to the
	// floor is `n * floor` minus what was already above it; the rest is
	// scaled proportionally so the relative ordering of strong arms is
	// preserved.
	if banditExplorationFloor > 0 && len(candidates) > 1 {
		applyExplorationFloor(out, banditExplorationFloor)
	}
	return out
}

// applyExplorationFloor enforces a minimum weight per key in m. Mutates m
// in place. After the lift, the remaining mass above floor*n is rescaled
// so the total is 1.0. Safe when n*floor >= 1: the result is uniform.
func applyExplorationFloor(m map[string]float64, floor float64) {
	n := len(m)
	if n == 0 || floor <= 0 {
		return
	}
	if float64(n)*floor >= 1.0 {
		// No room for differentiation — uniform.
		u := 1.0 / float64(n)
		for k := range m {
			m[k] = u
		}
		return
	}
	// Lift each weight to at least the floor, sum the excess above floor.
	excess := 0.0
	for _, v := range m {
		if v > floor {
			excess += v - floor
		}
	}
	if excess <= 0 {
		// Everything was at or below the floor — go uniform across floor.
		u := 1.0 / float64(n)
		for k := range m {
			m[k] = u
		}
		return
	}
	// Available mass above the floor must equal 1 - n*floor.
	available := 1.0 - float64(n)*floor
	scale := available / excess
	for k, v := range m {
		if v > floor {
			m[k] = floor + (v-floor)*scale
		} else {
			m[k] = floor
		}
	}
}

// expSafe is math.Exp clamped so huge logits don't turn into +Inf.
func expSafe(x float64) float64 {
	if x > 50 {
		x = 50
	}
	if x < -50 {
		x = -50
	}
	return math.Exp(x)
}

// updateArm credits an outcome (reward in [0,1]) to the strategy and persists.
// reward=1.0 → alpha++, reward=0.0 → beta++. Fractional rewards are supported
// for partial credit (e.g. 0.5 = "okay but not great").
func (b *bandit) updateArm(userID, strat string, reward float64) {
	if strat == "" || userID == "" {
		return
	}
	if reward < 0 {
		reward = 0
	}
	if reward > 1 {
		reward = 1
	}
	a := b.armOrDefault(strat)
	b.mu.Lock()
	a.alpha += reward
	a.beta += 1.0 - reward
	// Cap total observations to keep the bandit responsive to recent shifts.
	if total := a.alpha + a.beta; total > 200 {
		scale := 200.0 / total
		a.alpha *= scale
		a.beta *= scale
	}
	saveA, saveB := a.alpha, a.beta
	b.mu.Unlock()

	key := "bandit:" + userID
	errA := rdb.HSet(rctx, key, strat+"_a", fmt.Sprintf("%.3f", saveA)).Err()
	errB := rdb.HSet(rctx, key, strat+"_b", fmt.Sprintf("%.3f", saveB)).Err()
	_ = rdb.Expire(rctx, key, 90*24*time.Hour).Err() // long retention; bandit state is cheap
	if metricBanditWrites != nil {
		if errA != nil || errB != nil {
			metricBanditWrites.WithLabelValues("error").Inc()
		} else {
			metricBanditWrites.WithLabelValues("ok").Inc()
		}
	}
}

// betaSample draws one value from Beta(alpha, beta). For alpha,beta in the
// sane range (1..hundreds) this approximation via two gamma samples is plenty.
func betaSample(alpha, beta float64, rnd *rand.Rand) float64 {
	x := gammaSample(alpha, rnd)
	y := gammaSample(beta, rnd)
	if x+y == 0 {
		return 0.5
	}
	return x / (x + y)
}

// gammaSample — Marsaglia & Tsang method for shape >= 1; for shape < 1 we use
// the boost trick. Gives a decent Gamma(shape, 1) draw.
func gammaSample(shape float64, rnd *rand.Rand) float64 {
	if shape < 1 {
		// Boost: Gamma(shape) = Gamma(shape+1) * U^(1/shape)
		return gammaSample(shape+1, rnd) * math.Pow(rnd.Float64(), 1.0/shape)
	}
	d := shape - 1.0/3.0
	c := 1.0 / math.Sqrt(9*d)
	for {
		var x, v float64
		for {
			x = rnd.NormFloat64()
			v = 1 + c*x
			if v > 0 {
				break
			}
		}
		v = v * v * v
		u := rnd.Float64()
		if u < 1-0.0331*x*x*x*x {
			return d * v
		}
		if math.Log(u) < 0.5*x*x+d*(1-v+math.Log(v)) {
			return d * v
		}
	}
}
