package main

import (
	"encoding/json"
	"math"
	"sync"
	"time"
)

// ─────────────────────────────────────────────────────────────────────────────
// PLATT CALIBRATION for LTR scores.
//
// The LTR model outputs a logit — a raw score in roughly [-ltrMaxDelta,
// +ltrMaxDelta]. That value gets added directly to the base score. But:
//
//   * raw logits are *not* probabilities
//   * their scale can drift as the model trains
//   * combining an uncalibrated logit with other heuristic terms of arbitrary
//     scale makes weight-tuning guesswork.
//
// Platt scaling fits a 1-D logistic regression:  p = σ(A·x + B)  on
// (score, label) pairs we collect during observe. Once trained, the feed
// engine asks calibrate(logit) and gets a probability in [0,1] that the
// user will positively engage. We can then scale that to a clean ranker
// bonus of known magnitude.
//
// Persisted to Redis so restarts don't throw away the fit.
// ─────────────────────────────────────────────────────────────────────────────

const (
	calibRedisKey       = "ltr:calib"
	calibMinSamples     = 200
	calibMaxBufferSize  = 5000
	calibRefitInterval  = 10 * time.Minute
	calibInitialA       = 1.0
	calibInitialB       = 0.0
)

type plattCalibrator struct {
	mu      sync.RWMutex
	A, B    float64
	fitted  bool
	samples []calibSample // rolling buffer of recent (score, label) pairs
}

type calibSample struct {
	X float64 `json:"x"`
	Y float64 `json:"y"` // 0 or 1
}

var platt = &plattCalibrator{
	A: calibInitialA,
	B: calibInitialB,
}

// plattRecord appends a (score, label) observation. Called from the LTR
// observe path so every training sample also feeds calibration.
func plattRecord(score, label float64) {
	platt.mu.Lock()
	defer platt.mu.Unlock()
	platt.samples = append(platt.samples, calibSample{X: score, Y: label})
	if len(platt.samples) > calibMaxBufferSize {
		// Drop oldest in bulk to keep the buffer bounded.
		platt.samples = platt.samples[len(platt.samples)-calibMaxBufferSize:]
	}
}

// plattCalibrate maps a raw score to a probability using the current fit.
// If not yet fitted, falls back to plain sigmoid(x) so callers always get
// a value in (0,1) — no unknown-state handling needed upstream.
func plattCalibrate(score float64) float64 {
	platt.mu.RLock()
	a, b, ok := platt.A, platt.B, platt.fitted
	platt.mu.RUnlock()
	if !ok {
		return 1.0 / (1.0 + math.Exp(-score))
	}
	return 1.0 / (1.0 + math.Exp(-(a*score + b)))
}

// plattFit runs a few epochs of batch SGD on the buffer to re-estimate A,B.
// Safe to call repeatedly; no-op if buffer is too small.
func plattFit() {
	// Snapshot the buffer + current params under a BRIEF lock, then run the
	// 50-epoch SGD (and the Redis persist) WITHOUT holding the lock. The old code
	// held platt.mu.Lock() across the entire fit + Redis round-trip, so every
	// hot-path plattCalibrate RLock stalled for the whole duration. Copying the
	// buffer also removes the data race with concurrent plattRecord appends that
	// re-slice platt.samples out from under the SGD loop.
	platt.mu.Lock()
	if len(platt.samples) < calibMinSamples {
		platt.mu.Unlock()
		return
	}
	samples := make([]calibSample, len(platt.samples))
	copy(samples, platt.samples)
	a, b := platt.A, platt.B
	platt.mu.Unlock()

	const epochs = 50
	const lr = 0.05
	for e := 0; e < epochs; e++ {
		var ga, gb float64
		for _, s := range samples {
			p := 1.0 / (1.0 + math.Exp(-(a*s.X + b)))
			err := p - s.Y
			ga += err * s.X
			gb += err
		}
		inv := 1.0 / float64(len(samples))
		a -= lr * ga * inv
		b -= lr * gb * inv
	}

	// Publish the new fit under a brief lock only.
	platt.mu.Lock()
	platt.A = a
	platt.B = b
	platt.fitted = true
	platt.mu.Unlock()

	// Persist OUTSIDE the lock — a Redis round-trip must not block plattCalibrate.
	if rdb != nil {
		if js, err := json.Marshal(map[string]float64{"a": a, "b": b}); err == nil {
			_ = rdb.Set(rctx, calibRedisKey, js, 30*24*time.Hour).Err()
		}
	}
	if metricPlattFits != nil {
		metricPlattFits.Inc()
	}
}

// plattLoad restores A,B from Redis on process start.
func plattLoad() {
	if rdb == nil {
		return
	}
	s, err := rdb.Get(rctx, calibRedisKey).Result()
	if err != nil || s == "" {
		return
	}
	var p struct {
		A float64 `json:"a"`
		B float64 `json:"b"`
	}
	if json.Unmarshal([]byte(s), &p) != nil {
		return
	}
	platt.mu.Lock()
	platt.A = p.A
	platt.B = p.B
	platt.fitted = true
	platt.mu.Unlock()
}

// startPlattRefitter re-estimates A,B periodically so calibration tracks
// the evolving LTR model.
func startPlattRefitter() {
	plattLoad()
	go func() {
		ticker := time.NewTicker(calibRefitInterval)
		defer ticker.Stop()
		for range ticker.C {
			plattFit()
		}
	}()
}
