package main

import (
	"math"
	"testing"
)

func TestPlattCalibrate_UnfittedFallsBackToSigmoid(t *testing.T) {
	// Reset calibrator to unfitted defaults.
	platt.mu.Lock()
	platt.A, platt.B, platt.fitted, platt.samples = 1.0, 0.0, false, nil
	platt.mu.Unlock()

	got := plattCalibrate(0)
	if math.Abs(got-0.5) > 1e-9 {
		t.Fatalf("sigmoid(0) should be 0.5, got %v", got)
	}
	if p := plattCalibrate(10); p <= 0.9 {
		t.Fatalf("sigmoid(10) should be close to 1, got %v", p)
	}
}

func TestPlattFit_NoOpIfUnderSampleFloor(t *testing.T) {
	platt.mu.Lock()
	platt.A, platt.B, platt.fitted, platt.samples = 1.0, 0.0, false, nil
	platt.mu.Unlock()

	for i := 0; i < calibMinSamples-10; i++ {
		plattRecord(0.1, 1.0)
	}
	plattFit()
	platt.mu.RLock()
	fitted := platt.fitted
	platt.mu.RUnlock()
	if fitted {
		t.Fatal("should not mark as fitted with fewer than calibMinSamples")
	}
}

func TestPlattFit_LearnsDirection(t *testing.T) {
	resetRedis(t)
	platt.mu.Lock()
	platt.A, platt.B, platt.fitted, platt.samples = 1.0, 0.0, false, nil
	platt.mu.Unlock()

	// Synth: label=1 when x>0, label=0 otherwise.
	for i := 0; i < 500; i++ {
		x := float64(i%10) - 5 // -5..4
		label := 0.0
		if x > 0 {
			label = 1.0
		}
		plattRecord(x, label)
	}
	plattFit()

	// Positive score should predict > 0.5 probability; negative, < 0.5.
	pHigh := plattCalibrate(3)
	pLow := plattCalibrate(-3)
	if pHigh <= 0.5 {
		t.Fatalf("positive logit should give p>0.5, got %v", pHigh)
	}
	if pLow >= 0.5 {
		t.Fatalf("negative logit should give p<0.5, got %v", pLow)
	}
}

func TestPlattRecord_BufferBounded(t *testing.T) {
	platt.mu.Lock()
	platt.samples = nil
	platt.mu.Unlock()

	for i := 0; i < calibMaxBufferSize+1000; i++ {
		plattRecord(float64(i), 1.0)
	}
	platt.mu.RLock()
	n := len(platt.samples)
	platt.mu.RUnlock()
	if n > calibMaxBufferSize {
		t.Fatalf("buffer should be capped at %d, got %d", calibMaxBufferSize, n)
	}
}
