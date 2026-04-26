package main

import (
	"strconv"
	"sync"
	"time"
)

// ─────────────────────────────────────────────────────────────────────────────
// PER-CREATOR REACH RESIDUAL CALIBRATION
//
// Some creators are systematically over-served by the ranker: their hash-
// trick fingerprint matches popular categories, their early views came in
// hot, the LTR weights happen to favor their content's feature pattern —
// and then the ranker keeps showing them. But user engagement doesn't
// follow. The model has a per-creator BIAS it never corrects.
//
// This module tracks, per creator, the running residual:
//
//   residual = predicted_engagement_rate - actual_engagement_rate
//
// Predicted is the mean LTR score for items by this creator that we
// served. Actual is the observed engagement rate on those served items.
// A positive residual means we over-rank them; negative means we under-
// rank them. We apply a corrective multiplier to their score in
// scoreForUser so the bias self-corrects within a few hundred served items.
//
// Math: corrected_score = raw_score - alpha * residual
// where alpha is small (0.15) so corrections are gentle but persistent.
//
// SAFETY:
//   - Bounded multiplier: ±creatorResidualMaxAdjust (0.20)
//   - Minimum sample threshold: bias estimate is unreliable below this
//   - Per-creator state in Redis with TTL so dormant creators don't
//     keep stale corrections
// ─────────────────────────────────────────────────────────────────────────────

const (
	creatorResidualRedisKey   = "crr:" // + creatorID
	creatorResidualTTL        = 30 * 24 * time.Hour
	creatorResidualMinSamples = 30
	creatorResidualAlpha      = 0.15
	creatorResidualMaxAdjust  = 0.20
	// EMA weight for new samples — small so a few outlier days don't
	// whiplash the estimate.
	creatorResidualEMA = 0.05
)

// creatorResidualState is the running state per creator. Predicted/actual
// are EMA-smoothed; residual is recomputed at read time.
type creatorResidualState struct {
	PredictedMean float64 // EMA of predicted engagement
	ActualMean    float64 // EMA of observed engagement
	N             int     // sample count
}

// residual returns predicted - actual. Positive = over-ranked, negative = under.
func (s creatorResidualState) residual() float64 {
	return s.PredictedMean - s.ActualMean
}

// In-process cache of creator residuals — read at scoring time per
// candidate, can't afford a Redis hit per candidate per request.
var creatorResidualCache = NewSignalCache[creatorResidualState](5 * time.Minute)

// Locks per creatorID to serialize concurrent updates without holding a
// global mutex.
var creatorResidualLock sync.Map // map[string]*sync.Mutex

func creatorResidualMutex(creatorID string) *sync.Mutex {
	v, _ := creatorResidualLock.LoadOrStore(creatorID, &sync.Mutex{})
	return v.(*sync.Mutex)
}

// loadCreatorResidual fetches the creator's state from cache → Redis.
// Returns zero-state on cold creator (residual = 0 → no adjustment).
func loadCreatorResidual(creatorID string) creatorResidualState {
	if creatorID == "" {
		return creatorResidualState{}
	}
	if v, ok := creatorResidualCache.Get(creatorID); ok {
		return v
	}
	if rdb == nil {
		return creatorResidualState{}
	}
	m, err := rdb.HGetAll(rctx, creatorResidualRedisKey+creatorID).Result()
	if err != nil || len(m) == 0 {
		return creatorResidualState{}
	}
	s := creatorResidualState{}
	if v, err := strconv.ParseFloat(m["pm"], 64); err == nil {
		s.PredictedMean = v
	}
	if v, err := strconv.ParseFloat(m["am"], 64); err == nil {
		s.ActualMean = v
	}
	if v, err := strconv.Atoi(m["n"]); err == nil {
		s.N = v
	}
	creatorResidualCache.Set(creatorID, s)
	return s
}

// observeCreatorResidual records a (predicted, actual) pair for one served
// item by this creator. EMA-updates the state and persists to Redis.
//
// predicted should be in [0, 1] — typically the Platt-calibrated probability
// of positive engagement. actual is binary 1.0 / 0.0 from the terminal event.
func observeCreatorResidual(creatorID string, predicted, actual float64) {
	if creatorID == "" || rdb == nil {
		return
	}
	mu := creatorResidualMutex(creatorID)
	mu.Lock()
	defer mu.Unlock()

	s := loadCreatorResidual(creatorID)
	if s.N == 0 {
		s.PredictedMean = predicted
		s.ActualMean = actual
	} else {
		s.PredictedMean = s.PredictedMean*(1-creatorResidualEMA) + predicted*creatorResidualEMA
		s.ActualMean = s.ActualMean*(1-creatorResidualEMA) + actual*creatorResidualEMA
	}
	s.N++

	pipe := rdb.TxPipeline()
	pipe.HSet(rctx, creatorResidualRedisKey+creatorID,
		"pm", strconv.FormatFloat(s.PredictedMean, 'f', 4, 64),
		"am", strconv.FormatFloat(s.ActualMean, 'f', 4, 64),
		"n", strconv.Itoa(s.N))
	pipe.Expire(rctx, creatorResidualRedisKey+creatorID, creatorResidualTTL)
	_, _ = pipe.Exec(rctx)
	creatorResidualCache.Set(creatorID, s)

	if metricCreatorResidualUpdate != nil {
		metricCreatorResidualUpdate.Inc()
	}
}

// creatorResidualAdjustment returns a bounded correction to apply to a
// candidate's score in scoreForUser. Positive residual (creator over-ranked)
// → negative adjustment; negative residual → positive adjustment.
//
// Returns 0 until the creator has accumulated minSamples observations.
func creatorResidualAdjustment(creatorID string) float64 {
	if creatorID == "" {
		return 0
	}
	s := loadCreatorResidual(creatorID)
	if s.N < creatorResidualMinSamples {
		return 0
	}
	adj := -creatorResidualAlpha * s.residual()
	if adj > creatorResidualMaxAdjust {
		adj = creatorResidualMaxAdjust
	}
	if adj < -creatorResidualMaxAdjust {
		adj = -creatorResidualMaxAdjust
	}
	return adj
}
