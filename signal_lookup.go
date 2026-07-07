package main

import (
	"encoding/json"
	"math"
	"strconv"
	"time"
)

// ════════════════════════════════════════════════════════════════════════════════
// SIGNAL LOOKUP — ranker-side readers for the nightly analytics job's output
// ════════════════════════════════════════════════════════════════════════════════
//
// These are called at feed-request time. They read the Redis keys the nightly
// job writes and cache them in-process for 2 minutes so scoring 100 candidates
// costs 1 Redis round-trip, not 100.

// precomputedSignals is the per-user bundle loaded once per feed request.
type precomputedSignals struct {
	creatorAffinity map[string]float64 // creatorId -> 0..1
	pageDwellMs     map[string]int64   // pageName  -> avg ms
	socialDrive     float64            // 0..1 (fallback; UserProfile.SocialDrive is primary)
}

var precomputedCache = NewSignalCache[*precomputedSignals](2 * time.Minute)
var tieStrengthCache = NewSignalCache[map[string]float64](2 * time.Minute)

// warmPrecomputedSignals loads the analytics-job output for a user into the
// per-request cache. Called alongside warmUserSignalCaches at the top of
// SmartFeedHandler. Tolerates missing keys — a brand-new user just has no data,
// every lookup returns zero, ranker falls back to its base weights.
func warmPrecomputedSignals(userID string) {
	p := &precomputedSignals{
		creatorAffinity: make(map[string]float64),
		pageDwellMs:     make(map[string]int64),
	}

	if s, err := rdb.Get(rctx, "creator_affinity:"+userID).Result(); err == nil && s != "" {
		_ = json.Unmarshal([]byte(s), &p.creatorAffinity)
	}
	if s, err := rdb.Get(rctx, "page_dwell:"+userID).Result(); err == nil && s != "" {
		_ = json.Unmarshal([]byte(s), &p.pageDwellMs)
	}
	if s, err := rdb.Get(rctx, "social_drive:"+userID).Result(); err == nil && s != "" {
		if f, err := strconv.ParseFloat(s, 64); err == nil {
			p.socialDrive = f
		}
	}
	precomputedCache.Set(userID, p)

	ties := make(map[string]float64)
	if m, err := rdb.HGetAll(rctx, "tie:"+userID).Result(); err == nil {
		for k, v := range m {
			if f, err := strconv.ParseFloat(v, 64); err == nil {
				ties[k] = f
			}
		}
	}
	tieStrengthCache.Set(userID, ties)
}

// getTieStrength returns the tie-strength score for (userID, otherID) in 0..1.
// Raw scores are soft-capped via 1 - e^(-raw/10) so they saturate near 1 for
// heavy daily chatters rather than running away to very large values.
func getTieStrength(userID, otherID string) float64 {
	m, ok := tieStrengthCache.Get(userID)
	if !ok {
		return 0
	}
	raw, ok := m[otherID]
	if !ok || raw <= 0 {
		return 0
	}
	return 1.0 - math.Exp(-raw/10.0)
}

// getCreatorAffinity returns the user's 0..1 affinity for this creator.
func getCreatorAffinity(userID, creatorID string) float64 {
	p, ok := precomputedCache.Get(userID)
	if !ok {
		return 0
	}
	v := p.creatorAffinity[creatorID]
	// Enforce the [0,1] contract defensively (mirrors getTieStrength's raw<=0
	// guard above). This protects against (a) stale pre-fix Redis values — the
	// analytics job used to store net-negative affinities, which linger under
	// analyticsRedisTTL after this deploy until the next nightly recompute — and
	// (b) any future regression of the source clamp. A net-negative affinity must
	// never reach the ranker as a floorless negative additive term.
	if v < 0 {
		return 0
	}
	if v > 1 {
		return 1
	}
	return v
}

// getPageDwellMs returns this user's average dwell (ms) on a given page type.
func getPageDwellMs(userID, pageName string) int64 {
	p, ok := precomputedCache.Get(userID)
	if !ok {
		return 0
	}
	return p.pageDwellMs[pageName]
}

// (getSocialDriveFallback removed — it was written for a "UserProfile
// failed to load" ranker path that doesn't exist: getOrComputeProfile
// always synthesizes a profile, so UserProfile.SocialDrive is the only
// serve-time source. The nightly job's activity-based variant is still
// warmed into precomputedCache for future consumers.)
