package main

import (
	"math"
	"strconv"
	"time"
)

// ─────────────────────────────────────────────────────────────────────────────
// ENGAGEMENT QUALITY WEIGHTING
//
// Without this, every "like" event in trending counts the same — a like
// from a verified power user with 60% completion history weighs the same
// as a like from a 1-day-old account that drives by 200 posts an hour.
// That's how spam farms manipulate trending in every social app.
//
// We assign each user an *engagement quality multiplier* in [0.2, 2.0]
// derived from observable signals:
//   - account age (older = more trust, capped at 90d)
//   - completion rate (higher = more engaged user, less drive-by)
//   - skip rate (lower = more discriminating)
//   - session count (more sessions = more invested user)
//   - flags / blocks against them (downweight)
//
// The multiplier scales the trending event weight before ZIncrBy. So a
// share from a 10x trusted user has the impact of 10 shares from average,
// while a share from a brand-new, all-skip account has a fifth of the
// impact. The bandit/embedding/LTR signals are unaffected — those are
// per-user state where one user's bias doesn't touch another's feed.
//
// CACHE: per-user multiplier is cached in Redis (10-min TTL) so we don't
// recompute on every event. Cold computation falls back to 1.0 (neutral).
// ─────────────────────────────────────────────────────────────────────────────

const (
	engQualityRedisKey = "engquality:"
	engQualityTTL      = 10 * time.Minute
	engQualityMin      = 0.20
	engQualityMax      = 2.00
	// Score of a median/established profile that should map to the neutral 1.0
	// multiplier (matching the cold/DB-error fallback). The score→multiplier map
	// is anchored here so a typical user isn't systematically deflated below 1.0.
	engQualityBaseline = 0.45
)

// userEngagementQuality returns a multiplier in [engQualityMin, engQualityMax]
// reflecting how trustworthy this user's engagement signal is. Cached in
// Redis with a 10-minute TTL — recomputation is cheap but not free.
//
// Returns 1.0 (neutral) on missing user, missing DB, or any error so the
// caller's math is unchanged.
func userEngagementQuality(userID string) float64 {
	if userID == "" {
		return 1.0
	}
	if rdb != nil {
		if s, err := rdb.Get(rctx, engQualityRedisKey+userID).Result(); err == nil && s != "" {
			if v, err := strconv.ParseFloat(s, 64); err == nil {
				return clampEngQuality(v)
			}
		}
	}
	q := computeEngagementQuality(userID)
	if rdb != nil {
		_ = rdb.Set(rctx, engQualityRedisKey+userID, strconv.FormatFloat(q, 'f', 4, 64), engQualityTTL).Err()
	}
	return q
}

// computeEngagementQuality is the cold-path computation. Reads from the
// users table + feed_events aggregate. Falls back to 1.0 when DB is nil
// (e.g. during tests with sqlmock not set up for these queries).
func computeEngagementQuality(userID string) float64 {
	if db == nil {
		return 1.0
	}
	// Pull a compact snapshot in one round-trip. Aggregates are over the
	// last 30 days so a brand-new account's reputation can grow.
	var ageDays float64
	var totalEvents, completes, skips, sessions, flagsAgainst int
	err := db.QueryRow(`
		SELECT
			COALESCE(EXTRACT(EPOCH FROM (NOW() - u.created_at))/86400, 0) AS age_days,
			(SELECT COUNT(*) FROM feed_events WHERE user_id = u.id::text AND created_at > NOW() - INTERVAL '30 days') AS total_events,
			(SELECT COUNT(*) FROM feed_events WHERE user_id = u.id::text AND event_type = 'complete' AND created_at > NOW() - INTERVAL '30 days') AS completes,
			(SELECT COUNT(*) FROM feed_events WHERE user_id = u.id::text AND event_type IN ('skip','not_interested') AND created_at > NOW() - INTERVAL '30 days') AS skips,
			(SELECT COUNT(DISTINCT session_id) FROM feed_events WHERE user_id = u.id::text AND created_at > NOW() - INTERVAL '30 days') AS sessions,
			(SELECT COUNT(*) FROM reports WHERE target_id = u.id::text AND target_type = 'user' AND created_at > NOW() - INTERVAL '30 days') AS flags_against
		FROM users u
		WHERE u.id::text = $1
	`, userID).Scan(&ageDays, &totalEvents, &completes, &skips, &sessions, &flagsAgainst)
	if err != nil {
		return 1.0
	}

	// Component 1: age trust — log curve, saturates at 90 days.
	ageTrust := math.Log1p(ageDays) / math.Log1p(90)
	if ageTrust > 1 {
		ageTrust = 1
	}

	// Component 2: completion-vs-skip ratio, SHRUNK toward the 0.5 prior with
	// pseudo-counts. The old hard `>5` cutover jumped straight from neutral 0.5
	// to a fully-trusted raw ratio (6 completes / 0 skips → 1.0) — the classic
	// small-sample hole a spam farm exploits to inflate a fresh account's weight
	// toward the 2.0 cap. With a Beta(0.5·K, 0.5·K) prior the estimate only
	// approaches its raw value as real evidence accumulates; no cliff.
	signalEvents := completes + skips
	const completionPriorK = 10.0 // pseudo-observations anchored at the 0.5 prior
	completionRatio := (float64(completes) + 0.5*completionPriorK) /
		(float64(signalEvents) + completionPriorK)

	// Component 3: session richness. log-curve, saturates at 30 sessions.
	sessionRichness := math.Log1p(float64(sessions)) / math.Log1p(30)
	if sessionRichness > 1 {
		sessionRichness = 1
	}

	// Component 4: flags-against penalty. Each report knocks 0.05 off the
	// final multiplier; capped at 0.5 so we don't zero out trust on a
	// brigade.
	flagPenalty := math.Min(0.5, float64(flagsAgainst)*0.05)

	// Combine: weighted average centered on 1.0, range roughly [0.2, 2.0].
	// Why these weights:
	//   age 0.25 — modest because very new users CAN be legit, just unproven
	//   completion 0.45 — strongest signal of "real engagement"
	//   sessions 0.30 — invested users matter more than touch-and-go
	//   flagPenalty subtracted at the end so a flagged-but-active user can
	//   still score above the floor
	score := 0.25*ageTrust + 0.45*completionRatio + 0.30*sessionRichness
	// Map score → multiplier with the neutral 1.0 ANCHORED at a median profile
	// (score == engQualityBaseline), so "centered on 1.0" actually holds and
	// matches the cold/DB-error fallback. The old linear map (0.2 + 1.8·score)
	// put a typical user well below 1.0 (~0.6), systematically deflating every
	// trending weight — which were calibrated assuming a ~1.0 average multiplier.
	var multiplier float64
	if score < engQualityBaseline {
		// below median → [engQualityMin, 1.0]
		multiplier = engQualityMin + (1.0-engQualityMin)*(score/engQualityBaseline)
	} else {
		// at/above median → [1.0, engQualityMax]
		multiplier = 1.0 + (engQualityMax-1.0)*((score-engQualityBaseline)/(1.0-engQualityBaseline))
	}
	multiplier -= flagPenalty
	return clampEngQuality(multiplier)
}

func clampEngQuality(v float64) float64 {
	if v < engQualityMin {
		return engQualityMin
	}
	if v > engQualityMax {
		return engQualityMax
	}
	return v
}

// invalidateEngagementQuality drops the cached multiplier for a user. Call
// this when their account state changes meaningfully (e.g., they get
// reported, or after a long inactive gap they come back). The next read
// will recompute. Optional — the 10-min TTL eventually picks up changes
// regardless.
func invalidateEngagementQuality(userID string) {
	if rdb == nil || userID == "" {
		return
	}
	_ = rdb.Del(rctx, engQualityRedisKey+userID).Err()
}
