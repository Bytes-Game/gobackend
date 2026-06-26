package main

import (
	"strings"
	"time"
)

// ─────────────────────────────────────────────────────────────────────────────
// Per-user timezone offset (minutes east of UTC, e.g. IST = +330).
//
// Hour-of-day routing was computed in server/UTC time, so the "morning commute /
// late-night" buckets were wrong for every user outside the server timezone. The
// client sends its UTC offset on each feed request; we stash it per user so both
// the profile build (computeUserProfile) and the serve-time lookup
// (categoryHourBoost/energyHourMatch) can bucket by the user's LOCAL hour.
// Default 0 (UTC) preserves the previous behaviour when the client doesn't send
// it — no regression.
// ─────────────────────────────────────────────────────────────────────────────

// storeUserTZOffset records the user's UTC offset (minutes), clamped to a sane
// range. Best-effort: a Redis blip just means we fall back to UTC for a request.
func storeUserTZOffset(userID string, offsetMin int) {
	if rdb == nil || userID == "" {
		return
	}
	if offsetMin < -840 || offsetMin > 840 { // ±14h, the real-world TZ range
		return
	}
	_ = rdb.Set(rctx, "tz:"+userID, offsetMin, 90*24*time.Hour).Err()
}

// getUserTZOffset returns the stored offset in minutes, or 0 (UTC) if unknown.
func getUserTZOffset(userID string) int {
	if rdb == nil || userID == "" {
		return 0
	}
	v, err := rdb.Get(rctx, "tz:"+userID).Int()
	if err != nil || v < -840 || v > 840 {
		return 0
	}
	return v
}

// ─────────────────────────────────────────────────────────────────────────────
// HOUR-OF-DAY CATEGORY ROUTING
//
// User behavior changes by hour. Morning commute (7-9am) is short-form,
// motivational, news-y. Evening wind-down (9-11pm) is longer, calmer,
// emotional. Late-night (12-2am) is doomscroll territory.
//
// We've been collecting CategoryByHour and EnergyByHour on UserProfile for
// a while but barely using them in the score. This module surfaces three
// signals that the ranker can mix in:
//
//   1. categoryHourBoost — content matching the user's preferred category
//      at the current local hour gets a small ranking lift.
//   2. energyHourMatch — content energy aligned with the user's typical
//      energy preference for this hour matches better.
//   3. timeContextStrategy — picks a candidate strategy mix based on the
//      hour bucket (morning/afternoon/evening/night).
//
// All signals are bounded so a wrong inference (cold profile, edge-case
// timezone) can't capsize the score; they only nudge.
// ─────────────────────────────────────────────────────────────────────────────

const (
	// Maximum lift the hour-routing module can add to a single candidate's
	// score. Kept small because hour-routing is a *prior* — strong recent
	// engagement should still dominate.
	hourCategoryMaxBoost = 0.12
	hourEnergyMaxBoost   = 0.08
)

// hourBucket maps an integer hour (0-23) to a coarse time-of-day label.
// Used for slot-pattern selection and as a fallback when CategoryByHour
// has no entry for the exact hour.
func hourBucket(h int) string {
	switch {
	case h >= 5 && h < 11:
		return "morning"
	case h >= 11 && h < 17:
		return "afternoon"
	case h >= 17 && h < 22:
		return "evening"
	}
	return "night" // 22-04
}

// categoryHourBoost returns the ranking lift for a content's category at
// the current hour, based on what categories this user has historically
// engaged with at this hour. Returns 0 when the profile is cold or the
// content category doesn't match.
//
// Magnitude:
//   exact hour match:     +hourCategoryMaxBoost (0.12)
//   adjacent-hour match:  +hourCategoryMaxBoost * 0.5
//   same-bucket match:    +hourCategoryMaxBoost * 0.25
//   no match:             0
func categoryHourBoost(profile *UserProfile, contentCategory string, now time.Time) float64 {
	if profile == nil || contentCategory == "" || len(profile.CategoryByHour) == 0 {
		return 0
	}
	h := now.Hour()
	cat := strings.ToLower(contentCategory)

	// Exact hour match
	if c, ok := profile.CategoryByHour[h]; ok && strings.EqualFold(c, cat) {
		return hourCategoryMaxBoost
	}
	// Adjacent-hour match (±1)
	for _, dh := range []int{-1, 1} {
		neighbour := (h + dh + 24) % 24
		if c, ok := profile.CategoryByHour[neighbour]; ok && strings.EqualFold(c, cat) {
			return hourCategoryMaxBoost * 0.5
		}
	}
	// Same-bucket match: count how often this category appears in any hour
	// of the same bucket; scale by share.
	bucket := hourBucket(h)
	matches := 0
	bucketHours := 0
	for hh, cc := range profile.CategoryByHour {
		if hourBucket(hh) != bucket {
			continue
		}
		bucketHours++
		if strings.EqualFold(cc, cat) {
			matches++
		}
	}
	if bucketHours > 0 && matches > 0 {
		return hourCategoryMaxBoost * 0.25 * float64(matches) / float64(bucketHours)
	}
	return 0
}

// energyHourMatch returns a bounded match score in [-hourEnergyMaxBoost,
// +hourEnergyMaxBoost] indicating how well content energy aligns with the
// user's typical energy preference at the current hour.
//
// EnergyByHour stores the user's average energy preference per hour
// (0=chill, 1=intense). If the content matches that within ±0.15 we add
// the full boost; further away we taper toward 0; the opposite extreme
// gets a small negative pull.
func energyHourMatch(profile *UserProfile, contentEnergy float64, now time.Time) float64 {
	if profile == nil || len(profile.EnergyByHour) == 0 {
		return 0
	}
	h := now.Hour()
	pref, ok := profile.EnergyByHour[h]
	if !ok {
		// Try the same bucket as a fallback.
		bucket := hourBucket(h)
		var sum float64
		var n int
		for hh, e := range profile.EnergyByHour {
			if hourBucket(hh) == bucket {
				sum += e
				n++
			}
		}
		if n == 0 {
			return 0
		}
		pref = sum / float64(n)
	}
	diff := contentEnergy - pref
	if diff < 0 {
		diff = -diff
	}
	// Continuous, graded taper matching the documented intent:
	//   0 diff → +full, 0.5 diff → 0, 1.0 diff → -full*0.5.
	// (The old code had a cliff at 0.5 — value jumped from ~0 to -0.04 — and then
	// a FLAT -0.04 across the whole upper half, so a mild mismatch and a total
	// opposite were penalized identically.)
	if diff <= 0.15 {
		return hourEnergyMaxBoost
	}
	if diff <= 0.5 {
		// +full → 0 across [0.15, 0.5]
		t := (diff - 0.15) / (0.5 - 0.15)
		return hourEnergyMaxBoost * (1.0 - t)
	}
	// 0 → -full*0.5 across [0.5, 1.0], no cliff
	t := (diff - 0.5) / (1.0 - 0.5)
	return -hourEnergyMaxBoost * 0.5 * t
}

// timeContextStrategyHints returns a list of strategies that historically
// work well for the current time-of-day bucket. Ranker can union these
// with bandit candidates so the soft-mix has hour-aware options to draw
// from.
//
// These are heuristic priors, not learned — but they give the bandit a
// reasonable starting set per bucket so cold users don't have to discover
// from scratch which strategies match late-night vs morning consumption.
func timeContextStrategyHints(now time.Time) []string {
	switch hourBucket(now.Hour()) {
	case "morning":
		// Morning commute: short, social-proof-y, trending. Discovery is
		// risky — users have minutes, not patience.
		return []string{strategyTrending, strategySocial, strategyStandard}
	case "afternoon":
		// Mid-day: balanced. Standard mix + discovery for variety.
		return []string{strategyStandard, strategyDiscovery, strategyTrending}
	case "evening":
		// Evening wind-down: creator deep-dives, mood-matched content,
		// social context (friends posting after work).
		return []string{strategyMoodMatch, strategyCreatorFocus, strategySocial}
	case "night":
		// Late night: calming, nostalgic, low-stimulation.
		return []string{strategyCalming, strategyNostalgic, strategyMoodMatch}
	}
	return nil
}
