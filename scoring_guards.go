package main

// scoring_guards.go — making the ranker safe against its own inputs.
//
// ════════════════════════════════════════════════════════════════════════════
// WHY THIS EXISTS
// ════════════════════════════════════════════════════════════════════════════
//
// scoreForUser is roughly forty terms of arithmetic over three structs it does
// not own. It had no defence of any kind: dereference a nil profile on the
// first line, divide by a budget it assumes is between zero and one, multiply
// by rates it assumes are rates.
//
// An audit found four ways to crash it and a class of input that turns the
// final score into infinity. NONE of them were reachable — every caller today
// happens to satisfy every assumption, and that was verified path by path
// rather than assumed. So this fixes nothing that is broken.
//
// It exists because of what the failure looks like if one of those callers
// ever changes.
//
// A crash is the good outcome: loud, immediate, obviously wrong. The bad one
// is a NaN, because a NaN does not crash and does not log. It compares FALSE
// against everything, including itself, so a sort containing one produces an
// arbitrary order. The feed does not break — it quietly stops being ranked,
// and the only symptom is that recommendations feel worse. That can run for
// months.
//
// The whole ranker depending on eleven separate clamps in
// updateSessionFromEvent staying correct forever is a lot to ask of eleven
// separate clamps.
//
// ════════════════════════════════════════════════════════════════════════════
// WHAT IT DELIBERATELY DOES NOT DO
// ════════════════════════════════════════════════════════════════════════════
//
// It does not change a single score. Every value the system can currently
// produce passes through untouched, because clamping a number that is already
// in range returns the same number. That is the property the tests pin.
//
// It is a floor under the arithmetic, not an opinion about ranking.

import "math"

// clamp01 keeps a rate a rate.
//
// NaN is the case worth reading twice: `if v < 0` and `if v > 1` are BOTH
// false for NaN, so the obvious two-branch version passes it straight through
// and the whole point is lost. The NaN test has to come first.
func clamp01(v float64) float64 {
	if math.IsNaN(v) {
		return 0
	}
	if v < 0 {
		return 0
	}
	if v > 1 {
		return 1
	}
	return v
}

// safeContentScore returns a ContentScore whose numbers cannot break the
// arithmetic downstream.
//
// ════════════════════════════════════════════════════════════════════════════
// IT RETURNS A COPY, AND THAT IS NOT A STYLE CHOICE
// ════════════════════════════════════════════════════════════════════════════
//
// The ContentScore handed to the scorer comes out of contentScoreCache, which
// is shared across every concurrent request in the process. Clamping it in
// place would mutate an object other goroutines are reading — a data race —
// and would permanently alter the cached copy for every future request.
//
// A shallow copy is enough. The two reference fields are only ever read:
// Tags is ranged over, and EmotionVector is looked up. Nothing downstream
// writes to either, so sharing them costs nothing and copying them would cost
// an allocation per candidate per request.
//
// A nil input returns a usable empty score rather than nil, because the one
// caller does not check — see the comment at that call site.
func safeContentScore(cs *ContentScore) *ContentScore {
	if cs == nil {
		return &ContentScore{
			EnergyLevel:      0.5,  // neutral, matching computeContentScore's default
			EnergyLevelLabel: 0.55, // the 'medium' label, same as computeContentScore
			Category:         "general",
			EmotionVector:    map[string]float64{},
		}
	}
	safe := *cs // shallow copy — see above

	// Rates, all genuinely 0..1 by definition.
	safe.AvgCompletionRate = clamp01(safe.AvgCompletionRate)
	safe.SkipRate = clamp01(safe.SkipRate)
	safe.RewatchRate = clamp01(safe.RewatchRate)
	safe.CreatorWinRate = clamp01(safe.CreatorWinRate)
	safe.EnergyLevel = clamp01(safe.EnergyLevel)
	safe.EnergyLevelLabel = clamp01(safe.EnergyLevelLabel)
	safe.QualityScore = clamp01(safe.QualityScore)

	// Wider natural range than a rate, but still bounded.
	//
	// trendingBonus is TrendingScore * 0.15, so a legitimate score around 1
	// contributes about 0.15 — the same order as every other term. The ceiling
	// is ten times that, generous enough that no real value is ever touched and
	// low enough that this term cannot decide a page by itself.
	//
	// clampFinite alone was not enough here: it removes infinity but leaves
	// huge finite values, and 1e308 * 0.15 still drowns out everything else.
	safe.TrendingScore = clampRange(safe.TrendingScore, 0, scoreTermCeiling)
	safe.EngagementVelocity = clampRange(safe.EngagementVelocity, 0, scoreTermCeiling)

	// Counts cannot be negative. A negative view count makes every rate
	// computed from it nonsense, including the ones the quality term divides
	// by.
	safe.ViewCount = max0(safe.ViewCount)
	safe.LikeCount = max0(safe.LikeCount)
	safe.CommentCount = max0(safe.CommentCount)
	safe.ShareCount = max0(safe.ShareCount)
	safe.NotInterestedCount = max0(safe.NotInterestedCount)
	safe.ResponseCount = max0(safe.ResponseCount)
	safe.CreatorFollowers = max0(safe.CreatorFollowers)
	safe.AvgWatchTimeMs = max0(safe.AvgWatchTimeMs)

	// The emotion-match term ranges over this and multiplies by its values.
	safe.EmotionVector = safeWeights(safe.EmotionVector, -1, 1)
	return &safe
}

// scoreTermCeiling bounds the score-like fields that are not rates.
//
// Ten is far above anything the system produces — these sit around 0..1 — and
// far below the point where one term stops ranking content and starts deciding
// the page on its own.
const scoreTermCeiling = 10

func clampRange(v, lo, hi float64) float64 {
	if math.IsNaN(v) {
		return lo
	}
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

// safeWeights bounds the values of a weight map WITHOUT allocating unless it
// has to.
//
// The allocation matters: scoreForUser runs once per candidate, so copying a
// map here would mean a hundred map allocations per feed request. In
// production nothing ever needs correcting, so the scan finds nothing and the
// original map is returned untouched — zero allocations, and the guard is
// still there for the day something does go wrong.
//
// Copy-on-write rather than copy-always, and the copy is essential when it
// does happen: these maps come out of userProfileCache and contentScoreCache,
// shared across every concurrent request.
func safeWeights(m map[string]float64, lo, hi float64) map[string]float64 {
	if m == nil {
		return map[string]float64{}
	}
	needsFixing := false
	for _, v := range m {
		if math.IsNaN(v) || v < lo || v > hi {
			needsFixing = true
			break
		}
	}
	if !needsFixing {
		return m
	}
	out := make(map[string]float64, len(m))
	for k, v := range m {
		out[k] = clampRange(v, lo, hi)
	}
	return out
}

// safeProfile returns a profile the scorer can read without checking.
//
// The default is the one SmartFeedHandler already substitutes when the profile
// load errors, so a nil here behaves exactly like a failed load rather than
// like some third thing.
func safeProfile(p *UserProfile) *UserProfile {
	if p == nil {
		return &UserProfile{
			CategoryAffinity: map[string]float64{},
			EnergyPreference: 0.5,
			SocialDrive:      0.5,
			NoveltyTolerance: 0.5,
		}
	}
	safe := *p
	safe.EnergyPreference = clamp01(safe.EnergyPreference)
	safe.SocialDrive = clamp01(safe.SocialDrive)
	safe.NoveltyTolerance = clamp01(safe.NoveltyTolerance)
	safe.EgoSensitivity = clamp01(safe.EgoSensitivity)
	safe.AvgCompletionRate = clamp01(safe.AvgCompletionRate)
	safe.AvgSkipRate = clamp01(safe.AvgSkipRate)
	// Affinity feeds the relevance term directly — `relevance = affinity` —
	// so its values are score values. computeUserProfile normalises them to
	// 0..1 and the negative miner writes down to about -0.3, so ±1 is
	// generous and still bounded.
	safe.CategoryAffinity = safeWeights(safe.CategoryAffinity, -1, 1)
	return &safe
}

// safeSession returns a session the scorer can read without checking.
//
// DopamineBudget defaults to 1.0, matching getSessionState's own default for a
// visit that has just started — somebody with a full budget rather than an
// exhausted one.
func safeSession(s *SessionState) *SessionState {
	if s == nil {
		return &SessionState{DopamineBudget: 1.0}
	}
	safe := *s
	// The budget is divided by, and it is the value eleven separate writers in
	// updateSessionFromEvent are each responsible for clamping. This is the
	// floor under all eleven.
	safe.DopamineBudget = clamp01(safe.DopamineBudget)
	return &safe
}

func max0(v int) int {
	if v < 0 {
		return 0
	}
	return v
}
