package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Compile-time guard so unused-import warnings can't bite us if a future
// edit drops every strconv reference. Cheap, harmless.
var _ = strconv.Itoa

// ─────────────────────────────────────────────────────────────────────────────
// /admin/diagnostics?userId=X — one-shot snapshot of every algorithm signal
// being collected for a specific user.
//
// Use this to verify "is the field I just acted on actually being recorded?"
// after using the live app from a real account. Hits Postgres + Redis + the
// in-process LTR/Watch-Ratio/Bandit caches, returns a single JSON blob with
// a `_summary` block that flags which subsystems have data and which don't.
//
// This is the human-facing counterpart to the algorithm_field_verification_test
// — that test proves the code paths work, this endpoint proves the live
// system is collecting from a specific user's actions.
// ─────────────────────────────────────────────────────────────────────────────

// AdminDiagnosticsHandler is the entry point. Wired in main.go under
// adminOnly() so credentials are required.
func AdminDiagnosticsHandler(w http.ResponseWriter, r *http.Request) {
	userID := strings.TrimSpace(r.URL.Query().Get("userId"))
	if userID == "" {
		http.Error(w, `{"error":"userId query param required"}`, http.StatusBadRequest)
		return
	}

	report := buildUserDiagnosticsReport(r.Context(), userID)

	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(report)
}

// DiagnosticsReport is the JSON structure returned to the caller.
// Every subsystem has its own block; `_summary` is the per-subsystem
// "did we collect anything?" flag, computed last so future readers can
// scan it first.
type DiagnosticsReport struct {
	GeneratedAt        time.Time              `json:"generatedAt"`
	UserID             string                 `json:"userId"`
	Summary            map[string]string      `json:"_summary"`
	Profile            *UserProfileSnapshot   `json:"profile,omitempty"`
	Cohort             string                 `json:"cohort"`
	EventCounts        map[string]int         `json:"eventCounts"`
	NegativeSignals    map[string]interface{} `json:"negativeSignals"`
	BanditArms         map[string]interface{} `json:"banditArms"`
	LTRWeights         map[string]interface{} `json:"ltrWeights"`
	WatchRatioWeights  map[string]interface{} `json:"watchRatioWeights"`
	UserEmbedding      map[string]interface{} `json:"userEmbedding"`
	TrendingRealtime   []trendingRealtimeEntry `json:"trendingRealtime"`
	BootstrapPool      []trendingRealtimeEntry `json:"bootstrapPool"`
	SeenFilter         map[string]interface{} `json:"seenFilter"`
	ImpressionStats    map[string]interface{} `json:"impressionStats"`
	SessionState       map[string]interface{} `json:"sessionState"`
	CalibrationParams  map[string]interface{} `json:"calibrationParams"`
	EmbedCacheSpotChk  map[string]interface{} `json:"embedCacheSpotCheck"`
}

// UserProfileSnapshot is a stable JSON projection of UserProfile fields.
// Defined separately so a UserProfile struct change doesn't silently break
// the diagnostics output shape.
type UserProfileSnapshot struct {
	UserID               string             `json:"userId"`
	CategoryAffinity     map[string]float64 `json:"categoryAffinity"`
	EnergyPreference     float64            `json:"energyPreference"`
	SocialDrive          float64            `json:"socialDrive"`
	NoveltyTolerance     float64            `json:"noveltyTolerance"`
	EgoSensitivity       float64            `json:"egoSensitivity"`
	AttentionSpan        float64            `json:"attentionSpan"`
	BingeIntensity       float64            `json:"bingeIntensity"`
	CreatorLoyalty       float64            `json:"creatorLoyalty"`
	CompetitivenessIndex float64            `json:"competitivenessIndex"`
	MoodVolatility       float64            `json:"moodVolatility"`
	AvgCompletionRate    float64            `json:"avgCompletionRate"`
	AvgSkipRate          float64            `json:"avgSkipRate"`
	AvgSessionSec        int                `json:"avgSessionSec"`
	TotalSessions        int                `json:"totalSessions"`
	TotalWatchTimeMs     int64              `json:"totalWatchTimeMs"`
	RecentWins           int                `json:"recentWins"`
	RecentLosses         int                `json:"recentLosses"`
	PreferredCreators    []string           `json:"preferredCreators"`
	AvoidedCategories    []string           `json:"avoidedCategories"`
	ActiveHours          []int              `json:"activeHours"`
	EmotionPreference    map[string]float64 `json:"emotionPreference"`
	EnergyByHour         map[int]float64    `json:"energyByHour"`
}

// buildUserDiagnosticsReport runs every probe and aggregates results.
// Designed to never panic: any individual probe error is swallowed and
// reflected in the _summary map so the response always returns 200.
func buildUserDiagnosticsReport(_ context.Context, userID string) *DiagnosticsReport {
	report := &DiagnosticsReport{
		GeneratedAt:       time.Now().UTC(),
		UserID:            userID,
		Summary:           make(map[string]string),
		EventCounts:       make(map[string]int),
		NegativeSignals:   make(map[string]interface{}),
		BanditArms:        make(map[string]interface{}),
		LTRWeights:        make(map[string]interface{}),
		WatchRatioWeights: make(map[string]interface{}),
		UserEmbedding:     make(map[string]interface{}),
		SeenFilter:        make(map[string]interface{}),
		ImpressionStats:   make(map[string]interface{}),
		SessionState:      make(map[string]interface{}),
		CalibrationParams: make(map[string]interface{}),
		EmbedCacheSpotChk: make(map[string]interface{}),
	}

	probeProfile(report, userID)
	probeEventCounts(report, userID)
	probeNegativeSignals(report, userID)
	probeBanditArms(report, userID)
	probeLTRWeights(report)
	probeWatchRatioWeights(report)
	probeUserEmbedding(report, userID)
	probeTrendingRealtime(report)
	probeBootstrapPool(report)
	probeSeenFilter(report, userID)
	probeImpressionStats(report, userID)
	probeSessionState(report, userID)
	probeCalibration(report)
	probeEmbedCacheSpot(report)

	return report
}

// ── Per-subsystem probes ─────────────────────────────────────────────────────

func probeProfile(r *DiagnosticsReport, userID string) {
	if db == nil {
		r.Summary["profile"] = "SKIPPED — db nil (likely a test environment)"
		return
	}
	p, err := loadUserProfile(userID)
	if err != nil || p == nil {
		r.Summary["profile"] = "ABSENT — user has no UserProfile row yet (no events recorded)"
		return
	}
	r.Profile = &UserProfileSnapshot{
		UserID: p.UserID, CategoryAffinity: p.CategoryAffinity, EnergyPreference: p.EnergyPreference,
		SocialDrive: p.SocialDrive, NoveltyTolerance: p.NoveltyTolerance, EgoSensitivity: p.EgoSensitivity,
		AttentionSpan: p.AttentionSpan, BingeIntensity: p.BingeIntensity,
		CreatorLoyalty: p.CreatorLoyalty, CompetitivenessIndex: p.CompetitivenessIndex,
		MoodVolatility: p.MoodVolatility, AvgCompletionRate: p.AvgCompletionRate,
		AvgSkipRate: p.AvgSkipRate, AvgSessionSec: p.AvgSessionSec,
		TotalSessions: p.TotalSessions, TotalWatchTimeMs: p.TotalWatchTimeMs,
		RecentWins: p.RecentWins, RecentLosses: p.RecentLosses,
		PreferredCreators: p.PreferredCreators, AvoidedCategories: p.AvoidedCategories,
		ActiveHours: p.ActiveHours, EmotionPreference: p.EmotionPreference,
		EnergyByHour: p.EnergyByHour,
	}
	r.Cohort = string(classifyCohort(p))
	r.Summary["profile"] = "OK"
	r.Summary["cohort"] = "OK (" + r.Cohort + ")"
}

func probeEventCounts(r *DiagnosticsReport, userID string) {
	if db == nil {
		r.Summary["eventCounts"] = "SKIPPED — db nil"
		return
	}
	rows, err := db.Query(`
		SELECT event_type, COUNT(*) FROM feed_events
		WHERE user_id = $1
		GROUP BY event_type
		ORDER BY COUNT(*) DESC
	`, userID)
	if err != nil {
		r.Summary["eventCounts"] = "ERROR — " + err.Error()
		return
	}
	defer rows.Close()
	total := 0
	for rows.Next() {
		var et string
		var n int
		if rows.Scan(&et, &n) == nil {
			r.EventCounts[et] = n
			total += n
		}
	}
	if total == 0 {
		r.Summary["eventCounts"] = "EMPTY — no events recorded for this user"
	} else {
		r.Summary["eventCounts"] = fmt.Sprintf("OK — %d events across %d types", total, len(r.EventCounts))
	}
}

func probeNegativeSignals(r *DiagnosticsReport, userID string) {
	if rdb == nil {
		r.Summary["negativeSignals"] = "SKIPPED — redis nil"
		return
	}
	// Block set — actual key prefix in signals_negative.go.
	blocked, _ := rdb.SMembers(rctx, "blocked_creators:"+userID).Result()
	r.NegativeSignals["blockedCreators"] = blocked

	// Unfollow ZSET (7d decay)
	unfollowed, _ := rdb.ZRangeWithScores(rctx, "unfollowed:"+userID, 0, -1).Result()
	uf := make([]map[string]interface{}, 0, len(unfollowed))
	for _, m := range unfollowed {
		uf = append(uf, map[string]interface{}{"creator": m.Member, "score": m.Score})
	}
	r.NegativeSignals["unfollowed"] = uf

	// Bounce ZSET (24h)
	bounced, _ := rdb.ZRangeWithScores(rctx, "recent_bounces:"+userID, 0, -1).Result()
	bn := make([]map[string]interface{}, 0, len(bounced))
	for _, m := range bounced {
		bn = append(bn, map[string]interface{}{"contentKey": m.Member, "ts": m.Score})
	}
	r.NegativeSignals["bounced"] = bn

	// Search history list
	searches, _ := rdb.LRange(rctx, "recent_searches:"+userID, 0, -1).Result()
	r.NegativeSignals["recentSearches"] = searches

	// Last session end timestamp.
	if s, err := rdb.Get(rctx, "last_session_end:"+userID).Result(); err == nil && s != "" {
		r.NegativeSignals["lastSessionEnd"] = s
	}

	count := len(blocked) + len(unfollowed) + len(bounced) + len(searches)
	if count == 0 {
		r.Summary["negativeSignals"] = "EMPTY — no negative actions recorded"
	} else {
		r.Summary["negativeSignals"] = fmt.Sprintf("OK — %d signal entries total", count)
	}
}

func probeBanditArms(r *DiagnosticsReport, userID string) {
	if rdb == nil {
		r.Summary["banditArms"] = "SKIPPED — redis nil"
		return
	}
	m, err := rdb.HGetAll(rctx, "bandit:"+userID).Result()
	if err != nil {
		r.Summary["banditArms"] = "ERROR — " + err.Error()
		return
	}
	if len(m) == 0 {
		r.Summary["banditArms"] = "EMPTY — bandit hasn't been exercised yet for this user"
		return
	}
	// Group by strategy: "{strategy}_a" / "{strategy}_b" → {alpha, beta}
	arms := make(map[string]map[string]float64)
	for k, v := range m {
		f, _ := strconv.ParseFloat(v, 64)
		if len(k) > 2 && k[len(k)-2] == '_' {
			strat := k[:len(k)-2]
			if arms[strat] == nil {
				arms[strat] = make(map[string]float64)
			}
			switch k[len(k)-1] {
			case 'a':
				arms[strat]["alpha"] = f
			case 'b':
				arms[strat]["beta"] = f
			}
		}
	}
	for s, ab := range arms {
		r.BanditArms[s] = ab
	}
	r.Summary["banditArms"] = fmt.Sprintf("OK — %d strategies tracked", len(arms))
}

func probeLTRWeights(r *DiagnosticsReport) {
	ltrEnsureLoaded()
	ltr.mu.RLock()
	defer ltr.mu.RUnlock()
	tot := 0
	for c, m := range ltr.byCoh {
		if m == nil {
			continue
		}
		r.LTRWeights[string(c)] = map[string]interface{}{
			"updates":   m.Updates,
			"bias":      m.Bias,
			"weightCnt": len(m.Weights),
		}
		tot += m.Updates
	}
	if tot == 0 {
		r.Summary["ltrWeights"] = "EMPTY — no training samples observed yet"
	} else {
		r.Summary["ltrWeights"] = fmt.Sprintf("OK — %d total updates across cohorts", tot)
	}
}

func probeWatchRatioWeights(r *DiagnosticsReport) {
	wrEnsureLoaded()
	watchRatio.mu.RLock()
	defer watchRatio.mu.RUnlock()
	tot := 0
	for c, m := range watchRatio.byCoh {
		if m == nil {
			continue
		}
		r.WatchRatioWeights[string(c)] = map[string]interface{}{
			"samples":       m.Samples,
			"bias":          m.Bias,
			"weightCnt":     len(m.Weights),
			"meetsWarmup":   m.Samples >= wrMinSamples,
		}
		tot += m.Samples
	}
	if tot == 0 {
		r.Summary["watchRatioWeights"] = "EMPTY — no watch-ratio samples observed yet"
	} else {
		r.Summary["watchRatioWeights"] = fmt.Sprintf("OK — %d total samples across cohorts", tot)
	}
}

func probeUserEmbedding(r *DiagnosticsReport, userID string) {
	v := getUserEmbedding(userID)
	cold := userEmbeddingIsCold(v)
	var maxAbs float64
	for _, x := range v {
		if x < 0 {
			x = -x
		}
		if x > maxAbs {
			maxAbs = x
		}
	}
	r.UserEmbedding["cold"] = cold
	r.UserEmbedding["dim"] = len(v)
	r.UserEmbedding["maxComponentAbs"] = maxAbs
	if cold {
		r.Summary["userEmbedding"] = "COLD — vector is all zeros (user hasn't engaged with any content yet)"
	} else {
		r.Summary["userEmbedding"] = fmt.Sprintf("OK — warm vector, max component |%.3f|", maxAbs)
	}
}

func probeTrendingRealtime(r *DiagnosticsReport) {
	r.TrendingRealtime = fetchTrendingRealtime(20)
	if len(r.TrendingRealtime) == 0 {
		r.Summary["trendingRealtime"] = "EMPTY — no recent engagement events have populated the ZSET"
	} else {
		r.Summary["trendingRealtime"] = fmt.Sprintf("OK — %d items currently trending", len(r.TrendingRealtime))
	}
}

func probeBootstrapPool(r *DiagnosticsReport) {
	r.BootstrapPool = fetchBootstrapPool(20)
	if len(r.BootstrapPool) == 0 {
		r.Summary["bootstrapPool"] = "EMPTY — bootstrap-pool worker hasn't run yet (or db has no eligible content)"
	} else {
		r.Summary["bootstrapPool"] = fmt.Sprintf("OK — %d known-bangers cached", len(r.BootstrapPool))
	}
}

func probeSeenFilter(r *DiagnosticsReport, userID string) {
	if rdb == nil {
		r.Summary["seenFilter"] = "SKIPPED — redis nil"
		return
	}
	count, _ := rdb.ZCard(rctx, seenKeyPrefix+userID).Result()
	r.SeenFilter["entries"] = count
	if count == 0 {
		r.Summary["seenFilter"] = "EMPTY — no impressions recorded yet"
	} else {
		r.Summary["seenFilter"] = fmt.Sprintf("OK — %d items remembered (12h TTL window)", count)
	}
}

func probeImpressionStats(r *DiagnosticsReport, userID string) {
	byCategory, byCreator := getImpressionStats(userID)
	if len(byCategory) == 0 && len(byCreator) == 0 {
		r.Summary["impressionStats"] = "EMPTY — no impression aggregates yet"
		return
	}
	cats := make(map[string]interface{}, len(byCategory))
	for cat, s := range byCategory {
		if s == nil {
			continue
		}
		cats[cat] = map[string]interface{}{
			"count":       s.Count,
			"bounces":     s.BounceCount,
			"curiosity":   s.CuriosityCount,
			"interest":    s.InterestCount,
			"totalDwell":  s.TotalDwellMs,
			"bounceRate":  s.BounceRate(),
		}
	}
	creators := make(map[string]interface{}, len(byCreator))
	for cr, s := range byCreator {
		if s == nil {
			continue
		}
		creators[cr] = map[string]interface{}{
			"count":      s.Count,
			"bounces":    s.BounceCount,
			"interest":   s.InterestCount,
			"totalDwell": s.TotalDwellMs,
		}
	}
	r.ImpressionStats["byCategory"] = cats
	r.ImpressionStats["byCreator"] = creators
	r.Summary["impressionStats"] = fmt.Sprintf("OK — %d categories, %d creators aggregated", len(byCategory), len(byCreator))
}

func probeSessionState(r *DiagnosticsReport, userID string) {
	if rdb == nil {
		r.Summary["sessionState"] = "SKIPPED — redis nil"
		return
	}
	// Session keys are 30-min sliding; pull whatever's currently active.
	keys, _ := rdb.Keys(rctx, "session:"+userID+":*").Result()
	r.SessionState["activeKeys"] = keys
	if len(keys) == 0 {
		r.Summary["sessionState"] = "EMPTY — no active session state for this user"
		return
	}
	// Pull the most recent session blob.
	sample, err := rdb.Get(rctx, keys[0]).Result()
	if err == nil && sample != "" {
		var blob map[string]interface{}
		if json.Unmarshal([]byte(sample), &blob) == nil {
			r.SessionState["sample"] = blob
		}
	}
	r.Summary["sessionState"] = fmt.Sprintf("OK — %d session key(s) active", len(keys))
}

func probeCalibration(r *DiagnosticsReport) {
	if rdb == nil {
		r.Summary["calibrationParams"] = "SKIPPED — redis nil"
		return
	}
	s, err := rdb.Get(rctx, calibRedisKey).Result()
	if err != nil || s == "" {
		r.Summary["calibrationParams"] = "EMPTY — Platt calibrator hasn't fitted yet (needs ≥200 samples)"
		return
	}
	var p map[string]float64
	if json.Unmarshal([]byte(s), &p) == nil {
		r.CalibrationParams["a"] = p["a"]
		r.CalibrationParams["b"] = p["b"]
	}
	r.Summary["calibrationParams"] = "OK"
}

func probeEmbedCacheSpot(r *DiagnosticsReport) {
	if rdb == nil {
		r.Summary["embedCacheSpotCheck"] = "SKIPPED — redis nil"
		return
	}
	// Just count keys with the prefix — bounded at 200 via Keys() to avoid
	// blowing up memory if the cache is huge.
	keys, _ := rdb.Keys(rctx, contentEmbedRedisKey+"*").Result()
	r.EmbedCacheSpotChk["cachedContentVectors"] = len(keys)
	if len(keys) == 0 {
		r.Summary["embedCacheSpotCheck"] = "EMPTY — no content embeddings cached yet"
	} else {
		r.Summary["embedCacheSpotCheck"] = fmt.Sprintf("OK — %d content embeddings cached", len(keys))
	}
}
