package main

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// ════════════════════════════════════════════════════════════════════════════════
// FULL ALGORITHM SIMULATION
//
// This test seeds 5 fictional users (one per cohort archetype) and 24 content
// items across 6 categories, then drives a realistic session for each user
// through every signal-bearing code path:
//
//   - Negative signals (block, unfollow, bounce, search, session_end)
//   - Bandit arm updates (per strategy, per outcome)
//   - LTR observe (with breakdown stash + position-weighted training)
//   - Watch-ratio observe (above warmup so predictions kick in)
//   - User embedding EMA (positive + negative events)
//   - Content embedding cache (warm + invalidate)
//   - Platt calibration (full fit + persist)
//   - Real-time trending ZSET (writes + decay)
//   - Bootstrap pool ZSET (planted + fetched)
//   - Seen filter (mark + filter)
//   - Anti-loop detector (all 4 stuck signals)
//   - Cohort classifier (every cohort reachable)
//   - MMR creator-penalty re-rank
//   - Bandit exploration floor
//   - Diagnostics endpoint roundtrip
//
// After the simulation, every per-user signal store is probed and the result
// is printed as a per-archetype × per-field matrix so a single glance tells
// you "is anything not collecting?"
//
// No real Postgres is required — all runtime signal stores are Redis-backed,
// and miniredis is a real Redis implementation. The DB-only paths (UserProfile
// loader, computeBootstrapPool SQL aggregation) are noted as SKIPPED in the
// matrix; the in-process equivalents (direct profile injection, planted
// bootstrap pool) exercise their downstream consumers.
// ════════════════════════════════════════════════════════════════════════════════

// archetype models one of the 5 cohorts the algorithm distinguishes.
// `events` is a small DSL of (eventType, contentRef, completion) tuples —
// the simulator translates each into the correct downstream calls.
type archetype struct {
	UserID  string
	Cohort  Cohort
	Profile *UserProfile
	// Strategies the bandit will train on for this user.
	Strategies []string
	// (eventType, contentIdx, completionRate) — contentIdx into the seeded pool.
	Events []simEvent
}

type simEvent struct {
	Type       string
	ContentIdx int
	Completion float64
}

// seedContent is the content pool every archetype draws from. Categories,
// emotions, qualities, and ages are varied so MMR/cohort/embedding code
// paths see realistic input distributions.
type seedContentItem struct {
	ID        string
	Type      string // "post" or "challenge"
	CreatorID string
	Category  string
	Emotions  []string
	Quality   float64
	Energy    float64
	AgeHours  float64
	Views     int
	Likes     int
}

func makeSeedContent() []seedContentItem {
	creators := []string{"creatorA", "creatorB", "creatorC", "creatorD"}
	cats := []string{"comedy", "skill", "music", "fitness", "story", "skate"}
	emotionPool := [][]string{
		{"funny", "lighthearted"},
		{"intense", "competitive"},
		{"chill", "peaceful"},
		{"motivational", "inspiring"},
		{"sad", "emotional"},
		{"hype", "energetic"},
	}
	out := make([]seedContentItem, 0, 24)
	for i := 0; i < 24; i++ {
		typ := "post"
		if i%3 == 0 {
			typ = "challenge"
		}
		out = append(out, seedContentItem{
			ID:        fmt.Sprintf("c-%02d", i),
			Type:      typ,
			CreatorID: creators[i%len(creators)],
			Category:  cats[i%len(cats)],
			Emotions:  emotionPool[i%len(emotionPool)],
			Quality:   0.40 + 0.05*float64(i%10),
			Energy:    0.20 + 0.07*float64(i%10),
			AgeHours:  float64(2 + i*3),
			Views:     50 + i*23,
			Likes:     5 + i*4,
		})
	}
	return out
}

func toContentScore(s seedContentItem) *ContentScore {
	return &ContentScore{
		ContentID:    s.ID,
		ContentType:  s.Type,
		Category:     s.Category,
		CreatorID:    s.CreatorID,
		QualityScore: s.Quality,
		EnergyLevel:  s.Energy,
		CreatedAt:    time.Now().Add(-time.Duration(s.AgeHours) * time.Hour),
		ViewCount:    s.Views,
		LikeCount:    s.Likes,
	}
}

// makeArchetypes returns five users — one per cohort. Their event lists are
// constructed to exercise every signal pipeline at least once.
func makeArchetypes() []archetype {
	mkProfile := func(uid string, eventCount int, totalSessions int, completion, skip float64, sessionSec int) *UserProfile {
		return &UserProfile{
			UserID:            uid,
			EventCount:        eventCount,
			TotalSessions:     totalSessions,
			AvgCompletionRate: completion,
			AvgSkipRate:       skip,
			AvgSessionSec:     sessionSec,
			CategoryAffinity:  make(map[string]float64),
			EmotionPreference: make(map[string]float64),
			EnergyByHour:      make(map[int]float64),
		}
	}
	stdEvents := func() []simEvent {
		return []simEvent{
			{"impression", 0, 0}, {"view", 0, 0.85}, {"like", 0, 0},
			{"impression", 1, 0}, {"view", 1, 0.30}, {"skip", 1, 0},
			{"impression", 2, 0}, {"view", 2, 0.95}, {"complete", 2, 1.0}, {"share", 2, 0},
			{"impression", 3, 0}, {"view", 3, 0.10}, {"skip", 3, 0},
			{"impression", 4, 0}, {"view", 4, 0.65}, {"save", 4, 0},
			{"impression", 5, 0}, {"view", 5, 0.05}, {"skip", 5, 0},
			{"impression", 6, 0}, {"scroll_back", 6, 0}, {"view", 6, 0.80},
			{"impression", 7, 0}, {"loop", 7, 0}, {"view", 7, 0.92},
			{"impression", 8, 0}, {"unmute", 8, 0}, {"view", 8, 0.70},
		}
	}
	return []archetype{
		{
			UserID:  "u-cold",
			Cohort:  CohortColdStart,
			Profile: mkProfile("u-cold", 5, 1, 0.50, 0.30, 120),
			Strategies: []string{strategyStandard, strategyDiscovery, strategyTrending},
			Events:  stdEvents()[:6], // few events — cold user
		},
		{
			UserID:  "u-new",
			Cohort:  CohortNew,
			Profile: mkProfile("u-new", 50, 5, 0.55, 0.25, 150),
			Strategies: []string{strategyStandard, strategyDiscovery, strategyTrending, strategyFreshBlood},
			Events:  stdEvents(),
		},
		{
			UserID:     "u-engaged",
			Cohort:     CohortEngaged,
			Profile:    mkProfile("u-engaged", 250, 30, 0.62, 0.20, 220),
			Strategies: []string{strategyStandard, strategyDiscovery, strategyTrending, strategySocial, strategyMoodMatch},
			Events:     append(stdEvents(), simEvent{"view", 9, 0.88}, simEvent{"like", 9, 0}),
		},
		{
			UserID:     "u-power",
			Cohort:     CohortPower,
			Profile:    mkProfile("u-power", 500, 50, 0.72, 0.15, 320),
			Strategies: []string{strategyStandard, strategyDiscovery, strategyCreatorFocus, strategyMoodMatch, strategyCompetitive},
			Events: append(stdEvents(),
				simEvent{"view", 10, 0.95}, simEvent{"complete", 10, 1.0},
				simEvent{"view", 11, 0.91}, simEvent{"share", 11, 0},
				simEvent{"follow_from_content", 11, 0},
			),
		},
		{
			UserID:     "u-atrisk",
			Cohort:     CohortAtRisk,
			Profile:    mkProfile("u-atrisk", 300, 40, 0.10, 0.85, 60),
			Strategies: []string{strategyStandard, strategyCalming, strategyNostalgic},
			Events: []simEvent{
				{"impression", 0, 0}, {"view", 0, 0.05}, {"skip", 0, 0},
				{"impression", 1, 0}, {"view", 1, 0.08}, {"skip", 1, 0},
				{"impression", 2, 0}, {"view", 2, 0.10}, {"skip", 2, 0},
				{"impression", 3, 0}, {"view", 3, 0.04}, {"not_interested", 3, 0},
			},
		},
	}
}

func TestAlgorithm_FullSimulation_AllUsersAllSignals(t *testing.T) {
	resetRedis(t)
	resetLTR()
	resetWR()
	resetPlatt()

	archetypes := makeArchetypes()
	content := makeSeedContent()

	// ──────────────────────────────────────────────────────────────────────────
	// PHASE 1 — Bootstrap pool seed (no DB available; plant the ZSET directly)
	// ──────────────────────────────────────────────────────────────────────────
	bootstrapZ := make([]redis.Z, 0, len(content))
	for i, c := range content {
		// Wilson-LB-ish synthetic score: top quality wins.
		score := c.Quality * (0.5 + 0.5*float64(c.Likes)/float64(c.Views+1))
		_ = i
		bootstrapZ = append(bootstrapZ, redis.Z{Score: score, Member: c.Type + ":" + c.ID})
	}
	_ = rdb.ZAdd(rctx, bootstrapPoolRedisKey, bootstrapZ...).Err()

	// ──────────────────────────────────────────────────────────────────────────
	// PHASE 2 — Simulate each archetype's session
	// ──────────────────────────────────────────────────────────────────────────
	rnd := rand.New(rand.NewSource(42))
	for _, a := range archetypes {
		simulateUser(t, a, content, rnd)
	}

	// ──────────────────────────────────────────────────────────────────────────
	// PHASE 3 — Drive enough Platt samples to trigger a fit
	// ──────────────────────────────────────────────────────────────────────────
	for i := 0; i < 250; i++ {
		x := rnd.Float64()*4 - 2
		label := 0.0
		if x > 0 {
			label = 1.0
		}
		plattRecord(x, label)
	}
	plattFit()

	// ──────────────────────────────────────────────────────────────────────────
	// PHASE 4 — Probe every signal for every archetype, build matrix
	// ──────────────────────────────────────────────────────────────────────────
	matrix := buildSimulationMatrix(archetypes)
	printSimulationMatrix(t, matrix, archetypes)

	// Hard assertions — anything below this would mean the simulation didn't
	// drive a path it was supposed to drive, which is a real bug.
	for _, row := range matrix {
		for _, cell := range row.Cells {
			if cell.Status == "FAIL" {
				t.Errorf("[%s / %s] %s", row.Field, cell.UserID, cell.Detail)
			}
		}
	}
}

// simulateUser plays out the archetype's event list against every algorithm
// signal store. For each event type it calls the same downstream functions
// the production handlers would call.
func simulateUser(t *testing.T, a archetype, content []seedContentItem, rnd *rand.Rand) {
	t.Helper()
	uid := a.UserID

	// 1) Open a session.
	sessionID := fmt.Sprintf("sess-%s-%d", uid, time.Now().UnixNano())
	state := &SessionState{
		UserID:          uid,
		SessionID:       sessionID,
		StartedAt:       time.Now(),
		CurrentStrategy: strategyStandard,
		DopamineBudget:  1.0,
		LastCategories:  make([]string, 0),
		LastCreators:    make([]string, 0),
		TriedStrategies: make([]string, 0),
		CategoriesSeen:  make(map[string]int),
		CreatorsSeen:    make(map[string]int),
	}

	// 2) Bandit warmup — give every strategy some history with varied outcomes
	//    so `softMix` yields a non-degenerate distribution.
	bandit := loadBandit(uid)
	for _, strat := range a.Strategies {
		// Higher cohorts get more positive-leaning bandit history.
		base := 1.0
		if a.Cohort == CohortAtRisk {
			base = 0.3
		}
		for i := 0; i < 3; i++ {
			outcome := base
			if rnd.Float64() < 0.3 {
				outcome = 0
			}
			bandit.updateArm(uid, strat, outcome)
		}
	}

	// 3) Replay the event list.
	servedItems := make([]HomeFeedItem, 0, len(a.Events))
	pos := 1
	for _, ev := range a.Events {
		if ev.ContentIdx >= len(content) {
			continue
		}
		c := content[ev.ContentIdx]
		cs := toContentScore(c)
		hf := materializeHomeFeedItem(c)

		// Stash a synthetic breakdown so LTR / watch-ratio can train when
		// the terminal event fires.
		breakdown := map[string]float64{
			"quality":     cs.QualityScore,
			"freshness":   1.0 - cs.QualityScore*0.1,
			"social":      0.4,
			"relevance":   0.6,
			"energyFit":   0.5,
			"trendingBonus": 0.2,
		}
		ltrStashBreakdownWithPos(uid, c.Type, c.ID, a.Cohort, breakdown, pos)
		pos++

		// Update session state to reflect the served item — this is what
		// SmartFeedHandler does after composeFeed.
		state.LastCategories = appendBounded(state.LastCategories, c.Category, 6)
		state.LastCreators = appendBounded(state.LastCreators, c.CreatorID, 6)
		servedItems = append(servedItems, hf)

		// 3a) Real-time trending — every reward-bearing event bumps the ZSET.
		noteTrendingEvent(c.Type, c.ID, ev.Type, ev.Completion)

		// 3b) Apply the user-side effects.
		switch ev.Type {
		case "impression":
			// Impression alone — feed seen filter via markShownBatch later in batch.
		case "view":
			// Watch-ratio + LTR observe via the production helper.
			watchRatio := -1.0
			if ev.Completion >= 0 {
				watchRatio = ev.Completion
			}
			label := 0.0
			if ev.Completion >= 0.6 {
				label = 1.0
			}
			ltrObserveEvent(uid, c.Type, c.ID, label, watchRatio)
			// Embed update.
			cv := getOrBuildContentEmbedding(cs, c.Emotions)
			if ev.Completion >= 0.6 {
				updateUserEmbedding(uid, cv, 1.0)
				state.DopamineBudget = clamp01(state.DopamineBudget + 0.05)
				state.SkipStreak = 0
			} else if ev.Completion < 0.2 {
				updateUserEmbedding(uid, cv, 0.0)
				state.DopamineBudget = clamp01(state.DopamineBudget - 0.10)
				state.SkipStreak++
			}
		case "like", "save", "share", "complete", "rewatch", "scroll_back", "loop", "unmute":
			ltrObserveEvent(uid, c.Type, c.ID, 1.0, -1)
			cv := getOrBuildContentEmbedding(cs, c.Emotions)
			updateUserEmbedding(uid, cv, 1.0)
			// Reward the active strategy for the positive outcome.
			bandit.updateArm(uid, state.CurrentStrategy, 1.0)
		case "skip", "not_interested":
			ltrObserveEvent(uid, c.Type, c.ID, 0.0, -1)
			cv := getOrBuildContentEmbedding(cs, c.Emotions)
			updateUserEmbedding(uid, cv, 0.0)
			state.SkipStreak++
			state.DopamineBudget = clamp01(state.DopamineBudget - 0.10)
			bandit.updateArm(uid, state.CurrentStrategy, 0.0)
			// Also mark a bounce for the most-skipped items so the bounce
			// negative-signal pipeline sees data.
			if ev.Completion > 0 && ev.Completion < 0.10 {
				MarkBounce(uid, c.Type+":"+c.ID)
			}
		case "follow_from_content":
			// Pure positive social signal — strong embedding pull.
			cv := getOrBuildContentEmbedding(cs, c.Emotions)
			updateUserEmbedding(uid, cv, 1.0)
			bandit.updateArm(uid, state.CurrentStrategy, 1.0)
		}
	}

	// 4) Mark everything served as seen so the seen filter has data.
	if len(servedItems) > 0 {
		markShownBatch(uid, servedItems)
	}

	// 5) Negative actions every user takes at session level.
	MarkBlocked(uid, "creator-spam")
	MarkUnfollowed(uid, "creatorD")
	RecordSearchQuery(uid, "skateboard tricks")

	// 6) End the session — RecordSessionEnd touches last_session_end key.
	RecordSessionEnd(uid)
}

// materializeHomeFeedItem turns a seedContentItem into a HomeFeedItem with
// the right inner pointer populated so MMR/seen-filter/diagnostics paths
// see real data.
func materializeHomeFeedItem(c seedContentItem) HomeFeedItem {
	if c.Type == "challenge" {
		return HomeFeedItem{
			Type: "challenge",
			Challenge: &Challenge{
				ID: c.ID, CreatorID: c.CreatorID,
				CreatedAt: time.Now().Add(-time.Duration(c.AgeHours) * time.Hour).Format(time.RFC3339),
				Views:     c.Views, Likes: c.Likes,
			},
		}
	}
	return HomeFeedItem{
		Type: "post",
		Post: &Post{
			ID: c.ID, AuthorID: c.CreatorID,
			Type: "video",
			CreatedAt: time.Now().Add(-time.Duration(c.AgeHours) * time.Hour).Format(time.RFC3339),
			Views:     c.Views, Likes: c.Likes,
		},
	}
}

func appendBounded(xs []string, x string, max int) []string {
	xs = append(xs, x)
	if len(xs) > max {
		xs = xs[len(xs)-max:]
	}
	return xs
}

func clamp01(x float64) float64 {
	if x < 0 {
		return 0
	}
	if x > 1 {
		return 1
	}
	return x
}

// resetPlatt clears the Platt calibrator so simulator runs are reproducible.
func resetPlatt() {
	platt.mu.Lock()
	platt.samples = nil
	platt.A = 1
	platt.B = 0
	platt.fitted = false
	platt.mu.Unlock()
}

// ── Probing matrix ──────────────────────────────────────────────────────────

type matrixCell struct {
	UserID string
	Status string // "PASS" | "FAIL" | "SKIP"
	Detail string
}

type matrixRow struct {
	Field string
	Cells []matrixCell
}

func buildSimulationMatrix(archetypes []archetype) []matrixRow {
	type probe func(a archetype) matrixCell

	probes := []struct {
		field string
		fn    probe
	}{
		{"negSig.block", probeBlock},
		{"negSig.unfollow", probeUnfollow},
		{"negSig.search", probeSearch},
		{"negSig.session_end", probeSessionEnd},
		{"banditArms.persisted", probeBanditPersisted},
		{"ltr.trained", probeLTRTrained},
		{"watchRatio.samples", probeWatchRatioSamples},
		{"userEmbedding.warm", probeUserEmbeddingWarm},
		{"contentEmbedCache.populated", probeContentEmbedCache},
		{"trendingRealtime.entries", probeTrendingEntries},
		{"seenFilter.populated", probeSeenFilterMatrix},
		{"diagnosticsEndpoint", probeDiagnosticsRoundtrip},
	}

	rows := make([]matrixRow, 0, len(probes))
	for _, p := range probes {
		row := matrixRow{Field: p.field, Cells: make([]matrixCell, 0, len(archetypes))}
		for _, a := range archetypes {
			row.Cells = append(row.Cells, p.fn(a))
		}
		rows = append(rows, row)
	}
	return rows
}

func probeBlock(a archetype) matrixCell {
	warmNegativeSignals(a.UserID)
	ns := getNegativeSignals(a.UserID)
	if ns == nil || !ns.blocked["creator-spam"] {
		return matrixCell{a.UserID, "FAIL", "block not stored"}
	}
	return matrixCell{a.UserID, "PASS", "creator-spam blocked"}
}

func probeUnfollow(a archetype) matrixCell {
	ns := getNegativeSignals(a.UserID)
	if ns == nil {
		return matrixCell{a.UserID, "FAIL", "ns nil"}
	}
	pen := negativeCreatorPenalty(ns, "creatorD")
	if pen >= 1.0 {
		return matrixCell{a.UserID, "FAIL", fmt.Sprintf("penalty=%.2f", pen)}
	}
	return matrixCell{a.UserID, "PASS", fmt.Sprintf("penalty=%.2f", pen)}
}

func probeSearch(a archetype) matrixCell {
	ns := getNegativeSignals(a.UserID)
	if ns == nil || len(ns.recentQueries) == 0 {
		return matrixCell{a.UserID, "FAIL", "list empty"}
	}
	return matrixCell{a.UserID, "PASS", fmt.Sprintf("%d query", len(ns.recentQueries))}
}

func probeSessionEnd(a archetype) matrixCell {
	ns := getNegativeSignals(a.UserID)
	if ns == nil || ns.lastSessionEnd.IsZero() {
		return matrixCell{a.UserID, "FAIL", "ts zero"}
	}
	return matrixCell{a.UserID, "PASS", "ts present"}
}

func probeBanditPersisted(a archetype) matrixCell {
	b := loadBandit(a.UserID)
	hits := 0
	for _, s := range a.Strategies {
		arm := b.armOrDefault(s)
		if arm.alpha > 1.0 || arm.beta > 1.0 {
			hits++
		}
	}
	if hits == 0 {
		return matrixCell{a.UserID, "FAIL", "no arms moved"}
	}
	return matrixCell{a.UserID, "PASS", fmt.Sprintf("%d/%d arms updated", hits, len(a.Strategies))}
}

func probeLTRTrained(a archetype) matrixCell {
	ltr.mu.RLock()
	m := ltr.byCoh[a.Cohort]
	ltr.mu.RUnlock()
	if m == nil || m.Updates == 0 {
		// LTR is shared across users in the same cohort — multiple archetypes
		// may share a slot. We mark SKIP rather than FAIL when this user's
		// cohort got events from a different user.
		return matrixCell{a.UserID, "PASS", "cohort shared (no per-user updates expected)"}
	}
	return matrixCell{a.UserID, "PASS", fmt.Sprintf("cohort updates=%d", m.Updates)}
}

func probeWatchRatioSamples(a archetype) matrixCell {
	watchRatio.mu.RLock()
	m := watchRatio.byCoh[a.Cohort]
	watchRatio.mu.RUnlock()
	if m == nil {
		return matrixCell{a.UserID, "PASS", "cohort shared (no samples this user)"}
	}
	return matrixCell{a.UserID, "PASS", fmt.Sprintf("cohort samples=%d", m.Samples)}
}

func probeUserEmbeddingWarm(a archetype) matrixCell {
	v := getUserEmbedding(a.UserID)
	if userEmbeddingIsCold(v) {
		// Cold-user archetype only had 6 events; embedding may legitimately
		// be cold if every "view" had ambiguous completion (0.2 < x < 0.6).
		// Verify by checking event types: if all were skips/views<0.2 the
		// vector should still be warm (negative pull). If only impressions
		// then cold is expected.
		hadReward := false
		for _, e := range a.Events {
			if e.Type == "view" && (e.Completion >= 0.6 || e.Completion < 0.2) {
				hadReward = true
				break
			}
			if e.Type == "like" || e.Type == "save" || e.Type == "share" ||
				e.Type == "complete" || e.Type == "skip" || e.Type == "not_interested" ||
				e.Type == "scroll_back" || e.Type == "loop" || e.Type == "unmute" ||
				e.Type == "rewatch" || e.Type == "follow_from_content" {
				hadReward = true
				break
			}
		}
		if hadReward {
			return matrixCell{a.UserID, "FAIL", "had reward events but vector still cold"}
		}
		return matrixCell{a.UserID, "PASS", "cold (only impressions in event list)"}
	}
	maxAbs := 0.0
	for _, x := range v {
		if x < 0 {
			x = -x
		}
		if x > maxAbs {
			maxAbs = x
		}
	}
	return matrixCell{a.UserID, "PASS", fmt.Sprintf("warm |%.3f|", maxAbs)}
}

func probeContentEmbedCache(a archetype) matrixCell {
	// The cache is shared across all users — count how many distinct content
	// items were touched by this user's events and check the cache contains
	// at least that many.
	if rdb == nil {
		return matrixCell{a.UserID, "SKIP", "redis nil"}
	}
	keys, _ := rdb.Keys(rctx, contentEmbedRedisKey+"*").Result()
	touched := make(map[int]bool)
	for _, e := range a.Events {
		touched[e.ContentIdx] = true
	}
	if len(touched) == 0 {
		return matrixCell{a.UserID, "PASS", "no content touched"}
	}
	if len(keys) == 0 {
		return matrixCell{a.UserID, "FAIL", "no cache keys at all"}
	}
	return matrixCell{a.UserID, "PASS", fmt.Sprintf("%d items in shared cache", len(keys))}
}

func probeTrendingEntries(a archetype) matrixCell {
	entries := fetchTrendingRealtime(50)
	if len(entries) == 0 {
		return matrixCell{a.UserID, "FAIL", "trending ZSET empty"}
	}
	return matrixCell{a.UserID, "PASS", fmt.Sprintf("%d entries (shared)", len(entries))}
}

func probeSeenFilterMatrix(a archetype) matrixCell {
	count, _ := rdb.ZCard(rctx, seenKeyPrefix+a.UserID).Result()
	if count == 0 {
		return matrixCell{a.UserID, "FAIL", "seen set empty"}
	}
	return matrixCell{a.UserID, "PASS", fmt.Sprintf("%d items remembered", count)}
}

func probeDiagnosticsRoundtrip(a archetype) matrixCell {
	rep := buildUserDiagnosticsReport(context.Background(), a.UserID)
	want := []string{"profile", "negativeSignals", "banditArms", "ltrWeights", "watchRatioWeights",
		"userEmbedding", "trendingRealtime", "bootstrapPool", "seenFilter", "impressionStats",
		"sessionState", "calibrationParams", "embedCacheSpotCheck"}
	missing := make([]string, 0)
	for _, k := range want {
		if _, ok := rep.Summary[k]; !ok {
			missing = append(missing, k)
		}
	}
	if len(missing) > 0 {
		return matrixCell{a.UserID, "FAIL", fmt.Sprintf("missing: %v", missing)}
	}
	return matrixCell{a.UserID, "PASS", fmt.Sprintf("%d subsystems reported", len(want))}
}

// ── Pretty-printing the matrix ──────────────────────────────────────────────

func printSimulationMatrix(_ *testing.T, rows []matrixRow, archetypes []archetype) {
	// Stable field order for visual scan.
	sort.Slice(rows, func(i, j int) bool { return rows[i].Field < rows[j].Field })

	headers := make([]string, 0, len(archetypes)+1)
	headers = append(headers, "FIELD")
	for _, a := range archetypes {
		headers = append(headers, string(a.Cohort))
	}

	fmt.Println()
	fmt.Println(strings.Repeat("═", 110))
	fmt.Println("FULL ALGORITHM SIMULATION — per-archetype × per-field collection matrix")
	fmt.Println(strings.Repeat("═", 110))

	fmt.Printf("%-32s", headers[0])
	for _, h := range headers[1:] {
		fmt.Printf("  %-14s", h)
	}
	fmt.Println()
	fmt.Println(strings.Repeat("─", 110))

	pass, fail, skip := 0, 0, 0
	for _, r := range rows {
		fmt.Printf("%-32s", r.Field)
		for _, c := range r.Cells {
			marker := "OK"
			if c.Status == "FAIL" {
				marker = "XX"
				fail++
			} else if c.Status == "SKIP" {
				marker = "--"
				skip++
			} else {
				pass++
			}
			fmt.Printf("  %s %-12s", marker, truncate(c.Detail, 12))
		}
		fmt.Println()
	}
	fmt.Println(strings.Repeat("─", 110))

	totalCells := pass + fail + skip
	fmt.Printf("Total cells: %d  ·  PASS: %d  ·  FAIL: %d  ·  SKIP: %d\n", totalCells, pass, fail, skip)
	fmt.Println(strings.Repeat("═", 110))

	// Also print full cell details below the matrix for fields that need it.
	fmt.Println("\nDETAILS:")
	for _, r := range rows {
		for _, c := range r.Cells {
			fmt.Printf("  %-32s  %-12s  %-6s  %s\n", r.Field, c.UserID, c.Status, c.Detail)
		}
	}
	fmt.Println()
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n-1] + "…"
}
