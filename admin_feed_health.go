package main

import (
	"encoding/json"
	"net/http"
)

// ════════════════════════════════════════════════════════════════════════════
// FEED HEALTH — the "is my ranking actually working?" dashboard.
//
// The per-user /admin/diagnostics shows ONE user's internal state. This shows
// the GLOBAL outcome KPIs that tell you whether the algorithm is doing its job,
// each with a good/watch/bad verdict so you don't have to memorize thresholds.
//
//   GET /admin/feed-health            (default 24h window)
//   GET /admin/feed-health?window=7d
//
// What to watch and why:
//   - completionRate / skipRate : are people watching vs rejecting? The core
//     "is the ranking matching people to content" signal.
//   - engagementRate            : likes+shares+comments per view.
//   - avgSessionSec             : the north star — longer sessions = the feed is
//     working (mood/retention, not just one good video).
//   - newContentShare           : % of views going to content uploaded in the
//     last 7d. THIS is your "is new content getting discovered, not buried"
//     metric — if it's near zero, the audition/breakout machinery isn't getting
//     content in front of people (or there are no new uploads).
//   - catalogCoverage           : distinct content shown ÷ catalog. Low = a few
//     items hog all exposure (filter bubble / the rich-get-richer trap).
//   - mlReady                   : are the learned models trained yet, or still
//     running on priors (i.e. you don't have enough data to judge the ML).
// ════════════════════════════════════════════════════════════════════════════

// verdict tags a value good/watch/bad against [goodAt, badAt]. higherIsBetter
// flips the comparison for metrics where smaller is worse.
func verdict(v, goodAt, badAt float64, higherIsBetter bool) string {
	if higherIsBetter {
		switch {
		case v >= goodAt:
			return "good"
		case v <= badAt:
			return "bad"
		default:
			return "watch"
		}
	}
	switch {
	case v <= goodAt:
		return "good"
	case v >= badAt:
		return "bad"
	default:
		return "watch"
	}
}

func AdminFeedHealthHandler(w http.ResponseWriter, r *http.Request) {
	if db == nil {
		http.Error(w, "db unavailable", http.StatusServiceUnavailable)
		return
	}
	window := "24 hours"
	switch r.URL.Query().Get("window") {
	case "7d":
		window = "7 days"
	case "1h":
		window = "1 hour"
	case "30d":
		window = "30 days"
	}

	// 1. Engagement outcomes over the window.
	var views, skips, eng, completes, activeUsers int
	var avgCompletion float64
	_ = db.QueryRow(`
		SELECT
			COUNT(*) FILTER (WHERE event_type = 'view'),
			COUNT(*) FILTER (WHERE event_type = 'skip'),
			COUNT(*) FILTER (WHERE event_type IN ('like','share','comment','save')),
			COUNT(*) FILTER (WHERE event_type = 'complete'),
			COALESCE(AVG(completion_rate) FILTER (WHERE event_type = 'view'), 0),
			COUNT(DISTINCT user_id)
		FROM feed_events
		WHERE created_at > NOW() - ($1)::interval`, window).
		Scan(&views, &skips, &eng, &completes, &avgCompletion, &activeUsers)

	skipRate, engagementRate := 0.0, 0.0
	if views+skips > 0 {
		skipRate = float64(skips) / float64(views+skips)
	}
	if views > 0 {
		engagementRate = float64(eng) / float64(views)
	}

	// 2. New-content discovery: share of challenge-views landing on content
	// uploaded in the last 7 days. Your "is new content getting pushed" metric.
	var chalViews, newViews int
	_ = db.QueryRow(`
		SELECT COUNT(*),
		       COUNT(*) FILTER (WHERE c.created_at > NOW() - INTERVAL '7 days')
		FROM feed_events fe
		JOIN challenges c ON fe.content_type = 'challenge' AND fe.content_id = CAST(c.id AS TEXT)
		WHERE fe.event_type = 'view' AND fe.created_at > NOW() - ($1)::interval`, window).
		Scan(&chalViews, &newViews)
	newContentShare := 0.0
	if chalViews > 0 {
		newContentShare = float64(newViews) / float64(chalViews)
	}

	// 3. Catalog coverage: distinct content shown ÷ arena catalog size.
	var shown, catalog int
	_ = db.QueryRow(`
		SELECT COUNT(DISTINCT content_id) FROM feed_events
		WHERE event_type IN ('view','impression') AND created_at > NOW() - ($1)::interval`, window).Scan(&shown)
	_ = db.QueryRow(`SELECT COUNT(*) FROM challenges WHERE visibility = 'arena'`).Scan(&catalog)
	catalogCoverage := 0.0
	if catalog > 0 {
		catalogCoverage = float64(shown) / float64(catalog)
	}

	// 4. Average session length (north star), from durable profiles.
	var avgSessionSec float64
	_ = db.QueryRow(`SELECT COALESCE(AVG(avg_session_sec), 0) FROM user_profiles WHERE total_sessions > 0`).Scan(&avgSessionSec)

	// 5. Are the learned models trained, or still on priors? (LTR updates per
	// cohort — if all near zero, you don't have the data to judge the ML yet.)
	ltrReady := false
	ltrEnsureLoaded()
	ltr.mu.RLock()
	for _, m := range ltr.byCoh {
		if m != nil && m.Updates >= 20 {
			ltrReady = true
			break
		}
	}
	ltr.mu.RUnlock()

	metric := func(value float64, status, note string) map[string]any {
		return map[string]any{"value": value, "status": status, "note": note}
	}

	resp := map[string]any{
		"window":      window,
		"activeUsers": activeUsers,
		"views":       views,
		"catalogSize": catalog,
		"kpis": map[string]any{
			"completionRate":  metric(round3(avgCompletion), verdict(avgCompletion, 0.5, 0.3, true), "avg % of each video watched — higher = ranking matches people to content"),
			"skipRate":        metric(round3(skipRate), verdict(skipRate, 0.4, 0.6, false), "share of shows that were skipped — lower is better"),
			"engagementRate":  metric(round3(engagementRate), verdict(engagementRate, 0.05, 0.01, true), "likes+shares+comments+saves per view"),
			"avgSessionSec":   metric(round3(avgSessionSec), verdict(avgSessionSec, 180, 60, true), "the north star — longer sessions mean the feed is working overall"),
			"newContentShare": metric(round3(newContentShare), verdict(newContentShare, 0.2, 0.05, true), "share of views on content uploaded in the last 7d — your 'new content gets discovered' metric"),
			"catalogCoverage": metric(round3(catalogCoverage), verdict(catalogCoverage, 0.5, 0.15, true), "distinct content shown ÷ catalog — low = a few items hog exposure"),
		},
		"mlReady": map[string]any{
			"ltrTrained": ltrReady,
			"note":       "false = learned ranking is still on hand-coded priors; it needs ~20+ terminal events per cohort to kick in. Expected until you have real traffic.",
		},
		"howToRead": "Each KPI has status good|watch|bad. With little/no real traffic most will read 'bad' or 'watch' simply because there's no data yet — that's expected. The signal you want post-launch: skipRate trending DOWN, completionRate + avgSessionSec + newContentShare trending UP as users and content grow.",
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

// round3 rounds to 3 decimals for a tidy payload.
func round3(v float64) float64 {
	return float64(int(v*1000+0.5)) / 1000
}
