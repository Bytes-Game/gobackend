package main

import (
	"database/sql"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
)

// ─────────────────────────────────────────────────────────────────────────────
// CREATOR INSIGHTS — feedback loop for creators
//
// Right now creators post into a black box. They see views/likes but not
// WHY a piece performed badly. They guess wrong and post worse content.
//
// This module surfaces the SAME signals the ranker already computes for
// scoring, repackaged for the creator:
//
//   - Per-content quality breakdown (completion, skip-second distribution,
//     reach percentile in their category)
//   - Aggregate trends across all their content (last 30 days)
//   - Plain-English recommendations derived from the signals
//
// Crucially: every benchmark is anonymized to "creators in your category"
// — never names another creator. We're coaching, not gossiping.
// ─────────────────────────────────────────────────────────────────────────────

// CreatorOverview is the top-level dashboard payload.
type CreatorOverview struct {
	CreatorID      string                  `json:"creatorId"`
	WindowDays     int                     `json:"windowDays"`
	TotalPosts     int                     `json:"totalPosts"`
	TotalViews     int                     `json:"totalViews"`
	TotalLikes     int                     `json:"totalLikes"`
	AvgCompletion  float64                 `json:"avgCompletion"`
	CompletionRank string                  `json:"completionRank"` // "top 10%", "below average", etc.
	BestContent    []CreatorContentSummary `json:"bestContent"`    // top 3 by completion
	WorstContent   []CreatorContentSummary `json:"worstContent"`   // bottom 3 by completion
	Recommendations []string               `json:"recommendations"`
	CategoryStats  map[string]CategoryStat `json:"categoryStats"`
}

// CreatorContentSummary is a single piece of content with its key metrics.
type CreatorContentSummary struct {
	ContentID     string  `json:"contentId"`
	ContentType   string  `json:"contentType"`
	Title         string  `json:"title"`
	Category      string  `json:"category"`
	Views         int     `json:"views"`
	Likes         int     `json:"likes"`
	Completion    float64 `json:"completion"`
	SkipRate      float64 `json:"skipRate"`
	CreatedAtIso  string  `json:"createdAt"`
	QualityScore  float64 `json:"qualityScore"`
}

// CategoryStat is anonymous benchmark vs other creators in this category.
type CategoryStat struct {
	YourCompletion       float64 `json:"yourCompletion"`
	CategoryP50          float64 `json:"categoryP50"`
	CategoryP90          float64 `json:"categoryP90"`
	YourPercentile       int     `json:"yourPercentile"` // 0-100
}

// CreatorPerContent is the per-content breakdown for a single piece.
type CreatorPerContent struct {
	ContentID         string             `json:"contentId"`
	ContentType       string             `json:"contentType"`
	Title             string             `json:"title"`
	Category          string             `json:"category"`
	Views             int                `json:"views"`
	Likes             int                `json:"likes"`
	Completion        float64            `json:"completion"`
	SkipRate          float64            `json:"skipRate"`
	QualityScore      float64            `json:"qualityScore"`
	DropOffSeconds    map[int]int        `json:"dropOffSeconds"` // second → count of skips at that mark
	WatchHistogram    map[string]int     `json:"watchHistogram"` // bucket → count (0-25%, 25-50%, ...)
	HookStrength      string             `json:"hookStrength"`   // "strong" | "ok" | "weak"
	EarlySkipPct      float64            `json:"earlySkipPct"`   // % of skips in first 25% of duration
	Recommendations   []string           `json:"recommendations"`
}

// buildCreatorOverview returns the dashboard summary for one creator.
func buildCreatorOverview(creatorID string, windowDays int) (CreatorOverview, error) {
	if db == nil {
		return CreatorOverview{}, fmt.Errorf("db missing")
	}
	if windowDays <= 0 {
		windowDays = 30
	}

	overview := CreatorOverview{
		CreatorID:     creatorID,
		WindowDays:    windowDays,
		CategoryStats: map[string]CategoryStat{},
	}

	// 1. Pull this creator's content with engagement aggregates.
	contents, err := loadCreatorContent(creatorID, windowDays)
	if err != nil {
		return overview, err
	}
	if len(contents) == 0 {
		return overview, nil
	}
	overview.TotalPosts = len(contents)

	// 2. Aggregate totals + per-category yours.
	categorySumByCat := make(map[string]float64)
	categoryCountByCat := make(map[string]int)
	totalCompletion := 0.0
	totalCompletionN := 0
	for _, c := range contents {
		overview.TotalViews += c.Views
		overview.TotalLikes += c.Likes
		if c.Completion > 0 {
			totalCompletion += c.Completion
			totalCompletionN++
		}
		categorySumByCat[c.Category] += c.Completion
		categoryCountByCat[c.Category]++
	}
	if totalCompletionN > 0 {
		overview.AvgCompletion = totalCompletion / float64(totalCompletionN)
	}

	// 3. Sort by completion to pick best/worst.
	sort.Slice(contents, func(i, j int) bool { return contents[i].Completion > contents[j].Completion })
	if n := len(contents); n > 0 {
		overview.BestContent = contents[:min3(n)]
		if n >= 4 {
			overview.WorstContent = reverseSummaries(contents[n-min3(n):])
		}
	}

	// 4. Per-category benchmark vs other creators (anonymized).
	for cat, sum := range categorySumByCat {
		if categoryCountByCat[cat] == 0 {
			continue
		}
		yours := sum / float64(categoryCountByCat[cat])
		stat := categoryBenchmark(cat, yours, creatorID, windowDays)
		overview.CategoryStats[cat] = stat
	}

	// 5. Compute completion rank (text label) vs ALL creators.
	overview.CompletionRank = completionRankLabel(creatorID, overview.AvgCompletion, windowDays)

	// 6. Plain-English recommendations.
	overview.Recommendations = recommendationsForOverview(overview, contents)

	return overview, nil
}

// loadCreatorContent fetches one creator's posts + challenges with engagement
// aggregates over the window.
func loadCreatorContent(creatorID string, windowDays int) ([]CreatorContentSummary, error) {
	out := []CreatorContentSummary{}
	if db == nil {
		return out, fmt.Errorf("db missing")
	}
	rows, err := db.Query(`
		SELECT
			id::text                                AS content_id,
			'challenge'                             AS content_type,
			COALESCE(NULLIF(subject, ''), 'Untitled') AS title,
			COALESCE(category, 'other')             AS category,
			views, COALESCE((SELECT COUNT(*) FROM challenge_likes WHERE challenge_id = c.id), 0) AS likes,
			(SELECT COALESCE(AVG(completion_rate), 0) FROM feed_events
				WHERE content_id = c.id::text AND content_type = 'challenge' AND event_type = 'view'
				  AND created_at > NOW() - ($2::int || ' days')::interval) AS completion,
			(SELECT COALESCE(AVG(CASE WHEN event_type IN ('skip','not_interested') THEN 1.0 ELSE 0.0 END), 0)
				FROM feed_events WHERE content_id = c.id::text AND content_type = 'challenge'
				  AND event_type IN ('view','skip','not_interested')
				  AND created_at > NOW() - ($2::int || ' days')::interval) AS skip_rate,
			created_at
		FROM challenges c
		WHERE c.creator_id::text = $1
		  AND c.created_at > NOW() - ($2::int || ' days')::interval

		UNION ALL

		SELECT
			id::text                                AS content_id,
			'post'                                  AS content_type,
			COALESCE(NULLIF(caption, ''), 'Post')   AS title,
			COALESCE(category, 'other')             AS category,
			views, likes,
			(SELECT COALESCE(AVG(completion_rate), 0) FROM feed_events
				WHERE content_id = p.id::text AND content_type = 'post' AND event_type = 'view'
				  AND created_at > NOW() - ($2::int || ' days')::interval) AS completion,
			(SELECT COALESCE(AVG(CASE WHEN event_type IN ('skip','not_interested') THEN 1.0 ELSE 0.0 END), 0)
				FROM feed_events WHERE content_id = p.id::text AND content_type = 'post'
				  AND event_type IN ('view','skip','not_interested')
				  AND created_at > NOW() - ($2::int || ' days')::interval) AS skip_rate,
			created_at
		FROM posts p
		WHERE p.user_id::text = $1
		  AND p.created_at > NOW() - ($2::int || ' days')::interval

		ORDER BY 9 DESC
	`, creatorID, windowDays)
	if err != nil {
		return out, err
	}
	defer rows.Close()
	for rows.Next() {
		var s CreatorContentSummary
		var createdAt time.Time
		if err := rows.Scan(&s.ContentID, &s.ContentType, &s.Title, &s.Category,
			&s.Views, &s.Likes, &s.Completion, &s.SkipRate, &createdAt); err != nil {
			continue
		}
		s.CreatedAtIso = createdAt.Format(time.RFC3339)
		// Quality score is a simple combination of completion + low-skip + likes-per-view.
		viewsF := float64(s.Views)
		if viewsF < 1 {
			viewsF = 1
		}
		ltvr := float64(s.Likes) / viewsF
		s.QualityScore = 0.5*s.Completion + 0.3*(1.0-s.SkipRate) + 0.2*math.Min(1.0, ltvr*10)
		out = append(out, s)
	}
	return out, nil
}

// categoryBenchmark compares this creator's completion in a category vs
// all OTHER creators in that category. Returns p50, p90, and your percentile.
func categoryBenchmark(category string, yours float64, creatorID string, windowDays int) CategoryStat {
	stat := CategoryStat{
		YourCompletion: yours,
		CategoryP50:    yours, // safe default
		CategoryP90:    yours,
	}
	if db == nil {
		return stat
	}
	rows, err := db.Query(`
		WITH per_creator AS (
			SELECT creator_id::text AS cid,
			       AVG(completion_rate) AS avg_completion
			FROM feed_events fe
			JOIN challenges c ON fe.content_type='challenge' AND fe.content_id = c.id::text
			WHERE COALESCE(c.category,'other') = $1
			  AND fe.event_type = 'view'
			  AND fe.created_at > NOW() - ($3::int || ' days')::interval
			GROUP BY c.creator_id
			HAVING COUNT(*) >= 5
			UNION ALL
			SELECT user_id::text AS cid,
			       AVG(completion_rate) AS avg_completion
			FROM feed_events fe
			JOIN posts p ON fe.content_type='post' AND fe.content_id = p.id::text
			WHERE COALESCE(p.category,'other') = $1
			  AND fe.event_type = 'view'
			  AND fe.created_at > NOW() - ($3::int || ' days')::interval
			GROUP BY p.user_id
			HAVING COUNT(*) >= 5
		)
		SELECT avg_completion FROM per_creator WHERE cid != $2
	`, category, creatorID, windowDays)
	if err != nil {
		return stat
	}
	defer rows.Close()
	others := []float64{}
	for rows.Next() {
		var c float64
		if err := rows.Scan(&c); err == nil {
			others = append(others, c)
		}
	}
	if len(others) == 0 {
		return stat
	}
	sort.Float64s(others)
	stat.CategoryP50 = percentileSorted(others, 50)
	stat.CategoryP90 = percentileSorted(others, 90)
	// Your percentile: how many others are below you.
	below := 0
	for _, o := range others {
		if o < yours {
			below++
		}
	}
	stat.YourPercentile = int(100.0 * float64(below) / float64(len(others)))
	return stat
}

// completionRankLabel turns the creator's avg completion into a friendly
// percentile label vs ALL creators with at least one view in the window.
func completionRankLabel(creatorID string, yourAvg float64, windowDays int) string {
	if db == nil {
		return "no data yet"
	}
	rows, err := db.Query(`
		SELECT AVG(completion_rate) AS avg_c
		FROM feed_events fe
		WHERE fe.event_type = 'view'
		  AND fe.created_at > NOW() - ($2::int || ' days')::interval
		GROUP BY fe.content_id
		HAVING COUNT(*) >= 3
	`, creatorID, windowDays)
	if err != nil {
		return "no data yet"
	}
	defer rows.Close()
	var sample sql.NullFloat64
	xs := []float64{}
	for rows.Next() {
		if err := rows.Scan(&sample); err == nil && sample.Valid {
			xs = append(xs, sample.Float64)
		}
	}
	if len(xs) < 5 {
		return "not enough comparison data"
	}
	sort.Float64s(xs)
	below := 0
	for _, v := range xs {
		if v < yourAvg {
			below++
		}
	}
	pct := int(100.0 * float64(below) / float64(len(xs)))
	switch {
	case pct >= 90:
		return fmt.Sprintf("top 10%% (you beat %d%% of creators)", pct)
	case pct >= 75:
		return fmt.Sprintf("top quartile (you beat %d%% of creators)", pct)
	case pct >= 50:
		return fmt.Sprintf("above average (you beat %d%% of creators)", pct)
	case pct >= 25:
		return fmt.Sprintf("below average (you beat %d%% of creators)", pct)
	default:
		return fmt.Sprintf("bottom quartile (room to grow)")
	}
}

// recommendationsForOverview turns the overview signals into 1-3 plain-
// English nudges. Each rule has a clear threshold so we don't cry wolf.
func recommendationsForOverview(o CreatorOverview, contents []CreatorContentSummary) []string {
	out := []string{}

	// Recommendation 1: low overall completion → hook is the issue.
	if o.AvgCompletion < 0.40 && o.TotalPosts >= 3 {
		out = append(out, "Your average completion is under 40%. The first 3 seconds are doing most of the damage — try opening with a question, a number, or a result instead of a setup.")
	}

	// Recommendation 2: high skip rate → content style mismatch.
	skipSum := 0.0
	skipN := 0
	for _, c := range contents {
		skipSum += c.SkipRate
		skipN++
	}
	if skipN > 0 && skipSum/float64(skipN) > 0.55 {
		out = append(out, "More than half of impressions skip past your content. Look at your top 3 posts vs bottom 3 — the gap usually points to a length / energy mismatch.")
	}

	// Recommendation 3: category-level laggard.
	for cat, stat := range o.CategoryStats {
		if stat.YourPercentile <= 25 && stat.CategoryP50-stat.YourCompletion > 0.10 {
			out = append(out, fmt.Sprintf(
				"In '%s' you're in the bottom 25%%. Median creators in this category complete at %.0f%%; you're at %.0f%%. Watch a couple of category leaders to see what hooks land.",
				cat, stat.CategoryP50*100, stat.YourCompletion*100))
			break // only one category callout to avoid overwhelming
		}
	}

	// Recommendation 4: positive reinforcement when doing well.
	if strings.Contains(o.CompletionRank, "top 10%") {
		out = append(out, "You're in the top 10% by completion — keep your current format and post cadence. This is your unfair advantage.")
	}

	// Recommendation 5: low view counts but high completion → reach problem.
	if o.AvgCompletion > 0.55 && o.TotalViews/max1(o.TotalPosts) < 50 {
		out = append(out, "Your completion rate is strong but views are low. Consider posting at peak hours for your category and using more searchable subject lines.")
	}

	if len(out) == 0 {
		out = append(out, "Your stats look healthy. Keep experimenting with format — variety in subject + length tends to grow audience faster than perfection in one format.")
	}
	return out
}

// buildCreatorPerContent returns the deep-dive insights for a single piece.
func buildCreatorPerContent(creatorID, contentType, contentID string, windowDays int) (CreatorPerContent, error) {
	if db == nil {
		return CreatorPerContent{}, fmt.Errorf("db missing")
	}
	if windowDays <= 0 {
		windowDays = 30
	}

	out := CreatorPerContent{
		ContentID:      contentID,
		ContentType:    contentType,
		DropOffSeconds: map[int]int{},
		WatchHistogram: map[string]int{},
	}

	// Verify creator owns the content (security: don't expose someone else's stats).
	if !creatorOwnsContent(creatorID, contentType, contentID) {
		return out, fmt.Errorf("forbidden")
	}

	// Pull title + category + aggregate stats.
	one, err := loadOneCreatorContent(contentType, contentID, windowDays)
	if err != nil {
		return out, err
	}
	out.Title = one.Title
	out.Category = one.Category
	out.Views = one.Views
	out.Likes = one.Likes
	out.Completion = one.Completion
	out.SkipRate = one.SkipRate
	out.QualityScore = one.QualityScore

	// Watch-time histogram + drop-off seconds from feed_events.
	watchRows, err := db.Query(`
		SELECT
			COALESCE(watch_duration_ms, 0) AS watch_ms,
			COALESCE(total_duration_ms, 0) AS total_ms,
			event_type
		FROM feed_events
		WHERE content_id = $1 AND content_type = $2
		  AND event_type IN ('view','complete','skip','not_interested')
		  AND created_at > NOW() - ($3::int || ' days')::interval
		LIMIT 5000
	`, contentID, contentType, windowDays)
	if err == nil {
		defer watchRows.Close()
		earlySkips := 0
		totalSkips := 0
		for watchRows.Next() {
			var watchMs, totalMs int
			var eventType string
			if err := watchRows.Scan(&watchMs, &totalMs, &eventType); err != nil {
				continue
			}
			// Watch histogram (only for view events with positive duration).
			if eventType == "view" && totalMs > 0 {
				ratio := float64(watchMs) / float64(totalMs)
				bucket := watchBucketLabel(ratio)
				out.WatchHistogram[bucket]++
			}
			// Drop-off bucket per skip second.
			if (eventType == "skip" || eventType == "not_interested") && watchMs > 0 {
				sec := watchMs / 1000
				if sec > 60 {
					sec = 60
				}
				out.DropOffSeconds[sec]++
				totalSkips++
				if totalMs > 0 && float64(watchMs)/float64(totalMs) < 0.25 {
					earlySkips++
				}
			}
		}
		if totalSkips > 0 {
			out.EarlySkipPct = float64(earlySkips) / float64(totalSkips)
		}
	}

	// Hook strength: derived from earlySkipPct + completion.
	out.HookStrength = hookStrengthLabel(out.EarlySkipPct, out.Completion)

	// Per-content recommendations.
	out.Recommendations = recommendationsForContent(out)

	return out, nil
}

// loadOneCreatorContent is loadCreatorContent restricted to a single ID.
func loadOneCreatorContent(contentType, contentID string, windowDays int) (CreatorContentSummary, error) {
	if db == nil {
		return CreatorContentSummary{}, fmt.Errorf("db missing")
	}
	var s CreatorContentSummary
	s.ContentID = contentID
	s.ContentType = contentType

	var query string
	if contentType == "challenge" {
		query = `
			SELECT
				COALESCE(NULLIF(c.subject, ''), 'Untitled'),
				COALESCE(c.category, 'other'),
				c.views,
				COALESCE((SELECT COUNT(*) FROM challenge_likes WHERE challenge_id = c.id), 0),
				(SELECT COALESCE(AVG(completion_rate), 0) FROM feed_events
					WHERE content_id = c.id::text AND content_type = 'challenge' AND event_type = 'view'
					  AND created_at > NOW() - ($2::int || ' days')::interval),
				(SELECT COALESCE(AVG(CASE WHEN event_type IN ('skip','not_interested') THEN 1.0 ELSE 0.0 END), 0)
					FROM feed_events WHERE content_id = c.id::text AND content_type = 'challenge'
					  AND event_type IN ('view','skip','not_interested')
					  AND created_at > NOW() - ($2::int || ' days')::interval)
			FROM challenges c WHERE c.id::text = $1
		`
	} else {
		query = `
			SELECT
				COALESCE(NULLIF(p.caption, ''), 'Post'),
				COALESCE(p.category, 'other'),
				p.views, p.likes,
				(SELECT COALESCE(AVG(completion_rate), 0) FROM feed_events
					WHERE content_id = p.id::text AND content_type = 'post' AND event_type = 'view'
					  AND created_at > NOW() - ($2::int || ' days')::interval),
				(SELECT COALESCE(AVG(CASE WHEN event_type IN ('skip','not_interested') THEN 1.0 ELSE 0.0 END), 0)
					FROM feed_events WHERE content_id = p.id::text AND content_type = 'post'
					  AND event_type IN ('view','skip','not_interested')
					  AND created_at > NOW() - ($2::int || ' days')::interval)
			FROM posts p WHERE p.id::text = $1
		`
	}
	err := db.QueryRow(query, contentID, windowDays).Scan(&s.Title, &s.Category, &s.Views, &s.Likes, &s.Completion, &s.SkipRate)
	if err != nil {
		return s, err
	}
	viewsF := float64(s.Views)
	if viewsF < 1 {
		viewsF = 1
	}
	ltvr := float64(s.Likes) / viewsF
	s.QualityScore = 0.5*s.Completion + 0.3*(1.0-s.SkipRate) + 0.2*math.Min(1.0, ltvr*10)
	return s, nil
}

// creatorOwnsContent prevents a creator from snooping on competitors' deep stats.
func creatorOwnsContent(creatorID, contentType, contentID string) bool {
	if db == nil {
		return false
	}
	var owner string
	var query string
	if contentType == "challenge" {
		query = `SELECT creator_id::text FROM challenges WHERE id::text = $1`
	} else if contentType == "post" {
		query = `SELECT user_id::text FROM posts WHERE id::text = $1`
	} else {
		return false
	}
	if err := db.QueryRow(query, contentID).Scan(&owner); err != nil {
		return false
	}
	return owner == creatorID
}

// recommendationsForContent generates per-content text suggestions.
func recommendationsForContent(c CreatorPerContent) []string {
	out := []string{}

	if c.EarlySkipPct >= 0.60 {
		out = append(out, fmt.Sprintf(
			"%.0f%% of skips happen in the first 25%% of your video. The hook isn't landing — try cutting the intro or opening with the result.",
			c.EarlySkipPct*100))
	}
	if c.Completion < 0.30 && c.Views > 20 {
		out = append(out, "Most viewers stop watching before 30%. Consider trimming the runtime — short-form audiences expect resolution by halfway.")
	}
	if c.Completion > 0.70 && c.Views < 100 {
		out = append(out, "Strong completion but limited reach. Repost at peak hours, or reuse this format with a more searchable subject line.")
	}
	if c.SkipRate > 0.65 {
		out = append(out, "Skip rate above 65%. The thumbnail or first frame might be the issue — viewers are bouncing before audio plays.")
	}
	if len(out) == 0 {
		out = append(out, "Performance is healthy. Pin this in your insights — it's a useful baseline for future content.")
	}
	return out
}

// ── helpers ────────────────────────────────────────────────────────────────

func percentileSorted(xs []float64, p int) float64 {
	if len(xs) == 0 {
		return 0
	}
	idx := int(float64(p) / 100.0 * float64(len(xs)-1))
	if idx < 0 {
		idx = 0
	}
	if idx >= len(xs) {
		idx = len(xs) - 1
	}
	return xs[idx]
}

func watchBucketLabel(ratio float64) string {
	switch {
	case ratio < 0.25:
		return "0-25%"
	case ratio < 0.50:
		return "25-50%"
	case ratio < 0.75:
		return "50-75%"
	case ratio < 1.0:
		return "75-100%"
	}
	return "100%+"
}

func hookStrengthLabel(earlySkipPct, completion float64) string {
	if earlySkipPct < 0.30 && completion > 0.55 {
		return "strong"
	}
	if earlySkipPct < 0.50 && completion > 0.35 {
		return "ok"
	}
	return "weak"
}

func min3(n int) int {
	if n < 3 {
		return n
	}
	return 3
}

func max1(n int) int {
	if n < 1 {
		return 1
	}
	return n
}

func reverseSummaries(in []CreatorContentSummary) []CreatorContentSummary {
	out := make([]CreatorContentSummary, len(in))
	for i, v := range in {
		out[len(in)-1-i] = v
	}
	return out
}
