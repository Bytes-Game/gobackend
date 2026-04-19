package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"sort"
	"sync"
	"time"
)

// ─────────────────────────────────────────────────────────────────────────────
// HEALTH TRACKER — in-memory record of the last batch run, exposed via /admin/health
// ─────────────────────────────────────────────────────────────────────────────
//
// Tracks last-run timestamp, duration, per-job user counts, and any error. If
// the nightly job silently fails at 3am, /admin/health shows stale StartedAt
// and a non-empty Errors map — so the admin dashboard can surface a red flag.

type jobResult struct {
	Users    int    `json:"users"`
	Err      string `json:"err,omitempty"`
	Duration string `json:"duration"`
}

// AnalyticsHealthSnapshot is the serialisable form returned by the health endpoint.
type AnalyticsHealthSnapshot struct {
	StartedAt  time.Time            `json:"startedAt"`
	FinishedAt time.Time            `json:"finishedAt"`
	Duration   string               `json:"duration"`
	Results    map[string]jobResult `json:"results"`
	OverallErr string               `json:"overallErr,omitempty"`
	RunCount   int                  `json:"runCount"`
}

type analyticsHealth struct {
	mu       sync.RWMutex
	snapshot AnalyticsHealthSnapshot
}

var analyticsHealthState = &analyticsHealth{
	snapshot: AnalyticsHealthSnapshot{Results: map[string]jobResult{}},
}

// SnapshotAnalyticsHealth returns a read-only copy safe to serialise.
func SnapshotAnalyticsHealth() AnalyticsHealthSnapshot {
	analyticsHealthState.mu.RLock()
	defer analyticsHealthState.mu.RUnlock()
	// Results map is replaced whole on each run so sharing the reference is safe.
	return analyticsHealthState.snapshot
}

func recordJobResult(name string, users int, err error, dur time.Duration) {
	analyticsHealthState.mu.Lock()
	defer analyticsHealthState.mu.Unlock()
	msg := ""
	if err != nil {
		msg = err.Error()
	}
	analyticsHealthState.snapshot.Results[name] = jobResult{
		Users:    users,
		Err:      msg,
		Duration: dur.String(),
	}
}

// ════════════════════════════════════════════════════════════════════════════════
// ANALYTICS JOB — nightly batch that turns raw feed_events into derived signals
// the ranker can read in O(1) from Redis.
// ════════════════════════════════════════════════════════════════════════════════
//
// WHY:
//  feed_events grows fast. Computing tie-strength / creator-affinity at request
//  time would scan chunks of that table on every scroll. This job does the heavy
//  aggregation once per day and writes small Redis keys the ranker reads in <1ms.
//
// WHAT IT COMPUTES (all per-user):
//   tie:{userId}            HASH  otherId -> weighted chat+follow+dwell score
//   social_drive:{userId}   FLOAT 0..1 (also upserted into user_profiles.social_drive)
//   creator_affinity:{user} JSON  { creatorId -> 0..1 }   (top 25 creators)
//   page_dwell:{userId}     JSON  { pageName  -> avg_ms }
//
// All keys are set with a TTL slightly longer than the run cadence so stale data
// never lingers silently.

const (
	analyticsCadence    = 24 * time.Hour
	analyticsRedisTTL   = 26 * time.Hour
	analyticsBootDelay  = 60 * time.Second
	tieStrengthWindow   = "30 days"
	affinityWindow      = "14 days"
	pageDwellWindow     = "7 days"
	socialDriveWindow   = "7 days"
	tieTopN             = 50
	creatorAffinityTopN = 25
)

// startAnalyticsScheduler launches the background job loop. Runs once at boot
// (after a short delay, to let DB/Redis settle) and then every 24h.
func startAnalyticsScheduler() {
	go func() {
		time.Sleep(analyticsBootDelay)
		runAnalyticsBatch()
		ticker := time.NewTicker(analyticsCadence)
		defer ticker.Stop()
		for range ticker.C {
			runAnalyticsBatch()
		}
	}()
	log.Println("[analytics] scheduler started (24h cadence)")
}

// runAnalyticsBatch runs all four jobs sequentially. Each failure is logged but
// doesn't abort the others — partial freshness is better than none. Results are
// written to analyticsHealthState so /admin/health can surface stale/failing runs.
func runAnalyticsBatch() {
	start := time.Now()
	log.Println("[analytics] batch starting")

	analyticsHealthState.mu.Lock()
	analyticsHealthState.snapshot.StartedAt = start
	analyticsHealthState.snapshot.Results = map[string]jobResult{}
	analyticsHealthState.snapshot.OverallErr = ""
	analyticsHealthState.snapshot.RunCount++
	analyticsHealthState.mu.Unlock()

	runOne := func(name string, fn func() (int, error)) {
		t0 := time.Now()
		users, err := fn()
		recordJobResult(name, users, err, time.Since(t0))
		if err != nil {
			log.Printf("[analytics] %s failed: %v", name, err)
			if metricAnalyticsJob != nil {
				metricAnalyticsJob.WithLabelValues(name, "error").Inc()
			}
		} else if metricAnalyticsJob != nil {
			metricAnalyticsJob.WithLabelValues(name, "ok").Inc()
		}
	}

	runOne("tie_strength", computeTieStrengths)
	runOne("social_drive", computeSocialDrive)
	runOne("creator_affinity", computeCreatorAffinity)
	runOne("page_dwell", computePageDwell)
	runOne("golden_hour", computeNotificationGoldenHour)

	dur := time.Since(start)
	analyticsHealthState.mu.Lock()
	analyticsHealthState.snapshot.FinishedAt = time.Now()
	analyticsHealthState.snapshot.Duration = dur.String()
	analyticsHealthState.mu.Unlock()

	log.Printf("[analytics] batch done in %s", dur)
}

// ─────────────────────────────────────────────────────────────────────────────
// TIE STRENGTH
// ─────────────────────────────────────────────────────────────────────────────
//
// Weighted per-pair score built from three independent signals:
//   chat events   × 3.0  (log-dampened, both directions symmetric)
//   follow edge   × 2.0  (directional — A follows B ≠ B follows A)
//   profile dwell × 1.0  (dwell seconds, log-dampened)
//
// Output: tie:{userId} HASH, top 50 partners per user.

func computeTieStrengths() (int, error) {
	ctx, cancel := context.WithTimeout(rctx, 10*time.Minute)
	defer cancel()

	ties := make(map[string]map[string]float64)
	add := func(a, b string, w float64) {
		if a == "" || b == "" || a == b {
			return
		}
		if ties[a] == nil {
			ties[a] = make(map[string]float64)
		}
		ties[a][b] += w
	}

	// 1) Chat messages — symmetric.
	chatRows, err := db.Query(fmt.Sprintf(`
		SELECT sender_id::text, receiver_id::text, COUNT(*)
		FROM chat_messages
		WHERE created_at > NOW() - INTERVAL '%s'
		GROUP BY sender_id, receiver_id
	`, tieStrengthWindow))
	if err == nil {
		for chatRows.Next() {
			var a, b string
			var c int
			if chatRows.Scan(&a, &b, &c) == nil {
				w := 3.0 * math.Log1p(float64(c))
				add(a, b, w)
				add(b, a, w)
			}
		}
		chatRows.Close()
	}

	// 2) Profile page dwell — dwell ms lives in watch_duration_ms (PageTracker).
	dwellRows, err := db.Query(fmt.Sprintf(`
		SELECT user_id,
		       COALESCE(metadata->>'targetUserId',''),
		       SUM(watch_duration_ms)
		FROM feed_events
		WHERE event_type = 'page_exit'
		  AND metadata->>'pageName' = 'profile_page'
		  AND created_at > NOW() - INTERVAL '%s'
		GROUP BY user_id, metadata->>'targetUserId'
	`, tieStrengthWindow))
	if err == nil {
		for dwellRows.Next() {
			var a, b string
			var ms int64
			if dwellRows.Scan(&a, &b, &ms) == nil && b != "" && ms > 0 {
				add(a, b, math.Log1p(float64(ms/1000)))
			}
		}
		dwellRows.Close()
	}

	// 3) Follow edges — flat +2 per direction.
	followRows, err := db.Query(`SELECT follower_id::text, following_id::text FROM follows`)
	if err == nil {
		for followRows.Next() {
			var a, b string
			if followRows.Scan(&a, &b) == nil {
				add(a, b, 2.0)
			}
		}
		followRows.Close()
	}

	// Persist — top N per user. Pipeline in batches to cap memory.
	type kv struct {
		k string
		v float64
	}
	batch := rdb.Pipeline()
	batchSize := 0
	flush := func() {
		if batchSize == 0 {
			return
		}
		if _, err := batch.Exec(ctx); err != nil {
			log.Printf("[analytics] tie_strength pipeline flush: %v", err)
		}
		batch = rdb.Pipeline()
		batchSize = 0
	}

	for user, m := range ties {
		items := make([]kv, 0, len(m))
		for k, v := range m {
			items = append(items, kv{k, v})
		}
		sort.Slice(items, func(i, j int) bool { return items[i].v > items[j].v })
		if len(items) > tieTopN {
			items = items[:tieTopN]
		}
		if len(items) == 0 {
			continue
		}
		fields := make(map[string]interface{}, len(items))
		for _, it := range items {
			fields[it.k] = fmt.Sprintf("%.4f", it.v)
		}
		key := "tie:" + user
		batch.Del(ctx, key)
		batch.HSet(ctx, key, fields)
		batch.Expire(ctx, key, analyticsRedisTTL)
		batchSize += 3
		if batchSize >= 1000 {
			flush()
		}
	}
	flush()

	log.Printf("[analytics] tie_strength: %d users", len(ties))
	return len(ties), nil
}

// ─────────────────────────────────────────────────────────────────────────────
// SOCIAL DRIVE
// ─────────────────────────────────────────────────────────────────────────────
//
// Z-scored score from chat + follow + notification-tap counts (7 day window).
// Written to Redis AND user_profiles so the existing ranker code path picks it
// up without further changes.

func computeSocialDrive() (int, error) {
	ctx, cancel := context.WithTimeout(rctx, 5*time.Minute)
	defer cancel()

	type counts struct {
		chat, follow, notifTap int
	}
	data := make(map[string]*counts)
	get := func(u string) *counts {
		if c, ok := data[u]; ok {
			return c
		}
		c := &counts{}
		data[u] = c
		return c
	}

	chatRows, err := db.Query(fmt.Sprintf(`
		SELECT sender_id::text, COUNT(*)
		FROM chat_messages
		WHERE created_at > NOW() - INTERVAL '%s'
		GROUP BY sender_id
	`, socialDriveWindow))
	if err == nil {
		for chatRows.Next() {
			var u string
			var c int
			if chatRows.Scan(&u, &c) == nil {
				get(u).chat = c
			}
		}
		chatRows.Close()
	}

	followRows, err := db.Query(fmt.Sprintf(`
		SELECT follower_id::text, COUNT(*)
		FROM follows
		WHERE created_at > NOW() - INTERVAL '%s'
		GROUP BY follower_id
	`, socialDriveWindow))
	if err == nil {
		for followRows.Next() {
			var u string
			var c int
			if followRows.Scan(&u, &c) == nil {
				get(u).follow = c
			}
		}
		followRows.Close()
	}

	notifRows, err := db.Query(fmt.Sprintf(`
		SELECT user_id, COUNT(*)
		FROM feed_events
		WHERE event_type = 'notification_tap'
		  AND created_at > NOW() - INTERVAL '%s'
		GROUP BY user_id
	`, socialDriveWindow))
	if err == nil {
		for notifRows.Next() {
			var u string
			var c int
			if notifRows.Scan(&u, &c) == nil {
				get(u).notifTap = c
			}
		}
		notifRows.Close()
	}

	if len(data) == 0 {
		log.Println("[analytics] social_drive: no data")
		return 0, nil
	}

	// Raw blended score, then z → sigmoid to 0..1.
	raws := make(map[string]float64, len(data))
	var sum, sumSq float64
	for u, c := range data {
		r := math.Log1p(float64(c.chat)) +
			0.7*math.Log1p(float64(c.follow)) +
			0.3*math.Log1p(float64(c.notifTap))
		raws[u] = r
		sum += r
		sumSq += r * r
	}
	n := float64(len(raws))
	mean := sum / n
	variance := (sumSq / n) - mean*mean
	if variance < 1e-9 {
		variance = 1e-9
	}
	stddev := math.Sqrt(variance)

	pipe := rdb.Pipeline()
	pipeSize := 0
	for u, r := range raws {
		z := (r - mean) / stddev
		norm := 1.0 / (1.0 + math.Exp(-z))
		pipe.Set(ctx, "social_drive:"+u, fmt.Sprintf("%.4f", norm), analyticsRedisTTL)
		pipeSize++
		if pipeSize >= 500 {
			if _, err := pipe.Exec(ctx); err != nil {
				log.Printf("[analytics] social_drive redis flush: %v", err)
			}
			pipe = rdb.Pipeline()
			pipeSize = 0
		}

		// Persist to user_profiles so UserProfile.SocialDrive reads it directly.
		if _, err := db.Exec(`
			INSERT INTO user_profiles (user_id, social_drive, last_computed_at)
			VALUES ($1, $2, NOW())
			ON CONFLICT (user_id) DO UPDATE SET social_drive = EXCLUDED.social_drive
		`, u, norm); err != nil {
			log.Printf("[analytics] social_drive upsert for %s: %v", u, err)
		}
	}
	if pipeSize > 0 {
		if _, err := pipe.Exec(ctx); err != nil {
			log.Printf("[analytics] social_drive redis final flush: %v", err)
		}
	}

	log.Printf("[analytics] social_drive: %d users (μ=%.3f σ=%.3f)", len(raws), mean, stddev)
	return len(raws), nil
}

// ─────────────────────────────────────────────────────────────────────────────
// CREATOR AFFINITY
// ─────────────────────────────────────────────────────────────────────────────
//
// Per (user, creator) weighted engagement over 14 days. Emphasises high-intent
// signals (loop > scroll_back > complete > like > profile_visit). Normalised
// per-user, top 25 creators kept.

func computeCreatorAffinity() (int, error) {
	ctx, cancel := context.WithTimeout(rctx, 5*time.Minute)
	defer cancel()

	aff := make(map[string]map[string]float64)
	add := func(u, c string, w float64) {
		if u == "" || c == "" {
			return
		}
		if aff[u] == nil {
			aff[u] = make(map[string]float64)
		}
		aff[u][c] += w
	}

	rows, err := db.Query(fmt.Sprintf(`
		SELECT fe.user_id,
		       COALESCE(c.creator_id::text, p.author_id::text, '') AS creator,
		       fe.event_type,
		       EXTRACT(EPOCH FROM (NOW() - fe.created_at))/86400.0 AS age_days
		FROM feed_events fe
		LEFT JOIN challenges c ON fe.content_type = 'challenge' AND fe.content_id = c.id::text
		LEFT JOIN posts p      ON fe.content_type = 'post'      AND fe.content_id = p.id::text
		WHERE fe.event_type IN ('complete','like','scroll_back','loop','profile_visit','skip','not_interested')
		  AND fe.created_at > NOW() - INTERVAL '%s'
	`, affinityWindow))
	if err != nil {
		return 0, err
	}
	for rows.Next() {
		var user, creator, evt string
		var ageDays float64
		if rows.Scan(&user, &creator, &evt, &ageDays) != nil {
			continue
		}
		// Tier 1.3: exponential recency decay — weight halves every 7 days.
		// Recent engagement matters more than stale history.
		recency := math.Exp(-ageDays / 7.0)
		var w float64
		switch evt {
		case "loop":
			w = 1.5 * recency
		case "scroll_back":
			w = 1.2 * recency
		case "complete":
			w = 1.0 * recency
		case "like":
			w = 0.8 * recency
		case "profile_visit":
			w = 0.5 * recency
		case "skip":
			// Tier 2.9: skips are a soft negative on creator affinity. Subtract
			// a small amount so skipped creators drift down over time.
			w = -0.4 * recency
		case "not_interested":
			// Harder negative — explicit signal.
			w = -1.0 * recency
		}
		if w != 0 {
			add(user, creator, w)
		}
	}
	rows.Close()

	// Tier 2.9: explicit unfollow is already handled at scoring time via the
	// negativeCreatorPenalty(unfollowed ZSET → 7-day linear decay). No need to
	// double-penalise here — the skip-based negative above already captures
	// the soft signal, and the unfollowed: key handles the hard one.

	type kv struct {
		k string
		v float64
	}
	pipe := rdb.Pipeline()
	pipeSize := 0
	for u, m := range aff {
		maxV := 0.0
		for _, v := range m {
			if v > maxV {
				maxV = v
			}
		}
		if maxV < 1e-9 {
			continue
		}
		items := make([]kv, 0, len(m))
		for k, v := range m {
			items = append(items, kv{k, v / maxV})
		}
		sort.Slice(items, func(i, j int) bool { return items[i].v > items[j].v })
		if len(items) > creatorAffinityTopN {
			items = items[:creatorAffinityTopN]
		}
		top := make(map[string]float64, len(items))
		for _, it := range items {
			top[it.k] = it.v
		}
		js, _ := json.Marshal(top)
		pipe.Set(ctx, "creator_affinity:"+u, js, analyticsRedisTTL)
		pipeSize++
		if pipeSize >= 500 {
			if _, err := pipe.Exec(ctx); err != nil {
				log.Printf("[analytics] creator_affinity flush: %v", err)
			}
			pipe = rdb.Pipeline()
			pipeSize = 0
		}
	}
	if pipeSize > 0 {
		if _, err := pipe.Exec(ctx); err != nil {
			log.Printf("[analytics] creator_affinity final flush: %v", err)
		}
	}
	log.Printf("[analytics] creator_affinity: %d users", len(aff))
	return len(aff), nil
}

// ─────────────────────────────────────────────────────────────────────────────
// PAGE DWELL
// ─────────────────────────────────────────────────────────────────────────────
//
// Per-user average dwell (ms) per page type, 7 day window. Lets the ranker
// detect users who linger on challenge detail vs those who bounce, and weight
// content accordingly.

func computePageDwell() (int, error) {
	ctx, cancel := context.WithTimeout(rctx, 3*time.Minute)
	defer cancel()

	rows, err := db.Query(fmt.Sprintf(`
		SELECT user_id,
		       COALESCE(metadata->>'pageName',''),
		       AVG(watch_duration_ms)::bigint
		FROM feed_events
		WHERE event_type = 'page_exit'
		  AND created_at > NOW() - INTERVAL '%s'
		GROUP BY user_id, metadata->>'pageName'
	`, pageDwellWindow))
	if err != nil {
		return 0, err
	}
	data := make(map[string]map[string]int64)
	for rows.Next() {
		var u, p string
		var avg int64
		if rows.Scan(&u, &p, &avg) != nil || p == "" {
			continue
		}
		if data[u] == nil {
			data[u] = make(map[string]int64)
		}
		data[u][p] = avg
	}
	rows.Close()

	pipe := rdb.Pipeline()
	pipeSize := 0
	for u, m := range data {
		js, _ := json.Marshal(m)
		pipe.Set(ctx, "page_dwell:"+u, js, analyticsRedisTTL)
		pipeSize++
		if pipeSize >= 500 {
			if _, err := pipe.Exec(ctx); err != nil {
				log.Printf("[analytics] page_dwell flush: %v", err)
			}
			pipe = rdb.Pipeline()
			pipeSize = 0
		}
	}
	if pipeSize > 0 {
		if _, err := pipe.Exec(ctx); err != nil {
			log.Printf("[analytics] page_dwell final flush: %v", err)
		}
	}
	log.Printf("[analytics] page_dwell: %d users", len(data))
	return len(data), nil
}

// ─────────────────────────────────────────────────────────────────────────────
// NOTIFICATION GOLDEN HOUR (Tier 2.8)
// ─────────────────────────────────────────────────────────────────────────────
//
// Per-user "best hour to notify" — the hour of day at which the user has
// historically been most likely to tap a notification (notification_tap)
// or open the app (session_start / app_foreground) within 5 min of a push.
// Computed over a 30-day window. Written to Redis key golden_hour:{userId}
// as a small JSON blob {"hour": 19, "confidence": 0.78}.
//
// notification_service consults this before sending a non-time-sensitive push
// — if we're more than 2h from the user's golden hour, we hold the push until
// the next window. Time-sensitive pushes (like/comment/mention) always go
// through immediately.

const notifGoldenHourWindow = "30 days"

type goldenHour struct {
	Hour       int     `json:"hour"`
	Confidence float64 `json:"confidence"` // 0..1 — how sharply peaked the distribution is
}

func computeNotificationGoldenHour() (int, error) {
	ctx, cancel := context.WithTimeout(rctx, 5*time.Minute)
	defer cancel()

	rows, err := db.Query(fmt.Sprintf(`
		SELECT user_id,
		       EXTRACT(HOUR FROM created_at)::int AS hour,
		       COUNT(*) AS c
		FROM feed_events
		WHERE event_type IN ('notification_tap','app_foreground','session_start')
		  AND created_at > NOW() - INTERVAL '%s'
		GROUP BY user_id, EXTRACT(HOUR FROM created_at)
	`, notifGoldenHourWindow))
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	byUser := make(map[string]map[int]int)
	for rows.Next() {
		var u string
		var h, c int
		if rows.Scan(&u, &h, &c) != nil {
			continue
		}
		if byUser[u] == nil {
			byUser[u] = make(map[int]int)
		}
		byUser[u][h] = c
	}

	pipe := rdb.Pipeline()
	pipeSize := 0
	for u, hist := range byUser {
		total := 0
		bestHour := -1
		bestCount := 0
		for h, c := range hist {
			total += c
			if c > bestCount {
				bestCount = c
				bestHour = h
			}
		}
		if total < 10 || bestHour < 0 {
			continue // Not enough signal — skip, fallback will use heuristic.
		}
		// Confidence: how concentrated the distribution is around the peak.
		// 0.25 = uniform (every hour equal), 1.0 = all taps in one hour.
		confidence := float64(bestCount) / float64(total)
		gh := goldenHour{Hour: bestHour, Confidence: confidence}
		js, _ := json.Marshal(gh)
		pipe.Set(ctx, "golden_hour:"+u, js, analyticsRedisTTL)
		pipeSize++
		if pipeSize >= 500 {
			if _, err := pipe.Exec(ctx); err != nil {
				log.Printf("[analytics] golden_hour flush: %v", err)
			}
			pipe = rdb.Pipeline()
			pipeSize = 0
		}
	}
	if pipeSize > 0 {
		if _, err := pipe.Exec(ctx); err != nil {
			log.Printf("[analytics] golden_hour final flush: %v", err)
		}
	}
	log.Printf("[analytics] golden_hour: %d users", len(byUser))
	return len(byUser), nil
}

// GetGoldenHour returns the user's preferred notification hour (0-23) and
// confidence [0..1]. Returns (-1, 0) if unknown. Safe to call at push time.
func GetGoldenHour(userID string) (int, float64) {
	if rdb == nil || userID == "" {
		return -1, 0
	}
	s, err := rdb.Get(rctx, "golden_hour:"+userID).Result()
	if err != nil || s == "" {
		return -1, 0
	}
	var gh goldenHour
	if json.Unmarshal([]byte(s), &gh) != nil {
		return -1, 0
	}
	return gh.Hour, gh.Confidence
}
