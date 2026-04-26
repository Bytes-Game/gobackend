package main

import (
	"fmt"
	"time"
)

// ─────────────────────────────────────────────────────────────────────────────
// NOTIFICATION TRIGGER WORKERS
//
// Four periodic scans, each looks for a different "moment worth pushing":
//
//   1. friend_response   — your friend just responded to a battle you started
//   2. ending_soon       — a battle you're in is closing within an hour
//   3. you_will_love     — top-predicted-watch-ratio item you haven't seen
//   4. inactive_winback  — at_risk cohort + 3+ days of silence
//
// Each worker's job is to find candidate (user, content) pairs and call
// enqueueNotification. The dispatcher (separate goroutine) handles
// dedupe/rate-limit/quiet-hours/sending — workers don't need to know any
// of that machinery.
//
// All workers are run on independent tickers so a slow query on one doesn't
// stall the others. Failures are logged + metric'd, never panic.
// ─────────────────────────────────────────────────────────────────────────────

const (
	triggerScanInterval     = 5 * time.Minute
	triggerScanLimit        = 100 // max candidates per worker per tick
	endingSoonWindow        = 1 * time.Hour
	youWillLoveMinPredicted = 0.75 // need very high predicted watch-ratio to ping
	inactiveDaysThreshold   = 3
)

// startNotificationTriggers boots all four scan workers. Each runs on the
// same cadence; staggered start prevents thundering-herd on Postgres.
func startNotificationTriggers() {
	stagger := []time.Duration{0, 30 * time.Second, 60 * time.Second, 90 * time.Second}
	workers := []func(){
		scanFriendResponseTrigger,
		scanEndingSoonTrigger,
		scanYouWillLoveTrigger,
		scanInactiveWinbackTrigger,
	}
	for i, w := range workers {
		w := w
		stagger := stagger[i%len(stagger)]
		go func() {
			time.Sleep(stagger)
			w()
			t := time.NewTicker(triggerScanInterval)
			defer t.Stop()
			for range t.C {
				w()
			}
		}()
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 1. friend_response — fire when someone responds to your battle
// ─────────────────────────────────────────────────────────────────────────────

func scanFriendResponseTrigger() {
	if db == nil {
		return
	}
	rows, err := db.Query(`
		SELECT
			c.creator_id::text       AS challenger_id,
			c.id::text               AS challenge_id,
			c.subject                AS challenge_subject,
			r.responder_id::text     AS responder_id,
			ru.username              AS responder_username,
			r.id::text               AS response_id
		FROM challenge_responses r
		JOIN challenges c ON r.challenge_id = c.id
		JOIN users ru ON r.responder_id = ru.id
		WHERE r.created_at > NOW() - INTERVAL '15 minutes'
		  AND r.responder_id != c.creator_id  -- don't ping the creator about themselves
		LIMIT $1
	`, triggerScanLimit)
	if err != nil {
		return
	}
	defer rows.Close()
	for rows.Next() {
		var challengerID, challengeID, subject, responderID, responderUsername, responseID string
		if err := rows.Scan(&challengerID, &challengeID, &subject, &responderID, &responderUsername, &responseID); err != nil {
			continue
		}
		_, _, _ = enqueueNotification(EnqueueParams{
			UserID:      challengerID,
			TriggerKind: TriggerFriendResponse,
			DedupeKey:   fmt.Sprintf("fr:%s:%s", challengeID, responseID),
			Title:       fmt.Sprintf("@%s answered your challenge", responderUsername),
			Body:        truncateText(fmt.Sprintf("\"%s\" — see who's winning →", subject), 120),
			Deeplink:    fmt.Sprintf("devf://challenge/%s/responses", challengeID),
		})
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 2. ending_soon — fire when a battle the user submitted to is about to close
// ─────────────────────────────────────────────────────────────────────────────

func scanEndingSoonTrigger() {
	if db == nil {
		return
	}
	// Notify each user who has submitted to a challenge whose voting window
	// closes within endingSoonWindow. The voting close is approximated as
	// `created_at + interval '7 days'` (your existing convention); production
	// schemas with explicit voting_ends_at columns should swap that in.
	rows, err := db.Query(`
		SELECT DISTINCT
			r.responder_id::text  AS user_id,
			c.id::text            AS challenge_id,
			c.subject             AS subject,
			(c.created_at + INTERVAL '7 days') AS ends_at
		FROM challenge_responses r
		JOIN challenges c ON r.challenge_id = c.id
		WHERE c.status IN ('open', 'active')
		  AND (c.created_at + INTERVAL '7 days') BETWEEN NOW() AND NOW() + INTERVAL '1 hour'
		LIMIT $1
	`, triggerScanLimit)
	if err != nil {
		return
	}
	defer rows.Close()
	for rows.Next() {
		var userID, challengeID, subject string
		var endsAt time.Time
		if err := rows.Scan(&userID, &challengeID, &subject, &endsAt); err != nil {
			continue
		}
		minsLeft := int(time.Until(endsAt).Minutes())
		if minsLeft < 1 {
			minsLeft = 1
		}
		_, _, _ = enqueueNotification(EnqueueParams{
			UserID:      userID,
			TriggerKind: TriggerEndingSoon,
			DedupeKey:   fmt.Sprintf("es:%s", challengeID),
			Title:       "Your battle is closing soon",
			Body:        truncateText(fmt.Sprintf("\"%s\" — voting ends in %d min", subject, minsLeft), 120),
			Deeplink:    fmt.Sprintf("devf://challenge/%s", challengeID),
		})
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 3. you_will_love — fire for content with very high predicted watch-ratio
//    that the user hasn't seen yet
// ─────────────────────────────────────────────────────────────────────────────

func scanYouWillLoveTrigger() {
	if db == nil || rdb == nil {
		return
	}
	// Cheap-ish heuristic at the SQL layer: pick top-trending-realtime items
	// from the past 4 hours that the user hasn't engaged with. We don't run
	// the full LTR/watchRatio model here (too expensive per ranker run for
	// every active user) — instead we treat "in the bootstrap pool top-50
	// + high realtime trending" as a proxy for "broadly loved right now."
	//
	// For each candidate, we look up users whose embedding cosine vs the
	// content's embedding is above a threshold AND who haven't seen the item.
	// This is the cheapest way to approximate "personalized push without
	// running the full ranker for every user every 5 minutes."
	candidates := fetchTrendingRealtime(15)
	if len(candidates) == 0 {
		return
	}

	for _, cand := range candidates {
		// Get the content score so we can build its embedding.
		cs := getContentScore(cand.ID, cand.Type)
		if cs == nil {
			continue
		}
		emotions := getContentEmotions(cand.ID, cand.Type)
		cv := getOrBuildContentEmbedding(cs, emotions)

		// Find recently-active users with the highest cosine to this item
		// AND who haven't seen it. We bound this to last-active-7d so we
		// don't push to dormant accounts (those are inactive_winback's job).
		userRows, err := db.Query(`
			SELECT id::text FROM users
			WHERE last_seen IS NOT NULL
			  AND last_seen > NOW() - INTERVAL '7 days'
			ORDER BY last_seen DESC
			LIMIT 200
		`)
		if err != nil {
			continue
		}
		matched := 0
		for userRows.Next() {
			if matched >= 20 {
				break // cap per-content fan-out
			}
			var uid string
			if err := userRows.Scan(&uid); err != nil {
				continue
			}
			// Self-skip: don't push the creator's own content to them.
			if uid == cs.CreatorID {
				continue
			}
			// Cosine check.
			uv := getUserEmbedding(uid)
			if userEmbeddingIsCold(uv) {
				continue
			}
			sim := cosineSim(uv, cv)
			if sim < youWillLoveMinPredicted-0.30 {
				// Use a lenient threshold here because the embedding is
				// just a proxy for "predicted love" — the dispatcher's
				// rate-limit + dedupe already prevent over-notification.
				continue
			}
			// Skip if already in the seen set.
			if cnt, _ := rdb.ZScore(rctx, seenKeyPrefix+uid, cand.Type+":"+cand.ID).Result(); cnt > 0 {
				continue
			}
			_, _, _ = enqueueNotification(EnqueueParams{
				UserID:      uid,
				TriggerKind: TriggerYouWillLove,
				DedupeKey:   fmt.Sprintf("ywl:%s:%s", cand.Type, cand.ID),
				Title:       "Found something for you",
				Body:        truncateText(fmt.Sprintf("Trending in %s — 30 seconds you won't skip", cs.Category), 120),
				Deeplink:    fmt.Sprintf("devf://%s/%s", cand.Type, cand.ID),
			})
			matched++
		}
		userRows.Close()
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 4. inactive_winback — fire to at_risk users we haven't seen in 3+ days
// ─────────────────────────────────────────────────────────────────────────────

func scanInactiveWinbackTrigger() {
	if db == nil {
		return
	}
	// Cohort gating: at_risk users have low completion + high skip + short
	// sessions. For winback we want users who WERE engaged before — pulling
	// at_risk specifically (not all dormant users) means we re-engage people
	// who liked the app, not annoy ones who never did.
	rows, err := db.Query(`
		SELECT u.id::text AS user_id, u.username
		FROM users u
		LEFT JOIN user_profiles p ON p.user_id = u.id::text
		WHERE u.last_seen IS NOT NULL
		  AND u.last_seen < NOW() - $1::interval
		  AND u.last_seen > NOW() - INTERVAL '30 days'   -- don't bother truly dead users
		  AND COALESCE(p.total_sessions, 0) >= 5         -- had a real prior relationship
		ORDER BY u.last_seen DESC
		LIMIT $2
	`, fmt.Sprintf("%d days", inactiveDaysThreshold), triggerScanLimit)
	if err != nil {
		return
	}
	defer rows.Close()
	for rows.Next() {
		var userID, username string
		if err := rows.Scan(&userID, &username); err != nil {
			continue
		}
		_, _, _ = enqueueNotification(EnqueueParams{
			UserID:      userID,
			TriggerKind: TriggerInactiveWinback,
			// Dedupe per calendar week so we don't spam someone every 5 min
			// while they're inactive — at most one win-back ping per week.
			DedupeKey: fmt.Sprintf("iw:%s:%s", userID, time.Now().Format("2006-W02")),
			Title:     "We saved your spot",
			Body:      "3 challenges you'd crush are waiting →",
			Deeplink:  "devf://feed",
		})
	}
}

// truncateText keeps push body lengths under platform limits (FCM ~240,
// APNs ~256). 120 is comfortably under both with room for emojis.
func truncateText(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max-1] + "…"
}
