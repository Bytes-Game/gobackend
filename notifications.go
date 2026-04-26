package main

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

// ─────────────────────────────────────────────────────────────────────────────
// PUSH NOTIFICATIONS — core types, prefs, outbox.
//
// We treat push as a queue, not a fire-and-forget. Every notification is
// staged in the `notification_outbox` table with a dedupe_key, then a
// dispatcher picks pending rows, checks rate-limits + opt-out + quiet
// hours, sends via the configured Sender, and marks delivered/failed.
//
// This shape gives us:
//   - dedupe (no double-buzzing the same user about the same event)
//   - retry without code changes (failed → pending after backoff)
//   - audit trail (every push is a row, sent_at/clicked_at columns drive
//     the notification → app-open funnel for product analytics)
//   - hot-swap senders (FCM/APNs/log) without touching trigger code
// ─────────────────────────────────────────────────────────────────────────────

// TriggerKind enumerates the four notification reasons we currently send.
// New triggers add to this list and to NotificationPrefs as a per-trigger
// boolean column.
type TriggerKind string

const (
	TriggerFriendResponse  TriggerKind = "friend_response"
	TriggerEndingSoon      TriggerKind = "ending_soon"
	TriggerYouWillLove     TriggerKind = "you_will_love"
	TriggerInactiveWinback TriggerKind = "inactive_winback"
)

// NotificationPrefs is the user's per-trigger opt-out + rate-limit settings.
type NotificationPrefs struct {
	UserID           string `json:"userId"`
	FriendResponse   bool   `json:"friendResponse"`
	EndingSoon       bool   `json:"endingSoon"`
	YouWillLove      bool   `json:"youWillLove"`
	InactiveWinback  bool   `json:"inactiveWinback"`
	QuietHoursStart  int    `json:"quietHoursStart"`  // 0-23, local hour
	QuietHoursEnd    int    `json:"quietHoursEnd"`
	MaxPerDay        int    `json:"maxPerDay"`
}

// defaultNotificationPrefs returns the schema defaults. Used when a user
// hasn't customized — we still want push enabled out-of-the-box because
// users opted in via OS-level permission grant.
func defaultNotificationPrefs(userID string) NotificationPrefs {
	return NotificationPrefs{
		UserID:           userID,
		FriendResponse:   true,
		EndingSoon:       true,
		YouWillLove:      true,
		InactiveWinback:  true,
		QuietHoursStart:  22,
		QuietHoursEnd:    8,
		MaxPerDay:        4,
	}
}

// allowedByPrefs returns true if this trigger kind is enabled for the user.
func (p NotificationPrefs) allowedByPrefs(kind TriggerKind) bool {
	switch kind {
	case TriggerFriendResponse:
		return p.FriendResponse
	case TriggerEndingSoon:
		return p.EndingSoon
	case TriggerYouWillLove:
		return p.YouWillLove
	case TriggerInactiveWinback:
		return p.InactiveWinback
	}
	return false
}

// inQuietHours returns true if `now` falls inside the user's local quiet
// window. Wrap-around (22 → 8) is handled by checking either side.
//
// We treat the timestamp as already-local — production should resolve the
// user's timezone via their device profile before calling this. For the
// MVP we assume server time is the user's time; cheap to fix later.
func (p NotificationPrefs) inQuietHours(now time.Time) bool {
	if p.QuietHoursStart == p.QuietHoursEnd {
		return false // disabled
	}
	h := now.Hour()
	if p.QuietHoursStart < p.QuietHoursEnd {
		return h >= p.QuietHoursStart && h < p.QuietHoursEnd
	}
	// Wrap-around: e.g. start=22, end=8 → quiet if h >= 22 OR h < 8.
	return h >= p.QuietHoursStart || h < p.QuietHoursEnd
}

// loadNotificationPrefs reads the user's prefs row, falling back to the
// defaults when no row exists yet.
func loadNotificationPrefs(userID string) NotificationPrefs {
	if db == nil || userID == "" {
		return defaultNotificationPrefs(userID)
	}
	var p NotificationPrefs
	p.UserID = userID
	err := db.QueryRow(`
		SELECT friend_response, ending_soon, you_will_love, inactive_winback,
		       quiet_hours_start, quiet_hours_end, max_per_day
		FROM notification_prefs WHERE user_id = $1
	`, userID).Scan(&p.FriendResponse, &p.EndingSoon, &p.YouWillLove, &p.InactiveWinback,
		&p.QuietHoursStart, &p.QuietHoursEnd, &p.MaxPerDay)
	if err != nil {
		return defaultNotificationPrefs(userID)
	}
	return p
}

// saveNotificationPrefs upserts the user's prefs row.
func saveNotificationPrefs(p NotificationPrefs) error {
	if db == nil || p.UserID == "" {
		return errors.New("db or userID missing")
	}
	_, err := db.Exec(`
		INSERT INTO notification_prefs
			(user_id, friend_response, ending_soon, you_will_love, inactive_winback,
			 quiet_hours_start, quiet_hours_end, max_per_day, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())
		ON CONFLICT (user_id) DO UPDATE SET
			friend_response  = EXCLUDED.friend_response,
			ending_soon      = EXCLUDED.ending_soon,
			you_will_love    = EXCLUDED.you_will_love,
			inactive_winback = EXCLUDED.inactive_winback,
			quiet_hours_start = EXCLUDED.quiet_hours_start,
			quiet_hours_end  = EXCLUDED.quiet_hours_end,
			max_per_day      = EXCLUDED.max_per_day,
			updated_at       = NOW()
	`, p.UserID, p.FriendResponse, p.EndingSoon, p.YouWillLove, p.InactiveWinback,
		p.QuietHoursStart, p.QuietHoursEnd, p.MaxPerDay)
	return err
}

// ─────────────────────────────────────────────────────────────────────────────
// DEVICE TOKENS
// ─────────────────────────────────────────────────────────────────────────────

// registerDeviceToken upserts a token. Same token re-registered just bumps
// last_seen_at (cheap idempotency for app-launch flows).
func registerDeviceToken(userID, token, platform string) error {
	if db == nil {
		return errors.New("db missing")
	}
	if userID == "" || token == "" {
		return errors.New("userID and token required")
	}
	platform = strings.ToLower(platform)
	if platform != "fcm" && platform != "apns" {
		return fmt.Errorf("unsupported platform %q", platform)
	}
	_, err := db.Exec(`
		INSERT INTO device_tokens (token, user_id, platform, registered_at, last_seen_at, active)
		VALUES ($1, $2, $3, NOW(), NOW(), TRUE)
		ON CONFLICT (token) DO UPDATE SET
			user_id      = EXCLUDED.user_id,
			platform     = EXCLUDED.platform,
			last_seen_at = NOW(),
			active       = TRUE
	`, token, userID, platform)
	return err
}

// deactivateDeviceToken marks a token inactive (e.g. after FCM/APNs returns
// "unregistered" — the token is dead). Doesn't delete; we keep the row for
// debugging / re-registration audit.
func deactivateDeviceToken(token string) {
	if db == nil || token == "" {
		return
	}
	_, _ = db.Exec(`UPDATE device_tokens SET active = FALSE WHERE token = $1`, token)
}

// activeTokensForUser returns the list of (token, platform) tuples the user
// currently has registered. Used by the dispatcher to fan-out a single
// outbox row to multiple devices.
type DeviceTokenRow struct {
	Token, Platform string
}

func activeTokensForUser(userID string) []DeviceTokenRow {
	if db == nil || userID == "" {
		return nil
	}
	rows, err := db.Query(`
		SELECT token, platform FROM device_tokens
		WHERE user_id = $1 AND active = TRUE
		ORDER BY last_seen_at DESC
		LIMIT 8
	`, userID)
	if err != nil {
		return nil
	}
	defer rows.Close()
	out := make([]DeviceTokenRow, 0, 4)
	for rows.Next() {
		var r DeviceTokenRow
		if err := rows.Scan(&r.Token, &r.Platform); err == nil {
			out = append(out, r)
		}
	}
	return out
}

// ─────────────────────────────────────────────────────────────────────────────
// OUTBOX
// ─────────────────────────────────────────────────────────────────────────────

// OutboxRow is one queued (or sent) push notification.
type OutboxRow struct {
	ID           int64
	UserID       string
	TriggerKind  TriggerKind
	DedupeKey    string
	Title        string
	Body         string
	Deeplink     string
	ScheduledAt  time.Time
	QueuedAt     time.Time
	SentAt       sql.NullTime
	ClickedAt    sql.NullTime
	FailedAt     sql.NullTime
	FailReason   sql.NullString
	Status       string
}

// EnqueueParams is the inputs an enqueueNotification call requires.
// Trigger workers construct one of these per candidate event.
type EnqueueParams struct {
	UserID      string
	TriggerKind TriggerKind
	DedupeKey   string
	Title       string
	Body        string
	Deeplink    string
	ScheduledAt time.Time
}

// enqueueNotification stages a push for delivery, applying opt-out + rate
// limit + quiet-hours checks up front. Idempotent on (user_id, dedupe_key)
// — the same trigger fired twice within its dedupe window will be dropped
// silently.
//
// Returns (id, queued, err). queued=false means it was deliberately dropped
// (opt-out, rate limited, quiet hours, dedupe hit) — not an error.
func enqueueNotification(p EnqueueParams) (int64, bool, error) {
	if db == nil {
		return 0, false, errors.New("db missing")
	}
	if p.UserID == "" || p.DedupeKey == "" {
		return 0, false, errors.New("userID and dedupeKey required")
	}
	if p.ScheduledAt.IsZero() {
		p.ScheduledAt = time.Now()
	}

	prefs := loadNotificationPrefs(p.UserID)
	if !prefs.allowedByPrefs(p.TriggerKind) {
		if metricNotifEnqueue != nil {
			metricNotifEnqueue.WithLabelValues(string(p.TriggerKind), "opted_out").Inc()
		}
		return 0, false, nil
	}
	// Rate limit: count notifications already sent today.
	if prefs.MaxPerDay > 0 {
		var sentToday int
		err := db.QueryRow(`
			SELECT COUNT(*) FROM notification_outbox
			WHERE user_id = $1
			  AND status IN ('sent', 'clicked')
			  AND sent_at > NOW() - INTERVAL '24 hours'
		`, p.UserID).Scan(&sentToday)
		if err == nil && sentToday >= prefs.MaxPerDay {
			if metricNotifEnqueue != nil {
				metricNotifEnqueue.WithLabelValues(string(p.TriggerKind), "rate_limited").Inc()
			}
			return 0, false, nil
		}
	}
	// Quiet-hours: defer to next-allowed time instead of dropping. Better UX.
	if prefs.inQuietHours(p.ScheduledAt) {
		// Reschedule to the first hour after quiet ends.
		end := prefs.QuietHoursEnd
		next := time.Date(p.ScheduledAt.Year(), p.ScheduledAt.Month(), p.ScheduledAt.Day(),
			end, 0, 0, 0, p.ScheduledAt.Location())
		// If the end is before "now" today (wrap-around case), push to tomorrow.
		if !next.After(p.ScheduledAt) {
			next = next.Add(24 * time.Hour)
		}
		p.ScheduledAt = next
	}

	// Insert with ON CONFLICT DO NOTHING for dedupe.
	var id int64
	err := db.QueryRow(`
		INSERT INTO notification_outbox
			(user_id, trigger_kind, dedupe_key, title, body, deeplink, scheduled_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (user_id, dedupe_key) DO NOTHING
		RETURNING id
	`, p.UserID, string(p.TriggerKind), p.DedupeKey, p.Title, p.Body, p.Deeplink, p.ScheduledAt).Scan(&id)
	if err == sql.ErrNoRows {
		// Dedupe hit.
		if metricNotifEnqueue != nil {
			metricNotifEnqueue.WithLabelValues(string(p.TriggerKind), "deduped").Inc()
		}
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	if metricNotifEnqueue != nil {
		metricNotifEnqueue.WithLabelValues(string(p.TriggerKind), "queued").Inc()
	}
	return id, true, nil
}

// fetchPendingNotifications returns up to `limit` rows scheduled at or
// before now, ordered oldest-first. Caller is the dispatcher.
func fetchPendingNotifications(limit int) []OutboxRow {
	if db == nil {
		return nil
	}
	rows, err := db.Query(`
		SELECT id, user_id, trigger_kind, dedupe_key, title, body,
		       COALESCE(deeplink, ''), scheduled_at, queued_at, status
		FROM notification_outbox
		WHERE status = 'pending' AND scheduled_at <= NOW()
		ORDER BY scheduled_at ASC
		LIMIT $1
	`, limit)
	if err != nil {
		return nil
	}
	defer rows.Close()
	out := make([]OutboxRow, 0, limit)
	for rows.Next() {
		var r OutboxRow
		var trigger string
		if err := rows.Scan(&r.ID, &r.UserID, &trigger, &r.DedupeKey, &r.Title, &r.Body,
			&r.Deeplink, &r.ScheduledAt, &r.QueuedAt, &r.Status); err == nil {
			r.TriggerKind = TriggerKind(trigger)
			out = append(out, r)
		}
	}
	return out
}

// markSent flips the row to status='sent' and stamps sent_at.
func markSent(id int64) {
	if db == nil {
		return
	}
	_, _ = db.Exec(`UPDATE notification_outbox SET status='sent', sent_at = NOW() WHERE id = $1`, id)
}

// markFailed records a delivery failure with reason.
func markFailed(id int64, reason string) {
	if db == nil {
		return
	}
	_, _ = db.Exec(`UPDATE notification_outbox
		SET status='failed', failed_at = NOW(), fail_reason = $2 WHERE id = $1`, id, reason)
}

// markClicked is called from the click-tracking endpoint. The transition
// 'sent' → 'clicked' is what powers the notification → open conversion
// funnel in /admin/funnels.
func markClicked(id int64) {
	if db == nil {
		return
	}
	_, _ = db.Exec(`UPDATE notification_outbox
		SET status='clicked', clicked_at = NOW() WHERE id = $1 AND status = 'sent'`, id)
}
