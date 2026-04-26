package main

import (
	"testing"
	"time"
)

// ─────────────────────────────────────────────────────────────────────────────
// Notification core: prefs, opt-out, quiet hours, sender selection.
//
// Most tests here run without a real DB (no notification_outbox row inserts).
// We focus on the pure-logic surface: prefs decisions, quiet-hour math,
// sender pluggability, log sender behavior. Trigger workers + dispatcher
// integration tests would need a real Postgres — out of scope for the
// in-process suite.
// ─────────────────────────────────────────────────────────────────────────────

func TestNotifPrefs_DefaultsAllowEverything(t *testing.T) {
	p := defaultNotificationPrefs("u1")
	if !p.allowedByPrefs(TriggerFriendResponse) ||
		!p.allowedByPrefs(TriggerEndingSoon) ||
		!p.allowedByPrefs(TriggerYouWillLove) ||
		!p.allowedByPrefs(TriggerInactiveWinback) {
		t.Errorf("defaults should allow every trigger, got %+v", p)
	}
	if p.MaxPerDay != 4 {
		t.Errorf("default cap should be 4, got %d", p.MaxPerDay)
	}
}

func TestNotifPrefs_OptedOutBlocks(t *testing.T) {
	p := defaultNotificationPrefs("u1")
	p.YouWillLove = false
	if p.allowedByPrefs(TriggerYouWillLove) {
		t.Error("disabled trigger must not be allowed")
	}
	// Other triggers still allowed.
	if !p.allowedByPrefs(TriggerFriendResponse) {
		t.Error("other triggers should remain allowed")
	}
}

func TestNotifPrefs_QuietHoursWrapAround(t *testing.T) {
	p := defaultNotificationPrefs("u1") // 22 → 8 wrap
	cases := []struct {
		hour int
		want bool
	}{
		{0, true},   // midnight — quiet
		{3, true},   // 3am — quiet
		{7, true},   // 7am — still quiet
		{8, false},  // 8am — end of quiet (exclusive)
		{12, false}, // noon — not quiet
		{21, false}, // 9pm — not quiet
		{22, true},  // 10pm — start of quiet (inclusive)
		{23, true},  // 11pm — quiet
	}
	for _, c := range cases {
		now := time.Date(2026, 4, 25, c.hour, 30, 0, 0, time.UTC)
		got := p.inQuietHours(now)
		if got != c.want {
			t.Errorf("hour=%d: want quiet=%v, got %v", c.hour, c.want, got)
		}
	}
}

func TestNotifPrefs_QuietHoursDisabled(t *testing.T) {
	p := defaultNotificationPrefs("u1")
	p.QuietHoursStart = 0
	p.QuietHoursEnd = 0 // start == end → disabled
	for h := 0; h < 24; h++ {
		now := time.Date(2026, 4, 25, h, 0, 0, 0, time.UTC)
		if p.inQuietHours(now) {
			t.Errorf("hour=%d: quiet should be disabled when start==end", h)
		}
	}
}

func TestSender_LogSenderAlwaysOK(t *testing.T) {
	notif := OutboxRow{
		ID: 1, UserID: "u1", TriggerKind: TriggerEndingSoon,
		Title: "test", Body: "test body", Deeplink: "devf://x",
	}
	tokens := []DeviceTokenRow{
		{Token: "tk_abc1234567890", Platform: "fcm"},
		{Token: "tk_def1234567890", Platform: "apns"},
	}
	results := logSender{}.Send(notif, tokens)
	if len(results) != len(tokens) {
		t.Fatalf("expected %d results, got %d", len(tokens), len(results))
	}
	for _, r := range results {
		if !r.OK {
			t.Errorf("logSender should always OK, got %+v", r)
		}
	}
}

func TestSender_FCMStubReportsNotConfigured(t *testing.T) {
	s := &fcmSender{}
	tokens := []DeviceTokenRow{{Token: "x", Platform: "fcm"}}
	results := s.Send(OutboxRow{}, tokens)
	if len(results) != 1 || results[0].OK {
		t.Errorf("unconfigured FCM should fail, got %+v", results)
	}
	if results[0].Reason != "fcm_not_configured" {
		t.Errorf("expected fcm_not_configured reason, got %q", results[0].Reason)
	}
}

func TestSender_MultiSenderRoutesByPlatform(t *testing.T) {
	// Build a multi-sender with logSender for both platforms — easier to
	// assert on the routing rather than depending on stubbed FCM/APNs.
	m := &multiSender{
		byPlatform: map[string]PushSender{
			"fcm":  logSender{},
			"apns": logSender{},
		},
		fallback: logSender{},
	}
	tokens := []DeviceTokenRow{
		{Token: "fcm-aaaaaaaaaa", Platform: "fcm"},
		{Token: "apns-bbbbbbbbb", Platform: "apns"},
		{Token: "fcm-cccccccccc", Platform: "fcm"},
	}
	results := m.Send(OutboxRow{ID: 1, UserID: "u1", Title: "t", Body: "b"}, tokens)
	if len(results) != 3 {
		t.Errorf("expected 3 results across both platforms, got %d", len(results))
	}
}

func TestSender_RedactToken(t *testing.T) {
	if r := redactToken("short"); r != "***" {
		t.Errorf("short tokens should be ***, got %q", r)
	}
	r := redactToken("abcdefghijklmnop")
	if r != "abcd…mnop" {
		t.Errorf("expected abcd…mnop, got %q", r)
	}
}

func TestSender_SetAndGetCurrentSender(t *testing.T) {
	original := getCurrentSender()
	defer setCurrentSender(original)

	setCurrentSender(&fcmSender{})
	if getCurrentSender().Name() != "fcm" {
		t.Errorf("expected fcm, got %s", getCurrentSender().Name())
	}
	setCurrentSender(logSender{})
	if getCurrentSender().Name() != "log" {
		t.Errorf("expected log, got %s", getCurrentSender().Name())
	}
}

func TestNotifTriggers_TruncateText(t *testing.T) {
	if truncateText("short", 10) != "short" {
		t.Errorf("under-limit should pass through")
	}
	got := truncateText("this is a very long string that exceeds", 12)
	if got != "this is a v…" {
		t.Errorf("expected 'this is a v…', got %q", got)
	}
}
