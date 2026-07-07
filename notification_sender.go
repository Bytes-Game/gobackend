package main

import (
	"errors"
	"log"
	"strings"
	"sync"
	"time"
)

// ─────────────────────────────────────────────────────────────────────────────
// PUSH SENDER — pluggable interface so production can swap in real FCM/APNs
// without touching trigger/outbox code.
//
// Three implementations ship today:
//   - logSender  : prints to log; default in dev/test
//   - fcmSender  : stub HTTP client to FCM v1 API (production fills in key)
//   - apnsSender : stub HTTP client to APNs (production fills in cert)
//
// The active sender is chosen at boot time via env vars:
//   NOTIFICATION_SENDER=log      → log
//   NOTIFICATION_SENDER=fcm      → FCM (requires FCM_PROJECT, FCM_KEY)
//   NOTIFICATION_SENDER=apns     → APNs (requires APNS_TEAM, APNS_KEY_ID, APNS_KEY)
//
// Anything unset/unknown defaults to logSender so a fresh deploy is safe.
// ─────────────────────────────────────────────────────────────────────────────

// SendResult tells the dispatcher whether the send succeeded for each token.
// Per-token outcome lets us deactivate dead tokens without losing the
// notification entirely (other tokens for the same user might still work).
type SendResult struct {
	Token   string
	OK      bool
	Reason  string // populated on !OK
	Dead    bool   // permanent failure (FCM/APNs returned "unregistered")
}

// PushSender is the interface every backend implements. Send returns one
// SendResult per input token.
type PushSender interface {
	Send(notif OutboxRow, tokens []DeviceTokenRow) []SendResult
	Name() string
}

// ─────────────────────────────────────────────────────────────────────────────
// logSender — default for dev. Writes to the standard log so we can see
// what *would* have been pushed without needing real FCM/APNs creds.
// ─────────────────────────────────────────────────────────────────────────────

type logSender struct{}

func (logSender) Name() string { return "log" }

func (logSender) Send(notif OutboxRow, tokens []DeviceTokenRow) []SendResult {
	out := make([]SendResult, 0, len(tokens))
	for _, t := range tokens {
		log.Printf("[push:log] uid=%s trigger=%s title=%q body=%q deeplink=%q token=%s/%s",
			notif.UserID, notif.TriggerKind, notif.Title, notif.Body, notif.Deeplink,
			t.Platform, redactToken(t.Token))
		out = append(out, SendResult{Token: t.Token, OK: true})
	}
	return out
}

// redactToken truncates a token for log safety — never write a full token
// to logs (it's effectively a session credential).
func redactToken(t string) string {
	if len(t) <= 8 {
		return "***"
	}
	return t[:4] + "…" + t[len(t)-4:]
}

// ─────────────────────────────────────────────────────────────────────────────
// fcmSender — real FCM HTTP v1 delivery (see fcm_v1.go for the OAuth +
// send plumbing). Without a parseable FCM_SERVICE_ACCOUNT_JSON it
// reports "fcm_not_configured" per token — the old stub behavior — so
// an unconfigured deploy stays safe and observable.
// ─────────────────────────────────────────────────────────────────────────────

type fcmSender struct {
	projectID string
	tokens    *fcmTokenSource // nil = not configured
}

// newFCMSender builds the sender from env. FCM_PROJECT overrides the
// project_id embedded in the service-account key.
func newFCMSender() *fcmSender {
	s := &fcmSender{projectID: getEnv("FCM_PROJECT", "")}
	if sa := parseFCMServiceAccount(getEnv("FCM_SERVICE_ACCOUNT_JSON", "")); sa != nil {
		s.tokens = &fcmTokenSource{sa: sa}
		if s.projectID == "" {
			s.projectID = sa.ProjectID
		}
	}
	return s
}

func (s *fcmSender) Name() string { return "fcm" }

func (s *fcmSender) configured() bool { return s.tokens != nil && s.projectID != "" }

func (s *fcmSender) Send(notif OutboxRow, tokens []DeviceTokenRow) []SendResult {
	out := make([]SendResult, 0, len(tokens))
	for _, t := range tokens {
		if t.Platform != "fcm" {
			continue
		}
		if !s.configured() {
			out = append(out, SendResult{
				Token:  t.Token,
				OK:     false,
				Reason: "fcm_not_configured",
			})
			continue
		}
		ok, dead, reason := sendFCMMessage(s.tokens, s.projectID, notif, t.Token)
		out = append(out, SendResult{Token: t.Token, OK: ok, Dead: dead, Reason: reason})
	}
	return out
}

// ─────────────────────────────────────────────────────────────────────────────
// apnsSender — same shape as fcmSender, separate stub for iOS.
// ─────────────────────────────────────────────────────────────────────────────

type apnsSender struct {
	teamID, keyID, key string
}

func (s *apnsSender) Name() string { return "apns" }

func (s *apnsSender) Send(notif OutboxRow, tokens []DeviceTokenRow) []SendResult {
	out := make([]SendResult, 0, len(tokens))
	for _, t := range tokens {
		if t.Platform != "apns" {
			continue
		}
		out = append(out, SendResult{
			Token:  t.Token,
			OK:     false,
			Reason: "apns_not_configured",
		})
	}
	return out
}

// ─────────────────────────────────────────────────────────────────────────────
// Multi-sender — fan out to whichever sender matches the token's platform.
// This is what the dispatcher actually invokes; FCM and APNs are wrapped
// inside it so a single user with both Android and iOS gets both pings.
// ─────────────────────────────────────────────────────────────────────────────

type multiSender struct {
	byPlatform map[string]PushSender
	fallback   PushSender
}

func (m *multiSender) Name() string { return "multi" }

func (m *multiSender) Send(notif OutboxRow, tokens []DeviceTokenRow) []SendResult {
	if len(tokens) == 0 {
		return nil
	}
	// Bucket tokens by platform so we hit each backend once.
	buckets := make(map[string][]DeviceTokenRow, 2)
	for _, t := range tokens {
		buckets[t.Platform] = append(buckets[t.Platform], t)
	}
	out := make([]SendResult, 0, len(tokens))
	for platform, ts := range buckets {
		s, ok := m.byPlatform[platform]
		if !ok {
			s = m.fallback
		}
		out = append(out, s.Send(notif, ts)...)
	}
	return out
}

// ─────────────────────────────────────────────────────────────────────────────
// Selection
// ─────────────────────────────────────────────────────────────────────────────

var (
	currentSender   PushSender = logSender{}
	currentSenderMu sync.RWMutex
)

func setCurrentSender(s PushSender) {
	currentSenderMu.Lock()
	currentSender = s
	currentSenderMu.Unlock()
}

func getCurrentSender() PushSender {
	currentSenderMu.RLock()
	defer currentSenderMu.RUnlock()
	return currentSender
}

// initPushSender is called from main(). Reads env to pick sender; safe to
// call without env vars set (falls back to log).
func initPushSender() {
	want := strings.ToLower(strings.TrimSpace(getEnv("NOTIFICATION_SENDER", "log")))
	switch want {
	case "fcm":
		setCurrentSender(newFCMSender())
	case "apns":
		setCurrentSender(&apnsSender{
			teamID: getEnv("APNS_TEAM", ""),
			keyID:  getEnv("APNS_KEY_ID", ""),
			key:    getEnv("APNS_KEY", ""),
		})
	case "multi":
		setCurrentSender(&multiSender{
			byPlatform: map[string]PushSender{
				"fcm": newFCMSender(),
				"apns": &apnsSender{
					teamID: getEnv("APNS_TEAM", ""),
					keyID:  getEnv("APNS_KEY_ID", ""),
					key:    getEnv("APNS_KEY", ""),
				},
			},
			fallback: logSender{},
		})
	default:
		setCurrentSender(logSender{})
	}
	sender := getCurrentSender()
	if f, ok := sender.(*fcmSender); ok && !f.configured() {
		log.Printf("notifications: FCM selected but FCM_SERVICE_ACCOUNT_JSON missing/unparseable — sends will fail with fcm_not_configured")
	}
	log.Printf("notifications: sender=%s", sender.Name())
}

// ─────────────────────────────────────────────────────────────────────────────
// DISPATCHER
// ─────────────────────────────────────────────────────────────────────────────

const (
	dispatchInterval = 30 * time.Second
	dispatchBatch    = 50
)

// startNotificationDispatcher polls the outbox every dispatchInterval,
// sends pending rows via the current sender, and updates outbox status.
// Resilient: per-row failures don't abort the loop.
func startNotificationDispatcher() {
	go func() {
		t := time.NewTicker(dispatchInterval)
		defer t.Stop()
		for range t.C {
			dispatchPendingNotifications()
		}
	}()
}

// dispatchPendingNotifications drains a batch of pending rows. Exposed
// (lowercase but package-scope) so tests can drive a single tick without
// waiting for the ticker.
func dispatchPendingNotifications() {
	rows := fetchPendingNotifications(dispatchBatch)
	if len(rows) == 0 {
		return
	}
	sender := getCurrentSender()
	for _, r := range rows {
		tokens := activeTokensForUser(r.UserID)
		if len(tokens) == 0 {
			markFailed(r.ID, "no_active_tokens")
			if metricNotifDispatch != nil {
				metricNotifDispatch.WithLabelValues(string(r.TriggerKind), "no_tokens").Inc()
			}
			continue
		}
		results := sender.Send(r, tokens)
		anyOK := false
		for _, res := range results {
			if res.OK {
				anyOK = true
				continue
			}
			if res.Dead {
				deactivateDeviceToken(res.Token)
			}
		}
		if anyOK {
			markSent(r.ID)
			if metricNotifDispatch != nil {
				metricNotifDispatch.WithLabelValues(string(r.TriggerKind), "sent").Inc()
			}
		} else {
			reason := "all_tokens_failed"
			if len(results) > 0 && results[0].Reason != "" {
				reason = results[0].Reason
			}
			markFailed(r.ID, reason)
			if metricNotifDispatch != nil {
				metricNotifDispatch.WithLabelValues(string(r.TriggerKind), "failed").Inc()
			}
		}
	}
}

// getEnv is a tiny helper so we don't depend on a third-party config lib.
func getEnv(k, def string) string {
	v := strings.TrimSpace(envLookup(k))
	if v == "" {
		return def
	}
	return v
}

// envLookup is a tiny wrapper that lets us substitute env access in tests
// without monkey-patching os.Getenv.
var envLookup = func(k string) string {
	return getEnvFromOS(k)
}

// Sentinel error so callers can distinguish "no config" from real errors.
var ErrNoConfig = errors.New("notification sender not configured")
