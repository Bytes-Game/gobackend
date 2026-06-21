package main

import (
	"encoding/json"
	"net/http"
	"strconv"
)

// ─────────────────────────────────────────────────────────────────────────────
// PUSH NOTIFICATION HTTP HANDLERS
//
//   POST /api/v1/notifications/register
//        body: { userId, token, platform: "fcm"|"apns" }
//        → 200 on success
//
//   GET  /api/v1/notifications/prefs?userId=X
//        → NotificationPrefs JSON
//
//   POST /api/v1/notifications/prefs
//        body: NotificationPrefs JSON
//        → 200 on success
//
//   POST /api/v1/notifications/clicked
//        body: { id: <outbox row id> }
//        → 200 — drives the notification → open conversion funnel
//
//   POST /api/v1/notifications/unregister
//        body: { token }
//        → 200 — used on logout / push permission revocation
// ─────────────────────────────────────────────────────────────────────────────

// HandleRegisterPushToken upserts a (userID, token, platform) tuple.
func HandleRegisterPushToken(w http.ResponseWriter, r *http.Request) {
	if r.Method == "OPTIONS" {
		w.WriteHeader(http.StatusOK)
		return
	}
	var body struct {
		UserID   string `json:"userId"`
		Token    string `json:"token"`
		Platform string `json:"platform"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid body", http.StatusBadRequest)
		return
	}
	// Register the token against the authenticated user.
	body.UserID = authUserID(r)
	if err := registerDeviceToken(body.UserID, body.Token, body.Platform); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}

// HandleUnregisterPushToken deactivates a token (logout / permission revoke).
func HandleUnregisterPushToken(w http.ResponseWriter, r *http.Request) {
	if r.Method == "OPTIONS" {
		w.WriteHeader(http.StatusOK)
		return
	}
	var body struct {
		Token string `json:"token"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid body", http.StatusBadRequest)
		return
	}
	deactivateDeviceToken(body.Token)
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}

// HandleGetNotificationPrefs returns the user's prefs (or defaults).
func HandleGetNotificationPrefs(w http.ResponseWriter, r *http.Request) {
	if r.Method == "OPTIONS" {
		w.WriteHeader(http.StatusOK)
		return
	}
	userID := authUserID(r)
	if userID == "" {
		http.Error(w, "userId required", http.StatusBadRequest)
		return
	}
	p := loadNotificationPrefs(userID)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(p)
}

// HandleSetNotificationPrefs upserts the user's prefs.
func HandleSetNotificationPrefs(w http.ResponseWriter, r *http.Request) {
	if r.Method == "OPTIONS" {
		w.WriteHeader(http.StatusOK)
		return
	}
	var p NotificationPrefs
	if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
		http.Error(w, "invalid body", http.StatusBadRequest)
		return
	}
	// Prefs always belong to the authenticated user.
	p.UserID = authUserID(r)
	if p.UserID == "" {
		http.Error(w, "userId required", http.StatusBadRequest)
		return
	}
	// Validate quiet-hours bounds.
	if p.QuietHoursStart < 0 || p.QuietHoursStart > 23 ||
		p.QuietHoursEnd < 0 || p.QuietHoursEnd > 23 {
		http.Error(w, "quietHours out of range", http.StatusBadRequest)
		return
	}
	if p.MaxPerDay < 0 || p.MaxPerDay > 30 {
		http.Error(w, "maxPerDay out of range", http.StatusBadRequest)
		return
	}
	if err := saveNotificationPrefs(p); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}

// HandleNotificationClicked records that the user tapped through from a
// push. Drives the notification → app-open conversion funnel.
func HandleNotificationClicked(w http.ResponseWriter, r *http.Request) {
	if r.Method == "OPTIONS" {
		w.WriteHeader(http.StatusOK)
		return
	}
	var body struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid body", http.StatusBadRequest)
		return
	}
	id, err := strconv.ParseInt(body.ID, 10, 64)
	if err != nil {
		http.Error(w, "id must be numeric", http.StatusBadRequest)
		return
	}
	markClicked(id)
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}
