package main

// fcm_v1.go — real FCM HTTP v1 delivery.
//
// Google's official Go SDK (firebase.google.com/go) pulls in ~40
// transitive modules; all we need is (1) a service-account OAuth2
// token and (2) one JSON POST per message. Both are small, stable
// protocols, so we hand-roll them with the stdlib + the golang-jwt
// dependency the auth layer already uses — same rationale as the
// hand-rolled SigV4 in media_storage.go.
//
// Configuration (all read at initPushSender time):
//
//	NOTIFICATION_SENDER=fcm (or multi)
//	FCM_SERVICE_ACCOUNT_JSON = the service-account key JSON, either
//	  raw or base64-encoded (Render env vars handle multi-line values,
//	  but base64 is friendlier to copy-paste).
//	FCM_PROJECT (optional) = overrides the project_id from the key.
//
// Without a parseable service account the sender reports
// "fcm_not_configured" per token — exactly the old stub behavior — so
// an unconfigured deploy stays safe and observable.

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

const (
	fcmTokenURL   = "https://oauth2.googleapis.com/token"
	fcmScope      = "https://www.googleapis.com/auth/firebase.messaging"
	fcmSendURLFmt = "https://fcm.googleapis.com/v1/projects/%s/messages:send"
)

// fcmServiceAccount is the subset of a Google service-account key file
// we need. PrivateKey is PKCS#8 PEM.
type fcmServiceAccount struct {
	ProjectID   string `json:"project_id"`
	ClientEmail string `json:"client_email"`
	PrivateKey  string `json:"private_key"`
}

// parseFCMServiceAccount accepts raw or base64-encoded key JSON.
func parseFCMServiceAccount(raw string) *fcmServiceAccount {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	tryParse := func(b []byte) *fcmServiceAccount {
		var sa fcmServiceAccount
		if err := json.Unmarshal(b, &sa); err != nil {
			return nil
		}
		if sa.ClientEmail == "" || sa.PrivateKey == "" {
			return nil
		}
		return &sa
	}
	if sa := tryParse([]byte(raw)); sa != nil {
		return sa
	}
	if dec, err := base64.StdEncoding.DecodeString(raw); err == nil {
		return tryParse(dec)
	}
	return nil
}

// fcmTokenSource mints and caches service-account access tokens.
// Tokens live ~1h; we refresh 5 minutes early.
type fcmTokenSource struct {
	sa *fcmServiceAccount

	mu      sync.Mutex
	token   string
	expires time.Time
}

func (ts *fcmTokenSource) accessToken() (string, error) {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	if ts.token != "" && time.Now().Before(ts.expires.Add(-5*time.Minute)) {
		return ts.token, nil
	}

	key, err := jwt.ParseRSAPrivateKeyFromPEM([]byte(ts.sa.PrivateKey))
	if err != nil {
		return "", fmt.Errorf("fcm: parse private key: %w", err)
	}
	now := time.Now()
	assertion, err := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.MapClaims{
		"iss":   ts.sa.ClientEmail,
		"scope": fcmScope,
		"aud":   fcmTokenURL,
		"iat":   now.Unix(),
		"exp":   now.Add(time.Hour).Unix(),
	}).SignedString(key)
	if err != nil {
		return "", fmt.Errorf("fcm: sign assertion: %w", err)
	}

	form := url.Values{
		"grant_type": {"urn:ietf:params:oauth:grant-type:jwt-bearer"},
		"assertion":  {assertion},
	}
	req, err := http.NewRequest("POST", fcmTokenURL, strings.NewReader(form.Encode()))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	res, err := fcmHTTPClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("fcm: token exchange: %w", err)
	}
	defer res.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(res.Body, 1<<16))
	if res.StatusCode != http.StatusOK {
		return "", fmt.Errorf("fcm: token exchange status %d: %s", res.StatusCode, string(body))
	}
	var tok struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"`
	}
	if err := json.Unmarshal(body, &tok); err != nil || tok.AccessToken == "" {
		return "", fmt.Errorf("fcm: bad token response")
	}
	ts.token = tok.AccessToken
	ts.expires = now.Add(time.Duration(tok.ExpiresIn) * time.Second)
	return ts.token, nil
}

// fcmHTTPClient bounds every FCM round-trip so a Google-side stall
// can't wedge the 30s dispatcher tick indefinitely.
var fcmHTTPClient = &http.Client{Timeout: 10 * time.Second}

// sendFCMMessage POSTs one message to the v1 API. Returns (ok, dead,
// reason): dead=true means the token is permanently invalid and should
// be deactivated (UNREGISTERED / 404).
func sendFCMMessage(ts *fcmTokenSource, projectID string, notif OutboxRow, token string) (bool, bool, string) {
	access, err := ts.accessToken()
	if err != nil {
		return false, false, "fcm_auth_failed"
	}

	// data values MUST be strings per the v1 schema.
	payload := map[string]interface{}{
		"message": map[string]interface{}{
			"token": token,
			"notification": map[string]string{
				"title": notif.Title,
				"body":  notif.Body,
			},
			"data": map[string]string{
				"deeplink": notif.Deeplink,
				"outboxId": fmt.Sprintf("%d", notif.ID),
				"trigger":  string(notif.TriggerKind),
			},
			"android": map[string]interface{}{
				"priority": "HIGH",
			},
		},
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return false, false, "fcm_marshal_failed"
	}
	req, err := http.NewRequest("POST", fmt.Sprintf(fcmSendURLFmt, projectID), bytes.NewReader(body))
	if err != nil {
		return false, false, "fcm_request_failed"
	}
	req.Header.Set("Authorization", "Bearer "+access)
	req.Header.Set("Content-Type", "application/json")

	res, err := fcmHTTPClient.Do(req)
	if err != nil {
		return false, false, "fcm_network_error"
	}
	defer res.Body.Close()
	if res.StatusCode == http.StatusOK {
		return true, false, ""
	}
	respBody, _ := io.ReadAll(io.LimitReader(res.Body, 1<<14))
	// 404 = token no longer registered; 400 UNREGISTERED/INVALID_ARGUMENT
	// with the token field = dead token. Anything else is retryable.
	dead := res.StatusCode == http.StatusNotFound ||
		(res.StatusCode == http.StatusBadRequest && strings.Contains(string(respBody), "UNREGISTERED"))
	return false, dead, fmt.Sprintf("fcm_status_%d", res.StatusCode)
}
