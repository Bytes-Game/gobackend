package main

import (
	"crypto/subtle"
	"log"
	"net/http"
	"os"
	"strings"
)

// ════════════════════════════════════════════════════════════════════════════════
// ADMIN AUTH — HTTP Basic Auth gate for /admin* surfaces
// ════════════════════════════════════════════════════════════════════════════════
//
// Reads two env vars at request time:
//   ADMIN_USER   — username
//   ADMIN_PASS   — password
//
// If either is missing, the gate returns 503 (service unavailable) so we never
// silently expose admin data because someone forgot to set the env. If they're
// set, the browser's native Basic-Auth prompt is used — no login page to build.
//
// Constant-time comparison prevents timing side-channels (not critical for a
// private admin, but cheap to get right).

// adminOnly wraps a handler and requires valid Basic Auth credentials.
// Usage:
//
//	api.HandleFunc("/admin/foo", adminOnly(FooHandler)).Methods("GET")
func adminOnly(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		wantUser := os.Getenv("ADMIN_USER")
		wantPass := os.Getenv("ADMIN_PASS")

		if wantUser == "" || wantPass == "" {
			http.Error(w, "admin auth not configured: set ADMIN_USER and ADMIN_PASS", http.StatusServiceUnavailable)
			return
		}

		gotUser, gotPass, ok := r.BasicAuth()
		userOK := ok && subtle.ConstantTimeCompare([]byte(gotUser), []byte(wantUser)) == 1
		passOK := ok && subtle.ConstantTimeCompare([]byte(gotPass), []byte(wantPass)) == 1

		if !userOK || !passOK {
			w.Header().Set("WWW-Authenticate", `Basic realm="devf-admin"`)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}

		h(w, r)
	}
}

// ════════════════════════════════════════════════════════════════════════════════
// STARTUP CREDENTIAL CHECK
// ════════════════════════════════════════════════════════════════════════════════

// weakAdminPasswords are the values that sit at the top of every credential
// list an attacker would try first. This is not a password strength meter —
// it exists for one specific failure, which is a placeholder that was set
// during development and never rotated before the service saw real traffic.
//
// The admin surface is not a minor one: /admin/diagnostics, /admin/errors and
// /admin/feed-health expose user behaviour and operational internals, and
// /admin/reseed DROPS AND REBUILDS THE DATABASE. Guessing this password is
// not a read-only mistake.
var weakAdminPasswords = map[string]bool{
	"password":      true,
	"password123":   true,
	"passw0rd":      true,
	"admin":         true,
	"admin123":      true,
	"administrator": true,
	"changeme":      true,
	"secret":        true,
	"letmein":       true,
	"qwerty":        true,
	"root":          true,
	"toor":          true,
	"pass":          true,
	"test":          true,
	"12345678":      true,
	"123456789":     true,
	"123456":        true,
	"devf":          true,
	"battlearena":   true,
}

// minAdminPassLen is the length below which we warn regardless of content. A
// short password is brute-forceable even when it is not on the list above,
// and the admin gate has no lockout of its own — it sits behind the global
// per-IP limiter and nothing else.
const minAdminPassLen = 12

// checkAdminConfig logs at startup when the admin credentials are missing or
// obviously unrotated, so an operator finds out from the boot log rather than
// from an incident.
//
// Deliberately warn-only, matching checkAuthConfig: refusing to boot over a
// weak admin password would take the whole API down with it, and the API
// serving users matters more than the dashboard being locked. The gate itself
// already fails closed when the vars are unset, so an unconfigured admin
// surface is unreachable rather than open — the warning is about the case
// where it IS reachable and the key is one somebody would guess.
func checkAdminConfig() {
	user := os.Getenv("ADMIN_USER")
	pass := os.Getenv("ADMIN_PASS")

	if user == "" || pass == "" {
		log.Println("NOTE: ADMIN_USER / ADMIN_PASS are not set — every /admin route " +
			"will answer 503 until they are. This is the safe default, not an error.")
		return
	}

	switch {
	case weakAdminPasswords[strings.ToLower(pass)]:
		log.Println("SECURITY WARNING: ADMIN_PASS is one of the most commonly guessed " +
			"passwords. The admin surface can read user analytics and can WIPE AND " +
			"RESEED THE DATABASE (/admin/reseed). Rotate it before this service takes " +
			"real traffic.")
	case len(pass) < minAdminPassLen:
		log.Printf("SECURITY WARNING: ADMIN_PASS is only %d characters. The admin gate "+
			"has no lockout of its own, so short passwords are brute-forceable. Use at "+
			"least %d characters.", len(pass), minAdminPassLen)
	}

	if strings.EqualFold(user, "admin") && len(pass) < minAdminPassLen {
		log.Println("SECURITY WARNING: ADMIN_USER is the default \"admin\" and ADMIN_PASS " +
			"is short — an attacker only has to guess one of the two.")
	}
}
