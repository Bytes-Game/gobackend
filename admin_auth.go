package main

import (
	"crypto/subtle"
	"net/http"
	"os"
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
