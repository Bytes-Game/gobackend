package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// withAllowedOrigins re-parses ALLOWED_ORIGINS for one test and restores the
// previous state afterwards. The parse is a sync.Once because it is on every
// request; resetting the Once is what lets a test exercise more than one
// configuration in a single process.
func withAllowedOrigins(t *testing.T, value string) {
	t.Helper()
	prevOnce, prevSet, prevWildcard := allowedOriginsOnce, allowedOriginSet, allowOriginWildcard
	t.Cleanup(func() {
		allowedOriginsOnce, allowedOriginSet, allowOriginWildcard = prevOnce, prevSet, prevWildcard
	})
	t.Setenv("ALLOWED_ORIGINS", value)
	allowedOriginsOnce = sync.Once{}
	allowedOriginSet = nil
	allowOriginWildcard = true
	loadAllowedOrigins()
}

// Unset must behave exactly as this service always has. Anything else would be
// a silent breaking change for a deployed web build.
func TestCORS_UnsetKeepsWildcard(t *testing.T) {
	withAllowedOrigins(t, "")

	allow, vary := corsAllowOrigin("https://anything.example")
	if allow != "*" {
		t.Errorf("allow = %q, want %q", allow, "*")
	}
	if vary {
		t.Error("wildcard is one fixed answer for every caller, so nothing varies by Origin")
	}
}

// An explicit "*" is the same as unset — it is how an operator says "I looked
// at this and I do want the wildcard", which should not behave differently
// from never having set it.
func TestCORS_ExplicitStarIsWildcard(t *testing.T) {
	withAllowedOrigins(t, "*")

	if allow, _ := corsAllowOrigin("https://anything.example"); allow != "*" {
		t.Errorf("allow = %q, want %q", allow, "*")
	}
}

func TestCORS_AllowlistEchoesOnlyListedOrigins(t *testing.T) {
	withAllowedOrigins(t, "https://app.example.com, https://staging.example.com")

	cases := []struct {
		name      string
		origin    string
		wantAllow string
	}{
		{"listed", "https://app.example.com", "https://app.example.com"},
		{"listed, second entry", "https://staging.example.com", "https://staging.example.com"},
		{"case-insensitive host", "HTTPS://APP.EXAMPLE.COM", "HTTPS://APP.EXAMPLE.COM"},
		{"not listed", "https://evil.example", ""},
		// A prefix match would let evil-app.example.com.attacker.test through,
		// which is the classic way an origin allowlist gets defeated.
		{"prefix of a listed origin", "https://app.example.com.attacker.test", ""},
		{"scheme downgrade", "http://app.example.com", ""},
		{"no origin header", "", ""},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			allow, _ := corsAllowOrigin(c.origin)
			if allow != c.wantAllow {
				t.Errorf("corsAllowOrigin(%q) = %q, want %q", c.origin, allow, c.wantAllow)
			}
		})
	}
}

// Once the answer depends on who asked, the response has to say so or a shared
// cache will hand one origin's allow header to another origin.
func TestCORS_AllowlistSetsVary(t *testing.T) {
	withAllowedOrigins(t, "https://app.example.com")

	if _, vary := corsAllowOrigin("https://app.example.com"); !vary {
		t.Error("an allowed origin must set Vary: Origin")
	}
	if _, vary := corsAllowOrigin("https://evil.example"); !vary {
		t.Error("a refused origin must set Vary: Origin too — otherwise a cached " +
			"refusal can be served to an origin that would have been allowed")
	}
	if _, vary := corsAllowOrigin(""); vary {
		t.Error("no Origin header means the response did not depend on one")
	}
}

// PATCH is a real route on this API (/api/v1/users/{id}). It was missing from
// the advertised methods, so a browser preflight for a profile edit would have
// been refused before the request was ever sent.
func TestCORS_AdvertisesEveryMethodTheAPIActuallyUses(t *testing.T) {
	withAllowedOrigins(t, "")

	rec := httptest.NewRecorder()
	handler := corsMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	handler.ServeHTTP(rec, httptest.NewRequest("GET", "/api/v1/profile", nil))

	methods := rec.Header().Get("Access-Control-Allow-Methods")
	for _, m := range []string{"GET", "POST", "PATCH", "DELETE", "OPTIONS"} {
		if !containsMethod(methods, m) {
			t.Errorf("Access-Control-Allow-Methods %q is missing %s", methods, m)
		}
	}
}

// A preflight must be answered by the middleware itself and must not fall
// through to the handler.
func TestCORS_PreflightShortCircuits(t *testing.T) {
	withAllowedOrigins(t, "")

	reached := false
	handler := corsMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reached = true
	}))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest("OPTIONS", "/api/v1/profile", nil))

	if reached {
		t.Error("preflight reached the wrapped handler")
	}
	if rec.Code != http.StatusOK {
		t.Errorf("preflight status = %d, want 200", rec.Code)
	}
}

// A refused origin must get NO allow header at all — an empty one is a
// different thing and some clients treat it as present.
func TestCORS_RefusedOriginGetsNoAllowHeader(t *testing.T) {
	withAllowedOrigins(t, "https://app.example.com")

	handler := corsMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/v1/profile", nil)
	req.Header.Set("Origin", "https://evil.example")
	handler.ServeHTTP(rec, req)

	if _, present := rec.Header()["Access-Control-Allow-Origin"]; present {
		t.Errorf("refused origin still received an allow header: %q",
			rec.Header().Get("Access-Control-Allow-Origin"))
	}
}

func containsMethod(header, method string) bool {
	for _, part := range strings.Split(header, ",") {
		if strings.TrimSpace(part) == method {
			return true
		}
	}
	return false
}
