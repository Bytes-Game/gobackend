package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// Most of these test the two pure functions directly, handing the parsed
// configuration in rather than reaching for package state. Only the tests that
// genuinely exercise the middleware need the reset hook below.

// resetAllowedOrigins makes the next corsAllowOrigin call re-read the
// environment. The parse is a sync.Once because it runs on every request;
// only a test needs it to happen twice in one process.
//
// The Once is REPLACED with a fresh zero value, never copied — copying a
// sync.Once is a vet error (copylocks), and it is not caught by `go test`
// because that only runs a subset of vet's checks. It is caught by the
// standalone `go vet ./...` step in CI.
func resetAllowedOrigins() {
	allowedOriginsOnce = sync.Once{}
	allowedOriginSet = nil
	allowOriginWildcard = true
	loadAllowedOrigins()
}

// withAllowedOrigins points the middleware at one configuration for the length
// of a test, then puts it back.
//
// Cleanup order matters and is doing real work here: t.Cleanup runs
// last-registered-first, so t.Setenv's own restore runs BEFORE ours. By the
// time resetAllowedOrigins runs at cleanup the environment is already back to
// what it was, so the re-parse lands on the original value.
func withAllowedOrigins(t *testing.T, value string) {
	t.Helper()
	t.Setenv("ALLOWED_ORIGINS", value)
	resetAllowedOrigins()
	t.Cleanup(resetAllowedOrigins)
}

func TestParseAllowedOrigins(t *testing.T) {
	cases := []struct {
		name         string
		raw          string
		wantWildcard bool
		wantSet      []string
	}{
		// Unset must behave exactly as this service always has. Anything else
		// is a silent breaking change for a deployed web build.
		{"unset keeps the wildcard", "", true, nil},
		{"whitespace only is still unset", "   ", true, nil},
		// An explicit "*" is how an operator says "I looked at this and I do
		// want the wildcard". It should not behave differently from never
		// having set it.
		{"explicit star is the wildcard", "*", true, nil},
		{"single origin", "https://app.example.com", false,
			[]string{"https://app.example.com"}},
		{"list, spaces trimmed", "https://a.example , https://b.example", false,
			[]string{"https://a.example", "https://b.example"}},
		{"empty entries dropped", "https://a.example,,  ,", false,
			[]string{"https://a.example"}},
		{"stored lowercased", "HTTPS://A.EXAMPLE", false,
			[]string{"https://a.example"}},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			wildcard, set := parseAllowedOrigins(c.raw)
			if wildcard != c.wantWildcard {
				t.Fatalf("wildcard = %v, want %v", wildcard, c.wantWildcard)
			}
			if wildcard {
				return
			}
			if len(set) != len(c.wantSet) {
				t.Fatalf("set has %d entries, want %d: %v", len(set), len(c.wantSet), set)
			}
			for _, want := range c.wantSet {
				if !set[want] {
					t.Errorf("set is missing %q: %v", want, set)
				}
			}
		})
	}
}

func TestCORSAllowOrigin_Wildcard(t *testing.T) {
	allow, vary := corsAllowOriginIn("https://anything.example", true, nil)
	if allow != "*" {
		t.Errorf("allow = %q, want %q", allow, "*")
	}
	if vary {
		t.Error("the wildcard is one fixed answer for every caller, so nothing varies by Origin")
	}
}

func TestCORSAllowOrigin_AllowlistMatching(t *testing.T) {
	_, set := parseAllowedOrigins("https://app.example.com, https://staging.example.com")

	cases := []struct {
		name      string
		origin    string
		wantAllow string
	}{
		{"listed", "https://app.example.com", "https://app.example.com"},
		{"listed, second entry", "https://staging.example.com", "https://staging.example.com"},
		{"case-insensitive host", "HTTPS://APP.EXAMPLE.COM", "HTTPS://APP.EXAMPLE.COM"},
		{"not listed", "https://evil.example", ""},
		// A prefix match would let app.example.com.attacker.test through,
		// which is the classic way an origin allowlist gets defeated.
		{"prefix of a listed origin", "https://app.example.com.attacker.test", ""},
		{"suffix of a listed origin", "https://not-app.example.com", ""},
		{"scheme downgrade", "http://app.example.com", ""},
		{"no origin header", "", ""},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			allow, _ := corsAllowOriginIn(c.origin, false, set)
			if allow != c.wantAllow {
				t.Errorf("corsAllowOriginIn(%q) = %q, want %q", c.origin, allow, c.wantAllow)
			}
		})
	}
}

// Once the answer depends on who asked, the response has to say so, or a
// shared cache will hand one origin's allow header to a different origin.
func TestCORSAllowOrigin_VaryTracksWhetherTheAnswerDepends(t *testing.T) {
	_, set := parseAllowedOrigins("https://app.example.com")

	if _, vary := corsAllowOriginIn("https://app.example.com", false, set); !vary {
		t.Error("an allowed origin must set Vary: Origin")
	}
	if _, vary := corsAllowOriginIn("https://evil.example", false, set); !vary {
		t.Error("a refused origin must set Vary: Origin too — otherwise a cached " +
			"refusal can be served to an origin that would have been allowed")
	}
	if _, vary := corsAllowOriginIn("", false, set); vary {
		t.Error("no Origin header means the response did not depend on one")
	}
}

// PATCH is a real route on this API (/api/v1/users/{id}). It was missing from
// the advertised methods, so a browser preflight for a profile edit would have
// been refused before the request was ever sent.
func TestCORSMiddleware_AdvertisesEveryMethodTheAPIUses(t *testing.T) {
	rec := httptest.NewRecorder()
	corsMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})).
		ServeHTTP(rec, httptest.NewRequest("GET", "/api/v1/profile", nil))

	methods := rec.Header().Get("Access-Control-Allow-Methods")
	for _, m := range []string{"GET", "POST", "PATCH", "DELETE", "OPTIONS"} {
		if !containsMethod(methods, m) {
			t.Errorf("Access-Control-Allow-Methods %q is missing %s", methods, m)
		}
	}
}

// A preflight must be answered by the middleware and must not fall through.
func TestCORSMiddleware_PreflightShortCircuits(t *testing.T) {
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

// A refused origin must get NO allow header at all. An empty one is a
// different thing on the wire, and some clients treat it as present.
func TestCORSMiddleware_RefusedOriginGetsNoAllowHeader(t *testing.T) {
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
	if rec.Header().Get("Vary") != "Origin" {
		t.Errorf("Vary = %q, want Origin", rec.Header().Get("Vary"))
	}
}

// The allowed case, end to end, so the wiring between middleware and decision
// is covered and not just the decision.
func TestCORSMiddleware_AllowedOriginIsEchoed(t *testing.T) {
	withAllowedOrigins(t, "https://app.example.com")

	handler := corsMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/v1/profile", nil)
	req.Header.Set("Origin", "https://app.example.com")
	handler.ServeHTTP(rec, req)

	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "https://app.example.com" {
		t.Errorf("Access-Control-Allow-Origin = %q, want the caller's own origin", got)
	}
	if rec.Header().Get("Vary") != "Origin" {
		t.Errorf("Vary = %q, want Origin", rec.Header().Get("Vary"))
	}
}

// With no ALLOWED_ORIGINS set the middleware must still send the wildcard,
// which is what every deployment does today.
func TestCORSMiddleware_DefaultsToWildcard(t *testing.T) {
	withAllowedOrigins(t, "")

	handler := corsMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/v1/profile", nil)
	req.Header.Set("Origin", "https://anything.example")
	handler.ServeHTTP(rec, req)

	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "*" {
		t.Errorf("Access-Control-Allow-Origin = %q, want *", got)
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
