package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// resetActionLimiters drops every lazily-created per-action bucket so one test
// cannot spend another test's budget. The limiters are package-level and
// created on first use, so without this the second test to touch "login" would
// start with whatever the first one left.
func resetActionLimiters(t *testing.T) {
	t.Helper()
	actionLimitersMu.Lock()
	actionLimiters = map[string]*rateLimiter{}
	actionLimitersMu.Unlock()
}

// The "login" row sat in actionLimitTable unused for the whole life of the
// file — the table described a limit that nothing enforced. This pins that
// the row exists AND that a limiter can actually be built from it, which is
// what "unknown actions are allowed unconditionally" would otherwise hide: a
// typo in the action name fails open and looks exactly like working code.
func TestLoginActionLimit_IsConfigured(t *testing.T) {
	cfg, ok := actionLimitTable["login"]
	if !ok {
		t.Fatal(`no "login" row in actionLimitTable — allowAction would fail OPEN, ` +
			`silently removing brute-force protection`)
	}
	if cfg.burst <= 0 || cfg.tokensPerSecond <= 0 {
		t.Fatalf("login limit is not enforceable: %+v", cfg)
	}
	if getActionLimiter("login") == nil {
		t.Fatal("getActionLimiter(\"login\") returned nil despite a table row")
	}
}

// Burst must be spendable, and the bucket must close once it is spent.
func TestLoginActionLimit_ExhaustsAfterBurst(t *testing.T) {
	resetActionLimiters(t)
	cfg := actionLimitTable["login"]

	for i := 0; i < cfg.burst; i++ {
		if !allowAction("user:alice", "login") {
			t.Fatalf("attempt %d of burst %d was refused; a real user typing a "+
				"password wrong twice must not be locked out", i+1, cfg.burst)
		}
	}
	if allowAction("user:alice", "login") {
		t.Error("attempt past the burst was allowed — the bucket is not closing")
	}
}

// The two buckets exist to stop two different attacks, so neither may drain
// the other. If they shared a key, one hammered account would lock out every
// other user behind the same address.
func TestLoginActionLimit_UsernameAndIPBucketsAreIndependent(t *testing.T) {
	resetActionLimiters(t)
	cfg := actionLimitTable["login"]

	for i := 0; i < cfg.burst; i++ {
		allowAction("user:alice", "login")
	}
	if allowAction("user:alice", "login") {
		t.Fatal("alice's bucket should be empty by now")
	}
	if !allowAction("user:bob", "login") {
		t.Error("bob's bucket was drained by alice — a password list against one " +
			"account would lock out everyone else")
	}
	if !allowAction("ip:203.0.113.7", "login") {
		t.Error("the IP bucket was drained by a username bucket — they must be separate")
	}
}

// Spraying one password across many accounts never repeats a username, so the
// username bucket alone would never fire. The IP bucket is what catches it.
func TestLoginActionLimit_IPBucketCatchesSprayAcrossAccounts(t *testing.T) {
	resetActionLimiters(t)
	cfg := actionLimitTable["login"]

	const attacker = "ip:198.51.100.4"
	for i := 0; i < cfg.burst; i++ {
		if !allowAction(attacker, "login") {
			t.Fatalf("attempt %d refused before burst %d was spent", i+1, cfg.burst)
		}
	}
	if allowAction(attacker, "login") {
		t.Error("one address kept guessing past its burst — password spraying " +
			"across distinct usernames would never be throttled")
	}
}

// End to end through the handler: once the bucket is empty LoginHandler must
// answer 429 and must do so WITHOUT reaching credential validation, which is
// what keeps the gate cheap under attack (no database work per guess).
func TestLoginHandler_ReturnsRateLimitedOnceBucketIsEmpty(t *testing.T) {
	resetActionLimiters(t)
	cfg := actionLimitTable["login"]

	// Drain both buckets the handler will consult, using the same keys it
	// builds: the username lowercased and trimmed, and the client IP.
	for i := 0; i < cfg.burst; i++ {
		allowAction("user:alice", "login")
	}
	for i := 0; i < cfg.burst; i++ {
		allowAction("ip:203.0.113.9", "login")
	}

	req := httptest.NewRequest("POST", "/login",
		strings.NewReader(`{"username":"  Alice  ","password":"guess"}`))
	req.Header.Set("X-Forwarded-For", "203.0.113.9")
	rec := httptest.NewRecorder()

	// db is nil in tests. Reaching IsValidUser would be a nil-database access,
	// so surviving this call is itself evidence the gate ran first.
	LoginHandler(rec, req)

	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusTooManyRequests)
	}
	if got := rec.Header().Get("Retry-After"); got == "" {
		t.Error("no Retry-After header — clients have nothing to back off against")
	}
	if !strings.Contains(rec.Body.String(), `"login"`) {
		t.Errorf("body %q does not name the limited action", rec.Body.String())
	}
}

// Casing and whitespace must not buy a second budget against one account.
func TestLoginHandler_NormalisesTheUsernameKey(t *testing.T) {
	resetActionLimiters(t)
	cfg := actionLimitTable["login"]

	for i := 0; i < cfg.burst; i++ {
		allowAction("user:alice", "login")
	}

	// "  ALICE  " must land in the same bucket as "alice".
	req := httptest.NewRequest("POST", "/login",
		strings.NewReader(`{"username":"  ALICE  ","password":"guess"}`))
	req.Header.Set("X-Forwarded-For", "203.0.113.55")
	rec := httptest.NewRecorder()

	LoginHandler(rec, req)

	if rec.Code != http.StatusTooManyRequests {
		t.Errorf("status = %d, want %d — a different casing got a fresh budget "+
			"against the same account", rec.Code, http.StatusTooManyRequests)
	}
}
