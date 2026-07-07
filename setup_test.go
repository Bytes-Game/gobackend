package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// mr is the shared miniredis instance used by every test in this package.
var mr *miniredis.Miniredis

// TestMain wires up a miniredis so the production code's global `rdb` client
// talks to an in-process fake Redis. No real Redis is required to run tests.
//
// We do NOT call InitDatabase / InitMeilisearch — tests that need SQL use the
// sqlmock path, and tests that need search use Meilisearch stubs where relevant.
func TestMain(m *testing.M) {
	// Session-token signing key for tests. issueToken/parseToken read this from
	// the env (fail-closed in prod), so set a deterministic one here before any
	// handler test mints or validates a token.
	_ = os.Setenv("JWT_SECRET", "test-jwt-secret-do-not-use-in-prod")

	// Tests assert on fresh per-call content scores (and run many cases against
	// the same content IDs with different DB state), so the production
	// content-score cache must be off for deterministic, isolated results.
	disableContentScoreCache = true
	disableUserProfileCache = true

	var err error
	mr, err = miniredis.Run()
	if err != nil {
		log.Fatalf("miniredis start failed: %v", err)
	}
	defer mr.Close()

	rdb = redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer func() { _ = rdb.Close() }()

	code := m.Run()
	os.Exit(code)
}

// resetRedis clears miniredis between tests so state from one test can't
// leak into the next. Call at the top of any test that writes Redis keys.
func resetRedis(t *testing.T) {
	t.Helper()
	mr.FlushAll()
}

// testToken mints a valid session token for handler tests (JWT_SECRET is set
// in TestMain). Use when a test drives a request through the authed() wrapper
// or a full router and needs a real Authorization header.
func testToken(t *testing.T, userID, username string) string {
	t.Helper()
	tok, err := issueToken(userID, username)
	if err != nil {
		t.Fatalf("issueToken: %v", err)
	}
	return tok
}

// withAuth returns req carrying the trusted-identity context that authed()
// would normally inject, so a handler invoked directly in a test (without
// routing through the middleware) sees the given authUserID(r)/authUsername(r).
func withAuth(req *http.Request, userID, username string) *http.Request {
	ctx := context.WithValue(req.Context(), userIDContextKey, userID)
	ctx = context.WithValue(ctx, usernameContextKey, username)
	return req.WithContext(ctx)
}
