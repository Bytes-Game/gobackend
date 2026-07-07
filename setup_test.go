package main

import (
	"log"
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

// (testToken and withAuth helpers were removed as unused — handler-auth
// tests mint tokens via issueToken directly. Recover from git history
// if a future test needs pre-injected identity context.)
