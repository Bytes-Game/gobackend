package main

import (
	"encoding/json"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

// withMockDB swaps the package-level `db` with a sqlmock connection for the
// duration of the test. Returns the mock for setting expectations.
func withMockDB(t *testing.T) (sqlmock.Sqlmock, func()) {
	t.Helper()
	orig := db
	m, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock init: %v", err)
	}
	db = m
	return mock, func() {
		_ = m.Close()
		db = orig
	}
}

func TestComputeNotificationGoldenHour_WritesRedisAboveThreshold(t *testing.T) {
	resetRedis(t)
	mock, cleanup := withMockDB(t)
	defer cleanup()

	// Two users: u1 has 20 taps concentrated at 19, u2 has only 4 taps (skipped).
	rows := sqlmock.NewRows([]string{"user_id", "hour", "c"}).
		AddRow("u1", 19, 15).
		AddRow("u1", 20, 3).
		AddRow("u1", 21, 2).
		AddRow("u2", 8, 2).
		AddRow("u2", 9, 2)
	mock.ExpectQuery(regexp.QuoteMeta("FROM feed_events")).WillReturnRows(rows)

	n, err := computeNotificationGoldenHour()
	if err != nil {
		t.Fatalf("computeNotificationGoldenHour: %v", err)
	}
	// Both users showed up in the scan, so byUser count == 2 — but only u1
	// crosses the 10-event threshold and lands in Redis.
	if n != 2 {
		t.Errorf("expected 2 users in byUser, got %d", n)
	}

	// u1 should have a confident golden_hour at 19.
	s, err := rdb.Get(rctx, "golden_hour:u1").Result()
	if err != nil || s == "" {
		t.Fatalf("u1 missing from golden_hour: err=%v s=%q", err, s)
	}
	var gh goldenHour
	if err := json.Unmarshal([]byte(s), &gh); err != nil {
		t.Fatalf("u1 payload unparseable: %v", err)
	}
	if gh.Hour != 19 {
		t.Errorf("u1 hour: got %d, want 19", gh.Hour)
	}
	if gh.Confidence < 0.7 {
		t.Errorf("u1 confidence: got %v, want ≥ 0.7 (15/20)", gh.Confidence)
	}

	// u2 must NOT have been written — below threshold.
	if exists, _ := rdb.Exists(rctx, "golden_hour:u2").Result(); exists != 0 {
		t.Error("u2 should not have been written (below 10-event threshold)")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sql expectations: %v", err)
	}
}

func TestComputeNotificationGoldenHour_EmptyDatasetNoCrash(t *testing.T) {
	resetRedis(t)
	mock, cleanup := withMockDB(t)
	defer cleanup()

	rows := sqlmock.NewRows([]string{"user_id", "hour", "c"}) // empty
	mock.ExpectQuery(regexp.QuoteMeta("FROM feed_events")).WillReturnRows(rows)

	n, err := computeNotificationGoldenHour()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 0 {
		t.Errorf("empty dataset should yield 0 users, got %d", n)
	}
}

func TestComputeCreatorAffinity_RecencyAndNegatives(t *testing.T) {
	resetRedis(t)
	mock, cleanup := withMockDB(t)
	defer cleanup()

	// Fresh (age=0) events for u1/cA; stale (age=14) for u1/cB; skip+negative for cC.
	rows := sqlmock.NewRows([]string{"user_id", "creator", "event_type", "age_days"}).
		AddRow("u1", "cA", "complete", 0.0).    // 1.0 * e^0 = 1.0
		AddRow("u1", "cA", "like", 0.0).        // 0.8
		AddRow("u1", "cA", "loop", 0.0).        // 1.5
		AddRow("u1", "cB", "complete", 14.0).   // 1.0 * e^-2 ≈ 0.135
		AddRow("u1", "cC", "skip", 0.0).        // -0.4
		AddRow("u1", "cC", "not_interested", 0.0). // -1.0
		AddRow("u1", "cD", "complete", 0.0)     // 1.0
	mock.ExpectQuery(regexp.QuoteMeta("FROM feed_events fe")).WillReturnRows(rows)

	n, err := computeCreatorAffinity()
	if err != nil {
		t.Fatalf("computeCreatorAffinity: %v", err)
	}
	if n != 1 {
		t.Errorf("expected 1 user, got %d", n)
	}

	// Read back — cA should be normalized to 1.0 (highest), cB much smaller,
	// cC either absent (negative → skipped from top-K if negative) or very low,
	// cD positive but below cA.
	s, err := rdb.Get(rctx, "creator_affinity:u1").Result()
	if err != nil {
		t.Fatalf("creator_affinity:u1 not written: %v", err)
	}
	var top map[string]float64
	if err := json.Unmarshal([]byte(s), &top); err != nil {
		t.Fatalf("parse: %v", err)
	}
	if cA, ok := top["cA"]; !ok || cA < 0.99 {
		t.Errorf("cA expected ~1.0 (normalized max), got %v", cA)
	}
	if cB, ok := top["cB"]; ok {
		if cB >= top["cA"] {
			t.Errorf("cB should be below cA after recency decay, got cA=%v cB=%v", top["cA"], cB)
		}
	}
}

func TestComputeCreatorAffinity_EmptyDatasetNoWrites(t *testing.T) {
	resetRedis(t)
	mock, cleanup := withMockDB(t)
	defer cleanup()

	rows := sqlmock.NewRows([]string{"user_id", "creator", "event_type", "age_days"})
	mock.ExpectQuery(regexp.QuoteMeta("FROM feed_events fe")).WillReturnRows(rows)

	n, err := computeCreatorAffinity()
	if err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	if n != 0 {
		t.Errorf("empty dataset should yield 0 users, got %d", n)
	}
	if keys := mr.Keys(); len(keys) > 0 {
		t.Errorf("empty dataset should write no keys, got %v", keys)
	}
}

func TestComputeCreatorAffinity_AllNegativeSkipsUser(t *testing.T) {
	resetRedis(t)
	mock, cleanup := withMockDB(t)
	defer cleanup()

	// Only negative events → maxV <= 0 → user is skipped.
	rows := sqlmock.NewRows([]string{"user_id", "creator", "event_type", "age_days"}).
		AddRow("u1", "cA", "skip", 0.0).
		AddRow("u1", "cA", "not_interested", 0.0)
	mock.ExpectQuery(regexp.QuoteMeta("FROM feed_events fe")).WillReturnRows(rows)

	if _, err := computeCreatorAffinity(); err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	if exists, _ := rdb.Exists(rctx, "creator_affinity:u1").Result(); exists != 0 {
		t.Errorf("user with only negatives should not be written")
	}
}
