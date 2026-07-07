package main

// Regression tests for the 2026-07 fix batch: HLS kind routing, FCM
// service-account parsing, the distributed rate-limit Lua bucket, the
// Redis lease used by keyed locks in multi-replica mode, and the
// mood/trajectory persistence round-trip.

import (
	"encoding/base64"
	"testing"
	"time"
)

func TestHLSTableForKind(t *testing.T) {
	cases := map[string]string{
		"":          "challenges",
		"challenge": "challenges",
		"bogus":     "challenges", // unknown kinds must not name arbitrary tables
		"response":  "challenge_responses",
	}
	for kind, want := range cases {
		if got := hlsTableForKind(kind); got != want {
			t.Errorf("hlsTableForKind(%q) = %q, want %q", kind, got, want)
		}
	}
}

func TestParseFCMServiceAccount(t *testing.T) {
	raw := `{"project_id":"devf-prod","client_email":"svc@devf.iam.gserviceaccount.com","private_key":"-----BEGIN PRIVATE KEY-----\nx\n-----END PRIVATE KEY-----\n"}`

	sa := parseFCMServiceAccount(raw)
	if sa == nil || sa.ProjectID != "devf-prod" {
		t.Fatalf("raw JSON parse failed: %+v", sa)
	}

	sa = parseFCMServiceAccount(base64.StdEncoding.EncodeToString([]byte(raw)))
	if sa == nil || sa.ClientEmail != "svc@devf.iam.gserviceaccount.com" {
		t.Fatalf("base64 parse failed: %+v", sa)
	}

	for _, bad := range []string{"", "not json", `{"project_id":"x"}`} {
		if got := parseFCMServiceAccount(bad); got != nil {
			t.Errorf("parseFCMServiceAccount(%q) = %+v, want nil", bad, got)
		}
	}
}

// TestDistributedRateLimitLua drives the Redis token bucket with a
// synthetic clock (the script takes now-ms as an argument, so no
// sleeping needed): burst is honored, exhaustion denies, elapsed time
// refills.
func TestDistributedRateLimitLua(t *testing.T) {
	resetRedis(t)

	key := []string{"rl:test:client1"}
	now := time.Now().UnixMilli()
	eval := func(atMs int64) int64 {
		res, err := rdb.Eval(rctx, rateLimitLua, key, 2.0, 3, atMs).Result()
		if err != nil {
			t.Fatalf("eval: %v", err)
		}
		n, _ := res.(int64)
		return n
	}

	// Burst of 3 allowed back-to-back, 4th denied.
	for i := 0; i < 3; i++ {
		if eval(now) != 1 {
			t.Fatalf("request %d within burst should be allowed", i+1)
		}
	}
	if eval(now) != 0 {
		t.Fatal("4th request at same instant should be denied")
	}

	// After 1s at 2 tokens/s, two more slots exist.
	later := now + 1000
	for i := 0; i < 2; i++ {
		if eval(later) != 1 {
			t.Fatalf("refill after 1s at 2 rps should allow request %d", i+1)
		}
	}
	if eval(later) != 0 {
		t.Fatal("third request after refill should be denied")
	}
}

// TestAcquireRedisLease verifies mutual exclusion and owner-checked
// release. acquireRedisLease is exercised directly — it doesn't consult
// multiReplica(), which the wrapping lock() gate owns.
func TestAcquireRedisLease(t *testing.T) {
	resetRedis(t)

	release1 := acquireRedisLease("lk:test:u1", time.Second)

	// While held, a second acquire must time out and fall open (no-op
	// release func returned, lease value untouched).
	start := time.Now()
	release2 := acquireRedisLease("lk:test:u1", time.Second)
	if time.Since(start) < redisLeaseWait {
		t.Fatal("second acquire should have spun for the full wait window")
	}
	held, _ := rdb.Get(rctx, "lk:test:u1").Result()
	release2()
	after, _ := rdb.Get(rctx, "lk:test:u1").Result()
	if held == "" || after != held {
		t.Fatal("fail-open release must not delete a lease it doesn't own")
	}

	// Real release frees the key for the next acquirer.
	release1()
	if n, _ := rdb.Exists(rctx, "lk:test:u1").Result(); n != 0 {
		t.Fatal("owner release should delete the lease")
	}
	release3 := acquireRedisLease("lk:test:u1", time.Second)
	defer release3()
	if n, _ := rdb.Exists(rctx, "lk:test:u1").Result(); n != 1 {
		t.Fatal("re-acquire after release should hold the lease")
	}
}

func TestMoodTransitionPersistRoundtrip(t *testing.T) {
	resetRedis(t)
	resetMoodTransitions()

	// Three observations of the same pair: rewards 1.0, 0.0, 1.0.
	persistMoodObservation("bored", "funny", 1.0)
	persistMoodObservation("bored", "funny", 0.0)
	persistMoodObservation("bored", "funny", 1.0)

	resetMoodTransitions()
	loadMoodTransitions()

	moodTransitions.mu.RLock()
	defer moodTransitions.mu.RUnlock()
	if got := moodTransitions.counts["bored"]["funny"]; got != 3 {
		t.Fatalf("count after reload = %d, want 3", got)
	}
	reward := moodTransitions.rewards["bored"]["funny"]
	if reward < 0.66 || reward > 0.67 {
		t.Fatalf("reward after reload = %v, want ~2/3 (sum/count)", reward)
	}
}

func TestTrajectoryPersistRoundtrip(t *testing.T) {
	resetRedis(t)
	resetSessionTrajectories()

	persistTrajObservation(CohortEngaged, "comedy:low", "comedy:high")
	persistTrajObservation(CohortEngaged, "comedy:low", "comedy:high")
	persistTrajObservation(CohortEngaged, "comedy:low", "music:low")

	resetSessionTrajectories()
	loadSessionTrajectories()

	sessionTrajectories.mu.RLock()
	st := sessionTrajectories.byCoh[CohortEngaged]
	sessionTrajectories.mu.RUnlock()
	if st == nil {
		t.Fatal("engaged cohort state missing after reload")
	}
	st.mu.RLock()
	defer st.mu.RUnlock()
	if got := st.transitions["comedy:low"]["comedy:high"]; got != 2 {
		t.Fatalf("transition count = %d, want 2", got)
	}
	if got := st.fromTotals["comedy:low"]; got != 3 {
		t.Fatalf("fromTotals = %d, want 3", got)
	}
}
