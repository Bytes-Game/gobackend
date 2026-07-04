package main

import (
	"net/http/httptest"
	"testing"
	"time"
)

// clientIP must prefer the forwarded client over the (proxy) RemoteAddr, or the
// per-IP limiter collapses every caller behind the proxy into one bucket.
func TestClientIP_Resolution(t *testing.T) {
	cases := []struct{ name, xff, xreal, remote, want string }{
		{"xff single", "203.0.113.7", "", "10.0.0.1:5000", "203.0.113.7"},
		{"xff list takes left-most", "203.0.113.7, 70.41.3.18, 150.172.238.178", "", "10.0.0.1:5000", "203.0.113.7"},
		{"x-real-ip fallback", "", "198.51.100.9", "10.0.0.1:5000", "198.51.100.9"},
		{"remoteaddr fallback strips port", "", "", "192.0.2.5:443", "192.0.2.5"},
	}
	for _, c := range cases {
		req := httptest.NewRequest("GET", "/", nil)
		req.RemoteAddr = c.remote
		if c.xff != "" {
			req.Header.Set("X-Forwarded-For", c.xff)
		}
		if c.xreal != "" {
			req.Header.Set("X-Real-IP", c.xreal)
		}
		if got := clientIP(req); got != c.want {
			t.Errorf("%s: clientIP=%q want %q", c.name, got, c.want)
		}
	}
}

func TestRateLimiter_DistinctKeysIndependent(t *testing.T) {
	rl := newRateLimiter(1, 2) // 1 tok/s, burst 2
	// Bind both calls first: `!allow || !allow` would short-circuit and consume
	// only one token when the first call fails, silently under-exercising burst 2.
	ok1 := rl.allow("a")
	ok2 := rl.allow("a")
	if !ok1 || !ok2 {
		t.Fatal("first two requests for A should pass (burst 2)")
	}
	if rl.allow("a") {
		t.Error("third request for A should be limited")
	}
	if !rl.allow("b") {
		t.Error("key B must have its own bucket, independent of A")
	}
}

func TestRateLimiter_CleanupEvictsIdle(t *testing.T) {
	rl := newRateLimiter(1, 2)
	rl.allow("x")
	if len(rl.buckets) != 1 {
		t.Fatalf("expected 1 bucket after use, got %d", len(rl.buckets))
	}
	// Make the bucket look untouched for an hour, then sweep.
	rl.mu.Lock()
	rl.buckets["x"].lastCheck = time.Now().Add(-time.Hour)
	rl.mu.Unlock()
	rl.cleanup(15 * time.Minute)
	if len(rl.buckets) != 0 {
		t.Errorf("idle bucket should have been evicted; %d remain", len(rl.buckets))
	}
}
