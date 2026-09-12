package main

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"time"
)

// ════════════════════════════════════════════════════════════════════════════
// SAYING WHY A VIDEO IS NOT WATCHABLE YET
// ════════════════════════════════════════════════════════════════════════════
//
// This endpoint's whole value is that it can be trusted. It reports a state
// worked out in Go from three columns, while the thing that actually decides
// whether a worker takes a row is a WHERE clause in SQL somewhere else. If
// those two drift apart the endpoint becomes a confident liar, which is worse
// than not having it — somebody would read "waiting" and wait.
//
// So the tests below check the answers, and then check that the answers are
// still derived from the same conditions the claim query uses.

func claimedAgo(d time.Duration) sql.NullTime {
	return sql.NullTime{Time: time.Now().Add(-d), Valid: true}
}

func TestHLSQueueState_TellsTheThreeInvisibleStatesApart(t *testing.T) {
	// The reason this exists: every list the app serves shows these three as
	// an identical blank field.
	never := sql.NullTime{}
	cases := []struct {
		name     string
		manifest string
		attempts int
		claimed  sql.NullTime
		want     string
	}{
		{"nobody has it", "", 0, never, hlsStateWaiting},
		{"a worker has it", "PENDING", 1, claimedAgo(time.Minute), hlsStateWorking},
		{"out of tries", "", maxHLSAttempts, claimedAgo(2 * time.Hour), hlsStateGivenUp},
		{"converted", "https://cdn/x/master.m3u8", 1, claimedAgo(time.Hour), hlsStateDone},
	}
	for _, c := range cases {
		got, why, _ := hlsQueueState(c.manifest, "https://src/v.mp4", c.attempts, c.claimed)
		if got != c.want {
			t.Errorf("%s: got %q, want %q", c.name, got, c.want)
		}
		if got != hlsStateDone && why == "" {
			t.Errorf("%s: reported %q with no explanation of what happens next",
				c.name, got)
		}
	}
}

func TestHLSQueueState_NoSourceIsItsOwnAnswer(t *testing.T) {
	// A row with no video will never convert however long anybody waits, and
	// reporting it as "waiting" sends whoever asked to watch a queue that can
	// never move.
	got, _, ready := hlsQueueState("", "", 0, sql.NullTime{})
	if got != hlsStateNoSource {
		t.Errorf("got %q, want %q", got, hlsStateNoSource)
	}
	if ready {
		t.Error("a row with no video is reported as ready for a worker")
	}
}

func TestHLSQueueState_RestingIsNotTheSameAsAvailable(t *testing.T) {
	// A video that just failed is waiting AND cannot be taken yet. Reporting
	// it as ready would have somebody watching for a worker to pick up a row
	// no worker is allowed to touch.
	state, why, ready := hlsQueueState("", "https://src/v.mp4", 2, claimedAgo(time.Minute))
	if state != hlsStateWaiting {
		t.Fatalf("got state %q, want %q", state, hlsStateWaiting)
	}
	if ready {
		t.Error("a video still inside its rest period is reported as ready")
	}
	if !strings.Contains(why, "resting") {
		t.Errorf("the explanation does not mention the rest period: %q", why)
	}

	_, _, ready = hlsQueueState("", "https://src/v.mp4", 2, claimedAgo(hlsRetryCooloff+time.Minute))
	if !ready {
		t.Error("a video whose rest period has passed is still reported as " +
			"not ready, so the queue would look permanently stalled")
	}
}

func TestHLSQueueState_AgreesWithWhatTheWorkerActuallyDoes(t *testing.T) {
	// The anti-drift test, written as behaviour rather than as matching text.
	//
	// Each case below is one leg of the claim query's WHERE clause. If this
	// endpoint says a row is ready while the claim query would skip it, it is
	// telling somebody to wait for something that will never happen.
	src := "https://src/v.mp4"
	fresh := sql.NullTime{}

	if _, _, ready := hlsQueueState("", src, 0, fresh); !ready {
		t.Fatal("a fresh untouched upload is not reported as claimable, but " +
			"the claim query would take it")
	}
	for _, c := range []struct {
		leg      string
		manifest string
		src      string
		attempts int
		claimed  sql.NullTime
	}{
		{"hls_manifest_url = ''", "PENDING", src, 0, claimedAgo(time.Minute)},
		{"hls_manifest_url = ''", "https://cdn/m.m3u8", src, 0, fresh},
		{"video_url <> ''", "", "", 0, fresh},
		{"hls_attempts < cap", "", src, maxHLSAttempts, claimedAgo(3 * time.Hour)},
		{"hls_claimed_at cool-off", "", src, 1, claimedAgo(time.Second)},
	} {
		if _, _, ready := hlsQueueState(c.manifest, c.src, c.attempts, c.claimed); ready {
			t.Errorf("a row the claim query skips (%s) is reported as ready "+
				"for a worker", c.leg)
		}
	}
}

func TestHLSQueueState_CoversEveryLegOfTheClaimQuery(t *testing.T) {
	// The test above can only check legs it knows about. This one notices a
	// NEW leg being added to the claim query, so nobody has to remember that
	// this endpoint exists.
	sql := claimSQL(t)
	legs := regexp.MustCompile(`\bAND\b`).FindAllString(sql, -1)
	// hls_manifest_url = '' is the first condition, so it has no AND; the
	// cool-off leg is a single AND holding an OR of two.
	const known = 3 // video_url, hls_attempts, hls_claimed_at
	if len(legs) != known {
		t.Errorf("the claim query now has %d AND-ed conditions, not %d.\n\n"+
			"A condition was added or removed. hlsQueueState has to make the "+
			"same decision or /admin/media/queue will report rows as ready "+
			"that no worker will take. Update both, then update this count.\n\n"+
			"claim query: %s", len(legs), known, sql)
	}
}

func TestHLSQueue_NeedsTheAdminPassword(t *testing.T) {
	// Which video somebody uploaded and where its source lives is not public.
	t.Setenv("ADMIN_USER", "admin")
	t.Setenv("ADMIN_PASS", "letmein")
	rec := httptest.NewRecorder()
	adminOnly(AdminHLSQueueHandler)(rec, httptest.NewRequest(http.MethodGet, "/admin/media/queue", nil))
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("no password got %d, want %d", rec.Code, http.StatusUnauthorized)
	}
}

func TestHLSQueue_RejectsNonsenseBeforeTouchingTheDatabase(t *testing.T) {
	// A bad request is a bad request whether or not the database is up, and
	// answering "service unavailable" sends whoever sent it looking in the
	// wrong place. db is nil in tests, which is exactly that situation.
	for _, q := range []string{"?limit=nonsense", "?limit=0", "?limit=-3", "?kind=videos"} {
		rec := httptest.NewRecorder()
		AdminHLSQueueHandler(rec, httptest.NewRequest(http.MethodGet, "/admin/media/queue"+q, nil))
		if rec.Code != http.StatusBadRequest {
			t.Errorf("%s got %d, want %d (%s)", q, rec.Code, http.StatusBadRequest,
				strings.TrimSpace(rec.Body.String()))
		}
	}
}

func TestHLSQueue_IsRegisteredAndReadOnly(t *testing.T) {
	// A handler nothing routes to is decorative, and this one must never be
	// reachable by a method that could change anything.
	main := readSourceFile(t, "main.go")
	if !strings.Contains(main, `"/admin/media/queue"`) {
		t.Fatal("the endpoint is not routed, so it cannot be reached")
	}
	line := ""
	for _, l := range strings.Split(main, "\n") {
		if strings.Contains(l, `"/admin/media/queue"`) {
			line = l
		}
	}
	if !strings.Contains(line, "adminOnly(") {
		t.Error("the queue view is not behind the admin login")
	}
	for _, m := range []string{`"POST"`, `"PUT"`, `"DELETE"`, `"PATCH"`} {
		if strings.Contains(line, m) {
			t.Errorf("the queue view accepts %s; it only reads", m)
		}
	}
	// And nothing in it writes.
	src := readSourceFile(t, "admin_hls_queue.go")
	for _, verb := range []string{"UPDATE ", "INSERT ", "DELETE ", "db.Exec("} {
		if strings.Contains(src, verb) {
			t.Errorf("the queue view contains %q; it is meant only to look", verb)
		}
	}
}

func TestHLSQueue_ResponseSurvivesARoundTrip(t *testing.T) {
	// The shape is the contract; a field that cannot be encoded is a field
	// nobody reading this will ever see.
	body, err := json.Marshal(hlsQueueResponse{
		Counts: map[string]int{hlsStateWaiting: 2},
		Rows: []hlsQueueRow{{
			ID: 46, Kind: "challenges", State: hlsStateGivenUp,
			Why: "it used all 5 tries", Attempts: 5, Cap: maxHLSAttempts,
			SourceURL: "https://example.invalid/v.mp4",
		}},
	})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var back hlsQueueResponse
	if err := json.Unmarshal(body, &back); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(back.Rows) != 1 || back.Rows[0].State != hlsStateGivenUp {
		t.Errorf("round trip lost the state: %s", body)
	}
	if back.Counts[hlsStateWaiting] != 2 {
		t.Errorf("round trip lost the counts: %s", body)
	}
}
