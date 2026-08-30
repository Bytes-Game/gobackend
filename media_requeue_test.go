package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

// ════════════════════════════════════════════════════════════════════════════
// PUTTING FINISHED VIDEOS BACK IN THE QUEUE
// ════════════════════════════════════════════════════════════════════════════
//
// This writes to every video row it selects, so the things worth pinning are
// the bounds: how many it can touch at once, which rows it is allowed to
// touch, and that it cannot be reached without the admin password.

// readSourceFile is how the checks below read a WHERE clause. A source-level
// test is unusual and it is here because these guards are SQL: a clause that
// loses a leg still compiles, still runs, and still returns rows — just the
// wrong ones, against production video.
func readSourceFile(t *testing.T, name string) string {
	t.Helper()
	b, err := os.ReadFile(name)
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	return string(b)
}

func requeuePost(t *testing.T, body string) *httptest.ResponseRecorder {
	t.Helper()
	var r *http.Request
	if body == "" {
		r = httptest.NewRequest("POST", "/api/v1/admin/media/requeue", nil)
	} else {
		r = httptest.NewRequest("POST", "/api/v1/admin/media/requeue", strings.NewReader(body))
	}
	w := httptest.NewRecorder()
	AdminRequeueMediaHandler(w, r)
	return w
}

func TestRequeue_NeedsADatabase(t *testing.T) {
	// db is nil under test, so every case below stops here. That is the
	// point: the handler must refuse rather than panic, and the refusal is
	// what makes the rest of these safe to run without a database.
	w := requeuePost(t, `{"limit":10}`)
	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("got %d with no database, want 503", w.Code)
	}
}

func TestRequeue_BatchSizeIsBounded(t *testing.T) {
	// Each video costs the worker a download and up to six encodes. An
	// unbounded batch is not a bigger request, it is hours of runner time
	// queued by one mistyped number.
	if requeueMaxBatch > 500 {
		t.Errorf("one call can queue %d videos; at roughly forty seconds each "+
			"that is over five hours of worker time", requeueMaxBatch)
	}
	if requeueDefaultBatch > requeueMaxBatch {
		t.Errorf("the default batch (%d) is above the ceiling (%d)",
			requeueDefaultBatch, requeueMaxBatch)
	}
	if requeueDefaultBatch <= 0 {
		t.Error("a request that names no size would queue nothing")
	}
}

func TestRequeue_OnlyEverTouchesFinishedRows(t *testing.T) {
	// The three states a row can be in are '' (waiting), 'PENDING' (a worker
	// is holding it right now) and a real URL (done). Only the third may be
	// reset — resetting the other two would either do nothing or yank a job
	// out from under a worker mid-transcode, and the completion callback
	// would then write a manifest for a row that had been re-queued.
	//
	// Source-level because the guard is a WHERE clause, and a WHERE clause
	// that loses a leg still compiles and still returns rows.
	src := readSourceFile(t, "media_requeue.go")
	for _, guard := range []string{
		`hls_manifest_url <> ''`,
		`hls_manifest_url <> 'PENDING'`,
	} {
		if !strings.Contains(src, guard) {
			t.Errorf("the re-queue no longer requires %s.\n\nWithout it this "+
				"can reset a row a worker is holding right now, and that "+
				"worker's completion callback will then overwrite the reset.",
				guard)
		}
	}
	// A row with no source video can never transcode; queueing it just burns
	// attempts until it hits the retry cap.
	if !strings.Contains(src, `COALESCE(video_url, '') <> ''`) {
		t.Error("rows with no source video are being queued; they cannot " +
			"succeed and will burn their retry budget failing")
	}
}

func TestRequeue_ClearsTheRetryCountToo(t *testing.T) {
	// hls_attempts stops a genuinely broken source being retried forever. A
	// row that already SUCCEEDED and is being sent round again deserves a
	// clean slate — otherwise a video that failed four times long ago gets
	// one attempt and is dropped for good.
	src := readSourceFile(t, "media_requeue.go")
	if !strings.Contains(src, "hls_attempts     = 0") {
		t.Error("the retry count is not reset, so a video with old failures " +
			"against it gets fewer attempts than a fresh upload")
	}
	if !strings.Contains(src, "hls_claimed_at    = NULL") {
		t.Error("the claim timestamp is not cleared; the reaper uses it to " +
			"find jobs a crashed worker abandoned")
	}
}

func TestRequeue_IsAdminOnly(t *testing.T) {
	// It rewrites the transcode state of every video it touches. The route
	// must be behind the same password as the rest of /admin.
	src := readSourceFile(t, "main.go")
	if !strings.Contains(src, `api.HandleFunc("/admin/media/requeue", adminOnly(AdminRequeueMediaHandler))`) {
		t.Error("the re-queue route is not wrapped in adminOnly")
	}
}

func TestRequeue_DecidesNothingAboutQuality(t *testing.T) {
	// Whether a video is worth re-encoding is a question about the FILE, and
	// only the worker has the file — see progressiveSkipBps, which leaves an
	// already-lean source alone. A second opinion here, guessing from a
	// database row, would be a rule nobody could keep in step with the first.
	src := readSourceFile(t, "media_requeue.go")
	for _, leak := range []string{"bit_rate", "bitrate", "Mbps"} {
		if strings.Contains(src, leak+" >") || strings.Contains(src, "WHERE "+leak) {
			t.Errorf("this is deciding on %q, but it cannot see the video "+
				"file. That decision belongs in the worker.", leak)
		}
	}
}

func TestRequeue_ReportsWhatItDid(t *testing.T) {
	// The response shape, so a caller can tell "queued 50" from "queued 0,
	// nothing left to do" without reading the server log.
	var resp requeueResponse
	if err := json.Unmarshal([]byte(`{"requeued":7,"kind":"challenges","note":"x"}`), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Requeued != 7 || resp.Kind != "challenges" {
		t.Errorf("the response does not round-trip: %+v", resp)
	}
}

// ════════════════════════════════════════════════════════════════════════════
// SAYING WHETHER THE WORKER ACTUALLY STARTED
// ════════════════════════════════════════════════════════════════════════════
//
// An upload pokes the worker in the background and swallows every error into a
// log line, because an upload must never fail or wait on GitHub. Correct, and
// it makes the feature untestable from outside: a token that is missing,
// expired or scoped wrong behaves exactly like one that works, and the only
// visible difference is how long a new video takes to become watchable.
//
// So the admin path answers the question out loud. These pin that it keeps
// doing so, because a diagnostic that quietly stops diagnosing is worse than
// none — it reads as a clean bill of health.

func TestRequeue_SaysWhenTheTokenIsMissing(t *testing.T) {
	t.Setenv(githubWorkerTokenEnv, "")

	started, note := startWorkerNow(context.Background())
	if started {
		t.Error("claimed the worker started with no token set")
	}
	if !strings.Contains(note, githubWorkerTokenEnv) {
		t.Errorf("the note does not name the variable to set, so nobody "+
			"reading it knows what to do: %q", note)
	}
	// The reason this matters is the wait, so the note has to say that much.
	if !strings.Contains(note, "scheduled run") {
		t.Errorf("the note does not say what the cost is: %q", note)
	}
}

func TestRequeue_SaysWhenGitHubRefusesTheToken(t *testing.T) {
	// The failure that looks most like success from the outside: the variable
	// is set, so every "is it configured?" check passes, and uploads are
	// silently falling back to the timer anyway.
	t.Setenv(githubWorkerTokenEnv, "a-token-that-will-not-work")
	t.Setenv(githubWorkerRepoEnv, "Bytes-Game/does-not-exist-"+t.Name())

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	started, note := startWorkerNow(ctx)
	if started {
		t.Fatal("claimed success against a repo that does not exist")
	}
	// Whatever GitHub said, the note has to carry enough to act on: what was
	// tried, and that uploads have the same problem.
	for _, want := range []string{"does-not-exist", "Uploads"} {
		if !strings.Contains(note, want) {
			t.Errorf("the note is missing %q, so it does not say what to "+
				"check: %q", want, note)
		}
	}
}

func TestRequeue_TheWorkerPokeIsPartOfTheAnswer(t *testing.T) {
	// Re-queueing without starting the worker leaves the videos sitting for
	// the timer, which is the thing this whole endpoint exists to avoid.
	src := readSourceFile(t, "media_requeue.go")
	if !strings.Contains(src, "startWorkerNow(r.Context())") {
		t.Error("the re-queue no longer starts the worker, so everything it " +
			"queues waits for the next scheduled run")
	}
	var resp requeueResponse
	if err := json.Unmarshal([]byte(
		`{"requeued":1,"kind":"challenges","note":"x",`+
			`"workerStarted":true,"workerNote":"y"}`), &resp); err != nil {
		t.Fatal(err)
	}
	if !resp.WorkerStarted || resp.WorkerNote != "y" {
		t.Errorf("the worker outcome does not round-trip: %+v", resp)
	}
}
