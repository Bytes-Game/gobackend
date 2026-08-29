package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
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
