package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"strconv"
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

// requeueUpdates returns the two UPDATE statements in media_requeue.go, one
// per path, with their whitespace collapsed.
//
// Split rather than searched, because the two paths are allowed to differ now
// and a whole-file search cannot tell which one it found. Counting matches
// across the file was the previous approach and it would pass happily with a
// guard in one statement and a hole in the other — which is exactly what a
// guard test must not do.
func requeueUpdates(t *testing.T) (batch, byID string) {
	t.Helper()
	src := readSourceFile(t, "media_requeue.go")
	split := strings.Index(src, "func requeueByID(")
	if split < 0 {
		t.Fatal("requeueByID is gone; move these tests with it")
	}
	flat := func(s string) string {
		i := strings.Index(s, "SET hls_manifest_url = ''")
		if i < 0 {
			t.Fatal("no re-queue UPDATE found in one half of media_requeue.go")
		}
		s = s[i:]
		// Stop at the end of the SQL literal, so one path cannot be read as
		// containing a guard that actually lives further down the file.
		j := strings.Index(s, "`,")
		if j < 0 {
			t.Fatal("could not find the end of the re-queue SQL")
		}
		s = s[:j]
		// The table name and the attempt cap are Go expressions spliced into
		// the SQL. Resolve them so the test can talk about the values.
		s = strings.ReplaceAll(s, "`+strconv.Itoa(maxHLSAttempts)+`",
			strconv.Itoa(maxHLSAttempts))
		s = strings.ReplaceAll(s, "`+table+`", "the_table")
		return regexp.MustCompile(`\s+`).ReplaceAllString(s, " ")
	}
	return flat(src[:split]), flat(src[split:])
}

func TestRequeue_NeverDisturbsAVideoAWorkerIsHolding(t *testing.T) {
	// 'PENDING' means a worker has this video right now. Resetting it lets a
	// second worker claim the same video, and the first one's completion
	// callback then writes a manifest over the reset.
	//
	// Source-level because the guard is a WHERE clause, and a WHERE clause
	// that loses a leg still compiles and still returns rows — against real
	// video.
	batch, byID := requeueUpdates(t)
	for name, sql := range map[string]string{"batch": batch, "by id": byID} {
		if !strings.Contains(sql, "hls_manifest_url <> 'PENDING'") {
			t.Errorf("the %s path can reset a video a worker is holding", name)
		}
		if !strings.Contains(sql, "COALESCE(video_url, '') <> ''") {
			t.Errorf("the %s path can queue a row with no source video, "+
				"which can only ever fail", name)
		}
	}
}

func TestRequeue_BatchPathOnlyTakesFinishedVideos(t *testing.T) {
	// The batch path walks the oldest videos so they get the benefit of a
	// better worker. That only makes sense for videos that finished. A video
	// still waiting its turn is already queued, and one that never finished
	// is a different problem — see the by-id path below, which is where
	// somebody names it deliberately.
	batch, _ := requeueUpdates(t)
	if !strings.Contains(batch, "hls_manifest_url <> '' AND") ||
		strings.Contains(batch, "OR hls_attempts") {
		t.Error("the oldest-first batch no longer restricts itself to videos " +
			"that finished, so a routine re-encode sweep can now reach videos " +
			"that are mid-queue or broken")
	}
}

func TestRequeue_ByIDRescuesAVideoTheQueueGaveUpOn(t *testing.T) {
	// This is the case the endpoint exists for and the one it used to refuse.
	//
	// A video with no transcode and no attempts left is stranded: the worker
	// will not offer it again, so nothing plays and nothing retries. Fifteen
	// were in that state in production, and naming every one of them returned
	// "skipped" with no way to do anything about it.
	_, byID := requeueUpdates(t)

	rescue := "(hls_manifest_url <> '' OR hls_attempts >= " +
		strconv.Itoa(maxHLSAttempts) + ")"
	if !strings.Contains(byID, rescue) {
		t.Errorf("naming a video by id cannot rescue one the queue gave up "+
			"on.\n\nexpected the guard to read %s\n              got: %s",
			rescue, byID)
	}

	// The other half of the same guard: a video that is simply waiting its
	// turn must still be left alone. Requeuing it changes nothing and reports
	// success, which tells whoever asked that something happened when nothing
	// did.
	if strings.Contains(byID, "OR TRUE") ||
		!strings.Contains(byID, "hls_attempts >=") {
		t.Error("the by-id path no longer distinguishes a video that is out " +
			"of attempts from one that is merely waiting")
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
// RE-QUEUEING PARTICULAR VIDEOS
// ════════════════════════════════════════════════════════════════════════════
//
// The batch path walks oldest-first, which cannot reach a recent video without
// re-running everything before it. That is how a single wrong tag on one
// recent video came to cost a full catalogue re-run — hours of worker time to
// correct one row.
//
// Naming ids is the way out, and the thing that makes it trustworthy is that
// it says which of the named videos actually moved. A named video that is
// unknown, still waiting, or held by a worker right now does NOT move, and a
// caller who is not told that will sit waiting for a result that is not
// coming.

func TestRequeue_AnEmptyIDListIsARefusalNotABatch(t *testing.T) {
	// The dangerous shape. A caller assembles a list, it comes out empty, and
	// a handler that treats "no ids" as "no ids field" quietly re-queues the
	// oldest fifty videos instead of the zero they asked for.
	w := requeuePost(t, `{"ids":[]}`)
	if w.Code != http.StatusBadRequest {
		t.Errorf("got %d for an empty id list, want 400. Anything else risks "+
			"falling through to the oldest %d videos.", w.Code, requeueDefaultBatch)
	}
}

func TestRequeue_TooManyIDsIsRefused(t *testing.T) {
	// Same ceiling as the batch path, and for the same reason: forty seconds
	// of worker time each.
	ids := make([]string, requeueMaxBatch+1)
	for i := range ids {
		ids[i] = "1"
	}
	w := requeuePost(t, `{"ids":[`+strings.Join(ids, ",")+`]}`)
	if w.Code != http.StatusBadRequest {
		t.Errorf("got %d for %d ids, want 400 — the ceiling is %d",
			w.Code, len(ids), requeueMaxBatch)
	}
}

func TestRequeue_ABadRequestIsNotBlamedOnTheDatabase(t *testing.T) {
	// db is nil under test. A malformed request must still read as malformed:
	// answering "service unavailable" sends whoever sent it to check the
	// database, which is fine, while their request stays broken.
	if w := requeuePost(t, `{"ids":[]}`); w.Code == http.StatusServiceUnavailable {
		t.Error("an empty id list is reported as a database problem")
	}
	// And the reverse still holds — a well-formed request with no database is
	// still a 503.
	if w := requeuePost(t, `{"ids":[260,262]}`); w.Code != http.StatusServiceUnavailable {
		t.Errorf("got %d for a valid request with no database, want 503", w.Code)
	}
}

func TestRequeue_SaysWhichNamedVideosDidNotMove(t *testing.T) {
	// The whole point of naming videos. Ask for four, three come back, and
	// the fourth has to be named — otherwise "requeued: 3" leaves the caller
	// guessing which one they are still waiting for.
	got := missingFrom([]int{260, 262, 999, 1000}, []int{262, 260})
	want := []int{999, 1000}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v (asked-for order)", got, want)
		}
	}
}

func TestRequeue_NothingSkippedWhenEverythingMoved(t *testing.T) {
	// A clean run must report an empty skip list, not a list of everything.
	// omitempty then keeps it out of the response entirely, so "skippedIds"
	// appearing at all means something needs attention.
	if got := missingFrom([]int{1, 2, 3}, []int{3, 2, 1}); len(got) != 0 {
		t.Errorf("got %v, want nothing skipped", got)
	}
}

func TestRequeue_ARepeatedIDIsReportedOnce(t *testing.T) {
	// A caller pasting a list twice should not be told the same video failed
	// twice.
	if got := missingFrom([]int{5, 5, 5}, nil); len(got) != 1 {
		t.Errorf("got %v, want [5] once", got)
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
