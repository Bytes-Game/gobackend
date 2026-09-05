package main

// media_requeue.go — putting finished videos back through the transcode
// worker, so the ones uploaded before it learned anything get the benefit.
//
// ════════════════════════════════════════════════════════════════════════════
// WHY
// ════════════════════════════════════════════════════════════════════════════
//
// The worker only ever sees a video once. It picks up rows whose
// hls_manifest_url is empty, sets it, and never looks at them again. That is
// right for the normal case and wrong the day the worker gets better at its
// job, because everything already uploaded keeps whatever it got the first
// time.
//
// It got better twice. It now moves an MP4's index to the front, and it makes
// our own progressive renditions instead of serving whatever the source
// happened to be. Neither has reached a single video in production, because
// every video was already marked done.
//
// What that costs, measured on one real imported clip:
//
//	as stored     853x480   10.5 MB   2.68 Mbps
//	our encode    852x480    2.9 MB   0.74 Mbps
//
// 2.68 Mbps at 480p is roughly four times what that picture size needs. The
// app pre-downloads 768 KB of every reel, which at that bitrate is 2.3
// seconds — so the player runs out of buffer almost immediately and streams
// the remaining thirty seconds live while the app is also warming the next
// reels. That is what "the video keeps stopping" is.
//
// At 0.74 Mbps the same 768 KB covers 8.5 seconds.
//
// ════════════════════════════════════════════════════════════════════════════
// WHAT THIS DOES AND DOES NOT TOUCH
// ════════════════════════════════════════════════════════════════════════════
//
// It clears the "already transcoded" mark on a bounded batch of rows. That is
// the whole change. The next worker run finds them the same way it finds a
// fresh upload, and everything downstream — the encode, the skip rule for
// already-lean files, the verification before anything is written back —
// behaves exactly as it does for a new video.
//
// It does NOT delete anything. The old HLS output and the original upload stay
// where they are; new files land beside them under a fresh prefix and the feed
// switches over when the row is updated.
//
// It does NOT touch rows the worker is currently holding ('PENDING') or rows
// still waiting ('') — only ones that finished. So it cannot disturb work in
// flight, and running it twice cannot double-claim anything.
//
// ════════════════════════════════════════════════════════════════════════════
// TWO WAYS TO CHOOSE THE ROWS
// ════════════════════════════════════════════════════════════════════════════
//
// By default it takes the oldest finished rows, which suits the reason above:
// the oldest were transcoded by the oldest worker and have the most to gain,
// and repeated calls walk steadily through the backlog.
//
// Naming ids instead suits the other reason, which turns out to be the common
// one: a bug is fixed and the videos that showed it need to go round again to
// prove it. Those are usually the NEWEST rows, so oldest-first could only
// reach them by re-running the whole catalogue — hours of worker time to
// correct one tag on one video. That is how it went the first time.
//
// Nothing decides quality here. Whether a video is worth re-encoding is a
// question about the file, and only the worker has the file — see
// progressiveSkipBps, which leaves an already-lean source alone.

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/lib/pq"
)

// requeueMaxBatch bounds one call.
//
// Each video costs the worker a download, four HLS renditions and up to two
// progressive ones — roughly forty seconds of runner time. The worker drains
// what it can inside its own runtime cap and the rest waits for the next
// scheduled run, so a large batch is not lost, just spread out. The ceiling is
// here so one mistyped request cannot queue up hours of work.
const requeueMaxBatch = 200

// requeueDefaultBatch is what a request that names no size gets.
const requeueDefaultBatch = 50

type requeueRequest struct {
	// How many videos to put back in the queue. Clamped to requeueMaxBatch.
	// Ignored when IDs is given — a named list already says how many.
	Limit int `json:"limit"`
	// Which table: "challenges" (the video every viewer sees) or "responses"
	// (the opponent side of a battle). Empty means challenges.
	Kind string `json:"kind"`
	// Particular videos to put back, instead of the oldest ones.
	//
	// The batch path walks the backlog oldest-first, which is right for
	// "everything should get the benefit of the newer worker" and useless for
	// the case that actually keeps coming up: a bug is fixed, and the two
	// videos that showed it need to go round again to prove it. Those two are
	// usually the NEWEST rows, so reaching them oldest-first meant re-running
	// the entire catalogue — hours of worker time to correct one tag.
	//
	// A video not in this table, or one the worker is already holding, is
	// reported back in SkippedIDs rather than silently dropped. Asking for a
	// video and being told nothing happened is the whole reason to name it.
	IDs []int `json:"ids"`
}

type requeueResponse struct {
	Requeued int    `json:"requeued"`
	Kind     string `json:"kind"`
	Note     string `json:"note"`
	// Which of the named ids were actually put back, and which were not.
	// Only filled in when the request named ids — the batch path picks its
	// own rows, so listing them would say nothing the count does not.
	RequeuedIDs []int `json:"requeuedIds,omitempty"`
	SkippedIDs  []int `json:"skippedIds,omitempty"`
	// Whether the worker was started immediately, and what happened if not.
	// See startWorkerNow.
	WorkerStarted bool   `json:"workerStarted"`
	WorkerNote    string `json:"workerNote"`
}

// startWorkerNow asks the transcode worker to begin, and says what happened.
//
// ════════════════════════════════════════════════════════════════════════════
// WHY THIS REPORTS INSTEAD OF JUST TRYING
// ════════════════════════════════════════════════════════════════════════════
//
// An upload already does this, through wakeTranscodeWorker — but deliberately
// in the background, swallowing every error into a log line, because an upload
// must never fail or wait because GitHub is slow. That is the right trade for
// an upload and it leaves the feature impossible to check: a token that is
// missing, expired, or scoped wrong behaves EXACTLY like a token that works,
// and the only difference is how long a new video waits to become watchable.
//
// Which is not a small difference. Without it a video waits for the scheduled
// run — nominally half an hour, in practice longer, because GitHub throttles
// cron on repositories with little activity. With it, about a minute.
//
// So this call is synchronous and its result is part of the answer. Re-queuing
// is an admin action nobody is waiting on, so it can afford the fifteen
// seconds an upload cannot, and in exchange the wiring becomes checkable
// without having to post a video and watch the Actions tab.
//
// It is also just correct on its own terms: putting videos in the queue and
// then not starting the worker leaves them sitting there for the timer.
func startWorkerNow(ctx context.Context) (bool, string) {
	token := strings.TrimSpace(os.Getenv(githubWorkerTokenEnv))
	if token == "" {
		return false, githubWorkerTokenEnv + " is not set, so the worker was " +
			"not started early — these will wait for the next scheduled run. " +
			"Uploads wait the same way. Set a fine-grained token with " +
			"Actions: read and write on " +
			envOrDefault(githubWorkerRepoEnv, defaultWorkerRepo) + "."
	}
	if err := dispatchWorkerRun(ctx, token); err != nil {
		return false, "the worker could not be started: " + err.Error() +
			" (repo " + envOrDefault(githubWorkerRepoEnv, defaultWorkerRepo) +
			", workflow " + envOrDefault(githubWorkerWorkflowEnv, defaultWorkerWorkflow) +
			", ref " + envOrDefault(githubWorkerRefEnv, defaultWorkerRef) +
			"). Uploads are failing to start it the same way, silently."
	}
	return true, "the worker was started immediately, so these do not wait " +
		"for the scheduled run — which also confirms uploads can start it."
}

// AdminRequeueMediaHandler puts finished videos back in the transcode queue.
//
// POST /api/v1/admin/media/requeue   {"limit": 50, "kind": "challenges"}
// POST /api/v1/admin/media/requeue   {"ids": [260, 262], "kind": "challenges"}
//
// Without ids: oldest first, deliberately. The oldest rows are the ones
// transcoded by the oldest worker, so they are the ones with the most to gain,
// and repeated calls walk steadily through the backlog instead of re-picking
// the same rows.
//
// With ids: exactly those, in any order, and the answer says which of them
// actually moved.
func AdminRequeueMediaHandler(w http.ResponseWriter, r *http.Request) {
	var req requeueRequest
	// An empty body is a valid request for the default batch, so a decode
	// failure is only an error when there was something there to decode.
	if r.Body != nil {
		_ = json.NewDecoder(r.Body).Decode(&req)
	}

	// Validate BEFORE looking at the database. A malformed request is a bad
	// request whether or not the database happens to be up, and answering
	// "service unavailable" to it sends whoever sent it looking in the wrong
	// place.
	//
	// An "ids" key that is there but empty means the caller built a list and
	// it came out empty. Falling through to "the oldest fifty" would be a
	// surprise nobody asked for, so say so instead. (A missing key decodes to
	// nil, an empty list to a non-nil empty slice — which is the difference
	// this relies on.)
	if req.IDs != nil && len(req.IDs) == 0 {
		http.Error(w, `"ids" was given but empty`, http.StatusBadRequest)
		return
	}
	if len(req.IDs) > requeueMaxBatch {
		http.Error(w, fmt.Sprintf("%d ids is more than the %d a single call "+
			"may queue", len(req.IDs), requeueMaxBatch), http.StatusBadRequest)
		return
	}

	if db == nil {
		http.Error(w, "db unavailable", http.StatusServiceUnavailable)
		return
	}

	limit := req.Limit
	if limit <= 0 {
		limit = requeueDefaultBatch
	}
	if limit > requeueMaxBatch {
		limit = requeueMaxBatch
	}
	table := hlsTableForKind(req.Kind)
	if req.Kind == "responses" {
		table = "challenge_responses"
	}

	var n int64
	var requeuedIDs, skippedIDs []int
	var err error
	if len(req.IDs) > 0 {
		requeuedIDs, err = requeueByID(table, req.IDs)
		if err == nil {
			n = int64(len(requeuedIDs))
			skippedIDs = missingFrom(req.IDs, requeuedIDs)
		}
	} else {
		// hls_attempts goes back to zero as well. It counts failures against a
		// row so a genuinely broken source stops being retried forever; a row
		// that succeeded and is being sent round again deserves a clean slate,
		// and without this a video that failed four times years ago would be
		// offered once and then dropped.
		var res sql.Result
		res, err = db.Exec(`
			UPDATE `+table+`
			   SET hls_manifest_url = '',
			       hls_attempts     = 0,
			       hls_claimed_at    = NULL
			 WHERE id IN (
			   SELECT id FROM `+table+`
			    WHERE hls_manifest_url <> ''
			      AND hls_manifest_url <> 'PENDING'
			      AND COALESCE(video_url, '') <> ''
			    ORDER BY created_at ASC
			    LIMIT $1
			 )`, limit)
		if err == nil {
			n, _ = res.RowsAffected()
		}
	}
	if err != nil {
		log.Printf("admin requeue: %s: %v", table, err)
		http.Error(w, "requeue failed", http.StatusInternalServerError)
		return
	}
	log.Printf("admin requeue: %d %s rows put back in the transcode queue", n, table)
	if len(skippedIDs) > 0 {
		log.Printf("admin requeue: %s ids not put back (unknown, still "+
			"queued, or held by the worker): %v", table, skippedIDs)
	}

	started, workerNote := startWorkerNow(r.Context())
	log.Printf("admin requeue: worker started=%v: %s", started, workerNote)

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(requeueResponse{
		Requeued: int(n),
		Kind:     table,
		Note: "nothing was deleted and the current files keep serving until " +
			"each one is replaced",
		RequeuedIDs:   requeuedIDs,
		SkippedIDs:    skippedIDs,
		WorkerStarted: started,
		WorkerNote:    workerNote,
	})
}

// requeueByID clears the "already transcoded" mark on the named rows and
// reports which ones it actually cleared.
//
// Same guard as the batch path — a row still waiting (”) or held by the
// worker ('PENDING') is left alone, so naming a video cannot disturb work in
// flight. RETURNING is what makes the difference visible: a caller who names
// five videos and gets three back knows two did not move, which is the
// question they were asking by naming them.
func requeueByID(table string, ids []int) ([]int, error) {
	rows, err := db.Query(`
		UPDATE `+table+`
		   SET hls_manifest_url = '',
		       hls_attempts     = 0,
		       hls_claimed_at    = NULL
		 WHERE id = ANY($1)
		   AND hls_manifest_url <> ''
		   AND hls_manifest_url <> 'PENDING'
		   AND COALESCE(video_url, '') <> ''
		RETURNING id`, pq.Array(ids))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	done := []int{}
	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		done = append(done, id)
	}
	return done, rows.Err()
}

// missingFrom returns the entries of want that are not in got, in the order
// they were asked for — the ids a caller named and did not get.
func missingFrom(want, got []int) []int {
	have := make(map[int]bool, len(got))
	for _, id := range got {
		have[id] = true
	}
	var missing []int
	for _, id := range want {
		if !have[id] {
			missing = append(missing, id)
			have[id] = true // an id named twice is reported once
		}
	}
	return missing
}
