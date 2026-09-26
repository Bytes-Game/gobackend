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
// The third way is "missing": name a rendition, and only videos that do not
// already have it are picked. That is what a NEW RUNG needs.
//
// 360p was added to the ladder for people on mobile data, and every video
// already on the platform was encoded before it existed. Oldest-first would
// get there eventually, but it re-encodes everything on the way — including
// the videos that already have the rung — so a catalogue of a few hundred
// costs hours of runner time to add one small file to each.
//
// "missing" asks the question the job is actually about, and it is safe to
// run over and over: a video that has been done drops out of the selection,
// so repeated calls walk through what is left and then stop finding anything.
//
// "Done" is not the same as "has it". Some videos rightly never get a given
// rendition: no 720p copy of a 480p video, no H.265 copy that would be no
// smaller than the H.264 one. Picking on "does not have it" alone sends those
// round again every time, and because the pick is oldest-first it is the SAME
// videos every round — the job never finishes and the ones behind them never
// get a turn. So a video also drops out once a conversion has considered the
// rendition (hls_ladder, written by storeLadder), whether or not it made one.
//
// Nothing decides quality here. Whether a video is worth re-encoding is a
// question about the file, and only the worker has the file — see
// planProgressive in cmd/hls-worker, which never aims a rung above what an
// already-lean source spent.

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/lib/pq"
)

// requeueMaxBatch bounds one call.
//
// Each video costs the worker a download, four HLS renditions and up to four
// progressive ones — roughly a minute of runner time. The worker drains
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
	// Only pick videos that do NOT already have this rendition.
	//
	// The case this is for: a rung is added to the ladder and every video
	// already on the platform predates it. Without this the only way to reach
	// them is the oldest-first walk, which re-encodes the whole catalogue to
	// add one small file to each video — and repeats that work every time it
	// is run, because nothing in the selection knows what has been done.
	//
	// With it the selection shrinks as the job progresses, so calling this
	// repeatedly finishes the backlog and then starts returning zero.
	//
	// Must be a label the backend would actually store (videoVariantLabels),
	// both because anything else can never be produced — so the request could
	// only ever queue the entire catalogue — and because this value reaches
	// the database.
	//
	// Ignored when IDs is given: a named list has already said which videos.
	Missing string `json:"missing"`
	// Count what is left and change nothing.
	//
	// ══════════════════════════════════════════════════════════════════
	// WHY A LIMIT OF ZERO WAS NOT THIS
	// ══════════════════════════════════════════════════════════════════
	//
	// The obvious way to ask "how many are there?" is to request none of
	// them. It does not work here: a limit of zero means "the caller named
	// no size", and falls through to the default batch of fifty. That is
	// the right reading of an empty request body, and it is a trap for
	// anybody who meant zero literally.
	//
	// It caught the backfill workflow's own dry run, which reported
	// "queueing nothing" and queued forty-four videos. Nothing was harmed —
	// re-queuing is safe and those were the videos we wanted anyway — but a
	// switch labelled "change nothing" that changes something is worth more
	// than the one line it saves.
	//
	// So asking is its own flag, and it cannot be confused with a number.
	// Only valid with Missing, because counting with nothing to count is
	// not a question.
	CountOnly bool `json:"countOnly"`
}

type requeueResponse struct {
	Requeued int    `json:"requeued"`
	Kind     string `json:"kind"`
	Note     string `json:"note"`
	// The rendition asked for, echoed back. Empty for an ordinary batch.
	// Without it a run of zero is ambiguous: "the backlog is finished" and
	// "the filter was ignored" look the same in a bare count.
	Missing string `json:"missing,omitempty"`
	// How many finished videos still lack that rendition AFTER this call.
	// The point of a backlog job is knowing when it is over, and a count of
	// rows moved does not say that. Zero here means done.
	//
	// A pointer, because zero is the answer that matters most and a plain
	// int cannot say it. "The backlog is finished" and "this sweep never
	// counted anything" are both 0, and they mean opposite things: stop
	// running this, versus this number is not about you.
	//
	// So: absent means "not asked for", and 0 means "none left".
	StillMissing *int `json:"stillMissing"`
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
// POST /api/v1/admin/media/requeue   {"limit": 50, "missing": "360p"}
//
// Without ids: oldest first, deliberately. The oldest rows are the ones
// transcoded by the oldest worker, so they are the ones with the most to gain,
// and repeated calls walk steadily through the backlog instead of re-picking
// the same rows.
//
// With ids: exactly those, in any order, and the answer says which of them
// actually moved.
//
// With missing: only videos that do not already have that rendition, oldest
// first. The answer carries stillMissing, which is how many are left after
// this call — run it again until that reaches zero.
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
	// A rendition nobody could ever store would match every row, so the
	// request would quietly become "re-encode the whole catalogue" — the
	// opposite of what naming a rendition is for. Refuse it instead.
	req.Missing = strings.TrimSpace(req.Missing)
	if req.CountOnly && req.Missing == "" {
		http.Error(w, `"countOnly" needs "missing" — there is nothing to count `+
			`without a rendition to look for`, http.StatusBadRequest)
		return
	}
	if req.CountOnly && len(req.IDs) > 0 {
		http.Error(w, `"countOnly" and "ids" ask for different things: one `+
			`counts, the other queues named videos`, http.StatusBadRequest)
		return
	}
	if req.Missing != "" && !videoVariantLabels[req.Missing] {
		http.Error(w, fmt.Sprintf("%q is not a rendition this backend stores; "+
			"every video would match it", req.Missing), http.StatusBadRequest)
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
	if req.CountOnly {
		// Nothing to do. The count below is the whole answer.
	} else if len(req.IDs) > 0 {
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
		if req.Missing != "" {
			// Oldest first among the ones that still lack the rendition.
			//
			// ->> 'label' IS NULL rather than the key-exists operator: `?`
			// is also a placeholder character in a lot of database tooling,
			// and a query that reads differently depending on who is looking
			// at it is not worth the two characters it saves.
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
				      AND COALESCE(video_variants, '{}'::jsonb)->>$2::text IS NULL
				      AND NOT COALESCE(hls_ladder, '[]'::jsonb) @> jsonb_build_array($2::text)
				    ORDER BY created_at ASC
				    LIMIT $1
				 )`, limit, req.Missing)
		} else {
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
		}
		if err == nil {
			n, _ = res.RowsAffected()
		}
	}
	if err != nil {
		log.Printf("admin requeue: %s: %v", table, err)
		http.Error(w, "requeue failed", http.StatusInternalServerError)
		return
	}
	// How much of this backlog is LEFT TO QUEUE. Asked after the update, so
	// the rows just moved are not counted — they are queued, not waiting.
	//
	// A count of rows moved cannot say whether the job is finished: fifty
	// moved looks the same on the first call and the last. This is the number
	// that says when to stop calling.
	//
	// It is deliberately not "videos without this rendition". A video the
	// worker is part-way through has no rendition yet and is not counted,
	// because queueing it again would be wrong. So zero means "nothing left
	// to hand over", not "every video now has one" — those become the same
	// thing only once the worker has drained.
	var stillMissing *int
	if req.Missing != "" {
		left := 0
		if cErr := db.QueryRow(`
			SELECT COUNT(*) FROM `+table+`
			 WHERE hls_manifest_url <> ''
			   AND hls_manifest_url <> 'PENDING'
			   AND COALESCE(video_url, '') <> ''
			   AND COALESCE(video_variants, '{}'::jsonb)->>$1::text IS NULL
			   AND NOT COALESCE(hls_ladder, '[]'::jsonb) @> jsonb_build_array($1::text)
			`, req.Missing).Scan(&left); cErr != nil {
			// Not fatal — the rows really were queued. But it must not read
			// as "nothing left", which is exactly what a swallowed error here
			// would look like to whoever is running the backlog.
			queryFailed("how many videos still lack the "+req.Missing+
				" rendition", "reporting -1 so the count is not mistaken "+
				"for a finished backlog", cErr)
			left = -1
		}
		stillMissing = &left
	}

	log.Printf("admin requeue: %d %s rows put back in the transcode queue", n, table)
	// Guarded on the pointer itself, not on req.Missing.
	//
	// The two say the same thing today — the count is only taken when a
	// rendition was named — and that is the problem: it is true because two
	// separate blocks agree, not because anything makes them agree. Reading
	// the pointer through a condition somewhere else is how a nil
	// dereference gets introduced later by an edit that looks harmless.
	if stillMissing != nil {
		log.Printf("admin requeue: %s rows still without a %s rendition: %d",
			table, req.Missing, *stillMissing)
	}
	if len(skippedIDs) > 0 {
		log.Printf("admin requeue: %s ids not put back (unknown, still "+
			"queued, or held by the worker): %v", table, skippedIDs)
	}

	// A question does not wake the worker. Counting queues nothing, so
	// there is nothing new for it to do, and starting it would burn a runner
	// to find an empty queue.
	started, workerNote := false, ""
	if !req.CountOnly {
		started, workerNote = startWorkerNow(r.Context())
		log.Printf("admin requeue: worker started=%v: %s", started, workerNote)
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(requeueResponse{
		Requeued: int(n),
		Kind:     table,
		Note: "nothing was deleted and the current files keep serving until " +
			"each one is replaced",
		Missing:       req.Missing,
		StillMissing:  stillMissing,
		RequeuedIDs:   requeuedIDs,
		SkippedIDs:    skippedIDs,
		WorkerStarted: started,
		WorkerNote:    workerNote,
	})
}

// requeueByID clears the "already transcoded" mark on the named rows and
// reports which ones it actually cleared.
//
// It will move a row in one of two states, and only those two.
//
// The first is a video that FINISHED — same as the batch path. That is the
// re-encode case: the worker got better, send it round again.
//
// The second is a video the queue has GIVEN UP ON: no transcode was ever
// produced and it has used every attempt it is allowed. That row is stranded.
// The worker will not offer it again (the claim query skips rows at the cap)
// and nothing else was willing to touch it either, so it sat there with no
// video anyone could play and no way back. Naming a stranded video is the
// single most likely reason anybody calls this endpoint by id, and until now
// it was the one thing the endpoint refused to do:
//
//	{"requeued":1,"requeuedIds":[8],
//	 "skippedIds":[2,4,9,10,11,27,28,29,30,31,32,33,34,35,46]}
//
// Fifteen videos, every one of them invisible in the app, every one of them
// reported back as "not moved" with no way to move it. The automatic rescue
// in InitDatabase does eventually free them, but only 24 hours after the last
// attempt AND only when the backend happens to restart. That is the right
// safety net for an infrastructure blip and much too slow to be an answer to
// somebody standing there asking for a video back.
//
// Everything else is still left alone, deliberately:
//
//   - 'PENDING' means a worker is holding it right now. Resetting it would
//     let a second worker claim the same video, and the first one's
//     completion callback would then write a manifest over the reset.
//   - An empty manifest with attempts still on the clock means the row is
//     already queued and its turn is coming. Nothing to fix, so nothing to
//     do, and saying "requeued" would be a lie.
//   - No source video at all can never transcode.
//
// RETURNING is what makes the difference visible: a caller who names five
// videos and gets three back knows two did not move, which is the question
// they were asking by naming them.
func requeueByID(table string, ids []int) ([]int, error) {
	rows, err := db.Query(`
		UPDATE `+table+`
		   SET hls_manifest_url = '',
		       hls_attempts     = 0,
		       hls_claimed_at    = NULL
		 WHERE id = ANY($1)
		   AND hls_manifest_url <> 'PENDING'
		   AND COALESCE(video_url, '') <> ''
		   AND (hls_manifest_url <> ''
		        OR hls_attempts >= `+strconv.Itoa(maxHLSAttempts)+`)
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
