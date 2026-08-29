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
// Nothing decides quality here. Whether a video is worth re-encoding is a
// question about the file, and only the worker has the file — see
// progressiveSkipBps, which leaves an already-lean source alone.

import (
	"encoding/json"
	"log"
	"net/http"
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
	Limit int `json:"limit"`
	// Which table: "challenges" (the video every viewer sees) or "responses"
	// (the opponent side of a battle). Empty means challenges.
	Kind string `json:"kind"`
}

type requeueResponse struct {
	Requeued int    `json:"requeued"`
	Kind     string `json:"kind"`
	Note     string `json:"note"`
}

// AdminRequeueMediaHandler puts finished videos back in the transcode queue.
//
// POST /api/v1/admin/media/requeue   {"limit": 50, "kind": "challenges"}
//
// Oldest first, deliberately: the oldest rows are the ones transcoded by the
// oldest worker, so they are the ones with the most to gain. It also makes
// repeated calls walk steadily through the backlog instead of re-picking the
// same rows.
func AdminRequeueMediaHandler(w http.ResponseWriter, r *http.Request) {
	if db == nil {
		http.Error(w, "db unavailable", http.StatusServiceUnavailable)
		return
	}
	var req requeueRequest
	// An empty body is a valid request for the default batch, so a decode
	// failure is only an error when there was something there to decode.
	if r.Body != nil {
		_ = json.NewDecoder(r.Body).Decode(&req)
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

	// hls_attempts goes back to zero as well. It counts failures against a
	// row so a genuinely broken source stops being retried forever; a row
	// that succeeded and is being sent round again deserves a clean slate,
	// and without this a video that failed four times years ago would be
	// offered once and then dropped.
	res, err := db.Exec(`
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
	if err != nil {
		log.Printf("admin requeue: %s: %v", table, err)
		http.Error(w, "requeue failed", http.StatusInternalServerError)
		return
	}
	n, _ := res.RowsAffected()
	log.Printf("admin requeue: %d %s rows put back in the transcode queue", n, table)

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(requeueResponse{
		Requeued: int(n),
		Kind:     table,
		Note: "the next worker run picks these up; nothing was deleted and " +
			"the current files keep serving until each one is replaced",
	})
}
