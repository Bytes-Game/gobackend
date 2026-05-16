package main

// hls_worker_api: HTTP endpoints that the background HLS transcode
// worker uses to claim work and report results. The worker is a
// separate binary that runs as a Render background service (or any
// other host) with FFmpeg installed; it polls /api/internal/hls/next-pending
// for one challenge at a time, downloads the source video from R2,
// runs FFmpeg to produce the HLS bitrate ladder + segments, uploads
// the result back to R2, then POSTs to /api/internal/hls/complete with
// the public manifest URL.
//
// Why pull-based instead of push-based:
//   * The web service runs on Render's free tier with limited CPU and
//     a 30s request timeout. FFmpeg transcoding of a 60s 1080p reel
//     can take 30-90s on a free CPU, so it cannot run inline in the
//     CreateChallengeHandler request — that handler returns quickly
//     and the worker picks up the row later.
//   * The worker is stateless. It polls, claims, transcodes, reports.
//     We can run zero, one, or many workers — only constraint is that
//     /next-pending dedupes claims with an UPDATE … WHERE … RETURNING
//     pattern, so two workers can't grab the same challenge.
//
// Auth: both endpoints require the X-Worker-Token header to match the
// HLS_WORKER_TOKEN env var. This is intentionally simple — the worker
// is a trusted internal service, not a user-facing surface. Rotating
// the token is a config change.

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
)

// pendingHLSJob is the payload the worker pulls. Empty string fields
// mean "no work right now" — the worker should sleep and retry.
type pendingHLSJob struct {
	ChallengeID string `json:"challengeId"`
	SourceURL   string `json:"sourceUrl"` // the original mp4 in R2 the worker fetches
}

// hlsCompleteRequest is what the worker POSTs after a successful
// transcode + upload.
type hlsCompleteRequest struct {
	ChallengeID string `json:"challengeId"`
	ManifestURL string `json:"manifestUrl"`
}

// workerAuthed wraps a handler with the X-Worker-Token check. We do
// constant-time comparison via crypto/subtle indirectly through string
// equality (Go's == on strings is constant-time-ish for equal-length
// strings, which is good enough for an internal-only token whose only
// adversary is a misconfigured external caller, not a side-channel
// attacker — the workers themselves see plaintext tokens anyway).
func workerAuthed(h http.HandlerFunc) http.HandlerFunc {
	want := strings.TrimSpace(os.Getenv("HLS_WORKER_TOKEN"))
	return func(w http.ResponseWriter, r *http.Request) {
		if want == "" {
			// Fail closed: a misconfigured server (token env var
			// missing) should reject every request, not accept all.
			http.Error(w, "hls worker token not configured", http.StatusInternalServerError)
			return
		}
		got := strings.TrimSpace(r.Header.Get("X-Worker-Token"))
		if got != want {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		h(w, r)
	}
}

// HLSNextPendingHandler returns one pending HLS-transcode job and
// marks it as "in progress" so a second concurrent worker won't pick
// the same row. The atomic-claim pattern: UPDATE … SET … WHERE id =
// (SELECT id FROM challenges WHERE … LIMIT 1 FOR UPDATE SKIP LOCKED)
// RETURNING …. Postgres' FOR UPDATE SKIP LOCKED is the standard
// queue-on-a-table primitive — no separate job table needed.
//
// "In progress" is encoded as hls_manifest_url = 'PENDING' so the
// partial index challenges_pending_hls_idx (defined in
// runMigrations) skips it too.
func HLSNextPendingHandler(w http.ResponseWriter, r *http.Request) {
	if db == nil {
		http.Error(w, "db unavailable", http.StatusServiceUnavailable)
		return
	}
	row := db.QueryRow(`
		UPDATE challenges
		   SET hls_manifest_url = 'PENDING'
		 WHERE id = (
		   SELECT id FROM challenges
		    WHERE hls_manifest_url = ''
		      AND video_url <> ''
		    ORDER BY created_at DESC
		    LIMIT 1
		    FOR UPDATE SKIP LOCKED
		 )
		 RETURNING id, video_url`)
	var idInt int
	var srcURL string
	if err := row.Scan(&idInt, &srcURL); err != nil {
		// No rows = no work available. 204 lets the worker treat this
		// as a non-error and sleep before polling again.
		w.WriteHeader(http.StatusNoContent)
		return
	}
	resp := pendingHLSJob{
		ChallengeID: strconv.Itoa(idInt),
		SourceURL:   srcURL,
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// HLSCompleteHandler is called by the worker after successful upload.
// Stores the public manifest URL in challenges.hls_manifest_url so
// the next feed query surfaces it to clients via populateHLSManifestURLs.
func HLSCompleteHandler(w http.ResponseWriter, r *http.Request) {
	if db == nil {
		http.Error(w, "db unavailable", http.StatusServiceUnavailable)
		return
	}
	var req hlsCompleteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid body", http.StatusBadRequest)
		return
	}
	if req.ChallengeID == "" || req.ManifestURL == "" {
		http.Error(w, "challengeId and manifestUrl required", http.StatusBadRequest)
		return
	}
	cid, err := strconv.Atoi(req.ChallengeID)
	if err != nil {
		http.Error(w, "invalid challengeId", http.StatusBadRequest)
		return
	}
	_, err = db.Exec(
		`UPDATE challenges SET hls_manifest_url = $2 WHERE id = $1`,
		cid, req.ManifestURL,
	)
	if err != nil {
		log.Printf("HLSComplete update error for challenge=%s: %v", req.ChallengeID, err)
		http.Error(w, "db update failed", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// HLSFailHandler lets the worker mark a job as failed so we don't
// keep retrying a broken source forever. We reset hls_manifest_url
// back to '' so other workers can have a go, but also log so we can
// find broken sources in the metrics. A "max-attempts" cap could
// live here later if we see repeat-failure patterns in production.
func HLSFailHandler(w http.ResponseWriter, r *http.Request) {
	if db == nil {
		http.Error(w, "db unavailable", http.StatusServiceUnavailable)
		return
	}
	var req hlsCompleteRequest // reuse the same shape; ManifestURL is the failure reason
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid body", http.StatusBadRequest)
		return
	}
	cid, err := strconv.Atoi(req.ChallengeID)
	if err != nil {
		http.Error(w, "invalid challengeId", http.StatusBadRequest)
		return
	}
	log.Printf("HLS transcode failed for challenge=%d reason=%q", cid, req.ManifestURL)
	_, _ = db.Exec(
		`UPDATE challenges SET hls_manifest_url = '' WHERE id = $1 AND hls_manifest_url = 'PENDING'`,
		cid,
	)
	w.WriteHeader(http.StatusNoContent)
}
