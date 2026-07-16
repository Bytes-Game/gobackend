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
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

// maxHLSAttempts bounds how many times a source video gets claimed for
// transcoding before we give up on it. Attempts are counted at CLAIM
// time (not fail time) so worker crashes mid-job count too — a corrupt
// mp4 or a codec FFmpeg can't read stops consuming worker cycles after
// this many tries instead of being re-claimed and re-failed forever.
const maxHLSAttempts = 5

// hlsKindResponse marks a job/report as targeting the
// challenge_responses table; anything else (including the empty string
// older workers send) means the challenges table. Battle responses get
// the same adaptive-bitrate treatment as the primary video — before
// this, the opponent leg of every battle played the raw MP4 only.
const hlsKindResponse = "response"

// pendingHLSJob is the payload the worker pulls. Empty string fields
// mean "no work right now" — the worker should sleep and retry.
type pendingHLSJob struct {
	ChallengeID string `json:"challengeId"` // row id in the kind's table (name kept for wire compat)
	SourceURL   string `json:"sourceUrl"`   // the original mp4 in R2 the worker fetches
	Kind        string `json:"kind"`        // "challenge" (default) | "response"
	// PublicBaseURL is the host clients fetch media from — the backend's
	// R2_PUBLIC_BASE_URL. Sent with every job so the worker builds
	// manifest URLs from the SAME base the rest of the app uses. Without
	// it, a worker whose optional R2_PUBLIC_BASE_URL env was unset used
	// to fabricate pub-<ACCOUNT_ID>.r2.dev/<bucket> — a URL shape that
	// never resolves (the real pub-*.r2.dev hash is random per bucket),
	// which 401'd every HLS stream in the player.
	PublicBaseURL string `json:"publicBaseUrl,omitempty"`
}

// hlsCompleteRequest is what the worker POSTs after a successful
// transcode + upload.
type hlsCompleteRequest struct {
	ChallengeID string `json:"challengeId"`
	ManifestURL string `json:"manifestUrl"`
	Kind        string `json:"kind"` // "" / "challenge" | "response"
}

// hlsTableForKind maps the wire kind to the table whose
// hls_manifest_url state machine the request drives. Defaulting to
// challenges keeps already-deployed workers (which don't send kind)
// working unchanged.
func hlsTableForKind(kind string) string {
	if kind == hlsKindResponse {
		return "challenge_responses"
	}
	return "challenges"
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
	// Challenges first (the primary video every viewer sees), then
	// battle responses. Two cheap indexed probes instead of a UNION so
	// each table keeps its own partial index and the common case (no
	// backlog) stays a single no-op query.
	//
	// hls_attempts is incremented AT CLAIM so crashes count as attempts;
	// rows that reach maxHLSAttempts stop being offered. hls_claimed_at
	// lets startHLSReaper reset jobs orphaned by a worker that died
	// mid-transcode (state stuck at 'PENDING').
	claim := func(table string) (int, string, bool) {
		row := db.QueryRow(`
			UPDATE ` + table + `
			   SET hls_manifest_url = 'PENDING',
			       hls_claimed_at   = NOW(),
			       hls_attempts     = hls_attempts + 1
			 WHERE id = (
			   SELECT id FROM ` + table + `
			    WHERE hls_manifest_url = ''
			      AND video_url <> ''
			      AND hls_attempts < ` + strconv.Itoa(maxHLSAttempts) + `
			    ORDER BY created_at DESC
			    LIMIT 1
			    FOR UPDATE SKIP LOCKED
			 )
			 RETURNING id, video_url`)
		var idInt int
		var srcURL string
		if err := row.Scan(&idInt, &srcURL); err != nil {
			return 0, "", false
		}
		return idInt, srcURL, true
	}

	publicBase := ""
	if cfg, err := loadR2Config(); err == nil {
		publicBase = cfg.PublicBaseURL
	}

	if id, src, ok := claim("challenges"); ok {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(pendingHLSJob{
			ChallengeID: strconv.Itoa(id), SourceURL: src, Kind: "challenge",
			PublicBaseURL: publicBase,
		})
		return
	}
	if id, src, ok := claim("challenge_responses"); ok {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(pendingHLSJob{
			ChallengeID: strconv.Itoa(id), SourceURL: src, Kind: hlsKindResponse,
			PublicBaseURL: publicBase,
		})
		return
	}
	// No rows = no work available. 204 lets the worker treat this
	// as a non-error and sleep before polling again.
	w.WriteHeader(http.StatusNoContent)
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
	table := hlsTableForKind(req.Kind)
	_, err = db.Exec(
		`UPDATE `+table+` SET hls_manifest_url = $2 WHERE id = $1`,
		cid, req.ManifestURL,
	)
	if err != nil {
		log.Printf("HLSComplete update error for %s=%s: %v", table, req.ChallengeID, err)
		http.Error(w, "db update failed", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// HLSFailHandler lets the worker mark a job as failed. We reset
// hls_manifest_url back to '' so another attempt can claim it — but
// only while hls_attempts stays under maxHLSAttempts (counted at claim
// time in HLSNextPendingHandler), so a genuinely broken source stops
// being retried after the cap instead of looping forever.
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
	table := hlsTableForKind(req.Kind)
	log.Printf("HLS transcode failed for %s=%d reason=%q", table, cid, req.ManifestURL)
	_, _ = db.Exec(
		`UPDATE `+table+` SET hls_manifest_url = '' WHERE id = $1 AND hls_manifest_url = 'PENDING'`,
		cid,
	)
	w.WriteHeader(http.StatusNoContent)
}

// healHLSManifestURLs rewrites manifest URLs written by workers whose
// optional R2_PUBLIC_BASE_URL env was unset: those fabricated a base of
// https://pub-<ACCOUNT_ID>.r2.dev/<bucket>, a URL shape that never
// resolves on Cloudflare (the real pub-*.r2.dev subdomain is a random
// per-bucket hash, and public dev URLs don't take a bucket path
// segment) — so every affected row 401'd in the player while the
// underlying HLS files sat perfectly fine in the bucket. One-shot at
// boot: swap the fabricated prefix for the backend's authoritative
// public base, which points at the same objects.
func healHLSManifestURLs() {
	if db == nil {
		return
	}
	cfg, err := loadR2Config()
	if err != nil || cfg.PublicBaseURL == "" {
		return
	}
	fabricated := fmt.Sprintf("https://pub-%s.r2.dev/%s/", cfg.AccountID, cfg.Bucket)
	right := cfg.PublicBaseURL + "/"
	if fabricated == right {
		// The backend's own env still points at the fabricated shape —
		// nothing better to rewrite to.
		return
	}
	for _, table := range []string{"challenges", "challenge_responses"} {
		res, err := db.Exec(
			`UPDATE `+table+` SET hls_manifest_url = replace(hls_manifest_url, $1, $2)
			  WHERE hls_manifest_url LIKE $3`,
			fabricated, right, fabricated+"%",
		)
		if err != nil {
			log.Printf("hls url heal %s error: %v", table, err)
			continue
		}
		if n, _ := res.RowsAffected(); n > 0 {
			log.Printf("hls url heal: fixed %d row(s) in %s", n, table)
		}
	}
}

// startHLSReaper resets jobs orphaned in the 'PENDING' state — a worker
// that crashed (or was redeployed) mid-transcode never reports
// complete/fail, and without this sweep the row is invisible to
// next-pending forever. 30 minutes is ~20x a normal 60s-reel transcode,
// so a reset here virtually never races a live job; if it somehow does,
// the duplicate transcode is idempotent (last complete wins, R2 keys
// are random-prefixed per attempt).
//
// Rows that already burned maxHLSAttempts stay reset-to-'' but are
// never re-offered by the claim query's attempts filter.
func startHLSReaper() {
	go func() {
		ticker := time.NewTicker(10 * time.Minute)
		defer ticker.Stop()
		for range ticker.C {
			if db == nil {
				continue
			}
			for _, table := range []string{"challenges", "challenge_responses"} {
				res, err := db.Exec(`
					UPDATE ` + table + `
					   SET hls_manifest_url = ''
					 WHERE hls_manifest_url = 'PENDING'
					   AND hls_claimed_at < NOW() - INTERVAL '30 minutes'`)
				if err != nil {
					log.Printf("hls reaper %s error: %v", table, err)
					continue
				}
				if n, _ := res.RowsAffected(); n > 0 {
					log.Printf("hls reaper: reset %d stuck PENDING row(s) in %s", n, table)
				}
			}
		}
	}()
}
