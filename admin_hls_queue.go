package main

// admin_hls_queue.go — a window onto the transcode queue.
//
// ════════════════════════════════════════════════════════════════════════════
// WHY
// ════════════════════════════════════════════════════════════════════════════
//
// A video is not watchable until the worker converts it, and until now there
// was no way to ask why one had not been. Every window onto a video hides the
// two things that decide it.
//
// A row's transcode state is one of three:
//
//	''         nobody has it — waiting its turn
//	'PENDING'  a worker is holding it right now
//	a URL      done
//
// Every list the app serves collapses the first two into "no manifest",
// deliberately: 'PENDING' is a claim marker, not a URL, and a client that
// received it would try to play the word. Correct for clients, and it means
// the arena list, the profile and the feed all answer "waiting" and "being
// worked on" identically.
//
// Neither shows hls_attempts, which is the other half. A row that has used
// its five tries is not offered again — so "waiting" and "given up on" also
// look the same from outside.
//
// The cost of that is not hypothetical. Working out why fifteen videos were
// unwatchable took reading the worker's GitHub Actions logs line by line, and
// the answer arrived at from the outside was still wrong: they were reported
// as out of tries, on the strength of the re-queue endpoint skipping them,
// when "skipped" only ever meant "no finished transcode". Two states that
// need completely different actions — wait, or fix the source — and nothing
// anywhere could tell them apart.
//
// This endpoint reads those columns and says which state each row is in. It
// changes nothing. It exists so the next question of this kind is answered by
// looking instead of by inferring.
//
// Admin-only, same door as the re-queue endpoint next to it: this is
// operational detail about somebody's upload, not something a viewer needs.

import (
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"time"
)

// hlsQueueDefaultLimit is what a request that names no size gets — enough to
// see a whole small catalogue at once.
const hlsQueueDefaultLimit = 50

// hlsQueueMaxLimit bounds one request.
const hlsQueueMaxLimit = 500

type hlsQueueRow struct {
	ID   int    `json:"id"`
	Kind string `json:"kind"`
	// State is the plain-language answer: "waiting", "working", "given up",
	// "done", or "no source". See hlsQueueState.
	State string `json:"state"`
	// Why says what has to happen next for a row that is not done.
	Why string `json:"why,omitempty"`
	// Attempts is how many times a worker has claimed it, and Cap the number
	// at which it stops being offered.
	Attempts int `json:"attempts"`
	Cap      int `json:"cap"`
	// ClaimedAt is when a worker last took it, empty if never. For a row that
	// is "working" this is what the reaper measures; for one that failed it is
	// when the cool-off started.
	ClaimedAt string `json:"claimedAt,omitempty"`
	// Ready says whether the worker could claim it right now. A row can be
	// waiting with tries left and still not be offered, because a video rests
	// after a failed try — see hlsRetryCooloff.
	Ready bool `json:"ready"`
	// SourceURL is the video the worker would download. Included because the
	// commonest reason a row never finishes is that this answers 403.
	SourceURL string `json:"sourceUrl,omitempty"`
	// ManifestURL and Variants are what the app would actually PLAY: the HLS
	// master playlist, and the label → URL map of progressive MP4s.
	//
	// Here because checking a finished row meant guessing. The worker names
	// its output directory with eight random hex characters, so nothing can
	// reconstruct these from the id — and every other way to reach them
	// (the arena list, a challenge's own page) either leaves rows out or
	// counts a view. Answering "is this video actually the right length now"
	// took three different endpoints and still could not cover every row.
	//
	// Empty until the worker reports the row finished.
	ManifestURL string            `json:"manifestUrl,omitempty"`
	Variants    map[string]string `json:"variants,omitempty"`
	CreatedAt   string            `json:"createdAt,omitempty"`
}

type hlsQueueResponse struct {
	Counts map[string]int `json:"counts"`
	Rows   []hlsQueueRow  `json:"rows"`
	Note   string         `json:"note"`
}

// AdminHLSQueueHandler answers "why is this video not watchable yet?".
//
//	GET /api/v1/admin/media/queue[?limit=N][&kind=challenges|responses|all]
//	   [&state=waiting|working|given up|done|no source]
//
// Done rows are left out unless asked for by state, because the question this
// answers is about the ones that are not.
func AdminHLSQueueHandler(w http.ResponseWriter, r *http.Request) {
	// Validate BEFORE looking at the database, the same way the re-queue
	// endpoint next door does. A malformed request is malformed whether or
	// not the database happens to be up, and answering "service unavailable"
	// to it sends whoever sent it looking in the wrong place.
	limit := hlsQueueDefaultLimit
	if v := r.URL.Query().Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			http.Error(w, "limit must be a positive whole number", http.StatusBadRequest)
			return
		}
		limit = n
	}
	if limit > hlsQueueMaxLimit {
		limit = hlsQueueMaxLimit
	}

	tables := map[string]string{
		"challenges": "challenges",
		"responses":  "challenge_responses",
	}
	which := r.URL.Query().Get("kind")
	switch which {
	case "", "all":
		// both
	case "challenges", "responses":
		tables = map[string]string{which: tables[which]}
	default:
		http.Error(w, `kind must be "challenges", "responses" or "all"`,
			http.StatusBadRequest)
		return
	}
	wantState := r.URL.Query().Get("state")

	if db == nil {
		http.Error(w, "db unavailable", http.StatusServiceUnavailable)
		return
	}

	counts := map[string]int{}
	rows := []hlsQueueRow{}
	for kind, table := range tables {
		got, err := readHLSQueue(table, kind, limit)
		if err != nil {
			log.Printf("admin hls queue: %s: %v", table, err)
			http.Error(w, "could not read the queue", http.StatusInternalServerError)
			return
		}
		for _, row := range got {
			counts[row.State]++
			if wantState != "" && row.State != wantState {
				continue
			}
			if wantState == "" && row.State == hlsStateDone {
				continue
			}
			rows = append(rows, row)
		}
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(hlsQueueResponse{
		Counts: counts,
		Rows:   rows,
		Note: "counts cover everything looked at; rows leave out finished " +
			`videos unless you ask for state=done`,
	})
}

// The five states a video can be in on its way to being watchable. Written as
// answers rather than as column values, because the question being asked is
// "why can nobody watch this yet".
const (
	hlsStateWaiting  = "waiting"   // in the queue, nobody has it
	hlsStateWorking  = "working"   // a worker is holding it
	hlsStateGivenUp  = "given up"  // used every try, will not be offered again
	hlsStateDone     = "done"      // converted
	hlsStateNoSource = "no source" // no video to convert
)

func readHLSQueue(table, kind string, limit int) ([]hlsQueueRow, error) {
	q := `SELECT id,
	             COALESCE(hls_manifest_url, ''),
	             COALESCE(hls_attempts, 0),
	             hls_claimed_at,
	             COALESCE(video_url, ''),
	             COALESCE(video_variants::text, '{}'),
	             created_at
	        FROM ` + table + `
	       ORDER BY created_at DESC
	       LIMIT $1`
	res, err := db.Query(q, limit)
	if err != nil {
		return nil, err
	}
	defer res.Close()

	out := []hlsQueueRow{}
	for res.Next() {
		var (
			id                      int
			manifest, src, variants string
			attempts                int
			claimed                 sql.NullTime
			created                 time.Time
		)
		if err := res.Scan(&id, &manifest, &attempts, &claimed, &src, &variants, &created); err != nil {
			return nil, err
		}
		row := hlsQueueRow{
			ID: id, Kind: kind,
			Attempts: attempts, Cap: maxHLSAttempts,
			SourceURL: src,
			Variants:  parseVariantMap(variants),
			CreatedAt: created.UTC().Format(time.RFC3339),
		}
		// "PENDING" is the marker a claim writes into the manifest column; it
		// is not a URL and must never be reported as one.
		if manifest != "" && manifest != hlsClaimMarker {
			row.ManifestURL = manifest
		}
		if claimed.Valid {
			row.ClaimedAt = claimed.Time.UTC().Format(time.RFC3339)
		}
		row.State, row.Why, row.Ready = hlsQueueState(manifest, src, attempts, claimed)
		out = append(out, row)
	}
	return out, res.Err()
}

// parseVariantMap turns the video_variants JSONB column into the label → URL
// map the app plays from.
//
// Never fails the request. This endpoint's job is to explain why a video is
// not watchable; refusing to answer because ONE row's column is malformed
// would hide the other fifty-five. A row whose variants cannot be read
// simply reports none, which is also what a row that has none reports.
func parseVariantMap(raw string) map[string]string {
	// No special cases above the parse. An earlier version checked for "",
	// "{}" and "null" first, which read as thorough and was the opposite:
	// those three are exactly what the parse already handles, so the length
	// check below became unreachable and stopped being worth anything. Two
	// guards where one of them can never fire is one guard and some noise.
	out := map[string]string{}
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		return nil // empty column, or something that is not JSON
	}
	if len(out) == 0 {
		return nil // "{}" and "null" both land here
	}
	return out
}

// hlsQueueState turns the three columns into the answer, using exactly the
// conditions the claim query uses. If those two ever disagree this endpoint
// becomes a confident liar, which is worse than having no endpoint — so a
// test holds them together.
func hlsQueueState(manifest, src string, attempts int, claimed sql.NullTime) (state, why string, ready bool) {
	switch {
	case src == "":
		return hlsStateNoSource, "there is no video file to convert", false
	case manifest == hlsClaimMarker:
		return hlsStateWorking,
			"a worker has it; if that worker died it is freed again 30 minutes after it was claimed",
			false
	case manifest != "":
		return hlsStateDone, "", false
	case attempts >= maxHLSAttempts:
		return hlsStateGivenUp,
			"it used all " + strconv.Itoa(maxHLSAttempts) + " tries. Check whether " +
				"the source still downloads; re-queue it by id to give it another go",
			false
	}
	// Waiting — but not necessarily offerable yet, because a video rests
	// after a failed try.
	if claimed.Valid && time.Since(claimed.Time) < hlsRetryCooloff {
		wait := hlsRetryCooloff - time.Since(claimed.Time)
		return hlsStateWaiting,
			"resting after a failed try; the worker can take it again in " +
				wait.Round(time.Second).String(),
			false
	}
	return hlsStateWaiting, "in the queue, waiting for a worker", true
}
