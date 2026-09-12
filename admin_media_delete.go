package main

// admin_media_delete.go — deleting a video that is not yours to delete.
//
// ════════════════════════════════════════════════════════════════════════════
// WHY THIS EXISTS
// ════════════════════════════════════════════════════════════════════════════
//
// A creator can delete their own video. Nobody could delete anybody else's.
//
// DeleteChallengeHandler says in its own comment that "admin / moderator
// delete uses a separate endpoint with its own auth path". That endpoint was
// never written. DeleteChallengeByID says it is safe for "internal callers
// (admin moderation, scheduled GC)" to use directly, and nothing did.
//
// So the case that actually comes up had no answer at all: four videos in
// production point at a source that answers 403 and can never be converted.
// They sit in the arena forever, unwatchable, run through the retry cap over
// and over, and belong to four different accounts — so the creator-only path
// would have needed four people's passwords.
//
// ════════════════════════════════════════════════════════════════════════════
// WHAT MAKES THIS SAFE ENOUGH TO EXIST
// ════════════════════════════════════════════════════════════════════════════
//
// This is the only irreversible endpoint in the app, so the shape matters
// more than the code.
//
// IT ONLY EVER DELETES VIDEOS YOU NAME. There is no "delete the oldest
// fifty", no filter, no match-by-anything. Every other admin endpoint here
// has a batch mode and this one deliberately does not: a batch delete is one
// mistyped number away from emptying the app, and no amount of care at the
// call site fixes that.
//
// IT TELLS YOU WHAT ELSE WENT. Deleting a battle cascades into the answers
// people posted to it — somebody else's video, gone as collateral, with
// nothing in the reply saying so. It is counted BEFORE the delete and
// reported per video, because after the delete there is nothing left to
// count and the question "how much did I just destroy" has no answer.
//
// IT IS BOUNDED. A small ceiling, well under the re-queue endpoint's, because
// the cost of getting a re-queue wrong is some wasted runner time.
//
// It does NOT clear the storage bucket inline. DeleteChallengeByID reads
// where every file lives — the video's own and every answer's — and queues
// them for the background cleaner, which is the same path a creator's own
// delete takes.

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strconv"
)

// adminDeleteMaxBatch bounds one call. Deliberately small: this is the one
// thing here that cannot be undone.
const adminDeleteMaxBatch = 25

type adminDeleteRequest struct {
	// Which videos to delete. Required — there is no other way to choose.
	IDs []int `json:"ids"`
	// "challenges" (default). Named for symmetry with the re-queue endpoint
	// so the two are not subtly different to call.
	Kind string `json:"kind"`
	// Must be exactly "yes, delete these permanently". A request that reaches
	// this endpoint by accident — a retried curl, a wrong path in a script —
	// does nothing without it.
	Confirm string `json:"confirm"`
}

const adminDeleteConfirmPhrase = "yes, delete these permanently"

type adminDeletedVideo struct {
	ID int `json:"id"`
	// Responses is how many answer videos went with it. Somebody else's
	// upload, so it is reported rather than left to be discovered.
	Responses int `json:"responsesAlsoDeleted"`
}

type adminDeleteResponse struct {
	Deleted []adminDeletedVideo `json:"deleted"`
	// NotFound are ids that named nothing. Reported rather than treated as
	// success, because "I deleted 4" when one id was a typo is a lie that
	// reads as a confirmation.
	NotFound []int  `json:"notFound,omitempty"`
	Failed   []int  `json:"failed,omitempty"`
	Note     string `json:"note"`
}

// AdminDeleteMediaHandler permanently deletes the videos it is given.
//
//	POST /api/v1/admin/media/delete
//	{"ids": [11, 28, 30, 46], "confirm": "yes, delete these permanently"}
func AdminDeleteMediaHandler(w http.ResponseWriter, r *http.Request) {
	var req adminDeleteRequest
	if r.Body != nil {
		_ = json.NewDecoder(r.Body).Decode(&req)
	}

	// Validated before the database is touched, so a malformed request is
	// answered as malformed whether or not the database happens to be up.
	if len(req.IDs) == 0 {
		http.Error(w, `"ids" is required — this endpoint only ever deletes `+
			`videos you name, one list at a time`, http.StatusBadRequest)
		return
	}
	if len(req.IDs) > adminDeleteMaxBatch {
		http.Error(w, fmt.Sprintf("%d ids is more than the %d a single call "+
			"may delete", len(req.IDs), adminDeleteMaxBatch),
			http.StatusBadRequest)
		return
	}
	if req.Confirm != adminDeleteConfirmPhrase {
		http.Error(w, `"confirm" must be exactly `+
			strconv.Quote(adminDeleteConfirmPhrase)+
			` — this cannot be undone`, http.StatusBadRequest)
		return
	}
	if req.Kind != "" && req.Kind != "challenges" {
		http.Error(w, `"kind" must be "challenges"`, http.StatusBadRequest)
		return
	}

	if db == nil {
		http.Error(w, "db unavailable", http.StatusServiceUnavailable)
		return
	}

	out := adminDeleteResponse{
		Deleted: []adminDeletedVideo{},
		Note: "the videos are gone and their files are queued for removal " +
			"from storage; answers posted to a battle went with it",
	}
	for _, id := range req.IDs {
		// Counted first. After the delete there is nothing left to count, and
		// how much went is the thing worth knowing.
		responses := countChallengeResponses(id)

		if err := DeleteChallengeByID(strconv.Itoa(id)); err != nil {
			if isNoSuchChallenge(err) {
				out.NotFound = append(out.NotFound, id)
				continue
			}
			log.Printf("admin delete: challenge %d: %v", id, err)
			out.Failed = append(out.Failed, id)
			continue
		}
		log.Printf("admin delete: challenge %d deleted (%d response(s) with it)",
			id, responses)
		out.Deleted = append(out.Deleted, adminDeletedVideo{ID: id, Responses: responses})
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}

// countChallengeResponses reports how many answer videos are attached to a
// challenge, so a delete can say what it took with it.
func countChallengeResponses(id int) int {
	if db == nil {
		return 0
	}
	var n int
	if err := db.QueryRow(
		`SELECT COUNT(*) FROM challenge_responses WHERE challenge_id = $1`, id,
	).Scan(&n); err != nil {
		// Not worth failing the delete over; the count is reporting, not
		// safety. Reported as -1 rather than 0 so "we could not tell" does
		// not read as "nothing was attached".
		return -1
	}
	return n
}

// isNoSuchChallenge distinguishes "that id names nothing" from a real
// failure. Treating the two the same would report a typo as a database
// problem, and a database problem as a typo.
//
// Matched on the sentinel rather than on the wording. Comparing message text
// works right up until somebody rewords the error, at which point every
// missing id starts reading as a failure and nothing says why.
func isNoSuchChallenge(err error) bool {
	return errors.Is(err, errChallengeNotFound)
}
