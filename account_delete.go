package main

// account_delete.go — self-service account deletion.
//
// DELETE /api/v1/users/{id} (authed; the path id must match the token
// identity). Required for Google Play compliance (apps with account
// creation must offer in-app deletion) and simply the right thing.
//
// Scope: hard-deletes the user's row, social graph edges, engagement
// history, profile/model state, device tokens, and their CONTENT
// (challenges cascade to responses/likes/votes/comments via
// DeleteChallengeByID, which also feeds the search-index removal).
// Chat messages the user SENT remain (they're the other participant's
// history too) but are already attributed by numeric id only.
// R2 media objects are not deleted here (same policy as challenge
// deletion — decoupled storage cleanup).

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/gorilla/mux"
)

// DeleteAccountHandler — DELETE /api/v1/users/{id}
func DeleteAccountHandler(w http.ResponseWriter, r *http.Request) {
	if db == nil {
		http.Error(w, "service unavailable", http.StatusServiceUnavailable)
		return
	}
	userID := mux.Vars(r)["id"]
	// Only the account owner may delete it.
	if userID == "" || authUserID(r) != userID {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}

	// 1) Content first: reuse the challenge-deletion path so responses,
	// likes, votes, comments, and the Meilisearch document all go with
	// each challenge.
	rows, err := db.Query(`SELECT id FROM challenges WHERE creator_id::text = $1`, userID)
	if err == nil {
		ids := []string{}
		for rows.Next() {
			var id string
			if rows.Scan(&id) == nil {
				ids = append(ids, id)
			}
		}
		rows.Close()
		for _, id := range ids {
			if err := DeleteChallengeByID(id); err != nil {
				log.Printf("account delete %s: challenge %s cleanup failed: %v", userID, id, err)
			}
		}
	}

	// 2) Everything else in one transaction. Order doesn't matter (no
	// FK chains between these), but the users row goes last so a crash
	// mid-way leaves a recoverable half-cleaned account rather than an
	// orphaned login.
	tx, err := db.Begin()
	if err != nil {
		http.Error(w, "delete failed", http.StatusInternalServerError)
		return
	}
	stmts := []string{
		`DELETE FROM challenge_responses WHERE responder_id::text = $1`,
		`DELETE FROM follows WHERE follower_id::text = $1 OR following_id::text = $1`,
		`DELETE FROM user_blocks WHERE blocker_id::text = $1 OR blocked_id::text = $1`,
		`DELETE FROM device_tokens WHERE user_id = $1`,
		`DELETE FROM notification_prefs WHERE user_id = $1`,
		`DELETE FROM notification_outbox WHERE user_id = $1`,
		`DELETE FROM saved_challenges WHERE user_id::text = $1`,
		`DELETE FROM challenge_likes WHERE user_id::text = $1`,
		`DELETE FROM challenge_votes WHERE voter_id::text = $1`,
		`DELETE FROM watch_events WHERE user_id::text = $1`,
		`DELETE FROM feed_events WHERE user_id = $1`,
		`DELETE FROM session_outcomes WHERE user_id = $1`,
		`DELETE FROM experiment_exposures WHERE user_id = $1`,
		`DELETE FROM user_similarities WHERE user_id::text = $1 OR similar_user_id::text = $1`,
		`DELETE FROM user_profiles WHERE user_id = $1`,
		`DELETE FROM user_totp WHERE user_id::text = $1`,
		`DELETE FROM users WHERE id::text = $1`,
	}
	for _, s := range stmts {
		if _, err := tx.Exec(s, userID); err != nil {
			_ = tx.Rollback()
			log.Printf("account delete %s failed at %q: %v", userID, s, err)
			http.Error(w, "delete failed", http.StatusInternalServerError)
			return
		}
	}
	if err := tx.Commit(); err != nil {
		http.Error(w, "delete failed", http.StatusInternalServerError)
		return
	}

	// 3) Best-effort Redis state: embeddings, seen-set, signals. TTLs
	// reap the rest; these are just the long-lived keys.
	if rdb != nil {
		for _, k := range []string{
			"embed:user:" + userID, "seen:" + userID,
			"blocked_creators:" + userID, "unfollowed:" + userID,
			"recent_bounces:" + userID, "recent_searches:" + userID,
			"creator_affinity:" + userID, "tie:" + userID,
			"lasteng:" + userID, "ltrneg:" + userID,
		} {
			_ = rdb.Del(rctx, k).Err()
		}
	}

	log.Printf("Account deleted: user %s", userID)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"deleted": true})
}
