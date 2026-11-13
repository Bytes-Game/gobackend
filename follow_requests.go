package main

// follow_requests.go — asking to follow a private account.
//
// ════════════════════════════════════════════════════════════════════════
// WHAT WAS WRONG
// ════════════════════════════════════════════════════════════════════════
//
// A private account's videos are for its followers (private_accounts.go).
// But following needed nobody's say-so. Anybody could tap Follow on a
// private account, become a follower that second, and see everything in
// it. "Private" kept nothing private.
//
// Now following a private account is a request. The person asked sees it
// in their notifications and says yes or no. Only a yes makes a follower.
// A public account is followed at once, as before.
//
//	POST /api/v1/follow                 → 202 {"requested": true} for a
//	                                      private account
//	GET  /api/v1/follow-requests        → who is asking to follow me
//	POST /api/v1/follow-requests/answer → {"requesterId", "accept"}
//	POST /api/v1/unfollow               → also takes a request back
//
// An account that goes back to public accepts everybody who was waiting:
// they could all follow it now anyway.

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
)

// Kinds of notification for follow requests, as the app reads them.
const (
	noteFollowRequest  = "follow_request"
	noteFollowAccepted = "follow_accepted"
)

// accountIsPrivate is whether [userID]'s account is private.
func accountIsPrivate(userID int) (bool, error) {
	var private bool
	err := db.QueryRow(`SELECT visibility = 'friends' FROM users WHERE id = $1`, userID).Scan(&private)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return private, err
}

// isFollowing is whether [followerID] follows [targetID].
func isFollowing(followerID, targetID int) (bool, error) {
	var yes bool
	err := db.QueryRow(`SELECT EXISTS (SELECT 1 FROM follows WHERE follower_id = $1 AND following_id = $2)`,
		followerID, targetID).Scan(&yes)
	return yes, err
}

// askToFollow records that [requesterID] asks to follow [targetID], and
// says whether that is new: asking twice is asking once.
func askToFollow(requesterID, targetID int) (bool, error) {
	res, err := db.Exec(`
		INSERT INTO follow_requests (requester_id, target_id) VALUES ($1, $2)
		ON CONFLICT DO NOTHING`, requesterID, targetID)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n == 1, nil
}

// followRequested is whether [requesterID] is waiting for [targetID]'s
// answer. A database that cannot say answers no and logs it.
func followRequested(requesterID, targetID string) bool {
	if db == nil || requesterID == "" || targetID == "" || requesterID == targetID {
		return false
	}
	var yes bool
	err := db.QueryRow(`
		SELECT EXISTS (SELECT 1 FROM follow_requests
		                WHERE requester_id::text = $1 AND target_id::text = $2)`,
		requesterID, targetID).Scan(&yes)
	if queryFailed("whether user "+requesterID+" has asked to follow user "+targetID,
		"the profile shows Follow instead of Requested", err) {
		return false
	}
	return yes
}

// answerFollowRequest accepts or declines [requesterID]'s request to
// follow [targetID]. Accepting makes the follow and removes the request,
// both or neither. It says whether there was a request to answer.
func answerFollowRequest(ctx context.Context, targetID, requesterID int, accept bool) (bool, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback() }()
	res, err := tx.ExecContext(ctx,
		`DELETE FROM follow_requests WHERE requester_id = $1 AND target_id = $2`, requesterID, targetID)
	if err != nil {
		return false, err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return false, nil
	}
	if accept {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO follows (follower_id, following_id) VALUES ($1, $2)
			ON CONFLICT DO NOTHING`, requesterID, targetID); err != nil {
			return false, err
		}
	}
	return true, tx.Commit()
}

// acceptEveryFollowRequest makes a follower of everybody waiting on
// [targetID]: for an account that has just gone public.
func acceptEveryFollowRequest(targetID int) {
	if db == nil {
		return
	}
	_, err := db.Exec(`
		WITH waiting AS (
			DELETE FROM follow_requests WHERE target_id = $1 RETURNING requester_id)
		INSERT INTO follows (follower_id, following_id)
		SELECT requester_id, $1 FROM waiting
		ON CONFLICT DO NOTHING`, targetID)
	queryFailed("accepting everybody who asked to follow account "+strconv.Itoa(targetID)+" as it went public",
		"they stay waiting; each can follow again, which now works at once", err)
}

// FollowRequestsHandler — GET /api/v1/follow-requests: who is asking to
// follow the signed-in person, newest first.
func FollowRequestsHandler(w http.ResponseWriter, r *http.Request) {
	uid := authUserID(r)
	rows, err := db.QueryContext(r.Context(), `
		SELECT u.id::text, u.username, COALESCE(u.full_name, ''), COALESCE(u.avatar_url, ''),
		       u.league, fr.created_at
		  FROM follow_requests fr JOIN users u ON u.id = fr.requester_id
		 WHERE fr.target_id::text = $1
		   AND u.deletion_requested_at IS NULL AND u.suspended_at IS NULL
		 ORDER BY fr.created_at DESC
		 LIMIT 200`, uid)
	if queryFailed("who is asking to follow user "+uid, "answering that it could not be read", err) {
		http.Error(w, "could not read follow requests", http.StatusInternalServerError)
		return
	}
	defer rows.Close()
	type asking struct {
		ID          string `json:"id"`
		Username    string `json:"username"`
		FullName    string `json:"fullName"`
		AvatarURL   string `json:"avatarUrl,omitempty"`
		League      string `json:"league"`
		RequestedAt string `json:"requestedAt"`
	}
	out := []asking{}
	bad := 0
	for rows.Next() {
		var a asking
		var at sql.NullTime
		if scanFailed("somebody asking to follow user "+uid,
			rows.Scan(&a.ID, &a.Username, &a.FullName, &a.AvatarURL, &a.League, &at), &bad) {
			continue
		}
		if at.Valid {
			a.RequestedAt = at.Time.UTC().Format("2006-01-02T15:04:05Z07:00")
		}
		out = append(out, a)
	}
	writeJSON(w, http.StatusOK, map[string]any{"requests": out, "count": len(out)})
}

// AnswerFollowRequestHandler — POST /api/v1/follow-requests/answer
// {"requesterId": "7", "accept": true}.
func AnswerFollowRequestHandler(w http.ResponseWriter, r *http.Request) {
	uid := authUserID(r)
	if !allowAction(uid, "follow") {
		writeRateLimited(w, "follow")
		return
	}
	var in struct {
		RequesterID string `json:"requesterId"`
		Accept      bool   `json:"accept"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 4<<10)).Decode(&in); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}
	me, _ := strconv.Atoi(uid)
	requester, err := strconv.Atoi(in.RequesterID)
	if err != nil || requester <= 0 || me <= 0 {
		http.Error(w, "requesterId is required", http.StatusBadRequest)
		return
	}
	answered, err := answerFollowRequest(r.Context(), me, requester, in.Accept)
	if queryFailed("answering user "+in.RequesterID+"'s request to follow user "+uid,
		"nothing changed; the request is still waiting", err) {
		http.Error(w, "could not answer the request", http.StatusInternalServerError)
		return
	}
	if !answered {
		writeJSON(w, http.StatusNotFound, map[string]any{
			"error": "That request is no longer waiting.", "gone": true})
		return
	}
	if in.Accept {
		// The person who asked is told. A no is not announced.
		owner := authUsername(r)
		go notifyUser(InboxNote{
			UserID:    requester,
			Kind:      noteFollowAccepted,
			ActorID:   me,
			ActorName: owner,
			Body:      "accepted your follow request.",
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "accepted": in.Accept})
}
