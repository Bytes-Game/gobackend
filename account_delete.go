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
// Their chats go too: the database removes every message they sent or
// received along with their row (chat_messages cascades on both people).
//
// ════════════════════════════════════════════════════════════════════════
// THEIR FILES
// ════════════════════════════════════════════════════════════════════════
//
// What they uploaded is removed from storage as well, by a background
// worker (media_delete.go). Only the files the database lists for this
// account go:
//
//   - their posts, and the answers to them      (DeleteChallengeByID)
//   - their answers to other people's posts
//   - their profile photo
//   - the photos and voice notes in their chats, unless a forwarded copy in
//     a chat that stays still shows the same file
//
// NEVER THEIR WHOLE FOLDER. Every upload sits under u/<their number>/, and
// this used to clear that folder in one go. But a number is only unique
// inside one database. When the database is replaced with a fresh one — as
// it was when the free one expired — numbering starts again from 1, while
// the old uploads stay in storage under the old numbers. A new account can
// then have an old account's number, and clearing "its" folder wipes the
// old person's videos: exactly what anyone would want back from the old
// database. A file nothing in this database points at is left alone.

import (
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"strings"

	"github.com/gorilla/mux"
	"github.com/lib/pq"
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
	if !queryFailed("the list of challenges belonging to the account being deleted",
		"their videos are being left behind while the account goes", err) {
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

	// Their other files, read from the rows that point at them. Read now:
	// the rows go below, and the addresses go with them.
	files := accountFiles(userID)

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
	// Queued only now. A delete that failed above leaves the account where
	// it was, and it keeps its photo and its answers.
	files.queue()

	// 3) Best-effort Redis state: embeddings, seen-set, signals. TTLs
	// reap the rest; these are just the long-lived keys.
	if rdb != nil {
		for _, k := range []string{
			"embed:user:" + userID, "seen:" + userID,
			"blocked_creators:" + userID, "unfollowed:" + userID,
			"recent_bounces:" + userID, "recent_searches:" + userID,
			recentSearchAccountsKey(userID),
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

// accountFileList is what deleting an account clears from storage, besides
// the posts, which DeleteChallengeByID queues one by one.
type accountFileList struct {
	// Folders that are this account's alone: the profile photo, and each
	// of their answers to other people's posts.
	folders []string
	// Every photo and voice note in their chats, theirs and the other
	// person's. Those chats go with the account, but a copy forwarded into
	// a chat that stays can still show the same file, so each one is
	// checked again once the account is gone.
	chatFiles []string
}

// accountFiles works out, from the rows that point at them, which files go
// with an account. Must run BEFORE those rows are deleted.
func accountFiles(userID string) accountFileList {
	var out accountFileList
	uid, err := strconv.Atoi(userID)
	if db == nil || err != nil {
		return out
	}
	cfg, err := mediaDeleteStorage()
	if err != nil || cfg == nil {
		// No storage set up, so nothing was ever uploaded to it.
		return out
	}

	// The profile photo, if it is their own upload. The app cannot set any
	// other (validProfilePhoto), but a photo saved before that check
	// existed could point at somebody else's file.
	var photo string
	err = db.QueryRow(`SELECT COALESCE(avatar_url, '') FROM users WHERE id = $1`, uid).Scan(&photo)
	if !queryFailed("the profile photo of an account being deleted",
		"the photo stays in storage", err) {
		if p := mediaPrefixFromPublicURL(cfg, photo); strings.HasPrefix(p, "u/"+userID+"/") {
			out.folders = append(out.folders, p)
		}
	}

	// Their answers to other people's posts. Answers to their own posts
	// went with those posts.
	rows, err := db.Query(`SELECT id FROM challenge_responses WHERE responder_id = $1`, uid)
	if !queryFailed("the answers of an account being deleted",
		"their videos stay in storage", err) {
		var answers []int
		bad := 0
		for rows.Next() {
			var id int
			if scanFailed("an answer of an account being deleted", rows.Scan(&id), &bad) {
				continue
			}
			answers = append(answers, id)
		}
		rows.Close()
		for _, id := range answers {
			out.folders = append(out.folders, mediaPrefixesForResponse(id)...)
		}
	}

	// The photos and voice notes in their chats.
	rows, err = db.Query(`SELECT DISTINCT media_url FROM chat_messages
	                       WHERE (sender_id = $1 OR receiver_id = $1)
	                         AND COALESCE(media_url, '') <> ''`, uid)
	if !queryFailed("the chat photos and voice notes of an account being deleted",
		"they stay in storage", err) {
		bad := 0
		for rows.Next() {
			var url string
			if scanFailed("a chat file of an account being deleted", rows.Scan(&url), &bad) {
				continue
			}
			out.chatFiles = append(out.chatFiles, url)
		}
		rows.Close()
	}
	return out
}

// queue hands the files to the cleanup worker. Call it only once the account
// is really gone.
func (f accountFileList) queue() {
	enqueueMediaDeletions(f.folders)
	if len(f.chatFiles) == 0 || db == nil {
		return
	}
	// A chat file goes unless a message that is still there shows it: a copy
	// somebody forwarded into another chat. One question for all of them,
	// rather than one each — someone who sent a lot of photos would
	// otherwise make the delete crawl.
	rows, err := db.Query(`SELECT u FROM unnest($1::text[]) AS t(u)
	                        WHERE NOT EXISTS (
	                          SELECT 1 FROM chat_messages m
	                           WHERE m.media_url = t.u AND m.is_deleted IS NOT TRUE)`,
		pq.Array(f.chatFiles))
	if queryFailed("which chat files of a deleted account nothing shows any more",
		"keeping them all, to be safe", err) {
		return
	}
	var gone []string
	bad := 0
	for rows.Next() {
		var url string
		if scanFailed("a chat file of a deleted account", rows.Scan(&url), &bad) {
			continue
		}
		if folder := chatFileFolder(url); folder != "" {
			gone = append(gone, folder)
		}
	}
	rows.Close()
	enqueueMediaDeletions(gone)
}
