package main

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
)

// ════════════════════════════════════════════════════════════════════
// Profile editing
// ════════════════════════════════════════════════════════════════════

// UpdateUserProfileHandler — PATCH /api/v1/users/{id}
//
// Updates whichever optional fields the client sends. Each field is
// validated independently so a bad bio doesn't reject a valid
// fullName change.
//
// Why we accept the userId both in the URL AND in the body: the URL
// is cosmetic (so the path reads as `/users/123`), but the body's
// `userId` is the one we authorize against. Once we have real
// session auth, the body field drops and the session subject takes
// over — keeping the body field now means that future change is a
// one-line cutover at the validation step, not an API break.
func UpdateUserProfileHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	pathID := vars["id"]
	if pathID == "" {
		http.Error(w, "missing user id", http.StatusBadRequest)
		return
	}

	var payload struct {
		UserID     string                  `json:"userId"`
		FullName   *string                 `json:"fullName,omitempty"`
		Bio        *string                 `json:"bio,omitempty"`
		Visibility *string                 `json:"visibility,omitempty"`
		Settings   *map[string]any `json:"settings,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "invalid body", http.StatusBadRequest)
		return
	}
	// You may only edit your own profile — identity comes from the token,
	// and the path id must match it.
	uid, ok := requirePathUser(w, r, pathID)
	if !ok {
		return
	}
	payload.UserID = uid

	// Profile edits are rare. 60/hr is generous (one minute apart on
	// average), burst of 3 covers a quick typo-fix sequence. Anything
	// faster than that is either UI-glitch retries or someone
	// brute-forcing username availability via the same endpoint.
	if !allowAction(payload.UserID, "profile_edit") {
		writeRateLimited(w, "profile_edit")
		return
	}

	// Validate. Empty bio/fullName ARE valid (means "clear it") — we
	// only reject when the supplied value is invalid (too long, bad
	// chars). The pointer-of-string form lets us distinguish "field
	// not sent" (no change) from "field set to empty" (clear).
	if payload.FullName != nil && len(*payload.FullName) > 100 {
		http.Error(w, "fullName too long (max 100)", http.StatusBadRequest)
		return
	}
	if payload.Bio != nil && len(*payload.Bio) > 500 {
		http.Error(w, "bio too long (max 500)", http.StatusBadRequest)
		return
	}
	if payload.Visibility != nil {
		v := *payload.Visibility
		if v != "public" && v != "friends" {
			http.Error(w, "visibility must be public or friends", http.StatusBadRequest)
			return
		}
	}

	// Build dynamic UPDATE. Anything not sent stays unchanged — we
	// don't issue an UPDATE for a missing field.
	sets := []string{}
	args := []any{}
	idx := 1
	if payload.FullName != nil {
		sets = append(sets, "full_name = $"+strconv.Itoa(idx))
		args = append(args, *payload.FullName)
		idx++
	}
	if payload.Bio != nil {
		sets = append(sets, "bio = $"+strconv.Itoa(idx))
		args = append(args, strings.TrimSpace(*payload.Bio))
		idx++
	}
	if payload.Visibility != nil {
		sets = append(sets, "visibility = $"+strconv.Itoa(idx))
		args = append(args, *payload.Visibility)
		idx++
	}
	if payload.Settings != nil {
		raw, err := json.Marshal(*payload.Settings)
		if err != nil {
			http.Error(w, "settings: invalid json", http.StatusBadRequest)
			return
		}
		sets = append(sets, "settings = $"+strconv.Itoa(idx)+"::jsonb")
		args = append(args, string(raw))
		idx++
	}
	if len(sets) == 0 {
		// No-op request — treat as success so retry-safe clients
		// don't ping-pong.
		writeJSON(w, http.StatusOK, map[string]any{"updated": false})
		return
	}

	// users.id is an INT column. lib/pq binds Go strings as TEXT,
	// and PostgreSQL refuses to implicitly compare TEXT to INT in
	// WHERE — the UPDATE silently matched 0 rows and the handler
	// returned 404 even for valid users. Parse to int so the bind
	// is the right SQL type. Same fix pattern as the history /
	// likes / clear-history handlers.
	pathIDInt, err := strconv.Atoi(pathID)
	if err != nil {
		http.Error(w, "invalid user id", http.StatusBadRequest)
		return
	}
	args = append(args, pathIDInt)
	query := "UPDATE users SET " + strings.Join(sets, ", ") + " WHERE id = $" + strconv.Itoa(idx)
	res, err := db.Exec(query, args...)
	if err != nil {
		http.Error(w, "update failed: "+err.Error(), http.StatusInternalServerError)
		return
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		http.Error(w, "user not found", http.StatusNotFound)
		return
	}

	// Round-trip the fresh user so the client doesn't have to make a
	// follow-up GET to reflect its own edit.
	updated, ok := GetUserByID(pathID)
	if !ok {
		writeJSON(w, http.StatusOK, map[string]any{"updated": true})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"updated": true,
		"user":    updated,
	})
}

// ════════════════════════════════════════════════════════════════════
// Followers / Following
// ════════════════════════════════════════════════════════════════════

// GetFollowersHandler — GET /api/v1/users/{id}/followers?page=&limit=
//
// Returns a bounded page of the accounts that follow {id}. Replaces the old
// client behaviour of fetching the ENTIRE users table and filtering by
// followingList in Dart — which is both slow and incorrect once the roster
// exceeds one page.
func GetFollowersHandler(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["id"]
	if id == "" {
		http.Error(w, "missing user id", http.StatusBadRequest)
		return
	}
	limit := parseIntOrDefault(r.URL.Query().Get("limit"), 30, 100)
	page := parseIntOrDefault(r.URL.Query().Get("page"), 1, 1_000_000)
	users := GetFollowers(id, limit, (page-1)*limit)
	if users == nil {
		users = []User{}
	}
	writeJSON(w, http.StatusOK, users)
}

// GetFollowingHandler — GET /api/v1/users/{id}/following?page=&limit=
func GetFollowingHandler(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["id"]
	if id == "" {
		http.Error(w, "missing user id", http.StatusBadRequest)
		return
	}
	limit := parseIntOrDefault(r.URL.Query().Get("limit"), 30, 100)
	page := parseIntOrDefault(r.URL.Query().Get("page"), 1, 1_000_000)
	users := GetFollowing(id, limit, (page-1)*limit)
	if users == nil {
		users = []User{}
	}
	writeJSON(w, http.StatusOK, users)
}

// ════════════════════════════════════════════════════════════════════
// Liked videos
// ════════════════════════════════════════════════════════════════════

// GetLikedChallengesHandler — GET /api/v1/users/{id}/likes?limit=&before=
//
// Returns challenges this user has liked, newest-liked first. Uses the
// composite idx_challenge_likes_user_time so the LIMIT scan is index-
// only. Pagination is cursor-based on the like timestamp to stay
// stable while users add new likes.
func GetLikedChallengesHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	userID := vars["id"]
	if userID == "" {
		http.Error(w, "missing user id", http.StatusBadRequest)
		return
	}
	// A user's liked list is private — only the owner may read it.
	if _, ok := requirePathUser(w, r, userID); !ok {
		return
	}
	// challenge_likes.user_id is INT; bind as int so PostgreSQL can
	// type-check the comparison. See GetWatchHistoryHandler for the
	// same fix's rationale — passing a string here previously made
	// the page return zero rows even when the user had real likes.
	uidInt, err := strconv.Atoi(userID)
	if err != nil {
		http.Error(w, "invalid user id", http.StatusBadRequest)
		return
	}
	limit := parseIntOrDefault(r.URL.Query().Get("limit"), 24, 100)
	beforeRaw := r.URL.Query().Get("before") // unix seconds, optional cursor

	// SELECT joins so we return ChallengeModel-shaped rows the
	// existing /feed/smart parser already handles.
	q := `
		SELECT c.id, c.creator_id, COALESCE(u.username,'') AS creator_username,
		       COALESCE(u.league,'Bronze') AS creator_league,
		       c.video_url, COALESCE(c.thumbnail_url,''),
		       c.prefix, c.subject, c.visibility, c.status,
		       (SELECT COUNT(*) FROM challenge_likes WHERE challenge_id = c.id) AS likes,
		       c.views,
		       c.created_at,
		       cl.created_at AS liked_at
		  FROM challenge_likes cl
		  JOIN challenges c ON c.id = cl.challenge_id
		  LEFT JOIN users u ON u.id = c.creator_id
		 WHERE cl.user_id = $1`
	args := []any{uidInt}
	if beforeRaw != "" {
		if beforeSec, err := strconv.ParseInt(beforeRaw, 10, 64); err == nil {
			q += " AND cl.created_at < $2"
			args = append(args, time.Unix(beforeSec, 0))
		}
	}
	q += " ORDER BY cl.created_at DESC LIMIT $" + strconv.Itoa(len(args)+1)
	args = append(args, limit+1) // fetch one extra to know if there's more

	rows, err := db.Query(q, args...)
	if err != nil {
		http.Error(w, "query failed: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	items := []map[string]any{}
	var lastLikedAt time.Time
	for rows.Next() {
		var id, creatorID int
		var creatorUsername, creatorLeague, videoURL, thumbURL, prefix, subject, visibility, status string
		var likes, views int
		var createdAt, likedAt time.Time
		if err := rows.Scan(&id, &creatorID, &creatorUsername, &creatorLeague,
			&videoURL, &thumbURL, &prefix, &subject, &visibility, &status,
			&likes, &views, &createdAt, &likedAt); err != nil {
			continue
		}
		lastLikedAt = likedAt
		items = append(items, map[string]any{
			"id":              strconv.Itoa(id),
			"creatorId":       strconv.Itoa(creatorID),
			"creatorUsername": creatorUsername,
			"creatorLeague":   creatorLeague,
			"videoUrl":        videoURL,
			"thumbnailUrl":    thumbURL,
			"prefix":          prefix,
			"subject":         subject,
			"visibility":      visibility,
			"status":          status,
			"likes":           likes,
			"views":           views,
			"createdAt":       createdAt.Format(time.RFC3339),
			"likedAt":         likedAt.Format(time.RFC3339),
		})
	}

	hasMore := len(items) > limit
	if hasMore {
		items = items[:limit] // drop the lookahead row
	}
	next := ""
	if hasMore && !lastLikedAt.IsZero() {
		next = strconv.FormatInt(lastLikedAt.Unix(), 10)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"items":      items,
		"hasMore":    hasMore,
		"nextCursor": next,
	})
}

// ════════════════════════════════════════════════════════════════════
// Watch history
// ════════════════════════════════════════════════════════════════════

// GetWatchHistoryHandler — GET /api/v1/users/{id}/history?limit=&before=
//
// Newest-first list of challenges this user has watched. We only
// surface watch_events rows whose content_type='challenge' — post-
// type watch events are kept in the table for the recommender but
// aren't user-facing here (the post entity is retired in the UI).
//
// Each row is a (challenge, watchedAt, watchDurationMs, completed)
// tuple — same JSON-friendly shape the WatchHistoryPage expects.
func GetWatchHistoryHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	userID := vars["id"]
	if userID == "" {
		http.Error(w, "missing user id", http.StatusBadRequest)
		return
	}
	// Watch history is private — only the owner may read it.
	if _, ok := requirePathUser(w, r, userID); !ok {
		return
	}
	// watch_events.user_id and challenges.id are both INT columns;
	// lib/pq binds Go strings as TEXT, which PostgreSQL refuses to
	// implicitly compare to an INT (returns "operator does not exist:
	// integer = text"). Parse to int up front so the query passes both
	// type-checking AND uses the indexed (user_id, created_at DESC)
	// path. The bug here was the page silently returning zero rows.
	uidInt, err := strconv.Atoi(userID)
	if err != nil {
		http.Error(w, "invalid user id", http.StatusBadRequest)
		return
	}
	limit := parseIntOrDefault(r.URL.Query().Get("limit"), 30, 100)
	beforeRaw := r.URL.Query().Get("before")

	// Two-pass query via a CTE:
	//   1) `latest` picks the most recent watch_event per challenge
	//      (DISTINCT ON content_id, sorted by created_at DESC).
	//   2) The outer SELECT joins that to challenges + users for
	//      render data, then sorts by created_at globally and limits.
	//
	// Why a CTE instead of an inline subquery: the previous version
	// wrapped a join-inside-DISTINCT-ON query in `SELECT *`, and the
	// outer's `ORDER BY created_at DESC` was technically valid but
	// ambiguous to read. Splitting the dedupe from the join makes
	// each phase do one thing, and the column aliases below make the
	// outer SELECT positionally stable regardless of how Postgres
	// names the COALESCE outputs.
	//
	// Dedupe is necessary because the Flutter client now records a
	// watch event TWICE per reel (once on play-start via
	// _initialWatchTimer, once on scroll-transition via
	// _flushCurrentItemEvent) — both rows for the same challenge
	// would otherwise show up as duplicate rows in the timeline.
	baseQ := `
		WITH latest AS (
		  SELECT DISTINCT ON (content_id)
		         id, content_id, watch_time, completed, created_at
		    FROM watch_events
		   WHERE user_id = $1 AND content_type = 'challenge'`
	args := []any{uidInt}
	if beforeRaw != "" {
		if beforeSec, err := strconv.ParseInt(beforeRaw, 10, 64); err == nil {
			baseQ += " AND created_at < $2"
			args = append(args, time.Unix(beforeSec, 0))
		}
	}
	baseQ += `
		   ORDER BY content_id, created_at DESC
		)
		SELECT l.id, l.content_id, l.watch_time, l.completed, l.created_at,
		       COALESCE(c.video_url, '')       AS video_url,
		       COALESCE(c.thumbnail_url, '')   AS thumbnail_url,
		       COALESCE(c.prefix, '')          AS prefix,
		       COALESCE(c.subject, '')         AS subject,
		       COALESCE(u.username, '')        AS creator_username,
		       COALESCE(u.league, 'Bronze')    AS creator_league,
		       COALESCE(c.creator_id, 0)       AS creator_id
		  FROM latest l
		  JOIN challenges c ON c.id = l.content_id
		  LEFT JOIN users u ON u.id = c.creator_id
		 ORDER BY l.created_at DESC
		 LIMIT $` + strconv.Itoa(len(args)+1)
	args = append(args, limit+1)
	q := baseQ

	rows, err := db.Query(q, args...)
	if err != nil {
		http.Error(w, "query failed: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	items := []map[string]any{}
	var lastAt time.Time
	for rows.Next() {
		var eventID, contentID, watchTime, creatorID int
		var completed bool
		var createdAt time.Time
		var videoURL, thumbURL, prefix, subject, creatorUsername, creatorLeague string
		if err := rows.Scan(&eventID, &contentID, &watchTime, &completed, &createdAt,
			&videoURL, &thumbURL, &prefix, &subject,
			&creatorUsername, &creatorLeague, &creatorID); err != nil {
			continue
		}
		lastAt = createdAt
		items = append(items, map[string]any{
			"eventId":         strconv.Itoa(eventID),
			"watchedAt":       createdAt.Format(time.RFC3339),
			"watchDurationMs": watchTime,
			"completed":       completed,
			"challenge": map[string]any{
				"id":              strconv.Itoa(contentID),
				"creatorId":       strconv.Itoa(creatorID),
				"creatorUsername": creatorUsername,
				"creatorLeague":   creatorLeague,
				"videoUrl":        videoURL,
				"thumbnailUrl":    thumbURL,
				"prefix":          prefix,
				"subject":         subject,
			},
		})
	}

	hasMore := len(items) > limit
	if hasMore {
		items = items[:limit]
	}
	next := ""
	if hasMore && !lastAt.IsZero() {
		next = strconv.FormatInt(lastAt.Unix(), 10)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"items":      items,
		"hasMore":    hasMore,
		"nextCursor": next,
	})
}

// DeleteWatchHistoryHandler — DELETE /api/v1/users/{id}/history
//
// Clears ALL of the user's watch history. The eventId-scoped variant
// shares the same code path with a single-row WHERE — if the future
// requirements want per-row delete we add a {eventId} sub-route that
// dispatches into the same SQL with one extra clause.
//
// Soft delete vs hard delete: we currently hard-delete because the
// recommender doesn't need history events older than the rolling
// 30-day window for its signals (the user_profiles table is what
// gets aggregated to). If we move to a model where the long-tail
// matters, this should become an `is_hidden` flag.
func DeleteWatchHistoryHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	userID := vars["id"]
	if userID == "" {
		http.Error(w, "missing user id", http.StatusBadRequest)
		return
	}
	// Same authorize-on-path-equality pattern as the profile update
	// — clients pass userId in the body so we can phase to session
	// auth without an API break.
	var payload struct {
		UserID string `json:"userId"`
	}
	_ = json.NewDecoder(r.Body).Decode(&payload)
	// Identity comes from the session token; the path id must be the caller's own.
	if _, ok := requirePathUser(w, r, userID); !ok {
		return
	}
	// Same int-cast as the GET — DELETE … WHERE user_id = $1 must
	// bind as int because the column is int.
	uidInt, err := strconv.Atoi(userID)
	if err != nil {
		http.Error(w, "invalid user id", http.StatusBadRequest)
		return
	}

	res, err := db.Exec(`DELETE FROM watch_events WHERE user_id = $1`, uidInt)
	if err != nil {
		http.Error(w, "delete failed: "+err.Error(), http.StatusInternalServerError)
		return
	}
	n, _ := res.RowsAffected()
	writeJSON(w, http.StatusOK, map[string]any{
		"deleted": n,
	})
}

// ════════════════════════════════════════════════════════════════════
// Blocks
// ════════════════════════════════════════════════════════════════════

// BlockUserHandler — POST /api/v1/blocks
// Body: { blockerId, blockedId }
//
// Adds a row to user_blocks. ON CONFLICT DO NOTHING so a duplicate
// block (user double-tapped) is idempotent rather than an error.
// Also tears down any existing follow edges in both directions —
// blocking and continuing to follow doesn't make sense, and leaving
// stale edges would let the blocked user's content keep slipping
// into the recommender via following-affinity boosts.
func BlockUserHandler(w http.ResponseWriter, r *http.Request) {
	var payload struct {
		BlockerID string `json:"blockerId"`
		BlockedID string `json:"blockedId"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "invalid body", http.StatusBadRequest)
		return
	}
	// The blocker is the authenticated user.
	payload.BlockerID = authUserID(r)
	if payload.BlockerID == "" || payload.BlockedID == "" {
		http.Error(w, "blockerId and blockedId required", http.StatusBadRequest)
		return
	}
	if payload.BlockerID == payload.BlockedID {
		http.Error(w, "cannot block yourself", http.StatusBadRequest)
		return
	}

	// 20 blocks/min per user. Block-bombing was real on early TikTok
	// — users would block every account that liked a controversial
	// rival to mute them on each other's mentions. Bucket it.
	if !allowAction(payload.BlockerID, "block") {
		writeRateLimited(w, "block")
		return
	}

	tx, err := db.Begin()
	if err != nil {
		http.Error(w, "tx failed", http.StatusInternalServerError)
		return
	}
	defer tx.Rollback()

	if _, err := tx.Exec(
		`INSERT INTO user_blocks (blocker_id, blocked_id)
		 VALUES ($1, $2)
		 ON CONFLICT DO NOTHING`,
		payload.BlockerID, payload.BlockedID,
	); err != nil {
		http.Error(w, "block insert failed: "+err.Error(), http.StatusInternalServerError)
		return
	}
	// Drop both directions of any follow between them.
	if _, err := tx.Exec(
		`DELETE FROM follows
		  WHERE (follower_id = $1 AND following_id = $2)
		     OR (follower_id = $2 AND following_id = $1)`,
		payload.BlockerID, payload.BlockedID,
	); err != nil {
		http.Error(w, "follow cleanup failed: "+err.Error(), http.StatusInternalServerError)
		return
	}
	if err := tx.Commit(); err != nil {
		http.Error(w, "commit failed", http.StatusInternalServerError)
		return
	}
	// Mirror the block into the Redis blocked-creators set the ranker actually
	// reads (negativeCreatorPenalty zeroes blocked creators). Without this the
	// button-block updated user_blocks but never hid the creator's content from
	// the feed — blocking via the UI did nothing to ranking. Synchronous so the
	// very next feed request already excludes them.
	MarkBlocked(payload.BlockerID, payload.BlockedID)
	writeJSON(w, http.StatusOK, map[string]any{"blocked": true})
}

// UnblockUserHandler — POST /api/v1/unblock
// Body: { blockerId, blockedId }
func UnblockUserHandler(w http.ResponseWriter, r *http.Request) {
	var payload struct {
		BlockerID string `json:"blockerId"`
		BlockedID string `json:"blockedId"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "invalid body", http.StatusBadRequest)
		return
	}
	// The unblocker is the authenticated user.
	payload.BlockerID = authUserID(r)
	if payload.BlockerID == "" || payload.BlockedID == "" {
		http.Error(w, "blockerId and blockedId required", http.StatusBadRequest)
		return
	}
	_, err := db.Exec(
		`DELETE FROM user_blocks WHERE blocker_id = $1 AND blocked_id = $2`,
		payload.BlockerID, payload.BlockedID,
	)
	if err != nil {
		http.Error(w, "delete failed: "+err.Error(), http.StatusInternalServerError)
		return
	}
	// Keep the ranker's Redis blocked-creators set in sync so the creator can
	// reappear in the feed after an unblock.
	UnmarkBlocked(payload.BlockerID, payload.BlockedID)
	writeJSON(w, http.StatusOK, map[string]any{"unblocked": true})
}

// ListBlockedUsersHandler — GET /api/v1/users/{id}/blocks
func ListBlockedUsersHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	userID := vars["id"]
	if userID == "" {
		http.Error(w, "missing user id", http.StatusBadRequest)
		return
	}
	// The block list is private — only the owner may read it.
	if _, ok := requirePathUser(w, r, userID); !ok {
		return
	}
	rows, err := db.Query(
		`SELECT u.id, u.username, COALESCE(u.full_name,''), COALESCE(u.league,'Bronze'),
		        ub.created_at
		   FROM user_blocks ub
		   JOIN users u ON u.id = ub.blocked_id
		  WHERE ub.blocker_id = $1
		  ORDER BY ub.created_at DESC`,
		userID,
	)
	if err != nil {
		http.Error(w, "query failed: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()
	out := []map[string]any{}
	for rows.Next() {
		var id int
		var username, fullName, league string
		var createdAt time.Time
		if err := rows.Scan(&id, &username, &fullName, &league, &createdAt); err != nil {
			continue
		}
		out = append(out, map[string]any{
			"id":        strconv.Itoa(id),
			"username":  username,
			"fullName":  fullName,
			"league":    league,
			"blockedAt": createdAt.Format(time.RFC3339),
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": out})
}

// ════════════════════════════════════════════════════════════════════
// TOTP / 2FA
// ════════════════════════════════════════════════════════════════════

// EnrollTOTPHandler — POST /api/v1/users/{id}/totp/enroll
//
// Starts (or restarts) TOTP enrollment. Generates a fresh secret +
// recovery codes, persists the secret with is_active=FALSE so the
// row exists but doesn't yet block sign-in. The client renders the
// otpauth URI as a QR, the user scans + types a 6-digit code, and
// the follow-up /verify call flips is_active=TRUE.
//
// We deliberately allow re-enrollment on an already-active account
// (the upsert overwrites the secret) so a user who loses their
// authenticator can re-enroll by signing in with a recovery code
// and then visiting this endpoint. The recovery codes are also
// regenerated each time so the old ones can't keep being used.
func EnrollTOTPHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	userID := vars["id"]
	if userID == "" {
		http.Error(w, "missing user id", http.StatusBadRequest)
		return
	}
	var payload struct {
		UserID string `json:"userId"`
	}
	_ = json.NewDecoder(r.Body).Decode(&payload)
	// Identity comes from the session token; the path id must be the caller's own.
	if _, ok := requirePathUser(w, r, userID); !ok {
		return
	}

	user, ok := GetUserByID(userID)
	if !ok {
		http.Error(w, "user not found", http.StatusNotFound)
		return
	}

	secret, err := generateTOTPSecret()
	if err != nil {
		http.Error(w, "secret generation failed", http.StatusInternalServerError)
		return
	}
	plaintextCodes, hashes, err := generateRecoveryCodes(10)
	if err != nil {
		http.Error(w, "recovery code generation failed", http.StatusInternalServerError)
		return
	}
	hashesJSON, _ := json.Marshal(hashes)

	// Upsert into user_totp. is_active stays FALSE until /verify
	// flips it — that's how we make sure a user can't accidentally
	// lock themselves out by enrolling without ever testing a code.
	if _, err := db.Exec(
		`INSERT INTO user_totp (user_id, secret, recovery_codes, is_active, enrolled_at)
		 VALUES ($1, $2, $3::jsonb, FALSE, NOW())
		 ON CONFLICT (user_id) DO UPDATE
		    SET secret = EXCLUDED.secret,
		        recovery_codes = EXCLUDED.recovery_codes,
		        is_active = FALSE,
		        enrolled_at = NOW()`,
		userID, secret, string(hashesJSON),
	); err != nil {
		http.Error(w, "enroll failed: "+err.Error(), http.StatusInternalServerError)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"secret":         secret,
		"otpauthUri":     otpauthURI("devf", user.Username, secret),
		"recoveryCodes":  plaintextCodes, // only time we ever return these
		"digits":         totpDigits,
		"periodSeconds":  totpStepSeconds,
		"requiresVerify": true,
	})
}

// VerifyTOTPHandler — POST /api/v1/users/{id}/totp/verify
// Body: { userId, code }
//
// Activates the pending enrollment if `code` is valid for the
// stored secret. Idempotent: re-verifying when already active just
// returns the current state. Bad code → 401.
func VerifyTOTPHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	userID := vars["id"]
	if userID == "" {
		http.Error(w, "missing user id", http.StatusBadRequest)
		return
	}
	var payload struct {
		UserID string `json:"userId"`
		Code   string `json:"code"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "invalid body", http.StatusBadRequest)
		return
	}
	// Identity comes from the session token; the path id must be the caller's own.
	if _, ok := requirePathUser(w, r, userID); !ok {
		return
	}
	if payload.Code == "" {
		http.Error(w, "code required", http.StatusBadRequest)
		return
	}

	var secret string
	var active bool
	err := db.QueryRow(
		`SELECT secret, is_active FROM user_totp WHERE user_id = $1`,
		userID,
	).Scan(&secret, &active)
	if err == sql.ErrNoRows {
		http.Error(w, "no enrollment in progress", http.StatusNotFound)
		return
	} else if err != nil {
		http.Error(w, "lookup failed", http.StatusInternalServerError)
		return
	}

	if !verifyTOTPCode(secret, strings.TrimSpace(payload.Code)) {
		http.Error(w, "invalid code", http.StatusUnauthorized)
		return
	}

	if !active {
		if _, err := db.Exec(
			`UPDATE user_totp SET is_active = TRUE, last_used_at = NOW() WHERE user_id = $1`,
			userID,
		); err != nil {
			http.Error(w, "activate failed", http.StatusInternalServerError)
			return
		}
	} else {
		// Already active — just bump last_used_at for replay defense.
		_, _ = db.Exec(`UPDATE user_totp SET last_used_at = NOW() WHERE user_id = $1`, userID)
	}
	writeJSON(w, http.StatusOK, map[string]any{"verified": true, "active": true})
}

// DisableTOTPHandler — POST /api/v1/users/{id}/totp/disable
// Body: { userId, code }
//
// Requires a valid TOTP code (or recovery code — TBD when sign-in
// uses 2FA) to disable. Without this, anyone who got a session
// cookie could turn off 2FA without proving they're the real user.
func DisableTOTPHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	userID := vars["id"]
	if userID == "" {
		http.Error(w, "missing user id", http.StatusBadRequest)
		return
	}
	var payload struct {
		UserID string `json:"userId"`
		Code   string `json:"code"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "invalid body", http.StatusBadRequest)
		return
	}
	// Identity comes from the session token; the path id must be the caller's own.
	if _, ok := requirePathUser(w, r, userID); !ok {
		return
	}

	var secret string
	err := db.QueryRow(
		`SELECT secret FROM user_totp WHERE user_id = $1 AND is_active = TRUE`,
		userID,
	).Scan(&secret)
	if err == sql.ErrNoRows {
		http.Error(w, "2FA not active", http.StatusNotFound)
		return
	} else if err != nil {
		http.Error(w, "lookup failed", http.StatusInternalServerError)
		return
	}

	if !verifyTOTPCode(secret, strings.TrimSpace(payload.Code)) {
		// Try recovery code as a fallback.
		if !consumeRecoveryCode(userID, payload.Code) {
			http.Error(w, "invalid code", http.StatusUnauthorized)
			return
		}
	}

	if _, err := db.Exec(`DELETE FROM user_totp WHERE user_id = $1`, userID); err != nil {
		http.Error(w, "disable failed", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"disabled": true})
}

// consumeRecoveryCode returns true if `code` matches one of the
// user's stored hashes AND removes that hash from the row so the
// code can't be reused. Single-use enforcement is critical: without
// the remove step, an attacker who got one recovery code holds 2FA
// bypass forever.
func consumeRecoveryCode(userID, code string) bool {
	var hashesJSON string
	if err := db.QueryRow(
		`SELECT COALESCE(recovery_codes::text, '[]') FROM user_totp WHERE user_id = $1`,
		userID,
	).Scan(&hashesJSON); err != nil {
		return false
	}
	var hashes []string
	if err := json.Unmarshal([]byte(hashesJSON), &hashes); err != nil {
		return false
	}
	target := hashRecoveryCode(code)
	idx := -1
	for i, h := range hashes {
		if h == target {
			idx = i
			break
		}
	}
	if idx < 0 {
		return false
	}
	// Remove the consumed hash.
	hashes = append(hashes[:idx], hashes[idx+1:]...)
	newJSON, _ := json.Marshal(hashes)
	_, _ = db.Exec(
		`UPDATE user_totp SET recovery_codes = $1::jsonb WHERE user_id = $2`,
		string(newJSON), userID,
	)
	return true
}

// ════════════════════════════════════════════════════════════════════
// Shared helpers
// ════════════════════════════════════════════════════════════════════

// writeJSON writes the standard JSON response with the given status.
// Centralizes the Content-Type header so individual handlers can't
// forget it.
func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

// parseIntOrDefault parses a small unsigned int from a query string,
// returning `def` on failure and capping at `max`. Used by the
// pagination cursors.
func parseIntOrDefault(s string, def, max int) int {
	if s == "" {
		return def
	}
	n, err := strconv.Atoi(strings.TrimSpace(s))
	if err != nil || n <= 0 {
		return def
	}
	if n > max {
		return max
	}
	return n
}
