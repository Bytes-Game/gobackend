package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strconv"
)

// HandleFollowEvent handles. the logic for when a user follows another user.
func HandleFollowEvent(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST method is allowed", http.StatusMethodNotAllowed)
		return
	}

	var payload FollowEventPayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "Invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}
	// The follower is whoever the token says it is — never trust the body.
	// Their name too: the notification says "X started following you", and
	// X used to be whatever name the caller typed, so anybody could send a
	// follow notice that looked as if it came from somebody else.
	payload.FollowerID = authUserID(r)
	payload.FollowerUsername = authUsername(r)

	// Per-user follow rate-limit. Catches follow-bombing without
	// blocking organic follow flurries (burst is 5 in actionLimitTable).
	if !allowAction(payload.FollowerID, "follow") {
		writeRateLimited(w, "follow")
		return
	}

	// Who is being followed, worked out once, here: the account the id (or,
	// for an older app, the name) points at. Everything after this uses
	// that one account. The id and the name used to be read separately, so
	// a caller could follow one person and have the notice sent to another.
	followingID, err := resolveUserID(payload.FollowingID, payload.FollowingUsername)
	if err != nil {
		http.Error(w, "That account doesn't exist.", http.StatusNotFound)
		return
	}
	payload.FollowingID = strconv.Itoa(followingID)
	if payload.FollowingUsername, err = currentUsername(payload.FollowingID); err != nil {
		http.Error(w, "That account doesn't exist.", http.StatusNotFound)
		return
	}
	if payload.FollowingID == payload.FollowerID {
		http.Error(w, "You can't follow yourself.", http.StatusBadRequest)
		return
	}

	// An account waiting to be deleted cannot be followed: it would come
	// straight back into a following list (account_leaving.go). Said as a
	// block from their side is, which the app already shows.
	if isLeaving(payload.FollowingID) {
		writeJSON(w, http.StatusForbidden, map[string]string{
			"error":  "You can't follow this account.",
			"reason": "unavailable",
		})
		return
	}

	created, requested, err := ProcessFollowEvent(payload)
	if err != nil {
		// Across a block: said plainly, with a reason the app can act on.
		switch {
		case errors.Is(err, errYouBlocked):
			writeJSON(w, http.StatusConflict, map[string]string{
				"error":  "You've blocked this person. Unblock them to follow.",
				"reason": "you_blocked",
			})
		case errors.Is(err, errTheyBlocked):
			writeJSON(w, http.StatusForbidden, map[string]string{
				"error":  "You can't follow this account.",
				"reason": "unavailable",
			})
		default:
			serverFault(w, "following "+payload.FollowingUsername, err)
		}
		return
	}

	// A private account: nothing is followed yet. They are asked, and the
	// app shows "Requested" (follow_requests.go).
	if requested {
		if created {
			go SendFollowRequestNotification(payload)
		}
		writeJSON(w, http.StatusAccepted, map[string]any{
			"requested": true,
			"message":   "Requested. They'll see your request.",
		})
		return
	}

	// Told once, when the follow is new: following somebody you already
	// follow is not news.
	if created {
		go SendFollowNotification(payload)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"message": "Follow event processed successfully"})
}

// HandleUnfollowEvent handles the logic for when a user unfollows another user.
func HandleUnfollowEvent(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST method is allowed", http.StatusMethodNotAllowed)
		return
	}
	var payload UnfollowEventPayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "Invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}
	// The actor is the authenticated user, not a client-supplied id.
	payload.UnfollowerID = authUserID(r)

	// Same per-user gate as follow. Unfollow-bombing is rarer but
	// not unheard of as a way to escape recommender memory by
	// resetting affinity.
	if !allowAction(payload.UnfollowerID, "unfollow") {
		writeRateLimited(w, "unfollow")
		return
	}

	if err := ProcessUnfollowEvent(payload); err != nil {
		serverFault(w, "unfollowing", err)
		return
	}

	// Tier 1.2: record a soft negative signal so the ranker attenuates this
	// creator's content for ~7 days. Best-effort — Redis blips don't fail the
	// primary flow.
	unfollowerID := payload.UnfollowerID
	if unfollowerID == "" {
		unfollowerID = payload.UnfollowerUsername
	}
	unfollowedID := payload.UnfollowedID
	if unfollowedID == "" {
		unfollowedID = payload.UnfollowedUsername
	}
	go MarkUnfollowed(unfollowerID, unfollowedID)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"message": "Unfollow event processed successfully"})
}

// (HandleLikeEvent, HandleCommentEvent, GetCommentsHandler retired —
// they were post-centric. Challenge engagement uses LikeChallengeHandler /
// AddChallengeCommentHandler / GetChallengeCommentsHandler in
// challenge_handler.go.)

// HandleWatchEvent records a watch event for analytics.
// POST /api/v1/watch
func HandleWatchEvent(w http.ResponseWriter, r *http.Request) {
	var payload WatchEventPayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "Invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}
	// Attribute the watch to the authenticated user, ignoring any body userId.
	payload.UserID = authUserID(r)

	if payload.ContentType == "" || payload.ContentID == "" || payload.UserID == "" {
		http.Error(w, "userId, contentId, and contentType are required", http.StatusBadRequest)
		return
	}

	if err := RecordWatchEvent(payload); err != nil {
		serverFault(w, "recording a watch event", err)
		return
	}

	// Fire a server-side prefetch hint over WebSocket so the client
	// can warm a VideoPlayerController for the user's next likely
	// reel BEFORE they swipe. Async + best-effort — never blocks the
	// watch-event ack. See next_reel_hint.go for throttle + filtering.
	if payload.ContentType == "challenge" && payload.ContentID != "" {
		go SendNextReelHint(payload.UserID, payload.ContentID)
	}

	// The video's views as they now stand, so the app shows the real number
	// rather than guessing: a view counts once a day per person, and only
	// after 1.5 seconds on screen, so "one more" was often wrong.
	reply := map[string]any{"message": "Watch event recorded"}
	if payload.ContentType == "challenge" {
		var views int
		err := db.QueryRow(`SELECT COALESCE(views, 0) FROM challenges WHERE id = $1`,
			payload.ContentID).Scan(&views)
		if err == nil {
			reply["views"] = views
		} else if err != sql.ErrNoRows {
			queryFailed("reading the views of challenge "+payload.ContentID+" after a watch",
				"answering without them, so the app keeps the number it has", err)
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(reply)
}

// HandleReportEvent creates a new report.
// POST /api/v1/report
func HandleReportEvent(w http.ResponseWriter, r *http.Request) {
	var payload ReportPayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "Invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}
	// The reporter is the authenticated user; never let the body name someone else.
	payload.ReporterID = authUserID(r)
	reporter, err := strconv.Atoi(payload.ReporterID)
	if err != nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	targetID, problem := cleanReport(&payload)
	if problem != "" {
		http.Error(w, problem, http.StatusBadRequest)
		return
	}

	// 10 reports/hour per user. The most common abuse here is
	// retaliatory mass-reporting to silence a rival; this cap makes
	// the strategy infeasible without slowing legitimate reporting
	// (rare enough that a real user maxing this is itself a flag).
	if !allowAction(payload.ReporterID, "report") {
		writeRateLimited(w, "report")
		return
	}

	// Whose it is: what the review list shows, whom "suspend" means, and
	// whose videos a serious report hides from the reporter. Hiding the
	// video's own number instead (as this did) hid some other account
	// that happened to have that number.
	owner, found, err := reportedOwner(payload.TargetType, targetID)
	if queryFailed("whose reported "+payload.TargetType+" "+payload.TargetID+" is",
		"the report is refused, and the app says to try again", err) {
		http.Error(w, "temporarily unavailable", http.StatusInternalServerError)
		return
	}
	if !found {
		http.Error(w, "That is no longer there.", http.StatusNotFound)
		return
	}
	if owner == reporter {
		http.Error(w, "That is yours.", http.StatusBadRequest)
		return
	}

	// One open report per person per thing: tapping Report twice is one
	// report, not two people.
	if open, err := openReportBy(reporter, payload.TargetType, targetID); queryFailed(
		"whether account "+payload.ReporterID+" already reported "+payload.TargetType+" "+payload.TargetID,
		"a second report may be kept", err) {
		// carried on: a duplicate is better than a lost report
	} else if open > 0 {
		writeJSON(w, http.StatusOK, map[string]any{"id": strconv.Itoa(open), "alreadyReported": true})
		return
	}

	report, err := CreateReport(payload, owner)
	if err != nil {
		log.Printf("report: %s %s from account %s could not be kept: %v",
			payload.TargetType, payload.TargetID, payload.ReporterID, err)
		http.Error(w, "Failed to create report", http.StatusInternalServerError)
		return
	}
	log.Printf("report: account %s reported %s %s (%s); it waits on /admin/reports",
		payload.ReporterID, payload.TargetType, payload.TargetID, payload.Reason)

	// Tier 1.2: on a high-severity report (block/abuse/harassment/hate), treat
	// the target creator as blocked for the reporter so their feed stops
	// serving that creator immediately. Lower-severity reasons don't trigger
	// a block — they go through normal moderation.
	ownerID := strconv.Itoa(owner)
	if isHardBlockReason(payload.Reason) {
		go MarkBlocked(payload.ReporterID, ownerID)
	}

	// A report counts against the target's engagement-quality trust
	// multiplier (flags-against penalty in computeEngagementQuality).
	// Drop their cached multiplier so the next trending-weight read
	// reflects the report immediately instead of after the 10-min TTL.
	go invalidateEngagementQuality(ownerID)

	// Drop the content's cached two-tower embedding so the next ranker pass
	// rebuilds it from current metadata (which moderation may have changed,
	// e.g. category re-tagging or visibility flip). Cheap and best-effort.
	if payload.TargetType == "challenge" {
		go invalidateContentEmbedding(payload.TargetID)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(report)
}

// isHardBlockReason decides whether a report reason is severe enough that the
// reporter should have the target creator hidden from their feed immediately.
// Matches on a small set of known-severe reasons; everything else is soft.
func isHardBlockReason(reason string) bool {
	switch reason {
	case "block", "abuse", "harassment", "hate", "threats", "sexual_content", "violence", "child_safety":
		return true
	}
	return false
}
