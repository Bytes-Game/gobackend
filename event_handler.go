package main

import (
	"encoding/json"
	"net/http"
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
	payload.FollowerID = authUserID(r)

	// Per-user follow rate-limit. Catches follow-bombing without
	// blocking organic follow flurries (burst is 5 in actionLimitTable).
	if !allowAction(payload.FollowerID, "follow") {
		writeRateLimited(w, "follow")
		return
	}

	if err := ProcessFollowEvent(payload); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// After successfully processing the follow event, send a notification.
	go SendFollowNotification(payload)

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
		http.Error(w, err.Error(), http.StatusInternalServerError)
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
		http.Error(w, "Failed to record watch event: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Fire a server-side prefetch hint over WebSocket so the client
	// can warm a VideoPlayerController for the user's next likely
	// reel BEFORE they swipe. Async + best-effort — never blocks the
	// watch-event ack. See next_reel_hint.go for throttle + filtering.
	if payload.ContentType == "challenge" && payload.ContentID != "" {
		go SendNextReelHint(payload.UserID, payload.ContentID)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]string{"message": "Watch event recorded"})
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

	if payload.ReporterID == "" || payload.TargetID == "" || payload.Reason == "" {
		http.Error(w, "reporterId, targetId, and reason are required", http.StatusBadRequest)
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

	report, err := CreateReport(payload)
	if err != nil {
		http.Error(w, "Failed to create report: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Tier 1.2: on a high-severity report (block/abuse/harassment/hate), treat
	// the target creator as blocked for the reporter so their feed stops
	// serving that creator immediately. Lower-severity reasons don't trigger
	// a block — they go through normal moderation.
	if isHardBlockReason(payload.Reason) {
		go MarkBlocked(payload.ReporterID, payload.TargetID)
	}

	// A report counts against the target's engagement-quality trust
	// multiplier (flags-against penalty in computeEngagementQuality).
	// Drop their cached multiplier so the next trending-weight read
	// reflects the report immediately instead of after the 10-min TTL.
	go invalidateEngagementQuality(payload.TargetID)

	// Drop the content's cached two-tower embedding so the next ranker pass
	// rebuilds it from current metadata (which moderation may have changed,
	// e.g. category re-tagging or visibility flip). Cheap and best-effort.
	if payload.TargetType == "post" || payload.TargetType == "challenge" || payload.TargetType == "" {
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
	case "block", "abuse", "harassment", "hate", "threats", "sexual_content", "violence":
		return true
	}
	return false
}
