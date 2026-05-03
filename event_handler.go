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

	if payload.ContentType == "" || payload.ContentID == "" || payload.UserID == "" {
		http.Error(w, "userId, contentId, and contentType are required", http.StatusBadRequest)
		return
	}

	if err := RecordWatchEvent(payload); err != nil {
		http.Error(w, "Failed to record watch event: "+err.Error(), http.StatusInternalServerError)
		return
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

	if payload.ReporterID == "" || payload.TargetID == "" || payload.Reason == "" {
		http.Error(w, "reporterId, targetId, and reason are required", http.StatusBadRequest)
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

// HandleUnblockEvent reverses a previous block so the creator can re-appear
// in the user's feed. POST /api/v1/unblock { userId, creatorId }.
func HandleUnblockEvent(w http.ResponseWriter, r *http.Request) {
	var payload struct {
		UserID    string `json:"userId"`
		CreatorID string `json:"creatorId"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "Invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}
	if payload.UserID == "" || payload.CreatorID == "" {
		http.Error(w, "userId and creatorId are required", http.StatusBadRequest)
		return
	}
	go UnmarkBlocked(payload.UserID, payload.CreatorID)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ok"}`))
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
