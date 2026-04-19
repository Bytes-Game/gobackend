package main

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
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

// HandleLikeFvent handles toggling a like on a post.
// POST /api/v1/like body:{ postId, userId, username }
func HandleLikeEvent(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST method is allowed", http.StatusMethodNotAllowed)
		return
	}

	var payload LikePayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "Invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}

	liked, newCount, post := ToggleLike(payload.PostID, payload.UserID)

	// Send like notification to the post author (only on like, not unlike)
	if liked && post.AuthorID != payload.UserID {
		go SendLikeNotification(payload.Username, post.AuthorUsername, post.Caption)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"liked":  liked,
		"likes":  newCount,
		"postId": payload.PostID,
	})
}

// HandleCommentEvent handles adding a comment to a post.
// POST /api/v1/comments body:{ postId, userId, username, text }
func HandleCommentEvent(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST method is allowed", http.StatusMethodNotAllowed)
		return
	}

	var payload CommentPayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "Invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}

	if payload.Text == "" {
		http.Error(w, "Comment text cannot be empty", http.StatusBadRequest)
		return
	}

	comment := AddComment(payload.PostID, payload.UserID, payload.Username, payload.Text)

	// Send notification to the post author (only if commenter != author)
	post, found := GetPostByID(payload.PostID)
	if found && post.AuthorID != payload.UserID {
		go SendCommentNotification(payload.Username, post.AuthorUsername, payload.Text, post.Caption)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(comment)
}

// GetCommentsHandler returns all comments for a given post.
// GET /api/v1/comments/{postId}
func GetCommentsHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	postID := vars["postId"]

	comments := GetComments(postID)
	if comments == nil {
		comments = []Comment{}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(comments)
}

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
