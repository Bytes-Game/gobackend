package main

import (
	"encoding/json"
	"net/http"
)

// HandleFollowEvent handles the logic for when a user follows another user.
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

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"message": "Unfollow event processed successfully"})
}
