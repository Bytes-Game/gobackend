package main

import (
	"encoding/json"
	"log"
	"net/http"
)

// WatchTimeEvent represents the data sent from the frontend for a single video view.
// We expect to receive a list of these events.
type WatchTimeEvent struct {
	VideoID   string  `json:"video_id"`
	WatchTime float64 `json:"watch_time"`
	Username  string  `json:"username"` // We need to know which user this event belongs to
}

// TrackWatchTimeHandler receives a batch of watch time events from the frontend.
func TrackWatchTimeHandler(w http.ResponseWriter, r *http.Request) {
	var events []WatchTimeEvent
	if err := json.NewDecoder(r.Body).Decode(&events); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// Process each event
	for _, event := range events {
		UpdateUserEngagement(event.Username, event.WatchTime)
	}

	w.WriteHeader(http.StatusAccepted) // 202 Accepted is a good response for async processing
}

func UpdateUserEngagement(username string, watchTime float64) {
	usersDBMu.Lock()
	defer usersDBMu.Unlock()

	for i, user := range users {
		if user.Username == username {
			// Calculate the new average watch time
			// This is a simple weighted average. You could use a more sophisticated algorithm here.
			currentAvg := user.Engagement.AvgWatchTime
			// A simple moving average
			newAvg := (currentAvg*9 + watchTime) / 10

			users[i].Engagement.AvgWatchTime = newAvg

			log.Printf("Updated AvgWatchTime for user %s to %f", username, newAvg)
			return
		}
	}

	log.Printf("User %s not found for engagement update", username)
}
