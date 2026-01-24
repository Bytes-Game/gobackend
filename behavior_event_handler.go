package main

import (
	"encoding/json"
	"log"
	"net/http"
	"time"
)

// BehaviorEvent is a generic struct to capture any user interaction from the frontend.
// The `Data` field can hold different JSON structures depending on the `EventType`.
type BehaviorEvent struct {
	EventType string          `json:"event_type"`
	Username  string          `json:"username"`
	Data      json.RawMessage `json:"data"`
}

// BehaviorEventHandler processes all user behavior events sent from the frontend.
func BehaviorEventHandler(w http.ResponseWriter, r *http.Request) {
	var event BehaviorEvent
	if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	log.Printf("Received behavior event: %s for user: %s", event.EventType, event.Username)

	// Use a switch to handle different event types.
	switch event.EventType {
	case "watch":
		var watchData struct {
			VideoID       string  `json:"videoId"`
			WatchTime     float64 `json:"watchTime"`
			VideoDuration float64 `json:"videoDuration"`
		}
		if err := json.Unmarshal(event.Data, &watchData); err != nil {
			log.Printf("Error unmarshalling watch data: %v", err)
			return
		}
		UpdateWatchMetrics(event.Username, watchData.VideoID, watchData.WatchTime, watchData.VideoDuration)

	case "like":
		var interactionData struct {
			VideoCategory string `json:"videoCategory"`
		}
		if err := json.Unmarshal(event.Data, &interactionData); err != nil {
			log.Printf("Error unmarshalling like data: %v", err)
			return
		}
		UpdateInteractionBehavior(event.Username, "like", interactionData.VideoCategory)

	case "comment":
		var interactionData struct {
			VideoCategory string `json:"videoCategory"`
		}
		if err := json.Unmarshal(event.Data, &interactionData); err != nil {
			log.Printf("Error unmarshalling comment data: %v", err)
			return
		}
		UpdateInteractionBehavior(event.Username, "comment", interactionData.VideoCategory)

	case "share":
		var interactionData struct {
			VideoCategory string `json:"videoCategory"`
		}
		if err := json.Unmarshal(event.Data, &interactionData); err != nil {
			log.Printf("Error unmarshalling share data: %v", err)
			return
		}
		UpdateInteractionBehavior(event.Username, "share", interactionData.VideoCategory)

	case "dislike":
		var interactionData struct {
			VideoCategory string `json:"videoCategory"`
		}
		if err := json.Unmarshal(event.Data, &interactionData); err != nil {
			log.Printf("Error unmarshalling dislike data: %v", err)
			return
		}
		UpdateInteractionBehavior(event.Username, "dislike", interactionData.VideoCategory)

	case "save":
		var interactionData struct {
			VideoCategory string `json:"videoCategory"`
		}
		if err := json.Unmarshal(event.Data, &interactionData); err != nil {
			log.Printf("Error unmarshalling save data: %v", err)
			return
		}
		UpdateInteractionBehavior(event.Username, "save", interactionData.VideoCategory)

	case "app_close":
		var appCloseData struct {
			LastVideoCategory string `json:"lastVideoCategory"`
		}
		if err := json.Unmarshal(event.Data, &appCloseData); err != nil {
			log.Printf("Error unmarshalling app_close data: %v", err)
			return
		}
		UpdateRageQuitTriggers(event.Username, appCloseData.LastVideoCategory)

	case "user_lost_game":
		go MonitorPostLossEngagement(event.Username)

	case "revenge_play":
		UpdateRevengePlayProbability(event.Username)

	default:
		log.Printf("Unknown event type received: %s", event.EventType)
	}

	w.WriteHeader(http.StatusAccepted)
}

func UpdateWatchMetrics(username string, videoID string, watchTime float64, videoDuration float64) {
	usersDBMu.Lock()
	defer usersDBMu.Unlock()

	for i, user := range users {
		if user.Username == username {
			// Calculate new AvgWatchTime (simple moving average)
			currentAvgWatchTime := user.Engagement.AvgWatchTime
			newAvgWatchTime := (currentAvgWatchTime*9 + watchTime) / 10
			users[i].Engagement.AvgWatchTime = newAvgWatchTime

			// Calculate new WatchCompletionRate (simple moving average)
			completionRate := watchTime / videoDuration
			currentCompletionRate := user.Engagement.WatchCompletionRate
			newCompletionRate := (currentCompletionRate*9 + completionRate) / 10
			users[i].Engagement.WatchCompletionRate = newCompletionRate

			// Update SkipSpeed if the video was skipped
			if watchTime < 2.0 { // Assuming a skip is a watch time of less than 2 seconds
				currentSkipSpeed := user.Engagement.SkipSpeed
				newSkipSpeed := (currentSkipSpeed*9 + watchTime) / 10
				users[i].Engagement.SkipSpeed = newSkipSpeed
			}

			// Update RewatchRate
			tempUserData := GetTempUserData(username)
			if tempUserData.WatchedVideos[videoID] {
				users[i].Engagement.RewatchRate = (user.Engagement.RewatchRate + 1) / 2
			} else {
				tempUserData.WatchedVideos[videoID] = true
			}

			log.Printf("Updated watch metrics for user %s", username)
			return
		}
	}

	log.Printf("User %s not found for watch metrics update", username)
}

func UpdateInteractionBehavior(username string, interactionType string, videoCategory string) {
	usersDBMu.Lock()
	defer usersDBMu.Unlock()

	for i, user := range users {
		if user.Username == username {
			switch interactionType {
			case "like":
				users[i].Engagement.LikeBehavior = videoCategory
			case "comment":
				users[i].Engagement.CommentBehavior = videoCategory
			case "share":
				users[i].Engagement.ShareBehavior = videoCategory
			case "dislike":
				users[i].Engagement.DislikeBehavior = videoCategory
			case "save":
				users[i].Engagement.SavesBehavior = videoCategory
			}
			log.Printf("Updated %s behavior for user %s", interactionType, username)
			return
		}
	}
}

func UpdateRageQuitTriggers(username string, lastVideoCategory string) {
	usersDBMu.Lock()
	defer usersDBMu.Unlock()

	for i, user := range users {
		if user.Username == username {
			users[i].Engagement.RageQuitTriggers = append(user.Engagement.RageQuitTriggers, lastVideoCategory)
			log.Printf("Updated rage quit triggers for user %s", username)
			return
		}
	}
}

func MonitorPostLossEngagement(username string) {
	initialEngagement := GetCurrentUserEngagement(username)
	time.Sleep(10 * time.Minute)
	finalEngagement := GetCurrentUserEngagement(username)

	engagementDrop := (initialEngagement - finalEngagement) / initialEngagement

	usersDBMu.Lock()
	defer usersDBMu.Unlock()

	for i, user := range users {
		if user.Username == username {
			users[i].Engagement.PostLossEngagementDrop = engagementDrop
			log.Printf("Updated post-loss engagement drop for user %s", username)
			return
		}
	}
}

func GetCurrentUserEngagement(username string) float64 {
	usersDBMu.Lock()
	defer usersDBMu.Unlock()

	for _, user := range users {
		if user.Username == username {
			return (user.Engagement.AvgWatchTime + user.Engagement.WatchCompletionRate) / 2
		}
	}
	return 0
}

func UpdateRevengePlayProbability(username string) {
	usersDBMu.Lock()
	defer usersDBMu.Unlock()

	tempUserData := GetTempUserData(username)
	tempUserData.TotalLosses++
	tempUserData.RevengePlays++

	for i, user := range users {
		if user.Username == username {
			users[i].Engagement.RevengePlayProbability = float64(tempUserData.RevengePlays) / float64(tempUserData.TotalLosses)
			log.Printf("Updated revenge play probability for user %s", username)
			return
		}
	}
}
