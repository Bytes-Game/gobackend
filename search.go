package main

import (
	"encoding/json"
	"net/http"
	"sort"
	"strings"
	"time"
)

// We no longer need a separate SearchUser struct, as the main User struct in models.go is sufficient.
// We also don't need a separate WeightedUser, we can do this on the fly.

type SearchResponse struct {
	Results []User `json:"results"`
	Total   int    `json:"total"`
}

// Helper struct for sorting users with their scores.
type scoredUser struct {
	User  User
	Score float64
}

func SearchHandler(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query().Get("q")
	userLocation := r.URL.Query().Get("location")

	if query == "" {
		http.Error(w, "Missing search query parameter 'q'", http.StatusBadRequest)
		return
	}

	var scoredUsers []scoredUser

	// Use the global 'users' slice from database.go
	for _, user := range users {
		score := calculateScore(user, query, userLocation)
		if score > 0 {
			scoredUsers = append(scoredUsers, scoredUser{User: user, Score: score})
		}
	}

	// Sort users based on their score in descending order.
	sort.Slice(scoredUsers, func(i, j int) bool {
		return scoredUsers[i].Score > scoredUsers[j].Score
	})

	// Extract the User objects from the sorted list.
	resultUsers := make([]User, len(scoredUsers))
	for i, su := range scoredUsers {
		resultUsers[i] = su.User
	}

	response := SearchResponse{
		Results: resultUsers,
		Total:   len(resultUsers),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// calculateScore now uses the rich User model from models.go
func calculateScore(user User, query, userLocation string) float64 {
	var score float64
	query = strings.ToLower(query)

	if strings.Contains(strings.ToLower(user.Username), query) {
		score += 10.0
	}
	if strings.Contains(strings.ToLower(user.Name), query) {
		score += 10.0 // Changed from FullName to Name
	}
	if strings.Contains(strings.ToLower(user.Caption), query) {
		score += 2.0
	}

	score += float64(user.Followers) * 0.5
	score += float64(user.Wins) * 0.3

	if user.Location != "" && userLocation != "" && strings.Contains(strings.ToLower(user.Location), strings.ToLower(userLocation)) {
		score += 5.0
	}

	if !user.LastLogin.IsZero() {
		hoursSinceLogin := time.Since(user.LastLogin).Hours()
		if hoursSinceLogin <= 24 {
			score += 10.0
		} else if hoursSinceLogin <= 168 {
			score += 5.0
		}
	}

	return score
}
