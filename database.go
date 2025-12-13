package main

import (
	"encoding/json"
	"net/http"
	"sort"
	"strings"
)

// SearchUser defines the user data that is returned in search results.
// It's a subset of the main User struct.
type SearchUser struct {
	Username string `json:"username"`
	Name     string `json:"name"`
}

// WeightedUser is a helper struct to hold a user and their calculated search score.
type WeightedUser struct {
	User  User
	Score float64
}

// SearchResponse is the structure for the final JSON response.
type SearchResponse struct {
	Results []SearchUser `json:"results"`
	Total   int          `json:"total"`
}

func SearchHandler(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query().Get("q")

	if query == "" {
		http.Error(w, "Missing search query parameter 'q'", http.StatusBadRequest)
		return
	}

	var weightedUsers []WeightedUser

	// Iterate through all users in the database.
	for _, user := range users {
		// Calculate a score based on the query.
		score := calculateScore(user, query)
		// If the score is greater than 0, it's a match.
		if score > 0 {
			weightedUsers = append(weightedUsers, WeightedUser{User: user, Score: score})
		}
	}

	// Sort users from highest score to lowest.
	sort.Slice(weightedUsers, func(i, j int) bool {
		return weightedUsers[i].Score > weightedUsers[j].Score
	})

	// Convert the sorted users into the simplified SearchUser format.
	resultUsers := make([]SearchUser, len(weightedUsers))
	for i, wu := range weightedUsers {
		resultUsers[i] = SearchUser{
			Username: wu.User.Username,
			Name:     wu.User.Name,
		}
	}

	// Create the final response object.
	response := SearchResponse{
		Results: resultUsers,
		Total:   len(resultUsers),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// calculateScore gives a simple score based on matching the username or name.
func calculateScore(user User, query string) float64 {
	var score float64
	query = strings.ToLower(query)

	// Higher score for matching the username.
	if strings.Contains(strings.ToLower(user.Username), query) {
		score += 10.0
	}
	// Lower score for matching the full name.
	if strings.Contains(strings.ToLower(user.Name), query) {
		score += 5.0
	}

	return score
}
