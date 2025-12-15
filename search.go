package main

import (
	"encoding/json"
	"net/http"
	"sort"
	"strings"
)

// scoredUser is a helper struct just for sorting users with their search score.
// It is not exported and only used within this file.
type scoredUser struct {
	User  User
	Score float64
}

// SearchHandler handles requests to the /search endpoint.
func SearchHandler(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query().Get("q")

	if query == "" {
		http.Error(w, "Missing search query parameter 'q'", http.StatusBadRequest)
		return
	}

	var scoredUsers []scoredUser

	// Use the global 'users' slice from database.go
	for _, user := range users {
		score := calculateScore(user, query)
		if score > 0 {
			scoredUsers = append(scoredUsers, scoredUser{User: user, Score: score})
		}
	}

	// Sort users based on their score in descending order.
	sort.Slice(scoredUsers, func(i, j int) bool {
		return scoredUsers[i].Score > scoredUsers[j].Score
	})

	// Extract the User objects from the sorted list for the response.
	resultUsers := make([]User, len(scoredUsers))
	for i, su := range scoredUsers {
		resultUsers[i] = su.User
	}

	// The SearchResponse struct is defined in models.go
	response := SearchResponse{
		Results: resultUsers,
		Total:   len(resultUsers),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// calculateScore calculates a relevance score for a user based on a search query.
// It tokenizes the query and gives points for matches in username and full name.
func calculateScore(user User, query string) float64 {
	var score float64
	lowerQuery := strings.ToLower(query)
	lowerUsername := strings.ToLower(user.Username)
	lowerFullName := strings.ToLower(user.FullName)

	// Strongest bonus for an exact username match
	if lowerUsername == lowerQuery {
		score += 100.0
	}

	// Tokenize the query to search for individual words
	queryTokens := strings.Fields(lowerQuery)

	for _, token := range queryTokens {
		// Bonus for token matches in username
		if strings.Contains(lowerUsername, token) {
			score += 10.0
		}
		
		// Weaker bonus for token matches in full name
		if strings.Contains(lowerFullName, token) {
			score += 5.0
		}
	}

	// Add a small bonus for having more followers
	score += float64(user.Followers) * 0.01

	return score
}
