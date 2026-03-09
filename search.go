package main

import (
	"encoding/json"
	"net/http"
	"sort"
	"strings"
)

// scoreduser is a helper struct just for sorting users with their search score.
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

	// Fetch all users from the database
	allUsers := GetAllUsers()
	for _, user := range allUsers {
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
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// calculatescore calculates a relevance score for a user based on a search query.
// It uses exact matching, prefix matching, contains matching, Levenshtein distance
// for typo tolerance, and league matching.
func calculateScore(user User, query string) float64 {
	var score float64
	lowerQuery := strings.ToLower(strings.TrimSpace(query))
	lowerUsername := strings.ToLower(user.Username)
	lowerFullName := strings.ToLower(user.FullName)
	lowerLeague := strings.ToLower(user.League)

	// ——1. Exact username match (highest priority) ——
	if lowerUsername == lowerQuery {
		score += 100.0
	}

	// ——2. Prefix match on username (e.g.,"play"→"player1") ——
	if strings.HasPrefix(lowerUsername, lowerQuery) {
		score += 50.0
	}

	// ——3. Fuzzy match on full username via Levenshtein distance ——
	dist := levenshteinDistance(lowerQuery, lowerUsername)
	maxLen := len(lowerQuery)
	if len(lowerUsername) > maxLen {
		maxLen = len(lowerUsername)
	}
	if maxLen > 0 {
		similarity := 1.0 - float64(dist)/float64(maxLen)
		if similarity >= 0.45 { // tolerant threshold for typos
			score += similarity * 35.0
		}
	}

	// —— 4. Token-based matching ——
	queryTokens := strings.Fields(lowerQuery)

	for _, token := range queryTokens {
		// Direct substring in username
		if strings.Contains(lowerUsername, token) {
			score += 10.0
		}
		// Direct substring in full name
		if strings.Contains(lowerFullName, token) {
			score += 5.0
		}
		// Direct substring in league
		if strings.Contains(lowerLeague, token) {
			score += 3.0
		}

		// Fuzzy match each query token against individual name tokens
		nameTokens := strings.Fields(lowerFullName)
		for _, nameToken := range nameTokens {
			tokenDist := levenshteinDistance(token, nameToken)
			tokenMaxLen := len(token)
			if len(nameToken) > tokenMaxLen {
				tokenMaxLen = len(nameToken)
			}
			if tokenMaxLen > 0 {
				sim := 1.0 - float64(tokenDist)/float64(tokenMaxLen)
				if sim >= 0.55 { // e.g."playr" vs "player"≈ 0.83
					score += sim * 8.0
				}
			}
		}

		// Fuzzy token vs username
		tokenDistU := levenshteinDistance(token, lowerUsername)
		tokenMaxU := len(token)
		if len(lowerUsername) > tokenMaxU {
			tokenMaxU = len(lowerUsername)
		}
		if tokenMaxU > 0 {
			simU := 1.0 - float64(tokenDistU)/float64(tokenMaxU)
			if simU >= 0.5 {
				score += simU * 6.0
			}
		}
	}

	// —— 5. Prefix match on full name ——
	if strings.HasPrefix(lowerFullName, lowerQuery) {
		score += 15.0
	}

	// —— 6. Follower bonus ——
	score += float64(user.Followers) * 0.01

	return score
}

// levenshteinDistance computes the minimum edit distance between two strings
// This allows the search to tolerate typos (e.g."playar" still matches "player").
func levenshteinDistance(a, b string) int {
	la, lb := len(a), len(b)
	if la == 0 {
		return lb
	}
	if lb == 0 {
		return la
	}

	matrix := make([][]int, la+1)
	for i := range matrix {
		matrix[i] = make([]int, lb+1)
		matrix[i][0] = i
	}
	for j := 0; j <= lb; j++ {
		matrix[0][j] = j
	}

	for i := 1; i <= la; i++ {
		for j := 1; j <= lb; j++ {
			cost := 1
			if a[i-1] == b[j-1] {
				cost = 0
			}
			del := matrix[i-1][j] + 1
			ins := matrix[i][j-1] + 1
			sub := matrix[i-1][j-1] + cost
			best := del
			if ins < best {
				best = ins
			}
			if sub < best {
				best = sub
			}
			matrix[i][j] = best
		}
	}
	return matrix[la][lb]
}
