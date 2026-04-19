package main

import (
	"encoding/json"
	"net/http"
	"sort"
	"strconv"
	"strings"
)

// scoredUser is a helper struct just for sorting users with their search score.
type scoredUser struct {
	User  User
	Score float64
}

// UnifiedSearchResponse wraps search results for challenges and users.
type UnifiedSearchResponse struct {
	Challenges []Challenge `json:"challenges"`
	Users      []User      `json:"users"`
}

// SearchHandler handles requests to the /search endpoint.
// Supports ?q=query&type=all|users|challenges
func SearchHandler(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query().Get("q")
	searchType := r.URL.Query().Get("type") // "all", "users", "challenges"
	userID := r.URL.Query().Get("userId")

	if query == "" {
		http.Error(w, "Missing search query parameter 'q'", http.StatusBadRequest)
		return
	}

	if searchType == "" {
		searchType = "all"
	}

	// Tier 1.4: capture the query into the user's recent-search LIST so the
	// feed ranker can bias toward matching categories/captions for 24h. Best
	// effort — a Redis miss doesn't affect the search response.
	if userID != "" {
		go RecordSearchQuery(userID, query)
	}

	// Try Meilisearch first
	if meili != nil {
		meiliResults := MeiliSearchAll(query, searchType)
		if meiliResults != nil {
			var challenges []Challenge
			var users []User

			for _, doc := range meiliResults {
				docType, _ := doc["_type"].(string)
				if docType == "challenge" {
					challenges = append(challenges, meiliDocToChallenge(doc))
				} else if docType == "user" {
					users = append(users, meiliDocToUser(doc))
				}
			}

			if challenges == nil {
				challenges = []Challenge{}
			}
			if users == nil {
				users = []User{}
			}

			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(UnifiedSearchResponse{
				Challenges: challenges,
				Users:      users,
			})
			return
		}
	}

	// Fallback: PostgreSQL search
	var challenges []Challenge
	var users []User

	if searchType == "all" || searchType == "users" {
		users = searchUsersFallback(query)
	}
	if searchType == "all" || searchType == "challenges" {
		challenges = searchChallengesFallback(query)
	}

	if challenges == nil {
		challenges = []Challenge{}
	}
	if users == nil {
		users = []User{}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(UnifiedSearchResponse{
		Challenges: challenges,
		Users:      users,
	})
}

// searchUsersFallback uses the existing Levenshtein-based user search.
func searchUsersFallback(query string) []User {
	var scoredUsers []scoredUser
	allUsers := GetAllUsers()
	for _, user := range allUsers {
		score := calculateScore(user, query)
		if score > 0 {
			scoredUsers = append(scoredUsers, scoredUser{User: user, Score: score})
		}
	}

	sort.Slice(scoredUsers, func(i, j int) bool {
		return scoredUsers[i].Score > scoredUsers[j].Score
	})

	if len(scoredUsers) > 20 {
		scoredUsers = scoredUsers[:20]
	}

	result := make([]User, len(scoredUsers))
	for i, su := range scoredUsers {
		result[i] = su.User
	}
	return result
}

// searchChallengesFallback searches challenges by title/creator using simple substring matching.
func searchChallengesFallback(query string) []Challenge {
	q := strings.ToLower(strings.TrimSpace(query))
	allChallenges := GetArenaChallenges()
	var results []Challenge

	for _, c := range allChallenges {
		title := strings.ToLower(c.Prefix + " " + c.Subject)
		creator := strings.ToLower(c.CreatorUsername)
		if strings.Contains(title, q) || strings.Contains(creator, q) {
			results = append(results, c)
		}
	}

	if len(results) > 20 {
		results = results[:20]
	}
	return results
}

// meiliDocToChallenge converts a Meilisearch hit to a Challenge.
func meiliDocToChallenge(doc map[string]interface{}) Challenge {
	return Challenge{
		ID:              toString(doc["id"]),
		CreatorID:       toString(doc["creatorId"]),
		CreatorUsername:  toString(doc["creatorUsername"]),
		CreatorLeague:   toString(doc["creatorLeague"]),
		Prefix:          toString(doc["prefix"]),
		Subject:         toString(doc["subject"]),
		Visibility:      toString(doc["visibility"]),
		Status:          toString(doc["status"]),
		Likes:           toInt(doc["likes"]),
		Views:           toInt(doc["views"]),
		ResponseCount:   toInt(doc["responseCount"]),
		VideoURL:        toString(doc["videoUrl"]),
		ThumbnailURL:    toString(doc["thumbnailUrl"]),
		CreatedAt:       toString(doc["createdAt"]),
	}
}

// meiliDocToUser converts a Meilisearch hit to a User.
func meiliDocToUser(doc map[string]interface{}) User {
	return User{
		ID:       toString(doc["id"]),
		Username: toString(doc["username"]),
		FullName: toString(doc["fullName"]),
		League:   toString(doc["league"]),
		Followers: toInt(doc["followers"]),
		Wins:     toInt(doc["wins"]),
		Losses:   toInt(doc["losses"]),
	}
}

func toString(v interface{}) string {
	if v == nil {
		return ""
	}
	switch val := v.(type) {
	case string:
		return val
	case float64:
		if val == float64(int(val)) {
			return strconv.Itoa(int(val))
		}
		return strconv.FormatFloat(val, 'f', -1, 64)
	default:
		return ""
	}
}

func toInt(v interface{}) int {
	if v == nil {
		return 0
	}
	switch val := v.(type) {
	case float64:
		return int(val)
	case int:
		return val
	default:
		return 0
	}
}

// calculateScore calculates a relevance score for a user based on a search query.
func calculateScore(user User, query string) float64 {
	var score float64
	lowerQuery := strings.ToLower(strings.TrimSpace(query))
	lowerUsername := strings.ToLower(user.Username)
	lowerFullName := strings.ToLower(user.FullName)
	lowerLeague := strings.ToLower(user.League)

	if lowerUsername == lowerQuery {
		score += 100.0
	}
	if strings.HasPrefix(lowerUsername, lowerQuery) {
		score += 50.0
	}

	dist := levenshteinDistance(lowerQuery, lowerUsername)
	maxLen := len(lowerQuery)
	if len(lowerUsername) > maxLen {
		maxLen = len(lowerUsername)
	}
	if maxLen > 0 {
		similarity := 1.0 - float64(dist)/float64(maxLen)
		if similarity >= 0.45 {
			score += similarity * 35.0
		}
	}

	queryTokens := strings.Fields(lowerQuery)
	for _, token := range queryTokens {
		if strings.Contains(lowerUsername, token) {
			score += 10.0
		}
		if strings.Contains(lowerFullName, token) {
			score += 5.0
		}
		if strings.Contains(lowerLeague, token) {
			score += 3.0
		}

		nameTokens := strings.Fields(lowerFullName)
		for _, nameToken := range nameTokens {
			tokenDist := levenshteinDistance(token, nameToken)
			tokenMaxLen := len(token)
			if len(nameToken) > tokenMaxLen {
				tokenMaxLen = len(nameToken)
			}
			if tokenMaxLen > 0 {
				sim := 1.0 - float64(tokenDist)/float64(tokenMaxLen)
				if sim >= 0.55 {
					score += sim * 8.0
				}
			}
		}

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

	if strings.HasPrefix(lowerFullName, lowerQuery) {
		score += 15.0
	}

	score += float64(user.Followers) * 0.01

	totalGames := user.Wins + user.Losses
	if totalGames > 0 {
		winRate := float64(user.Wins) / float64(totalGames)
		score += winRate * 5.0
		if totalGames > 50 {
			score += 3.0
		} else if totalGames > 20 {
			score += 1.5
		}
	}

	return score
}

// levenshteinDistance computes the minimum edit distance between two strings.
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
