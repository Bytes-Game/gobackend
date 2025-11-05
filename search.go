package main

import (
	"encoding/json"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

type WeightedUser struct {
	User  User
	Score float64
}

type SearchResponse struct {
	Results []SearchUser `json:"results"`
	Total   int          `json:"total"`
	Page    int          `json:"page"`
}

func SearchHandler(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query().Get("q")
	userLocation := r.URL.Query().Get("location")
	pageStr := r.URL.Query().Get("page")
	pageSizeStr := r.URL.Query().Get("pageSize")

	if query == "" {
		http.Error(w, "Missing search query", http.StatusBadRequest)
		return
	}

	page, err := strconv.Atoi(pageStr)
	if err != nil || page < 1 {
		page = 1
	}

	pageSize, err := strconv.Atoi(pageSizeStr)
	if err != nil || pageSize < 1 {
		pageSize = 10 // Default page size
	}

	var weightedUsers []WeightedUser

	for _, user := range users {
		score := calculateScore(user, query, userLocation)
		if score > 0 {
			weightedUsers = append(weightedUsers, WeightedUser{User: user, Score: score})
		}
	}

	sort.Slice(weightedUsers, func(i, j int) bool {
		return weightedUsers[i].Score > weightedUsers[j].Score
	})

	totalResults := len(weightedUsers)
	start := (page - 1) * pageSize
	end := start + pageSize
	if start > totalResults {
		start = totalResults
	}
	if end > totalResults {
		end = totalResults
	}

	pagedUsers := weightedUsers[start:end]

	resultUsers := make([]SearchUser, len(pagedUsers))
	for i, wu := range pagedUsers {
		resultUsers[i] = SearchUser{
			Username:  wu.User.Username,
			FullName:  wu.User.FullName,
			League:    wu.User.League,
			Followers: wu.User.Followers,
		}
	}

	response := SearchResponse{
		Results: resultUsers,
		Total:   totalResults,
		Page:    page,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func calculateScore(user User, query, userLocation string) float64 {
	var score float64
	query = strings.ToLower(query)

	if strings.Contains(strings.ToLower(user.Username), query) {
		score += 10.0
	}
	if strings.Contains(strings.ToLower(user.FullName), query) {
		score += 10.0
	}
	if strings.Contains(strings.ToLower(user.Caption), query) {
		score += 2.0
	}

	score += float64(user.Followers) * 0.5
	score += float64(user.Wins) * 0.3
	score += float64(user.PostsCount) * 0.2

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

	score += float64(len(user.MutualConnections)) * 3.0

	score += float64(user.Interactions.Likes) * 0.2
	score += float64(user.Interactions.Searches) * 0.5
	score += float64(user.Interactions.DMs) * 1.0

	return score
}
