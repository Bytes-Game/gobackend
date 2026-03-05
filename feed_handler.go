package main

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
)

// FeedHandler returns a paginated list of posts
//
// GET /api/v1/feed?page=1&limit=20
//
// Defaults: page=1, limit=20 (capped at 50)
func FeedHandler(w http.ResponseWriter, r *http.Request) {
	pageStr := r.URL.Query().Get("page")
	limitStr := r.URL.Query().Get("limit")

	page := 1
	limit := 20

	if p, err := strconv.Atoi(pageStr); err == nil && p > 0 {
		page = p
	}
	if l, err := strconv.Atoi(limitStr); err == nil && l > 0 && l <= 50 {
		limit = l
	}

	allPosts := GetAllPosts()
	start := (page - 1) * limit

	// Beyond the range → return empty list.
	if start >= len(allPosts) {
		resp := FeedResponse{Posts: []Post{}, Page: page, HasMore: false}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
		return
	}

	end := start + limit
	if end > len(allPosts) {
		end = len(allPosts)
	}

	paginated := allPosts[start:end]
	hasMore := end < len(allPosts)

	resp := FeedResponse{Posts: paginated, Page: page, HasMore: hasMore}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// UserPostsHandler returns all posts by a given user.
//
// GET /api/v1/posts/{userID}
func UserPostsHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	userID := vars["userId"]

	userPosts := GetPostsByUserID(userID)
	if userPosts == nil {
		userPosts = []Post{}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(userPosts)
}
