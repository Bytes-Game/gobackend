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

	posts, hasMore := GetPostsPaginated(page, limit)
	if posts == nil {
		posts = []Post{}
	}

	resp := FeedResponse{Posts: posts, Page: page, HasMore: hasMore}
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

// HomeFeedHandler returns the mixed home feed (3 challenges + 1 post repeating).
//
// GET / api/v1/home
func HomeFeedHandler(w http.ResponseWriter, r *http.Request) {
	items := GetHomeFeed()
	if items == nil {
		items = []HomeFeedItem{}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(items)
}
