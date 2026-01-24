package main

import "time"

// Post represents a single post made by a user, matching the client's expectations.
type Post struct {
	ID        string    `json:"id"`
	URL       string    `json:"url"`
	Caption   string    `json:"caption"`
	Timestamp time.Time `json:"timestamp"`
}

// User represents a user's profile data, with the Posts field corrected.
type User struct {
	Username      string   `json:"username"`
	password      string   `json:"-"` // The '-' tag prevents the password from being serialized into JSON.
	FullName      string   `json:"fullName"`
	Caption       string   `json:"caption"`
	Followers     int      `json:"followers"`
	Following     int      `json:"following"`
	Posts         []Post   `json:"posts"` // Corrected: This is now a slice of Post objects.
	Wins          int      `json:"wins"`
	Losses        int      `json:"losses"`
	League        string   `json:"league"`
	FollowingList []string `json:"followingList"`
}

// SearchResponse is a temporary struct for the search handler.
type SearchResponse struct {
	Results []User `json:"results"`
	Total   int    `json:"total"`
}
