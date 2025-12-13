package main

import "time"

// User is the central struct for a user's profile data.
// It is used for the database and for API responses.
type User struct {
	Username          string    `json:"username"`
	Password          string    `json:"password,omitempty"` // omitempty prevents it from being sent in JSON responses
	Name              string    `json:"name"`
	League            string    `json:"league,omitempty"`
	Followers         int       `json:"followers,omitempty"`
	Wins              int       `json:"wins,omitempty"`
	PostsCount        int       `json:"postsCount,omitempty"`
	Caption           string    `json:"caption,omitempty"`
	Location          string    `json:"location,omitempty"`
	LastLogin         time.Time `json:"lastLogin,omitempty"`
	MutualConnections []string  `json:"mutualConnections,omitempty"`
}

// SearchResponse defines the structure for the /search endpoint's JSON response.
type SearchResponse struct {
	Results []User `json:"results"`
	Total   int    `json:"total"`
}
