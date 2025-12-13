package main

import "time"

// User struct defines the data model for a user.
// This is the central definition used across the application.
type User struct {
	Username          string    `json:"username"`
	Password          string    `json:"password,omitempty"` // Should be hashed; omitempty for security
	Name              string    `json:"name"`
	League            string    `json:"league,omitempty"`
	Followers         int       `json:"followers,omitempty"`
	Wins              int       `json:"wins,omitempty"`
	PostsCount        int       `json:"postsCount,omitempty"`
	Caption           string    `json:"caption,omitempty"`
	Location          string    `json:"location,omitempty"`
	LastLogin         time.Time `json:"lastLogin,omitempty"`
	MutualConnections []string  `json:"mutualConnections,omitempty"`
	Interactions      struct {
		Likes    int `json:"likes,omitempty"`
		Searches int `json:"searches,omitempty"`
		DMs      int `json:"dms,omitempty"`
	} `json:"interactions,omitempty"`
}
