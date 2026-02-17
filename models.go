package main

import "time"

// User represents a user's profile data, with the Posts field corrected.
type User struct {
	ID            string   `json:"id" bson:"_id,omitempty"` // Example for MongoDB
	Username      string   `json:"username"`
	password      string   `json:"-"` // The '-' tag prevents the password from being serialized into JSON.
	FullName      string   `json:"fullName"`
	Followers     int      `json:"followers"`
	FollowingList []string `json:"followingList"`
	Wins          int      `json:"wins"`
	Losses        int      `json:"losses"`
	League        string   `json:"league"`
}

// SearchResponse is a temporary struct for the search handler.
type SearchResponse struct {
	Results []User `json:"results"`
}

// Represents the incoming payload for a /follow request
type FollowEventPayload struct {
	FollowerID        string    `json:"followerId"`
	FollowerUsername  string    `json:"followerUsername"`
	FollowingID       string    `json:"followingId"`
	FollowingUsername string    `json:"followingUsername"`
	ClientTimestamp   time.Time `json:"clientTimestamp"` // Use time.Time for proper parsing
}

// Represents the incoming payload for an /unfollow request
type UnfollowEventPayload struct {
	UnfollowerID        string    `json:"unfollowerId"`
	UnfollowerUsername  string    `json:"unfollowerUsername"`
	UnfollowedID        string    `json:"unfollowedId"`
	UnfollowedUsername  string    `json:"unfollowedUsername"`
	ClientTimestamp     time.Time `json:"clientTimestamp"`
}

// Notification represents a message to be sent to a user.
type Notification struct {
	Type    string `json:"type"`
	Message string `json:"message"`
}
