package main

// User represents a user's profile data, with the Posts field corrected.
type User struct {
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
