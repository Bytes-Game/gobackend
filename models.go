package main

// User is the central struct for a user's profile data.
// This struct has been updated to exactly match the Flutter UserModel.
type User struct {
	Username      string   `json:"username"`
	password      string   // This field is unexported and will not be sent in JSON responses.
	FullName      string   `json:"fullName"`
	Caption       string   `json:"caption"`
	Followers     int      `json:"followers"`
	Following     int      `json:"following"`
	Posts         int      `json:"posts"`
	Wins          int      `json:"wins"`
	Losses        int      `json:"losses"`
	League        string   `json:"league"`
	FollowingList []string `json:"followingList"`
}

// SearchResponse defines the structure for the /search endpoint's JSON response.
type SearchResponse struct {
	Results []User `json:"results"`
	Total   int    `json:"total"`
}
