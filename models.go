package main

// PostModel represents a single post made by a user.
// This is needed to match the Flutter frontend's data structure.
type PostModel struct {
	ImageURL string `json:"imageUrl"`
	Caption  string `json:"caption"`
}

// User is the central struct for a user's profile data.
// The 'Posts' field is now a slice of PostModel to match the frontend.
type User struct {
	Username      string      `json:"username"`
	password      string      // This field is unexported and will not be sent in JSON responses.
	FullName      string      `json:"fullName"`
	Caption       string      `json:"caption"`
	Followers     int         `json:"followers"`
	Following     int         `json:"following"`
	Posts         []PostModel `json:"posts"` // Correctly matches the Flutter List<PostModel>
	Wins          int         `json:"wins"`
	Losses        int         `json:"losses"`
	League        string      `json:"league"`
	FollowingList []string    `json:"followingList"`
}

// SearchResponse defines the structure for the /search endpoint's JSON response.
type SearchResponse struct {
	Results []User `json:"results"`
	Total   int    `json:"total"`
}
