package main

import "time"

type Post struct {
	Type    string `json:"type"` // "image" or "video"
	URL     string `json:"url"`
	Caption string `json:"caption"`
}

type InteractionStats struct {
	Likes    int `json:"likes"`
	Searches int `json:"searches"`
	DMs      int `json:"dms"`
}

type User struct {
	Username          string           `json:"username"`
	FullName          string           `json:"fullName"`
	League            string           `json:"league"`
	Caption           string           `json:"caption"`
	PostsCount        int              `json:"postsCount"`
	Wins              int              `json:"wins"`
	Losses            int              `json:"losses"`
	Followers         int              `json:"followers"`
	Following         int              `json:"following"`
	Posts             []Post           `json:"posts"`
	Location          string           `json:"location,omitempty"`
	LastLogin         time.Time        `json:"lastLogin,omitempty"`
	MutualConnections []string         `json:"mutualConnections,omitempty"`
	Interactions      InteractionStats `json:"interactions,omitempty"`
}

// SearchUser is a lightweight version of the User struct for search results

type SearchUser struct {
	Username  string `json:"username"`
	FullName  string `json:"fullName"`
	League    string `json:"league"`
	Followers int    `json:"followers"`
}

var users = []User{
	{
		Username:   "player1",
		FullName:   "John Doe",
		League:     "Gold",
		Caption:    " aspiring pro player!",
		PostsCount: 3,
		Wins:       150,
		Losses:     95,
		Followers:  1200,
		Following:  250,
		Posts: []Post{
			{Type: "image", URL: "https://images.unsplash.com/photo-1503023345310-bd7c1de61c7d?auto=format&fit=crop&w=800&q=80", Caption: "First post!"},
			{Type: "video", URL: "https://filesamples.com/samples/video/mp4/sample_640x360.mp4", Caption: "Highlight reel"},
			{Type: "image", URL: "https://images.unsplash.com/photo-1503023345310-bd7c1de61c7d?auto=format&fit=crop&w=800&q=80", Caption: "New setup"},
		},
		Location:  "New York, NY",
		LastLogin: time.Now().Add(-24 * time.Hour),
	},
	{
		Username:   "gamer_girl",
		FullName:   "Jane Smith",
		League:     "Diamond",
		Caption:    "Just for fun ",
		PostsCount: 5,
		Wins:       300,
		Losses:     120,
		Followers:  5600,
		Following:  500,
		Posts: []Post{
			{Type: "image", URL: "https://images.unsplash.com/photo-1503023345310-bd7c1de61c7d?auto=format&fit=crop&w=800&q=80", Caption: "Enjoying the game"},
			{Type: "image", URL: "https://images.unsplash.com/photo-1503023345310-bd7c1de61c7d?auto=format&fit=crop&w=800&q=80", Caption: "Team up?"},
			{Type: "video", URL: "https://filesamples.com/samples/video/mp4/sample_640x360.mp4", Caption: "Funny moments"},
			{Type: "image", URL: "https://images.unsplash.com/photo-1503023345310-bd7c1de61c7d?auto=format&fit=crop&w=800&q=80", Caption: "My cat watching me play"},
			{Type: "image", URL: "https://images.unsplash.com/photo-1503023345310-bd7c1de61c7d?auto=format&fit=crop&w=800&q=80", Caption: "Just hit Diamond!"},
		},
		Location:          "London, UK",
		LastLogin:         time.Now().Add(-72 * time.Hour),
		MutualConnections: []string{"player1"},
		Interactions:      InteractionStats{Likes: 50, Searches: 10, DMs: 2},
	},
	{
		Username:   "pro_streamer",
		FullName:   "Alex Johnson",
		League:     "Challenger",
		Caption:    "Streaming daily at twitch.tv/pro_streamer",
		PostsCount: 2,
		Wins:       500,
		Losses:     50,
		Followers:  100000,
		Following:  100,
		Posts: []Post{
			{Type: "video", URL: "https://filesamples.com/samples/video/mp4/sample_640x360.mp4", Caption: "1v5 clutch"},
			{Type: "video", URL: "https://filesamples.com/samples/video/mp4/sample_640x360.mp4", Caption: "Tournament win!"},
		},
		LastLogin: time.Now().Add(-5 * time.Minute),
	},
}


