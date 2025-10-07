package main

type Post struct {
	Type    string `json:"type"` // "image" or "video"
	URL     string `json:"url"`
	Caption string `json:"caption"`
}

type User struct {
	Username    string `json:"username"`
	FullName    string `json:"fullName"`
	League      string `json:"league"`
	Caption     string `json:"caption"`
	PostsCount  int    `json:"postsCount"`
	Wins        int    `json:"wins"`
	Losses      int    `json:"losses"`
	Followers   int    `json:"followers"`
	Following   int    `json:"following"`
	Posts       []Post `json:"posts"`
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
			{Type: "image", URL: "https://example.com/post1.jpg", Caption: "First post!"},
			{Type: "video", URL: "https://example.com/post2.mp4", Caption: "Highlight reel"},
			{Type: "image", URL: "https://example.com/post3.jpg", Caption: "New setup"},
		},
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
			{Type: "image", URL: "https://example.com/gg_post1.jpg", Caption: "Enjoying the game"},
			{Type: "image", URL: "https://example.com/gg_post2.jpg", Caption: "Team up?"},
			{Type: "video", URL: "https://example.com/gg_post3.mp4", Caption: "Funny moments"},
			{Type: "image", URL: "https://example.com/gg_post4.jpg", Caption: "My cat watching me play"},
			{Type: "image", URL: "https://example.com/gg_post5.jpg", Caption: "Just hit Diamond!"},
		},
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
			{Type: "video", URL: "https://example.com/stream_highlight1.mp4", Caption: "1v5 clutch"},
			{Type: "video", URL: "https://example.com/stream_highlight2.mp4", Caption: "Tournament win!"},
		},
	},
}
