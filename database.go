package main

import (
	"fmt"
	"sync"
	"time"
)

// users is the in-memory database for our application.
var users []User

// posts is the in-memory feed database.
var posts []Post

// comments is the in-memory comments database, keyed by post ID.
var comments = make(map[string][]Comment)
var commentsMu sync.Mutex
var commentIDCounter int

// usersDBMu is a mutex to make access to the users slice safe in a concurrent environment.
var usersDBMu sync.Mutex

// InitDatabase populates the in-memory user database with sample data.
// It now dynamically calculates the initial follower counts to ensure data consistency.
func InitDatabase() {
	usersDBMu.Lock()
	defer usersDBMu.Unlock()

	//  Initialize users without the `Followers` field, as it will be calculated dynamically.
	initialUsers := []User{
		{
			ID:            "1",
			Username:      "player1",
			password:      "pass1",
			FullName:      "Player One",
			Wins:          32,
			Losses:        18,
			League:        "Gold",
			FollowingList: []string{"2", "3", "4"},
		},
		{
			ID:            "2",
			Username:      "player2",
			password:      "pass2",
			FullName:      "Player Two",
			Wins:          120,
			Losses:        15,
			League:        "Diamond",
			FollowingList: []string{"1", "5"},
		},
		{
			ID:            "3",
			Username:      "player3",
			password:      "pass3",
			FullName:      "Player Three",
			Wins:          10,
			Losses:        5,
			League:        "Bronze",
			FollowingList: []string{"1", "2"},
		},
		{
			ID:            "4",
			Username:      "shadowstrike",
			password:      "pass4",
			FullName:      "Shadow Strike",
			Wins:          95,
			Losses:        30,
			League:        "Platinum",
			FollowingList: []string{"1", "2", "7"},
		},
		{
			ID:            "5",
			Username:      "blazerunner",
			password:      "pass5",
			FullName:      "Blaze Runner",
			Wins:          55,
			Losses:        40,
			League:        "Silver",
			FollowingList: []string{"2", "4", "6"},
		},
		{
			ID:            "6",
			Username:      "stormchaser",
			password:      "pass6",
			FullName:      "Storm Chaser",
			Wins:          78,
			Losses:        22,
			League:        "Gold",
			FollowingList: []string{"1", "5", "7"},
		},
		{
			ID:            "7",
			Username:      "frostbyte",
			password:      "pass7",
			FullName:      "Frost Byte",
			Wins:          140,
			Losses:        50,
			League:        "Diamond",
			FollowingList: []string{"2", "4", "6", "8"},
		},
		{
			ID:            "8",
			Username:      "nightowl",
			password:      "pass8",
			FullName:      "Night Owl",
			Wins:          15,
			Losses:        10,
			League:        "Bronze",
			FollowingList: []string{"1", "7"},
		},
		{
			ID:            "9",
			Username:      "thunderbolt",
			password:      "pass9",
			FullName:      "Thunder Bolt",
			Wins:          62,
			Losses:        38,
			League:        "Silver",
			FollowingList: []string{"4", "7", "10"},
		},
		{
			ID:            "10",
			Username:      "cyberking",
			password:      "pass10",
			FullName:      "Cyber King",
			Wins:          100,
			Losses:        25,
			League:        "Platinum",
			FollowingList: []string{"1", "2", "4", "7"},
		},
	}

	// DYNAMIC CALCULATION: Create a map to hold the calculated follower counts.
	followerCounts := make(map[string]int)

	// DYNAMIC CALCULATION: Iterate through all users and their following lists.
	for _, u := range initialUsers {
		for _, followedID := range u.FollowingList {
			// For each user someone is following, increment that user's follower count.
			followerCounts[followedID]++
		}
	}

	// DYNAMIC CALCULATION: Assign the calculated follower counts back to the users.
	for i := range initialUsers {
		initialUsers[i].Followers = followerCounts[initialUsers[i].ID]
	}

	// The `users` slice now contains the fully consistent, dynamically. calculated data.
	users = initialUsers

	// Populate the feed with sample posts.
	initPosts()
}

// GetUserByUsername searches for a user by their username and returns the user object.
func GetUserByUsername(username string) (User, bool) {
	usersDBMu.Lock()
	defer usersDBMu.Unlock()

	for _, user := range users {
		if user.Username == username {
			return user, true
		}
	}

	return User{}, false
}

// GetUserByID searches for a user by their ID and returns the user object.
func GetUserByID(id string) (User, bool) {
	usersDBMu.Lock()
	defer usersDBMu.Unlock()

	for _, user := range users {
		if user.ID == id {
			return user, true
		}
	}

	return User{}, false
}

// UserExists checks if a username exists in the database
func UserExists(username string) bool {
	usersDBMu.Lock()
	defer usersDBMu.Unlock()

	for _, user := range users {
		if user.Username == username {
			return true
		}
	}

	return false
}

// IsValidUser checks if a username and password combination is valid.
func IsValidUser(username, password string) bool {
	usersDBMu.Lock()
	defer usersDBMu.Unlock()

	for _, user := range users {
		if user.Username == username && user.password == password {
			return true
		}
	}

	return false
}

// GetAllUsers returns a copy of all users in the database.
func GetAllUsers() []User {
	usersDBMu.Lock()
	defer usersDBMu.Unlock()

	allUsers := make([]User, len(users))
	copy(allUsers, users)
	return allUsers
}

// initPosts seeds the in-memory feed with sample posts from various users.
// Player 1, 2, 3 posts have real media URLs for testing.
func initPosts() {
	posts = []Post{
		{ID: "p01", AuthorID: "2", AuthorUsername: "player2", AuthorLeague: "Diamond", Type: "video", ContentURL: "https://cdn.pixabay.com/video/2026/02/09/333600_small.mp4", ThumbnailURL: "https://cdn.pixabay.com/video/2026/02/09/333600_small.jpg", Caption: "Diamond league gameplay - watch and learn", Likes: 890, Views: 5400, Comments: 3, CreatedAt: "2026-02-20T14:00:00Z"},
		{ID: "p02", AuthorID: "7", AuthorUsername: "frostbyte", AuthorLeague: "Diamond", Type: "video", Caption: "Frozen intime New combo showcase!", Likes: 1200, Views: 8800, Comments: 210, CreatedAt: "2026-02-20T13:30:00Z"},
		{ID: "p03", AuthorID: "1", AuthorUsername: "player1", AuthorLeague: "Gold", Type: "video", ContentURL: "https://cdn.pixabay.com/video/2026/02/15/334716_small.mp4", ThumbnailURL: "https://cdn.pixabay.com/video/2026/02/15/334716_small.jpg", Caption: "Finally broke my personal record", Likes: 245, Views: 1200, Comments: 2, CreatedAt: "2026-02-20T12:45:00Z"},
		{ID: "p04", AuthorID: "10", AuthorUsername: "cyberking", AuthorLeague: "Platinum", Type: "image", Caption: "New setup reveal Ready to dominate!", Likes: 560, Views: 3200, Comments: 89, CreatedAt: "2026-02-20T11:15:00Z"},
		{ID: "p05", AuthorID: "4", AuthorUsername: "shadowstrike", AuthorLeague: "Platinum", Type: "video", Caption: "Shadow techniques vol.3 - the comeback is real ", Likes: 780, Views: 4100, Comments: 124, CreatedAt: "2026-02-20T10:30:00Z"},
		{ID: "p06", AuthorID: "5", AuthorUsername: "blazerunner", AuthorLeague: "Silver", Type: "video", Caption: "Speed run challenge accepted!", Likes: 340, Views: 2100, Comments: 54, CreatedAt: "2026-02-20T09:00:00Z"},
		{ID: "p07", AuthorID: "6", AuthorUsername: "stormchaser", AuthorLeague: "Gold", Type: "video", Caption: "Storm surge combo into triple elimination", Likes: 670, Views: 3800, Comments: 98, CreatedAt: "2026-02-19T22:00:00Z"},
		{ID: "p08", AuthorID: "3", AuthorUsername: "player3", AuthorLeague: "Bronze", Type: "image", ContentURL: "https://pixabay.com/get/g1cb3fb5e78308321688cd47550266beec73006700f8fd3f16549b183bd601669b6128582de0d1c92a4800ae0109174150e040eaeba050fdae70980c53ef8761c_1280.jpg", Caption: "Just started competing - wish me luck! ", Likes: 120, Views: 800, Comments: 2, CreatedAt: "2026-02-19T20:30:00Z"},
		{ID: "p09", AuthorID: "9", AuthorUsername: "thunderbolt", AuthorLeague: "Silver", Type: "video", Caption: "Thunder strike compilation ", Likes: 450, Views: 2600, Comments: 67, CreatedAt: "2026-02-19T18:45:00Z"},
		{ID: "p10", AuthorID: "8", AuthorUsername: "nightowl", AuthorLeague: "Bronze", Type: "video", Caption: "Late-night practice session ", Likes: 95, Views: 650, Comments: 18, CreatedAt: "2026-02-19T17:00:00Z"},
		{ID: "p11", AuthorID: "2", AuthorUsername: "player2", AuthorLeague: "Diamond", Type: "video", ContentURL: "https://cdn.pixabay.com/video/2026/01/09/326739_small.mp4", ThumbnailURL: "https://cdn.pixabay.com/video/2026/01/09/326739_small.jpg", Caption: "1v1 challenge against shadowstrike - who wins?", Likes: 1520, Views: 9200, Comments: 2, CreatedAt: "2026-02-19T15:30:00Z"},
		{ID: "p12", AuthorID: "4", AuthorUsername: "shadowstrike", AuthorLeague: "Platinum", Type: "image", Caption: "Platinum badge unlocked!", Likes: 920, Views: 5100, Comments: 145, CreatedAt: "2026-02-19T14:00:00Z"},
		{ID: "p13", AuthorID: "7", AuthorUsername: "frostbyte", AuthorLeague: "Diamond", Type: "video", Caption: "Tutorial: Advanced freeze frame technique", Likes: 680, Views: 4200, Comments: 112, CreatedAt: "2026-02-19T12:15:00Z"},
		{ID: "p14", AuthorID: "1", AuthorUsername: "player1", AuthorLeague: "Gold", Type: "video", ContentURL: "https://cdn.pixabay.com/video/2026/01/19/328740_small.mp4", ThumbnailURL: "https://cdn.pixabay.com/video/2026/01/19/328740_small.jpg", Caption: "Gold league highlights - best plays this week ", Likes: 310, Views: 1800, Comments: 1, CreatedAt: "2026-02-19T10:30:00Z"},
		{ID: "p15", AuthorID: "6", AuthorUsername: "stormchaser", AuthorLeague: "Platinum", Type: "video", Caption: "AI - assisted training results are insane", Likes: 1100, Views: 7500, Comments: 198, CreatedAt: "2026-02-19T08:00:00Z"},
		{ID: "p16", AuthorID: "6", AuthorUsername: "stormchaser", AuthorLeague: "Gold", Type: "video", Caption: "Road to Platinum - day 45 of the grind ", Likes: 420, Views: 2400, Comments: 55, CreatedAt: "2026-02-18T23:00:00Z"},
		{ID: "p17", AuthorID: "5", AuthorUsername: "blazerunner", AuthorLeague: "Silver", Type: "image", Caption: "New controller just dropped ", Likes: 280, Views: 1600, Comments: 38, CreatedAt: "2026-02-18T21:00:00Z"},
		{ID: "p18", AuthorID: "9", AuthorUsername: "thunderbolt", AuthorLeague: "Silver", Type: "video", Caption: "My best clutch moment yet - 1 HP survival!!", Likes: 750, Views: 4800, Comments: 130, CreatedAt: "2026-02-18T19:00:00Z"},
		{ID: "P19", AuthorID: "3", AuthorUsername: "player3", AuthorLeague: "Bronze", Type: "video", ContentURL: "https://cdn.pixabay.com/video/2026/01/19/328740_small.mp4", ThumbnailURL: "https://cdn.pixabay.com/video/2026/01/19/328740_small.jpg", Caption: "Learning from the pros Watch me improve!", Likes: 85, Views: 500, Comments: 0, CreatedAt: "2026-02-18T17:30:00Z"},
		{ID: "p20", AuthorID: "8", AuthorUsername: "nightowl", AuthorLeague: "Bronze", Type: "video", Caption: "First win of the season ", Likes: 140, Views: 900, Comments: 25, CreatedAt: "2026-02-18T16:00:00Z"},
		{ID: "p21", AuthorID: "2", AuthorUsername: "player2", AuthorLeague: "Diamond", Type: "video", ContentURL: "https://cdn.pixabay.com/video/2026/02/15/334716_small.mp4", ThumbnailURL: "https://cdn.pixabay.com/video/2026/02/15/334716_small.jpg", Caption: "How I got to Diamond in 30 days - full guide", Likes: 2100, Views: 12000, Comments: 1, CreatedAt: "2026-02-18T14:00:00Z"},
		{ID: "P22", AuthorID: "7", AuthorUsername: "frostbyte", AuthorLeague: "Diamond", Type: "image", Caption: "New character skin unlocked - thoughts?", Likes: 530, Views: 3100, Comments: 76, CreatedAt: "2026-02-18T12:00:00Z"},
		{ID: "p23", AuthorID: "4", AuthorUsername: "shadowstrike", AuthorLeague: "Platinum", Type: "video", Caption: "Top 5 mistakes beginners make (avoid these!)", Likes: 890, Views: 5600, Comments: 167, CreatedAt: "2026-02-18T10:00:00Z"},
		{ID: "p24", AuthorID: "10", AuthorUsername: "cyberking", AuthorLeague: "Platinum", Type: "video", Caption: "Challenge accepted: 24-hour win streak attempt ", Likes: 1340, Views: 8900, Comments: 245, CreatedAt: "2026-02-18T08:00:00Z"},
		{ID: "p25", AuthorID: "6", AuthorUsername: "stormchaser", AuthorLeague: "Gold", Type: "video", Caption: "Storm vs Blaze - epic rivalry match recap ", Likes: 620, Views: 3600, Comments: 88, CreatedAt: "2026-02-17T22:00:00Z"},
		{ID: "p26", AuthorID: "1", AuthorUsername: "player1", AuthorLeague: "Gold", Type: "image", ContentURL: "https://cdn.pixabay.com/video/2026/01/09/326739_small.jpg https://pixabay.com/get/g6d2fb7b4dde02b4febf151624eaf6ee7f096bc81be660c3da10483538d9a3386d3a89ce0a6cfb52722f4c7e824fd261614b56eddbf31da7d1d4f96fcab32dc5e_640.jpg", Caption: "My journey from Bronze to Gold in one month", Likes: 410, Views: 2200, Comments: 1, CreatedAt: "2026-02-17T18:00:00Z"},
		{ID: "p27", AuthorID: "5", AuthorUsername: "blazerunner", AuthorLeague: "Silver", Type: "video", Caption: "Speed kills - fastest elimination compilation", Likes: 380, Views: 2000, Comments: 48, CreatedAt: "2026-02-17T15:00:00Z"},
		{ID: "p28", AuthorID: "9", AuthorUsername: "thunderbolt", AuthorLeague: "Silver", Type: "video", Caption: "Team challenge highlights with frostbyte!", Likes: 290, Views: 1700, Comments: 35, CreatedAt: "2026-02-17T12:00:00Z"},
	}

	// Seed sample comments so the UI has data to show.
	initComments()
}

// GetAllPosts returns a copy of all posts.
func GetAllPosts() []Post {
	return posts
}

// GetPostsByUserID returns all posts authored by a given user ID.
func GetPostsByUserID(userID string) []Post {
	var result []Post
	for _, p := range posts {
		if p.AuthorID == userID {
			result = append(result, p)
		}
	}
	return result
}

// GetPostByID returns a post by its ID.
func GetPostByID(id string) (Post, bool) {
	for _, p := range posts {
		if p.ID == id {
			return p, true
		}
	}
	return Post{}, false
}

// ToggleLike toggles a like on a post for the given user. Returns (liked bool, newCount int, post).
func ToggleLike(postID, userID string) (bool, int, Post) {
	for i := range posts {
		if posts[i].ID == postID {
			// Check if already liked
			for j, uid := range posts[i].LikedBy {
				if uid == userID {
					// Unlike
					posts[i].LikedBy = append(posts[i].LikedBy[:j], posts[i].LikedBy[j+1:]...)
					posts[i].Likes--
					return false, posts[i].Likes, posts[i]
				}
			}
			// Like
			posts[i].LikedBy = append(posts[i].LikedBy, userID)
			posts[i].Likes++
			return true, posts[i].Likes, posts[i]
		}
	}
	return false, 0, Post{}
}

// AddComment adds a comment to a post and increments the post's comment count.
func AddComment(postID, authorID, authorUsername, text string) Comment {
	commentsMu.Lock()
	defer commentsMu.Unlock()

	commentIDCounter++
	c := Comment{
		ID:             fmt.Sprintf("c%d", commentIDCounter),
		PostID:         postID,
		AuthorID:       authorID,
		AuthorUsername: authorUsername,
		Text:           text,
		CreatedAt:      time.Now().UTC().Format(time.RFC3339),
	}
	comments[postID] = append(comments[postID], c)

	// Increment comment count on the post
	for i := range posts {
		if posts[i].ID == postID {
			posts[i].Comments++
			break
		}
	}

	return c
}

// GetComments returns all comments for a given post ID.
func GetComments(postID string) []Comment {
	commentsMu.Lock()
	defer commentsMu.Unlock()
	return comments[postID]
}

// initComments seeds some sample comments so the feed has data.
func initComments() {
	commentIDCounter = 20 // start after seed IDs
	comments = map[string][]Comment{
		"p01": {
			{ID: "c1", PostID: "p01", AuthorID: "1", AuthorUsername: "player1", Text: "Insane gameplay! Teach me your ways", CreatedAt: "2026-02-20T14:30:00Z"},
			{ID: "c2", PostID: "p01", AuthorID: "4", AuthorUsername: "shadowstrike", Text: "Diamond players are built different", CreatedAt: "2026-02-20T14:45:00Z"},
			{ID: "c3", PostID: "p01", AuthorID: "7", AuthorUsername: "frostbyte", Text: "GG, let's run duos sometime", CreatedAt: "2026-02-20T15:30:00Z"},
		},
		"p02": {
			{ID: "c4", PostID: "p02", AuthorID: "2", AuthorUsername: "player2", Text: "That combo is nutty", CreatedAt: "2026-02-20T13:45:00Z"},
			{ID: "c5", PostID: "p02", AuthorID: "9", AuthorUsername: "thunderbolt", Text: "How do you do the freeze cancel?", CreatedAt: "2026-02-20T14:00:00Z"},
		},
		"p03": {
			{ID: "c6", PostID: "p03", AuthorID: "2", AuthorUsername: "player2", Text: "Nice one! Keep grinding", CreatedAt: "2026-02-20T13:00:00Z"},
			{ID: "c7", PostID: "p03", AuthorID: "6", AuthorUsername: "stormchaser", Text: "Gold league represent!", CreatedAt: "2026-02-20T13:15:00Z"},
		},
		"p04": {
			{ID: "c8", PostID: "p04", AuthorID: "7", AuthorUsername: "frostbyte", Text: "Clean setup! What monitor is that?", CreatedAt: "2026-02-20T11:45:00Z"},
		},
		"p05": {
			{ID: "c9", PostID: "p05", AuthorID: "10", AuthorUsername: "cyberking", Text: "Shadow gameplay is always entertaining", CreatedAt: "2026-02-20T11:00:00Z"},
		},
		"p08": {
			{ID: "c10", PostID: "p08", AuthorID: "1", AuthorUsername: "player1", Text: "Welcome to the arena! Good luck", CreatedAt: "2026-02-19T21:00:00Z"},
			{ID: "c11", PostID: "p08", AuthorID: "5", AuthorUsername: "blazerunner", Text: "We all started somewhere, you got this!", CreatedAt: "2026-02-19T21:30:00Z"},
		},
		"p11": {
			{ID: "c12", PostID: "p11", AuthorID: "4", AuthorUsername: "shadowstrike", Text: "Rematch? I want that w back", CreatedAt: "2026-02-19T16:00:00Z"},
			{ID: "c13", PostID: "p11", AuthorID: "1", AuthorUsername: "player1", Text: "This match was legendary!", CreatedAt: "2026-02-19T16:30:00Z"},
		},
		"p12": {
			{ID: "c14", PostID: "p12", AuthorID: "2", AuthorUsername: "player2", Text: "Welcome to Platinum! See you in Diamond soon", CreatedAt: "2026-02-19T14:30:00Z"},
		},
		"p14": {
			{ID: "c15", PostID: "p14", AuthorID: "3", AuthorUsername: "player3", Text: "Your highlights are so motivating!", CreatedAt: "2026-02-19T11:00:00Z"},
		},
		"p21": {
			{ID: "c16", PostID: "p21", AuthorID: "8", AuthorUsername: "nightowl", Text: "This guide helped me so much, thanks!", CreatedAt: "2026-02-18T15:00:00Z"},
		},
		"p26": {
			{ID: "c17", PostID: "p26", AuthorID: "3", AuthorUsername: "player3", Text: "Inspiring journey! I'm working on the same goal ", CreatedAt: "2026-02-17T19:00:00Z"},
		},
	}
}
