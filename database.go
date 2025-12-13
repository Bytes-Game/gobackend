package main

import (
	"sync"
	"time"
)

// users is the in-memory database for our application.
var users []User

// usersDBMu is a mutex to make access to the users slice safe in a concurrent environment.
var usersDBMu sync.Mutex

// InitDatabase populates the in-memory user database with sample data.
func InitDatabase() {
	usersDBMu.Lock()
	defer usersDBMu.Unlock()

	// Sample posts for our users
	postsForPlayer1 := []Post{
		{ID: "post_1", URL: "https://example.com/post1.png", Caption: "My first post!", Timestamp: time.Now().Add(-24 * time.Hour)},
		{ID: "post_2", URL: "https://example.com/post2.png", Caption: "Another great day!", Timestamp: time.Now().Add(-48 * time.Hour)},
	}

	postsForPlayer2 := []Post{
		{ID: "post_3", URL: "https://example.com/post3.png", Caption: "Winning streak!", Timestamp: time.Now().Add(-72 * time.Hour)},
	}

	users = []User{
		{
			Username:      "player1",
			password:      "pass1",
			FullName:      "Player One",
			Caption:       "Just for fun!",
			Followers:     150,
			Following:     50,
			Posts:         postsForPlayer1, // Correctly using a slice of Post objects
			Wins:          32,
			Losses:        18,
			League:        "Gold",
			FollowingList: []string{"player2", "player3"},
		},
		{
			Username:      "player2",
			password:      "pass2",
			FullName:      "Player Two",
			Caption:       "Competitive player.",
			Followers:     2500,
			Following:     100,
			Posts:         postsForPlayer2, // Correctly using a slice of Post objects
			Wins:          120,
			Losses:        45,
			League:        "Diamond",
			FollowingList: []string{"player1"},
		},
		{
			Username:      "player3",
			password:      "pass3",
			FullName:      "Player Three",
			Caption:       "Streaming on weekends.",
			Followers:     1,
			Following:     1000,
			Posts:         []Post{}, // Player 3 has no posts
			Wins:          10,
			Losses:        5,
			League:        "Bronze",
			FollowingList: []string{"player1", "player2"},
		},
	}
}

// GetAllUsers returns a slice of all users in the database.
func GetAllUsers() []User {
	usersDBMu.Lock()
	defer usersDBMu.Unlock()
	usersCopy := make([]User, len(users))
	copy(usersCopy, users)
	return usersCopy
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

// UserExists checks if a username exists in the database.
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
