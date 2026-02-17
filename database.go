package main

import (
	"sync"
)

// users is the in-memory database for our application.
var users []User

// usersDBMu is a mutex to make access to the users slice safe in a concurrent environment.
var usersDBMu sync.Mutex

// InitDatabase populates the in-memory user database with sample data.
// It now dynamically calculates the initial follower counts to ensure data consistency.
func InitDatabase() {
	usersDBMu.Lock()
	defer usersDBMu.Unlock()

	// Initialize users without the `Followers` field, as it will be calculated dynamically.
	initialUsers := []User{
		{
			ID:            "1",
			Username:      "player1",
			password:      "pass1",
			FullName:      "Player One",
			Wins:          32,
			Losses:        18,
			League:        "Gold",
			FollowingList: []string{"2", "3"}, // Player 1 follows players 2 and 3
		},
		{
			ID:            "2",
			Username:      "player2",
			password:      "pass2",
			FullName:      "Player Two",
			Wins:          120,
			Losses:        45,
			League:        "Diamond",
			FollowingList: []string{"1"}, // Player 2 follows player 1
		},
		{
			ID:            "3",
			Username:      "player3",
			password:      "pass3",
			FullName:      "Player Three",
			Wins:          10,
			Losses:        5,
			League:        "Bronze",
			FollowingList: []string{"1", "2"}, // Player 3 follows players 1 and 2
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

	// The `users` slice now contains the fully consistent, dynamically calculated data.
	users = initialUsers
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
