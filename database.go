package main

import "sync"

// users is the in-memory database for our application.
// The User struct is defined in models.go
var users []User

// usersDBMu is a mutex to make access to the users slice safe in a concurrent environment.
var usersDBMu sync.Mutex

// InitDatabase populates the in-memory user database with sample data.
func InitDatabase() {
	usersDBMu.Lock()
	defer usersDBMu.Unlock()

	// Create some dummy users for login and search testing.
	// The User struct now includes more fields from models.go.
	users = []User{
		{Username: "player1", Password: "pass1", Name: "Player One", Followers: 150, Location: "USA"},
		{Username: "player2", Password: "pass2", Name: "Player Two", Followers: 2500, Location: "Canada"},
		{Username: "player3", Password: "pass3", Name: "Player Three", Followers: 1, Location: "USA"},
	}
}

// IsValidUser checks if a username and password combination is valid.
func IsValidUser(username, password string) bool {
	usersDBMu.Lock()
	defer usersDBMu.Unlock()

	for _, user := range users {
		if user.Username == username && user.Password == password {
			return true // Found a matching user
		}
	}

	return false // No match found
}
