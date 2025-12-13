package main

import "sync"

// We use a simple slice to store users for this example.
// The User struct itself is now defined in models.go
var users []User
var usersDBMu sync.Mutex

// InitDatabase populates our in-memory user database.
func InitDatabase() {
	usersDBMu.Lock()
	defer usersDBMu.Unlock()

	// Create some dummy users for login testing using the central User struct.
	users = []User{
		{Username: "player1", Password: "pass1", Name: "Player One"},
		{Username: "player2", Password: "pass2", Name: "Player Two"},
		{Username: "player3", Password: "pass3", Name: "Player Three"},
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
