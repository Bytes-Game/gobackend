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

	// The User struct's 'password' field is now unexported.
	// We initialize it directly here.
	users = []User{
		{Username: "player1", password: "pass1", Name: "Player One", Followers: 150, Location: "USA"},
		{Username: "player2", password: "pass2", Name: "Player Two", Followers: 2500, Location: "Canada"},
		{Username: "player3", password: "pass3", Name: "Player Three", Followers: 1, Location: "USA"},
	}
}

// UserExists checks if a username exists in the database.
// This is used for the temporary, insecure login flow.
func UserExists(username string) bool {
	usersDBMu.Lock()
	defer usersDBMu.Unlock()

	for _, user := range users {
		if user.Username == username {
			return true // Found the user
		}
	}

	return false // No match found
}


// IsValidUser checks if a username and password combination is valid.
func IsValidUser(username, password string) bool {
	usersDBMu.Lock()
	defer usersDBMu.Unlock()

	for _, user := range users {
		// Access the unexported 'password' field for comparison.
		if user.Username == username && user.password == password {
			return true // Found a matching user
		}
	}

	return false // No match found
}
