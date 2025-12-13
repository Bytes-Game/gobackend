package main

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/gorilla/mux"
)

// LoginHandler validates credentials and returns the user's data and all other users.
func LoginHandler(w http.ResponseWriter, r *http.Request) {
	var creds struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}

	if err := json.NewDecoder(r.Body).Decode(&creds); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// First, validate the user's credentials.
	if !IsValidUser(creds.Username, creds.Password) {
		http.Error(w, "Invalid username or password", http.StatusUnauthorized)
		log.Printf("Failed login attempt for user %s.", creds.Username)
		return
	}

	// If valid, fetch the logged-in user's full profile.
	user, exists := GetUserByUsername(creds.Username)
	if !exists {
		// This should not happen if IsValidUser passed, but it's good practice to check.
		http.Error(w, "Could not find user data after successful login", http.StatusInternalServerError)
		return
	}

	// Fetch all users for the search/discovery feature.
	allUsers := GetAllUsers()

	// Create the response payload that the Flutter client expects.
	response := map[string]interface{}{
		"user":     user,
		"allUsers": allUsers,
	}

	// Send the successful response.
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
	log.Printf("User %s logged in successfully. Sent user profile and all users list.", creds.Username)
}

// GetAllUsersHandler returns a list of all users.
func GetAllUsersHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	allUsers := GetAllUsers()
	if err := json.NewEncoder(w).Encode(allUsers); err != nil {
		http.Error(w, "Failed to encode users", http.StatusInternalServerError)
	}
}

// GetUserHandler returns the data for a single user.
func GetUserHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	username := vars["username"]

	user, exists := GetUserByUsername(username)
	if !exists {
		http.Error(w, "User not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(user); err != nil {
		http.Error(w, "Failed to encode user data", http.StatusInternalServerError)
	}
}

// main is the entry point for the application.
func main() {
	InitDatabase()

	r := mux.NewRouter()

	api := r.PathPrefix("/api/v1").Subrouter()
	api.HandleFunc("/users", GetAllUsersHandler).Methods("GET")
	api.HandleFunc("/users/{username}", GetUserHandler).Methods("GET")

	r.HandleFunc("/login", LoginHandler).Methods("POST")
	r.HandleFunc("/ws/{username}", WebsocketHandler).Methods("GET")

	log.Println("Starting server on :8081...")
	if err := http.ListenAndServe(":8081", r); err != nil {
		log.Fatalf("Could not start server: %s\n", err)
	}
}
