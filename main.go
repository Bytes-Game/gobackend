package main

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/gorilla/mux"
)

// LoginHandler validates user credentials. 
// FOR DEVELOPMENT ONLY: This handler is temporarily insecure and allows login with just a username.
func LoginHandler(w http.ResponseWriter, r *http.Request) {
	var creds struct {
		Username string `json:"username"`
		Password string `json:"password"` // Password may be empty during front-end dev
	}

	if err := json.NewDecoder(r.Body).Decode(&creds); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// --- TEMPORARY, INSECURE LOGIN FOR DEVELOPMENT ---
	// This block allows login with just a username, bypassing the password check.
	// It should be removed and replaced with the commented-out secure code below
	// before moving to production.
	if UserExists(creds.Username) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"status": "success"})
		log.Printf("INSECURE LOGIN: User %s logged in without a password.", creds.Username)
		return
	}
	// --- END OF TEMPORARY CODE ---

	/*
	// --- SECURE LOGIN CODE ---
	// This is the proper, secure way to handle logins.
	// Re-enable this block when the front end is ready to send passwords.
	if IsValidUser(creds.Username, creds.Password) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"status": "success"})
		log.Printf("User %s logged in successfully.", creds.Username)
	} else {
		http.Error(w, "Invalid username or password", http.StatusUnauthorized)
		log.Printf("Failed login attempt for user %s.", creds.Username)
	}
	// --- END OF SECURE LOGIN CODE ---
	*/

	// If the insecure login also fails, deny access.
	http.Error(w, "Invalid username", http.StatusUnauthorized)
	log.Printf("Failed login attempt for user %s.", creds.Username)
}


// main is the entry point for the application.
func main() {
	// Initialize the in-memory user database from database.go
	InitDatabase()

	// Create a new router
	r := mux.NewRouter()

	// --- API ROUTES ---

	// Authentication endpoint
	r.HandleFunc("/login", LoginHandler).Methods("POST")

	// Search endpoint (handler is in search.go)
	r.HandleFunc("/search", SearchHandler).Methods("GET")

	// --- SERVER STARTUP ---

	log.Println("Starting server on :8081...")
	if err := http.ListenAndServe(":8081", r); err != nil {
		log.Fatalf("Could not start server: %s\n", err)
	}
}
