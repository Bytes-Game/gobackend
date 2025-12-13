package main

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/gorilla/mux"
)

// LoginHandler validates user credentials against the in-memory database.
// It uses the User struct from models.go and IsValidUser from database.go.
func LoginHandler(w http.ResponseWriter, r *http.Request) {
	var creds User
	if err := json.NewDecoder(r.Body).Decode(&creds); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if IsValidUser(creds.Username, creds.Password) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"status": "success"})
		log.Printf("User %s logged in successfully.", creds.Username)
	} else {
		http.Error(w, "Invalid username or password", http.StatusUnauthorized)
		log.Printf("Failed login attempt for user %s.", creds.Username)
	}
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
