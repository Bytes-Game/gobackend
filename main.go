package main

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/gorilla/mux"
)

// LoginHandler validates user credentials against the in-memory database.
func LoginHandler(w http.ResponseWriter, r *http.Request) {
	var creds struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}

	if err := json.NewDecoder(r.Body).Decode(&creds); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// Validate credentials using the function from database.go
	if IsValidUser(creds.Username, creds.Password) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"status": "success"})
		log.Printf("User %s logged in successfully.", creds.Username)
	} else {
		// If credentials are not valid, return a 401 Unauthorized error.
		http.Error(w, "Invalid username or password", http.StatusUnauthorized)
		log.Printf("Failed login attempt for user %s.", creds.Username)
	}
}

// GetAllUsersHandler returns a list of all users.
func GetAllUsersHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	// The GetAllUsers function is assumed to be in database.go
	allUsers := GetAllUsers()
	if err := json.NewEncoder(w).Encode(allUsers); err != nil {
		http.Error(w, "Failed to encode users", http.StatusInternalServerError)
	}
}

// GetUserHandler returns the data for a single user.
func GetUserHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	username := vars["username"]

	user, exists := GetUserByUsername(username) // Assumed to be in database.go
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
	// Initialize the in-memory user database from database.go
	InitDatabase()

	// Create a new router
	r := mux.NewRouter()

	// --- API ROUTES ---
	api := r.PathPrefix("/api/v1").Subrouter()

	// User-related endpoints
	api.HandleFunc("/users", GetAllUsersHandler).Methods("GET")
	api.HandleFunc("/users/{username}", GetUserHandler).Methods("GET")

	// Authentication endpoint (outside the /api/v1 prefix)
	r.HandleFunc("/login", LoginHandler).Methods("POST")

	// WebSocket endpoint
	r.HandleFunc("/ws/{username}", WebsocketHandler).Methods("GET")

	// --- SERVER STARTUP ---

	log.Println("Starting server on :8081...")
	if err := http.ListenAndServe(":8081", r); err != nil {
		log.Fatalf("Could not start server: %s\n", err)
	}
}
