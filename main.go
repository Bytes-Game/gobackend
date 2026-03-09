package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"

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

// GetuserHandler returns the data for a single user.
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

// corsMiddleware adds CORS headers to every response so the Flutter app
// (web or mobile) can reach the backend without cross-origin errors
func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type,_ Authorization")

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// main is the entry point for the application.
func main() {
	InitDatabase()
	InitRedis()

	r := mux.NewRouter()

	api := r.PathPrefix("/api/v1").Subrouter()
	api.HandleFunc("/users", GetAllUsersHandler).Methods("GET", "OPTIONS")
	api.HandleFunc("/users/{username}", GetUserHandler).Methods("GET", "OPTIONS")
	api.HandleFunc("/follow", HandleFollowEvent).Methods("POST", "OPTIONS")
	api.HandleFunc("/unfollow", HandleUnfollowEvent).Methods("POST", "OPTIONS")
	api.HandleFunc("/feed", FeedHandler).Methods("GET", "OPTIONS")
	api.HandleFunc("/posts/{userId}", UserPostsHandler).Methods("GET", "OPTIONS")
	api.HandleFunc("/like", HandleLikeEvent).Methods("POST", "OPTIONS")
	api.HandleFunc("/comments", HandleCommentEvent).Methods("POST", "OPTIONS")
	api.HandleFunc("/comments/{postId}", GetCommentsHandler).Methods("GET", "OPTIONS")

	r.HandleFunc("/login", LoginHandler).Methods("POST", "OPTIONS")
	r.HandleFunc("/ws/{username}", WebsocketHandler).Methods("GET")
	r.HandleFunc("/search", SearchHandler).Methods("GET", "OPTIONS")

	port := os.Getenv("PORT")
	if port == "" {
		port = "8081"
	}
	log.Printf("Starting server on :%s...\n", port)
	if err := http.ListenAndServe(":"+port, corsMiddleware(r)); err != nil {
		log.Fatalf("Could not start server: %s\n", err)
	}
}
