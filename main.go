package main

import (
	"encoding/json"
	"io"
	"log"
	"net/http"

	"github.com/gorilla/mux"
)

// LoginHandler validates user credentials.
func LoginHandler(w http.ResponseWriter, r *http.Request) {
	var creds User
	if err := json.NewDecoder(r.Body).Decode(&creds); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// Use the function from database.go to check credentials.
	if IsValidUser(creds.Username, creds.Password) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"status": "success", "username": creds.Username})
		log.Printf("User %s logged in successfully.", creds.Username)
	} else {
		// If credentials are bad, return an unauthorized error.
		http.Error(w, "Invalid username or password", http.StatusUnauthorized)
		log.Printf("Failed login attempt for user %s.", creds.Username)
	}
}

// FollowHandler processes follow requests
func FollowHandler(w http.ResponseWriter, r *http.Request) {
    var req struct {
        Follower  string `json:"follower"`
        Following string `json:"following"`
    }

    if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
        http.Error(w, "Invalid request body", http.StatusBadRequest)
        return
    }

    log.Printf("Processing follow request: %s is now following %s", req.Follower, req.Following)

    // In a real application, you would update your database here.
    // For this example, we'll just log it and assume success.

    w.WriteHeader(http.StatusOK)
    json.NewEncoder(w).Encode(map[string]string{"status": "success"})
}

func NotificationHandler(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	username := params["username"]
	log.Printf("Received notification for user %s", username)

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Error reading request body", http.StatusInternalServerError)
		return
	}
	defer r.Body.Close()

	HandleNotification(username, body)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "notification processed"})
}

func GetUsersHandler(w http.ResponseWriter, r *http.Request) {
	log.Println("Received request for /users")
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(users)
}

func GetUserHandler(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	log.Printf("Received request for /users/%s", params["username"])
	w.Header().Set("Content-Type", "application/json")
	for _, item := range users {
		if item.Username == params["username"] {
			json.NewEncoder(w).Encode(item)
			return
		}
	}
	json.NewEncoder(w).Encode(&User{})
}

func CreateUserHandler(w http.ResponseWriter, r *http.Request) {
	log.Println("Received request to create a new user")
	w.Header().Set("Content-Type", "application/json")
	var user User
	_ = json.NewDecoder(r.Body).Decode(&user)
	users = append(users, user)
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(user)
}

func main() {
    // Initialize the in-memory stores
    InitRedis()      // Initializes the (simulated) Redis notification store.
    InitDatabase() // Initializes the in-memory user database.

	r := mux.NewRouter()

    // --- AUTHENTICATION ---
    r.HandleFunc("/login", LoginHandler).Methods("POST")

    // --- USER & SOCIAL ---
	r.HandleFunc("/users", GetUsersHandler).Methods("GET")
	r.HandleFunc("/users/{username}", GetUserHandler).Methods("GET")
	r.HandleFunc("/users", CreateUserHandler).Methods("POST")
	r.HandleFunc("/users/follow", FollowHandler).Methods("POST")

    // --- NOTIFICATIONS & REAL-TIME ---
	r.HandleFunc("/notifications/{username}", NotificationHandler).Methods("POST")
	r.HandleFunc("/ws", WebSocketHandler)

	log.Println("Starting server on :8081")
	log.Fatal(http.ListenAndServe(":8081", r))
}
