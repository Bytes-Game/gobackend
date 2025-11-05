package main

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/gorilla/mux"
)

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
	r := mux.NewRouter()

	r.HandleFunc("/users", GetUsersHandler).Methods("GET")
	r.HandleFunc("/users/{username}", GetUserHandler).Methods("GET")
	r.HandleFunc("/users", CreateUserHandler).Methods("POST")
	r.HandleFunc("/search", SearchHandler).Methods("GET")

	log.Println("Starting server on :8081")
	log.Fatal(http.ListenAndServe(":8081", r))
}
