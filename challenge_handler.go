package main

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
)

// ------------------------------------------------------------------------------------
// Challenge HTTP handlers
// ------------------------------------------------------------------------------------

// CreateChallengeHandler creates a new challenge.
// POST /api/v1/challenges
func CreateChallengeHandler(w http.ResponseWriter, r *http.Request) {
	var payload CreateChallengePayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "Invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}

	if payload.Prefix == "" || payload.Subject == "" {
		http.Error(w, "prefix and subject are required", http.StatusBadRequest)
		return
	}

	challenge, err := CreateChallenge(payload)
	if err != nil {
		http.Error(w, "Failed to create challenge: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Notify friends if visibility is friends.
	if payload.Visibility == "friends" {
		creator, _ := GetUserByID(payload.CreatorID)
		go SendChallengeNotification(creator.Username, payload.Prefix+" "+payload.Subject, payload.VisibleTo)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(challenge)
}

// GetArenaChallengesHandler returns all arena challenges.
// GET /api/v1/challenges/arena
func GetArenaChallengesHandler(w http.ResponseWriter, r *http.Request) {
	challenges := GetArenaChallenges()
	if challenges == nil {
		challenges = []Challenge{}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(challenges)
}

// GetFriendsChallengesHandler returns friends-only challenges visible to requesting user.
// GET /api/v1/challenges/friends?userId=x
func GetFriendsChallengesHandler(w http.ResponseWriter, r *http.Request) {
	userID := r.URL.Query().Get("userId")
	if userID == "" {
		http.Error(w, "userId query parameter required", http.StatusBadRequest)
		return
	}

	challenges := GetFriendsChallenges(userID)
	if challenges == nil {
		challenges = []Challenge{}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(challenges)
}

// GetChallengeDetailHandler returns a challenge with all its responses.
// GET /api/v1/challenges/{id)
func GetChallengeDetailHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]

	challenge, found := GetChallengeByID(id)
	if !found {
		http.Error(w, "Challenge not found", http.StatusNotFound)
		return
	}

	// Increment views.
	go IncrementChallengeViews(id)

	responses := GetChallengeResponses(id)
	if responses == nil {
		responses = []ChallengeResponse{}
	}

	resp := ChallengeDetailResponse{
		Challenge: challenge,
		Responses: responses,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// AcceptChallengeHandler lets a user respond to a challenge.
// POST /api/v1/challenges/accept
func AcceptChallengeHandler(w http.ResponseWriter, r *http.Request) {
	var payload AcceptChallengePayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "Invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}

	response, err := AcceptChallenge(payload)
	if err != nil {
		http.Error(w, "Failed to accept challenge: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Notify the challenger that someone accepted.
	challenge, found := GetChallengeByID(payload.ChallengeID)
	if found {
		responder, _ := GetUserByID(payload.ResponderID)
		go SendChallengeAcceptedNotification(responder.Username, challenge.CreatorUsername, challenge.Prefix+" "+challenge.Subject)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(response)
}

// LikeChallengeHandler toggles a like on a challenge.
// POST /api/v1/challenges/like body:{ challengeId, userId }
func LikeChallengeHandler(w http.ResponseWriter, r *http.Request) {
	var payload struct {
		ChallengeID string `json:"challengeId"`
		UserID      string `json:"userId"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	liked, count := ToggleChallengeLike(payload.ChallengeID, payload.UserID)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"liked":       liked,
		"likes":       count,
		"challengeId": payload.ChallengeID,
	})
}
