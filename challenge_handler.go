package main

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"

	"github.com/gorilla/mux"
)

// leagueTier maps league names to numeric tiers for matchmaking comparison.
// Users can only challenge within ±2 tiers for fair play.
var leagueTier = map[string]int{
	"Bronze":   1,
	"Silver":   2,
	"Gold":     3,
	"Platinum": 4,
	"Diamond":  5,
}

const maxLeagueDiff = 2 // Maximum league tier difference allowed for challenges

// checkLeagueEligibility verifies two users are within the allowed league range.
// Returns nil if eligible, or an error describing why not.
func checkLeagueEligibility(userID1, userID2 string) error {
	user1, found1 := GetUserByID(userID1)
	user2, found2 := GetUserByID(userID2)
	if !found1 || !found2 {
		return fmt.Errorf("user not found")
	}

	tier1, ok1 := leagueTier[user1.League]
	tier2, ok2 := leagueTier[user2.League]
	if !ok1 {
		tier1 = 1 // Default to Bronze if unknown
	}
	if !ok2 {
		tier2 = 1
	}

	diff := math.Abs(float64(tier1 - tier2))
	if diff > maxLeagueDiff {
		return fmt.Errorf(
			"league mismatch: %s (%s) cannot challenge %s (%s) — max %d tier difference allowed",
			user1.Username, user1.League, user2.Username, user2.League, maxLeagueDiff,
		)
	}
	return nil
}

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
	// The creator is the authenticated user, never a client-supplied id.
	payload.CreatorID = authUserID(r)

	if payload.Prefix == "" || payload.Subject == "" {
		http.Error(w, "prefix and subject are required", http.StatusBadRequest)
		return
	}

	// 5 challenge creates per hour per user. Burst of 2 so back-to-back
	// posts during a creative streak don't get throttled.
	if !allowAction(payload.CreatorID, "challenge_create") {
		writeRateLimited(w, "challenge_create")
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

	// Index in Meilisearch
	go IndexChallenge(challenge)
	// Bump the autocomplete popularity counter for this subject so
	// the next typer who matches it gets it ranked higher. Fire-and-
	// forget: a hiccup in the suggest index never blocks the create
	// response.
	go recordSubjectUsage(challenge.Subject)

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
	// Identity comes from the session token, not the query string.
	userID := authUserID(r)
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
// GET /api/v1/challenges/{id}?userId=x
// If userId is provided, includes league eligibility check in response.
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

	votes := GetVoteSummary(id)
	if votes == nil {
		votes = []VoteSummary{}
	}

	// Check league eligibility for the AUTHENTICATED viewer. Advisory
	// only (drives the client's canAccept flag; AcceptChallengeHandler
	// re-checks authoritatively), but it used to read userId from the
	// query string — the one identity-shaped input in the codebase not
	// derived from the token. Token first; query-param fallback kept for
	// the public (unauthed) detail route where there is no token.
	canAccept := true
	leagueMsg := ""
	viewerID := authUserID(r)
	if viewerID == "" {
		viewerID = r.URL.Query().Get("userId")
	}
	if viewerID != "" {
		if err := checkLeagueEligibility(challenge.CreatorID, viewerID); err != nil {
			canAccept = false
			leagueMsg = err.Error()
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"challenge":      challenge,
		"responses":      responses,
		"votes":          votes,
		"canAccept":      canAccept,
		"leagueMessage":  leagueMsg,
	})
}

// AcceptChallengeHandler lets a user respond to a challenge.
// POST /api/v1/challenges/accept
// Enforces league-restricted matchmaking: responder must be within ±2 league tiers of creator.
func AcceptChallengeHandler(w http.ResponseWriter, r *http.Request) {
	var payload AcceptChallengePayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "Invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}
	// The responder is the authenticated user.
	payload.ResponderID = authUserID(r)

	// 30 accepts per hour per user. Higher than challenge_create
	// because responding is a lighter act, but still bounded so a
	// script can't churn out filler responses.
	if !allowAction(payload.ResponderID, "challenge_accept") {
		writeRateLimited(w, "challenge_accept")
		return
	}

	// League restriction: verify the responder is within ±2 tiers of the challenge creator
	challenge, found := GetChallengeByID(payload.ChallengeID)
	if !found {
		http.Error(w, "Challenge not found", http.StatusNotFound)
		return
	}
	if err := checkLeagueEligibility(challenge.CreatorID, payload.ResponderID); err != nil {
		http.Error(w, err.Error(), http.StatusForbidden)
		return
	}

	// Tier-1 structural validation: duration bounds, video dedupe, one-per-challenge,
	// challenge-still-open, per-user rate limit. Cheap checks that fire on every upload.
	if err := validateChallengeResponseSubmission(payload, challenge); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	response, err := AcceptChallenge(payload)
	if err != nil {
		http.Error(w, "Failed to accept challenge: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Notify the challenger that someone accepted.
	responder, _ := GetUserByID(payload.ResponderID)
	go SendChallengeAcceptedNotification(responder.Username, challenge.CreatorUsername, challenge.Prefix+" "+challenge.Subject)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(response)
}

// VoteChallengeHandler lets a user vote for a challenge response.
// POST /api/v1/challenges/vote body:{ challengeId, responseId, voterId }
func VoteChallengeHandler(w http.ResponseWriter, r *http.Request) {
	var payload ChallengeVotePayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "Invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}
	// The voter is the authenticated user.
	payload.VoterID = authUserID(r)

	if payload.ChallengeID == "" || payload.ResponseID == "" || payload.VoterID == "" {
		http.Error(w, "challengeId, responseId, and voterId are required", http.StatusBadRequest)
		return
	}

	// 30 votes/min per user — comfortably above realistic engagement
	// but blocks vote-stuffing scripts that try to swing leaderboards.
	if !allowAction(payload.VoterID, "vote") {
		writeRateLimited(w, "vote")
		return
	}

	voted, err := CastVote(payload)
	if err != nil {
		http.Error(w, "Failed to cast vote: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Get updated vote summary
	votes := GetVoteSummary(payload.ChallengeID)
	if votes == nil {
		votes = []VoteSummary{}
	}

	// Send vote notification to the response owner
	go SendVoteNotification(payload)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"voted": voted,
		"votes": votes,
	})
}

// GetVoteResultsHandler returns vote counts for a challenge.
// GET /api/v1/challenges/{id}/votes
func GetVoteResultsHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]

	votes := GetVoteSummary(id)
	if votes == nil {
		votes = []VoteSummary{}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(votes)
}

// AddChallengeCommentHandler adds a comment to a challenge.
// POST /api/v1/challenges/comments body:{ challengeId, userId, username, text }
func AddChallengeCommentHandler(w http.ResponseWriter, r *http.Request) {
	var payload ChallengeCommentPayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}
	// Author identity (id + display name) comes from the token, not the body,
	// so a caller can't post a comment under someone else's name.
	payload.UserID = authUserID(r)
	if u := authUsername(r); u != "" {
		payload.Username = u
	}

	if payload.Text == "" {
		http.Error(w, "text is required", http.StatusBadRequest)
		return
	}

	// ~20 comments/min per user. Burst of 8 covers a back-and-forth
	// discussion thread without breaking real conversation pacing.
	if !allowAction(payload.UserID, "comment") {
		writeRateLimited(w, "comment")
		return
	}

	comment, err := AddChallengeComment(payload.ChallengeID, payload.UserID, payload.Username, payload.Text)
	if err != nil {
		http.Error(w, "Failed to add comment: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(comment)
}

// GetChallengeCommentsHandler returns all comments for a challenge.
// GET /api/v1/challenges/{id}/comments
func GetChallengeCommentsHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]

	comments := GetChallengeComments(id)
	if comments == nil {
		comments = []ChallengeComment{}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(comments)
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
	// The liker is the authenticated user.
	payload.UserID = authUserID(r)

	// ~60 likes/min per user. High enough for an enthusiastic skim
	// through the feed; low enough to catch bot-scripted heart spam
	// that's the easiest way to game the like-leaderboard.
	if !allowAction(payload.UserID, "like") {
		writeRateLimited(w, "like")
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

// DeleteChallengeHandler removes a challenge the caller created.
// POST /api/v1/challenges/delete body:{ challengeId, userId }
//
// Authorization is creator-only: we fetch the challenge first and
// reject the request unless `userId` matches `challenge.CreatorID`.
// Admin / moderator delete uses a separate endpoint with its own auth
// path; this handler is purely the "user deletes their own post" flow.
//
// On success: the DB cascades through all child rows (responses,
// likes, votes, comments, saves, hls jobs). The R2 storage objects
// are NOT deleted here — see DeleteChallengeByID's docstring for the
// rationale (decoupled cleanup job).
func DeleteChallengeHandler(w http.ResponseWriter, r *http.Request) {
	var payload struct {
		ChallengeID string `json:"challengeId"`
		UserID      string `json:"userId"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}
	// Ownership is checked below against the trusted (token) user id.
	payload.UserID = authUserID(r)
	if payload.ChallengeID == "" || payload.UserID == "" {
		http.Error(w, "challengeId and userId are required", http.StatusBadRequest)
		return
	}

	challenge, found := GetChallengeByID(payload.ChallengeID)
	if !found {
		http.Error(w, "Challenge not found", http.StatusNotFound)
		return
	}
	if challenge.CreatorID != payload.UserID {
		// 403 (not 401) — the user IS authenticated, they just don't
		// own this resource.
		http.Error(w, "Only the creator can delete this challenge", http.StatusForbidden)
		return
	}

	if err := DeleteChallengeByID(payload.ChallengeID); err != nil {
		http.Error(w, "Failed to delete: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"deleted":     true,
		"challengeId": payload.ChallengeID,
	})
}
