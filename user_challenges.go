package main

// user_challenges.go — everything a person has posted, on their own profile.
//
// ════════════════════════════════════════════════════════════════════════════
// WHY THIS EXISTS
// ════════════════════════════════════════════════════════════════════════════
//
// The profile screen had no way to ask for somebody's posts. It fetched the
// whole arena list and picked out the ones with a matching creator, and the
// app said so in a comment: a stopgap until this route existed.
//
// The stopgap did not just cost a wasted download. It quietly hid people's
// videos. The arena list keeps a challenge only while it is answered or under
// a day old — right for an arena, where the point is what is live now, and
// wrong for a profile, where the point is what this person has made. An
// unanswered post passed twenty-four hours, dropped off the arena list, and
// vanished from its own author's profile. Measured on production: the arena
// held ONE challenge out of a catalogue of ninety-two.
//
// So a profile asks its own question here, with no clock in it.
//
// ════════════════════════════════════════════════════════════════════════════
// WHAT ANOTHER PERSON SEES
// ════════════════════════════════════════════════════════════════════════════
//
// Your own profile shows everything you posted. Somebody else's shows only
// what they posted to the arena — a friends-only challenge stays with the
// friends it was meant for, and this route is not the place to widen that.
//
// The check is on the session token, never on the id in the path, so asking
// for another person's profile cannot be turned into asking as them.

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
)

// userChallengesDefaultLimit is what a request that names no size gets.
//
// A profile grid shows a page at a time and the client asks again as it
// scrolls, so this is a page, not a cap on a career.
const userChallengesDefaultLimit = 50

// userChallengesMaxLimit bounds one request. Without it a single call could
// ask the database for somebody's entire history and hand it to a phone.
const userChallengesMaxLimit = 100

// GetUserChallengesHandler returns the challenges a person has posted.
//
// GET /api/v1/users/{id}/challenges?limit=&offset=
func GetUserChallengesHandler(w http.ResponseWriter, r *http.Request) {
	ownerID := mux.Vars(r)["id"]
	if _, err := strconv.Atoi(ownerID); err != nil {
		http.Error(w, "user id must be a number", http.StatusBadRequest)
		return
	}

	limit := userChallengesDefaultLimit
	if raw := r.URL.Query().Get("limit"); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 {
			limit = n
		}
	}
	if limit > userChallengesMaxLimit {
		limit = userChallengesMaxLimit
	}
	offset := 0
	if raw := r.URL.Query().Get("offset"); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 {
			offset = n
		}
	}

	// Identity from the session, not the path. Viewing your own profile is
	// the only thing that unlocks your non-arena posts.
	viewingOwnProfile := authUserID(r) == ownerID

	challenges := GetChallengesByCreator(ownerID, viewingOwnProfile, limit, offset)
	if challenges == nil {
		challenges = []Challenge{}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(challenges)
}

// GetChallengesByCreator returns one person's challenges, newest first.
//
// No age window and no status filter, deliberately — see the note at the top
// of this file. Every row carries its encoded versions and manifest, because
// challengeBaseQuery selects them, so a profile plays what the worker made
// rather than the file that came off the camera.
func GetChallengesByCreator(creatorID string, includePrivate bool, limit, offset int) []Challenge {
	visibility := ""
	if !includePrivate {
		visibility = profileVisitorClause
	}
	return queryChallenges(challengeBaseQuery+profileWhereClause+visibility+`
	  ORDER BY c.created_at DESC
	  LIMIT $2 OFFSET $3`, creatorID, limit, offset)
}

// profileWhereClause is the whole of what a profile filters on: whose it is.
//
// Named so a test can assert there is no clock in it. The old path had one by
// accident — it borrowed the arena list, whose age window is right for an
// arena and wrong for a profile — and people's unanswered posts disappeared
// from their own profile after a day.
const profileWhereClause = `
	  WHERE c.creator_id = CAST($1 AS INT)`

// profileVisitorClause is added when the profile is not the viewer's own.
const profileVisitorClause = `
	    AND c.visibility = 'arena'`
