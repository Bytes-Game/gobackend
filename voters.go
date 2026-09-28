package main

// voters.go — who voted for whom in a battle, and who liked a video.
//
// For the people it is about, and nobody else: the two players in a battle
// can see who voted and for which of them; whoever posted a video can see
// who liked it. Nobody is TOLD when a vote comes in — that was a stream of
// pings on any live battle — the list is there to open when they want it.

import (
	"net/http"
	"strconv"
	"time"

	"github.com/gorilla/mux"
)

// PersonAt is someone on one of these lists, and when.
type PersonAt struct {
	UserID   string `json:"userId"`
	Username string `json:"username"`
	League   string `json:"league,omitempty"`
	At       string `json:"at"`
}

// VoterSide is one side of a battle and the people who voted for it.
type VoterSide struct {
	Username   string     `json:"username"`
	Role       string     `json:"role"` // "creator" or "responder"
	ResponseID string     `json:"responseId,omitempty"`
	Voters     []PersonAt `json:"voters"`
}

// battlePlayers is the creator and everyone who answered [challengeID].
func battlePlayers(challengeID int) (creatorID string, answerers map[string]bool, ok bool) {
	var creator int
	err := db.QueryRow(`SELECT creator_id FROM challenges WHERE id = $1`,
		challengeID).Scan(&creator)
	if queryFailed("finding who made a challenge", "refusing the list", err) {
		return "", nil, false
	}
	answerers = map[string]bool{}
	rows, err := db.Query(`SELECT responder_id::text FROM challenge_responses
	                        WHERE challenge_id = $1`, challengeID)
	if queryFailed("finding who answered a challenge", "refusing the list", err) {
		return "", nil, false
	}
	defer rows.Close()
	bad := 0
	for rows.Next() {
		var id string
		if scanFailed("someone who answered", rows.Scan(&id), &bad) {
			continue
		}
		answerers[id] = true
	}
	return strconv.Itoa(creator), answerers, true
}

// GET /api/v1/challenges/{id}/voters — who voted for whom. Only for the
// battle's own players.
func ChallengeVotersHandler(w http.ResponseWriter, r *http.Request) {
	viewer := authUserID(r)
	cid, err := strconv.Atoi(mux.Vars(r)["id"])
	if err != nil || viewer == "" {
		http.Error(w, "sign in, with a challenge id", http.StatusBadRequest)
		return
	}
	creator, answerers, ok := battlePlayers(cid)
	if !ok {
		http.Error(w, "no such challenge", http.StatusNotFound)
		return
	}
	if viewer != creator && !answerers[viewer] {
		http.Error(w, "only the people in this battle can see who voted",
			http.StatusForbidden)
		return
	}

	sides := []*VoterSide{}
	byResponse := map[string]*VoterSide{}
	var creatorName string
	err = db.QueryRow(`SELECT username FROM users WHERE id = $1`,
		creator).Scan(&creatorName)
	queryFailed("reading a challenge creator's name", "showing it blank", err)
	creatorSide := &VoterSide{Username: creatorName, Role: "creator",
		Voters: []PersonAt{}}
	sides = append(sides, creatorSide)

	rows, err := db.Query(`
		SELECT cr.id::text, u.username
		  FROM challenge_responses cr
		  JOIN users u ON u.id = cr.responder_id
		 WHERE cr.challenge_id = $1
		 ORDER BY cr.created_at`, cid)
	if queryFailed("listing a battle's answers", "answering with an error", err) {
		http.Error(w, "could not read the battle", http.StatusInternalServerError)
		return
	}
	bad := 0
	for rows.Next() {
		side := &VoterSide{Role: "responder", Voters: []PersonAt{}}
		if scanFailed("an answer in a battle",
			rows.Scan(&side.ResponseID, &side.Username), &bad) {
			continue
		}
		byResponse[side.ResponseID] = side
		sides = append(sides, side)
	}
	rows.Close()

	// A vote with no answer id is a vote for the creator (migration 011).
	vrows, err := db.Query(`
		SELECT COALESCE(cv.response_id::text, ''), u.id::text, u.username,
		       COALESCE(u.league, ''), cv.created_at
		  FROM challenge_votes cv
		  JOIN users u ON u.id = cv.voter_id
		 WHERE cv.challenge_id = $1
		 ORDER BY cv.created_at DESC
		 LIMIT 1000`, cid)
	if queryFailed("listing who voted in a battle", "answering with an error", err) {
		http.Error(w, "could not read the votes", http.StatusInternalServerError)
		return
	}
	defer vrows.Close()
	bad = 0
	for vrows.Next() {
		var resp string
		var p PersonAt
		var at time.Time
		if scanFailed("a vote in a battle",
			vrows.Scan(&resp, &p.UserID, &p.Username, &p.League, &at), &bad) {
			continue
		}
		p.At = at.UTC().Format(time.RFC3339)
		side := creatorSide
		if resp != "" {
			if s, ok := byResponse[resp]; ok {
				side = s
			} else {
				continue
			}
		}
		side.Voters = append(side.Voters, p)
	}
	writeJSON(w, http.StatusOK, map[string]any{"sides": sides})
}

// GET /api/v1/challenges/{id}/likers — who liked it. Only for whoever
// posted it.
func ChallengeLikersHandler(w http.ResponseWriter, r *http.Request) {
	viewer := authUserID(r)
	cid, err := strconv.Atoi(mux.Vars(r)["id"])
	if err != nil || viewer == "" {
		http.Error(w, "sign in, with a challenge id", http.StatusBadRequest)
		return
	}
	var creator int
	err = db.QueryRow(`SELECT creator_id FROM challenges WHERE id = $1`,
		cid).Scan(&creator)
	if queryFailed("finding who posted a video", "refusing the list", err) {
		http.Error(w, "no such video", http.StatusNotFound)
		return
	}
	if strconv.Itoa(creator) != viewer {
		http.Error(w, "only whoever posted it can see who liked it",
			http.StatusForbidden)
		return
	}
	rows, err := db.Query(`
		SELECT u.id::text, u.username, COALESCE(u.league, ''), cl.created_at
		  FROM challenge_likes cl
		  JOIN users u ON u.id = cl.user_id
		 WHERE cl.challenge_id = $1
		 ORDER BY cl.created_at DESC
		 LIMIT 1000`, cid)
	if queryFailed("listing who liked a video", "answering with an error", err) {
		http.Error(w, "could not read the likes", http.StatusInternalServerError)
		return
	}
	defer rows.Close()
	people := []PersonAt{}
	bad := 0
	for rows.Next() {
		var p PersonAt
		var at time.Time
		if scanFailed("someone who liked a video",
			rows.Scan(&p.UserID, &p.Username, &p.League, &at), &bad) {
			continue
		}
		p.At = at.UTC().Format(time.RFC3339)
		people = append(people, p)
	}
	writeJSON(w, http.StatusOK, map[string]any{"likers": people})
}
