package main

// voters.go — who voted for whom in a battle, and who liked a video.
//
// For anyone who may watch the video, the way Instagram shows who liked a
// post: tap the number, see the people. It used to be only the battle's
// players and the poster; the owner asked for it open. A friends-only video
// stays closed to everyone it is not for (mayWatch), the same rule that
// decides who can watch it at all.
//
// Nobody is TOLD when a vote comes in — that was a stream of pings on any
// live battle — the list is there to open when they want it.
//
// Each person carries their profile photo, so the list shows faces.

// mayWatchList answers whether [viewer] may see the lists on challenge
// [cid], writing the refusal itself when not. A video they may not watch
// is "no such video" rather than "forbidden": saying it exists would tell
// them a friends-only video is there.

import (
	"net/http"
	"strconv"
	"time"

	"github.com/gorilla/mux"
)

// PersonAt is someone on one of these lists, and when.
type PersonAt struct {
	UserID    string `json:"userId"`
	Username  string `json:"username"`
	League    string `json:"league,omitempty"`
	AvatarURL string `json:"avatarUrl,omitempty"`
	At        string `json:"at"`
}

func mayWatchList(w http.ResponseWriter, viewer string, cid int) bool {
	vid, err := strconv.Atoi(viewer)
	if err != nil {
		http.Error(w, "sign in", http.StatusUnauthorized)
		return false
	}
	ok, err := mayWatch(vid, cid)
	if queryFailed("checking who may see a video's lists", "refusing the list", err) {
		http.Error(w, "could not check the video", http.StatusInternalServerError)
		return false
	}
	if !ok {
		http.Error(w, "no such video", http.StatusNotFound)
		return false
	}
	return true
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

// GET /api/v1/challenges/{id}/voters — who voted for whom. For anyone who
// may watch the battle.
func ChallengeVotersHandler(w http.ResponseWriter, r *http.Request) {
	viewer := authUserID(r)
	cid, err := strconv.Atoi(mux.Vars(r)["id"])
	if err != nil || viewer == "" {
		http.Error(w, "sign in, with a challenge id", http.StatusBadRequest)
		return
	}
	creator, _, ok := battlePlayers(cid)
	if !ok {
		http.Error(w, "no such challenge", http.StatusNotFound)
		return
	}
	if !mayWatchList(w, viewer, cid) {
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
		       COALESCE(u.league, ''), u.avatar_url, cv.created_at
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
			vrows.Scan(&resp, &p.UserID, &p.Username, &p.League, &p.AvatarURL, &at), &bad) {
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

// GET /api/v1/challenges/{id}/likers — who liked it. For anyone who may
// watch it.
func ChallengeLikersHandler(w http.ResponseWriter, r *http.Request) {
	viewer := authUserID(r)
	cid, err := strconv.Atoi(mux.Vars(r)["id"])
	if err != nil || viewer == "" {
		http.Error(w, "sign in, with a challenge id", http.StatusBadRequest)
		return
	}
	if !mayWatchList(w, viewer, cid) {
		return
	}
	rows, err := db.Query(`
		SELECT u.id::text, u.username, COALESCE(u.league, ''), u.avatar_url,
		       cl.created_at
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
			rows.Scan(&p.UserID, &p.Username, &p.League, &p.AvatarURL, &at), &bad) {
			continue
		}
		p.At = at.UTC().Format(time.RFC3339)
		people = append(people, p)
	}
	writeJSON(w, http.StatusOK, map[string]any{"likers": people})
}
