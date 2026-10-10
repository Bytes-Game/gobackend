package main

// battles_open.go — "Open to battles".
//
// Every post used to be a challenge: anybody could answer it with their own
// video, and the two then battled for votes. But people also post just to
// post — a trip, a pet, a song — with no wish to compete. So each post now
// says whether it is open to battles.
//
// Open (the default, and every post from before): as it always was.
//
// Not open: a normal post. People watch, like, comment and share it, but
// nobody can answer it. The server refuses an answer
// (validateChallengeResponseSubmission), it is not listed among the
// owner's open challenges, and the arena's list of live contests leaves it
// out. The feed needs nothing new: a post with no answers is ranked as a
// short already, and that is what a normal post is.
//
// A post that is not open needs no question. Its Subject is its caption,
// up to maxCaptionChars, and its Prefix may be empty — which is also why a
// title only ends in "?" when it has an opener (challengeTitle).
//
// The owner can change it at any time (SetOpenToBattlesHandler). Closing a
// post that already has an answer stops anyone else joining; the battle
// already running carries on to its end.

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"unicode/utf8"

	"github.com/gorilla/mux"
	"github.com/lib/pq"
)

// notOpenToBattles is what somebody trying to answer a closed post is told.
const notOpenToBattles = "this post is not open to battles"

// maxCaptionChars is the longest a normal post's caption may be. A
// challenge's subject is short because it finishes a question ("Who is
// better at … pranks?"); a caption is the whole line.
const maxCaptionChars = 80

// openToBattles says whether the post being created may be answered.
// Absent means yes: apps from before the switch do not send it.
func (p CreateChallengePayload) openToBattles() bool {
	return p.OpenToBattles == nil || *p.OpenToBattles
}

// postWordsRefusal says what is wrong with a new post's words, or "" when
// they are fine. A post open to battles is a question and needs both
// halves; a normal post needs only its caption.
func postWordsRefusal(prefix, subject string, open bool) string {
	if !open {
		if subject == "" {
			return "write a caption for your post"
		}
		return captionTooLong(prefix, subject)
	}
	if prefix == "" || subject == "" {
		return "prefix and subject are required"
	}
	return challengeWordsTooLong(prefix, subject)
}

// fillBattlesOpen marks which of [chs] are not open to battles, for every
// list that hands posts to the app (markViewerState). The feed reads posts
// through dozens of queries of its own, and none of them select the
// column — so without this a normal post in the feed would offer
// "Accept challenge", and the answer would only be refused once somebody
// had recorded it.
func fillBattlesOpen(chs []*Challenge) {
	if db == nil || len(chs) == 0 {
		return
	}
	byID := map[int64][]*Challenge{}
	ids := make([]int64, 0, len(chs))
	for _, c := range chs {
		if c == nil {
			continue
		}
		id, err := strconv.ParseInt(c.ID, 10, 64)
		if err != nil {
			continue
		}
		if _, dup := byID[id]; !dup {
			ids = append(ids, id)
		}
		byID[id] = append(byID[id], c)
	}
	if len(ids) == 0 {
		return
	}
	rows, err := db.Query(`
		SELECT id, open_to_battles FROM challenges
		 WHERE id = ANY($1::int[])`, pq.Array(ids))
	if queryFailed("reading which posts are open to battles",
		"they go out as they were read, and an answer to a closed one is "+
			"still refused", err) {
		return
	}
	defer rows.Close()
	bad := 0
	for rows.Next() {
		var id int64
		var open bool
		if scanFailed("whether a post is open to battles", rows.Scan(&id, &open), &bad) {
			continue
		}
		for _, c := range byID[id] {
			c.ClosedToBattles = !open
		}
	}
	if err := rows.Err(); err != nil {
		queryFailed("reading which posts are open to battles", "some go out unmarked", err)
	}
}

// captionTooLong is challengeWordsTooLong for a normal post, whose caption
// may run longer than a challenge's subject.
func captionTooLong(prefix, subject string) string {
	if utf8.RuneCountInString(prefix) > maxPrefixChars {
		return fmt.Sprintf("The first part can be at most %d characters.", maxPrefixChars)
	}
	if utf8.RuneCountInString(subject) > maxCaptionChars {
		return fmt.Sprintf("A caption can be at most %d characters.", maxCaptionChars)
	}
	return ""
}

// PATCH /api/v1/challenges/{id}/battles   {"open": true|false}
//
// The owner opens their post to battles, or closes it. Answers already
// given stay; closing only stops new ones.
func SetOpenToBattlesHandler(w http.ResponseWriter, r *http.Request) {
	uid := authUserID(r)
	cid, err := strconv.Atoi(mux.Vars(r)["id"])
	if err != nil || uid == "" {
		http.Error(w, "sign in, with a post id", http.StatusBadRequest)
		return
	}
	var body struct {
		Open *bool `json:"open"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Open == nil {
		http.Error(w, `send {"open": true} or {"open": false}`, http.StatusBadRequest)
		return
	}
	if !allowAction(uid, "settings") {
		writeRateLimited(w, "settings")
		return
	}
	var owner int
	err = db.QueryRow(`SELECT creator_id FROM challenges WHERE id = $1`, cid).Scan(&owner)
	if queryFailed("finding who owns a post, to open or close it to battles",
		"answering no such post", err) {
		http.Error(w, "no such post", http.StatusNotFound)
		return
	}
	if strconv.Itoa(owner) != uid {
		http.Error(w, "only the person who posted it can change this", http.StatusForbidden)
		return
	}
	if _, err := db.Exec(`UPDATE challenges SET open_to_battles = $1 WHERE id = $2`,
		*body.Open, cid); err != nil {
		queryFailed("opening or closing a post to battles",
			"telling the owner it did not change", err)
		http.Error(w, "could not change it, try again", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"openToBattles": *body.Open})
}
