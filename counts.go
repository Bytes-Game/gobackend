package main

// counts.go — one count for everything.
//
// ════════════════════════════════════════════════════════════════════════
// THE SAME BATTLE HAD THREE DIFFERENT VIEW COUNTS
// ════════════════════════════════════════════════════════════════════════
//
// The reel said 20 views, the live score said 6 and 3, and the battle page
// said 31. Each place counted its own way. Opening the battle page added a
// view every time, however often the same person opened it. The live score
// left out the two players themselves and very new accounts. Nothing
// counted views or shares per video, so there was nothing to add up.
//
// Now each thing is counted once, here, and every screen reads the result:
//
//   - a view is one person watching one video — the creator's or an
//     answer's — for at least viewMinMs, counted at most once a day.
//     challenges.views is the whole card, every video in it added up;
//     challenge_responses.views is one answer's part of that.
//   - a share is one person sharing one video, once.
//   - likes, votes, comments and saves are the rows themselves.
//
// fillCounts puts them on every video the app is sent, and the live score
// (loadBattleStandings) shows the same numbers per player.

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/gorilla/mux"
	"github.com/lib/pq"
)

// viewMinMs is how long a video has to be on screen to count as viewed.
const viewMinMs = 1500

// answerOf reports whether [responseID] is an answer in battle [challengeID].
func answerOf(challengeID, responseID int) bool {
	var ok bool
	err := db.QueryRow(`
		SELECT EXISTS (SELECT 1 FROM challenge_responses
		                WHERE id = $1 AND challenge_id = $2)`,
		responseID, challengeID).Scan(&ok)
	if queryFailed("checking an answer belongs to its battle", "treating it as not", err) {
		return false
	}
	return ok
}

// videosWatched is which videos of a card were watched: 0 for the
// creator's, an answer's id for the answer's. An app that does not say — a
// short, or an app from before sides were sent — watched the creator's.
func videosWatched(challengeID int, p WatchEventPayload) []int {
	if p.CreatorMs == 0 && p.OpponentMs == 0 {
		return []int{0}
	}
	var out []int
	if p.CreatorMs >= viewMinMs {
		out = append(out, 0)
	}
	if p.OpponentMs >= viewMinMs {
		if rid, err := strconv.Atoi(p.ResponseID); err == nil && rid > 0 &&
			answerOf(challengeID, rid) {
			out = append(out, rid)
		}
	}
	return out
}

// recordView counts [userID] watching one video of a card, at most once a
// day. Returns whether it counted.
func recordView(userID, challengeID, responseID int) bool {
	res, err := db.Exec(`
		INSERT INTO video_views (user_id, challenge_id, response_id, day)
		VALUES ($1, $2, $3, CURRENT_DATE)
		ON CONFLICT DO NOTHING`, userID, challengeID, responseID)
	if err != nil {
		queryFailed("keeping a view of challenge "+strconv.Itoa(challengeID),
			"not counting it", err)
		return false
	}
	if n, err := res.RowsAffected(); err != nil || n == 0 {
		return false
	}
	_, err = db.Exec(`UPDATE challenges SET views = COALESCE(views, 0) + 1 WHERE id = $1`,
		challengeID)
	queryFailed("adding a view to challenge "+strconv.Itoa(challengeID),
		"its total is one short", err)
	if responseID > 0 {
		_, err = db.Exec(`
			UPDATE challenge_responses SET views = COALESCE(views, 0) + 1
			 WHERE id = $1`, responseID)
		queryFailed("adding a view to answer "+strconv.Itoa(responseID),
			"its count is one short", err)
	}
	return true
}

// POST /api/v1/challenges/share   {"challengeId": "7", "responseId": "12"}
//
// Someone shared a video: the creator's, or with responseId, an answer's.
// Counted once per person per video. Answers with the new total.
func ShareChallengeHandler(w http.ResponseWriter, r *http.Request) {
	uid, err := strconv.Atoi(authUserID(r))
	if err != nil || uid <= 0 {
		http.Error(w, "sign in to share", http.StatusUnauthorized)
		return
	}
	var body struct {
		ChallengeID string `json:"challengeId"`
		ResponseID  string `json:"responseId"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "body must be {\"challengeId\": ...}", http.StatusBadRequest)
		return
	}
	cid, err := strconv.Atoi(body.ChallengeID)
	if err != nil || cid <= 0 {
		http.Error(w, "challengeId must be a number", http.StatusBadRequest)
		return
	}
	var exists bool
	err = db.QueryRow(`SELECT EXISTS (SELECT 1 FROM challenges WHERE id = $1)`, cid).
		Scan(&exists)
	if queryFailed("finding a video someone shared", "answering 500", err) {
		http.Error(w, "could not read the video", http.StatusInternalServerError)
		return
	}
	if !exists {
		http.Error(w, "no such video", http.StatusNotFound)
		return
	}
	// The creator's video can be named by leaving responseId out, or — as
	// every vote dialog in the app does — by the challenge's own id.
	rid := 0
	if n, err := strconv.Atoi(body.ResponseID); err == nil && n > 0 && n != cid {
		if !answerOf(cid, n) {
			http.Error(w, "that answer is not part of this battle", http.StatusBadRequest)
			return
		}
		rid = n
	}
	_, err = db.Exec(`
		INSERT INTO video_shares (user_id, challenge_id, response_id)
		VALUES ($1, $2, $3)
		ON CONFLICT DO NOTHING`, uid, cid, rid)
	if queryFailed("keeping a share of challenge "+strconv.Itoa(cid), "answering 500", err) {
		http.Error(w, "could not save the share", http.StatusInternalServerError)
		return
	}
	var total int
	err = db.QueryRow(`SELECT COUNT(*) FROM video_shares WHERE challenge_id = $1`, cid).
		Scan(&total)
	queryFailed("counting shares of challenge "+strconv.Itoa(cid), "answering 0", err)
	writeJSON(w, http.StatusOK, map[string]any{"shares": total})
}

// fillCounts puts the comment, vote, share and save counts on each video,
// in one round trip. Every handler that sends videos to the app reaches
// this through markViewerState, so the reel, the profile grid, search and
// the battle page all show the same numbers.
func fillCounts(chs []*Challenge) {
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
		SELECT 'comments', challenge_id, COUNT(*) FROM challenge_comments
		 WHERE challenge_id = ANY($1::int[]) GROUP BY challenge_id
		UNION ALL
		SELECT 'votes', challenge_id, COUNT(*) FROM challenge_votes
		 WHERE challenge_id = ANY($1::int[]) GROUP BY challenge_id
		UNION ALL
		SELECT 'shares', challenge_id, COUNT(*) FROM video_shares
		 WHERE challenge_id = ANY($1::int[]) GROUP BY challenge_id
		UNION ALL
		SELECT 'saves', challenge_id, COUNT(*) FROM saved_challenges
		 WHERE challenge_id = ANY($1::int[]) GROUP BY challenge_id`, pq.Array(ids))
	if queryFailed("counting comments, votes, shares and saves",
		"the videos go out with those counts at 0", err) {
		return
	}
	defer rows.Close()
	bad := 0
	for rows.Next() {
		var kind string
		var id int64
		var n int
		if scanFailed("a count on a video", rows.Scan(&kind, &id, &n), &bad) {
			continue
		}
		for _, c := range byID[id] {
			switch kind {
			case "comments":
				c.CommentCount = n
			case "votes":
				c.VoteCount = n
			case "shares":
				c.ShareCount = n
			case "saves":
				c.SaveCount = n
			}
		}
	}
	if err := rows.Err(); err != nil {
		queryFailed("reading the counts on videos", "some go out at 0", err)
	}
}

// PeopleSide is one video in a battle — the creator's or an answer — and
// the people on a list for it: who liked it, voted for it, or shared it.
type PeopleSide struct {
	Username   string     `json:"username"`
	Role       string     `json:"role"` // "creator" or "responder"
	ResponseID string     `json:"responseId,omitempty"`
	People     []PersonAt `json:"people"`
}

// peopleLists are the lists there are.
var peopleLists = map[string]bool{"likes": true, "votes": true, "shares": true}

// peopleBySide reads one list for battle [cid], split by video. Sides come
// in battle order: the creator first, then the answers as they arrived.
func peopleBySide(cid int, what string) ([]*PeopleSide, error) {
	if !peopleLists[what] {
		return nil, errUnknownList
	}
	var creatorName string
	err := db.QueryRow(`
		SELECT u.username FROM challenges c JOIN users u ON u.id = c.creator_id
		 WHERE c.id = $1`, cid).Scan(&creatorName)
	if err != nil {
		return nil, err
	}
	creator := &PeopleSide{Username: creatorName, Role: "creator", People: []PersonAt{}}
	sides := []*PeopleSide{creator}
	byResponse := map[int]*PeopleSide{0: creator}

	rows, err := db.Query(`
		SELECT cr.id, u.username
		  FROM challenge_responses cr
		  JOIN users u ON u.id = cr.responder_id
		 WHERE cr.challenge_id = $1
		 ORDER BY cr.created_at, cr.id`, cid)
	if err != nil {
		return nil, err
	}
	bad := 0
	for rows.Next() {
		var rid int
		side := &PeopleSide{Role: "responder", People: []PersonAt{}}
		if scanFailed("an answer in a battle", rows.Scan(&rid, &side.Username), &bad) {
			continue
		}
		side.ResponseID = strconv.Itoa(rid)
		byResponse[rid] = side
		sides = append(sides, side)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}

	// Each list is the video (0 for the creator's), who, and when.
	var prows *sql.Rows
	switch what {
	case "likes":
		prows, err = db.Query(`
			SELECT 0, u.id::text, u.username, COALESCE(u.league, ''),
			       COALESCE(cl.created_at, NOW())
			  FROM challenge_likes cl
			  JOIN users u ON u.id = cl.user_id
			 WHERE cl.challenge_id = $1
			UNION ALL
			SELECT crl.response_id, u.id::text, u.username, COALESCE(u.league, ''),
			       COALESCE(crl.created_at, NOW())
			  FROM challenge_response_likes crl
			  JOIN challenge_responses cr ON cr.id = crl.response_id
			  JOIN users u ON u.id = crl.user_id
			 WHERE cr.challenge_id = $1
			 ORDER BY 5 DESC
			 LIMIT 2000`, cid)
	case "votes":
		prows, err = db.Query(`
			SELECT COALESCE(cv.response_id, 0), u.id::text, u.username,
			       COALESCE(u.league, ''), cv.created_at
			  FROM challenge_votes cv
			  JOIN users u ON u.id = cv.voter_id
			 WHERE cv.challenge_id = $1
			 ORDER BY cv.created_at DESC
			 LIMIT 2000`, cid)
	default: // shares
		prows, err = db.Query(`
			SELECT vs.response_id, u.id::text, u.username, COALESCE(u.league, ''),
			       vs.created_at
			  FROM video_shares vs
			  JOIN users u ON u.id = vs.user_id
			 WHERE vs.challenge_id = $1
			 ORDER BY vs.created_at DESC
			 LIMIT 2000`, cid)
	}
	if err != nil {
		return nil, err
	}
	defer prows.Close()
	bad = 0
	for prows.Next() {
		var rid int
		var p PersonAt
		var at time.Time
		if scanFailed("someone on a "+what+" list",
			prows.Scan(&rid, &p.UserID, &p.Username, &p.League, &at), &bad) {
			continue
		}
		p.At = at.UTC().Format(time.RFC3339)
		if side, ok := byResponse[rid]; ok {
			side.People = append(side.People, p)
		}
	}
	return sides, prows.Err()
}

var errUnknownList = &listError{"what must be likes, votes or shares"}

type listError struct{ msg string }

func (e *listError) Error() string { return e.msg }

// GET /api/v1/challenges/{id}/people?what=likes|votes|shares
//
// Who liked each video, who voted for whom, or who shared — for the people
// in it: whoever posted it, and on a battle whoever answered. Everyone else
// sees the counts, not the names.
func ChallengePeopleHandler(w http.ResponseWriter, r *http.Request) {
	viewer := authUserID(r)
	cid, err := strconv.Atoi(mux.Vars(r)["id"])
	if err != nil || viewer == "" {
		http.Error(w, "sign in, with a challenge id", http.StatusBadRequest)
		return
	}
	what := r.URL.Query().Get("what")
	if !peopleLists[what] {
		http.Error(w, errUnknownList.Error(), http.StatusBadRequest)
		return
	}
	creator, answerers, ok := battlePlayers(cid)
	if !ok {
		http.Error(w, "no such video", http.StatusNotFound)
		return
	}
	if viewer != creator && !answerers[viewer] {
		http.Error(w, "only the people in this video can see who", http.StatusForbidden)
		return
	}
	sides, err := peopleBySide(cid, what)
	if err == sql.ErrNoRows {
		http.Error(w, "no such video", http.StatusNotFound)
		return
	}
	if queryFailed("listing who "+what+" on challenge "+strconv.Itoa(cid),
		"answering with an error", err) {
		http.Error(w, "could not read the list", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"sides": sides})
}
