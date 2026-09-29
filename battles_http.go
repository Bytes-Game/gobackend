package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/gorilla/mux"
)

// GET /api/v1/challenges/{id}/standings
//
// Who is winning, at any time: genuine votes, likes, views and shares per
// side, each side's rank, how many votes were taken off and why, and when
// voting closes. The owner asked that people in a battle can watch it
// progress rather than wait a week to find out.
func BattleStandingsHandler(w http.ResponseWriter, r *http.Request) {
	if db == nil {
		http.Error(w, "db unavailable", http.StatusServiceUnavailable)
		return
	}
	id, err := strconv.Atoi(mux.Vars(r)["id"])
	if err != nil {
		http.Error(w, "challenge id must be a number", http.StatusBadRequest)
		return
	}
	st, ok, err := loadBattleStandings(r.Context(), db, id)
	if err != nil {
		queryFailed("standings for battle "+strconv.Itoa(id), "answered 500", err)
		http.Error(w, "could not count this battle", http.StatusInternalServerError)
		return
	}
	if !ok {
		http.Error(w, "no such battle", http.StatusNotFound)
		return
	}
	// So the score page opens already showing your vote, and never offers
	// a second one.
	if viewer, err := strconv.Atoi(signedInUserID(r)); err == nil && viewer > 0 {
		st.YourVote = viewerVote(id, viewer)
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(st)
}

// POST /api/v1/challenges/{id}/battle-length
//
// Refused, always. A battle runs for the length chosen when it was posted:
// people vote knowing when it ends, and the owner asked that nobody can
// stretch it once it is up. The route stays so an older app that still
// offers "Make it longer" gets a plain answer instead of a 404.
func ExtendBattleHandler(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "A battle's length is set when it is posted and can't be changed.",
		http.StatusForbidden)
}

// ════════════════════════════════════════════════════════════════════════════
// A PERSON'S BATTLES, FOR THEIR PROFILE
// ════════════════════════════════════════════════════════════════════════════

// BattleCard is one row in a profile tab.
type BattleCard struct {
	ChallengeID  string     `json:"challengeId"`
	Title        string     `json:"title"`
	ThumbnailURL string     `json:"thumbnailUrl"`
	VideoURL     string     `json:"videoUrl"`
	Role         string     `json:"role"`   // "creator" | "responder"
	Status       string     `json:"status"` // open | active | completed
	Outcome      string     `json:"outcome,omitempty"`
	CreatedAt    time.Time  `json:"createdAt"`
	EndsAt       *time.Time `json:"endsAt,omitempty"`
	DecidedAt    *time.Time `json:"decidedAt,omitempty"`
	MyVotes      float64    `json:"myVotes"`
	TheirVotes   float64    `json:"theirVotes"`
	Opponent     string     `json:"opponent,omitempty"`
	RatingChange int        `json:"ratingChange"`
	Leading      bool       `json:"leading"`
	// The whole video, as a feed would send it, so the tab can show and
	// play it. See profile_videos.go.
	Video *Challenge `json:"video,omitempty"`
}

// BattleSummary is the record at the top of a profile.
type BattleSummary struct {
	Rating   int            `json:"rating"`
	League   string         `json:"league"`
	Wins     int            `json:"wins"`
	Losses   int            `json:"losses"`
	Draws    int            `json:"draws"`
	Streak   int            `json:"streak"`   // how many in a row
	StreakOf string         `json:"streakOf"` // "won" | "lost" | ""
	Counts   map[string]int `json:"counts"`   // open, live, won, lost, draw
}

var battleTabs = map[string]bool{"open": true, "live": true, "won": true, "lost": true, "draw": true}

// GET /api/v1/users/{id}/battles?tab=open|live|won|lost|draw&limit=&offset=
//
// Open: challenges nobody has accepted yet. Live: battles still being voted
// on, with who is ahead right now. Won / lost / draw: decided battles. The
// owner asked for all of these to be visible to everyone; a visitor sees only
// arena battles, the same rule as the profile's video grid.
func UserBattlesHandler(w http.ResponseWriter, r *http.Request) {
	if db == nil {
		http.Error(w, "db unavailable", http.StatusServiceUnavailable)
		return
	}
	uid, err := strconv.Atoi(mux.Vars(r)["id"])
	if err != nil {
		http.Error(w, "user id must be a number", http.StatusBadRequest)
		return
	}
	tab := r.URL.Query().Get("tab")
	if tab == "" {
		tab = "live"
	}
	if !battleTabs[tab] {
		http.Error(w, "tab must be open, live, won, lost or draw", http.StatusBadRequest)
		return
	}
	limit := 30
	if n, err := strconv.Atoi(r.URL.Query().Get("limit")); err == nil && n > 0 && n <= 60 {
		limit = n
	}
	offset := 0
	if n, err := strconv.Atoi(r.URL.Query().Get("offset")); err == nil && n > 0 {
		offset = n
	}
	visitor := authUserID(r) != strconv.Itoa(uid)

	summary, err := loadBattleSummary(r.Context(), uid, visitor)
	if err != nil {
		queryFailed("battle summary for user "+strconv.Itoa(uid), "answered 500", err)
		http.Error(w, "could not read this record", http.StatusInternalServerError)
		return
	}
	cards, err := loadBattleCards(r.Context(), db, uid, tab, visitor, limit, offset)
	if err != nil {
		queryFailed("battles tab "+tab+" for user "+strconv.Itoa(uid), "answered 500", err)
		http.Error(w, "could not read these battles", http.StatusInternalServerError)
		return
	}
	if cards == nil {
		cards = []BattleCard{}
	}
	attachBattleVideos(cards, authUserID(r))
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"summary": summary, "tab": tab, "battles": cards})
}

// visibleTo narrows a query to what a visitor may see. Written out at each
// call so the SQL checker can read every statement whole.
const battleVisitorClause = ` AND (NOT $2 OR c.visibility = 'arena')`

func loadBattleSummary(ctx context.Context, uid int, visitor bool) (BattleSummary, error) {
	s := BattleSummary{Counts: map[string]int{}}
	err := db.QueryRowContext(ctx, `
		SELECT rating, COALESCE(league, 'Unranked'), wins, losses, draws
		  FROM users WHERE id = $1`, uid).Scan(&s.Rating, &s.League, &s.Wins, &s.Losses, &s.Draws)
	if err != nil {
		return s, err
	}
	var open, live, won, lost, draw int
	err = db.QueryRowContext(ctx, `
		SELECT
		  (SELECT COUNT(*) FROM challenges c
		    WHERE c.creator_id = $1 AND c.status = 'open'`+battleVisitorClause+`),
		  (SELECT COUNT(*) FROM challenges c
		    WHERE c.status = 'active' AND c.resolved_at IS NULL
		      AND (c.creator_id = $1 OR EXISTS (SELECT 1 FROM challenge_responses cr
		                                         WHERE cr.challenge_id = c.id AND cr.responder_id = $1
		                                           AND NOT COALESCE(cr.is_hidden, FALSE)))`+battleVisitorClause+`),
		  (SELECT COUNT(*) FROM battle_results br JOIN challenges c ON c.id = br.challenge_id
		    WHERE br.user_id = $1 AND br.outcome = 'won'`+battleVisitorClause+`),
		  (SELECT COUNT(*) FROM battle_results br JOIN challenges c ON c.id = br.challenge_id
		    WHERE br.user_id = $1 AND br.outcome = 'lost'`+battleVisitorClause+`),
		  (SELECT COUNT(*) FROM battle_results br JOIN challenges c ON c.id = br.challenge_id
		    WHERE br.user_id = $1 AND br.outcome = 'draw'`+battleVisitorClause+`)`,
		uid, visitor).Scan(&open, &live, &won, &lost, &draw)
	if err != nil {
		return s, err
	}
	s.Counts = map[string]int{"open": open, "live": live, "won": won, "lost": lost, "draw": draw}

	// The current run: consecutive wins or losses, newest first. A draw ends
	// a run without starting one.
	rows, err := db.QueryContext(ctx, `
		SELECT outcome FROM battle_results
		 WHERE user_id = $1 AND outcome IN ('won','lost','draw')
		 ORDER BY decided_at DESC LIMIT 50`, uid)
	if err != nil {
		return s, err
	}
	defer rows.Close()
	seen := 0
	for rows.Next() {
		var o string
		if scanFailed("streak for user "+strconv.Itoa(uid), rows.Scan(&o), &seen) {
			continue
		}
		if s.StreakOf == "" {
			if o == "draw" {
				break
			}
			s.StreakOf = o
		}
		if o != s.StreakOf {
			break
		}
		s.Streak++
	}
	return s, rows.Err()
}

func loadBattleCards(ctx context.Context, q querier, uid int, tab string, visitor bool, limit, offset int) ([]BattleCard, error) {
	var rows *sql.Rows
	var err error
	switch tab {
	case "open":
		rows, err = q.QueryContext(ctx, `
			SELECT c.id, TRIM(c.prefix || ' ' || c.subject), COALESCE(c.thumbnail_url, ''),
			       COALESCE(c.video_url, ''), 'creator', c.status, '', c.created_at,
			       c.voting_ends_at, NULL::timestamptz, 0::real, 0::real, '', 0
			  FROM challenges c
			 WHERE c.creator_id = $1 AND c.status = 'open'`+battleVisitorClause+`
			 ORDER BY c.created_at DESC
			 LIMIT $3 OFFSET $4`, uid, visitor, limit, offset)
	case "live":
		rows, err = q.QueryContext(ctx, `
			SELECT c.id, TRIM(c.prefix || ' ' || c.subject), COALESCE(c.thumbnail_url, ''),
			       COALESCE(c.video_url, ''),
			       CASE WHEN c.creator_id = $1 THEN 'creator' ELSE 'responder' END,
			       c.status, '', c.created_at, c.voting_ends_at, NULL::timestamptz,
			       0::real, 0::real, '', 0
			  FROM challenges c
			 WHERE c.status = 'active' AND c.resolved_at IS NULL
			   AND (c.creator_id = $1 OR EXISTS (SELECT 1 FROM challenge_responses cr
			                                      WHERE cr.challenge_id = c.id AND cr.responder_id = $1
			                                        AND NOT COALESCE(cr.is_hidden, FALSE)))`+battleVisitorClause+`
			 ORDER BY c.voting_ends_at ASC NULLS LAST
			 LIMIT $3 OFFSET $4`, uid, visitor, limit, offset)
	default: // won, lost, draw
		rows, err = q.QueryContext(ctx, `
			SELECT c.id, TRIM(c.prefix || ' ' || c.subject), COALESCE(c.thumbnail_url, ''),
			       COALESCE(c.video_url, ''), br.role, c.status, br.outcome, c.created_at,
			       c.voting_ends_at, br.decided_at, br.raw_votes,
			       COALESCE((SELECT MAX(o.raw_votes) FROM battle_results o
			                  WHERE o.challenge_id = br.challenge_id AND o.user_id <> br.user_id), 0),
			       COALESCE((SELECT u.username FROM battle_results o JOIN users u ON u.id = o.user_id
			                  WHERE o.challenge_id = br.challenge_id AND o.user_id <> br.user_id
			                  ORDER BY o.votes DESC, o.user_id LIMIT 1), ''),
			       br.rating_after - br.rating_before
			  FROM battle_results br
			  JOIN challenges c ON c.id = br.challenge_id
			 WHERE br.user_id = $1 AND br.outcome = $5`+battleVisitorClause+`
			 ORDER BY br.decided_at DESC
			 LIMIT $3 OFFSET $4`, uid, visitor, limit, offset, tab)
	}
	if err != nil {
		return nil, err
	}
	var cards []BattleCard
	seen := 0
	for rows.Next() {
		var c BattleCard
		var id int
		var ends, decided sql.NullTime
		if scanFailed("battles tab "+tab, rows.Scan(&id, &c.Title, &c.ThumbnailURL, &c.VideoURL,
			&c.Role, &c.Status, &c.Outcome, &c.CreatedAt, &ends, &decided,
			&c.MyVotes, &c.TheirVotes, &c.Opponent, &c.RatingChange), &seen) {
			continue
		}
		c.ChallengeID = strconv.Itoa(id)
		if ends.Valid {
			t := ends.Time
			c.EndsAt = &t
		}
		if decided.Valid {
			t := decided.Time
			c.DecidedAt = &t
		}
		cards = append(cards, c)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}

	// A live battle's score is the live standings, not anything stored.
	if tab == "live" {
		me := strconv.Itoa(uid)
		for i := range cards {
			id, _ := strconv.Atoi(cards[i].ChallengeID)
			st, ok, err := loadBattleStandings(ctx, q, id)
			if err != nil || !ok {
				queryFailed("live score for battle "+cards[i].ChallengeID,
					"shown with no score", err)
				continue
			}
			// The votes people see everywhere else: every vote cast.
			for _, p := range st.Participants {
				shown := float64(p.ShownVotes)
				if p.UserID == me {
					cards[i].MyVotes = shown
					cards[i].Leading = p.Leading
				} else if shown >= cards[i].TheirVotes {
					cards[i].TheirVotes = shown
					cards[i].Opponent = p.Username
				}
			}
		}
	}
	return cards, nil
}

// POST /api/v1/challenges/responses/like   {"responseId": "12"}
//
// Likes, or un-likes, one ANSWER in a battle.
//
// Until this existed nothing ever wrote to challenge_response_likes. Every
// like on a battle reel went to the challenge — the creator's side — whichever
// video was on screen. So every answer on the platform showed 0 likes, and
// likes, the first tiebreak after votes, always favoured the creator.
func LikeResponseHandler(w http.ResponseWriter, r *http.Request) {
	if db == nil {
		http.Error(w, "db unavailable", http.StatusServiceUnavailable)
		return
	}
	var body struct {
		ResponseID string `json:"responseId"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "body must be {\"responseId\": \"…\"}", http.StatusBadRequest)
		return
	}
	rid, err := strconv.Atoi(body.ResponseID)
	if err != nil {
		http.Error(w, "responseId must be a number", http.StatusBadRequest)
		return
	}
	me := authUserID(r)
	uid, err := strconv.Atoi(me)
	if err != nil {
		http.Error(w, "sign in to like", http.StatusUnauthorized)
		return
	}
	// The same allowance as liking a challenge: one pool of likes per person.
	if !allowAction(me, "like") {
		writeRateLimited(w, "like")
		return
	}
	liked, count, err := toggleResponseLike(r.Context(), rid, uid)
	if errors.Is(err, errNoSuchAnswer) {
		http.Error(w, "no such answer", http.StatusNotFound)
		return
	}
	if err != nil {
		queryFailed("like answer "+body.ResponseID, "answered 500, nothing changed", err)
		http.Error(w, "could not save the like", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"liked": liked, "likes": count, "responseId": body.ResponseID,
	})
}

var errNoSuchAnswer = errors.New("no such answer")

// toggleResponseLike flips one person's like on one answer and returns the
// new state and the answer's like count.
func toggleResponseLike(ctx context.Context, rid, uid int) (bool, int, error) {
	var exists bool
	if err := db.QueryRowContext(ctx,
		`SELECT EXISTS (SELECT 1 FROM challenge_responses WHERE id = $1)`, rid).Scan(&exists); err != nil {
		return false, 0, err
	}
	if !exists {
		return false, 0, errNoSuchAnswer
	}
	res, err := db.ExecContext(ctx,
		`DELETE FROM challenge_response_likes WHERE response_id = $1 AND user_id = $2`, rid, uid)
	if err != nil {
		return false, 0, err
	}
	removed, err := res.RowsAffected()
	if err != nil {
		return false, 0, err
	}
	liked := removed == 0
	if liked {
		if _, err := db.ExecContext(ctx, `
			INSERT INTO challenge_response_likes (response_id, user_id)
			VALUES ($1, $2) ON CONFLICT DO NOTHING`, rid, uid); err != nil {
			return false, 0, err
		}
	}
	var count int
	if err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM challenge_response_likes WHERE response_id = $1`, rid).Scan(&count); err != nil {
		return liked, 0, err
	}
	return liked, count, nil
}
