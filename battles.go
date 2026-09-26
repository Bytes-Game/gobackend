package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"math"
	"sort"
	"strconv"
	"time"

	"github.com/lib/pq"
)

// ════════════════════════════════════════════════════════════════════════════
// BATTLES: WHEN THEY END, WHO WON, AND WHAT IT DOES TO A RATING
// ════════════════════════════════════════════════════════════════════════════
//
// Until this file a battle had no end. It went from 'open' to 'active' when
// somebody answered it and stayed there; nothing ever wrote to users.wins or
// users.losses, and every league was the column default. The profile could
// show "12 wins" only in the sense that it could show a number.
//
// The rules, as the product owner set them:
//
//   * A battle runs at least SEVEN DAYS from the moment it is accepted. The
//     creator may choose longer, up to thirty, and may extend it while it
//     runs — never shorten it.
//   * Everyone can see, at any time, who is winning (loadBattleStandings).
//   * The winner is decided mostly by votes, then likes, then views, then
//     shares. Only GENUINE ones count: see voteWeight for what that means.
//   * Everyone can see a person's won and lost battles.
//   * Losing costs rating. Nobody stays on top without battling: a high
//     rating slowly drains while its owner sits out (decayIdleRatings).
//
// Everything that decides an outcome is a pure function over plain values,
// so the rules can be tested without a database. The queries that feed them
// are tested against a real one — see battles_db_test.go.

const (
	battleMinDays = 7
	battleMaxDays = 30

	ratingStart = 1000
	ratingFloor = 100
	eloK        = 32.0

	// A rating above this drains while its owner does not battle.
	decayAbove = 1200
	// How much a week off costs, and how often it is charged.
	decayPerWeek = 15

	// Removed votes that make a participant look like they bought them: at
	// least this many, and at least this share of everything cast for them.
	flagMinRemoved   = 3
	flagMinShare     = 0.30
	integrityPenalty = 25
)

// clampBattleDays keeps a requested length inside the rules. Zero means "the
// app did not say", which is the minimum.
func clampBattleDays(d int) int {
	if d < battleMinDays {
		return battleMinDays
	}
	if d > battleMaxDays {
		return battleMaxDays
	}
	return d
}

// leagueFor names the league for a rating. Somebody with no decided battles
// has not earned one yet, whatever their rating says.
func leagueFor(rating, decided int) string {
	if decided <= 0 {
		return "Unranked"
	}
	switch {
	case rating >= 1500:
		return "Diamond"
	case rating >= 1300:
		return "Platinum"
	case rating >= 1150:
		return "Gold"
	case rating >= 1050:
		return "Silver"
	default:
		return "Bronze"
	}
}

// ════════════════════════════════════════════════════════════════════════════
// WHAT COUNTS AS A GENUINE VOTE
// ════════════════════════════════════════════════════════════════════════════
//
// A vote is removed (weight 0) when:
//
//   * it was cast by somebody IN the battle. The vote endpoint refuses these
//     now; this catches any cast before it did.
//   * the voter's account was made AFTER the battle started. Opening fresh
//     accounts to vote for yourself is the cheapest way to cheat, and this is
//     the one signal it cannot avoid.
//   * the voter never watched the battle. The app records a view when a
//     video is on screen; a vote from somebody with no view of either side is
//     a script calling the API, not a person choosing.
//   * it is one of a BURST: three or more thin accounts (see below), made
//     within an hour of each other, all voting for the same side.
//
// A vote counts HALF when the voter has been active on fewer than two
// different days. A real new user is not a fraud, but one day of history is
// also what a throwaway account looks like, so it is not allowed to count
// the same as a regular's.
//
// Every removal is counted by reason and reported alongside the standings,
// so a participant can see what was taken off and why.

const (
	reasonOwnBattle  = "voted in their own battle"
	reasonNewAccount = "account made after the battle started"
	reasonNoWatch    = "voted without watching"
	reasonBurst      = "one of a burst of new accounts"
)

// voteEvidence is what the database knows about one vote and its voter.
type voteEvidence struct {
	VoterID        int
	ResponseID     int // 0 = a vote for the creator
	AccountCreated time.Time
	Watched        bool
	ActiveDays     int
}

// voteWeight is how much one vote counts, and if it counts for nothing, why.
func voteWeight(v voteEvidence, acceptedAt time.Time, participants map[int]bool) (float64, string) {
	if participants[v.VoterID] {
		return 0, reasonOwnBattle
	}
	if !acceptedAt.IsZero() && v.AccountCreated.After(acceptedAt) {
		return 0, reasonNewAccount
	}
	if !v.Watched {
		return 0, reasonNoWatch
	}
	if v.ActiveDays < 2 {
		return 0.5, ""
	}
	return 1, ""
}

// markBursts removes the votes that arrive as a burst of thin accounts: three
// or more voters with under three active days, whose accounts were all made
// within one hour, voting for the same side. It changes weights and reasons
// in place.
func markBursts(votes []voteEvidence, weights []float64, reasons []string) {
	bySide := map[int][]int{}
	for i, v := range votes {
		if weights[i] > 0 && v.ActiveDays < 3 {
			bySide[v.ResponseID] = append(bySide[v.ResponseID], i)
		}
	}
	for _, idx := range bySide {
		if len(idx) < 3 {
			continue
		}
		sort.Slice(idx, func(a, b int) bool {
			return votes[idx[a]].AccountCreated.Before(votes[idx[b]].AccountCreated)
		})
		// Sliding window: any hour holding three or more of them.
		start := 0
		for end := range idx {
			for votes[idx[end]].AccountCreated.Sub(votes[idx[start]].AccountCreated) > time.Hour {
				start++
			}
			if end-start+1 >= 3 {
				for k := start; k <= end; k++ {
					weights[idx[k]] = 0
					reasons[idx[k]] = reasonBurst
				}
			}
		}
	}
}

// ════════════════════════════════════════════════════════════════════════════
// STANDINGS
// ════════════════════════════════════════════════════════════════════════════

// Standing is one side of a battle and everything counted for it.
type Standing struct {
	UserID       string  `json:"userId"`
	Username     string  `json:"username"`
	Role         string  `json:"role"`       // "creator" | "responder"
	ResponseID   string  `json:"responseId"` // "" for the creator
	Votes        float64 `json:"votes"`      // genuine, after weighting
	RawVotes     int     `json:"rawVotes"`
	RemovedVotes int     `json:"removedVotes"`
	Likes        int     `json:"likes"`
	Views        int     `json:"views"`
	Shares       int     `json:"shares"`
	Rank         int     `json:"rank"`
	Leading      bool    `json:"leading"`

	userID     int
	responseID int
}

// BattleStandings is the live picture of one battle.
type BattleStandings struct {
	ChallengeID  string         `json:"challengeId"`
	Status       string         `json:"status"`
	BattleDays   int            `json:"battleDays"`
	AcceptedAt   *time.Time     `json:"acceptedAt,omitempty"`
	EndsAt       *time.Time     `json:"endsAt,omitempty"`
	Resolved     bool           `json:"resolved"`
	Participants []Standing     `json:"participants"`
	Removed      map[string]int `json:"removed"`
}

// querier is what the standings need from a database handle, so the same
// code reads through a plain connection for the live view and through the
// resolver's transaction when a battle is being decided.
type querier interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// betterSide reports whether a outranks b: votes first, then likes, views,
// shares. The owner's rule — "mostly votes, then likes, views, shares" — is
// an ordering, not a blend, so ten likes never outweigh one genuine vote.
func betterSide(a, b Standing) bool {
	if a.Votes != b.Votes {
		return a.Votes > b.Votes
	}
	if a.Likes != b.Likes {
		return a.Likes > b.Likes
	}
	if a.Views != b.Views {
		return a.Views > b.Views
	}
	return a.Shares > b.Shares
}

func sameScore(a, b Standing) bool {
	return !betterSide(a, b) && !betterSide(b, a)
}

func emptyScore(s Standing) bool {
	return s.Votes == 0 && s.Likes == 0 && s.Views == 0 && s.Shares == 0
}

// rankSides sorts best first and gives equal scores the same rank. Leading is
// set on rank one only when somebody has anything at all.
func rankSides(sides []Standing) {
	sort.SliceStable(sides, func(i, j int) bool { return betterSide(sides[i], sides[j]) })
	for i := range sides {
		if i > 0 && sameScore(sides[i], sides[i-1]) {
			sides[i].Rank = sides[i-1].Rank
		} else {
			sides[i].Rank = i + 1
		}
		sides[i].Leading = sides[i].Rank == 1 && !emptyScore(sides[i])
	}
}

// loadBattleStandings counts one battle. The bool is false when there is no
// such challenge.
func loadBattleStandings(ctx context.Context, q querier, challengeID int) (BattleStandings, bool, error) {
	out := BattleStandings{ChallengeID: strconv.Itoa(challengeID), Removed: map[string]int{}}

	var creatorID int
	var creatorName string
	var accepted, ends, resolved sql.NullTime
	err := q.QueryRowContext(ctx, `
		SELECT c.creator_id, u.username, c.status, c.battle_days,
		       c.accepted_at, c.voting_ends_at, c.resolved_at
		  FROM challenges c
		  JOIN users u ON u.id = c.creator_id
		 WHERE c.id = $1`, challengeID).Scan(
		&creatorID, &creatorName, &out.Status, &out.BattleDays, &accepted, &ends, &resolved)
	if err == sql.ErrNoRows {
		return out, false, nil
	}
	if err != nil {
		return out, false, fmt.Errorf("read battle %d: %w", challengeID, err)
	}
	if accepted.Valid {
		t := accepted.Time
		out.AcceptedAt = &t
	}
	if ends.Valid {
		t := ends.Time
		out.EndsAt = &t
	}
	out.Resolved = resolved.Valid

	sides := []Standing{{
		UserID: strconv.Itoa(creatorID), Username: creatorName, Role: "creator",
		userID: creatorID,
	}}
	participants := map[int]bool{creatorID: true}
	contentIDs := []string{strconv.Itoa(challengeID)}
	responseIDs := []string{}

	rows, err := q.QueryContext(ctx, `
		SELECT cr.id, cr.responder_id, u.username
		  FROM challenge_responses cr
		  JOIN users u ON u.id = cr.responder_id
		 WHERE cr.challenge_id = $1
		 ORDER BY cr.created_at, cr.id`, challengeID)
	if err != nil {
		return out, true, fmt.Errorf("read answers to battle %d: %w", challengeID, err)
	}
	seenScan := 0
	for rows.Next() {
		var rid, uid int
		var name string
		if scanFailed("battle standings: answers", rows.Scan(&rid, &uid, &name), &seenScan) {
			continue
		}
		// One side per person. Somebody who answered twice is still one
		// competitor, and the first answer is the one that joined the battle.
		if participants[uid] {
			continue
		}
		participants[uid] = true
		sides = append(sides, Standing{
			UserID: strconv.Itoa(uid), Username: name, Role: "responder",
			ResponseID: strconv.Itoa(rid), userID: uid, responseID: rid,
		})
		contentIDs = append(contentIDs, strconv.Itoa(rid))
		responseIDs = append(responseIDs, strconv.Itoa(rid))
	}
	if err := rows.Close(); err != nil {
		return out, true, err
	}
	sideByResponse := map[int]int{}
	for i, s := range sides {
		sideByResponse[s.responseID] = i
	}
	participantIDs := []int64{}
	participantText := []string{}
	for id := range participants {
		participantIDs = append(participantIDs, int64(id))
		participantText = append(participantText, strconv.Itoa(id))
	}

	// ── votes ────────────────────────────────────────────────────────────
	//
	// "Watched" is any event on either side's video from before the battle
	// closed. Not bounded by the vote's own time: the app sends its events
	// in batches, so the view of a video can land on the server a little
	// after the vote cast while watching it.
	vrows, err := q.QueryContext(ctx, `
		SELECT cv.voter_id,
		       COALESCE(cv.response_id, 0),
		       COALESCE(u.created_at, TIMESTAMPTZ 'epoch'),
		       EXISTS (SELECT 1 FROM feed_events fe
		                WHERE fe.user_id = cv.voter_id::text
		                  AND fe.content_id = ANY($2::text[])
		                  AND fe.created_at <= COALESCE($3, NOW())),
		       (SELECT COUNT(DISTINCT (fe.created_at AT TIME ZONE 'UTC')::date)
		          FROM feed_events fe
		         WHERE fe.user_id = cv.voter_id::text
		           AND fe.created_at <= COALESCE($3, NOW()))
		  FROM challenge_votes cv
		  JOIN users u ON u.id = cv.voter_id
		 WHERE cv.challenge_id = $1`,
		challengeID, pq.Array(contentIDs), nullTimeArg(ends))
	if err != nil {
		return out, true, fmt.Errorf("read votes in battle %d: %w", challengeID, err)
	}
	votes := []voteEvidence{}
	seenScan = 0
	for vrows.Next() {
		var v voteEvidence
		if scanFailed("battle standings: votes",
			vrows.Scan(&v.VoterID, &v.ResponseID, &v.AccountCreated, &v.Watched, &v.ActiveDays),
			&seenScan) {
			continue
		}
		votes = append(votes, v)
	}
	if err := vrows.Close(); err != nil {
		return out, true, err
	}
	var acceptedAt time.Time
	if accepted.Valid {
		acceptedAt = accepted.Time
	}
	weights := make([]float64, len(votes))
	reasons := make([]string, len(votes))
	for i, v := range votes {
		weights[i], reasons[i] = voteWeight(v, acceptedAt, participants)
	}
	markBursts(votes, weights, reasons)
	for i, v := range votes {
		idx, ok := sideByResponse[v.ResponseID]
		if !ok {
			// A vote for an answer that is not in this battle. The vote
			// endpoint refuses these and migration 011 cleared the old ones,
			// so this is a loud impossibility rather than a quiet skip.
			log.Printf("battle %d: vote by %d names response %d, which is not "+
				"a side of this battle — not counted", challengeID, v.VoterID, v.ResponseID)
			continue
		}
		sides[idx].RawVotes++
		sides[idx].Votes += weights[i]
		if weights[i] == 0 {
			sides[idx].RemovedVotes++
			out.Removed[reasons[i]]++
		}
	}

	// ── likes ────────────────────────────────────────────────────────────
	//
	// Not from anybody in the battle, and not from a thin account made after
	// it started — the same shape as a bought vote.
	lrows, err := q.QueryContext(ctx, `
		SELECT l.response_id, COUNT(*)
		  FROM (SELECT 0 AS response_id, cl.user_id
		          FROM challenge_likes cl
		         WHERE cl.challenge_id = $1
		        UNION ALL
		        SELECT crl.response_id, crl.user_id
		          FROM challenge_response_likes crl
		          JOIN challenge_responses cr ON cr.id = crl.response_id
		         WHERE cr.challenge_id = $1) l
		  JOIN users u ON u.id = l.user_id
		 WHERE NOT (l.user_id = ANY($2::int[]))
		   AND NOT ($3::timestamptz IS NOT NULL
		            AND u.created_at > $3
		            AND (SELECT COUNT(DISTINCT (fe.created_at AT TIME ZONE 'UTC')::date)
		                   FROM feed_events fe
		                  WHERE fe.user_id = l.user_id::text) < 2)
		 GROUP BY l.response_id`,
		challengeID, pq.Array(participantIDs), nullTimeArg(accepted))
	if err != nil {
		return out, true, fmt.Errorf("read likes in battle %d: %w", challengeID, err)
	}
	seenScan = 0
	for lrows.Next() {
		var rid, n int
		if scanFailed("battle standings: likes", lrows.Scan(&rid, &n), &seenScan) {
			continue
		}
		if idx, ok := sideByResponse[rid]; ok {
			sides[idx].Likes = n
		}
	}
	if err := lrows.Close(); err != nil {
		return out, true, err
	}

	// ── views and shares ─────────────────────────────────────────────────
	//
	// A battle reel shows both videos under one card, and for a long time
	// every view, like and share on it was recorded against the challenge
	// alone. An event like that cannot be split between the two sides, and
	// counting it for the creator would hand every tie to them. So only
	// events that say which side they were for are counted:
	//
	//   - a view of the reel says how long each face was on screen:
	//     metadata {"creatorMs", "opponentMs", "responseId"}. One view can
	//     count for both sides, if both were watched for two seconds.
	//   - a completion or share on the reel says which face was showing:
	//     metadata {"side": "creator" | "opponent", "responseId"}.
	//   - an answer played on its own is recorded against the answer.
	//
	// Then the same filters as likes, and the same end as votes. Without
	// them a vote from a throwaway account was removed but its view still
	// counted, so three throwaways could not win a battle on votes and could
	// on views.
	erows, err := q.QueryContext(ctx, `
		WITH ev AS (
		    SELECT '0' AS side, fe.user_id, 'view' AS kind, fe.created_at
		      FROM feed_events fe
		     WHERE fe.content_type = 'challenge' AND fe.content_id = $1
		       AND fe.event_type = 'view'
		       AND CASE WHEN fe.metadata->>'creatorMs' ~ '^[0-9]{1,9}$'
		                THEN (fe.metadata->>'creatorMs')::int >= 2000 ELSE FALSE END
		    UNION ALL
		    SELECT fe.metadata->>'responseId', fe.user_id, 'view', fe.created_at
		      FROM feed_events fe
		     WHERE fe.content_type = 'challenge' AND fe.content_id = $1
		       AND fe.event_type = 'view'
		       AND fe.metadata->>'responseId' = ANY($2::text[])
		       AND CASE WHEN fe.metadata->>'opponentMs' ~ '^[0-9]{1,9}$'
		                THEN (fe.metadata->>'opponentMs')::int >= 2000 ELSE FALSE END
		    UNION ALL
		    SELECT CASE WHEN fe.metadata->>'side' = 'creator' THEN '0'
		                ELSE fe.metadata->>'responseId' END,
		           fe.user_id,
		           CASE WHEN fe.event_type = 'share' THEN 'share' ELSE 'view' END,
		           fe.created_at
		      FROM feed_events fe
		     WHERE fe.content_type = 'challenge' AND fe.content_id = $1
		       AND fe.event_type IN ('complete', 'share')
		       AND (fe.metadata->>'side' = 'creator'
		            OR (fe.metadata->>'side' = 'opponent'
		                AND fe.metadata->>'responseId' = ANY($2::text[])))
		    UNION ALL
		    SELECT fe.content_id, fe.user_id,
		           CASE WHEN fe.event_type = 'share' THEN 'share' ELSE 'view' END,
		           fe.created_at
		      FROM feed_events fe
		     WHERE fe.content_type = 'response' AND fe.content_id = ANY($2::text[])
		       AND (fe.event_type IN ('complete', 'share')
		            OR (fe.event_type = 'view' AND fe.watch_duration_ms >= 2000))
		), counted AS (
		    SELECT side, user_id, kind
		      FROM ev
		     WHERE NOT (user_id = ANY($3::text[]))
		       AND created_at <= COALESCE($4, NOW())
		), thin AS (
		    SELECT u.id::text AS user_id
		      FROM users u
		     WHERE $5::timestamptz IS NOT NULL
		       AND u.id::text IN (SELECT user_id FROM counted)
		       AND u.created_at > $5
		       AND (SELECT COUNT(DISTINCT (f.created_at AT TIME ZONE 'UTC')::date)
		              FROM feed_events f
		             WHERE f.user_id = u.id::text) < 2
		)
		SELECT side,
		       COUNT(DISTINCT user_id) FILTER (WHERE kind = 'view'),
		       COUNT(DISTINCT user_id) FILTER (WHERE kind = 'share')
		  FROM counted
		 WHERE user_id NOT IN (SELECT user_id FROM thin)
		 GROUP BY side`,
		strconv.Itoa(challengeID), pq.Array(responseIDs), pq.Array(participantText),
		nullTimeArg(ends), nullTimeArg(accepted))
	if err != nil {
		return out, true, fmt.Errorf("read views in battle %d: %w", challengeID, err)
	}
	seenScan = 0
	for erows.Next() {
		var side string
		var views, shares int
		if scanFailed("battle standings: views", erows.Scan(&side, &views, &shares), &seenScan) {
			continue
		}
		rid, _ := strconv.Atoi(side)
		if idx, ok := sideByResponse[rid]; ok {
			sides[idx].Views, sides[idx].Shares = views, shares
		}
	}
	if err := erows.Close(); err != nil {
		return out, true, err
	}

	// Ranked on the exact weights, then rounded for display.
	rankSides(sides)
	for i := range sides {
		sides[i].Votes = math.Round(sides[i].Votes*10) / 10
	}
	out.Participants = sides
	return out, true, nil
}

func nullTimeArg(t sql.NullTime) any {
	if t.Valid {
		return t.Time
	}
	return nil
}

// ════════════════════════════════════════════════════════════════════════════
// DECIDING
// ════════════════════════════════════════════════════════════════════════════

// competitor is a participant's record going into a decision.
type competitor struct {
	UserID              int
	Rating              int
	Wins, Losses, Draws int
}

// battleVerdict is what one participant takes away from a decided battle.
type battleVerdict struct {
	Standing
	Outcome      string // "won" | "lost" | "draw" | "none"
	RatingBefore int
	RatingAfter  int
	Flagged      bool
}

// expected is the Elo chance that a beats b.
func expected(a, b float64) float64 {
	return 1 / (1 + math.Pow(10, (b-a)/400))
}

// decideBattle turns final standings into outcomes and new ratings.
//
// Top score wins, everyone below loses. Several sides tied on top draw with
// each other and each beat everyone below. Nobody with anything at all —
// not one genuine vote, like, view or share — means no result: nobody's
// record changes for a battle nobody took part in.
//
// Ratings move by Elo: beating somebody rated above you gains more than
// beating somebody below you, and losing to somebody below you costs more.
// It is what makes the top a place you have to keep earning. A participant
// flagged for bought votes (see flagMinRemoved) also pays integrityPenalty.
func decideBattle(sides []Standing, records map[int]competitor) []battleVerdict {
	out := make([]battleVerdict, len(sides))
	ranked := append([]Standing(nil), sides...)
	rankSides(ranked)
	for i, s := range ranked {
		r := records[s.userID].Rating
		if r == 0 {
			r = ratingStart
		}
		out[i] = battleVerdict{Standing: s, RatingBefore: r, Outcome: "none"}
	}
	if len(ranked) < 2 || emptyScore(ranked[0]) {
		for i := range out {
			out[i].RatingAfter = out[i].RatingBefore
		}
		return out
	}

	var top []int
	for i := range out {
		if out[i].Rank == 1 {
			top = append(top, i)
		}
	}
	delta := make([]float64, len(out))
	for i := range out {
		if out[i].Rank == 1 {
			if len(top) > 1 {
				out[i].Outcome = "draw"
			} else {
				out[i].Outcome = "won"
			}
			continue
		}
		out[i].Outcome = "lost"
	}
	// Winners (or those tied on top) against everyone below them. With a tie
	// on top the gain is shared, so a three-way draw does not triple it.
	share := 1.0 / float64(len(top))
	for _, w := range top {
		for l := range out {
			if out[l].Rank == 1 {
				continue
			}
			e := expected(float64(out[w].RatingBefore), float64(out[l].RatingBefore))
			d := eloK * (1 - e) * share
			delta[w] += d
			delta[l] -= d
		}
	}
	// Tied on top: a draw pulls ratings toward each other.
	for a := 0; a < len(top); a++ {
		for b := a + 1; b < len(top); b++ {
			i, j := top[a], top[b]
			e := expected(float64(out[i].RatingBefore), float64(out[j].RatingBefore))
			d := eloK * (0.5 - e)
			delta[i] += d
			delta[j] -= d
		}
	}
	for i := range out {
		after := out[i].RatingBefore + int(math.Round(delta[i]))
		if out[i].RemovedVotes >= flagMinRemoved &&
			float64(out[i].RemovedVotes) >= flagMinShare*float64(out[i].RawVotes) {
			out[i].Flagged = true
			after -= integrityPenalty
		}
		if after < ratingFloor {
			after = ratingFloor
		}
		out[i].RatingAfter = after
	}
	return out
}

// ════════════════════════════════════════════════════════════════════════════
// THE RESOLVER
// ════════════════════════════════════════════════════════════════════════════

// resolveBattle decides one battle whose voting has closed. Safe to call twice
// and from two processes: the row is locked, and a battle already decided is
// left alone. Returns whether this call decided it.
func resolveBattle(ctx context.Context, challengeID int) (bool, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback() }()

	var due bool
	err = tx.QueryRowContext(ctx, `
		SELECT voting_ends_at <= NOW()
		  FROM challenges
		 WHERE id = $1 AND resolved_at IS NULL AND voting_ends_at IS NOT NULL
		 FOR UPDATE`, challengeID).Scan(&due)
	if err == sql.ErrNoRows || (err == nil && !due) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("lock battle %d: %w", challengeID, err)
	}

	st, ok, err := loadBattleStandings(ctx, tx, challengeID)
	if err != nil || !ok {
		return false, err
	}

	ids := make([]int64, 0, len(st.Participants))
	for _, s := range st.Participants {
		ids = append(ids, int64(s.userID))
	}
	records := map[int]competitor{}
	rows, err := tx.QueryContext(ctx, `
		SELECT id, rating, wins, losses, draws
		  FROM users WHERE id = ANY($1::int[])
		 ORDER BY id
		 FOR UPDATE`, pq.Array(ids))
	if err != nil {
		return false, fmt.Errorf("lock competitors of battle %d: %w", challengeID, err)
	}
	for rows.Next() {
		var c competitor
		if err := rows.Scan(&c.UserID, &c.Rating, &c.Wins, &c.Losses, &c.Draws); err != nil {
			_ = rows.Close()
			return false, fmt.Errorf("read competitor in battle %d: %w", challengeID, err)
		}
		records[c.UserID] = c
	}
	if err := rows.Close(); err != nil {
		return false, err
	}

	for _, v := range decideBattle(st.Participants, records) {
		var rid any
		if v.responseID != 0 {
			rid = v.responseID
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO battle_results
			    (challenge_id, user_id, role, response_id, outcome,
			     votes, raw_votes, removed_votes, likes, views, shares,
			     flagged, rating_before, rating_after)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
			ON CONFLICT (challenge_id, user_id) DO NOTHING`,
			challengeID, v.userID, v.Role, rid, v.Outcome,
			v.Votes, v.RawVotes, v.RemovedVotes, v.Likes, v.Views, v.Shares,
			v.Flagged, v.RatingBefore, v.RatingAfter); err != nil {
			return false, fmt.Errorf("record result for %d in battle %d: %w", v.userID, challengeID, err)
		}
		if v.Outcome == "none" {
			continue
		}
		c := records[v.userID]
		switch v.Outcome {
		case "won":
			c.Wins++
		case "lost":
			c.Losses++
		case "draw":
			c.Draws++
		}
		strike := 0
		if v.Flagged {
			strike = 1
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE users
			   SET wins = $2, losses = $3, draws = $4, rating = $5, league = $6,
			       last_battle_at = NOW(),
			       integrity_strikes = integrity_strikes + $7
			 WHERE id = $1`,
			v.userID, c.Wins, c.Losses, c.Draws, v.RatingAfter,
			leagueFor(v.RatingAfter, c.Wins+c.Losses+c.Draws), strike); err != nil {
			return false, fmt.Errorf("update record of %d after battle %d: %w", v.userID, challengeID, err)
		}
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE challenges SET status = 'completed', resolved_at = NOW()
		 WHERE id = $1`, challengeID); err != nil {
		return false, fmt.Errorf("close battle %d: %w", challengeID, err)
	}
	return true, tx.Commit()
}

// resolveDueBattles decides every battle whose time is up, oldest first.
func resolveDueBattles(ctx context.Context, limit int) (int, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT id FROM challenges
		 WHERE resolved_at IS NULL
		   AND voting_ends_at IS NOT NULL
		   AND voting_ends_at <= NOW()
		 ORDER BY voting_ends_at
		 LIMIT $1`, limit)
	if err != nil {
		return 0, err
	}
	var ids []int
	seen := 0
	for rows.Next() {
		var id int
		if scanFailed("battles due to be decided", rows.Scan(&id), &seen) {
			continue
		}
		ids = append(ids, id)
	}
	if err := rows.Close(); err != nil {
		return 0, err
	}
	decided := 0
	for _, id := range ids {
		ok, err := resolveBattle(ctx, id)
		if err != nil {
			log.Printf("battle %d could not be decided: %v — it stays open and "+
				"is tried again next round", id, err)
			continue
		}
		if ok {
			decided++
		}
	}
	return decided, nil
}

// decayIdleRatings charges a week off to every rating above decayAbove whose
// owner has not finished a battle in that week. Once a week at most per
// person, never below decayAbove. Returns how many were charged.
func decayIdleRatings(ctx context.Context) (int, error) {
	rows, err := db.QueryContext(ctx, `
		UPDATE users
		   SET rating = GREATEST($1, rating - $2),
		       rating_decayed_at = NOW()
		 WHERE rating > $1
		   AND COALESCE(last_battle_at, TIMESTAMPTZ 'epoch') < NOW() - INTERVAL '7 days'
		   AND COALESCE(rating_decayed_at, TIMESTAMPTZ 'epoch') < NOW() - INTERVAL '7 days'
		RETURNING id, rating, wins + losses + draws`, decayAbove, decayPerWeek)
	if err != nil {
		return 0, err
	}
	type row struct{ id, rating, decided int }
	var changed []row
	seen := 0
	for rows.Next() {
		var r row
		if scanFailed("ratings after decay", rows.Scan(&r.id, &r.rating, &r.decided), &seen) {
			continue
		}
		changed = append(changed, r)
	}
	if err := rows.Close(); err != nil {
		return 0, err
	}
	for _, r := range changed {
		if _, err := db.ExecContext(ctx, `UPDATE users SET league = $2 WHERE id = $1`,
			r.id, leagueFor(r.rating, r.decided)); err != nil {
			log.Printf("league of %d after decay: %v — shows the old league until "+
				"their next battle", r.id, err)
		}
	}
	return len(changed), nil
}

// startBattleResolver decides due battles every ten minutes and charges idle
// ratings once an hour. A battle's end is a date, not an event, so something
// has to look at the clock.
func startBattleResolver() {
	go func() {
		tick := time.NewTicker(10 * time.Minute)
		defer tick.Stop()
		for round := 0; ; round++ {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			battleResolverRound(ctx, round)
			cancel()
			<-tick.C
		}
	}()
}

// battleResolverRound is one pass of the resolver: decide what is due, and on
// every sixth round (once an hour) charge idle ratings. Split out so a test
// can run a round without waiting ten minutes for the ticker.
func battleResolverRound(ctx context.Context, round int) {
	if n, err := resolveDueBattles(ctx, 50); err != nil {
		log.Printf("battle resolver: %v — nothing decided this round", err)
	} else if n > 0 {
		log.Printf("battle resolver: decided %d battle(s)", n)
	}
	if round%6 == 0 {
		if n, err := decayIdleRatings(ctx); err != nil {
			log.Printf("rating decay: %v — idle ratings kept this hour", err)
		} else if n > 0 {
			log.Printf("rating decay: %d idle rating(s) charged a week off", n)
		}
	}
}
