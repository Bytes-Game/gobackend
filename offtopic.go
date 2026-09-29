package main

// offtopic.go — a video that doesn't match its challenge costs the person who
// posted it, and when the evidence is strong enough it is taken down.
//
// ════════════════════════════════════════════════════════════════════════════
// WHY THIS EXISTS
// ════════════════════════════════════════════════════════════════════════════
//
// Anybody could answer "Who can juggle five balls?" with a cat video and
// nothing happened. The battle ran its week, and the cat could win it. It went
// the other way too: a challenge could ask one thing and show another.
//
// ════════════════════════════════════════════════════════════════════════════
// HOW A VIDEO IS CAUGHT
// ════════════════════════════════════════════════════════════════════════════
//
// Two kinds of evidence:
//
//	THE MODEL — the worker that prepares every upload also asks its model
//	            "does this video match <the challenge>?" and gets back yes,
//	            no or unsure. It goes by what is said and written in the
//	            video, or by still frames when nothing is said. See
//	            cmd/hls-worker/understand.go.
//
//	PEOPLE    — anybody can report a video that doesn't match: the other
//	            side of the battle, or a viewer.
//
// What happens:
//
//   - The model said "no" AND at least one person reported it: the video is
//     TAKEN DOWN. Both kinds of evidence agree.
//   - offTopicReportsAlone viewers with no part in the battle reported it,
//     whatever the model said: its owner is PENALISED, once, and the video
//     STAYS UP. The owner asked for this — with so few videos on the app,
//     reports alone must not be able to remove one. (The model may never
//     have been asked: videos from before this, or a worker with no model.)
//   - The model on its own takes nothing down and charges nothing. It is
//     small and it can be wrong. What it does on its own is warn the owner,
//     so they can put it right before anybody reports it.
//
// A report counts only from somebody who can fairly make it: somebody else in
// the battle, or a viewer who actually watched that video and whose account
// is older than it. Without that, three accounts made in the same minute
// could take down anything.
//
// ════════════════════════════════════════════════════════════════════════════
// WHAT IT COSTS
// ════════════════════════════════════════════════════════════════════════════
//
// Penalised on reports alone: the owner loses integrityPenalty rating points
// and gets a strike. Nothing else changes; the battle goes on.
//
// Taken down:
//
//   - The video comes down. An answer drops out of its battle; a challenge
//     is removed from the app.
//   - Its owner loses integrityPenalty rating points and gets a strike —
//     unless reports already charged them that for this video; nobody pays
//     it twice for the same video.
//   - If the battle was still being fought, the owner also LOSES it: a loss
//     on their record and the rating a loss costs, on top. If that leaves
//     one side standing, that side wins now and the battle is over. When the
//     challenge itself comes down, every answer still standing wins.
//   - A battle that was already decided stays decided. The owner still pays
//     the penalty.
//   - Both sides are told.

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"strconv"
	"strings"

	"github.com/gorilla/mux"
	"github.com/lib/pq"
)

const (
	// The model's three answers, as the worker sends them.
	matchYes    = "yes"
	matchNo     = "no"
	matchUnsure = "unsure"

	// How many viewers with no part in the battle it takes to penalise a
	// video's owner with no word from the model. The video stays up.
	offTopicReportsAlone = 3

	// Kinds of notification, as the app reads them.
	noteOffTopic        = "off_topic"
	noteOffTopicWarning = "off_topic_warning"
	noteOffTopicPenalty = "off_topic_penalty"
)

// offTopicAction is what the evidence against a video adds up to.
type offTopicAction int

const (
	offTopicNothing  offTopicAction = iota
	offTopicPenalise                // reports alone: the owner pays, the video stays
	offTopicTakeDown                // the model and a person agree: it comes down
)

// TriggerOffTopic is the phone push for "your video was taken down". Sent
// under the battle-updates setting, with "you won your battle".
const TriggerOffTopic TriggerKind = "off_topic"

// cleanMatch keeps only the three answers the model may give. Anything else —
// an older worker that sends nothing, a model that rambled — is "not asked".
func cleanMatch(s string) string {
	switch s = strings.ToLower(strings.TrimSpace(s)); s {
	case matchYes, matchNo, matchUnsure:
		return s
	}
	return ""
}

// offTopicVerdict decides what happens to a video. partyReports are from
// somebody else in the battle; viewerReports from viewers with no part in it.
// Both count only reports that can fairly be made — see the top of this file.
func offTopicVerdict(model string, partyReports, viewerReports int) offTopicAction {
	if model == matchNo && partyReports+viewerReports >= 1 {
		return offTopicTakeDown
	}
	if viewerReports >= offTopicReportsAlone {
		return offTopicPenalise
	}
	return offTopicNothing
}

// forfeitRatings is what taking a video down does to ratings while its battle
// is still being fought.
//
// The owner loses to every side still standing, the same way last place loses
// in decideBattle, and pays [penalty] on top — integrityPenalty, or nothing
// when reports already charged it for this video. gains are what each side
// still standing takes if the battle ends here; they are only paid out when
// it does.
func forfeitRatings(offender int, standing []int, penalty int) (offenderAfter int, gains []int) {
	gains = make([]int, len(standing))
	loss := 0.0
	if len(standing) > 0 {
		share := 1.0 / float64(len(standing))
		for i, r := range standing {
			d := eloK * (1 - expected(float64(r), float64(offender))) * share
			loss += d
			gains[i] = int(math.Round(d))
		}
	}
	return max(offender-int(math.Round(loss))-penalty, ratingFloor), gains
}

// penaltyRating is a rating after [penalty] points are taken off it, never
// below the floor. What a video costs when there is no battle to lose: no
// answer yet, a battle already decided, or reports alone.
func penaltyRating(rating, penalty int) int {
	return max(rating-penalty, ratingFloor)
}

// ════════════════════════════════════════════════════════════════════════════
// THE QUESTION THE WORKER ASKS
// ════════════════════════════════════════════════════════════════════════════

// jobQuestion is the challenge a video must match, the way people read it. A
// challenge is checked against its own words; an answer against the
// challenge it answers. Empty when it can't be read, and the worker then asks
// nothing — a video is never checked against a blank.
func jobQuestion(table string, id int) string {
	var prefix, subject string
	var err error
	if table == "challenge_responses" {
		err = db.QueryRow(`
			SELECT COALESCE(c.prefix, ''), COALESCE(c.subject, '')
			  FROM challenge_responses cr
			  JOIN challenges c ON c.id = cr.challenge_id
			 WHERE cr.id = $1`, id).Scan(&prefix, &subject)
	} else {
		err = db.QueryRow(`
			SELECT COALESCE(prefix, ''), COALESCE(subject, '')
			  FROM challenges WHERE id = $1`, id).Scan(&prefix, &subject)
	}
	if queryFailed(fmt.Sprintf("jobQuestion: reading the challenge for %s id=%d", table, id),
		"the worker is not asked whether this video matches its challenge", err) {
		return ""
	}
	return challengeTitle(prefix, subject)
}

// settleQuestionMatch writes down what the model said, and acts on a "no".
//
// Only a real answer is written. A reading with none — an older worker, or a
// model that could not tell — leaves the last one where it was.
//
// When the answer turns into "no", the owner is warned and the video is
// judged: somebody may have reported it before the model had a say.
func settleQuestionMatch(table string, id int, raw string) {
	match := cleanMatch(raw)
	if db == nil || match == "" {
		return
	}
	var challengeID, ownerID int
	var standing bool
	var err error
	if table == "challenge_responses" {
		err = db.QueryRow(`
			UPDATE challenge_responses SET question_match = $2
			 WHERE id = $1 AND question_match <> $2
			RETURNING challenge_id, responder_id, off_topic_at IS NULL`,
			id, match).Scan(&challengeID, &ownerID, &standing)
	} else {
		err = db.QueryRow(`
			UPDATE challenges SET question_match = $2
			 WHERE id = $1 AND question_match <> $2
			RETURNING id, creator_id, off_topic_at IS NULL`,
			id, match).Scan(&challengeID, &ownerID, &standing)
	}
	if err == sql.ErrNoRows {
		return // the same answer as last time: already acted on
	}
	if queryFailed(fmt.Sprintf("settleQuestionMatch: saving the model's answer for %s id=%d", table, id),
		"a video the model thinks doesn't match stays up until three people report it", err) {
		return
	}
	log.Printf("off-topic: the model says %s id=%d %s its challenge", table, id,
		map[string]string{matchYes: "matches", matchNo: "does NOT match", matchUnsure: "may or may not match"}[match])
	if match != matchNo || !standing {
		return
	}

	responseID := 0
	if table == "challenge_responses" {
		responseID = id
	}
	title := jobQuestion("challenges", challengeID)
	body := "Heads up: your video may not match"
	if title != "" {
		body += fmt.Sprintf(" “%s”", title)
	}
	body += ". If somebody reports it, it will be taken down and cost you rating points."
	notifyUser(InboxNote{UserID: ownerID, Kind: noteOffTopicWarning,
		ChallengeID: challengeID, Body: body})

	if _, err := judgeOffTopic(context.Background(), challengeID, responseID); err != nil {
		log.Printf("off-topic: could not judge %s id=%d after the model said no: %v — "+
			"the next report on it judges it again", table, id, err)
	}
}

// ════════════════════════════════════════════════════════════════════════════
// JUDGING
// ════════════════════════════════════════════════════════════════════════════

// judgeOffTopic looks at the evidence against one video and acts on it:
// takes it down, or charges its owner for reports alone. responseID 0 means
// the challenge's own video. Returns whether this call took it down.
func judgeOffTopic(ctx context.Context, challengeID, responseID int) (bool, error) {
	var model string
	var down, paid bool
	var party, viewers int
	var err error
	if responseID == 0 {
		err = db.QueryRowContext(ctx, `
			SELECT c.question_match, c.off_topic_at IS NOT NULL,
			       c.report_penalty_at IS NOT NULL,
			       COUNT(f.user_id) FILTER (WHERE EXISTS (
			           SELECT 1 FROM challenge_responses o
			            WHERE o.challenge_id = c.id AND o.responder_id = f.user_id)),
			       COUNT(f.user_id) FILTER (WHERE NOT EXISTS (
			           SELECT 1 FROM challenge_responses o
			            WHERE o.challenge_id = c.id AND o.responder_id = f.user_id)
			         AND EXISTS (SELECT 1 FROM video_views v
			                      WHERE v.user_id = f.user_id AND v.challenge_id = c.id
			                        AND v.response_id = 0)
			         AND COALESCE(u.created_at, TIMESTAMPTZ 'epoch') < c.created_at)
			  FROM challenges c
			  LEFT JOIN challenge_flags f ON f.challenge_id = c.id AND f.user_id <> c.creator_id
			  LEFT JOIN users u ON u.id = f.user_id
			 WHERE c.id = $1
			 GROUP BY c.id`, challengeID).Scan(&model, &down, &paid, &party, &viewers)
	} else {
		err = db.QueryRowContext(ctx, `
			SELECT cr.question_match, cr.off_topic_at IS NOT NULL,
			       cr.report_penalty_at IS NOT NULL,
			       COUNT(f.user_id) FILTER (WHERE f.user_id = c.creator_id OR EXISTS (
			           SELECT 1 FROM challenge_responses o
			            WHERE o.challenge_id = c.id AND o.responder_id = f.user_id)),
			       COUNT(f.user_id) FILTER (WHERE f.user_id <> c.creator_id AND NOT EXISTS (
			           SELECT 1 FROM challenge_responses o
			            WHERE o.challenge_id = c.id AND o.responder_id = f.user_id)
			         AND EXISTS (SELECT 1 FROM video_views v
			                      WHERE v.user_id = f.user_id AND v.challenge_id = c.id
			                        AND v.response_id = cr.id)
			         AND COALESCE(u.created_at, TIMESTAMPTZ 'epoch') < cr.created_at)
			  FROM challenge_responses cr
			  JOIN challenges c ON c.id = cr.challenge_id
			  LEFT JOIN challenge_response_flags f
			         ON f.response_id = cr.id AND f.user_id <> cr.responder_id
			  LEFT JOIN users u ON u.id = f.user_id
			 WHERE cr.id = $1 AND cr.challenge_id = $2
			 GROUP BY cr.id, c.id`, responseID, challengeID).Scan(&model, &down, &paid, &party, &viewers)
	}
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("weigh the reports on battle %d video %d: %w", challengeID, responseID, err)
	}
	if down {
		return false, nil
	}
	switch offTopicVerdict(model, party, viewers) {
	case offTopicNothing:
		return false, nil
	case offTopicPenalise:
		if paid {
			return false, nil // charged once for reports on this video already
		}
		log.Printf("off-topic: %d viewers reported battle %d video %d — charging its "+
			"owner, the video stays up", viewers, challengeID, responseID)
		charge, ok, err := penaliseForReports(ctx, challengeID, responseID)
		if err != nil || !ok {
			return false, err
		}
		tellReportPenalty(charge)
		return false, nil
	}
	log.Printf("off-topic: taking down battle %d video %d — the model said %q, %d report(s) "+
		"from the battle, %d from viewers", challengeID, responseID, model, party, viewers)
	ruling, ok, err := ruleOffTopic(ctx, challengeID, responseID)
	if err != nil || !ok {
		return false, err
	}
	tellOffTopic(ruling)
	if responseID == 0 {
		go reindexChallengeForSearch(challengeID)
	}
	return true, nil
}

// offTopicRuling is what happened, for telling the people it happened to.
type offTopicRuling struct {
	ChallengeID  int
	Title        string
	IsChallenge  bool
	Offender     int
	OffenderName string
	LostBattle   bool
	RatingLost   int
	Winners      []Standing
}

// ruleOffTopic takes one video down and settles what it costs, in one
// transaction. Safe to call twice: a video already taken down is left alone.
func ruleOffTopic(ctx context.Context, challengeID, responseID int) (offTopicRuling, bool, error) {
	out := offTopicRuling{ChallengeID: challengeID, IsChallenge: responseID == 0}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return out, false, err
	}
	defer func() { _ = tx.Rollback() }()

	// The battle first, always: the resolver locks it first too, so the two
	// can never each hold what the other is waiting for.
	var creatorID int
	var status, prefix, subject string
	var ends, resolved sql.NullTime
	var challengeDown, paid bool
	err = tx.QueryRowContext(ctx, `
		SELECT creator_id, status, COALESCE(prefix, ''), COALESCE(subject, ''),
		       voting_ends_at, resolved_at, off_topic_at IS NOT NULL,
		       report_penalty_at IS NOT NULL
		  FROM challenges WHERE id = $1
		 FOR UPDATE`, challengeID).Scan(
		&creatorID, &status, &prefix, &subject, &ends, &resolved, &challengeDown, &paid)
	if err == sql.ErrNoRows {
		return out, false, nil
	}
	if err != nil {
		return out, false, fmt.Errorf("lock battle %d: %w", challengeID, err)
	}
	out.Title = challengeTitle(prefix, subject)
	out.Offender = creatorID
	if out.IsChallenge {
		if challengeDown {
			return out, false, nil
		}
	} else {
		var answerDown bool
		err = tx.QueryRowContext(ctx, `
			SELECT responder_id, off_topic_at IS NOT NULL,
			       report_penalty_at IS NOT NULL
			  FROM challenge_responses
			 WHERE id = $1 AND challenge_id = $2
			 FOR UPDATE`, responseID, challengeID).Scan(&out.Offender, &answerDown, &paid)
		if err == sql.ErrNoRows || (err == nil && answerDown) {
			return out, false, nil
		}
		if err != nil {
			return out, false, fmt.Errorf("lock answer %d: %w", responseID, err)
		}
	}
	live := status == "active" && ends.Valid && !resolved.Valid

	// The score as it stands, with the video still in it: the owner's side
	// is recorded with the numbers it had, and everybody else's is what they
	// take away if the battle ends here.
	st, ok, err := loadBattleStandings(ctx, tx, challengeID)
	if err != nil || !ok {
		return out, false, err
	}
	offSide := Standing{userID: out.Offender, responseID: responseID, Role: "responder"}
	if out.IsChallenge {
		offSide.Role = "creator"
	}
	var standing []Standing
	for _, s := range st.Participants {
		if s.userID == out.Offender {
			offSide = s
			continue
		}
		standing = append(standing, s)
	}
	out.OffenderName = offSide.Username

	if out.IsChallenge {
		_, err = tx.ExecContext(ctx, `
			UPDATE challenges SET status = 'removed', off_topic_at = NOW()
			 WHERE id = $1`, challengeID)
	} else {
		_, err = tx.ExecContext(ctx, `
			UPDATE challenge_responses SET is_hidden = TRUE, off_topic_at = NOW()
			 WHERE id = $1`, responseID)
	}
	if err != nil {
		return out, false, fmt.Errorf("take down battle %d video %d: %w", challengeID, responseID, err)
	}

	// Everybody whose record may change, locked in id order like the
	// resolver does.
	ids := []int64{int64(out.Offender)}
	for _, s := range standing {
		ids = append(ids, int64(s.userID))
	}
	records := map[int]competitor{}
	rows, err := tx.QueryContext(ctx, `
		SELECT id, rating, wins, losses, draws
		  FROM users WHERE id = ANY($1::int[])
		 ORDER BY id
		 FOR UPDATE`, pq.Array(ids))
	if err != nil {
		return out, false, fmt.Errorf("lock the people in battle %d: %w", challengeID, err)
	}
	for rows.Next() {
		var c competitor
		if err := rows.Scan(&c.UserID, &c.Rating, &c.Wins, &c.Losses, &c.Draws); err != nil {
			_ = rows.Close()
			return out, false, fmt.Errorf("read a person in battle %d: %w", challengeID, err)
		}
		records[c.UserID] = c
	}
	if err := rows.Close(); err != nil {
		return out, false, err
	}

	for id, c := range records {
		if c.Rating == 0 {
			c.Rating = ratingStart
			records[id] = c
		}
	}
	// Reports already charged the penalty and the strike for this video:
	// taking it down costs the battle, not the same penalty twice.
	penalty, strike := integrityPenalty, 1
	if paid {
		penalty, strike = 0, 0
	}
	off := records[out.Offender]
	after := penaltyRating(off.Rating, penalty)
	gains := make([]int, len(standing)) // what winning here is worth, per side
	if live {
		ratings := make([]int, len(standing))
		for i, s := range standing {
			ratings[i] = records[s.userID].Rating
		}
		after, gains = forfeitRatings(off.Rating, ratings, penalty)
		off.Losses++
		out.LostBattle = true
	}
	out.RatingLost = off.Rating - after
	if _, err := tx.ExecContext(ctx, `
		UPDATE users
		   SET rating = $2, losses = $3, league = $4,
		       integrity_strikes = integrity_strikes + $6,
		       last_battle_at = CASE WHEN $5 THEN NOW() ELSE last_battle_at END
		 WHERE id = $1`,
		out.Offender, after, off.Losses,
		leagueFor(after, off.Wins+off.Losses+off.Draws), live, strike); err != nil {
		return out, false, fmt.Errorf("charge %d for battle %d: %w", out.Offender, challengeID, err)
	}
	if live {
		if err := recordForfeit(ctx, tx, challengeID, offSide, "lost", off.Rating, after); err != nil {
			return out, false, err
		}
	}

	// One side left, or the challenge itself gone: the battle is over.
	if live && (out.IsChallenge || len(standing) == 1) {
		for i, s := range standing {
			c := records[s.userID]
			c.Wins++
			won := c.Rating + gains[i]
			if _, err := tx.ExecContext(ctx, `
				UPDATE users
				   SET rating = $2, wins = $3, league = $4, last_battle_at = NOW()
				 WHERE id = $1`,
				s.userID, won, c.Wins, leagueFor(won, c.Wins+c.Losses+c.Draws)); err != nil {
				return out, false, fmt.Errorf("record the win of %d in battle %d: %w", s.userID, challengeID, err)
			}
			if err := recordForfeit(ctx, tx, challengeID, s, "won", c.Rating, won); err != nil {
				return out, false, err
			}
			out.Winners = append(out.Winners, s)
		}
		if out.IsChallenge {
			_, err = tx.ExecContext(ctx, `
				UPDATE challenges SET resolved_at = NOW() WHERE id = $1`, challengeID)
		} else {
			_, err = tx.ExecContext(ctx, `
				UPDATE challenges SET status = 'completed', resolved_at = NOW()
				 WHERE id = $1`, challengeID)
		}
		if err != nil {
			return out, false, fmt.Errorf("close battle %d: %w", challengeID, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return out, false, err
	}
	return out, true, nil
}

// reportCharge is what reports alone cost somebody, for telling them.
type reportCharge struct {
	ChallengeID int
	ResponseID  int
	Title       string
	Owner       int
	RatingLost  int
}

// penaliseForReports charges a video's owner for reports alone: the penalty
// and a strike, once per video. The video stays up and the battle goes on.
// Returns false when this video was already charged, or is already down.
func penaliseForReports(ctx context.Context, challengeID, responseID int) (reportCharge, bool, error) {
	out := reportCharge{ChallengeID: challengeID, ResponseID: responseID}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return out, false, err
	}
	defer func() { _ = tx.Rollback() }()

	// Marking the video first is what makes this once only: of two reports
	// arriving together, only one gets a row back.
	var prefix, subject string
	if responseID == 0 {
		err = tx.QueryRowContext(ctx, `
			UPDATE challenges SET report_penalty_at = NOW()
			 WHERE id = $1 AND report_penalty_at IS NULL AND off_topic_at IS NULL
			RETURNING creator_id, COALESCE(prefix, ''), COALESCE(subject, '')`,
			challengeID).Scan(&out.Owner, &prefix, &subject)
	} else {
		err = tx.QueryRowContext(ctx, `
			UPDATE challenge_responses cr SET report_penalty_at = NOW()
			  FROM challenges c
			 WHERE cr.id = $1 AND cr.challenge_id = $2 AND c.id = cr.challenge_id
			   AND cr.report_penalty_at IS NULL AND cr.off_topic_at IS NULL
			RETURNING cr.responder_id, COALESCE(c.prefix, ''), COALESCE(c.subject, '')`,
			responseID, challengeID).Scan(&out.Owner, &prefix, &subject)
	}
	if err == sql.ErrNoRows {
		return out, false, nil
	}
	if err != nil {
		return out, false, fmt.Errorf("mark battle %d video %d as charged: %w", challengeID, responseID, err)
	}
	out.Title = challengeTitle(prefix, subject)

	var c competitor
	if err := tx.QueryRowContext(ctx, `
		SELECT rating, wins, losses, draws FROM users WHERE id = $1 FOR UPDATE`,
		out.Owner).Scan(&c.Rating, &c.Wins, &c.Losses, &c.Draws); err != nil {
		return out, false, fmt.Errorf("lock the owner of battle %d video %d: %w", challengeID, responseID, err)
	}
	if c.Rating == 0 {
		c.Rating = ratingStart
	}
	after := penaltyRating(c.Rating, integrityPenalty)
	if _, err := tx.ExecContext(ctx, `
		UPDATE users
		   SET rating = $2, league = $3, integrity_strikes = integrity_strikes + 1
		 WHERE id = $1`,
		out.Owner, after, leagueFor(after, c.Wins+c.Losses+c.Draws)); err != nil {
		return out, false, fmt.Errorf("charge %d for battle %d video %d: %w", out.Owner, challengeID, responseID, err)
	}
	if err := tx.Commit(); err != nil {
		return out, false, err
	}
	out.RatingLost = c.Rating - after
	return out, true, nil
}

// tellReportPenalty tells somebody that reports cost them, and that their
// video is still up.
func tellReportPenalty(c reportCharge) {
	quoted := ""
	if c.Title != "" {
		quoted = fmt.Sprintf(" “%s”", c.Title)
	}
	notifyUser(InboxNote{UserID: c.Owner, Kind: noteOffTopicPenalty, ChallengeID: c.ChallengeID,
		Body: fmt.Sprintf("Several people reported your video in%s for not matching the "+
			"challenge. It cost you %d rating points. Your video stays up.", quoted, c.RatingLost)})
	if _, _, err := enqueueNotification(EnqueueParams{
		UserID:      strconv.Itoa(c.Owner),
		TriggerKind: TriggerOffTopic,
		DedupeKey:   fmt.Sprintf("off_topic_penalty:%d:%d:%d", c.ChallengeID, c.ResponseID, c.Owner),
		Title:       "Your video was reported",
		Body:        strings.TrimSpace(c.Title + " — people say it didn't match the challenge"),
		Deeplink:    fmt.Sprintf("devf://challenge/%d", c.ChallengeID),
	}); err != nil {
		log.Printf("off-topic: could not queue the report-penalty push for user %d: %v — "+
			"they still have it in their list", c.Owner, err)
	}
}

// recordForfeit writes one side's result of a battle ended, or lost, by a
// video taken down. flagged marks the side that was taken down.
func recordForfeit(ctx context.Context, tx *sql.Tx, challengeID int, s Standing, outcome string, before, after int) error {
	var rid any
	if s.responseID != 0 {
		rid = s.responseID
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO battle_results
		    (challenge_id, user_id, role, response_id, outcome,
		     votes, raw_votes, removed_votes, likes, views, shares,
		     flagged, rating_before, rating_after)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
		ON CONFLICT (challenge_id, user_id) DO NOTHING`,
		challengeID, s.userID, s.Role, rid, outcome,
		s.Votes, s.RawVotes, s.RemovedVotes, s.Likes, s.Views, s.Shares,
		outcome == "lost", before, after); err != nil {
		return fmt.Errorf("record %s for %d in battle %d: %w", outcome, s.userID, challengeID, err)
	}
	return nil
}

// tellOffTopic tells the owner their video came down and why, and tells
// anybody who won because of it.
func tellOffTopic(r offTopicRuling) {
	quoted := ""
	if r.Title != "" {
		quoted = fmt.Sprintf(" “%s”", r.Title)
	}
	var body string
	if r.IsChallenge {
		body = "Your challenge" + quoted + " was taken down: your video didn't match it."
	} else {
		body = "Your answer to" + quoted + " was taken down: it didn't match the challenge."
	}
	if r.LostBattle {
		body += fmt.Sprintf(" You lost the battle and %d rating points.", r.RatingLost)
	} else {
		body += fmt.Sprintf(" It cost you %d rating points.", r.RatingLost)
	}
	notifyUser(InboxNote{UserID: r.Offender, Kind: noteOffTopic,
		ChallengeID: r.ChallengeID, Body: body})
	if _, _, err := enqueueNotification(EnqueueParams{
		UserID:      strconv.Itoa(r.Offender),
		TriggerKind: TriggerOffTopic,
		DedupeKey:   fmt.Sprintf("off_topic:%d:%d", r.ChallengeID, r.Offender),
		Title:       "Your video was taken down",
		Body:        strings.TrimSpace(r.Title + " — it didn't match the challenge"),
		Deeplink:    fmt.Sprintf("devf://challenge/%d", r.ChallengeID),
	}); err != nil {
		log.Printf("off-topic: could not queue the push for user %d: %v — "+
			"they still have it in their list", r.Offender, err)
	}

	for _, w := range r.Winners {
		note := "You won" + quoted
		if r.OffenderName != "" {
			note += ": " + r.OffenderName + "'s video didn't match the challenge."
		} else {
			note += ": the other video didn't match the challenge."
		}
		notifyUser(InboxNote{UserID: w.userID, Username: w.Username, Kind: noteBattleWon,
			ChallengeID: r.ChallengeID, Body: note})
		if _, _, err := enqueueNotification(EnqueueParams{
			UserID:      strconv.Itoa(w.userID),
			TriggerKind: TriggerBattleWon,
			DedupeKey:   "battle_won:" + strconv.Itoa(r.ChallengeID),
			Title:       "You won your battle 🏆",
			Body:        strings.TrimSpace(r.Title + " — the other video didn't match the challenge"),
			Deeplink:    fmt.Sprintf("devf://challenge/%d", r.ChallengeID),
		}); err != nil {
			log.Printf("off-topic: could not queue the winner's push for user %d: %v — "+
				"they still have it in their list", w.userID, err)
		}
	}
}

// ════════════════════════════════════════════════════════════════════════════
// REPORTING
// ════════════════════════════════════════════════════════════════════════════

// ReportOffTopicHandler records that somebody thinks a video doesn't match
// its challenge, and takes it down if that is now enough.
//
// POST /api/v1/challenges/{id}/report  body: {"responseId": "71"}
// No responseId (or "") reports the challenge's own video.
func ReportOffTopicHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "OPTIONS" {
		w.WriteHeader(http.StatusOK)
		return
	}
	uid, err := strconv.Atoi(authUserID(r))
	if err != nil || uid <= 0 {
		http.Error(w, "sign in to report a video", http.StatusUnauthorized)
		return
	}
	cid, err := strconv.Atoi(mux.Vars(r)["id"])
	if err != nil {
		http.Error(w, "invalid challenge id", http.StatusBadRequest)
		return
	}
	var body struct {
		ResponseID string `json:"responseId"`
	}
	// No body at all is fine: it reports the challenge's own video.
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil && !errors.Is(err, io.EOF) {
		http.Error(w, "invalid body", http.StatusBadRequest)
		return
	}
	rid := 0
	if s := strings.TrimSpace(body.ResponseID); s != "" {
		if rid, err = strconv.Atoi(s); err != nil || rid <= 0 {
			http.Error(w, "invalid response id", http.StatusBadRequest)
			return
		}
	}

	status, msg, down, err := reportOffTopic(r.Context(), uid, cid, rid)
	if err != nil {
		log.Printf("off-topic: report by %d on battle %d video %d failed: %v", uid, cid, rid, err)
		http.Error(w, "could not record the report — try again", http.StatusInternalServerError)
		return
	}
	if status != http.StatusOK {
		http.Error(w, msg, status)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"reported": true, "takenDown": down, "message": msg,
	})
}

// reportOffTopic is the handler without the HTTP: who, which battle, which
// video (0 = the challenge's own). Returns the status to answer with, what to
// tell the person, and whether the video is down now.
func reportOffTopic(ctx context.Context, uid, cid, rid int) (int, string, bool, error) {
	var owner int
	var down bool
	var err error
	if rid == 0 {
		err = db.QueryRowContext(ctx, `
			SELECT creator_id, off_topic_at IS NOT NULL
			  FROM challenges WHERE id = $1`, cid).Scan(&owner, &down)
	} else {
		err = db.QueryRowContext(ctx, `
			SELECT responder_id, off_topic_at IS NOT NULL
			  FROM challenge_responses WHERE id = $1 AND challenge_id = $2`,
			rid, cid).Scan(&owner, &down)
	}
	if err == sql.ErrNoRows {
		return http.StatusNotFound, "That video isn't there any more.", false, nil
	}
	if err != nil {
		return 0, "", false, err
	}
	if owner == uid {
		return http.StatusForbidden, "You can't report your own video.", false, nil
	}
	if down {
		return http.StatusOK, "This video has already been taken down.", true, nil
	}

	if rid == 0 {
		_, err = db.ExecContext(ctx, `
			INSERT INTO challenge_flags (challenge_id, user_id)
			VALUES ($1, $2)
			ON CONFLICT (challenge_id, user_id) DO NOTHING`, cid, uid)
	} else {
		_, err = db.ExecContext(ctx, `
			INSERT INTO challenge_response_flags (response_id, user_id)
			VALUES ($1, $2)
			ON CONFLICT (response_id, user_id) DO NOTHING`, rid, uid)
		if err == nil {
			// The count the answer list has always carried.
			_, err = db.ExecContext(ctx, `
				UPDATE challenge_responses
				   SET off_topic_flags = (SELECT COUNT(*) FROM challenge_response_flags
				                           WHERE response_id = $1)
				 WHERE id = $1`, rid)
		}
	}
	if err != nil {
		return 0, "", false, err
	}

	down, err = judgeOffTopic(ctx, cid, rid)
	if err != nil {
		// The report is kept, and the next report or the model's answer
		// judges again. Not the reporter's problem.
		log.Printf("off-topic: report on battle %d video %d kept, but judging it failed: %v",
			cid, rid, err)
	}
	if down {
		return http.StatusOK, "Thanks. It didn't match the challenge, so it has been taken down.", true, nil
	}
	return http.StatusOK, "Thanks for reporting. If it's confirmed not to match the challenge, it will be taken down.", false, nil
}
