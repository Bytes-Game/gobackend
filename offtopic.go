package main

// offtopic.go — a video that doesn't match its challenge costs the person who
// posted it. Nothing is ever taken down.
//
// ════════════════════════════════════════════════════════════════════════════
// WHY THIS EXISTS
// ════════════════════════════════════════════════════════════════════════════
//
// Anybody could answer "Who can juggle five balls?" with a cat video and
// nothing happened. It went the other way too: a challenge could ask one
// thing and show another.
//
// ════════════════════════════════════════════════════════════════════════════
// WHAT COUNTS AS EVIDENCE
// ════════════════════════════════════════════════════════════════════════════
//
//	THE MODEL — the worker that prepares every upload also asks its model
//	            "does this video match <the challenge>?" and gets back yes,
//	            no or unsure. It goes by what is said and written in the
//	            video, or by still frames when nothing is said. See
//	            cmd/hls-worker/understand.go.
//
//	PEOPLE    — anybody can report a video that doesn't match: somebody in
//	            the battle, or a viewer.
//
// A report counts only from somebody who can fairly make it: somebody else in
// the battle, or a viewer who actually watched that video and whose account
// is older than it. Without that, three accounts made in the same minute
// could charge anybody.
//
// ════════════════════════════════════════════════════════════════════════════
// WHAT IT COSTS
// ════════════════════════════════════════════════════════════════════════════
//
// The owner asked for this: with so few videos on the app, NOTHING IS TAKEN
// DOWN. The video stays up and its battle goes on. Its owner pays instead:
//
//   - offTopicReportsAlone viewers with no part in the battle reported it:
//     penaltyReported rating points and a strike.
//   - The model said "no" AND somebody reported it: penaltyConfirmed, the
//     higher charge. When the smaller one was paid already, only the
//     difference is charged — the most one video ever costs is
//     penaltyConfirmed, and it is one strike.
//   - The model on its own charges nothing. It is small and can be wrong.
//     It warns the owner, so they know before anybody reports it.
//
// penalty_level on the video is what makes each charge happen once.
//
// ════════════════════════════════════════════════════════════════════════════
// REPORTING TO HURT AN OPPONENT
// ════════════════════════════════════════════════════════════════════════════
//
// Somebody in the battle has a reason to report the other side whatever the
// video shows. So:
//
//   - Their report never counts toward the viewers' threshold, and on its
//     own it does nothing. It only counts when the model agrees.
//   - When the model says the video DOES match, everybody in the battle who
//     reported it pays falseReportPenalty, once per video. The app tells them
//     so before they report.

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/gorilla/mux"
)

const (
	// The model's three answers, as the worker sends them.
	matchYes    = "yes"
	matchNo     = "no"
	matchUnsure = "unsure"

	// How many viewers with no part in the battle it takes to charge a
	// video's owner with no word from the model.
	offTopicReportsAlone = 3

	// What a video that doesn't match costs its owner, in total.
	penaltyReported  = integrityPenalty     // reports alone
	penaltyConfirmed = 2 * integrityPenalty // the model and a report agree

	// What reporting the other side of your own battle costs, when the model
	// finds the video does match.
	falseReportPenalty = 10

	// Kinds of notification, as the app reads them.
	noteOffTopicWarning = "off_topic_warning"
	noteOffTopicPenalty = "off_topic_penalty"
	noteFalseReport     = "false_report"
)

// How far a video's owner has been charged, as penalty_level stores it.
const (
	levelNone      = 0
	levelReported  = 1
	levelConfirmed = 2
)

// TriggerOffTopic is the phone push for "your video was reported". Sent under
// the battle-updates setting, with "you won your battle".
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

// offTopicVerdict is the level the evidence against a video supports.
// partyReports are from somebody else in the battle; viewerReports from
// viewers with no part in it. Both count only reports that can fairly be
// made — see the top of this file.
func offTopicVerdict(model string, partyReports, viewerReports int) int {
	if model == matchNo && partyReports+viewerReports >= 1 {
		return levelConfirmed
	}
	if viewerReports >= offTopicReportsAlone {
		return levelReported
	}
	return levelNone
}

// pointsFor is what a video at [level] has cost its owner in total.
func pointsFor(level int) int {
	switch level {
	case levelReported:
		return penaltyReported
	case levelConfirmed:
		return penaltyConfirmed
	}
	return 0
}

// penaltyRating is a rating after [penalty] points are taken off it, never
// below the floor.
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

// settleQuestionMatch writes down what the model said, and acts on it.
//
// Only a real answer is written. A reading with none — an older worker, or a
// model that could not tell — leaves the last one where it was.
//
// A new "no" warns the owner, and a new "yes" or "no" judges the video
// again: somebody may have reported it before the model had a say.
func settleQuestionMatch(table string, id int, raw string) {
	match := cleanMatch(raw)
	if db == nil || match == "" {
		return
	}
	var challengeID, ownerID int
	var err error
	if table == "challenge_responses" {
		err = db.QueryRow(`
			UPDATE challenge_responses SET question_match = $2
			 WHERE id = $1 AND question_match <> $2
			RETURNING challenge_id, responder_id`,
			id, match).Scan(&challengeID, &ownerID)
	} else {
		err = db.QueryRow(`
			UPDATE challenges SET question_match = $2
			 WHERE id = $1 AND question_match <> $2
			RETURNING id, creator_id`,
			id, match).Scan(&challengeID, &ownerID)
	}
	if err == sql.ErrNoRows {
		return // the same answer as last time: already acted on
	}
	if queryFailed(fmt.Sprintf("settleQuestionMatch: saving the model's answer for %s id=%d", table, id),
		"reports on this video are judged without the model's word", err) {
		return
	}
	log.Printf("off-topic: the model says %s id=%d %s its challenge", table, id,
		map[string]string{matchYes: "matches", matchNo: "does NOT match", matchUnsure: "may or may not match"}[match])

	responseID := 0
	if table == "challenge_responses" {
		responseID = id
	}
	if match == matchNo {
		title := jobQuestion("challenges", challengeID)
		body := "Heads up: our check thinks your video may not match"
		if title != "" {
			body += fmt.Sprintf(" “%s”", title)
		}
		body += fmt.Sprintf(". If somebody reports it, it will cost you %d rating points. "+
			"Your video stays up either way.", penaltyConfirmed)
		notifyUser(InboxNote{UserID: ownerID, Kind: noteOffTopicWarning,
			ChallengeID: challengeID, Body: body})
	}
	if match == matchNo || match == matchYes {
		if _, err := judgeOffTopic(context.Background(), challengeID, responseID); err != nil {
			log.Printf("off-topic: could not judge %s id=%d after the model said %s: %v — "+
				"the next report on it judges it again", table, id, match, err)
		}
	}
}

// ════════════════════════════════════════════════════════════════════════════
// JUDGING
// ════════════════════════════════════════════════════════════════════════════

// judgement is what one look at a video's evidence did.
type judgement struct {
	Charged        bool  // its owner was charged, or charged more
	FalseReporters []int // people in the battle charged for a false report
}

// judgeOffTopic looks at the evidence about one video and charges whoever it
// says should pay: the owner, when the video doesn't match; people in the
// battle who reported it, when it does. responseID 0 means the challenge's
// own video. The video itself is never touched.
func judgeOffTopic(ctx context.Context, challengeID, responseID int) (judgement, error) {
	var j judgement
	var model string
	var level, party, viewers int
	var err error
	if responseID == 0 {
		err = db.QueryRowContext(ctx, `
			SELECT c.question_match, c.penalty_level,
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
			 GROUP BY c.id`, challengeID).Scan(&model, &level, &party, &viewers)
	} else {
		err = db.QueryRowContext(ctx, `
			SELECT cr.question_match, cr.penalty_level,
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
			 GROUP BY cr.id, c.id`, responseID, challengeID).Scan(&model, &level, &party, &viewers)
	}
	if err == sql.ErrNoRows {
		return j, nil
	}
	if err != nil {
		return j, fmt.Errorf("weigh the reports on battle %d video %d: %w", challengeID, responseID, err)
	}

	if target := offTopicVerdict(model, party, viewers); target > level {
		log.Printf("off-topic: charging the owner of battle %d video %d to level %d — the model "+
			"said %q, %d report(s) from the battle, %d from viewers; the video stays up",
			challengeID, responseID, target, model, party, viewers)
		charge, ok, err := chargeOwner(ctx, challengeID, responseID, target)
		if err != nil {
			return j, err
		}
		if ok {
			tellOwnerCharge(charge)
			j.Charged = true
		}
	}
	if model == matchYes && party > 0 {
		charged, err := chargeFalseReports(ctx, challengeID, responseID)
		if err != nil {
			return j, err
		}
		tellFalseReports(charged)
		for _, c := range charged {
			j.FalseReporters = append(j.FalseReporters, c.User)
		}
	}
	return j, nil
}

// ownerCharge is what a video cost its owner, for telling them.
type ownerCharge struct {
	ChallengeID int
	ResponseID  int
	Title       string
	Owner       int
	Level       int
	RatingLost  int
}

// chargeOwner raises a video to [target] and charges its owner the
// difference: the points between the level it was at and this one, and a
// strike the first time. Returns false when it was already that far.
func chargeOwner(ctx context.Context, challengeID, responseID, target int) (ownerCharge, bool, error) {
	out := ownerCharge{ChallengeID: challengeID, ResponseID: responseID, Level: target}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return out, false, err
	}
	defer func() { _ = tx.Rollback() }()

	// The video first, locked, so two reports arriving together charge once:
	// the second waits here and then finds the level already raised.
	var level int
	var prefix, subject string
	if responseID == 0 {
		err = tx.QueryRowContext(ctx, `
			SELECT creator_id, penalty_level, COALESCE(prefix, ''), COALESCE(subject, '')
			  FROM challenges WHERE id = $1
			 FOR UPDATE`, challengeID).Scan(&out.Owner, &level, &prefix, &subject)
	} else {
		err = tx.QueryRowContext(ctx, `
			SELECT cr.responder_id, cr.penalty_level, COALESCE(c.prefix, ''), COALESCE(c.subject, '')
			  FROM challenge_responses cr
			  JOIN challenges c ON c.id = cr.challenge_id
			 WHERE cr.id = $1 AND cr.challenge_id = $2
			 FOR UPDATE OF cr`, responseID, challengeID).Scan(&out.Owner, &level, &prefix, &subject)
	}
	if err == sql.ErrNoRows {
		return out, false, nil
	}
	if err != nil {
		return out, false, fmt.Errorf("lock battle %d video %d: %w", challengeID, responseID, err)
	}
	if level >= target {
		return out, false, nil
	}
	out.Title = challengeTitle(prefix, subject)
	if responseID == 0 {
		_, err = tx.ExecContext(ctx, `
			UPDATE challenges SET penalty_level = $2 WHERE id = $1`, challengeID, target)
	} else {
		_, err = tx.ExecContext(ctx, `
			UPDATE challenge_responses SET penalty_level = $2 WHERE id = $1`, responseID, target)
	}
	if err != nil {
		return out, false, fmt.Errorf("mark battle %d video %d as charged: %w", challengeID, responseID, err)
	}

	var c competitor
	if err := tx.QueryRowContext(ctx, `
		SELECT rating, wins, losses, draws FROM users WHERE id = $1 FOR UPDATE`,
		out.Owner).Scan(&c.Rating, &c.Wins, &c.Losses, &c.Draws); err != nil {
		return out, false, fmt.Errorf("lock the owner of battle %d video %d: %w", challengeID, responseID, err)
	}
	if c.Rating == 0 {
		c.Rating = ratingStart
	}
	strike := 0
	if level == levelNone {
		strike = 1
	}
	after := penaltyRating(c.Rating, pointsFor(target)-pointsFor(level))
	if _, err := tx.ExecContext(ctx, `
		UPDATE users
		   SET rating = $2, league = $3, integrity_strikes = integrity_strikes + $4
		 WHERE id = $1`,
		out.Owner, after, leagueFor(after, c.Wins+c.Losses+c.Draws), strike); err != nil {
		return out, false, fmt.Errorf("charge %d for battle %d video %d: %w", out.Owner, challengeID, responseID, err)
	}
	if err := tx.Commit(); err != nil {
		return out, false, err
	}
	out.RatingLost = c.Rating - after
	return out, true, nil
}

// tellOwnerCharge tells somebody what their video cost them, and that it is
// still up.
func tellOwnerCharge(c ownerCharge) {
	quoted := ""
	if c.Title != "" {
		quoted = fmt.Sprintf(" “%s”", c.Title)
	}
	why := fmt.Sprintf("Several people reported your video in%s for not matching the challenge.", quoted)
	if c.Level == levelConfirmed {
		why = fmt.Sprintf("Our check and a report both say your video in%s doesn't match the challenge.", quoted)
	}
	notifyUser(InboxNote{UserID: c.Owner, Kind: noteOffTopicPenalty, ChallengeID: c.ChallengeID,
		Body: fmt.Sprintf("%s It cost you %d rating points. Your video stays up.", why, c.RatingLost)})
	if _, _, err := enqueueNotification(EnqueueParams{
		UserID:      strconv.Itoa(c.Owner),
		TriggerKind: TriggerOffTopic,
		DedupeKey:   fmt.Sprintf("off_topic_penalty:%d:%d:%d:%d", c.ChallengeID, c.ResponseID, c.Owner, c.Level),
		Title:       "Your video was reported",
		Body:        strings.TrimSpace(c.Title + " — it didn't match the challenge"),
		Deeplink:    fmt.Sprintf("devf://challenge/%d", c.ChallengeID),
	}); err != nil {
		log.Printf("off-topic: could not queue the penalty push for user %d: %v — "+
			"they still have it in their list", c.Owner, err)
	}
}

// falseReport is what reporting a video that matches cost somebody.
type falseReport struct {
	ChallengeID int
	Title       string
	User        int
	RatingLost  int
}

// chargeFalseReports charges everybody in the battle who reported a video
// the model says DOES match — once each per video.
func chargeFalseReports(ctx context.Context, challengeID, responseID int) ([]falseReport, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	// Marked as charged in the same statement that finds them, so a report
	// is never charged twice, however often the video is judged.
	var rows *sql.Rows
	if responseID == 0 {
		rows, err = tx.QueryContext(ctx, `
			UPDATE challenge_flags f SET charged_at = NOW()
			  FROM challenges c
			 WHERE f.challenge_id = $1 AND c.id = f.challenge_id
			   AND f.charged_at IS NULL AND c.question_match = 'yes'
			   AND EXISTS (SELECT 1 FROM challenge_responses o
			                WHERE o.challenge_id = c.id AND o.responder_id = f.user_id)
			RETURNING f.user_id, COALESCE(c.prefix, ''), COALESCE(c.subject, '')`, challengeID)
	} else {
		rows, err = tx.QueryContext(ctx, `
			UPDATE challenge_response_flags f SET charged_at = NOW()
			  FROM challenge_responses cr
			  JOIN challenges c ON c.id = cr.challenge_id
			 WHERE f.response_id = $1 AND cr.id = f.response_id AND cr.challenge_id = $2
			   AND f.charged_at IS NULL AND cr.question_match = 'yes'
			   AND (f.user_id = c.creator_id
			        OR EXISTS (SELECT 1 FROM challenge_responses o
			                    WHERE o.challenge_id = c.id AND o.responder_id = f.user_id
			                      AND o.id <> cr.id))
			RETURNING f.user_id, COALESCE(c.prefix, ''), COALESCE(c.subject, '')`,
			responseID, challengeID)
	}
	if err != nil {
		return nil, fmt.Errorf("find false reports on battle %d video %d: %w", challengeID, responseID, err)
	}
	var out []falseReport
	seen := 0
	for rows.Next() {
		var r falseReport
		var prefix, subject string
		if scanFailed("false reports", rows.Scan(&r.User, &prefix, &subject), &seen) {
			continue
		}
		r.ChallengeID, r.Title = challengeID, challengeTitle(prefix, subject)
		out = append(out, r)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}

	// People locked in id order, the same order everything else here uses.
	sort.Slice(out, func(a, b int) bool { return out[a].User < out[b].User })
	for i := range out {
		var c competitor
		if err := tx.QueryRowContext(ctx, `
			SELECT rating, wins, losses, draws FROM users WHERE id = $1 FOR UPDATE`,
			out[i].User).Scan(&c.Rating, &c.Wins, &c.Losses, &c.Draws); err != nil {
			return nil, fmt.Errorf("lock reporter %d: %w", out[i].User, err)
		}
		if c.Rating == 0 {
			c.Rating = ratingStart
		}
		after := penaltyRating(c.Rating, falseReportPenalty)
		if _, err := tx.ExecContext(ctx, `
			UPDATE users SET rating = $2, league = $3 WHERE id = $1`,
			out[i].User, after, leagueFor(after, c.Wins+c.Losses+c.Draws)); err != nil {
			return nil, fmt.Errorf("charge reporter %d: %w", out[i].User, err)
		}
		out[i].RatingLost = c.Rating - after
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return out, nil
}

// tellFalseReports tells each person charged for a false report why.
func tellFalseReports(charged []falseReport) {
	for _, r := range charged {
		quoted := ""
		if r.Title != "" {
			quoted = fmt.Sprintf(" “%s”", r.Title)
		}
		notifyUser(InboxNote{UserID: r.User, Kind: noteFalseReport, ChallengeID: r.ChallengeID,
			Body: fmt.Sprintf("You reported the other side's video in%s, but our check found it "+
				"does match the challenge. A false report cost you %d rating points.", quoted, r.RatingLost)})
	}
}

// ════════════════════════════════════════════════════════════════════════════
// REPORTING
// ════════════════════════════════════════════════════════════════════════════

// ReportOffTopicHandler records that somebody thinks a video doesn't match
// its challenge, and charges whoever the evidence now says should pay.
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

	status, msg, err := reportOffTopic(r.Context(), uid, cid, rid)
	if err != nil {
		log.Printf("off-topic: report by %d on battle %d video %d failed: %v", uid, cid, rid, err)
		http.Error(w, "could not record the report — try again", http.StatusInternalServerError)
		return
	}
	if status != http.StatusOK {
		http.Error(w, msg, status)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"reported": true, "message": msg})
}

// reportOffTopic is the handler without the HTTP: who, which battle, which
// video (0 = the challenge's own). Returns the status to answer with and
// what to tell the person.
func reportOffTopic(ctx context.Context, uid, cid, rid int) (int, string, error) {
	var owner int
	var err error
	if rid == 0 {
		err = db.QueryRowContext(ctx, `
			SELECT creator_id FROM challenges WHERE id = $1`, cid).Scan(&owner)
	} else {
		err = db.QueryRowContext(ctx, `
			SELECT responder_id FROM challenge_responses
			 WHERE id = $1 AND challenge_id = $2`, rid, cid).Scan(&owner)
	}
	if err == sql.ErrNoRows {
		return http.StatusNotFound, "That video isn't there any more.", nil
	}
	if err != nil {
		return 0, "", err
	}
	if owner == uid {
		return http.StatusForbidden, "You can't report your own video.", nil
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
		return 0, "", err
	}

	j, err := judgeOffTopic(ctx, cid, rid)
	if err != nil {
		// The report is kept, and the next report or the model's answer
		// judges again. Not the reporter's problem.
		log.Printf("off-topic: report on battle %d video %d kept, but judging it failed: %v",
			cid, rid, err)
	}
	for _, u := range j.FalseReporters {
		if u == uid {
			return http.StatusOK, fmt.Sprintf("Our check found this video does match the "+
				"challenge, so this report cost you %d rating points.", falseReportPenalty), nil
		}
	}
	return http.StatusOK, "Thanks for reporting.", nil
}
