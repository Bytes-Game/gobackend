package main

// moderation.go — what happens to a report.
//
// Anyone signed in can report a video, an answer, a comment or an account
// (HandleReportEvent). Until now a report only lowered the account's
// standing in the feed; nobody ever read one. The app stores need more
// than that from an app where people post: somebody must look at reports
// and act on them, and the law in India gives 24 hours for some kinds and
// 15 days for the rest.
//
// So reports wait on the admin page /admin/reports, oldest first, one line
// per thing reported however many people reported it. For each, the admin
// can:
//
//   - dismiss: nothing wrong. The reports are closed.
//   - remove: the video, answer or comment is deleted for good, the same
//     delete as its owner's.
//   - suspend: the account behind it can no longer sign in, every phone
//     signed in as it is signed out, and it is hidden everywhere, the way
//     an account waiting to be deleted is. Nothing is deleted, so it can be
//     undone ("Unsuspend", same page).
//   - remove and suspend: both.
//
// Every open report on the same thing is closed together, so one decision
// answers all of them.

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/lib/pq"
)

// reportTargets are the things that can be reported.
var reportTargets = map[string]string{
	"challenge": "video",
	"response":  "answer",
	"comment":   "comment",
	"user":      "account",
}

// reportReasons are the reasons the app offers, by code. A reason the
// server does not know is kept as "other", so a newer app never fails here.
var reportReasons = map[string]bool{
	"sexual_content": true, // nudity or sexual content
	"child_safety":   true, // a child at risk
	"violence":       true, // violence, threats or dangerous acts
	"hate":           true, // hate speech
	"harassment":     true, // bullying or harassment
	"self_harm":      true, // suicide or self-harm
	"spam":           true, // spam or scams
	"impersonation":  true, // pretending to be someone
	"ip":             true, // uses somebody's work without permission
	"other":          true,
	// Older names, still accepted.
	"abuse": true, "threats": true, "block": true,
}

// reportNoteMax is the most a reporter may write, in characters.
const reportNoteMax = 1000

// suspendedMessage is what a suspended account is told when it signs in.
const suspendedMessage = "This account is suspended for breaking the Community Guidelines. " +
	"If you think this is a mistake, email " + supportEmail + "."

// moderationActions are what the admin can do with a report, and what each
// is recorded as.
var moderationActions = map[string]string{
	"dismiss":        "dismissed",
	"remove":         "removed",
	"suspend":        "suspended",
	"remove_suspend": "removed_suspended",
}

// reportedOwner is the account behind what was reported: a video's or an
// answer's creator, a comment's author, or the account itself. found is
// false when there is no such thing.
func reportedOwner(targetType string, targetID int) (owner int, found bool, err error) {
	var q string
	switch targetType {
	case "challenge":
		q = `SELECT creator_id FROM challenges WHERE id = $1`
	case "response":
		q = `SELECT responder_id FROM challenge_responses WHERE id = $1`
	case "comment":
		q = `SELECT author_id FROM challenge_comments WHERE id = $1`
	case "user":
		q = `SELECT id FROM users WHERE id = $1`
	default:
		return 0, false, nil
	}
	err = db.QueryRow(q, targetID).Scan(&owner)
	if err == sql.ErrNoRows {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	return owner, true, nil
}

// cleanReport puts a report the app sent into the shape kept: a known
// target, a known reason, and a note of a sane length. It says what is
// wrong with one that cannot be kept.
func cleanReport(p *ReportPayload) (targetID int, problem string) {
	p.TargetType = strings.ToLower(strings.TrimSpace(p.TargetType))
	if _, ok := reportTargets[p.TargetType]; !ok {
		return 0, "targetType must be challenge, response, comment or user"
	}
	id, err := strconv.Atoi(strings.TrimSpace(p.TargetID))
	if err != nil || id <= 0 {
		return 0, "targetId must be a number"
	}
	p.Reason = strings.ToLower(strings.TrimSpace(p.Reason))
	if p.Reason == "" {
		return 0, "reason is required"
	}
	if !reportReasons[p.Reason] {
		p.Reason = "other"
	}
	p.Description = strings.TrimSpace(p.Description)
	if r := []rune(p.Description); len(r) > reportNoteMax {
		p.Description = string(r[:reportNoteMax])
	}
	return id, ""
}

// openReportBy is the id of [reporter]'s report on this thing that is
// still waiting to be read, or 0.
func openReportBy(reporter int, targetType string, targetID int) (int, error) {
	var id int
	err := db.QueryRow(`
		SELECT id FROM reports
		 WHERE reporter_id = $1 AND target_type = $2 AND target_id = $3 AND status = 'pending'
		 LIMIT 1`, reporter, targetType, targetID).Scan(&id)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return id, err
}

// ─── suspension ─────────────────────────────────────────────────────────

// accountSuspended is whether [userID] is suspended. A database that
// cannot answer says no, and says so in the log: refusing would stop
// everybody signing in.
func accountSuspended(userID string) bool {
	var suspended bool
	err := db.QueryRow(`SELECT suspended_at IS NOT NULL FROM users WHERE id::text = $1`, userID).Scan(&suspended)
	if err == sql.ErrNoRows {
		return false
	}
	if queryFailed("whether account "+userID+" is suspended", "letting the sign-in through", err) {
		return false
	}
	return suspended
}

// suspendAccount stops [userID] signing in, signs out every phone it is
// signed in on, and hides it everywhere. Nothing is deleted. It reports
// whether the account was suspended by this call (false: it already was).
func suspendAccount(userID int, reason string) (bool, error) {
	res, err := db.Exec(`
		UPDATE users SET suspended_at = NOW(), suspended_reason = $2
		 WHERE id = $1 AND suspended_at IS NULL`, userID, reason)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	forgetCutoff(strconv.Itoa(userID))
	forgetLeaving()
	return n == 1, nil
}

// unsuspendAccount lets [userID] sign in again and shows it again. Phones
// that were signed in before the suspension stay signed out: their
// sign-ins were taken away, and do not come back.
func unsuspendAccount(userID int) (bool, error) {
	res, err := db.Exec(`
		UPDATE users SET suspended_at = NULL, suspended_reason = '', signed_out_at = NOW()
		 WHERE id = $1 AND suspended_at IS NOT NULL`, userID)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	forgetCutoff(strconv.Itoa(userID))
	forgetLeaving()
	return n == 1, nil
}

// ─── the review list ────────────────────────────────────────────────────

// openReport is one thing waiting for a decision, with every open report
// on it.
type openReport struct {
	TargetType     string    `json:"targetType"`
	TargetID       int       `json:"targetId"`
	OwnerID        int       `json:"ownerId"`
	Owner          string    `json:"owner"`
	OwnerSuspended bool      `json:"ownerSuspended"`
	Reports        int       `json:"reports"`
	Reasons        []string  `json:"reasons"`
	Notes          []string  `json:"notes"`
	FirstAt        time.Time `json:"firstAt"`
	LastAt         time.Time `json:"lastAt"`
	// Gone: the thing was deleted since (by its owner, or already removed).
	Gone bool `json:"gone"`
	// Text is a comment's words, or a video's title.
	Text         string `json:"text"`
	VideoURL     string `json:"videoUrl"`
	ThumbnailURL string `json:"thumbnailUrl"`
	// ChallengeID is the video an answer or comment belongs to.
	ChallengeID int `json:"challengeId"`
}

// openReportsLimit is the most lines the review list shows at once.
const openReportsLimit = 200

// listOpenReports is everything waiting for a decision, the longest
// waiting first.
func listOpenReports() ([]openReport, error) {
	rows, err := db.Query(`
		WITH open AS (
			SELECT target_type, target_id,
			       MAX(owner_id)   AS owner_id,
			       COUNT(*)        AS n,
			       MIN(created_at) AS first_at,
			       MAX(created_at) AS last_at,
			       ARRAY_AGG(DISTINCT reason) AS reasons,
			       ARRAY_REMOVE(ARRAY_AGG(NULLIF(description, '') ORDER BY created_at), NULL) AS notes
			  FROM reports
			 WHERE status = 'pending'
			 GROUP BY target_type, target_id
		)
		SELECT o.target_type, o.target_id, COALESCE(o.owner_id, 0), o.n, o.first_at, o.last_at,
		       o.reasons, o.notes,
		       COALESCE(u.username, ''), COALESCE(u.suspended_at IS NOT NULL, FALSE),
		       CASE o.target_type
		            WHEN 'challenge' THEN c.id IS NULL
		            WHEN 'response'  THEN cr.id IS NULL
		            WHEN 'comment'   THEN cc.id IS NULL
		            ELSE u.id IS NULL END,
		       COALESCE(cc.text, NULLIF(TRIM(c.prefix || ' ' || c.subject), ''), ''),
		       COALESCE(c.video_url, cr.video_url, ''),
		       COALESCE(c.thumbnail_url, cr.thumbnail_url, ''),
		       COALESCE(cr.challenge_id, cc.challenge_id, 0)
		  FROM open o
		  LEFT JOIN users u                ON u.id = o.owner_id
		  LEFT JOIN challenges c           ON o.target_type = 'challenge' AND c.id = o.target_id
		  LEFT JOIN challenge_responses cr ON o.target_type = 'response'  AND cr.id = o.target_id
		  LEFT JOIN challenge_comments cc  ON o.target_type = 'comment'   AND cc.id = o.target_id
		 ORDER BY o.first_at
		 LIMIT $1`, openReportsLimit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []openReport{}
	for rows.Next() {
		var o openReport
		var reasons, notes pq.StringArray
		if err := rows.Scan(&o.TargetType, &o.TargetID, &o.OwnerID, &o.Reports, &o.FirstAt, &o.LastAt,
			&reasons, &notes, &o.Owner, &o.OwnerSuspended, &o.Gone,
			&o.Text, &o.VideoURL, &o.ThumbnailURL, &o.ChallengeID); err != nil {
			return nil, err
		}
		o.Reasons, o.Notes = []string(reasons), []string(notes)
		if o.Notes == nil {
			o.Notes = []string{}
		}
		out = append(out, o)
	}
	return out, rows.Err()
}

// suspendedAccount is one suspended account, for the list that undoes it.
type suspendedAccount struct {
	ID          int       `json:"id"`
	Username    string    `json:"username"`
	SuspendedAt time.Time `json:"suspendedAt"`
	Reason      string    `json:"reason"`
}

func listSuspended() ([]suspendedAccount, error) {
	rows, err := db.Query(`
		SELECT id, username, suspended_at, suspended_reason
		  FROM users WHERE suspended_at IS NOT NULL
		 ORDER BY suspended_at DESC LIMIT 500`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []suspendedAccount{}
	for rows.Next() {
		var s suspendedAccount
		if err := rows.Scan(&s.ID, &s.Username, &s.SuspendedAt, &s.Reason); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// AdminOpenReportsHandler — GET /api/v1/admin/reports: what is waiting for
// a decision, and who is suspended.
func AdminOpenReportsHandler(w http.ResponseWriter, r *http.Request) {
	open, err := listOpenReports()
	if err != nil {
		log.Printf("admin: the review list could not be read: %v", err)
		http.Error(w, "the review list could not be read", http.StatusInternalServerError)
		return
	}
	suspended, err := listSuspended()
	if err != nil {
		log.Printf("admin: the suspended accounts could not be read: %v", err)
		http.Error(w, "the suspended accounts could not be read", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"open": open, "suspended": suspended})
}

type resolveRequest struct {
	TargetType string `json:"targetType"`
	TargetID   int    `json:"targetId"`
	Action     string `json:"action"`
	// Note is the admin's own words, kept with a suspension.
	Note string `json:"note"`
	// Confirm must be "yes" for anything but dismiss: a retried script or
	// a wrong address does nothing without it.
	Confirm string `json:"confirm"`
}

// AdminResolveReportHandler — POST /api/v1/admin/reports/resolve: the
// admin's decision on one thing, which closes every open report on it.
func AdminResolveReportHandler(w http.ResponseWriter, r *http.Request) {
	var req resolveRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "the request could not be read", http.StatusBadRequest)
		return
	}
	resolution, ok := moderationActions[req.Action]
	if !ok {
		http.Error(w, "action must be dismiss, remove, suspend or remove_suspend", http.StatusBadRequest)
		return
	}
	if _, ok := reportTargets[req.TargetType]; !ok || req.TargetID <= 0 {
		http.Error(w, "targetType and targetId name nothing that can be reported", http.StatusBadRequest)
		return
	}
	if req.Action != "dismiss" && req.Confirm != "yes" {
		http.Error(w, `this cannot be undone from here without "confirm": "yes"`, http.StatusBadRequest)
		return
	}
	remove := req.Action == "remove" || req.Action == "remove_suspend"
	suspend := req.Action == "suspend" || req.Action == "remove_suspend"
	if remove && req.TargetType == "user" {
		http.Error(w, "an account is suspended, not removed", http.StatusBadRequest)
		return
	}

	var owner int
	if err := db.QueryRow(`
		SELECT COALESCE(MAX(owner_id), 0) FROM reports
		 WHERE target_type = $1 AND target_id = $2`, req.TargetType, req.TargetID).Scan(&owner); err != nil {
		log.Printf("admin: whose %s %d is could not be read: %v", req.TargetType, req.TargetID, err)
		http.Error(w, "temporarily unavailable", http.StatusInternalServerError)
		return
	}
	if req.TargetType == "user" {
		owner = req.TargetID
	}

	out := map[string]any{"resolution": resolution}
	if remove {
		gone, err := removeReported(req.TargetType, req.TargetID)
		if err != nil {
			log.Printf("admin: %s %d could not be removed: %v", req.TargetType, req.TargetID, err)
			http.Error(w, "it could not be removed: "+err.Error(), http.StatusInternalServerError)
			return
		}
		out["removed"] = gone
	}
	if suspend {
		if owner <= 0 {
			http.Error(w, "nobody is known to be behind it, so there is nobody to suspend", http.StatusConflict)
			return
		}
		note := strings.TrimSpace(req.Note)
		if note == "" {
			note = "reported " + reportTargets[req.TargetType] + " " + strconv.Itoa(req.TargetID)
		}
		changed, err := suspendAccount(owner, note)
		if err != nil {
			log.Printf("admin: account %d could not be suspended: %v", owner, err)
			http.Error(w, "the account could not be suspended", http.StatusInternalServerError)
			return
		}
		out["suspended"] = owner
		out["alreadySuspended"] = !changed
	}

	res, err := db.Exec(`
		UPDATE reports SET status = 'resolved', resolution = $3, resolved_at = NOW()
		 WHERE target_type = $1 AND target_id = $2 AND status = 'pending'`,
		req.TargetType, req.TargetID, resolution)
	if err != nil {
		log.Printf("admin: the reports on %s %d could not be closed: %v", req.TargetType, req.TargetID, err)
		http.Error(w, "the decision was carried out, but the reports could not be closed", http.StatusInternalServerError)
		return
	}
	closed, _ := res.RowsAffected()
	out["closed"] = closed
	log.Printf("admin: %s %d %s (%d reports closed)", req.TargetType, req.TargetID, resolution, closed)
	writeJSON(w, http.StatusOK, out)
}

// removeReported deletes a reported video, answer or comment for good. It
// reports false when it was already gone.
func removeReported(targetType string, targetID int) (bool, error) {
	var err error
	switch targetType {
	case "challenge":
		err = DeleteChallengeByID(strconv.Itoa(targetID))
	case "response":
		err = DeleteResponseByID(strconv.Itoa(targetID))
	case "comment":
		var res sql.Result
		res, err = db.Exec(`DELETE FROM challenge_comments WHERE id = $1`, targetID)
		if err == nil {
			if n, nerr := res.RowsAffected(); nerr == nil && n == 0 {
				return false, nil
			}
		}
	default:
		return false, fmt.Errorf("a %s cannot be removed", targetType)
	}
	if errors.Is(err, errChallengeNotFound) {
		return false, nil
	}
	return err == nil, err
}

// AdminUnsuspendHandler — POST /api/v1/admin/unsuspend {userId}.
func AdminUnsuspendHandler(w http.ResponseWriter, r *http.Request) {
	var req struct {
		UserID int `json:"userId"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID <= 0 {
		http.Error(w, "userId is required", http.StatusBadRequest)
		return
	}
	changed, err := unsuspendAccount(req.UserID)
	if err != nil {
		log.Printf("admin: account %d could not be unsuspended: %v", req.UserID, err)
		http.Error(w, "the account could not be unsuspended", http.StatusInternalServerError)
		return
	}
	if !changed {
		http.Error(w, "that account is not suspended", http.StatusNotFound)
		return
	}
	log.Printf("admin: account %d unsuspended", req.UserID)
	writeJSON(w, http.StatusOK, map[string]any{"unsuspended": req.UserID})
}
