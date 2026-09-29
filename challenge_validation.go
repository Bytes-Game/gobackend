package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
)

// ════════════════════════════════════════════════════════════════════════════════
// CHALLENGE RESPONSE VALIDATION + COMMUNITY MODERATION
// ════════════════════════════════════════════════════════════════════════════════
//
// Two tiers of validation, both designed to scale to millions of users without
// per-upload AI inference costs:
//
//   Tier 1 (structural, cheap, fires on every upload):
//     - Duration bounds (2s - 180s)
//     - Same user can't reuse the same video URL across challenges
//     - One response per user per challenge
//     - Challenge must still be open/active (not closed)
//     - Rate limit: max 5 responses per user per hour (Redis counter)
//
//   Tier 2 (does the video match its challenge):
//     - The worker's model is asked, and people can report a video
//     - When they agree, the owner loses rating points — see offtopic.go
//     - Lightweight keyword-overlap relevance score computed at upload time
//     - Per-user repeat-offender tracking via off_topic_rate
//
// At scale, tier-3 (vision/audio AI) only runs on community-flagged content,
// not on every upload — that's how we keep per-upload cost ~ zero.

const (
	// Tier-1 duration bounds
	minVideoDurationMs = 2000 // < 2s = empty/glitched upload

	// maxVideoDurationMs is the same three-minute ceiling the upload gate
	// enforces, in the units this file works in. Derived from it, never
	// typed again: this limit used to live only here, so it covered battle
	// answers and not the challenges they answer, and a ten-minute challenge
	// walked in through a door nobody was watching. See maxUploadDuration.
	maxVideoDurationMs = int(maxUploadDuration / time.Millisecond)

	// Tier-1 rate limit (Redis bucket: responses:rate:{userID}, EX 3600)
	maxResponsesPerHour = 5

	// Somebody with more than this share of their answers hidden can't
	// post new ones. See userOffTopicRate.
	offTopicUserCutoff = 0.4
)

// ════════════════════════════════════════════════════════════════════════════════
// TIER 1 — STRUCTURAL VALIDATION
// ════════════════════════════════════════════════════════════════════════════════

// validateChallengeResponseSubmission runs all tier-1 checks on a new response.
// Returns nil on success or a user-facing error on failure.
func validateChallengeResponseSubmission(payload AcceptChallengePayload, challenge Challenge) error {
	// --- Duration bounds ---
	if payload.DurationMs < minVideoDurationMs {
		return fmt.Errorf("video too short — minimum %d seconds", minVideoDurationMs/1000)
	}
	if payload.DurationMs > maxVideoDurationMs {
		return fmt.Errorf("video too long — maximum %d seconds", maxVideoDurationMs/1000)
	}

	// --- Video URL must not be empty ---
	if strings.TrimSpace(payload.VideoURL) == "" {
		return fmt.Errorf("video URL is required")
	}

	// --- Challenge must still accept responses ---
	if challenge.Status == "closed" || challenge.Status == "expired" {
		return fmt.Errorf("challenge is no longer accepting responses")
	}
	if challenge.Status == "removed" {
		return fmt.Errorf("this challenge has been removed")
	}

	// --- Same user can't reuse the same video on any challenge ---
	rid, err := strconv.Atoi(payload.ResponderID)
	if err != nil {
		return fmt.Errorf("invalid responder ID")
	}
	cid, err := strconv.Atoi(payload.ChallengeID)
	if err != nil {
		return fmt.Errorf("invalid challenge ID")
	}
	var dupExists bool
	if err := db.QueryRow(
		`SELECT EXISTS(SELECT 1 FROM challenge_responses WHERE responder_id=$1 AND video_url=$2)`,
		rid, payload.VideoURL,
	).Scan(&dupExists); err != nil {
		queryFailed("validateChallengeResponseSubmission: could not check whether "+
			"this video has already been used",
			"letting the upload through, so the same video can answer twice", err)
	}
	if dupExists {
		return fmt.Errorf("you have already used this video for another challenge — record a new one")
	}

	// --- One response per user per challenge ---
	var alreadyResponded bool
	if err := db.QueryRow(
		`SELECT EXISTS(SELECT 1 FROM challenge_responses WHERE responder_id=$1 AND challenge_id=$2)`,
		rid, cid,
	).Scan(&alreadyResponded); err != nil {
		queryFailed("validateChallengeResponseSubmission: could not check whether "+
			"this person has already answered",
			"letting the upload through, so one person can answer twice", err)
	}
	if alreadyResponded {
		return fmt.Errorf("you have already responded to this challenge")
	}

	// --- Repeat-offender gate ---
	// If >40% of this user's past responses have been community-hidden as
	// off-topic, reject further submissions outright until a human reviews.
	// Protects the challenge feed from well-tested bad actors without
	// needing an explicit ban list.
	if rate := userOffTopicRate(payload.ResponderID); rate > offTopicUserCutoff {
		return fmt.Errorf("too many of your past responses were flagged off-topic — contact support")
	}

	// --- Per-user rate limit (Redis sliding-hour counter) ---
	if err := enforceResponseRateLimit(payload.ResponderID); err != nil {
		return err
	}

	return nil
}

// enforceResponseRateLimit increments a Redis counter and rejects when over the
// per-hour limit. Counter auto-expires after 1 hour, so this is a simple
// fixed-window limiter — good enough for anti-spam without sliding-window cost.
func enforceResponseRateLimit(userID string) error {
	if rdb == nil {
		// Redis not available — fail open rather than block uploads
		return nil
	}
	key := "responses:rate:" + userID
	count, err := rdb.Incr(rctx, key).Result()
	if err != nil {
		return nil // Don't block on Redis failure
	}
	// First increment: set the 1-hour expiration
	if count == 1 {
		rdb.Expire(rctx, key, time.Hour)
	}
	if count > maxResponsesPerHour {
		return fmt.Errorf("rate limit reached — max %d responses per hour", maxResponsesPerHour)
	}
	return nil
}

// ════════════════════════════════════════════════════════════════════════════════
// TIER 2 — RELEVANCE SCORING + COMMUNITY MODERATION
// ════════════════════════════════════════════════════════════════════════════════

// computeRelevanceScore measures how related the response caption is to the
// challenge's prompt. Pure keyword/token overlap — cheap, runs at upload time.
// Returns a value in [0.0, 1.0]. Used by the feed engine to down-rank
// off-topic-looking responses.
//
// Heuristic: Jaccard-ish overlap of meaningful tokens between
//
//	challenge.subject + challenge.prefix + challenge.category
//
// and
//
//	response.caption
//
// If the caption is empty, fall back to neutral score (0.5) — we don't know,
// we don't want to penalize creators who skip the caption field.
func computeRelevanceScore(challenge Challenge, caption string) float64 {
	if strings.TrimSpace(caption) == "" {
		return 0.5 // neutral
	}

	challengeTokens := tokenize(strings.ToLower(
		challenge.Subject + " " + challenge.Prefix + " " + challenge.Category,
	))
	captionTokens := tokenize(strings.ToLower(caption))

	if len(challengeTokens) == 0 || len(captionTokens) == 0 {
		return 0.5
	}

	// Build set of challenge tokens for O(1) lookups
	challengeSet := make(map[string]bool, len(challengeTokens))
	for _, t := range challengeTokens {
		challengeSet[t] = true
	}

	overlap := 0
	for _, t := range captionTokens {
		if challengeSet[t] {
			overlap++
		}
	}

	// Score = overlap / unique challenge tokens (so a caption that hits
	// every challenge keyword gets ~1.0 even if it's longer)
	return float64(overlap) / float64(len(challengeSet))
}

// tokenize splits text into lowercase tokens of length >= 3, dropping stopwords.
// Cheap heuristic — good enough for keyword overlap, no NLP library needed.
func tokenize(text string) []string {
	stopwords := map[string]bool{
		"the": true, "and": true, "for": true, "you": true, "are": true,
		"can": true, "with": true, "your": true, "this": true, "that": true,
		"from": true, "but": true, "not": true, "all": true, "have": true,
		"who": true, "what": true, "how": true, "why": true, "when": true,
	}
	var out []string
	var cur strings.Builder
	flush := func() {
		s := cur.String()
		cur.Reset()
		if len(s) >= 3 && !stopwords[s] {
			out = append(out, s)
		}
	}
	for _, r := range text {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			cur.WriteRune(r)
		default:
			flush()
		}
	}
	flush()
	return out
}

// FlagResponseHandler is the older way to report an answer, by the answer's
// own id. It now goes through exactly the same rules as ReportOffTopicHandler:
// see offtopic.go.
//
// POST /api/v1/challenges/responses/{id}/flag
//
// It used to hide an answer once five people had flagged it AND those flags
// were most of its views. Nothing in the app ever called it, and a rule that
// needs five flags on a video almost nobody has watched yet would not have
// caught anything if it had.
func FlagResponseHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "OPTIONS" {
		w.WriteHeader(http.StatusOK)
		return
	}
	uid, err := strconv.Atoi(authUserID(r))
	if err != nil || uid <= 0 {
		http.Error(w, "sign in to report a video", http.StatusUnauthorized)
		return
	}
	rid, err := strconv.Atoi(mux.Vars(r)["id"])
	if err != nil || rid <= 0 {
		http.Error(w, "invalid response id", http.StatusBadRequest)
		return
	}
	var cid int
	err = db.QueryRowContext(r.Context(),
		`SELECT challenge_id FROM challenge_responses WHERE id = $1`, rid).Scan(&cid)
	if err == sql.ErrNoRows {
		http.Error(w, "That video isn't there any more.", http.StatusNotFound)
		return
	}
	if err != nil {
		log.Printf("flag: could not find the battle of answer %d: %v", rid, err)
		http.Error(w, "could not record the report — try again", http.StatusInternalServerError)
		return
	}
	status, msg, err := reportOffTopic(r.Context(), uid, cid, rid)
	if err != nil {
		log.Printf("flag: report by %d on answer %d failed: %v", uid, rid, err)
		http.Error(w, "could not record the report — try again", http.StatusInternalServerError)
		return
	}
	if status != http.StatusOK {
		http.Error(w, msg, status)
		return
	}
	// Nothing is hidden for a report any more: videos stay up and their
	// owners pay instead. See offtopic.go.
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": true,
		"hidden":  false,
		"message": msg,
	})
}

// userOffTopicRate returns the fraction of a user's past responses that were
// hidden (auto-moderated). Used to apply progressively stricter validation
// to repeat offenders.
func userOffTopicRate(userID string) float64 {
	rid, err := strconv.Atoi(userID)
	if err != nil {
		return 0
	}
	var total, hidden int
	if err := db.QueryRow(`SELECT COUNT(*) FROM challenge_responses WHERE responder_id = $1`, rid).Scan(&total); err != nil {
		queryFailed("userOffTopicRate: could not read challenge_responses",
			"carrying on as if the answer were empty", err)
	}
	if total < 5 {
		return 0 // Not enough history to judge
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM challenge_responses WHERE responder_id = $1 AND is_hidden = TRUE`, rid).Scan(&hidden); err != nil {
		queryFailed("userOffTopicRate: could not read challenge_responses",
			"carrying on as if the answer were empty", err)
	}
	return float64(hidden) / float64(total)
}
