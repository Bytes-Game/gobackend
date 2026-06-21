package main

import (
	"encoding/json"
	"fmt"
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
//   Tier 2 (community-driven, no AI):
//     - Voters can flag responses as off-topic
//     - >= flagThreshold flags + flag-to-view ratio > flagRatioCutoff = auto-hide
//     - Lightweight keyword-overlap relevance score computed at upload time
//     - Per-user repeat-offender tracking via off_topic_rate
//
// At scale, tier-3 (vision/audio AI) only runs on community-flagged content,
// not on every upload — that's how we keep per-upload cost ~ zero.

const (
	// Tier-1 duration bounds
	minResponseDurationMs = 2000   // < 2s = empty/glitched upload
	maxResponseDurationMs = 180000 // > 3min = not short-form video

	// Tier-1 rate limit (Redis bucket: responses:rate:{userID}, EX 3600)
	maxResponsesPerHour = 5

	// Tier-2 community moderation thresholds
	flagThreshold       = 5    // need at least 5 flags to consider hiding
	flagRatioCutoff     = 0.6  // flags / max(views, flags) >= 0.6 → hide
	relevanceLowCutoff  = 0.10 // below this score = "off-topic-ish" (down-rank in feed)
	offTopicUserCutoff  = 0.4  // user with > 40% historically hidden responses gets stricter checks
)

// ════════════════════════════════════════════════════════════════════════════════
// TIER 1 — STRUCTURAL VALIDATION
// ════════════════════════════════════════════════════════════════════════════════

// validateChallengeResponseSubmission runs all tier-1 checks on a new response.
// Returns nil on success or a user-facing error on failure.
func validateChallengeResponseSubmission(payload AcceptChallengePayload, challenge Challenge) error {
	// --- Duration bounds ---
	if payload.DurationMs < minResponseDurationMs {
		return fmt.Errorf("video too short — minimum %d seconds", minResponseDurationMs/1000)
	}
	if payload.DurationMs > maxResponseDurationMs {
		return fmt.Errorf("video too long — maximum %d seconds", maxResponseDurationMs/1000)
	}

	// --- Video URL must not be empty ---
	if strings.TrimSpace(payload.VideoURL) == "" {
		return fmt.Errorf("video URL is required")
	}

	// --- Challenge must still accept responses ---
	if challenge.Status == "closed" || challenge.Status == "expired" {
		return fmt.Errorf("challenge is no longer accepting responses")
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
	db.QueryRow(
		`SELECT EXISTS(SELECT 1 FROM challenge_responses WHERE responder_id=$1 AND video_url=$2)`,
		rid, payload.VideoURL,
	).Scan(&dupExists)
	if dupExists {
		return fmt.Errorf("you have already used this video for another challenge — record a new one")
	}

	// --- One response per user per challenge ---
	var alreadyResponded bool
	db.QueryRow(
		`SELECT EXISTS(SELECT 1 FROM challenge_responses WHERE responder_id=$1 AND challenge_id=$2)`,
		rid, cid,
	).Scan(&alreadyResponded)
	if alreadyResponded {
		return fmt.Errorf("you have already responded to this challenge")
	}

	// --- Repeat-offender gate ---
	// If >40% of this user's past responses have been community-hidden as
	// off-topic, reject further submissions outright until a human reviews.
	// Protects the challenge feed from well-tested bad actors without
	// needing an explicit ban list.
	if rate := userOffTopicRate(payload.ResponderID); rate > 0.4 {
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
//   challenge.subject + challenge.prefix + challenge.category
// and
//   response.caption
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

// FlagResponseHandler registers an off-topic flag from a community member.
// POST /api/v1/challenges/responses/{id}/flag  body: {"userId":"...","reason":"off_topic"}
//
// Each (response, user) pair can flag at most once (PRIMARY KEY constraint).
// After insertion we recount flags and auto-hide if both:
//   - flag count >= flagThreshold
//   - flag rate (flags / max(views, flags)) >= flagRatioCutoff
// This avoids a single brigade hiding good content, but quickly hides obviously
// off-topic uploads that have been seen by many people without engagement.
func FlagResponseHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "OPTIONS" {
		w.WriteHeader(http.StatusOK)
		return
	}
	vars := mux.Vars(r)
	responseIDStr := vars["id"]
	responseID, err := strconv.Atoi(responseIDStr)
	if err != nil {
		http.Error(w, "invalid response id", http.StatusBadRequest)
		return
	}

	var payload FlagResponsePayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "invalid body", http.StatusBadRequest)
		return
	}
	// The flagger is the authenticated user.
	payload.UserID = authUserID(r)
	userID, err := strconv.Atoi(payload.UserID)
	if err != nil {
		http.Error(w, "invalid userId", http.StatusBadRequest)
		return
	}
	reason := payload.Reason
	if reason == "" {
		reason = "off_topic"
	}

	// Insert the flag (idempotent via PRIMARY KEY (response_id, user_id))
	_, err = db.Exec(
		`INSERT INTO challenge_response_flags (response_id, user_id, reason)
		 VALUES ($1, $2, $3)
		 ON CONFLICT (response_id, user_id) DO NOTHING`,
		responseID, userID, reason,
	)
	if err != nil {
		http.Error(w, "failed to record flag", http.StatusInternalServerError)
		return
	}

	// Re-evaluate hiding criteria
	hidden := evaluateHidingThreshold(responseID)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": true,
		"hidden":  hidden,
	})
}

// evaluateHidingThreshold counts flags + views and hides the response when
// both thresholds are crossed. Returns the new is_hidden state.
func evaluateHidingThreshold(responseID int) bool {
	var flags, views int
	db.QueryRow(`SELECT COUNT(*) FROM challenge_response_flags WHERE response_id = $1`, responseID).Scan(&flags)
	db.QueryRow(`SELECT COALESCE(views, 0) FROM challenge_responses WHERE id = $1`, responseID).Scan(&views)

	// Update the cached counter on the response (used for fast feed reads)
	db.Exec(`UPDATE challenge_responses SET off_topic_flags = $1 WHERE id = $2`, flags, responseID)

	if flags < flagThreshold {
		return false
	}
	denom := views
	if flags > denom {
		denom = flags
	}
	if denom == 0 {
		return false
	}
	ratio := float64(flags) / float64(denom)
	if ratio >= flagRatioCutoff {
		db.Exec(`UPDATE challenge_responses SET is_hidden = TRUE WHERE id = $1`, responseID)
		return true
	}
	return false
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
	db.QueryRow(`SELECT COUNT(*) FROM challenge_responses WHERE responder_id = $1`, rid).Scan(&total)
	if total < 5 {
		return 0 // Not enough history to judge
	}
	db.QueryRow(`SELECT COUNT(*) FROM challenge_responses WHERE responder_id = $1 AND is_hidden = TRUE`, rid).Scan(&hidden)
	return float64(hidden) / float64(total)
}
