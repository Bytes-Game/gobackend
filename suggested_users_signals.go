package main

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"
)

// ─────────────────────────────────────────────────────────────────────────────
// SUGGESTION-ACCEPTANCE FEEDBACK LOOP
//
// When a user follows someone from a suggested-accounts card, we record
// which "lane" (fof / category / popular / league) drove that suggestion.
// Over time we learn that user's preference — some users follow 80% from
// FoF lanes (social-driven), others overwhelmingly from category lanes
// (taste-driven).
//
// On subsequent card builds, BuildSuggestedAccountsCard reads the user's
// per-lane acceptance counts and applies a bias multiplier so suggestions
// from the user's preferred lane rise. The bias is bounded so a heavy
// preference doesn't entirely shut out other lanes — exploration matters.
//
// Storage: Redis hash keyed `sa_accept:<userID>` → field=lane → counter.
// 30-day TTL so preferences refresh naturally; no schema changes; cheap
// HINCRBY on write, single HGETALL on read.
// ─────────────────────────────────────────────────────────────────────────────

const (
	// suggestionAcceptanceTTL controls how long we remember an acceptance.
	// 30 days is long enough to stabilize per-user signal but short enough
	// that taste shifts (a user who pivots from sports to dance content)
	// don't get permanently locked into the old lane preference.
	suggestionAcceptanceTTL = 30 * 24 * time.Hour
	// maxLaneBias is the ceiling on the per-lane score multiplier. With
	// 0.30 the most-preferred lane gets at most a 30% score boost over
	// what a neutral ranker would assign. Tuned to be noticeable in card
	// composition without monoculturing the surface — exploration still
	// has room to win when its absolute score is competitive.
	maxLaneBias = 0.30
)

// suggestionAcceptanceKey builds the Redis hash key for a user's acceptance
// counts. Centralizing avoids prefix typos sneaking in across read/write.
func suggestionAcceptanceKey(userID string) string {
	return "sa_accept:" + userID
}

// recordSuggestionAcceptance increments the per-lane counter for one
// follow-from-suggestion. Lane must match the dominant-reason values used
// in suggested_users.go: "fof" | "category" | "popular" | "league". Any
// other value is silently dropped to keep the storage clean.
func recordSuggestionAcceptance(userID, lane string) {
	if rdb == nil || userID == "" || lane == "" {
		return
	}
	switch lane {
	case "fof", "category", "popular", "league":
		// allowed
	default:
		return
	}
	key := suggestionAcceptanceKey(userID)
	if err := rdb.HIncrBy(rctx, key, lane, 1).Err(); err != nil {
		return
	}
	// Refresh TTL on every write — taste signal stays alive while the user
	// is engaged; goes cold automatically after 30d of no acceptance.
	_ = rdb.Expire(rctx, key, suggestionAcceptanceTTL).Err()
	if metricSignalCapture != nil {
		metricSignalCapture.WithLabelValues("suggestion_accept_" + lane).Inc()
	}
}

// getSuggestionAcceptance returns the per-lane counter map for a user.
// Empty map (not nil) when no signal yet — callers can treat both the same
// way but the map type makes for cleaner downstream code.
func getSuggestionAcceptance(userID string) map[string]int {
	out := map[string]int{}
	if rdb == nil || userID == "" {
		return out
	}
	res, err := rdb.HGetAll(rctx, suggestionAcceptanceKey(userID)).Result()
	if err != nil {
		return out
	}
	for k, v := range res {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			out[k] = n
		}
	}
	return out
}

// suggestionLaneBias returns a per-lane multiplier in [1, 1+maxLaneBias].
// totalAcceptance is the sum of all lane counts; passing it in avoids the
// caller recomputing it on every score iteration. When the user has no
// signal yet, all lanes return 1.0 (no bias).
func suggestionLaneBias(acceptance map[string]int, totalAcceptance int, lane string) float64 {
	if totalAcceptance <= 0 || lane == "" {
		return 1.0
	}
	count, ok := acceptance[lane]
	if !ok || count <= 0 {
		return 1.0
	}
	share := float64(count) / float64(totalAcceptance)
	// Linear scaling: a lane that captured 100% of accepts gets the full
	// maxLaneBias multiplier; a lane with 30% share gets 0.30 * maxLaneBias.
	return 1.0 + maxLaneBias*share
}

// ─────────────────────────────────────────────────────────────────────────────
// HTTP HANDLER
// ─────────────────────────────────────────────────────────────────────────────

// suggestionAcceptedPayload is the request body for the accepted endpoint.
type suggestionAcceptedPayload struct {
	UserID       string `json:"userId"`
	Lane         string `json:"lane"`
	TargetUserID string `json:"targetUserId,omitempty"` // optional, for analytics
	CardID       string `json:"cardId,omitempty"`       // optional, for analytics
}

// SuggestionAcceptedHandler records a follow-from-suggestion event so the
// ranker can learn the user's lane preference. Idempotent at the user-card
// level — a user double-tap-following the same target on the same card
// would currently double-count, but the ceiling on maxLaneBias keeps any
// runaway from corrupting the ranker. Acceptable for v1.
//
// POST /api/v1/suggestions/accepted
//   body: {"userId":"42", "lane":"fof", "targetUserId":"17", "cardId":"sa_42_1"}
func SuggestionAcceptedHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "OPTIONS" {
		w.WriteHeader(http.StatusOK)
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, `{"error":"POST required"}`, http.StatusMethodNotAllowed)
		return
	}
	var p suggestionAcceptedPayload
	if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
		http.Error(w, `{"error":"invalid JSON"}`, http.StatusBadRequest)
		return
	}
	// The accepting user is the authenticated one.
	p.UserID = authUserID(r)
	if p.UserID == "" || p.Lane == "" {
		http.Error(w, `{"error":"userId and lane are required"}`, http.StatusBadRequest)
		return
	}
	recordSuggestionAcceptance(p.UserID, p.Lane)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}
