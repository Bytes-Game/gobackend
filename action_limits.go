package main

import (
	"net/http"
	"strconv"
	"sync"
)

// Per-user, per-action rate limiter.
//
// The existing IP-based rateLimitMiddleware (main.go) catches scrapers
// and broad abuse, but it's far too generous for individual mutating
// actions. A user behind one phone IP can hit 10 follows/second for
// minutes before tripping the global limit — that's enough to mass-
// follow every newly-signed-up user and game social ranking.
//
// This file adds finer-grained limits keyed on (userId, action). Each
// action gets its own bucket config — the limits below mirror the
// playbook every major social app converges on after a year of abuse
// reports. Tweak constants as needed; the structure is stable.
//
// Why not a middleware: middleware has to peek at the body to find the
// userId, and once it does, the handler can't re-read the same
// io.ReadCloser without buffering. We avoid that whole mess by having
// each handler call allowAction() inline AFTER it has parsed its
// payload — the userId field is right there in the decoded struct.

// actionLimitConfig captures rate + burst for one action. Same shape
// as rateLimiter's params; we reuse the bucket struct so the math
// path is identical.
type actionLimitConfig struct {
	tokensPerSecond float64
	burst           int
}

// actionLimitTable is the source of truth for per-action limits.
// Edit here to retune; constants are tokens-per-second and burst (the
// instantaneous max). For "N per hour" think tokensPerSecond = N/3600.
//
// Burst is intentionally generous-but-bounded so a real user doing a
// brief flurry (e.g. liking 5 reels in a row) doesn't get throttled,
// while a script doing 50 in a second still does.
var actionLimitTable = map[string]actionLimitConfig{
	// Engagement actions — high volume, low individual cost.
	"like":             {tokensPerSecond: 1.0, burst: 20},  // 60/min, burst 20
	"comment":          {tokensPerSecond: 0.33, burst: 8},  // ~20/min, burst 8
	"vote":             {tokensPerSecond: 0.5, burst: 10},  // 30/min, burst 10
	"save":             {tokensPerSecond: 1.0, burst: 20},  // 60/min, burst 20

	// Social graph — these are abuse-prone (follow-bombing,
	// stalking-via-block) so we keep them tight.
	"follow":           {tokensPerSecond: 0.5, burst: 5},   // 30/min, burst 5
	"unfollow":         {tokensPerSecond: 0.5, burst: 5},
	"block":            {tokensPerSecond: 0.33, burst: 5},  // 20/min
	"unblock":          {tokensPerSecond: 0.33, burst: 5},

	// Content creation — expensive (storage + processing) so the per-
	// hour cap is quite low. Burst of 2 means even back-to-back
	// uploads after a long break are fine.
	"challenge_create": {tokensPerSecond: 5.0 / 3600.0, burst: 2}, // 5/hr
	"challenge_accept": {tokensPerSecond: 0.0083, burst: 3},       // 30/hr, burst 3

	// Messaging — chat needs to feel instant for real conversations
	// but a script could absolutely spam. 1 msg/sec sustained, burst
	// of 10 for a quick exchange.
	"chat":             {tokensPerSecond: 1.0, burst: 10},

	// Reports — moderation tooling abuse-prone (false reports to
	// silence rivals), keep tight. 10/hr sustained.
	"report":           {tokensPerSecond: 0.0028, burst: 3},

	// Profile mutations — typical user does these once or twice.
	// Sustained rate is throwaway; burst is what matters.
	"profile_edit":     {tokensPerSecond: 0.0167, burst: 3},       // 60/hr

	// Auth — login / signup attempts. Anything more than a few per
	// minute from one account is a credential-stuffing attempt.
	"login":            {tokensPerSecond: 0.1, burst: 5},          // 6/min
	"signup":           {tokensPerSecond: 0.05, burst: 3},         // 3/min
}

// actionLimiterRegistry holds one rateLimiter per action key. Created
// lazily on first use — cheaper than initializing the full table at
// boot when most actions might not be exercised in a given process.
var (
	actionLimitersMu sync.RWMutex
	actionLimiters   = map[string]*rateLimiter{}
)

// getActionLimiter returns the limiter for the named action. Creates
// it on first call from the actionLimitTable config. Returns nil if
// the action isn't in the table — callers should treat nil as
// "unlimited" (no per-action gate, IP-based global gate still applies).
func getActionLimiter(action string) *rateLimiter {
	actionLimitersMu.RLock()
	rl, ok := actionLimiters[action]
	actionLimitersMu.RUnlock()
	if ok {
		return rl
	}
	cfg, exists := actionLimitTable[action]
	if !exists {
		return nil
	}
	actionLimitersMu.Lock()
	defer actionLimitersMu.Unlock()
	// Double-check after acquiring the write lock — another goroutine
	// may have created it between our RUnlock and Lock.
	if rl, ok = actionLimiters[action]; ok {
		return rl
	}
	rl = newRateLimiter(cfg.tokensPerSecond, cfg.burst)
	actionLimiters[action] = rl
	return rl
}

// allowAction returns true if the (userID, action) bucket has tokens
// available. Consumes one token on success. Anonymous callers (empty
// userID) get rate-limited too — same bucket keyed by "anon:<action>"
// — so a logged-out spammer can't bypass the gate by omitting the
// payload field.
//
// Unknown actions are allowed unconditionally. That's intentional:
// callers should be able to add new endpoint logic without first
// touching this file. Once the new action's usage is observed it
// should get its own row in actionLimitTable.
func allowAction(userID, action string) bool {
	rl := getActionLimiter(action)
	if rl == nil {
		return true // no per-action gate configured
	}
	key := userID
	if key == "" {
		key = "anon"
	}
	return rl.allow(key + ":" + action)
}

// writeRateLimited sends the canonical 429 response. Centralized so
// every handler's "you're going too fast" path emits the same shape
// and headers — clients can key off Retry-After to back off cleanly.
func writeRateLimited(w http.ResponseWriter, action string) {
	w.Header().Set("Content-Type", "application/json")
	// Retry-After is a hint, not enforced. Approximate from the
	// bucket's fill rate so clients can back off intelligently
	// instead of hammering with re-tries.
	if cfg, ok := actionLimitTable[action]; ok && cfg.tokensPerSecond > 0 {
		retry := int(1.0/cfg.tokensPerSecond) + 1
		if retry > 300 {
			retry = 300 // cap so we don't tell clients to wait 5+ minutes
		}
		w.Header().Set("Retry-After", strconv.Itoa(retry))
	}
	w.WriteHeader(http.StatusTooManyRequests)
	_, _ = w.Write([]byte(`{"error":"rate limited","action":"` + action + `"}`))
}
