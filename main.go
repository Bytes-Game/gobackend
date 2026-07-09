package main

import (
	"context"
	"encoding/json"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/mux"
)

// LoginHandler validates credentials and returns the user's data and all other users.
func LoginHandler(w http.ResponseWriter, r *http.Request) {
	var creds struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}

	if err := json.NewDecoder(r.Body).Decode(&creds); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// First, validate the user's credentials.
	if !IsValidUser(creds.Username, creds.Password) {
		http.Error(w, "Invalid username or password", http.StatusUnauthorized)
		log.Printf("Failed login attempt for user %s.", creds.Username)
		return
	}

	// If valid, fetch the logged-in user's full profile.
	user, exists := GetUserByUsername(creds.Username)
	if !exists {
		// This should not happen if IsValidUser passed, but it's good practice to check.
		http.Error(w, "Could not find user data after successful login", http.StatusInternalServerError)
		return
	}

	// Mint a signed session token. From here on the client authenticates every
	// protected request with this token (Authorization: Bearer <token>) and the
	// server derives identity from it — never from a client-supplied userId.
	token, err := issueToken(user.ID, user.Username)
	if err != nil {
		// Almost always means JWT_SECRET is unset — fail closed rather than hand
		// back a session the protected routes will reject anyway.
		log.Printf("token issuance failed for %s: %v", creds.Username, err)
		http.Error(w, "Login temporarily unavailable", http.StatusInternalServerError)
		return
	}

	// Create the response payload — no longer sending all users for security.
	response := map[string]interface{}{
		"user":     user,
		"token":    token,
		"allUsers": []User{},
	}

	// Send the successful response.
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
	log.Printf("User %s logged in successfully.", creds.Username)
}

// ReseedHandler drops all data and reseeds the database.
func ReseedHandler(w http.ResponseWriter, r *http.Request) {
	ReseedDatabase()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "reseeded"})
}

// GetAllUsersHandler returns a bounded page of users.
// GET /api/v1/users?page=&limit=  (limit default 50, capped at 100).
//
// Previously this returned the ENTIRE users table and the client called it on
// every login — an unbounded query that does not survive growth. It's paginated
// now; callers that need the whole set (background indexers) use GetAllUsers
// directly server-side.
func GetAllUsersHandler(w http.ResponseWriter, r *http.Request) {
	limit := parseIntOrDefault(r.URL.Query().Get("limit"), 50, 100)
	page := parseIntOrDefault(r.URL.Query().Get("page"), 1, 1_000_000)
	users := GetUsersPaginated(limit, (page-1)*limit)
	if users == nil {
		users = []User{}
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(users); err != nil {
		http.Error(w, "Failed to encode users", http.StatusInternalServerError)
	}
}

// GetuserHandler returns the data for a single user.
func GetUserHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	username := vars["username"]

	user, exists := GetUserByUsername(username)
	if !exists {
		http.Error(w, "User not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(user); err != nil {
		http.Error(w, "Failed to encode user data", http.StatusInternalServerError)
	}
}

// corsMiddleware adds CORS headers to every response so the Flutter app
// (web or mobile) can reach the backend without cross-origin errors.
func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// ---------------------------------------------------------------------------
// In-memory token-bucket rate limiter
// ---------------------------------------------------------------------------

type rateLimiter struct {
	mu       sync.Mutex
	buckets  map[string]*bucket
	rate     float64 // tokens per second
	capacity int
	// name namespaces this limiter's keys in Redis for the distributed
	// path. Empty = in-process only (tests, or unnamed limiters).
	name string
}

type bucket struct {
	tokens    float64
	lastCheck time.Time
}

// rateLimitLua is an atomic token bucket in Redis — the multi-replica
// variant of (rl *rateLimiter).allow. State per key is a hash
// {t: tokens, ts: last-check ms}; the script refills by elapsed time,
// consumes one token when available, and returns 1/0. A 15-minute TTL
// mirrors the in-process janitor (an idle bucket is full anyway, so
// expiry loses nothing).
const rateLimitLua = `
local rate = tonumber(ARGV[1])
local cap  = tonumber(ARGV[2])
local now  = tonumber(ARGV[3])
local h = redis.call('HMGET', KEYS[1], 't', 'ts')
local tokens = tonumber(h[1])
local ts     = tonumber(h[2])
if tokens == nil then tokens = cap; ts = now end
local elapsed = (now - ts) / 1000.0
if elapsed > 0 then
  tokens = math.min(cap, tokens + elapsed * rate)
end
local allowed = 0
if tokens >= 1 then tokens = tokens - 1; allowed = 1 end
redis.call('HSET', KEYS[1], 't', tokens, 'ts', now)
redis.call('PEXPIRE', KEYS[1], 900000)
return allowed
`

// limiterRegistry tracks every rateLimiter created so a single background
// janitor can sweep idle buckets out of all of them. Without this the buckets
// maps grow unbounded (one entry per distinct client/user forever) and leak
// memory — at scale that's an eventual OOM.
var (
	limiterRegistryMu sync.Mutex
	limiterRegistry   []*rateLimiter
)

func newRateLimiter(rps float64, burst int) *rateLimiter {
	rl := &rateLimiter{
		buckets:  make(map[string]*bucket),
		rate:     rps,
		capacity: burst,
	}
	limiterRegistryMu.Lock()
	limiterRegistry = append(limiterRegistry, rl)
	limiterRegistryMu.Unlock()
	return rl
}

// newNamedRateLimiter is newRateLimiter plus a Redis key namespace,
// which opts the limiter into the distributed token bucket when
// MULTI_REPLICA is on. Production limiters use this; unnamed limiters
// (tests) always stay in-process.
func newNamedRateLimiter(name string, rps float64, burst int) *rateLimiter {
	rl := newRateLimiter(rps, burst)
	rl.name = name
	return rl
}

// cleanup evicts buckets untouched for longer than maxIdle. A bucket idle that
// long has already refilled to full capacity, so dropping it loses no state —
// the next request from that key just recreates a full bucket.
func (rl *rateLimiter) cleanup(maxIdle time.Duration) {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	now := time.Now()
	for k, b := range rl.buckets {
		if now.Sub(b.lastCheck) > maxIdle {
			delete(rl.buckets, k)
		}
	}
}

// startLimiterJanitor periodically sweeps idle buckets out of every registered
// limiter (the global per-IP one plus every per-action one). One goroutine for
// all of them. Single-instance by design — when this runs on multiple replicas
// each keeps its own buckets; move to a Redis-backed limiter before scaling out.
func startLimiterJanitor() {
	go func() {
		tk := time.NewTicker(5 * time.Minute)
		defer tk.Stop()
		for range tk.C {
			limiterRegistryMu.Lock()
			snapshot := append([]*rateLimiter(nil), limiterRegistry...)
			limiterRegistryMu.Unlock()
			for _, rl := range snapshot {
				rl.cleanup(15 * time.Minute)
			}
		}
	}()
}

// clientIP returns the real caller IP for rate-limit keying. Behind Render (and
// most proxies) the connecting socket is the proxy, so r.RemoteAddr is useless
// — every client collapses to one bucket. The proxy forwards the real client in
// X-Forwarded-For ("client, proxy1, …"); we take the left-most entry. Falls back
// to the RemoteAddr host (port stripped) for direct/local connections.
func clientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		if i := strings.IndexByte(xff, ','); i >= 0 {
			return strings.TrimSpace(xff[:i])
		}
		return strings.TrimSpace(xff)
	}
	if xr := strings.TrimSpace(r.Header.Get("X-Real-IP")); xr != "" {
		return xr
	}
	if host, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
		return host
	}
	return r.RemoteAddr
}

func (rl *rateLimiter) allow(key string) bool {
	// Multi-replica mode: one shared token bucket in Redis instead of a
	// per-replica bucket (N replicas would otherwise multiply every
	// budget by N). On Redis error, fall THROUGH to the in-process
	// bucket — degraded to per-replica limiting, never to unlimited.
	if rl.name != "" && multiReplica() && rdb != nil {
		res, err := rdb.Eval(rctx, rateLimitLua,
			[]string{"rl:" + rl.name + ":" + key},
			rl.rate, rl.capacity, time.Now().UnixMilli()).Result()
		if err == nil {
			n, _ := res.(int64)
			return n == 1
		}
	}

	rl.mu.Lock()
	defer rl.mu.Unlock()

	b, exists := rl.buckets[key]
	now := time.Now()

	if !exists {
		rl.buckets[key] = &bucket{tokens: float64(rl.capacity) - 1, lastCheck: now}
		return true
	}

	elapsed := now.Sub(b.lastCheck).Seconds()
	b.tokens += elapsed * rl.rate
	if b.tokens > float64(rl.capacity) {
		b.tokens = float64(rl.capacity)
	}
	b.lastCheck = now

	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

func rateLimitMiddleware(limiter *rateLimiter) mux.MiddlewareFunc {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !limiter.allow(clientIP(r)) {
				http.Error(w, "Too many requests", http.StatusTooManyRequests)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// main is the entry point for the application.
func main() {
	InitDatabase()
	InitRedis()
	InitMeilisearch()
	// Warn loudly (but don't crash) if the session-token signing key is missing,
	// so it's noticed at boot rather than on the first failed login.
	checkAuthConfig()
	// Build the challenge-subject autocomplete index. Runs the
	// curated-vocab + existing-challenge merge in the background so
	// it doesn't block startup; the handler degrades to the in-binary
	// vocabulary if a request lands before the index finishes seeding.
	go seedChallengeSubjects()
	registerMetrics()
	startSimilarityWorker()
	startImpressionAggregator()
	startAnalyticsScheduler()
	startLTRFlusher()
	startPlattRefitter()
	startTrendingPruner()
	startBootstrapPoolWorker()
	startWatchRatioFlusher()
	startEmbeddingBackfillWorker()
	initPushSender()
	startNotificationDispatcher()
	startNotificationTriggers()
	// Reset HLS transcode jobs orphaned at 'PENDING' by crashed workers.
	startHLSReaper()
	// DB-backed experiments: seed on fresh DB, refresh every 60s so
	// experiment edits (incl. the active=false kill switch) apply
	// without a redeploy.
	startExperimentRefresher()
	// Restore the learned mood-transition + session-trajectory state
	// that lives in process memory (write-through keeps Redis current).
	loadMoodTransitions()
	loadSessionTrajectories()
	// Cross-replica WebSocket delivery (no-op unless MULTI_REPLICA=1).
	startWSRelay()
	// Evict idle rate-limiter buckets so the in-memory limiter maps don't grow
	// without bound (one entry per client/user forever).
	startLimiterJanitor()

	r := mux.NewRouter()

	// Apply rate limiting: 10 requests/sec with burst of 20 per IP
	limiter := newNamedRateLimiter("ip", 10, 20)
	r.Use(rateLimitMiddleware(limiter))
	r.Use(metricsMiddleware)

	api := r.PathPrefix("/api/v1").Subrouter()
	api.HandleFunc("/users", GetAllUsersHandler).Methods("GET", "OPTIONS")
	api.HandleFunc("/users/{username}", GetUserHandler).Methods("GET", "OPTIONS")
	api.HandleFunc("/follow", authed(HandleFollowEvent)).Methods("POST", "OPTIONS")
	api.HandleFunc("/unfollow", authed(HandleUnfollowEvent)).Methods("POST", "OPTIONS")
	// Legacy post-centric routes retired (/feed, /home, /posts/{userId},
	// /like, /comments) — the home reels feed now serves challenges only
	// (battles + unaccepted-as-shorts) via /feed/smart, and per-challenge
	// engagement uses /challenges/like + /challenges/{id}/comments.
	// Direct-to-R2 upload presigning. Mints AWS SigV4 PUT URLs for one or
	// more variants (480p/720p/1080p video + thumbnail) so the mobile
	// client uploads bytes directly to object storage without ever
	// streaming them through Render.
	api.HandleFunc("/media/presign", authed(PresignMediaUploadHandler)).Methods("POST", "OPTIONS")
	// HLS background worker endpoints. /next-pending claims one
	// transcode job (atomic SKIP LOCKED), /complete records the
	// finished manifest URL, /fail returns the row to the queue so
	// another worker can retry. All three require the X-Worker-Token
	// header — workers authenticate via HLS_WORKER_TOKEN env var. See
	// hls_worker_api.go for the contract details.
	api.HandleFunc("/internal/hls/next-pending", workerAuthed(HLSNextPendingHandler)).Methods("POST", "OPTIONS")
	api.HandleFunc("/internal/hls/complete", workerAuthed(HLSCompleteHandler)).Methods("POST", "OPTIONS")
	api.HandleFunc("/internal/hls/fail", workerAuthed(HLSFailHandler)).Methods("POST", "OPTIONS")
	api.HandleFunc("/challenges", authed(CreateChallengeHandler)).Methods("POST", "OPTIONS")
	api.HandleFunc("/challenges/arena", GetArenaChallengesHandler).Methods("GET", "OPTIONS")
	api.HandleFunc("/challenges/friends", authed(GetFriendsChallengesHandler)).Methods("GET", "OPTIONS")
	api.HandleFunc("/challenges/accept", authed(AcceptChallengeHandler)).Methods("POST", "OPTIONS")
	api.HandleFunc("/challenges/like", authed(LikeChallengeHandler)).Methods("POST", "OPTIONS")
	api.HandleFunc("/challenges/delete", authed(DeleteChallengeHandler)).Methods("POST", "OPTIONS")
	api.HandleFunc("/challenges/vote", authed(VoteChallengeHandler)).Methods("POST", "OPTIONS")
	api.HandleFunc("/challenges/comments", authed(AddChallengeCommentHandler)).Methods("POST", "OPTIONS")
	api.HandleFunc("/challenges/responses/{id}/flag", authed(FlagResponseHandler)).Methods("POST", "OPTIONS")
	api.HandleFunc("/challenges/{id}/votes", GetVoteResultsHandler).Methods("GET", "OPTIONS")
	api.HandleFunc("/challenges/{id}/comments", GetChallengeCommentsHandler).Methods("GET", "OPTIONS")
	api.HandleFunc("/challenges/{id}", GetChallengeDetailHandler).Methods("GET", "OPTIONS")
	api.HandleFunc("/feed/recommended", authed(RecommendedFeedHandler)).Methods("GET", "OPTIONS")
	api.HandleFunc("/feed/following", authed(FollowingFeedHandler)).Methods("GET", "OPTIONS")
	// Psychology-based recommendation engine (v2)
	api.HandleFunc("/feed/smart", authed(SmartFeedHandler)).Methods("GET", "OPTIONS")
	api.HandleFunc("/feed/following/v2", authed(FollowingFeedV2Handler)).Methods("GET", "OPTIONS")
	api.HandleFunc("/feed/explore", authed(ExploreFeedHandler)).Methods("GET", "OPTIONS")
	api.HandleFunc("/categories", CategoriesHandler).Methods("GET", "OPTIONS")
	api.HandleFunc("/events", authed(TrackEventHandler)).Methods("POST", "OPTIONS")
	api.HandleFunc("/events/batch", authed(TrackBatchEventsHandler)).Methods("POST", "OPTIONS")
	api.HandleFunc("/profile", authed(UserProfileHandler)).Methods("GET", "OPTIONS")
	// Challenge-creation autocomplete. Two surfaces — prefix (small,
	// curated, in-memory) and subject (large, Meilisearch-backed +
	// popularity-ranked). See suggest_handlers.go for the ranking.
	api.HandleFunc("/suggest/challenge-prefix", SuggestChallengePrefixHandler).Methods("GET", "OPTIONS")
	api.HandleFunc("/suggest/challenge-subject", SuggestChallengeSubjectHandler).Methods("GET", "OPTIONS")
	// Profile editing — PATCH /users/{id} accepting any subset of
	// {fullName, bio, visibility, settings}. Authorization happens
	// inside the handler (path-id must match body userId until
	// session auth lands). See profile_handlers.go.
	api.HandleFunc("/users/{id}", authed(UpdateUserProfileHandler)).Methods("PATCH", "OPTIONS")
	// Activity surfaces — paginated list of challenges the user has
	// liked / watched. Cursor-based on the action timestamp so the
	// page stays stable as the user keeps engaging.
	// Paginated social-graph lists for any user (logged-in callers only).
	api.HandleFunc("/users/{id}/followers", authed(GetFollowersHandler)).Methods("GET", "OPTIONS")
	api.HandleFunc("/users/{id}/following", authed(GetFollowingHandler)).Methods("GET", "OPTIONS")
	api.HandleFunc("/users/{id}/likes", authed(GetLikedChallengesHandler)).Methods("GET", "OPTIONS")
	api.HandleFunc("/users/{id}/history", authed(GetWatchHistoryHandler)).Methods("GET", "OPTIONS")
	api.HandleFunc("/users/{id}/history", authed(DeleteWatchHistoryHandler)).Methods("DELETE", "OPTIONS")
	// Block list management. Blocking tears down follow edges in
	// both directions inside one transaction so the recommender
	// stops seeing the blocked user's content on the next page load.
	api.HandleFunc("/blocks", authed(BlockUserHandler)).Methods("POST", "OPTIONS")
	api.HandleFunc("/unblock", authed(UnblockUserHandler)).Methods("POST", "OPTIONS")
	api.HandleFunc("/users/{id}/blocks", authed(ListBlockedUsersHandler)).Methods("GET", "OPTIONS")
	// TOTP-based 2FA. Enroll mints a fresh secret + recovery codes
	// (returned ONCE in plaintext), verify activates the row,
	// disable requires proving knowledge of a current code.
	api.HandleFunc("/users/{id}/totp/enroll", authed(EnrollTOTPHandler)).Methods("POST", "OPTIONS")
	api.HandleFunc("/users/{id}/totp/verify", authed(VerifyTOTPHandler)).Methods("POST", "OPTIONS")
	api.HandleFunc("/users/{id}/totp/disable", authed(DisableTOTPHandler)).Methods("POST", "OPTIONS")
	api.HandleFunc("/experiments", ExperimentsListHandler).Methods("GET", "OPTIONS")
	api.HandleFunc("/experiments/results", authed(ExperimentResultsHandler)).Methods("GET", "OPTIONS")
	api.HandleFunc("/users/similar", authed(SimilarUsersHandler)).Methods("GET", "OPTIONS")
	api.HandleFunc("/users/suggested", authed(SuggestedUsersHandler)).Methods("GET", "OPTIONS")
	api.HandleFunc("/suggestions/accepted", authed(SuggestionAcceptedHandler)).Methods("POST", "OPTIONS")
	api.HandleFunc("/watch", authed(HandleWatchEvent)).Methods("POST", "OPTIONS")
	api.HandleFunc("/report", authed(HandleReportEvent)).Methods("POST", "OPTIONS")
	api.HandleFunc("/admin/reseed", adminOnly(ReseedHandler)).Methods("POST", "OPTIONS")
	api.HandleFunc("/admin/funnels", adminOnly(AdminFunnelsHandler)).Methods("GET", "OPTIONS")
	api.HandleFunc("/admin/errors", adminOnly(AdminErrorsHandler)).Methods("GET", "OPTIONS")
	api.HandleFunc("/admin/health", adminOnly(AdminHealthHandler)).Methods("GET", "OPTIONS")
	api.HandleFunc("/admin/golden_hour", adminOnly(AdminGoldenHourHandler)).Methods("GET", "OPTIONS")
	api.HandleFunc("/admin/online", adminOnly(AdminOnlineUsersHandler)).Methods("GET", "OPTIONS")
	api.HandleFunc("/admin/diagnostics", adminOnly(AdminDiagnosticsHandler)).Methods("GET", "OPTIONS")
	// Global "is the ranking working?" KPI snapshot (completion/skip/engagement/
	// session length, new-content discovery, catalog coverage) with good/watch/bad
	// verdicts. See admin_feed_health.go.
	api.HandleFunc("/admin/feed-health", adminOnly(AdminFeedHealthHandler)).Methods("GET", "OPTIONS")
	// Experiment CRUD: upsert an experiment (or kill one with
	// "active": false) without a redeploy — refresher propagates the
	// change to every replica within 60s.
	api.HandleFunc("/admin/experiments", adminOnly(AdminUpsertExperimentHandler)).Methods("POST", "OPTIONS")

	// Search-page empty state: the caller's recent queries (authed —
	// personal data) and the platform's trending queries (public).
	api.HandleFunc("/search/recent", authed(RecentSearchesHandler)).Methods("GET", "OPTIONS")
	api.HandleFunc("/search/trending", TrendingSearchesHandler).Methods("GET", "OPTIONS")

	// Push notifications: token registration, prefs, click tracking.
	// /unregister and /clicked are intentionally unauthenticated: the push token
	// (resp. notification id) is itself the capability, and both must work during
	// logout / from a tapped system notification when no session is in context.
	api.HandleFunc("/notifications/register", authed(HandleRegisterPushToken)).Methods("POST", "OPTIONS")
	api.HandleFunc("/notifications/unregister", HandleUnregisterPushToken).Methods("POST", "OPTIONS")
	api.HandleFunc("/notifications/prefs", authed(HandleGetNotificationPrefs)).Methods("GET", "OPTIONS")
	api.HandleFunc("/notifications/prefs", authed(HandleSetNotificationPrefs)).Methods("POST", "OPTIONS")
	api.HandleFunc("/notifications/clicked", HandleNotificationClicked).Methods("POST", "OPTIONS")

	// Creator insights — feedback loop for creators to understand reach.
	api.HandleFunc("/creator/insights", authed(HandleCreatorInsightsOverview)).Methods("GET", "OPTIONS")
	api.HandleFunc("/creator/insights/content", authed(HandleCreatorInsightsPerContent)).Methods("GET", "OPTIONS")

	r.HandleFunc("/admin", adminOnly(AdminDashboardHandler)).Methods("GET")
	api.HandleFunc("/chat/send", authed(SendMessageHandler)).Methods("POST", "OPTIONS")
	api.HandleFunc("/chat/conversations/{userId}", authed(GetConversationsHandler)).Methods("GET", "OPTIONS")
	api.HandleFunc("/chat/messages/{userId}/{otherUserId}", authed(GetMessagesHandler)).Methods("GET", "OPTIONS")
	api.HandleFunc("/chat/read", authed(MarkReadHandler)).Methods("POST", "OPTIONS")
	api.HandleFunc("/chat/edit", authed(EditMessageHandler)).Methods("POST", "OPTIONS")
	api.HandleFunc("/chat/delete", authed(DeleteMessageHandler)).Methods("POST", "OPTIONS")
	api.HandleFunc("/chat/forward", authed(ForwardMessageHandler)).Methods("POST", "OPTIONS")
	api.HandleFunc("/chat/online/{username}", OnlineStatusHandler).Methods("GET", "OPTIONS")
	api.HandleFunc("/save", authed(SaveChallengeHandler)).Methods("POST", "OPTIONS")
	api.HandleFunc("/saved/{userId}", authed(GetSavedChallengesHandler)).Methods("GET", "OPTIONS")

	r.HandleFunc("/login", LoginHandler).Methods("POST", "OPTIONS")
	// Registration + live username availability (both public; signup is
	// anonymous-rate-limited via the "signup" action bucket).
	r.HandleFunc("/signup", SignupHandler).Methods("POST", "OPTIONS")
	r.HandleFunc("/signup/available", UsernameAvailableHandler).Methods("GET", "OPTIONS")
	// Token refresh (authed) — active users never hit the 7-day expiry.
	api.HandleFunc("/auth/refresh", authed(RefreshTokenHandler)).Methods("POST", "OPTIONS")
	// Onboarding interest picker → seeds CategoryAffinity for cold start.
	api.HandleFunc("/profile/interests", authed(SeedInterestsHandler)).Methods("POST", "OPTIONS")
	r.HandleFunc("/ws/{username}", WebsocketHandler).Methods("GET")
	r.HandleFunc("/search", SearchHandler).Methods("GET", "OPTIONS")

	// Prometheus metrics endpoint — scrape target.
	r.Handle("/metrics", MetricsHandler()).Methods("GET")

	// Health check endpoint for Render and uptime monitors
	r.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ok"}`))
	}).Methods("GET")

	port := os.Getenv("PORT")
	if port == "" {
		port = "8081"
	}

	srv := &http.Server{
		Addr:         ":" + port,
		Handler:      corsMiddleware(r),
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Graceful shutdown
	go func() {
		log.Printf("Starting server on :%s...\n", port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Could not start server: %s\n", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("Shutting down server...")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Fatalf("Server forced to shutdown: %v", err)
	}
	log.Println("Server exited gracefully")
}
