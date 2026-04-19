package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
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

	// Create the response payload — no longer sending all users for security.
	response := map[string]interface{}{
		"user":     user,
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

// GetAllUsersHandler returns a list of all users.
func GetAllUsersHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	allUsers := GetAllUsers()
	if err := json.NewEncoder(w).Encode(allUsers); err != nil {
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
}

type bucket struct {
	tokens    float64
	lastCheck time.Time
}

func newRateLimiter(rps float64, burst int) *rateLimiter {
	return &rateLimiter{
		buckets:  make(map[string]*bucket),
		rate:     rps,
		capacity: burst,
	}
}

func (rl *rateLimiter) allow(key string) bool {
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
			ip := r.RemoteAddr
			if !limiter.allow(ip) {
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
	startSimilarityWorker()
	startImpressionAggregator()
	startAnalyticsScheduler()
	startLTRFlusher()

	r := mux.NewRouter()

	// Apply rate limiting: 10 requests/sec with burst of 20 per IP
	limiter := newRateLimiter(10, 20)
	r.Use(rateLimitMiddleware(limiter))

	api := r.PathPrefix("/api/v1").Subrouter()
	api.HandleFunc("/users", GetAllUsersHandler).Methods("GET", "OPTIONS")
	api.HandleFunc("/users/{username}", GetUserHandler).Methods("GET", "OPTIONS")
	api.HandleFunc("/follow", HandleFollowEvent).Methods("POST", "OPTIONS")
	api.HandleFunc("/unfollow", HandleUnfollowEvent).Methods("POST", "OPTIONS")
	api.HandleFunc("/feed", FeedHandler).Methods("GET", "OPTIONS")
	api.HandleFunc("/home", HomeFeedHandler).Methods("GET", "OPTIONS")
	api.HandleFunc("/posts/{userId}", UserPostsHandler).Methods("GET", "OPTIONS")
	api.HandleFunc("/like", HandleLikeEvent).Methods("POST", "OPTIONS")
	api.HandleFunc("/comments", HandleCommentEvent).Methods("POST", "OPTIONS")
	api.HandleFunc("/comments/{postId}", GetCommentsHandler).Methods("GET", "OPTIONS")
	api.HandleFunc("/challenges", CreateChallengeHandler).Methods("POST", "OPTIONS")
	api.HandleFunc("/challenges/arena", GetArenaChallengesHandler).Methods("GET", "OPTIONS")
	api.HandleFunc("/challenges/friends", GetFriendsChallengesHandler).Methods("GET", "OPTIONS")
	api.HandleFunc("/challenges/accept", AcceptChallengeHandler).Methods("POST", "OPTIONS")
	api.HandleFunc("/challenges/like", LikeChallengeHandler).Methods("POST", "OPTIONS")
	api.HandleFunc("/challenges/vote", VoteChallengeHandler).Methods("POST", "OPTIONS")
	api.HandleFunc("/challenges/comments", AddChallengeCommentHandler).Methods("POST", "OPTIONS")
	api.HandleFunc("/challenges/responses/{id}/flag", FlagResponseHandler).Methods("POST", "OPTIONS")
	api.HandleFunc("/challenges/{id}/votes", GetVoteResultsHandler).Methods("GET", "OPTIONS")
	api.HandleFunc("/challenges/{id}/comments", GetChallengeCommentsHandler).Methods("GET", "OPTIONS")
	api.HandleFunc("/challenges/{id}", GetChallengeDetailHandler).Methods("GET", "OPTIONS")
	api.HandleFunc("/feed/recommended", RecommendedFeedHandler).Methods("GET", "OPTIONS")
	api.HandleFunc("/feed/following", FollowingFeedHandler).Methods("GET", "OPTIONS")
	// Psychology-based recommendation engine (v2)
	api.HandleFunc("/feed/smart", SmartFeedHandler).Methods("GET", "OPTIONS")
	api.HandleFunc("/feed/following/v2", FollowingFeedV2Handler).Methods("GET", "OPTIONS")
	api.HandleFunc("/categories", CategoriesHandler).Methods("GET", "OPTIONS")
	api.HandleFunc("/events", TrackEventHandler).Methods("POST", "OPTIONS")
	api.HandleFunc("/events/batch", TrackBatchEventsHandler).Methods("POST", "OPTIONS")
	api.HandleFunc("/profile", UserProfileHandler).Methods("GET", "OPTIONS")
	api.HandleFunc("/experiments", ExperimentsListHandler).Methods("GET", "OPTIONS")
	api.HandleFunc("/experiments/results", ExperimentResultsHandler).Methods("GET", "OPTIONS")
	api.HandleFunc("/users/similar", SimilarUsersHandler).Methods("GET", "OPTIONS")
	api.HandleFunc("/watch", HandleWatchEvent).Methods("POST", "OPTIONS")
	api.HandleFunc("/report", HandleReportEvent).Methods("POST", "OPTIONS")
	api.HandleFunc("/admin/reseed", adminOnly(ReseedHandler)).Methods("POST", "OPTIONS")
	api.HandleFunc("/admin/funnels", adminOnly(AdminFunnelsHandler)).Methods("GET", "OPTIONS")
	api.HandleFunc("/admin/errors", adminOnly(AdminErrorsHandler)).Methods("GET", "OPTIONS")
	api.HandleFunc("/admin/health", adminOnly(AdminHealthHandler)).Methods("GET", "OPTIONS")
	api.HandleFunc("/admin/golden_hour", adminOnly(AdminGoldenHourHandler)).Methods("GET", "OPTIONS")
	r.HandleFunc("/admin", adminOnly(AdminDashboardHandler)).Methods("GET")
	api.HandleFunc("/chat/send", SendMessageHandler).Methods("POST", "OPTIONS")
	api.HandleFunc("/chat/conversations/{userId}", GetConversationsHandler).Methods("GET", "OPTIONS")
	api.HandleFunc("/chat/messages/{userId}/{otherUserId}", GetMessagesHandler).Methods("GET", "OPTIONS")
	api.HandleFunc("/chat/read", MarkReadHandler).Methods("POST", "OPTIONS")
	api.HandleFunc("/chat/edit", EditMessageHandler).Methods("POST", "OPTIONS")
	api.HandleFunc("/chat/delete", DeleteMessageHandler).Methods("POST", "OPTIONS")
	api.HandleFunc("/chat/forward", ForwardMessageHandler).Methods("POST", "OPTIONS")
	api.HandleFunc("/chat/online/{username}", OnlineStatusHandler).Methods("GET", "OPTIONS")
	api.HandleFunc("/save", SaveChallengeHandler).Methods("POST", "OPTIONS")
	api.HandleFunc("/saved/{userId}", GetSavedChallengesHandler).Methods("GET", "OPTIONS")

	r.HandleFunc("/login", LoginHandler).Methods("POST", "OPTIONS")
	r.HandleFunc("/ws/{username}", WebsocketHandler).Methods("GET")
	r.HandleFunc("/search", SearchHandler).Methods("GET", "OPTIONS")

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
