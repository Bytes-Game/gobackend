package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

// ════════════════════════════════════════════════════════════════════════════════
// TYPES — The data structures that power the algorithm
// ════════════════════════════════════════════════════════════════════════════════

// FeedEvent captures every meaningful user interaction.
// This is the raw fuel for the entire algorithm — every score is derived from these.
//
// WHY: TikTok's #1 insight was that implicit signals (watch time, skips) are
// more honest than explicit signals (likes). A user might not "like" a video
// but watching it 3 times tells us everything. We capture both.
type FeedEvent struct {
	ID             string  `json:"id"`
	UserID         string  `json:"userId"`
	ContentID      string  `json:"contentId"`
	ContentType    string  `json:"contentType"`    // "post", "challenge", "response"
	EventType      string  `json:"eventType"`      // "view","like","skip","impression","scroll_back","complete","loop","unmute","seek_back","seek_forward","profile_visit","hashtag_tap","follow_from_content","report","block","comment_panel_open","long_press","app_background","app_foreground","video_pause","video_play","battle_switch",etc.
	WatchDurationMs int    `json:"watchDurationMs"` // How long they actually watched (or dwell ms for impressions)
	TotalDurationMs int    `json:"totalDurationMs"` // Total content length
	CompletionRate float64 `json:"completionRate"`  // 0.0 to 1.0
	SessionID      string  `json:"sessionId"`       // Groups events into sessions
	SessionPosition int    `json:"sessionPosition"` // Nth item in this session (fatigue signal)
	Metadata       map[string]interface{} `json:"metadata,omitempty"` // Event-specific extra data
	CreatedAt      string  `json:"createdAt"`
}

// SessionState is the real-time psychological state of a user during a session.
// Stored in Redis with TTL. This is what makes the feed feel "alive" — it adapts
// within a single session, not just between sessions.
//
// WHY: Instagram/TikTok both track session-level fatigue. A user who has been
// scrolling for 20 minutes needs different content than someone who just opened
// the app. Without this, you serve intense content to exhausted users and they leave.
type SessionState struct {
	UserID          string             `json:"userId"`
	SessionID       string             `json:"sessionId"`
	StartedAt       time.Time          `json:"startedAt"`
	ItemsSeen       int                `json:"itemsSeen"`
	TotalWatchMs    int                `json:"totalWatchMs"`
	SkipCount       int                `json:"skipCount"`
	SkipStreak      int                `json:"skipStreak"`      // Consecutive skips — resistance signal
	LikeCount       int                `json:"likeCount"`
	ShareCount      int                `json:"shareCount"`
	CategoriesSeen  map[string]int     `json:"categoriesSeen"`  // category -> count (saturation tracking)
	CreatorsSeen    map[string]int     `json:"creatorsSeen"`    // creatorId -> count (diversity)
	LastEmotions    []string           `json:"lastEmotions"`    // Last 5 content emotions consumed
	DopamineBudget  float64            `json:"dopamineBudget"`  // 1.0=fresh, depletes to 0
	ResistanceLevel int                `json:"resistanceLevel"` // 0-3, triggers strategy switches
	CurrentStrategy string             `json:"currentStrategy"` // see strategy constants below
	// === Impression resistance (new) ===
	ImpressionCount int               `json:"impressionCount"` // Impressions collected this session
	BounceCount     int               `json:"bounceCount"`     // Impressions with dwell < 500ms
	BounceStreak    int               `json:"bounceStreak"`    // Consecutive bounces — early skip signal
	// === Mood & strategy memory (new) ===
	DetectedMood           string    `json:"detectedMood"`           // "energetic","chill","frustrated","bored","engaged","curious"
	TriedStrategies        []string  `json:"triedStrategies"`        // Strategies already used this session — don't repeat failed ones
	StrategyStartItems     int       `json:"strategyStartItems"`     // ItemsSeen when current strategy began
	StrategyStartLikes     int       `json:"strategyStartLikes"`     // LikeCount when current strategy began
	StrategyStartShares    int       `json:"strategyStartShares"`    // ShareCount when current strategy began
	StrategyStartSkips     int       `json:"strategyStartSkips"`     // SkipCount when current strategy began
	LastStrategySwitchAt   time.Time `json:"lastStrategySwitchAt"`   // Prevent thrashing (min 6 items between switches)
	// === Lifecycle tracking ===
	LastActivityAt    time.Time `json:"lastActivityAt"`    // Updated on every event — used to compute true session length
	BackgroundedAt    time.Time `json:"backgroundedAt"`    // Last time user backgrounded the app — zero when foreground
	BackgroundedCount int       `json:"backgroundedCount"` // How many times user has left+returned this session
	// === Cross-surface counters (built from non-feed events) ===
	// These count interactions OUTSIDE the feed so the recommendation engine
	// has visibility into the whole user, not just their video-watching.
	PageViewCount         int            `json:"pageViewCount"`
	TabSwitchCount        int            `json:"tabSwitchCount"`
	ProfileViewCount      int            `json:"profileViewCount"`
	SearchCount           int            `json:"searchCount"`
	ChatOpenCount         int            `json:"chatOpenCount"`
	MessagesSentCount     int            `json:"messagesSentCount"`
	FollowsCount          int            `json:"followsCount"`
	UnfollowsCount        int            `json:"unfollowsCount"`
	NotificationOpenCount int            `json:"notificationOpenCount"`
	SettingChangeCount    int            `json:"settingChangeCount"`
	UploadStartCount      int            `json:"uploadStartCount"`
	UploadCompleteCount   int            `json:"uploadCompleteCount"`
	ErrorCount            int            `json:"errorCount"`
	// Per-page dwell totals (ms) — accumulated from page_exit events
	PageDwellMs map[string]int `json:"pageDwellMs"`
}

// UserProfile is the long-term personality model. Computed from event history,
// stored in PostgreSQL, recalculated when stale (>1 hour or new session).
//
// WHY: This is the "who is this person" layer. TikTok builds user embeddings
// from behavior — we do the same thing with explicit dimensions instead of
// neural network vectors. At our scale, interpretable dimensions > black-box embeddings.
//
// The 5 psychological dimensions:
//  1. EnergyPreference  — Does this user prefer hype/competition or chill/passive?
//  2. SocialDrive       — Do they engage more with friends' content or discover solo?
//  3. NoveltyTolerance  — Do they explore new categories or stick to favorites?
//  4. EgoSensitivity    — How much do wins/losses affect their engagement?
//  5. CategoryAffinity  — What topics do they consistently engage with?
type UserProfile struct {
	UserID            string             `json:"userId"`
	// === Long-term personality (updated every session) ===
	CategoryAffinity  map[string]float64 `json:"categoryAffinity"`  // "comedy":0.4, "skill":0.3
	EnergyPreference  float64            `json:"energyPreference"`  // 0=chill, 1=intense
	SocialDrive       float64            `json:"socialDrive"`       // 0=solo, 1=social
	NoveltyTolerance  float64            `json:"noveltyTolerance"`  // 0=loyalist, 1=explorer
	EgoSensitivity    float64            `json:"egoSensitivity"`    // 0=unbothered, 1=highly reactive
	// === Extended personality (new dimensions) ===
	AttentionSpan        float64 `json:"attentionSpan"`        // 0=scanner/skimmer, 1=deep watcher
	BingeIntensity       float64 `json:"bingeIntensity"`       // 0=casual dipper, 1=binger (long tail sessions)
	CreatorLoyalty       float64 `json:"creatorLoyalty"`       // 0=promiscuous viewer, 1=fan (top creator concentration)
	CompetitivenessIndex float64 `json:"competitivenessIndex"` // 0=spectator, 1=competitor (posts/responds in battles)
	MoodVolatility       float64 `json:"moodVolatility"`       // 0=steady, 1=moody (engagement variance across sessions)
	AvgSessionSec     int                `json:"avgSessionSec"`
	ActiveHours       []int              `json:"activeHours"`       // Peak engagement hours [19,20,21]
	PreferredCreators []string           `json:"preferredCreators"` // Top 10 creator IDs
	AvoidedCategories []string           `json:"avoidedCategories"` // Categories they skip/not-interested
	// === Behavioral metrics ===
	AvgCompletionRate float64            `json:"avgCompletionRate"`
	AvgSkipRate       float64            `json:"avgSkipRate"`
	TotalSessions     int                `json:"totalSessions"`
	TotalWatchTimeMs  int64              `json:"totalWatchTimeMs"`
	// === Ego state (from battle results) ===
	RecentWins        int                `json:"recentWins"`   // Last 7 days
	RecentLosses      int                `json:"recentLosses"` // Last 7 days
	// === Context-aware personality ===
	CategoryByHour    map[int]string              `json:"categoryByHour"`    // hour→top category at that hour
	CategoryByEgo     map[string]map[string]float64 `json:"categoryByEgo"`   // "winning"/"losing"→{category:score}
	EmotionPreference map[string]float64          `json:"emotionPreference"` // "happy":0.6, "intense":0.3
	EnergyByHour      map[int]float64             `json:"energyByHour"`      // hour→avg energy preference
	// === Strategy success memory (new) ===
	// Per-user track record of how well each strategy worked when deployed.
	// Updated at strategy-switch time based on engagement delta (likes/shares/completes
	// minus skips) relative to items seen under that strategy. Values in [-1, 1].
	// This is how we avoid retrying a strategy that never works for THIS user.
	StrategySuccessHistory map[string]float64 `json:"strategySuccessHistory"`
	// === Metadata ===
	LastComputedAt    time.Time          `json:"lastComputedAt"`
	EventCount        int                `json:"eventCount"` // Total events used to compute
}

// ContentScore is the computed understanding of a piece of content.
// Derived from how ALL users interact with it, not just one user.
//
// WHY: Content understanding is separate from user preference. A comedy video
// is a comedy video regardless of who's watching. We compute this once and
// cache it, then the scoring engine combines it with the user profile.
type ContentScore struct {
	ContentID         string             `json:"contentId"`
	ContentType       string             `json:"contentType"`
	// === Engagement metrics (from all users) ===
	AvgCompletionRate float64            `json:"avgCompletionRate"`
	AvgWatchTimeMs    int                `json:"avgWatchTimeMs"`
	SkipRate          float64            `json:"skipRate"`
	RewatchRate       float64            `json:"rewatchRate"`
	ShareCount        int                `json:"shareCount"`
	NotInterestedCount int               `json:"notInterestedCount"`
	// === Derived scores ===
	EngagementVelocity float64           `json:"engagementVelocity"` // Engagement in first 2 hours
	TrendingScore      float64           `json:"trendingScore"`
	QualityScore       float64           `json:"qualityScore"`
	// === Content understanding ===
	EnergyLevel        float64           `json:"energyLevel"`   // 0=chill, 1=intense
	Category           string            `json:"category"`      // Primary category
	EmotionVector      map[string]float64 `json:"emotionVector"` // "happy":0.5, "competitive":0.3
	// === Creator info (denormalized for speed) ===
	CreatorID          string            `json:"creatorId"`
	CreatorLeague      string            `json:"creatorLeague"`
	CreatorFollowers   int               `json:"creatorFollowers"`
	CreatorWinRate     float64           `json:"creatorWinRate"`
	// === Metadata ===
	CreatedAt          time.Time         `json:"createdAt"`
	ViewCount          int               `json:"viewCount"`
	LikeCount          int               `json:"likeCount"`
	CommentCount       int               `json:"commentCount"`
	LastComputedAt     time.Time         `json:"lastComputedAt"`
}

// ScoredItem wraps a feed item with its computed score and assigned slot.
type ScoredItem struct {
	Item     HomeFeedItem `json:"item"`
	Score    float64      `json:"score"`
	SlotType string       `json:"slotType"` // "hook","social","discovery","trending","challenge","cooldown","ego_boost"
	// Debug info (only included when ?debug=true)
	ScoreBreakdown map[string]float64 `json:"scoreBreakdown,omitempty"`
}

// ════════════════════════════════════════════════════════════════════════════════
// CONSTANTS — Tuning knobs for the algorithm
// ════════════════════════════════════════════════════════════════════════════════
//
// WHY these specific values:
// - Weights sum to ~1.0 for the base score
// - Social is highest (0.25) because at small scale, social proof > algorithmic guess
// - Freshness is high (0.20) to give new content fair chance — prevents stale feeds
// - Energy fit is new and unique to us (0.20) — matches content intensity to user state
// - Relevance (0.15) is lower than you'd expect because at low data, overfitting to
//   categories creates filter bubbles fast
// - Quality (0.10) rewards good creators but doesn't dominate
// - Novelty (0.10) forces exploration to prevent echo chambers

const (
	// === Base score weights (sum = 1.0) ===
	wSocial     = 0.25  // Friends/following boost
	wFreshness  = 0.20  // Time decay — new content gets a chance
	wEnergyFit  = 0.20  // Does content intensity match user's current state?
	wRelevance  = 0.15  // Category/subject match
	wQuality    = 0.10  // Creator reputation
	wNovelty    = 0.10  // New category/creator bonus

	// === Freshness decay ===
	freshnessHalfLifeHours = 18.0  // Content loses half its freshness every 18h
	                                // WHY 18 not 12: our content is challenges, not memes.
	                                // Challenges take longer to discover and respond to.

	// === Session dynamics ===
	dopamineDepletionRate  = 0.03  // Budget drops 3% per item viewed
	                                // WHY: At 30 items, budget = 0.1 (fatigued). Average TikTok
	                                // session is 10-15 minutes ≈ 30-50 items. We want to detect
	                                // fatigue around the same threshold.

	maxItemsPerCreator     = 3     // Diversity: max 3 items from same creator in one feed page
	coldStartThreshold     = 15    // Users with <15 events are "cold start"
	contentColdThreshold   = 5     // Content with <5 views is "cold start"
	profileStalenessMin    = 60    // Recompute profile if older than 60 minutes
	sessionTTLMin          = 30    // Redis session expires after 30 min inactivity

	// === Resistance thresholds ===
	resistL1SkipRate = 0.30  // 30% skips = drifting
	resistL2SkipRate = 0.50  // 50% skips = resisting
	resistL3SkipRate = 0.70  // 70% skips = leaving
	resistL2SkipStreak = 5   // 5 consecutive skips = definitely resisting

	// === Impression-based resistance (earlier signal than skips) ===
	// Bounces are sub-500ms impressions — user zipped past without committing.
	// These fire before the user bothers to issue a "skip" event, catching
	// disengagement ~10s earlier.
	resistBounceRateL1    = 0.50  // 50% bounce rate = drifting
	resistBounceRateL2    = 0.70  // 70% bounce rate = resisting
	resistBounceRateL3    = 0.85  // 85% bounce rate = leaving
	resistBounceStreakL2  = 6     // 6 consecutive bounces = resisting
	resistBounceMinSample = 8     // Need at least 8 impressions before trusting bounce rate

	// === Strategy switching governance ===
	minItemsBetweenSwitches = 6    // Don't thrash strategies — give each a fair shot

	// === Feed page size ===
	defaultPageSize = 20
	maxPageSize     = 50
	candidateMultiplier = 5 // Fetch 5x page size as candidates, then score & filter

	// === Cold start content test audience ===
	coldContentTestSize = 50 // Show new content to ~50 random users to measure response
)

// Feed slot types — define what KIND of content goes in each position.
// Each slot serves a psychological purpose in the session arc.
const (
	slotHook      = "hook"       // High-confidence content user will engage with
	slotSocial    = "social"     // Content from friends/following
	slotDiscovery = "discovery"  // New category or creator — expand taste
	slotTrending  = "trending"   // Popular right now — social proof
	slotChallenge = "challenge"  // Battle/competition content
	slotCooldown  = "cooldown"   // Lower energy — palette cleanser
	slotEgoBoost  = "ego_boost"  // Content that validates the user
	slotCliffhang = "cliffhang"  // Content with suspense/sequel potential — "what happens next?"
	slotSurprise  = "surprise"   // Wildcard — random highly-engaging content from any category
	slotRival     = "rival"      // Content from someone they lost to or competed with — drives re-engagement
	slotNostalgic = "nostalgic"  // Content user previously loved — replay their own greatest hits
	slotFavCreator = "fav_creator" // Top preferred creators' content
	slotFreshBlood = "fresh_blood" // Brand-new content user has never seen in any feed
)

// Strategy names (centralized — previously strings were scattered).
const (
	strategyStandard     = "standard"
	strategySocial       = "social"
	strategyTrending     = "trending"
	strategyDiscovery    = "discovery"
	strategyEmergency    = "emergency"
	strategyCalming      = "calming"       // low-energy wind-down for fatigued/frustrated users
	strategyCompetitive  = "competitive"   // battle/challenge heavy for competitive personalities
	strategyCreatorFocus = "creator_focus" // deep dive on preferred creators (loyalists)
	strategyNostalgic    = "nostalgic"     // replay historical favorites (last-resort re-engagement)
	strategyFreshBlood   = "fresh_blood"   // only brand-new content (bored explorers)
	strategyMoodMatch    = "mood_match"    // slot pattern fully driven by detected mood
)

// ════════════════════════════════════════════════════════════════════════════════
// LAYER 1: EVENT COLLECTION
// ════════════════════════════════════════════════════════════════════════════════
//
// WHY this matters:
// Every recommendation system lives or dies by its event data. Without events,
// you're guessing. TikTok's advantage isn't their algorithm — it's that they
// capture EVERY micro-interaction (pause, rewatch, scroll speed, hover time).
// We capture the practical subset that works without client-side ML.

// TrackEventHandler receives user interaction events from the Flutter client.
// POST /api/v1/events
//
// The client sends events for: view, like, unlike, comment, share, save,
// unsave, skip, not_interested, rewatch — with watch duration and session info.
func TrackEventHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "OPTIONS" {
		w.WriteHeader(http.StatusOK)
		return
	}

	var event FeedEvent
	if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
		http.Error(w, `{"error":"invalid event payload"}`, http.StatusBadRequest)
		return
	}

	// Lifecycle events legitimately have no contentId (the user wasn't watching
	// anything when they backgrounded/returned). Allow those past the check.
	if event.UserID == "" || event.EventType == "" {
		http.Error(w, `{"error":"userId and eventType are required"}`, http.StatusBadRequest)
		return
	}
	if event.ContentID == "" && !isLifecycleEvent(event.EventType) {
		http.Error(w, `{"error":"contentId is required for non-lifecycle events"}`, http.StatusBadRequest)
		return
	}

	// Default session ID if not provided
	if event.SessionID == "" {
		event.SessionID = fmt.Sprintf("%s_%d", event.UserID, time.Now().Unix()/1800) // 30-min windows
	}

	// Compute completion rate if not provided
	if event.CompletionRate == 0 && event.TotalDurationMs > 0 && event.WatchDurationMs > 0 {
		event.CompletionRate = math.Min(float64(event.WatchDurationMs)/float64(event.TotalDurationMs), 1.0)
	}

	// Store in PostgreSQL
	go func() {
		if err := recordFeedEvent(event); err != nil {
			log.Printf("Failed to record feed event: %v", err)
		}
	}()

	// Update session state in Redis (real-time)
	go func() {
		updateSessionFromEvent(event)
	}()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

// TrackBatchEventsHandler receives multiple events at once.
// POST /api/v1/events/batch
//
// WHY batch: Mobile networks are unreliable. The Flutter client batches events
// and sends them when connection is stable, instead of one HTTP request per scroll.
func TrackBatchEventsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "OPTIONS" {
		w.WriteHeader(http.StatusOK)
		return
	}

	var payload struct {
		Events []FeedEvent `json:"events"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, `{"error":"invalid payload"}`, http.StatusBadRequest)
		return
	}

	go func() {
		for _, event := range payload.Events {
			if event.SessionID == "" {
				event.SessionID = fmt.Sprintf("%s_%d", event.UserID, time.Now().Unix()/1800)
			}
			if event.CompletionRate == 0 && event.TotalDurationMs > 0 && event.WatchDurationMs > 0 {
				event.CompletionRate = math.Min(float64(event.WatchDurationMs)/float64(event.TotalDurationMs), 1.0)
			}
			if err := recordFeedEvent(event); err != nil {
				log.Printf("Failed to record batch event: %v", err)
			}
			updateSessionFromEvent(event)
		}
	}()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":   "ok",
		"received": len(payload.Events),
	})
}

func recordFeedEvent(event FeedEvent) error {
	// Impressions are high-volume — route them to Redis aggregator instead of PostgreSQL.
	// This prevents billions of rows from piling up while still capturing the signal.
	if event.EventType == "impression" {
		go recordImpression(event)
		return nil
	}

	var metadataJSON []byte
	if len(event.Metadata) > 0 {
		metadataJSON, _ = json.Marshal(event.Metadata)
	}

	_, err := db.Exec(`
		INSERT INTO feed_events (user_id, content_id, content_type, event_type,
			watch_duration_ms, total_duration_ms, completion_rate,
			session_id, session_position, metadata)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
		event.UserID, event.ContentID, event.ContentType, event.EventType,
		event.WatchDurationMs, event.TotalDurationMs, event.CompletionRate,
		event.SessionID, event.SessionPosition, metadataJSON,
	)
	return err
}

// ════════════════════════════════════════════════════════════════════════════════
// LAYER 2: SESSION STATE (Redis — real-time, ephemeral)
// ════════════════════════════════════════════════════════════════════════════════
//
// WHY Redis and not PostgreSQL:
// Session state changes with EVERY interaction (every scroll, every skip).
// That's potentially 50+ writes per minute per active user. PostgreSQL would
// choke on this write volume. Redis handles it effortlessly with O(1) writes
// and automatic TTL expiry.
//
// WHY session state matters:
// This is the "dopamine budget" concept. A fresh user (just opened app) should
// see strong hooks. A fatigued user (scrolled 30 items) needs either lighter
// content or to be gracefully released. Without this, you burn out users and
// they associate your app with exhaustion.

func getSessionState(userID, sessionID string) *SessionState {
	key := fmt.Sprintf("session:%s:%s", userID, sessionID)
	data, err := rdb.Get(rctx, key).Result()
	if err != nil {
		// New session — start fresh with full dopamine budget
		return &SessionState{
			UserID:          userID,
			SessionID:       sessionID,
			StartedAt:       time.Now(),
			DopamineBudget:  1.0,
			CategoriesSeen:  make(map[string]int),
			CreatorsSeen:    make(map[string]int),
			LastEmotions:    []string{},
			CurrentStrategy: strategyStandard,
			TriedStrategies: []string{strategyStandard},
		}
	}

	var state SessionState
	if json.Unmarshal([]byte(data), &state) != nil {
		return &SessionState{
			UserID:         userID,
			SessionID:      sessionID,
			StartedAt:      time.Now(),
			DopamineBudget: 1.0,
			CategoriesSeen: make(map[string]int),
			CreatorsSeen:   make(map[string]int),
			LastEmotions:   []string{},
			CurrentStrategy: strategyStandard,
			TriedStrategies: []string{strategyStandard},
		}
	}
	return &state
}

func saveSessionState(state *SessionState) {
	key := fmt.Sprintf("session:%s:%s", state.UserID, state.SessionID)
	data, err := json.Marshal(state)
	if err != nil {
		return
	}
	rdb.Set(rctx, key, data, time.Duration(sessionTTLMin)*time.Minute)
}

// updateSessionFromEvent processes a single event and updates the session state.
// This is where the psychology happens in real-time:
// - Dopamine depletes with each item
// - Skip streaks trigger resistance detection
// - Category saturation is tracked
// - Mood is inferred from recent content emotions
// isLifecycleEvent returns true for events that describe app lifecycle (entry/exit)
// rather than content interaction. These shouldn't bump ItemsSeen or deplete dopamine.
func isLifecycleEvent(eventType string) bool {
	return eventType == "app_background" || eventType == "app_foreground" ||
		eventType == "session_start" || eventType == "session_end"
}

// isMetaEvent returns true for events that describe app interactions that are
// NOT content consumption (page navigation, taps, search, settings, errors).
// These are recorded for the recommendation engine to use as soft signals,
// but they MUST NOT deplete the dopamine budget or bump ItemsSeen — those
// counters are reserved for actual content the user was shown.
//
// We still update specific counters per event type so the user profile can
// derive cross-surface signals like SocialDrive (chat opens, follows) and
// UploadIntent (upload_start without complete = abandoned).
func isMetaEvent(eventType string) bool {
	switch eventType {
	case "page_view", "page_exit",
		"tap", "swipe", "list_scroll_depth",
		"tab_switch",
		"search_query", "search_result_tap", "search_abandoned",
		"profile_view",
		"follow", "unfollow",
		"chat_open", "message_sent", "messages_read",
		"notification_panel_open", "notification_tap",
		"upload_start", "upload_step", "upload_abandon", "upload_complete",
		"setting_change",
		"permission_granted", "permission_denied",
		"error", "perf":
		return true
	}
	return false
}

// applyMetaEventCounters updates the cross-surface counters on the session
// state based on the event type. Called for meta events only — the actual
// dopamine/ItemsSeen accounting is bypassed for these.
func applyMetaEventCounters(state *SessionState, event FeedEvent) {
	switch event.EventType {
	case "page_view":
		state.PageViewCount++
	case "page_exit":
		// Accumulate per-page dwell so we can later derive which surfaces
		// the user actually spends time on (vs. just bouncing through).
		if state.PageDwellMs == nil {
			state.PageDwellMs = make(map[string]int)
		}
		page := event.ContentID // page name is stored in contentId for these
		if page != "" {
			state.PageDwellMs[page] += event.WatchDurationMs
		}
	case "tab_switch":
		state.TabSwitchCount++
	case "profile_view":
		state.ProfileViewCount++
	case "search_query":
		state.SearchCount++
	case "chat_open":
		state.ChatOpenCount++
	case "message_sent":
		state.MessagesSentCount++
	case "follow":
		state.FollowsCount++
	case "unfollow":
		state.UnfollowsCount++
	case "notification_panel_open":
		state.NotificationOpenCount++
	case "setting_change":
		state.SettingChangeCount++
	case "upload_start":
		state.UploadStartCount++
	case "upload_complete":
		state.UploadCompleteCount++
	case "error":
		state.ErrorCount++
	}
}

// persistSessionLength records the just-ended session's duration into the
// long-term user_profiles row using a true running average:
//
//	new_avg = (old_avg * old_total + this_session) / (old_total + 1)
//
// Idempotent within a session: a Redis guard key ensures we only persist once
// per SessionID even if the user backgrounds → foregrounds → backgrounds again.
//
// We deliberately do NOT call full saveUserProfile here — that would race with
// computeUserProfile recomputations. We touch only the two columns we know about.
func persistSessionLength(state *SessionState) {
	if state.StartedAt.IsZero() || state.LastActivityAt.IsZero() {
		return
	}
	durationSec := int(state.LastActivityAt.Sub(state.StartedAt).Seconds())
	if durationSec < 5 {
		return // Too short — likely noise (app opened then immediately backgrounded)
	}

	// Idempotency guard — only persist this session once even if user
	// backgrounds repeatedly. Redis SET NX with 24h TTL is enough.
	if rdb != nil {
		guardKey := fmt.Sprintf("session:persisted:%s", state.SessionID)
		ok, _ := rdb.SetNX(rctx, guardKey, "1", 24*time.Hour).Result()
		if !ok {
			return
		}
	}

	if db == nil {
		return
	}
	// Upsert: if no profile row exists yet, seed it with this session.
	// Otherwise blend into the running average.
	_, err := db.Exec(`
		INSERT INTO user_profiles (user_id, avg_session_sec, total_sessions, last_computed_at)
		VALUES ($1, $2, 1, NOW())
		ON CONFLICT (user_id) DO UPDATE SET
			avg_session_sec = ((user_profiles.avg_session_sec * user_profiles.total_sessions) + $2)
			                  / (user_profiles.total_sessions + 1),
			total_sessions  = user_profiles.total_sessions + 1
	`, state.UserID, durationSec)
	if err != nil {
		log.Printf("persistSessionLength: %v", err)
	}
}

func updateSessionFromEvent(event FeedEvent) {
	state := getSessionState(event.UserID, event.SessionID)

	now := time.Now()
	state.LastActivityAt = now

	// Lifecycle events get their own handling — they don't represent
	// content consumption, so they must NOT bump ItemsSeen or deplete dopamine.
	if isLifecycleEvent(event.EventType) {
		switch event.EventType {
		case "app_background", "session_end":
			// User is leaving — record session length to long-term profile,
			// mark session as paused so subsequent foreground can compute "away time".
			state.BackgroundedAt = now
			persistSessionLength(state)
		case "app_foreground":
			// User came back. If they were backgrounded, count it.
			// Note: if they were away > sessionTimeout (30 min), the client rotates
			// the SessionID so we'll see this as a brand-new SessionState (full
			// dopamine budget). Within timeout, we reuse this state — no penalty
			// for stepping away briefly.
			if !state.BackgroundedAt.IsZero() {
				state.BackgroundedCount++
				state.BackgroundedAt = time.Time{}
			}
		}
		saveSessionState(state)
		return
	}

	// Meta events (page nav, taps, search, settings, errors) are recorded into
	// cross-surface counters but DO NOT count as content consumption. The
	// dopamine budget is reserved for what the user was actually shown in the
	// feed — otherwise opening settings would burn the same energy as watching
	// a video, which is wrong.
	if isMetaEvent(event.EventType) {
		applyMetaEventCounters(state, event)
		saveSessionState(state)
		return
	}

	state.ItemsSeen++
	state.TotalWatchMs += event.WatchDurationMs

	// Deplete dopamine budget — each item costs attention energy
	state.DopamineBudget = math.Max(0, state.DopamineBudget-dopamineDepletionRate)

	switch event.EventType {
	case "skip", "not_interested":
		state.SkipCount++
		state.SkipStreak++
	case "like", "share", "save", "comment":
		state.SkipStreak = 0 // Positive engagement resets skip streak
		if event.EventType == "like" {
			state.LikeCount++
		}
		if event.EventType == "share" {
			state.ShareCount++
		}
		// Positive engagement gives small dopamine refill
		state.DopamineBudget = math.Min(1.0, state.DopamineBudget+0.02)
	case "rewatch", "loop":
		state.SkipStreak = 0
		// Rewatching / looping means content really resonated — dopamine boost
		state.DopamineBudget = math.Min(1.0, state.DopamineBudget+0.05)
	case "scroll_back":
		state.SkipStreak = 0
		// Scrolling back = user actively sought out content → very strong positive
		state.DopamineBudget = math.Min(1.0, state.DopamineBudget+0.08)
	case "complete":
		state.SkipStreak = 0
		state.DopamineBudget = math.Min(1.0, state.DopamineBudget+0.04)
	case "unmute":
		state.SkipStreak = 0
		state.DopamineBudget = math.Min(1.0, state.DopamineBudget+0.03)
	case "profile_visit", "follow_from_content":
		state.SkipStreak = 0
		// User is investigating the creator — deep engagement
		state.DopamineBudget = math.Min(1.0, state.DopamineBudget+0.04)
	case "seek_back":
		state.SkipStreak = 0
		// Rewinding to rewatch a moment = strong interest
		state.DopamineBudget = math.Min(1.0, state.DopamineBudget+0.03)
	case "report", "block":
		// Strong negative — treat as multiple skips
		state.SkipCount += 3
		state.SkipStreak += 2
	case "view":
		if event.CompletionRate > 0.8 {
			state.SkipStreak = 0 // Watching most of content = not skipping
		}
	}

	// Track content emotions for wellbeing spiral detection
	if event.EventType == "view" && event.CompletionRate > 0.5 {
		// Only track emotions for content they actually watched
		emotions := getContentEmotions(event.ContentID, event.ContentType)
		for _, e := range emotions {
			state.LastEmotions = append(state.LastEmotions, e)
		}
		// Keep only last 10 emotions
		if len(state.LastEmotions) > 10 {
			state.LastEmotions = state.LastEmotions[len(state.LastEmotions)-10:]
		}
	}

	// Track category saturation
	if event.EventType == "view" {
		cat := getContentCategory(event.ContentID, event.ContentType)
		if cat != "" {
			if state.CategoriesSeen == nil {
				state.CategoriesSeen = make(map[string]int)
			}
			state.CategoriesSeen[cat]++
		}
		// Track creator diversity
		creator := getContentCreator(event.ContentID, event.ContentType)
		if creator != "" {
			if state.CreatorsSeen == nil {
				state.CreatorsSeen = make(map[string]int)
			}
			state.CreatorsSeen[creator]++
		}
	}

	// "Second wind" mechanic: if dopamine is critically low but user just engaged
	// positively (like/share/rewatch), give a bigger refill to extend the session.
	// This mimics the "one more episode" effect — just when you're about to quit,
	// something great pulls you back.
	if state.DopamineBudget < 0.15 && state.DopamineBudget > 0 {
		switch event.EventType {
		case "like", "share", "rewatch":
			state.DopamineBudget = math.Min(1.0, state.DopamineBudget+0.12) // Big refill
		}
	}

	// Detect resistance level from session patterns (skips + bounces)
	state.ResistanceLevel = detectResistance(state)

	// Detect current mood from recent activity (drives mood_match strategy + slot tweaks)
	state.DetectedMood = detectMood(state)

	// Auto-switch strategy when resistance detected, governed by cooldown so we
	// don't thrash through strategies mid-session.
	if state.ResistanceLevel >= 2 &&
		(state.ItemsSeen-state.StrategyStartItems) >= minItemsBetweenSwitches {
		profile, _ := loadUserProfile(state.UserID)
		newStrat := pickAlternateStrategy(state, profile)
		if newStrat != "" && newStrat != state.CurrentStrategy {
			// Record how the previous strategy performed before switching
			recordStrategyOutcome(state, profile)
			switchStrategy(state, newStrat)
		}
	}

	saveSessionState(state)
}

// switchStrategy rotates the session to a new strategy and resets the
// window-metrics so the NEXT strategy's success can be measured cleanly.
func switchStrategy(state *SessionState, newStrategy string) {
	state.CurrentStrategy = newStrategy
	state.LastStrategySwitchAt = time.Now()
	state.StrategyStartItems = state.ItemsSeen
	state.StrategyStartLikes = state.LikeCount
	state.StrategyStartShares = state.ShareCount
	state.StrategyStartSkips = state.SkipCount
	// Skip streak resets — fresh slate for the new strategy
	state.SkipStreak = 0
	state.BounceStreak = 0
	// Remember we tried this strategy so the picker doesn't re-suggest it
	already := false
	for _, s := range state.TriedStrategies {
		if s == newStrategy {
			already = true
			break
		}
	}
	if !already {
		state.TriedStrategies = append(state.TriedStrategies, newStrategy)
	}
}

// recordStrategyOutcome measures how the current strategy performed during its
// run and writes the score into the user's long-term StrategySuccessHistory.
// Score is (engagement delta - skip delta) / items seen under this strategy,
// clamped to [-1, 1]. Persisted so future sessions prefer strategies that
// worked before for THIS specific user.
func recordStrategyOutcome(state *SessionState, profile *UserProfile) {
	if profile == nil || state.CurrentStrategy == "" {
		return
	}
	items := state.ItemsSeen - state.StrategyStartItems
	if items < 3 {
		return // Not enough signal
	}
	likes := state.LikeCount - state.StrategyStartLikes
	shares := state.ShareCount - state.StrategyStartShares
	skips := state.SkipCount - state.StrategyStartSkips
	// Weighted: shares count most (intent), likes next, skips subtract
	delta := (float64(shares)*1.5 + float64(likes) - float64(skips)*0.8) / float64(items)
	if delta > 1.0 {
		delta = 1.0
	} else if delta < -1.0 {
		delta = -1.0
	}
	if profile.StrategySuccessHistory == nil {
		profile.StrategySuccessHistory = make(map[string]float64)
	}
	// Exponential moving average — blend new sample with history (weight 0.3 for new)
	prev, ok := profile.StrategySuccessHistory[state.CurrentStrategy]
	if !ok {
		profile.StrategySuccessHistory[state.CurrentStrategy] = delta
	} else {
		profile.StrategySuccessHistory[state.CurrentStrategy] = prev*0.7 + delta*0.3
	}
	saveUserProfile(profile)
}

// detectResistance analyzes session patterns to determine if the user is
// rejecting our recommendations.
//
// WHY 4 levels:
// Level 0 (ENGAGED)   — Algorithm is working, keep going
// Level 1 (DRIFTING)  — Starting to lose them, increase variety
// Level 2 (RESISTING) — Actively rejecting, switch strategy entirely
// Level 3 (LEAVING)   — About to close app, emergency or let go gracefully
//
// This is how YouTube's "satisfaction" metric works internally — they detect
// when recommendations aren't landing and adjust mid-session.
func detectResistance(state *SessionState) int {
	if state.ItemsSeen <= 0 && state.ImpressionCount <= 0 {
		return 0
	}

	// ─── Impression-based (EARLY) resistance ─────────────────────────
	// Bounces fire before the user commits to a "skip" event — this catches
	// disengagement ~10s faster than the skip-only path.
	if state.ImpressionCount >= resistBounceMinSample {
		bounceRate := float64(state.BounceCount) / float64(state.ImpressionCount)
		if state.BounceStreak >= 10 {
			return 3
		}
		if bounceRate >= resistBounceRateL3 {
			return 3
		}
		if state.BounceStreak >= resistBounceStreakL2 || bounceRate >= resistBounceRateL2 {
			return 2
		}
		if bounceRate >= resistBounceRateL1 {
			return 1
		}
	}

	// ─── Skip-based (LATE) resistance ────────────────────────────────
	if state.ItemsSeen < 3 {
		return 0
	}
	skipRate := float64(state.SkipCount) / float64(state.ItemsSeen)

	// Skip streak is a stronger signal than overall skip rate
	if state.SkipStreak >= 8 {
		return 3
	}
	if state.SkipStreak >= resistL2SkipStreak {
		return 2
	}

	if skipRate >= resistL3SkipRate {
		return 3
	}
	if skipRate >= resistL2SkipRate {
		return 2
	}
	if skipRate >= resistL1SkipRate {
		return 1
	}
	return 0
}

// detectMood infers the user's current emotional state from recent engagement
// patterns. Drives the mood_match strategy and the mood-tinted slot tweaks.
//
// Six moods mapped from observable behavior (no introspection required):
//   energetic  — fast positive engagement, many likes/shares on intense content
//   chill      — slow dwells on calm content, low skip rate
//   frustrated — high skip + bounce, minimal positive engagement
//   bored      — medium dwell, no engagement, no skips either (just scrolling)
//   engaged    — completions + likes, strong consistent engagement
//   curious    — long impression dwells but few watches (scanning thumbnails)
func detectMood(state *SessionState) string {
	if state.ItemsSeen+state.ImpressionCount < 3 {
		return "" // Too early
	}

	bounceRate := 0.0
	if state.ImpressionCount > 0 {
		bounceRate = float64(state.BounceCount) / float64(state.ImpressionCount)
	}
	skipRate := 0.0
	if state.ItemsSeen > 0 {
		skipRate = float64(state.SkipCount) / float64(state.ItemsSeen)
	}
	engagementPerItem := 0.0
	if state.ItemsSeen > 0 {
		engagementPerItem = float64(state.LikeCount+state.ShareCount) / float64(state.ItemsSeen)
	}

	// Frustrated takes precedence — leave fast if true
	if skipRate > 0.5 || bounceRate > 0.7 {
		return "frustrated"
	}
	// Strong positive engagement → engaged
	if engagementPerItem > 0.3 {
		return "engaged"
	}
	// Energetic: fast positive responses on intense content
	if engagementPerItem > 0.15 && state.DopamineBudget > 0.6 && state.ShareCount > 0 {
		return "energetic"
	}
	// Curious: lots of impressions but few watches committed
	if state.ImpressionCount > state.ItemsSeen*2 && bounceRate < 0.3 {
		return "curious"
	}
	// Chill: slow pace, low skip, low engagement — calm consumption
	if skipRate < 0.1 && engagementPerItem < 0.1 && state.DopamineBudget > 0.4 {
		return "chill"
	}
	// Bored: scrolling without reacting
	return "bored"
}

// pickAlternateStrategy chooses a new feed strategy when the current one fails.
// Considers (in priority order):
//   1. Strategies NOT yet tried this session
//   2. The user's StrategySuccessHistory (prefer what worked before for THIS user)
//   3. Personality fit (competitive user → competitive, loyalist → creator_focus)
//   4. Detected mood (frustrated → calming, bored → fresh_blood, etc.)
//   5. Fallback tier — standard category-shift strategies
func pickAlternateStrategy(state *SessionState, profile *UserProfile) string {
	tried := make(map[string]bool, len(state.TriedStrategies))
	for _, s := range state.TriedStrategies {
		tried[s] = true
	}
	untried := func(s string) bool { return !tried[s] }

	// Level-3 "leaving" users get emergency regardless
	if state.ResistanceLevel >= 3 && untried(strategyEmergency) {
		return strategyEmergency
	}

	// Build candidate pool based on profile + mood
	var candidates []string

	// Mood-driven candidates (highest priority when mood is clear)
	switch state.DetectedMood {
	case "frustrated":
		candidates = append(candidates, strategyCalming, strategyNostalgic)
	case "bored":
		candidates = append(candidates, strategyFreshBlood, strategyDiscovery)
	case "energetic":
		candidates = append(candidates, strategyCompetitive, strategyTrending)
	case "chill":
		candidates = append(candidates, strategyCalming, strategySocial)
	case "curious":
		candidates = append(candidates, strategyDiscovery, strategyFreshBlood)
	}

	// Personality-driven candidates
	if profile != nil {
		if profile.CompetitivenessIndex > 0.6 {
			candidates = append(candidates, strategyCompetitive)
		}
		if profile.CreatorLoyalty > 0.6 {
			candidates = append(candidates, strategyCreatorFocus)
		}
		if profile.NoveltyTolerance > 0.7 {
			candidates = append(candidates, strategyFreshBlood, strategyDiscovery)
		} else if profile.NoveltyTolerance < 0.3 {
			// Loyalists prefer proven: nostalgic + creator focus
			candidates = append(candidates, strategyNostalgic, strategyCreatorFocus)
		}
		if profile.SocialDrive > 0.6 {
			candidates = append(candidates, strategySocial)
		}
		if profile.EnergyPreference < 0.4 {
			candidates = append(candidates, strategyCalming)
		}
	}

	// Fallback pool — universal alternatives
	candidates = append(candidates,
		strategySocial, strategyTrending, strategyDiscovery,
		strategyMoodMatch, strategyCalming, strategyFreshBlood,
	)

	// Filter to untried
	filtered := make([]string, 0, len(candidates))
	for _, c := range candidates {
		if untried(c) {
			filtered = append(filtered, c)
		}
	}
	if len(filtered) == 0 {
		// Exhausted everything — reset history and pick the historically best
		filtered = []string{strategySocial, strategyTrending, strategyDiscovery,
			strategyCalming, strategyFreshBlood, strategyNostalgic, strategyMoodMatch}
	}

	// Rank by StrategySuccessHistory (if available) — prefer what worked before
	if profile != nil && len(profile.StrategySuccessHistory) > 0 {
		bestScore := -2.0 // below min possible
		best := ""
		for _, c := range filtered {
			score, ok := profile.StrategySuccessHistory[c]
			if !ok {
				score = 0.0 // Never tried = neutral (beats known-bad)
			}
			if score > bestScore {
				bestScore = score
				best = c
			}
		}
		if best != "" {
			return best
		}
	}

	// No history — pick first candidate (already ordered by mood/personality priority)
	return filtered[0]
}

// ════════════════════════════════════════════════════════════════════════════════
// LAYER 3: USER PROFILE COMPUTATION
// ════════════════════════════════════════════════════════════════════════════════
//
// WHY compute profiles vs store raw preferences:
// Users don't know what they want. If you ask "what categories do you like?"
// they'll say "motivation" but actually spend 80% of their time on comedy.
// We compute preferences from BEHAVIOR, not declarations.
//
// The profile is recomputed when stale (>1 hour) to capture evolving tastes
// but not so frequently that it oscillates from single interactions.

func getOrComputeProfile(userID string) (*UserProfile, error) {
	// Try to load cached profile
	profile, err := loadUserProfile(userID)
	if err == nil && profile != nil {
		// Check staleness
		if time.Since(profile.LastComputedAt).Minutes() < profileStalenessMin {
			return profile, nil
		}
	}

	// Recompute from events
	return computeUserProfile(userID)
}

func loadUserProfile(userID string) (*UserProfile, error) {
	var p UserProfile
	var catJSON, hoursJSON, creatorsJSON, avoidedJSON []byte
	var catByHourJSON, catByEgoJSON, emotionPrefJSON, energyByHourJSON []byte
	var strategySuccessJSON []byte

	err := db.QueryRow(`
		SELECT user_id, category_affinity, energy_preference, social_drive,
			novelty_tolerance, ego_sensitivity, avg_session_sec, active_hours,
			preferred_creators, avoided_categories, avg_completion_rate,
			avg_skip_rate, total_sessions, total_watch_time_ms,
			recent_wins, recent_losses, last_computed_at, event_count,
			category_by_hour, category_by_ego, emotion_preference, energy_by_hour,
			attention_span, binge_intensity, creator_loyalty,
			competitiveness_index, mood_volatility, strategy_success_history
		FROM user_profiles WHERE user_id = $1`, userID).Scan(
		&p.UserID, &catJSON, &p.EnergyPreference, &p.SocialDrive,
		&p.NoveltyTolerance, &p.EgoSensitivity, &p.AvgSessionSec, &hoursJSON,
		&creatorsJSON, &avoidedJSON, &p.AvgCompletionRate,
		&p.AvgSkipRate, &p.TotalSessions, &p.TotalWatchTimeMs,
		&p.RecentWins, &p.RecentLosses, &p.LastComputedAt, &p.EventCount,
		&catByHourJSON, &catByEgoJSON, &emotionPrefJSON, &energyByHourJSON,
		&p.AttentionSpan, &p.BingeIntensity, &p.CreatorLoyalty,
		&p.CompetitivenessIndex, &p.MoodVolatility, &strategySuccessJSON,
	)
	if err != nil {
		return nil, err
	}

	json.Unmarshal(catJSON, &p.CategoryAffinity)
	json.Unmarshal(hoursJSON, &p.ActiveHours)
	json.Unmarshal(creatorsJSON, &p.PreferredCreators)
	json.Unmarshal(avoidedJSON, &p.AvoidedCategories)
	json.Unmarshal(catByHourJSON, &p.CategoryByHour)
	json.Unmarshal(catByEgoJSON, &p.CategoryByEgo)
	json.Unmarshal(emotionPrefJSON, &p.EmotionPreference)
	json.Unmarshal(energyByHourJSON, &p.EnergyByHour)
	json.Unmarshal(strategySuccessJSON, &p.StrategySuccessHistory)
	if p.StrategySuccessHistory == nil {
		p.StrategySuccessHistory = make(map[string]float64)
	}

	if p.CategoryAffinity == nil {
		p.CategoryAffinity = make(map[string]float64)
	}
	if p.CategoryByHour == nil {
		p.CategoryByHour = make(map[int]string)
	}
	if p.CategoryByEgo == nil {
		p.CategoryByEgo = make(map[string]map[string]float64)
	}
	if p.EmotionPreference == nil {
		p.EmotionPreference = make(map[string]float64)
	}
	if p.EnergyByHour == nil {
		p.EnergyByHour = make(map[int]float64)
	}
	return &p, nil
}

// computeUserProfile builds the personality model from raw event history.
//
// HOW each dimension is computed:
//
// CategoryAffinity: Count engagement events per category, normalize to 0-1 range.
//   A like in "comedy" = +1, a skip = -0.5, a rewatch = +2, a share = +3.
//   Higher weight for higher-effort actions.
//
// EnergyPreference: Average energy level of content they COMPLETED (>70% watched).
//   If they mostly finish high-energy content → energyPreference is high.
//
// SocialDrive: (events on friends' content) / (total events).
//   High ratio = social person. Low ratio = solo explorer.
//
// NoveltyTolerance: (unique categories engaged) / (total categories available).
//   Many categories = explorer. Few categories = loyalist.
//
// EgoSensitivity: Measures engagement change after wins vs losses.
//   If engagement drops sharply after a loss → high ego sensitivity.
func computeUserProfile(userID string) (*UserProfile, error) {
	// Preserve StrategySuccessHistory across recomputes — it's updated
	// incrementally at strategy-switch time, not derived from raw events.
	var preservedStrategyHistory map[string]float64
	if existing, err := loadUserProfile(userID); err == nil && existing != nil {
		preservedStrategyHistory = existing.StrategySuccessHistory
	}

	p := &UserProfile{
		UserID:           userID,
		CategoryAffinity: make(map[string]float64),
		EnergyPreference: 0.5, // default neutral
		SocialDrive:      0.5,
		NoveltyTolerance: 0.5,
		EgoSensitivity:   0.5,
		AttentionSpan:    0.5,
		BingeIntensity:   0.5,
		CreatorLoyalty:   0.5,
		CompetitivenessIndex: 0.5,
		MoodVolatility:       0.5,
		StrategySuccessHistory: preservedStrategyHistory,
		LastComputedAt:   time.Now(),
	}
	if p.StrategySuccessHistory == nil {
		p.StrategySuccessHistory = make(map[string]float64)
	}

	// Count total events
	var eventCount int
	db.QueryRow(`SELECT COUNT(*) FROM feed_events WHERE user_id = $1`, userID).Scan(&eventCount)
	p.EventCount = eventCount

	if eventCount == 0 {
		// No history — save default profile
		saveUserProfile(p)
		return p, nil
	}

	// === Category Affinity ===
	// Use stored category first, fall back to heuristic inference.
	rows, err := db.Query(`
		SELECT fe.event_type, fe.completion_rate, fe.content_type, fe.content_id,
			COALESCE(c.category, '') as c_cat, COALESCE(p.category, '') as p_cat,
			COALESCE(c.subject, '') as subject, COALESCE(c.prefix, '') as prefix,
			COALESCE(p.caption, '') as caption
		FROM feed_events fe
		LEFT JOIN challenges c ON fe.content_type = 'challenge' AND fe.content_id = CAST(c.id AS TEXT)
		LEFT JOIN posts p ON fe.content_type = 'post' AND fe.content_id = CAST(p.id AS TEXT)
		WHERE fe.user_id = $1
		ORDER BY fe.created_at DESC
		LIMIT 500`, userID)
	if err == nil {
		defer rows.Close()

		categoryScores := make(map[string]float64)
		var totalCompletion float64
		var completionCount int
		eventTypes := make(map[string]int)

		for rows.Next() {
			var evType, cType, cID, cCat, pCat, subject, prefix, caption string
			var completion float64
			rows.Scan(&evType, &completion, &cType, &cID, &cCat, &pCat, &subject, &prefix, &caption)

			// Prefer stored category, fall back to inference
			category := cCat
			if category == "" {
				category = pCat
			}
			if category == "" {
				category = inferCategory(subject, prefix, caption)
			}
			eventTypes[evType]++

			// Weight events by effort/intent with granular watch time tiers
			switch evType {
			case "share":
				categoryScores[category] += 3.0
			case "rewatch":
				categoryScores[category] += 2.5
			case "save":
				categoryScores[category] += 1.5
			case "like":
				categoryScores[category] += 1.0
			case "comment":
				categoryScores[category] += 1.2 // Comments require more effort than likes
			case "view":
				// Granular watch time scoring — more honest than binary thresholds
				if completion >= 0.9 {
					categoryScores[category] += 1.0 // Strong interest — nearly finished
				} else if completion >= 0.7 {
					categoryScores[category] += 0.5 // Good interest — watched most
				} else if completion >= 0.5 {
					categoryScores[category] += 0.1 // Neutral — watched half
				} else if completion >= 0.3 {
					categoryScores[category] -= 0.1 // Weak — gave it a chance, left
				}
				// <30% = no score change (neither positive nor negative)
			case "pause":
				categoryScores[category] += 0.3 // Paused to look = mild interest
			case "scroll_slow":
				categoryScores[category] += 0.2 // Slow scroll past = considering
			case "scroll_fast":
				categoryScores[category] -= 0.3 // Fast scroll = rejection
			case "skip":
				categoryScores[category] -= 0.5
			case "not_interested":
				categoryScores[category] -= 2.0
			}

			if completion > 0 {
				totalCompletion += completion
				completionCount++
			}
		}

		// Normalize category scores to 0-1
		maxCat := 0.0
		for _, v := range categoryScores {
			if v > maxCat {
				maxCat = v
			}
		}
		if maxCat > 0 {
			for k, v := range categoryScores {
				p.CategoryAffinity[k] = math.Max(0, v/maxCat)
			}
		}

		// Build avoided categories (negative scores)
		for k, v := range categoryScores {
			if v < -1.0 {
				p.AvoidedCategories = append(p.AvoidedCategories, k)
			}
		}

		// Average completion rate
		if completionCount > 0 {
			p.AvgCompletionRate = totalCompletion / float64(completionCount)
		}

		// Skip rate
		totalEvents := 0
		for _, c := range eventTypes {
			totalEvents += c
		}
		if totalEvents > 0 {
			p.AvgSkipRate = float64(eventTypes["skip"]+eventTypes["not_interested"]) / float64(totalEvents)
		}
	}

	// === Social Drive ===
	// How much does the user engage with followed creators vs random content?
	var followedEngagement, totalEngagement int
	db.QueryRow(`
		SELECT COUNT(*) FROM feed_events fe
		WHERE fe.user_id = $1 AND fe.event_type IN ('like','comment','share','save','rewatch')`, userID).Scan(&totalEngagement)
	db.QueryRow(`
		SELECT COUNT(*) FROM feed_events fe
		JOIN follows f ON f.follower_id = CAST($1 AS INT)
		WHERE fe.user_id = $1
		AND fe.event_type IN ('like','comment','share','save','rewatch')
		AND (
			(fe.content_type = 'post' AND EXISTS (
				SELECT 1 FROM posts p WHERE CAST(p.id AS TEXT) = fe.content_id AND p.author_id = f.following_id
			))
			OR
			(fe.content_type = 'challenge' AND EXISTS (
				SELECT 1 FROM challenges c WHERE CAST(c.id AS TEXT) = fe.content_id AND c.creator_id = f.following_id
			))
		)`, userID).Scan(&followedEngagement)
	if totalEngagement > 0 {
		p.SocialDrive = math.Min(1.0, float64(followedEngagement)/float64(totalEngagement))
	}

	// === Novelty Tolerance ===
	// How many unique categories does the user engage with?
	var uniqueCategories int
	db.QueryRow(`
		SELECT COUNT(DISTINCT COALESCE(c.category, p.category, 'other'))
		FROM feed_events fe
		LEFT JOIN challenges c ON fe.content_type = 'challenge' AND fe.content_id = CAST(c.id AS TEXT)
		LEFT JOIN posts p ON fe.content_type = 'post' AND fe.content_id = CAST(p.id AS TEXT)
		WHERE fe.user_id = $1 AND fe.event_type IN ('like','comment','share','save','view')`, userID).Scan(&uniqueCategories)
	// Normalize: 1-3 categories = low novelty, 8+ = high
	p.NoveltyTolerance = math.Min(1.0, float64(uniqueCategories)/8.0)

	// === Energy Preference ===
	// Average energy of content they completed (>70% watched)
	// For now, we approximate: challenges = higher energy, posts = lower energy
	var challengeViews, postViews int
	db.QueryRow(`
		SELECT COUNT(*) FROM feed_events
		WHERE user_id = $1 AND content_type = 'challenge' AND completion_rate > 0.7`, userID).Scan(&challengeViews)
	db.QueryRow(`
		SELECT COUNT(*) FROM feed_events
		WHERE user_id = $1 AND content_type = 'post' AND completion_rate > 0.7`, userID).Scan(&postViews)
	total := challengeViews + postViews
	if total > 0 {
		// Challenges are higher energy (0.7-1.0), posts are varied (0.3-0.7)
		p.EnergyPreference = 0.3 + 0.5*(float64(challengeViews)/float64(total))
	}

	// === Ego Sensitivity ===
	// Recent wins/losses and their effect on engagement
	db.QueryRow(`
		SELECT COUNT(*) FROM challenge_votes cv
		JOIN challenge_responses cr ON cv.response_id = cr.id
		WHERE cr.responder_id = CAST($1 AS INT)
		AND cv.created_at > NOW() - INTERVAL '7 days'`, userID).Scan(&p.RecentWins)
	// Approximate losses: challenges responded to but not voted for
	db.QueryRow(`
		SELECT COUNT(*) FROM challenge_responses cr
		WHERE cr.responder_id = CAST($1 AS INT)
		AND cr.created_at > NOW() - INTERVAL '7 days'
		AND NOT EXISTS (
			SELECT 1 FROM challenge_votes cv WHERE cv.response_id = cr.id
		)`, userID).Scan(&p.RecentLosses)
	if p.RecentWins+p.RecentLosses > 0 {
		// If they have many battles, they're competitive = higher ego sensitivity
		p.EgoSensitivity = math.Min(1.0, float64(p.RecentWins+p.RecentLosses)/10.0)
	}

	// === Session stats ===
	var sessionCount int
	var totalWatchMs int64
	db.QueryRow(`
		SELECT COUNT(DISTINCT session_id), COALESCE(SUM(watch_duration_ms), 0)
		FROM feed_events WHERE user_id = $1`, userID).Scan(&sessionCount, &totalWatchMs)
	p.TotalSessions = sessionCount
	p.TotalWatchTimeMs = totalWatchMs
	if sessionCount > 0 {
		p.AvgSessionSec = int(totalWatchMs / int64(sessionCount) / 1000)
	}

	// === Active hours ===
	hourRows, err := db.Query(`
		SELECT EXTRACT(HOUR FROM created_at)::INT as h, COUNT(*) as c
		FROM feed_events WHERE user_id = $1
		GROUP BY h ORDER BY c DESC LIMIT 4`, userID)
	if err == nil {
		defer hourRows.Close()
		for hourRows.Next() {
			var h, c int
			hourRows.Scan(&h, &c)
			p.ActiveHours = append(p.ActiveHours, h)
		}
	}

	// === Preferred creators ===
	creatorRows, err := db.Query(`
		SELECT creator_id, SUM(score) as total FROM (
			SELECT COALESCE(CAST(c.creator_id AS TEXT), CAST(p.author_id AS TEXT)) as creator_id,
			CASE fe.event_type
				WHEN 'share' THEN 3 WHEN 'rewatch' THEN 2 WHEN 'save' THEN 1.5
				WHEN 'like' THEN 1 WHEN 'comment' THEN 1 WHEN 'view' THEN 0.3
				ELSE 0
			END as score
			FROM feed_events fe
			LEFT JOIN challenges c ON fe.content_type = 'challenge' AND fe.content_id = CAST(c.id AS TEXT)
			LEFT JOIN posts p ON fe.content_type = 'post' AND fe.content_id = CAST(p.id AS TEXT)
			WHERE fe.user_id = $1
		) sub
		WHERE creator_id IS NOT NULL
		GROUP BY creator_id ORDER BY total DESC LIMIT 10`, userID)
	if err == nil {
		defer creatorRows.Close()
		for creatorRows.Next() {
			var cid string
			var score float64
			creatorRows.Scan(&cid, &score)
			if cid != "" {
				p.PreferredCreators = append(p.PreferredCreators, cid)
			}
		}
	}

	// === Context-aware: Category by Hour ===
	// What category does the user prefer at each hour of the day?
	p.CategoryByHour = make(map[int]string)
	p.EnergyByHour = make(map[int]float64)
	hourCatRows, err := db.Query(`
		SELECT EXTRACT(HOUR FROM fe.created_at)::INT as h,
			COALESCE(c.category, p.category, '') as cat,
			COALESCE(c.energy_level, p.energy_level, 'medium') as energy,
			COUNT(*) as cnt
		FROM feed_events fe
		LEFT JOIN challenges c ON fe.content_type = 'challenge' AND fe.content_id = CAST(c.id AS TEXT)
		LEFT JOIN posts p ON fe.content_type = 'post' AND fe.content_id = CAST(p.id AS TEXT)
		WHERE fe.user_id = $1
		AND fe.event_type IN ('like','comment','share','save','rewatch','view')
		AND fe.completion_rate > 0.5
		GROUP BY h, cat, energy
		ORDER BY h, cnt DESC`, userID)
	if err == nil {
		defer hourCatRows.Close()
		hourCatBest := make(map[int]string)       // hour → best category
		hourCatMax := make(map[int]int)             // hour → best count
		hourEnergySum := make(map[int]float64)      // hour → sum of energy scores
		hourEnergyCount := make(map[int]int)        // hour → count
		for hourCatRows.Next() {
			var h, cnt int
			var cat, energy string
			hourCatRows.Scan(&h, &cat, &energy, &cnt)
			if cat != "" && cnt > hourCatMax[h] {
				hourCatBest[h] = cat
				hourCatMax[h] = cnt
			}
			energyVal := 0.5
			switch energy {
			case "low":
				energyVal = 0.2
			case "high":
				energyVal = 0.8
			}
			hourEnergySum[h] += energyVal * float64(cnt)
			hourEnergyCount[h] += cnt
		}
		p.CategoryByHour = hourCatBest
		for h, sum := range hourEnergySum {
			if hourEnergyCount[h] > 0 {
				p.EnergyByHour[h] = sum / float64(hourEnergyCount[h])
			}
		}
	}

	// === Context-aware: Emotion Preference ===
	// Which emotion tags does the user engage with most?
	p.EmotionPreference = make(map[string]float64)
	emotionRows, err := db.Query(`
		SELECT tag, SUM(score) as total FROM (
			SELECT unnest(
				CASE
					WHEN fe.content_type = 'challenge' THEN (SELECT COALESCE(emotion_tags, '[]')::JSONB FROM challenges WHERE CAST(id AS TEXT) = fe.content_id)
					WHEN fe.content_type = 'post' THEN (SELECT COALESCE(emotion_tags, '[]')::JSONB FROM posts WHERE CAST(id AS TEXT) = fe.content_id)
				END
			)::TEXT as tag,
			CASE fe.event_type
				WHEN 'share' THEN 3 WHEN 'rewatch' THEN 2 WHEN 'save' THEN 1.5
				WHEN 'like' THEN 1 WHEN 'comment' THEN 1
				WHEN 'view' THEN CASE WHEN fe.completion_rate > 0.7 THEN 0.5 ELSE 0.1 END
				WHEN 'skip' THEN -0.5 WHEN 'not_interested' THEN -2
				ELSE 0
			END as score
			FROM feed_events fe
			WHERE fe.user_id = $1
		) sub
		WHERE tag IS NOT NULL AND tag != ''
		GROUP BY tag
		ORDER BY total DESC`, userID)
	if err == nil {
		defer emotionRows.Close()
		maxEmotionScore := 0.0
		rawEmotions := make(map[string]float64)
		for emotionRows.Next() {
			var tag string
			var score float64
			emotionRows.Scan(&tag, &score)
			// Strip JSON quotes from unnested JSONB text
			tag = strings.Trim(tag, "\"")
			if tag != "" {
				rawEmotions[tag] = score
				if score > maxEmotionScore {
					maxEmotionScore = score
				}
			}
		}
		if maxEmotionScore > 0 {
			for k, v := range rawEmotions {
				p.EmotionPreference[k] = math.Max(0, v/maxEmotionScore)
			}
		}
	}

	// === Context-aware: Category by Ego State ===
	// After wins vs losses, what does the user gravitate toward?
	p.CategoryByEgo = make(map[string]map[string]float64)
	p.CategoryByEgo["winning"] = make(map[string]float64)
	p.CategoryByEgo["losing"] = make(map[string]float64)

	// Find events within 1 hour after wins
	winCatRows, err := db.Query(`
		SELECT COALESCE(c2.category, p2.category, '') as cat, COUNT(*) as cnt
		FROM challenge_votes cv
		JOIN challenge_responses cr ON cv.response_id = cr.id
		JOIN feed_events fe ON fe.user_id = $1
			AND fe.created_at BETWEEN cv.created_at AND cv.created_at + INTERVAL '1 hour'
			AND fe.event_type IN ('like','share','save','rewatch')
		LEFT JOIN challenges c2 ON fe.content_type = 'challenge' AND fe.content_id = CAST(c2.id AS TEXT)
		LEFT JOIN posts p2 ON fe.content_type = 'post' AND fe.content_id = CAST(p2.id AS TEXT)
		WHERE cr.responder_id = CAST($1 AS INT)
		AND cv.created_at > NOW() - INTERVAL '30 days'
		GROUP BY cat
		ORDER BY cnt DESC LIMIT 5`, userID)
	if err == nil {
		defer winCatRows.Close()
		for winCatRows.Next() {
			var cat string
			var cnt int
			winCatRows.Scan(&cat, &cnt)
			if cat != "" {
				p.CategoryByEgo["winning"][cat] = float64(cnt)
			}
		}
	}

	// Find events within 1 hour after losses (responded but no votes)
	lossCatRows, err := db.Query(`
		SELECT COALESCE(c2.category, p2.category, '') as cat, COUNT(*) as cnt
		FROM challenge_responses cr
		LEFT JOIN challenge_votes cv ON cv.response_id = cr.id
		JOIN feed_events fe ON fe.user_id = $1
			AND fe.created_at BETWEEN cr.created_at AND cr.created_at + INTERVAL '1 hour'
			AND fe.event_type IN ('like','share','save','rewatch')
		LEFT JOIN challenges c2 ON fe.content_type = 'challenge' AND fe.content_id = CAST(c2.id AS TEXT)
		LEFT JOIN posts p2 ON fe.content_type = 'post' AND fe.content_id = CAST(p2.id AS TEXT)
		WHERE cr.responder_id = CAST($1 AS INT)
		AND cv.id IS NULL
		AND cr.created_at > NOW() - INTERVAL '30 days'
		GROUP BY cat
		ORDER BY cnt DESC LIMIT 5`, userID)
	if err == nil {
		defer lossCatRows.Close()
		for lossCatRows.Next() {
			var cat string
			var cnt int
			lossCatRows.Scan(&cat, &cnt)
			if cat != "" {
				p.CategoryByEgo["losing"][cat] = float64(cnt)
			}
		}
	}

	// ═══ EXTENDED PERSONALITY DIMENSIONS ═══════════════════════════════

	// --- AttentionSpan ---
	// Fraction of views that reached 70%+ completion, normalized.
	// High = deep watcher (finishes videos). Low = scanner (leaves early).
	var deepViews, totalViews int
	db.QueryRow(`
		SELECT COUNT(*) FROM feed_events
		WHERE user_id = $1 AND event_type = 'view' AND completion_rate > 0.7`, userID).Scan(&deepViews)
	db.QueryRow(`
		SELECT COUNT(*) FROM feed_events
		WHERE user_id = $1 AND event_type = 'view'`, userID).Scan(&totalViews)
	if totalViews > 0 {
		p.AttentionSpan = math.Min(1.0, float64(deepViews)/float64(totalViews))
	}

	// --- BingeIntensity ---
	// Tail of session length distribution. If a user has at least one very long
	// session (>15min) they're a binger. Otherwise scale by avg session length.
	var maxSessionMs int64
	db.QueryRow(`
		SELECT COALESCE(MAX(total_ms), 0) FROM (
			SELECT session_id, SUM(watch_duration_ms) AS total_ms
			FROM feed_events WHERE user_id = $1 GROUP BY session_id
		) s`, userID).Scan(&maxSessionMs)
	// 15min = 900,000ms = full binge. 5min = 300,000ms = casual. Below = dipper.
	if maxSessionMs > 0 {
		p.BingeIntensity = math.Min(1.0, float64(maxSessionMs)/900000.0)
	}

	// --- CreatorLoyalty ---
	// How concentrated are their positive events on their top 3 creators?
	// (share of positive engagement on top-3 creators / total positive engagement)
	var topCreatorEngagement, totalPositiveEngagement int
	db.QueryRow(`
		SELECT COUNT(*) FROM feed_events fe
		WHERE fe.user_id = $1 AND fe.event_type IN ('like','share','save','rewatch','complete','loop','unmute','profile_visit')`,
		userID).Scan(&totalPositiveEngagement)
	if totalPositiveEngagement > 0 {
		db.QueryRow(`
			WITH creator_engagement AS (
				SELECT COALESCE(c.creator_id::TEXT, p.author_id::TEXT) AS cid, COUNT(*) AS cnt
				FROM feed_events fe
				LEFT JOIN challenges c ON fe.content_type = 'challenge' AND fe.content_id = CAST(c.id AS TEXT)
				LEFT JOIN posts p ON fe.content_type = 'post' AND fe.content_id = CAST(p.id AS TEXT)
				WHERE fe.user_id = $1
				AND fe.event_type IN ('like','share','save','rewatch','complete','loop','unmute','profile_visit')
				AND COALESCE(c.creator_id::TEXT, p.author_id::TEXT) IS NOT NULL
				GROUP BY cid
				ORDER BY cnt DESC
				LIMIT 3
			)
			SELECT COALESCE(SUM(cnt), 0) FROM creator_engagement`, userID).Scan(&topCreatorEngagement)
		p.CreatorLoyalty = math.Min(1.0, float64(topCreatorEngagement)/float64(totalPositiveEngagement))
	}

	// --- CompetitivenessIndex ---
	// Do they create challenges, respond to battles, or just view?
	var challengesCreated, responsesSubmitted int
	db.QueryRow(`SELECT COUNT(*) FROM challenges WHERE creator_id = CAST($1 AS INT)`, userID).Scan(&challengesCreated)
	db.QueryRow(`SELECT COUNT(*) FROM challenge_responses WHERE responder_id = CAST($1 AS INT)`, userID).Scan(&responsesSubmitted)
	// 5 of either action = fully competitive.
	totalCompActions := challengesCreated*2 + responsesSubmitted // Creating weighs more
	if totalCompActions > 0 {
		p.CompetitivenessIndex = math.Min(1.0, float64(totalCompActions)/10.0)
	}

	// --- MoodVolatility ---
	// Variance of per-session skip rate across last 20 sessions. High variance =
	// moody user whose taste swings session to session; low = steady consumer.
	sessionSkipRates, _ := db.Query(`
		SELECT session_id,
			SUM(CASE WHEN event_type IN ('skip','not_interested') THEN 1 ELSE 0 END)::FLOAT / NULLIF(COUNT(*), 0) AS skip_rate
		FROM feed_events
		WHERE user_id = $1 AND session_id <> ''
		GROUP BY session_id
		ORDER BY MAX(created_at) DESC
		LIMIT 20`, userID)
	if sessionSkipRates != nil {
		var rates []float64
		for sessionSkipRates.Next() {
			var sid string
			var sr float64
			if err := sessionSkipRates.Scan(&sid, &sr); err == nil {
				rates = append(rates, sr)
			}
		}
		sessionSkipRates.Close()
		if len(rates) >= 3 {
			mean := 0.0
			for _, r := range rates {
				mean += r
			}
			mean /= float64(len(rates))
			variance := 0.0
			for _, r := range rates {
				variance += (r - mean) * (r - mean)
			}
			variance /= float64(len(rates))
			// stddev up to ~0.5 is reasonable range; normalize.
			p.MoodVolatility = math.Min(1.0, math.Sqrt(variance)/0.5)
		}
	}

	saveUserProfile(p)
	return p, nil
}

func saveUserProfile(p *UserProfile) {
	catJSON, _ := json.Marshal(p.CategoryAffinity)
	hoursJSON, _ := json.Marshal(p.ActiveHours)
	creatorsJSON, _ := json.Marshal(p.PreferredCreators)
	avoidedJSON, _ := json.Marshal(p.AvoidedCategories)
	catByHourJSON, _ := json.Marshal(p.CategoryByHour)
	catByEgoJSON, _ := json.Marshal(p.CategoryByEgo)
	emotionPrefJSON, _ := json.Marshal(p.EmotionPreference)
	energyByHourJSON, _ := json.Marshal(p.EnergyByHour)
	strategySuccessJSON, _ := json.Marshal(p.StrategySuccessHistory)

	_, err := db.Exec(`
		INSERT INTO user_profiles (user_id, category_affinity, energy_preference,
			social_drive, novelty_tolerance, ego_sensitivity, avg_session_sec,
			active_hours, preferred_creators, avoided_categories,
			avg_completion_rate, avg_skip_rate, total_sessions, total_watch_time_ms,
			recent_wins, recent_losses, last_computed_at, event_count,
			category_by_hour, category_by_ego, emotion_preference, energy_by_hour,
			attention_span, binge_intensity, creator_loyalty,
			competitiveness_index, mood_volatility, strategy_success_history)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,
			$19,$20,$21,$22,$23,$24,$25,$26,$27,$28)
		ON CONFLICT (user_id) DO UPDATE SET
			category_affinity=$2, energy_preference=$3, social_drive=$4,
			novelty_tolerance=$5, ego_sensitivity=$6, avg_session_sec=$7,
			active_hours=$8, preferred_creators=$9, avoided_categories=$10,
			avg_completion_rate=$11, avg_skip_rate=$12, total_sessions=$13,
			total_watch_time_ms=$14, recent_wins=$15, recent_losses=$16,
			last_computed_at=$17, event_count=$18,
			category_by_hour=$19, category_by_ego=$20, emotion_preference=$21, energy_by_hour=$22,
			attention_span=$23, binge_intensity=$24, creator_loyalty=$25,
			competitiveness_index=$26, mood_volatility=$27, strategy_success_history=$28`,
		p.UserID, catJSON, p.EnergyPreference, p.SocialDrive,
		p.NoveltyTolerance, p.EgoSensitivity, p.AvgSessionSec,
		hoursJSON, creatorsJSON, avoidedJSON,
		p.AvgCompletionRate, p.AvgSkipRate, p.TotalSessions, p.TotalWatchTimeMs,
		p.RecentWins, p.RecentLosses, p.LastComputedAt, p.EventCount,
		catByHourJSON, catByEgoJSON, emotionPrefJSON, energyByHourJSON,
		p.AttentionSpan, p.BingeIntensity, p.CreatorLoyalty,
		p.CompetitivenessIndex, p.MoodVolatility, strategySuccessJSON,
	)
	if err != nil {
		log.Printf("Failed to save user profile for %s: %v", p.UserID, err)
	}
}

// inferCategory attempts to determine content category from text signals.
// This is a heuristic bridge until we have proper ML classification.
//
// WHY heuristic: At our scale, training a text classifier needs data we don't have yet.
// These keyword rules are ~70% accurate, which is enough for recommendation.
// The algorithm is self-correcting: if we mis-categorize, the user's skip/engage
// pattern will adjust their profile anyway.
func inferCategory(subject, prefix, caption string) string {
	text := strings.ToLower(subject + " " + prefix + " " + caption)

	categories := map[string][]string{
		"comedy":     {"funny", "lol", "roast", "comedy", "laugh", "joke", "humor", "meme", "prank"},
		"motivation": {"grind", "discipline", "success", "hustle", "motivat", "inspire", "goal", "mindset", "win"},
		"skill":      {"tutorial", "learn", "how to", "technique", "skill", "trick", "tip", "teach", "lesson"},
		"dance":      {"dance", "choreo", "moves", "groove", "dancer", "dancing", "step"},
		"music":      {"sing", "song", "beat", "rap", "music", "vocal", "instrument", "cover"},
		"fitness":    {"workout", "gym", "fit", "exercise", "muscle", "train", "body", "weight"},
		"gaming":     {"game", "gaming", "play", "stream", "esport", "gamer"},
		"story":      {"story", "vlog", "life", "day in", "experience", "journey", "storytime"},
		"challenge":  {"challenge", "dare", "battle", "versus", "vs", "compete", "who is better"},
		"emotional":  {"sad", "cry", "emotional", "feel", "heart", "miss", "love", "pain"},
	}

	bestCategory := "general"
	bestScore := 0

	for category, keywords := range categories {
		score := 0
		for _, kw := range keywords {
			if strings.Contains(text, kw) {
				score++
			}
		}
		if score > bestScore {
			bestScore = score
			bestCategory = category
		}
	}

	return bestCategory
}

// autoTagFromCaption extracts emotion/mood tags from caption text automatically.
// Used when creator doesn't provide emotion tags — we infer from their words.
// Also merges with any custom tags the creator provided.
func autoTagFromCaption(text string, existingTags []string) []string {
	text = strings.ToLower(text)
	tagSet := make(map[string]bool)
	for _, t := range existingTags {
		tagSet[t] = true
	}

	for keyword, tags := range CaptionKeywordTags {
		if strings.Contains(text, keyword) {
			for _, tag := range tags {
				tagSet[tag] = true
			}
		}
	}

	result := make([]string, 0, len(tagSet))
	for tag := range tagSet {
		result = append(result, tag)
	}
	return result
}

// ════════════════════════════════════════════════════════════════════════════════
// LAYER 4: CONTENT UNDERSTANDING
// ════════════════════════════════════════════════════════════════════════════════
//
// WHY separate content scoring:
// Content quality is objective-ish — if 80% of viewers skip a video, it's
// probably not great regardless of who the next viewer is. We compute these
// aggregate metrics once and cache them, so the per-user scoring is fast.

func getContentScore(contentID, contentType string) *ContentScore {
	cs := &ContentScore{
		ContentID:   contentID,
		ContentType: contentType,
		EnergyLevel: 0.5,
		Category:    "general",
		EmotionVector: make(map[string]float64),
	}

	// Get aggregate engagement metrics
	var viewCount, likeCount, commentCount int
	var avgCompletion, avgWatchMs float64
	var skipCount, rewatchCount, shareCount, notIntCount int

	db.QueryRow(`
		SELECT
			COUNT(*) FILTER (WHERE event_type = 'view'),
			COUNT(*) FILTER (WHERE event_type = 'like'),
			COUNT(*) FILTER (WHERE event_type = 'comment'),
			COUNT(*) FILTER (WHERE event_type = 'skip'),
			COUNT(*) FILTER (WHERE event_type = 'rewatch'),
			COUNT(*) FILTER (WHERE event_type = 'share'),
			COUNT(*) FILTER (WHERE event_type = 'not_interested'),
			COALESCE(AVG(completion_rate) FILTER (WHERE event_type = 'view'), 0),
			COALESCE(AVG(watch_duration_ms) FILTER (WHERE event_type = 'view'), 0)
		FROM feed_events
		WHERE content_id = $1 AND content_type = $2`,
		contentID, contentType).Scan(
		&viewCount, &likeCount, &commentCount,
		&skipCount, &rewatchCount, &shareCount, &notIntCount,
		&avgCompletion, &avgWatchMs,
	)

	cs.ViewCount = viewCount
	cs.LikeCount = likeCount
	cs.CommentCount = commentCount
	cs.AvgCompletionRate = avgCompletion
	cs.AvgWatchTimeMs = int(avgWatchMs)
	cs.ShareCount = shareCount
	cs.NotInterestedCount = notIntCount

	totalInteractions := viewCount + skipCount
	if totalInteractions > 0 {
		cs.SkipRate = float64(skipCount) / float64(totalInteractions)
	}
	if viewCount > 0 {
		cs.RewatchRate = float64(rewatchCount) / float64(viewCount)
	}

	// Quality score: combination of engagement signals with granular completion tiers
	if totalInteractions > 0 {
		likeRate := math.Min(1.0, float64(likeCount)/float64(totalInteractions+1))
		shareRate := math.Min(1.0, float64(shareCount)/float64(totalInteractions+1))
		commentRate := math.Min(1.0, float64(commentCount)/float64(totalInteractions+1))

		// Granular completion scoring (replaces flat avgCompletion)
		completionScore := 0.0
		if avgCompletion >= 0.9 {
			completionScore = 1.0 // Strong: almost everyone finishes
		} else if avgCompletion >= 0.7 {
			completionScore = 0.7 // Good: most people watch most of it
		} else if avgCompletion >= 0.5 {
			completionScore = 0.4 // Neutral: half the content
		} else {
			completionScore = 0.1 // Weak: people leave early
		}

		cs.QualityScore = (completionScore*0.25 + likeRate*0.15 + shareRate*0.25 +
			cs.RewatchRate*0.20 + commentRate*0.15) * (1.0 - cs.SkipRate*0.5)
	}

	// Trending score: engagement velocity in last 2 hours
	var recentEngagement int
	db.QueryRow(`
		SELECT COUNT(*) FROM feed_events
		WHERE content_id = $1 AND content_type = $2
		AND event_type IN ('like','comment','share','save')
		AND created_at > NOW() - INTERVAL '2 hours'`,
		contentID, contentType).Scan(&recentEngagement)
	cs.TrendingScore = math.Min(1.0, float64(recentEngagement)/20.0) // 20 engagements in 2h = max trending

	// Energy level inference
	cs.EnergyLevel = inferContentEnergy(contentType, cs)

	// Category, creator info, and created_at — single query per content type
	if contentType == "challenge" {
		var subject, prefix, dbCategory, dbEnergy string
		var emotionJSON []byte
		var creatorID, league string
		var followers, wins, losses int
		var createdAt time.Time
		db.QueryRow(`
			SELECT COALESCE(c.subject,''), COALESCE(c.prefix,''),
				COALESCE(c.category,'other'), COALESCE(c.energy_level,'medium'),
				COALESCE(c.emotion_tags,'[]'::JSONB),
				CAST(u.id AS TEXT), u.league,
				(SELECT COUNT(*) FROM follows WHERE following_id = u.id),
				u.wins, u.losses, c.created_at
			FROM challenges c
			JOIN users u ON c.creator_id = u.id
			WHERE c.id = $1`, contentID).Scan(
			&subject, &prefix, &dbCategory, &dbEnergy, &emotionJSON,
			&creatorID, &league, &followers, &wins, &losses, &createdAt)

		if dbCategory != "" && dbCategory != "other" {
			cs.Category = dbCategory
		} else {
			cs.Category = inferCategory(subject, prefix, "")
		}
		switch dbEnergy {
		case "low":
			cs.EnergyLevel = 0.25
		case "high":
			cs.EnergyLevel = 0.85
		default:
			cs.EnergyLevel = 0.55
		}
		var emotions []string
		json.Unmarshal(emotionJSON, &emotions)
		// Auto-tag from caption if no creator-declared tags
		if len(emotions) == 0 {
			emotions = autoTagFromCaption(subject+" "+prefix, emotions)
		}
		for _, e := range emotions {
			cs.EmotionVector[e] = 1.0
		}
		cs.CreatorID = creatorID
		cs.CreatorLeague = league
		cs.CreatorFollowers = followers
		cs.CreatedAt = createdAt
		if wins+losses > 0 {
			cs.CreatorWinRate = float64(wins) / float64(wins+losses)
		}
	} else if contentType == "post" {
		var caption, dbCategory, dbEnergy string
		var emotionJSON []byte
		var authorID, league string
		var followers, wins, losses int
		var createdAt time.Time
		db.QueryRow(`
			SELECT COALESCE(p.caption,''),
				COALESCE(p.category,'other'), COALESCE(p.energy_level,'medium'),
				COALESCE(p.emotion_tags,'[]'::JSONB),
				CAST(u.id AS TEXT), u.league,
				(SELECT COUNT(*) FROM follows WHERE following_id = u.id),
				u.wins, u.losses, p.created_at
			FROM posts p
			JOIN users u ON p.author_id = u.id
			WHERE p.id = $1`, contentID).Scan(
			&caption, &dbCategory, &dbEnergy, &emotionJSON,
			&authorID, &league, &followers, &wins, &losses, &createdAt)

		if dbCategory != "" && dbCategory != "other" {
			cs.Category = dbCategory
		} else {
			cs.Category = inferCategory("", "", caption)
		}
		switch dbEnergy {
		case "low":
			cs.EnergyLevel = 0.25
		case "high":
			cs.EnergyLevel = 0.85
		default:
			cs.EnergyLevel = 0.55
		}
		var emotions []string
		json.Unmarshal(emotionJSON, &emotions)
		// Auto-tag from caption if no creator-declared tags
		if len(emotions) == 0 {
			emotions = autoTagFromCaption(caption, emotions)
		}
		for _, e := range emotions {
			cs.EmotionVector[e] = 1.0
		}
		cs.CreatorID = authorID
		cs.CreatorLeague = league
		cs.CreatorFollowers = followers
		cs.CreatedAt = createdAt
		if wins+losses > 0 {
			cs.CreatorWinRate = float64(wins) / float64(wins+losses)
		}
	}

	return cs
}

// inferContentEnergy determines how "intense" a piece of content is.
//
// WHY: Matching content energy to user energy state is how Netflix decides
// to show you a thriller vs a sitcom. High-energy content to a fatigued user
// = skip. Low-energy content to a hyped user = boring.
//
// Signals:
// - Challenges are inherently higher energy than posts (competition)
// - High rewatch rate + high completion = engaging/story-like (medium energy)
// - High share rate = viral/exciting (high energy)
// - High skip rate on short content = mismatch (could be any energy)
func inferContentEnergy(contentType string, cs *ContentScore) float64 {
	base := 0.5
	if contentType == "challenge" {
		base = 0.7 // Challenges are competitive = higher energy
	}

	// Adjust based on engagement patterns
	if cs.RewatchRate > 0.3 {
		base += 0.1 // High rewatch = emotionally engaging
	}
	if cs.ShareCount > 5 {
		base += 0.1 // Highly shared = viral energy
	}
	if cs.AvgCompletionRate > 0.8 {
		base -= 0.05 // High completion can mean relaxing/story content
	}

	return math.Min(1.0, math.Max(0.0, base))
}

// ════════════════════════════════════════════════════════════════════════════════
// LAYER 5: THE SCORING ENGINE
// ════════════════════════════════════════════════════════════════════════════════
//
// This is the core brain. For each candidate content item, it computes a score
// by combining all the layers:
//
//   final_score = social_boost     * wSocial      (0.25)
//               + freshness        * wFreshness    (0.20)
//               + energy_fit       * wEnergyFit    (0.20)
//               + relevance        * wRelevance    (0.15)
//               + quality          * wQuality      (0.10)
//               + novelty_bonus    * wNovelty      (0.10)
//               + ego_boost                        (conditional)
//               - fatigue_penalty                  (conditional)
//               - dopamine_penalty                 (conditional)
//               + unseen_bonus                     (0.15 if never shown)
//               + cold_start_bonus                 (for new content)
//
// WHY these specific factors:
//
// SOCIAL (25%): At small scale, "your friend liked this" is the strongest signal.
//   TikTok deprioritizes social for new users but amplifies it for retained users.
//   We're small enough that social proof matters most.
//
// FRESHNESS (20%): Without this, one viral video dominates forever. YouTube calls
//   this "age boosting" — new content gets an initial visibility window to prove itself.
//
// ENERGY FIT (20%): Our unique advantage. Battle apps have wins/losses that create
//   emotional states. A user who just lost shouldn't see "look how amazing this person is."
//   They need either comedy (mood lift) or easy wins (ego repair).
//
// RELEVANCE (15%): Category matching. Lower than you'd expect because at low data,
//   strong relevance filtering creates filter bubbles. We keep it moderate.
//
// QUALITY (10%): Creator reputation from win rate, followers, content metrics.
//   Low weight because new creators need a chance — high quality weight = incumbency advantage.
//
// NOVELTY (10%): Forced exploration. Shows users new categories they haven't tried.
//   Without this, you get echo chambers. TikTok does ~15-20% exploration.

func scoreForUser(cs *ContentScore, profile *UserProfile, session *SessionState, followingSet map[string]bool, fofSet map[string]bool, interactedIDs map[string]bool) (float64, map[string]float64) {
	breakdown := make(map[string]float64)

	// ── SOCIAL BOOST ──
	// Direct follow = strongest social signal
	// Friend-of-friend = weaker but still relevant
	// Mutual follow = extra trust signal
	social := 0.0
	if followingSet[cs.CreatorID] {
		social = 0.7 // Direct follow
	} else if fofSet[cs.CreatorID] {
		social = 0.3 // Friend of friend
	}
	// Preferred creator bonus
	for _, pc := range profile.PreferredCreators {
		if pc == cs.CreatorID {
			social = math.Min(1.0, social+0.2)
			break
		}
	}
	breakdown["social"] = social

	// ── FRESHNESS ──
	// Exponential decay with configurable half-life
	hoursSince := time.Since(cs.CreatedAt).Hours()
	freshness := math.Exp(-0.693 * hoursSince / freshnessHalfLifeHours) // ln(2) ≈ 0.693
	breakdown["freshness"] = freshness

	// ── ENERGY FIT ──
	// How well does the content's energy match the user's current state?
	// Perfect match = 1.0, complete mismatch = 0.0
	currentEnergy := profile.EnergyPreference
	if session.DopamineBudget < 0.3 {
		// Fatigued user → lower their effective energy preference
		currentEnergy *= session.DopamineBudget / 0.3
	}
	currentEnergy = math.Max(0, math.Min(1.0, currentEnergy))
	energyFit := 1.0 - math.Abs(currentEnergy-cs.EnergyLevel)
	breakdown["energyFit"] = energyFit

	// ── RELEVANCE ──
	// Category match from user profile affinity
	relevance := 0.0
	if affinity, ok := profile.CategoryAffinity[cs.Category]; ok {
		relevance = affinity
	}
	// Negative signal: avoided category
	for _, avoided := range profile.AvoidedCategories {
		if avoided == cs.Category {
			relevance = -0.3 // Active penalty
			break
		}
	}
	breakdown["relevance"] = relevance

	// ── QUALITY ──
	// Creator reputation + content engagement metrics
	quality := cs.QualityScore
	// League bonus
	switch cs.CreatorLeague {
	case "Diamond":
		quality = math.Min(1.0, quality+0.15)
	case "Platinum":
		quality = math.Min(1.0, quality+0.10)
	case "Gold":
		quality = math.Min(1.0, quality+0.05)
	}
	breakdown["quality"] = quality

	// ── NOVELTY ──
	// Is this a new category or creator for the user?
	novelty := 0.0
	if _, seen := profile.CategoryAffinity[cs.Category]; !seen {
		novelty = 0.6 // New category = exploration opportunity
	}
	isNewCreator := true
	for _, pc := range profile.PreferredCreators {
		if pc == cs.CreatorID {
			isNewCreator = false
			break
		}
	}
	if isNewCreator && !followingSet[cs.CreatorID] {
		novelty = math.Min(1.0, novelty+0.4) // New creator
	}
	// Scale by user's novelty tolerance
	novelty *= profile.NoveltyTolerance
	breakdown["novelty"] = novelty

	// ── TIE STRENGTH (precomputed nightly) ──
	// Personal graph strength to this creator — chat freq, follow, profile dwell.
	// Reads Redis HASH tie:{user} written by computeTieStrengths().
	tieStrength := getTieStrength(profile.UserID, cs.CreatorID)
	tieBoost := tieStrength * 0.25 // up to +0.25 for the strongest ties
	breakdown["tieStrength"] = tieBoost

	// ── CREATOR AFFINITY (precomputed nightly) ──
	// Long-term engagement strength with this creator — loops, completions, likes.
	// Reads Redis KEY creator_affinity:{user} written by computeCreatorAffinity().
	creatorAff := getCreatorAffinity(profile.UserID, cs.CreatorID)
	creatorAffinityBoost := creatorAff * 0.20
	breakdown["creatorAffinityBoost"] = creatorAffinityBoost

	// ── DWELL-WEIGHTED INTENT (precomputed nightly) ──
	// Users who linger on challenge-detail pages express deeper intent than
	// reflex-skimmers. Nudge challenge content up for those users.
	dwellBoost := 0.0
	if cs.ContentType == "challenge" {
		if avgMs := getPageDwellMs(profile.UserID, "challenge_detail_page"); avgMs > 4000 {
			// 4s = casual glance, 10s+ = real read. Cap the boost at 0.1.
			over := float64(avgMs-4000) / 6000.0
			if over > 1 {
				over = 1
			}
			dwellBoost = 0.10 * over
		}
	}
	breakdown["dwellIntentBoost"] = dwellBoost

	// ── SOCIAL-DRIVE WEIGHTING ──
	// High SocialDrive users benefit more from social signal and tie-strength;
	// low-SocialDrive users get a flatter blend (they prefer stranger content).
	// SocialDrive is written to user_profiles by computeSocialDrive() so
	// profile.SocialDrive is already the fresh value.
	socialWeightMult := 0.7 + 0.6*profile.SocialDrive // range 0.7 .. 1.3
	breakdown["socialDriveMult"] = socialWeightMult

	// ── BASE SCORE ──
	baseScore := social*wSocial*socialWeightMult + freshness*wFreshness + energyFit*wEnergyFit +
		relevance*wRelevance + quality*wQuality + novelty*wNovelty +
		tieBoost + creatorAffinityBoost + dwellBoost

	// ── EGO BOOST (conditional) ──
	// After a loss, boost content that validates the user
	egoBonus := 0.0
	if profile.RecentLosses > profile.RecentWins && profile.EgoSensitivity > 0.5 {
		// User is in a loss streak and ego-sensitive
		// Boost: content from lower-league creators (easy comparison),
		// content in categories they're good at
		if affinity, ok := profile.CategoryAffinity[cs.Category]; ok && affinity > 0.7 {
			egoBonus = 0.15 * profile.EgoSensitivity
		}
		// Content from lower leagues = "I'm better than this" feeling
		leagueTier := map[string]int{"Bronze": 1, "Silver": 2, "Gold": 3, "Platinum": 4, "Diamond": 5}
		var userLeague string
		db.QueryRow(`SELECT league FROM users WHERE CAST(id AS TEXT) = $1`, profile.UserID).Scan(&userLeague)
		if leagueTier[userLeague] > leagueTier[cs.CreatorLeague] {
			egoBonus += 0.1 * profile.EgoSensitivity
		}
	}
	breakdown["egoBoost"] = egoBonus

	// ── FATIGUE PENALTY (conditional) ──
	// If user has seen too much of this category in current session, penalize
	fatiguePenalty := 0.0
	if session.CategoriesSeen != nil {
		seen := session.CategoriesSeen[cs.Category]
		if seen >= 3 {
			fatiguePenalty = -0.15 * float64(seen-2) // Increasing penalty
			if fatiguePenalty < -0.4 {
				fatiguePenalty = -0.4 // Hard cap at -0.4
			}
		}
	}
	breakdown["fatiguePenalty"] = fatiguePenalty

	// ── DOPAMINE PENALTY (conditional) ──
	// Low dopamine budget = user is fatigued, penalize intense content
	dopaminePenalty := 0.0
	if session.DopamineBudget < 0.3 {
		// Penalize high-energy content when user is tired
		dopaminePenalty = -0.2 * cs.EnergyLevel * (1.0 - session.DopamineBudget/0.3)
	}
	breakdown["dopaminePenalty"] = dopaminePenalty

	// ── UNSEEN BONUS ──
	// Content the user has never interacted with gets a boost
	unseenBonus := 0.0
	contentKey := cs.ContentType + ":" + cs.ContentID
	if !interactedIDs[contentKey] {
		unseenBonus = 0.15
	}
	breakdown["unseenBonus"] = unseenBonus

	// ── COLD CONTENT BONUS ──
	// New content with few views gets a visibility boost to gather engagement data
	coldContentBonus := 0.0
	if cs.ViewCount < contentColdThreshold {
		coldContentBonus = 0.2 * (1.0 - float64(cs.ViewCount)/float64(contentColdThreshold))
	}
	breakdown["coldContentBonus"] = coldContentBonus

	// ── TRENDING BONUS ──
	trendingBonus := cs.TrendingScore * 0.15
	breakdown["trendingBonus"] = trendingBonus

	// ── CONTEXT-AWARE: HOUR CATEGORY MATCH ──
	// Boost content that matches what the user prefers at this hour
	hourBonus := 0.0
	currentHour := time.Now().Hour()
	if preferredCat, ok := profile.CategoryByHour[currentHour]; ok && preferredCat == cs.Category {
		hourBonus = 0.12 // User historically likes this category at this hour
	}
	// Also adjust energy fit based on hourly energy preference
	if hourEnergy, ok := profile.EnergyByHour[currentHour]; ok {
		// Blend hourly energy with base energy preference (70/30 in favor of hour)
		adjustedEnergy := hourEnergy*0.7 + profile.EnergyPreference*0.3
		hourEnergyFit := 1.0 - math.Abs(adjustedEnergy-cs.EnergyLevel)
		if hourEnergyFit > energyFit {
			hourBonus += (hourEnergyFit - energyFit) * 0.08
		}
	}
	breakdown["hourBonus"] = hourBonus

	// ── CONTEXT-AWARE: EMOTION MATCH ──
	// Boost content whose emotion vector matches user's emotion preference
	emotionBonus := 0.0
	if len(profile.EmotionPreference) > 0 && len(cs.EmotionVector) > 0 {
		bestMatch := 0.0
		for tag := range cs.EmotionVector {
			if pref, ok := profile.EmotionPreference[tag]; ok && pref > bestMatch {
				bestMatch = pref
			}
		}
		emotionBonus = bestMatch * 0.1 // Up to 0.1 bonus for perfect emotion match
	}
	breakdown["emotionBonus"] = emotionBonus

	// ── CONTEXT-AWARE: EGO STATE CATEGORY PREFERENCE ──
	// After wins/losses, boost categories the user gravitates toward in that state
	egoContextBonus := 0.0
	if profile.RecentWins > profile.RecentLosses {
		if winCats, ok := profile.CategoryByEgo["winning"]; ok {
			if score, ok := winCats[cs.Category]; ok {
				// Normalize: divide by max to get 0-1
				maxScore := 0.0
				for _, s := range winCats {
					if s > maxScore {
						maxScore = s
					}
				}
				if maxScore > 0 {
					egoContextBonus = (score / maxScore) * 0.08
				}
			}
		}
	} else if profile.RecentLosses > profile.RecentWins {
		if lossCats, ok := profile.CategoryByEgo["losing"]; ok {
			if score, ok := lossCats[cs.Category]; ok {
				maxScore := 0.0
				for _, s := range lossCats {
					if s > maxScore {
						maxScore = s
					}
				}
				if maxScore > 0 {
					egoContextBonus = (score / maxScore) * 0.08
				}
			}
		}
	}
	breakdown["egoContextBonus"] = egoContextBonus

	// ── COLLABORATIVE FILTERING BONUS ──
	// "Users similar to you also liked this"
	collabBonus := getCollaborativeBonus(profile.UserID, cs.ContentID, cs.ContentType)
	breakdown["collabBonus"] = collabBonus

	// ── WELLBEING: EMOTION SPIRAL PROTECTION ──
	// Detect if user is spiraling into negative emotions and inject counterweight.
	// If they've consumed 3+ "sad"/"scary"/"aggressive" in a row, boost positive content.
	wellbeingBonus := 0.0
	negativeEmotions := map[string]bool{"sad": true, "scary": true, "aggressive": true, "serious": true}
	positiveEmotions := map[string]bool{"happy": true, "inspiring": true, "funny": true, "chill": true}
	if len(session.LastEmotions) >= 3 {
		negativeStreak := 0
		for i := len(session.LastEmotions) - 1; i >= 0 && i >= len(session.LastEmotions)-5; i-- {
			if negativeEmotions[session.LastEmotions[i]] {
				negativeStreak++
			} else {
				break
			}
		}
		if negativeStreak >= 3 {
			// User is in a negative spiral — boost positive content
			for tag := range cs.EmotionVector {
				if positiveEmotions[tag] {
					wellbeingBonus = 0.25 // Strong boost for happy/inspiring/funny content
					break
				}
			}
			// Penalize more negative content to break the cycle
			for tag := range cs.EmotionVector {
				if negativeEmotions[tag] {
					wellbeingBonus = -0.15
					break
				}
			}
		}
	}
	breakdown["wellbeingBonus"] = wellbeingBonus

	// ── WIN/LOSS MOMENTUM ──
	// Win streaks → boost competitive/challenge content to ride the high
	// Loss streaks → boost recovery content (ego boost, chill, inspiring)
	// Neutral → no adjustment
	momentumBonus := 0.0
	winDiff := profile.RecentWins - profile.RecentLosses
	if winDiff >= 3 {
		// Strong win streak — user is confident, feed competitive fire
		if cs.ContentType == "challenge" {
			momentumBonus = 0.18 // Push challenges hard
		}
		if cs.EnergyLevel > 0.6 {
			momentumBonus += 0.08 // High-energy content matches confident mood
		}
		// Show rival content — "keep proving yourself"
		if cs.CreatorWinRate > 0.5 {
			momentumBonus += 0.06 // Content from other winners = worthy opponents
		}
	} else if winDiff >= 1 {
		// Mild win streak
		if cs.ContentType == "challenge" {
			momentumBonus = 0.10
		}
	} else if winDiff <= -3 {
		// Strong loss streak — user needs recovery, not more defeats
		// Boost: inspiring, chill, ego-validating content
		for tag := range cs.EmotionVector {
			if tag == "inspiring" || tag == "empowering" || tag == "wholesome" {
				momentumBonus = 0.15
				break
			}
		}
		if cs.EnergyLevel < 0.4 {
			momentumBonus += 0.08 // Chill content helps recovery
		}
		// Penalize challenges during loss streak (unless ego sensitivity is low)
		if cs.ContentType == "challenge" && profile.EgoSensitivity > 0.3 {
			momentumBonus -= 0.12
		}
	} else if winDiff <= -1 {
		// Mild loss streak
		for tag := range cs.EmotionVector {
			if tag == "inspiring" || tag == "empowering" {
				momentumBonus = 0.08
				break
			}
		}
	}
	breakdown["momentumBonus"] = momentumBonus

	// ── VARIABLE REWARD (addiction mechanic) ──
	// Slot-machine psychology: occasionally inject an unexpectedly high-scoring item.
	// This creates unpredictability — users keep scrolling because the NEXT item
	// might be a "jackpot". Key insight from behavioral psychology: variable ratio
	// reinforcement is the most addictive reward schedule.
	//
	// Every ~7th item has a chance of being a "jackpot" if it meets quality thresholds.
	// The unpredictability is what hooks — if every 7th item was boosted, it'd be predictable.
	variableReward := 0.0
	if session.ItemsSeen > 0 && cs.QualityScore > 0.6 {
		// Use a hash of session + items seen to create pseudo-random trigger points
		// This makes it deterministic per session but feels random to the user
		rewardSeed := (session.ItemsSeen * 7 + len(cs.ContentID)) % 11
		if rewardSeed == 0 || rewardSeed == 4 || rewardSeed == 9 {
			// ~27% of items that pass quality threshold get a jackpot boost
			variableReward = 0.20
			// Extra boost if it matches user preference — makes the "jackpot" feel earned
			if affinity, ok := profile.CategoryAffinity[cs.Category]; ok && affinity > 0.5 {
				variableReward = 0.28
			}
		}
	}
	breakdown["variableReward"] = variableReward

	// ── RE-ENTRY BONUS (retention mechanic) ──
	// When a user comes back after being away, make the first page exceptional.
	// This creates a positive association: "every time I open the app, the content is great."
	// The bonus decays over the session so it doesn't inflate everything.
	reentryBonus := 0.0
	if session.ItemsSeen < 5 {
		// First 5 items of session: boost high-quality, high-relevance content
		if cs.QualityScore > 0.5 {
			reentryBonus = 0.15 * (1.0 - float64(session.ItemsSeen)/5.0)
		}
		// Extra boost for social content on re-entry — "see what your friends did"
		if followingSet[cs.CreatorID] {
			reentryBonus += 0.10 * (1.0 - float64(session.ItemsSeen)/5.0)
		}
	}
	breakdown["reentryBonus"] = reentryBonus

	// ── STREAK BONUS (daily engagement) ──
	// Users with consecutive daily sessions get slightly better content surfacing.
	// This rewards loyalty and creates a "don't break the streak" psychology.
	streakBonus := 0.0
	if profile.TotalSessions > 3 {
		// Long-term users get a quality boost — we surface the best content for loyalists
		streakMultiplier := math.Min(1.0, float64(profile.TotalSessions)/30.0) // Caps at 30 sessions
		streakBonus = cs.QualityScore * 0.05 * streakMultiplier
	}
	breakdown["streakBonus"] = streakBonus

	// ── IMPRESSION BOUNCE PENALTY ──
	// If user has been bouncing (scrolling past quickly) on this category recently,
	// penalize showing more of it. This is the "I keep ignoring dance content" signal.
	impressionPenalty := 0.0
	if byCat, ok := impressionStatsCache.Get(profile.UserID); ok {
		if stats, exists := byCat[cs.Category]; exists && stats.Count >= minCategoryImpressions {
			br := stats.BounceRate()
			if br > bounceRateNegativeThreshold {
				// Stronger penalty for higher bounce rates, capped at -0.25
				impressionPenalty = -0.25 * ((br - bounceRateNegativeThreshold) / (1.0 - bounceRateNegativeThreshold))
			} else if br < bounceRatePositiveThreshold && stats.InterestCount > 0 {
				// User lingers on this category → positive signal
				impressionPenalty = 0.12 * (1.0 - br/bounceRatePositiveThreshold)
			}
		}
	}
	breakdown["impressionBouncePenalty"] = impressionPenalty

	// ── SCROLL-BACK BONUS ──
	// Content from creators the user has scrolled back to recently = strong interest.
	// Cached per user to avoid hitting DB per-item.
	scrollBackBonus := 0.0
	if scrollBackCreators, ok := scrollBackCache.Get(profile.UserID); ok {
		if scrollBackCreators[cs.CreatorID] {
			scrollBackBonus = 0.20
		}
	}
	breakdown["scrollBackBonus"] = scrollBackBonus

	// ── COMPLETION BONUS ──
	// Creators whose content this user has completed (watched 95%+) before = strong signal.
	completeBonus := 0.0
	if completedCreators, ok := completionCache.Get(profile.UserID); ok {
		if count, exists := completedCreators[cs.CreatorID]; exists {
			// Scale: 1 completion = +0.08, 2+ = +0.15, 5+ = +0.20
			switch {
			case count >= 5:
				completeBonus = 0.20
			case count >= 2:
				completeBonus = 0.15
			default:
				completeBonus = 0.08
			}
		}
	}
	breakdown["completeBonus"] = completeBonus

	// ── LOOP BONUS ──
	// Content in categories user has looped videos in = they enjoy this category enough to rewatch.
	loopBonus := 0.0
	if loopCats, ok := loopCache.Get(profile.UserID); ok {
		if loopCats[cs.Category] {
			loopBonus = 0.10
		}
	}
	breakdown["loopBonus"] = loopBonus

	// ── UNMUTE BONUS ──
	// Creators user has unmuted for = they want to HEAR this creator.
	// Very strong signal since most feed videos autoplay muted.
	unmuteBonus := 0.0
	if unmuteCreators, ok := unmuteCache.Get(profile.UserID); ok {
		if unmuteCreators[cs.CreatorID] {
			unmuteBonus = 0.12
		}
	}
	breakdown["unmuteBonus"] = unmuteBonus

	// ── PROFILE VISIT BONUS ──
	// Content from creators user has visited the profile of recently.
	profileVisitBonus := 0.0
	if visitedCreators, ok := profileVisitCache.Get(profile.UserID); ok {
		if visitedCreators[cs.CreatorID] {
			profileVisitBonus = 0.15
		}
	}
	breakdown["profileVisitBonus"] = profileVisitBonus

	// ── FINAL SCORE ──
	finalScore := baseScore + egoBonus + fatiguePenalty + dopaminePenalty +
		unseenBonus + coldContentBonus + trendingBonus +
		hourBonus + emotionBonus + egoContextBonus + wellbeingBonus +
		collabBonus + momentumBonus + variableReward + reentryBonus + streakBonus +
		impressionPenalty + scrollBackBonus + completeBonus + loopBonus +
		unmuteBonus + profileVisitBonus

	breakdown["baseScore"] = baseScore
	breakdown["finalScore"] = finalScore

	return finalScore, breakdown
}

// ════════════════════════════════════════════════════════════════════════════════
// LAYER 6: FEED COMPOSITION
// ════════════════════════════════════════════════════════════════════════════════
//
// WHY slot-based composition instead of pure ranking:
//
// Pure ranking: Sort by score, return top N. Problem: you get 20 comedy videos
// in a row because the user likes comedy. Feed feels monotonous. User leaves.
//
// Slot-based: Define a PATTERN of content types, then fill each slot with the
// best-scoring item of that type. The feed has rhythm — like a DJ set that
// alternates bangers with cool-down tracks.
//
// This is how TikTok's feed actually works. They don't just sort by predicted
// engagement. They compose a sequence that maximizes SESSION duration, not
// per-video engagement.

// getFeedPattern returns the slot pattern for the current page.
// The pattern adapts based on user state (fatigue, resistance, ego).
func getFeedPattern(profile *UserProfile, session *SessionState, pageSize int) []string {
	// Base pattern: 12-slot repeating unit
	// Designed like a DJ set: hook→engage→breathe→surprise→build→release
	// Position 1: Hook — grab attention immediately
	// Position 2: Social — leverage trust from friends
	// Position 3: Trending — "everyone's watching this" social proof
	// Position 4: Surprise — wildcard to break predictability (variable reward)
	// Position 5: Hook — re-engage with known preference
	// Position 6: Discovery — push boundary gently while engaged
	// Position 7: Challenge — competitive drive activation
	// Position 8: Cliffhang — "what happens next?" creates anticipation
	// Position 9: Cooldown — let dopamine settle before next wave
	// Position 10: Ego boost — validate user, feels good
	// Position 11: Rival — competitive fire, drives re-engagement
	// Position 12: Hook — strong closer that makes them want page 2
	basePattern := []string{
		slotHook, slotSocial, slotTrending, slotSurprise,
		slotHook, slotDiscovery, slotChallenge, slotCliffhang,
		slotCooldown, slotEgoBoost, slotRival, slotHook,
	}

	// Adapt pattern based on user state — each state has a unique rhythm
	switch {
	case session.ResistanceLevel >= 3:
		// Emergency: user is about to leave — pull out all stops
		// Heavy on surprise + rival + ego to trigger curiosity and emotion
		basePattern = []string{
			slotSurprise, slotRival, slotEgoBoost, slotSurprise,
			slotSocial, slotCliffhang, slotSurprise, slotHook,
			slotRival, slotSurprise, slotEgoBoost, slotCliffhang,
		}

	case session.ResistanceLevel >= 2:
		// User is resisting — switch strategy based on what works
		switch session.CurrentStrategy {
		case strategySocial:
			basePattern = []string{
				slotSocial, slotSocial, slotHook, slotSurprise,
				slotSocial, slotCliffhang, slotSocial, slotTrending,
				slotSocial, slotHook, slotRival, slotSocial,
			}
		case strategyTrending:
			basePattern = []string{
				slotTrending, slotTrending, slotHook, slotSurprise,
				slotTrending, slotCliffhang, slotTrending, slotSocial,
				slotTrending, slotHook, slotRival, slotTrending,
			}
		case strategyDiscovery:
			basePattern = []string{
				slotDiscovery, slotHook, slotDiscovery, slotSurprise,
				slotDiscovery, slotCliffhang, slotTrending, slotDiscovery,
				slotHook, slotDiscovery, slotRival, slotDiscovery,
			}
		case strategyCalming:
			// Fatigued/frustrated — dial everything down. Nearly all cooldown
			// + nostalgic (proven favorites) to rebuild trust gently.
			basePattern = []string{
				slotCooldown, slotNostalgic, slotCooldown, slotSocial,
				slotCooldown, slotCliffhang, slotCooldown, slotNostalgic,
				slotCooldown, slotFavCreator, slotCooldown, slotHook,
			}
		case strategyCompetitive:
			// Competitive personality — battles and rivals dominate.
			basePattern = []string{
				slotChallenge, slotRival, slotChallenge, slotHook,
				slotRival, slotChallenge, slotTrending, slotRival,
				slotChallenge, slotEgoBoost, slotRival, slotChallenge,
			}
		case strategyCreatorFocus:
			// Loyalist — deep-dive on their favorite creators + a sprinkle of new
			// creators to prevent total lock-in.
			basePattern = []string{
				slotFavCreator, slotFavCreator, slotHook, slotFavCreator,
				slotSocial, slotFavCreator, slotCliffhang, slotFavCreator,
				slotDiscovery, slotFavCreator, slotFavCreator, slotHook,
			}
		case strategyNostalgic:
			// Last-resort re-engagement — replay content user has loved before.
			basePattern = []string{
				slotNostalgic, slotNostalgic, slotHook, slotNostalgic,
				slotCliffhang, slotNostalgic, slotFavCreator, slotNostalgic,
				slotHook, slotNostalgic, slotEgoBoost, slotNostalgic,
			}
		case strategyFreshBlood:
			// Bored explorer — feed them only content they've literally never
			// seen in any feed before.
			basePattern = []string{
				slotFreshBlood, slotFreshBlood, slotSurprise, slotFreshBlood,
				slotFreshBlood, slotCliffhang, slotFreshBlood, slotHook,
				slotFreshBlood, slotDiscovery, slotFreshBlood, slotSurprise,
			}
		case strategyMoodMatch:
			// Pattern driven entirely by detected mood.
			basePattern = moodDrivenPattern(session.DetectedMood)
		}

	case session.DopamineBudget < 0.2:
		// Fatigued — gentle rhythm: lots of cooldown + cliffhang to create curiosity without intensity
		basePattern = []string{
			slotHook, slotCooldown, slotSocial, slotCooldown,
			slotCliffhang, slotCooldown, slotHook, slotCooldown,
			slotSurprise, slotCooldown, slotEgoBoost, slotCooldown,
		}

	case session.DopamineBudget < 0.5:
		// Moderate fatigue — reduce intensity, keep engagement via curiosity
		basePattern = []string{
			slotHook, slotSocial, slotCooldown, slotCliffhang,
			slotHook, slotDiscovery, slotCooldown, slotSurprise,
			slotEgoBoost, slotSocial, slotCooldown, slotHook,
		}

	case profile.RecentLosses > profile.RecentWins && profile.EgoSensitivity > 0.5:
		// Post-loss ego-sensitive — heavy ego boost, show rivals to reignite competitive fire
		basePattern = []string{
			slotHook, slotEgoBoost, slotSocial, slotEgoBoost,
			slotHook, slotCliffhang, slotEgoBoost, slotCooldown,
			slotRival, slotEgoBoost, slotSurprise, slotHook,
		}

	case profile.RecentWins > profile.RecentLosses+2:
		// Win streak — user is confident, feed them challenges and rivals to ride momentum
		basePattern = []string{
			slotHook, slotChallenge, slotRival, slotTrending,
			slotHook, slotChallenge, slotSocial, slotCliffhang,
			slotRival, slotDiscovery, slotChallenge, slotHook,
		}

	case profile.NoveltyTolerance > 0.7:
		// Explorer — heavy discovery + surprise, sprinkle cliffhangs for "one more" feeling
		basePattern = []string{
			slotHook, slotDiscovery, slotDiscovery, slotSurprise,
			slotHook, slotDiscovery, slotChallenge, slotCliffhang,
			slotDiscovery, slotTrending, slotSurprise, slotDiscovery,
		}

	case profile.SocialDrive > 0.7:
		// Highly social user — friends' content dominates, rivals add spice
		basePattern = []string{
			slotSocial, slotSocial, slotHook, slotSurprise,
			slotSocial, slotChallenge, slotSocial, slotCliffhang,
			slotRival, slotSocial, slotTrending, slotHook,
		}

	case profile.EgoSensitivity > 0.7 && profile.RecentWins >= profile.RecentLosses:
		// Ego-driven winner — mix ego boosts with challenges to keep them proving themselves
		basePattern = []string{
			slotHook, slotEgoBoost, slotChallenge, slotTrending,
			slotHook, slotRival, slotEgoBoost, slotCliffhang,
			slotChallenge, slotSurprise, slotEgoBoost, slotHook,
		}
	}

	// Extend pattern to fill page
	pattern := make([]string, pageSize)
	for i := 0; i < pageSize; i++ {
		pattern[i] = basePattern[i%len(basePattern)]
	}
	return pattern
}

// moodDrivenPattern returns a 12-slot pattern tuned to the user's detected
// mood. Used by the mood_match strategy and also as a modifier elsewhere.
func moodDrivenPattern(mood string) []string {
	switch mood {
	case "frustrated":
		// Rebuild trust — calm, familiar, known-good content
		return []string{
			slotNostalgic, slotCooldown, slotFavCreator, slotCooldown,
			slotNostalgic, slotSocial, slotCooldown, slotNostalgic,
			slotFavCreator, slotCooldown, slotCliffhang, slotNostalgic,
		}
	case "bored":
		// Novelty blast — only new creators and surprise content
		return []string{
			slotFreshBlood, slotSurprise, slotFreshBlood, slotDiscovery,
			slotSurprise, slotFreshBlood, slotCliffhang, slotFreshBlood,
			slotSurprise, slotFreshBlood, slotHook, slotSurprise,
		}
	case "energetic":
		// Match their energy with intensity + competition
		return []string{
			slotHook, slotChallenge, slotRival, slotTrending,
			slotHook, slotChallenge, slotSurprise, slotRival,
			slotChallenge, slotTrending, slotHook, slotRival,
		}
	case "chill":
		// Steady, comfortable rhythm — social + cooldown
		return []string{
			slotSocial, slotCooldown, slotFavCreator, slotSocial,
			slotCooldown, slotCliffhang, slotSocial, slotCooldown,
			slotFavCreator, slotSocial, slotCooldown, slotHook,
		}
	case "curious":
		// They're scanning — give them variety worth scanning
		return []string{
			slotDiscovery, slotFreshBlood, slotSurprise, slotDiscovery,
			slotCliffhang, slotDiscovery, slotHook, slotFreshBlood,
			slotSurprise, slotDiscovery, slotTrending, slotDiscovery,
		}
	case "engaged":
		// Keep them engaged — hooks + cliffhangs to extend session
		return []string{
			slotHook, slotCliffhang, slotSocial, slotHook,
			slotTrending, slotCliffhang, slotHook, slotChallenge,
			slotCliffhang, slotHook, slotSurprise, slotCliffhang,
		}
	}
	// Unknown mood — neutral mix
	return []string{
		slotHook, slotSocial, slotTrending, slotSurprise,
		slotHook, slotDiscovery, slotChallenge, slotCliffhang,
		slotCooldown, slotEgoBoost, slotRival, slotHook,
	}
}

// composeFeed takes scored items and arranges them into the slot pattern.
// Each slot is filled with the best available item matching that slot type.
func composeFeed(scored []ScoredItem, pattern []string, followingSet map[string]bool) []ScoredItem {
	// Bucket items by their best-fit slot type
	buckets := map[string][]ScoredItem{
		slotHook:      {},
		slotSocial:    {},
		slotDiscovery: {},
		slotTrending:  {},
		slotChallenge: {},
		slotCooldown:  {},
		slotEgoBoost:  {},
		slotCliffhang: {},
		slotSurprise:  {},
		slotRival:     {},
	}

	for _, item := range scored {
		// Classify item into slot buckets based on its characteristics
		bd := item.ScoreBreakdown

		// Hook: highest overall score — everything is a candidate
		buckets[slotHook] = append(buckets[slotHook], item)

		// Social: content from followed creators
		if bd["social"] > 0.5 {
			buckets[slotSocial] = append(buckets[slotSocial], item)
		}

		// Discovery: new category/creator for user
		if bd["novelty"] > 0.3 {
			buckets[slotDiscovery] = append(buckets[slotDiscovery], item)
		}

		// Trending: high trending score
		if bd["trendingBonus"] > 0.05 {
			buckets[slotTrending] = append(buckets[slotTrending], item)
		}

		// Challenge: challenge content type
		if item.Item.Type == "challenge" {
			buckets[slotChallenge] = append(buckets[slotChallenge], item)
		}

		// Cooldown: low energy content — gentle, relaxing items
		if bd["energyFit"] > 0.7 && item.Item.Type == "post" {
			buckets[slotCooldown] = append(buckets[slotCooldown], item)
		}

		// Ego boost: content that validates the user
		if bd["egoBoost"] > 0 {
			buckets[slotEgoBoost] = append(buckets[slotEgoBoost], item)
		}

		// Cliffhang: content with suspense/sequel emotion tags — creates "what happens next?" urge
		// Suspenseful, surprising, or multi-part content that pulls them forward
		if hasEmotionTag(item, "suspenseful") || hasEmotionTag(item, "surprising") || hasEmotionTag(item, "intense") {
			buckets[slotCliffhang] = append(buckets[slotCliffhang], item)
		}

		// Surprise: wildcard — high quality content the user wouldn't normally see
		// Opposite of their usual preferences but objectively engaging
		if bd["novelty"] > 0.5 && bd["quality"] > 0.5 {
			buckets[slotSurprise] = append(buckets[slotSurprise], item)
		} else if bd["collabBonus"] > 0.1 && bd["relevance"] < 0.3 {
			// Similar users liked it but it's outside this user's normal categories
			buckets[slotSurprise] = append(buckets[slotSurprise], item)
		}

		// Rival: content from competitors — someone they lost to or a close-league creator
		// Drives re-engagement through competitive fire
		if item.Item.Type == "challenge" && bd["egoContextBonus"] > 0 {
			buckets[slotRival] = append(buckets[slotRival], item)
		} else if bd["social"] < 0.1 && item.Item.Type == "challenge" {
			// Non-friend challenge creators are potential rivals
			buckets[slotRival] = append(buckets[slotRival], item)
		}
	}

	// Sort each bucket by score (descending)
	for _, bucket := range buckets {
		sort.Slice(bucket, func(i, j int) bool {
			return bucket[i].Score > bucket[j].Score
		})
	}

	// Fill slots from pattern
	used := make(map[string]bool)   // Track used content IDs
	creatorCount := make(map[string]int) // Diversity: max items per creator
	result := make([]ScoredItem, 0, len(pattern))
	bucketIdx := make(map[string]int) // Current index in each bucket

	for _, slot := range pattern {
		bucket := buckets[slot]
		idx := bucketIdx[slot]

		filled := false
		for idx < len(bucket) {
			item := bucket[idx]
			idx++
			contentKey := item.Item.Type + ":" + getItemID(item.Item)
			creatorID := getItemCreatorID(item.Item)

			// Skip if already used or creator over-represented
			if used[contentKey] {
				continue
			}
			if creatorCount[creatorID] >= maxItemsPerCreator {
				continue
			}

			item.SlotType = slot
			result = append(result, item)
			used[contentKey] = true
			creatorCount[creatorID]++
			filled = true
			break
		}
		bucketIdx[slot] = idx

		// If no item found for this slot, fall back to hook bucket
		if !filled && slot != slotHook {
			hookIdx := bucketIdx[slotHook]
			for hookIdx < len(buckets[slotHook]) {
				item := buckets[slotHook][hookIdx]
				hookIdx++
				contentKey := item.Item.Type + ":" + getItemID(item.Item)
				creatorID := getItemCreatorID(item.Item)
				if used[contentKey] || creatorCount[creatorID] >= maxItemsPerCreator {
					continue
				}
				item.SlotType = slot
				result = append(result, item)
				used[contentKey] = true
				creatorCount[creatorID]++
				break
			}
			bucketIdx[slotHook] = hookIdx
		}
	}

	return result
}

func getItemID(item HomeFeedItem) string {
	if item.Challenge != nil {
		return item.Challenge.ID
	}
	if item.Post != nil {
		return item.Post.ID
	}
	return ""
}

func getItemCreatorID(item HomeFeedItem) string {
	if item.Challenge != nil {
		return item.Challenge.CreatorID
	}
	if item.Post != nil {
		return item.Post.AuthorID
	}
	return ""
}

// hasEmotionTag checks if a scored item's content has a specific emotion tag.
func hasEmotionTag(item ScoredItem, tag string) bool {
	if item.Item.Challenge != nil {
		for _, t := range item.Item.Challenge.EmotionTags {
			if t == tag {
				return true
			}
		}
	}
	if item.Item.Post != nil {
		for _, t := range item.Item.Post.EmotionTags {
			if t == tag {
				return true
			}
		}
	}
	return false
}

// ════════════════════════════════════════════════════════════════════════════════
// LAYER 7: COLD START
// ════════════════════════════════════════════════════════════════════════════════
//
// WHY cold start is the hardest problem:
//
// New user → no event history → no profile → can't score anything → show garbage
// → user leaves → never comes back. This is the "cold start death spiral."
//
// TikTok's solution (and ours):
// 1. Show DIVERSE popular content to new users (popularity is a safe default)
// 2. Measure their reactions to the first 10-15 items VERY carefully
// 3. After ~15 interactions, we have enough to build a basic profile
// 4. Each additional interaction improves accuracy
//
// For NEW CONTENT (cold content):
// 1. Show it to a small random audience (~50 users)
// 2. If engagement is above average → amplify to larger audience
// 3. If engagement is below average → reduce visibility
// This is TikTok's "small pool test" — every video gets a fair chance.

func isColdStartUser(profile *UserProfile) bool {
	return profile.EventCount < coldStartThreshold
}

// coldStartFeed returns a diversity-optimized feed for new users.
// Uses popularity + diversity instead of personalization.
func coldStartFeed(userID string, page, limit int) ([]HomeFeedItem, bool, error) {
	offset := (page - 1) * limit

	// Get popular challenges (by views + likes, recent)
	challengeRows, err := db.Query(`
		SELECT c.id, c.creator_id, u.username, u.league, c.video_url,
			c.thumbnail_url, c.prefix, c.subject, c.visibility, c.status,
			c.views, COALESCE(cl.likes,0), c.created_at,
			COALESCE(c.created_at + INTERVAL '24 hours', NOW()) as expires_at,
			(SELECT COUNT(*) FROM challenge_responses WHERE challenge_id = c.id) as response_count
		FROM challenges c
		JOIN users u ON c.creator_id = u.id
		LEFT JOIN (SELECT challenge_id, COUNT(*) as likes FROM challenge_likes GROUP BY challenge_id) cl
			ON cl.challenge_id = c.id
		WHERE c.visibility = 'arena' AND c.status IN ('open','active','completed')
		AND c.created_at > NOW() - INTERVAL '14 days'
		ORDER BY (c.views + COALESCE(cl.likes,0) * 3) DESC, c.created_at DESC
		LIMIT $1 OFFSET $2`, limit/2, offset/2)
	if err != nil {
		return nil, false, err
	}
	defer challengeRows.Close()

	var items []HomeFeedItem
	for challengeRows.Next() {
		var ch Challenge
		var creatorID int
		var views int
		var likes int
		var createdAt, expiresAt time.Time
		var responseCount int

		challengeRows.Scan(&ch.ID, &creatorID, &ch.CreatorUsername, &ch.CreatorLeague,
			&ch.VideoURL, &ch.ThumbnailURL, &ch.Prefix, &ch.Subject,
			&ch.Visibility, &ch.Status, &views, &likes,
			&createdAt, &expiresAt, &responseCount)

		ch.CreatorID = strconv.Itoa(creatorID)
		ch.Views = views
		ch.Likes = likes
		ch.CreatedAt = createdAt.Format(time.RFC3339)
		ch.ExpiresAt = expiresAt.Format(time.RFC3339)
		ch.ResponseCount = responseCount

		items = append(items, HomeFeedItem{Type: "challenge", Challenge: &ch})
	}

	// Get popular posts
	postRows, err := db.Query(`
		SELECT p.id, p.author_id, u.username, u.league, p.type, p.content_url,
			p.thumbnail_url, p.caption, p.views, p.created_at,
			COALESCE(pl.likes,0) as likes,
			(SELECT COUNT(*) FROM comments WHERE post_id = p.id) as comment_count
		FROM posts p
		JOIN users u ON p.author_id = u.id
		LEFT JOIN (SELECT post_id, COUNT(*) as likes FROM post_likes GROUP BY post_id) pl
			ON pl.post_id = p.id
		WHERE p.created_at > NOW() - INTERVAL '14 days'
		ORDER BY (p.views + COALESCE(pl.likes,0) * 3) DESC, p.created_at DESC
		LIMIT $1 OFFSET $2`, limit/2, offset/2)
	if err != nil {
		return nil, false, err
	}
	defer postRows.Close()

	for postRows.Next() {
		var post Post
		var authorID int
		var createdAt time.Time

		postRows.Scan(&post.ID, &authorID, &post.AuthorUsername, &post.AuthorLeague,
			&post.Type, &post.ContentURL, &post.ThumbnailURL, &post.Caption,
			&post.Views, &createdAt, &post.Likes, &post.Comments)

		post.AuthorID = strconv.Itoa(authorID)
		post.CreatedAt = createdAt.Format(time.RFC3339)

		items = append(items, HomeFeedItem{Type: "post", Post: &post})
	}

	// Shuffle to ensure diversity (don't show all challenges then all posts)
	rand.Shuffle(len(items), func(i, j int) {
		items[i], items[j] = items[j], items[i]
	})

	// Enforce diversity: max 2 consecutive items of same type
	items = enforceDiversity(items)

	hasMore := len(items) > limit
	if hasMore {
		items = items[:limit]
	}

	return items, hasMore, nil
}

// enforceDiversity ensures no more than 2 consecutive items of the same type.
func enforceDiversity(items []HomeFeedItem) []HomeFeedItem {
	if len(items) <= 2 {
		return items
	}

	result := []HomeFeedItem{items[0]}
	sameCount := 1

	for i := 1; i < len(items); i++ {
		if items[i].Type == result[len(result)-1].Type {
			sameCount++
			if sameCount > 2 {
				// Find next item of different type and swap
				for j := i + 1; j < len(items); j++ {
					if items[j].Type != items[i].Type {
						items[i], items[j] = items[j], items[i]
						sameCount = 1
						break
					}
				}
				if sameCount > 2 {
					continue // Skip if no different type found
				}
			}
		} else {
			sameCount = 1
		}
		result = append(result, items[i])
	}

	return result
}

// ════════════════════════════════════════════════════════════════════════════════
// LAYER 8: API HANDLERS
// ════════════════════════════════════════════════════════════════════════════════

// SmartFeedHandler is the main personalized feed endpoint.
// GET /api/v1/feed/smart?userId=X&sessionId=Y&page=Z&limit=W&debug=true
//
// This replaces the old RecommendedFeedHandler with the full psychology engine.
//
// Flow:
// 1. Load/compute user profile
// 2. Get/create session state
// 3. Check if cold start user → serve popularity feed
// 4. Fetch candidate content (5x page size)
// 5. Score each candidate using all layers
// 6. Compose feed using slot pattern
// 7. Return ordered items
func SmartFeedHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "OPTIONS" {
		w.WriteHeader(http.StatusOK)
		return
	}

	userID := r.URL.Query().Get("userId")
	sessionID := r.URL.Query().Get("sessionId")
	pageStr := r.URL.Query().Get("page")
	limitStr := r.URL.Query().Get("limit")
	debug := r.URL.Query().Get("debug") == "true"

	if userID == "" {
		http.Error(w, `{"error":"userId is required"}`, http.StatusBadRequest)
		return
	}

	page, _ := strconv.Atoi(pageStr)
	if page < 1 {
		page = 1
	}
	limit, _ := strconv.Atoi(limitStr)
	if limit < 1 || limit > maxPageSize {
		limit = defaultPageSize
	}
	if sessionID == "" {
		sessionID = fmt.Sprintf("%s_%d", userID, time.Now().Unix()/1800)
	}

	// Step 1: Load user profile
	profile, err := getOrComputeProfile(userID)
	if err != nil {
		log.Printf("Profile error for %s: %v", userID, err)
		profile = &UserProfile{
			UserID:           userID,
			CategoryAffinity: make(map[string]float64),
			EnergyPreference: 0.5,
			SocialDrive:      0.5,
			NoveltyTolerance: 0.5,
		}
	}

	// Step 2: Get session state
	session := getSessionState(userID, sessionID)

	// Step 3: Cold start check
	if isColdStartUser(profile) {
		items, hasMore, err := coldStartFeed(userID, page, limit)
		if err != nil {
			http.Error(w, `{"error":"feed error"}`, http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"items":    items,
			"page":     page,
			"hasMore":  hasMore,
			"coldStart": true,
			"profile":  nil,
		})
		return
	}

	// Step 3.5: Log experiment exposure
	for _, exp := range activeExperiments {
		if exp.Active {
			variantID := assignVariant(userID, exp.ID)
			go logExperimentExposure(userID, exp.ID, variantID, sessionID)
		}
	}

	// Step 4: Build context (following set, interacted IDs)
	followingSet, fofSet := buildSocialSets(userID)
	interactedIDs := buildInteractedSet(userID)

	// Step 4.5: Warm signal caches so scoring can read them in O(1)
	warmUserSignalCaches(userID)
	warmPrecomputedSignals(userID)

	// Step 5: Fetch candidates
	candidateLimit := limit * candidateMultiplier
	candidates := fetchCandidates(userID, candidateLimit)

	// Step 6: Score each candidate
	scored := make([]ScoredItem, 0, len(candidates))
	for _, item := range candidates {
		contentID := getItemID(item)
		contentType := item.Type
		cs := getContentScore(contentID, contentType)

		score, breakdown := scoreForUser(cs, profile, session, followingSet, fofSet, interactedIDs)

		si := ScoredItem{
			Item:  item,
			Score: score,
		}
		if debug {
			si.ScoreBreakdown = breakdown
		} else {
			si.ScoreBreakdown = breakdown // needed for composition, stripped before response
		}
		scored = append(scored, si)
	}

	// Sort by score for initial ranking
	sort.Slice(scored, func(i, j int) bool {
		return scored[i].Score > scored[j].Score
	})

	// Step 7: Compose feed with slot pattern
	pattern := getFeedPattern(profile, session, limit)
	composed := composeFeed(scored, pattern, followingSet)

	// Pagination
	hasMore := len(composed) >= limit

	// Strip debug info if not requested
	responseItems := make([]interface{}, 0, len(composed))
	for _, item := range composed {
		if debug {
			responseItems = append(responseItems, item)
		} else {
			responseItems = append(responseItems, map[string]interface{}{
				"type":      item.Item.Type,
				"challenge": item.Item.Challenge,
				"post":      item.Item.Post,
				"slotType":  item.SlotType,
			})
		}
	}

	// Compute session hooks for client-side retention triggers
	sessionHooks := map[string]interface{}{}
	if page == 1 && session.ItemsSeen == 0 {
		// First page of new session — check how long since last session
		var lastSessionTime time.Time
		db.QueryRow(
			`SELECT MAX(created_at) FROM feed_events WHERE user_id = $1 AND created_at < $2`,
			userID, session.StartedAt,
		).Scan(&lastSessionTime)

		if !lastSessionTime.IsZero() {
			hoursSinceLastSession := time.Since(lastSessionTime).Hours()
			if hoursSinceLastSession >= 24 {
				sessionHooks["comebackType"] = "daily_return"
				sessionHooks["hoursAway"] = int(hoursSinceLastSession)
			} else if hoursSinceLastSession >= 4 {
				sessionHooks["comebackType"] = "session_return"
				sessionHooks["hoursAway"] = int(hoursSinceLastSession)
			}
		}
		// Streak tracking: check consecutive daily sessions
		var activeDays int
		db.QueryRow(
			`SELECT COUNT(DISTINCT DATE(created_at)) FROM feed_events
			 WHERE user_id = $1 AND created_at > NOW() - INTERVAL '7 days'`,
			userID,
		).Scan(&activeDays)
		if activeDays > 1 {
			sessionHooks["dailyStreak"] = activeDays
		}
	}

	response := map[string]interface{}{
		"items":        responseItems,
		"page":         page,
		"hasMore":      hasMore,
		"sessionHooks": sessionHooks,
	}
	if debug {
		response["profile"] = profile
		response["session"] = session
		response["strategy"] = session.CurrentStrategy
		response["resistanceLevel"] = session.ResistanceLevel
		response["dopamineBudget"] = session.DopamineBudget
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// FollowingFeedV2Handler returns only content from followed users.
// GET /api/v1/feed/following/v2?userId=X&page=Y&limit=Z
//
// This is simpler than SmartFeed — no slot composition, just chronological
// with scoring to break ties among same-time content.
func FollowingFeedV2Handler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "OPTIONS" {
		w.WriteHeader(http.StatusOK)
		return
	}

	userID := r.URL.Query().Get("userId")
	pageStr := r.URL.Query().Get("page")
	limitStr := r.URL.Query().Get("limit")

	if userID == "" {
		http.Error(w, `{"error":"userId is required"}`, http.StatusBadRequest)
		return
	}

	page, _ := strconv.Atoi(pageStr)
	if page < 1 {
		page = 1
	}
	limit, _ := strconv.Atoi(limitStr)
	if limit < 1 || limit > maxPageSize {
		limit = defaultPageSize
	}

	followingSet, _ := buildSocialSets(userID)

	// Fetch only from followed creators, chronological
	var items []HomeFeedItem

	// Challenges from followed creators
	cRows, err := db.Query(`
		SELECT c.id, c.creator_id, u.username, u.league, c.video_url,
			c.thumbnail_url, c.prefix, c.subject, c.visibility, c.status,
			c.views, COALESCE(cl.likes,0), c.created_at,
			COALESCE(c.created_at + INTERVAL '24 hours', NOW()),
			(SELECT COUNT(*) FROM challenge_responses WHERE challenge_id = c.id)
		FROM challenges c
		JOIN users u ON c.creator_id = u.id
		LEFT JOIN (SELECT challenge_id, COUNT(*) as likes FROM challenge_likes GROUP BY challenge_id) cl
			ON cl.challenge_id = c.id
		WHERE c.visibility = 'arena'
		AND c.creator_id IN (SELECT following_id FROM follows WHERE follower_id = CAST($1 AS INT))
		AND c.created_at > NOW() - INTERVAL '14 days'
		ORDER BY c.created_at DESC
		LIMIT $2`, userID, limit)
	if err == nil {
		defer cRows.Close()
		for cRows.Next() {
			var ch Challenge
			var creatorID, views, likes, rc int
			var createdAt, expiresAt time.Time
			cRows.Scan(&ch.ID, &creatorID, &ch.CreatorUsername, &ch.CreatorLeague,
				&ch.VideoURL, &ch.ThumbnailURL, &ch.Prefix, &ch.Subject,
				&ch.Visibility, &ch.Status, &views, &likes,
				&createdAt, &expiresAt, &rc)
			ch.CreatorID = strconv.Itoa(creatorID)
			ch.Views = views
			ch.Likes = likes
			ch.CreatedAt = createdAt.Format(time.RFC3339)
			ch.ExpiresAt = expiresAt.Format(time.RFC3339)
			ch.ResponseCount = rc
			items = append(items, HomeFeedItem{Type: "challenge", Challenge: &ch})
		}
	}

	// Posts from followed creators
	pRows, err := db.Query(`
		SELECT p.id, p.author_id, u.username, u.league, p.type, p.content_url,
			p.thumbnail_url, p.caption, p.views, p.created_at,
			COALESCE(pl.likes,0),
			(SELECT COUNT(*) FROM comments WHERE post_id = p.id)
		FROM posts p
		JOIN users u ON p.author_id = u.id
		LEFT JOIN (SELECT post_id, COUNT(*) as likes FROM post_likes GROUP BY post_id) pl
			ON pl.post_id = p.id
		WHERE p.author_id IN (SELECT following_id FROM follows WHERE follower_id = CAST($1 AS INT))
		AND p.created_at > NOW() - INTERVAL '14 days'
		ORDER BY p.created_at DESC
		LIMIT $2`, userID, limit)
	if err == nil {
		defer pRows.Close()
		for pRows.Next() {
			var post Post
			var authorID int
			var createdAt time.Time
			pRows.Scan(&post.ID, &authorID, &post.AuthorUsername, &post.AuthorLeague,
				&post.Type, &post.ContentURL, &post.ThumbnailURL, &post.Caption,
				&post.Views, &createdAt, &post.Likes, &post.Comments)
			post.AuthorID = strconv.Itoa(authorID)
			post.CreatedAt = createdAt.Format(time.RFC3339)
			items = append(items, HomeFeedItem{Type: "post", Post: &post})
		}
	}

	// Sort by created_at descending (chronological)
	sort.Slice(items, func(i, j int) bool {
		ti := getItemCreatedAt(items[i])
		tj := getItemCreatedAt(items[j])
		return ti.After(tj)
	})

	// Paginate
	offset := (page - 1) * limit
	if offset >= len(items) {
		items = nil
	} else {
		end := offset + limit
		if end > len(items) {
			end = len(items)
		}
		items = items[offset:end]
	}

	hasMore := len(items) >= limit
	_ = followingSet

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"items":   items,
		"page":    page,
		"hasMore": hasMore,
	})
}

// UserProfileHandler returns the computed user profile (for debugging/analytics).
// GET /api/v1/profile?userId=X
func UserProfileHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "OPTIONS" {
		w.WriteHeader(http.StatusOK)
		return
	}

	userID := r.URL.Query().Get("userId")
	if userID == "" {
		http.Error(w, `{"error":"userId is required"}`, http.StatusBadRequest)
		return
	}

	profile, err := getOrComputeProfile(userID)
	if err != nil {
		http.Error(w, `{"error":"profile not found"}`, http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(profile)
}

// ════════════════════════════════════════════════════════════════════════════════
// HELPERS
// ════════════════════════════════════════════════════════════════════════════════

func buildSocialSets(userID string) (following map[string]bool, fof map[string]bool) {
	following = make(map[string]bool)
	fof = make(map[string]bool)

	// Direct follows
	rows, err := db.Query(`
		SELECT CAST(following_id AS TEXT) FROM follows
		WHERE follower_id = CAST($1 AS INT)`, userID)
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var fid string
			rows.Scan(&fid)
			following[fid] = true
		}
	}

	// Friends of friends (2nd degree)
	fofRows, err := db.Query(`
		SELECT DISTINCT CAST(f2.following_id AS TEXT)
		FROM follows f1
		JOIN follows f2 ON f1.following_id = f2.follower_id
		WHERE f1.follower_id = CAST($1 AS INT)
		AND f2.following_id != CAST($1 AS INT)
		LIMIT 200`, userID)
	if err == nil {
		defer fofRows.Close()
		for fofRows.Next() {
			var fid string
			fofRows.Scan(&fid)
			if !following[fid] {
				fof[fid] = true
			}
		}
	}

	return following, fof
}

func buildInteractedSet(userID string) map[string]bool {
	interacted := make(map[string]bool)
	rows, err := db.Query(`
		SELECT DISTINCT content_type || ':' || content_id
		FROM feed_events WHERE user_id = $1
		LIMIT 1000`, userID)
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var key string
			rows.Scan(&key)
			interacted[key] = true
		}
	}
	return interacted
}

func fetchCandidates(userID string, limit int) []HomeFeedItem {
	var items []HomeFeedItem

	// Recent challenges
	cRows, err := db.Query(`
		SELECT c.id, c.creator_id, u.username, u.league, c.video_url,
			c.thumbnail_url, c.prefix, c.subject, c.visibility, c.status,
			c.views, COALESCE(cl.likes,0), c.created_at,
			COALESCE(c.created_at + INTERVAL '24 hours', NOW()),
			(SELECT COUNT(*) FROM challenge_responses WHERE challenge_id = c.id)
		FROM challenges c
		JOIN users u ON c.creator_id = u.id
		LEFT JOIN (SELECT challenge_id, COUNT(*) as likes FROM challenge_likes GROUP BY challenge_id) cl
			ON cl.challenge_id = c.id
		WHERE c.visibility = 'arena'
		AND c.status IN ('open','active','completed')
		AND c.created_at > NOW() - INTERVAL '14 days'
		AND c.creator_id != CAST($1 AS INT)
		ORDER BY c.created_at DESC
		LIMIT $2`, userID, limit/2)
	if err == nil {
		defer cRows.Close()
		for cRows.Next() {
			var ch Challenge
			var creatorID, views, likes, rc int
			var createdAt, expiresAt time.Time
			cRows.Scan(&ch.ID, &creatorID, &ch.CreatorUsername, &ch.CreatorLeague,
				&ch.VideoURL, &ch.ThumbnailURL, &ch.Prefix, &ch.Subject,
				&ch.Visibility, &ch.Status, &views, &likes,
				&createdAt, &expiresAt, &rc)
			ch.CreatorID = strconv.Itoa(creatorID)
			ch.Views = views
			ch.Likes = likes
			ch.CreatedAt = createdAt.Format(time.RFC3339)
			ch.ExpiresAt = expiresAt.Format(time.RFC3339)
			ch.ResponseCount = rc
			items = append(items, HomeFeedItem{Type: "challenge", Challenge: &ch})
		}
	}

	// Recent posts
	pRows, err := db.Query(`
		SELECT p.id, p.author_id, u.username, u.league, p.type, p.content_url,
			p.thumbnail_url, p.caption, p.views, p.created_at,
			COALESCE(pl.likes,0),
			(SELECT COUNT(*) FROM comments WHERE post_id = p.id)
		FROM posts p
		JOIN users u ON p.author_id = u.id
		LEFT JOIN (SELECT post_id, COUNT(*) as likes FROM post_likes GROUP BY post_id) pl
			ON pl.post_id = p.id
		WHERE p.author_id != CAST($1 AS INT)
		AND p.created_at > NOW() - INTERVAL '14 days'
		ORDER BY p.created_at DESC
		LIMIT $2`, userID, limit/2)
	if err == nil {
		defer pRows.Close()
		for pRows.Next() {
			var post Post
			var authorID int
			var createdAt time.Time
			pRows.Scan(&post.ID, &authorID, &post.AuthorUsername, &post.AuthorLeague,
				&post.Type, &post.ContentURL, &post.ThumbnailURL, &post.Caption,
				&post.Views, &createdAt, &post.Likes, &post.Comments)
			post.AuthorID = strconv.Itoa(authorID)
			post.CreatedAt = createdAt.Format(time.RFC3339)
			items = append(items, HomeFeedItem{Type: "post", Post: &post})
		}
	}

	return items
}

// CategoriesHandler returns the available content categories, emotion labels,
// and energy levels. Used by the Flutter UI to populate pickers.
// GET /api/v1/categories
func CategoriesHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "OPTIONS" {
		w.WriteHeader(http.StatusOK)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"categories":   ContentCategories,
		"emotionTags":  EmotionLabels,
		"moodTags":     MoodTags,
		"energyLevels": EnergyLevels,
	})
}

// RecommendedFeedHandler is a backward-compatible alias for SmartFeedHandler.
// Old clients calling /feed/recommended will get the new algorithm.
var RecommendedFeedHandler = SmartFeedHandler

// FollowingFeedHandler is a backward-compatible alias for FollowingFeedV2Handler.
var FollowingFeedHandler = FollowingFeedV2Handler

func getItemCreatedAt(item HomeFeedItem) time.Time {
	if item.Challenge != nil {
		t, _ := time.Parse(time.RFC3339, item.Challenge.CreatedAt)
		return t
	}
	if item.Post != nil {
		t, _ := time.Parse(time.RFC3339, item.Post.CreatedAt)
		return t
	}
	return time.Time{}
}

// getContentEmotions returns the emotion tags for a piece of content.
func getContentEmotions(contentID, contentType string) []string {
	var emotionJSON []byte
	if contentType == "challenge" {
		db.QueryRow(`SELECT COALESCE(emotion_tags, '[]'::JSONB) FROM challenges WHERE id = $1`, contentID).Scan(&emotionJSON)
	} else {
		db.QueryRow(`SELECT COALESCE(emotion_tags, '[]'::JSONB) FROM posts WHERE id = $1`, contentID).Scan(&emotionJSON)
	}
	var emotions []string
	json.Unmarshal(emotionJSON, &emotions)
	return emotions
}

// getContentCategory returns the category for a piece of content.
func getContentCategory(contentID, contentType string) string {
	var cat string
	if contentType == "challenge" {
		db.QueryRow(`SELECT COALESCE(category, 'other') FROM challenges WHERE id = $1`, contentID).Scan(&cat)
	} else {
		db.QueryRow(`SELECT COALESCE(category, 'other') FROM posts WHERE id = $1`, contentID).Scan(&cat)
	}
	return cat
}

// getContentCreator returns the creator ID for a piece of content.
func getContentCreator(contentID, contentType string) string {
	var creator string
	if contentType == "challenge" {
		db.QueryRow(`SELECT CAST(creator_id AS TEXT) FROM challenges WHERE id = $1`, contentID).Scan(&creator)
	} else {
		db.QueryRow(`SELECT CAST(author_id AS TEXT) FROM posts WHERE id = $1`, contentID).Scan(&creator)
	}
	return creator
}
