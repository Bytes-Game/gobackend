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
	"sync"
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
	LastEmotions    []string           `json:"lastEmotions"`    // Last ~10 items, ONE negative-priority emotion each (wellbeing spiral detection)
	LastMoodEmotions []string          `json:"lastMoodEmotions"` // Last ~10 items, FIRST/dominant emotion each (mood-transition learner — must match the serve-time "to" key)
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
	// === Sequence awareness (new) ===
	// Last N categories/creators shown, in order. Used by the ranker to avoid
	// clumpy repeats (same category/creator 3+ in a row = feed feels monotonous).
	LastCategories []string `json:"lastCategories"`
	LastCreators   []string `json:"lastCreators"`
	// TZOffsetMin is the user's UTC offset in minutes (e.g. IST = +330), set
	// from the feed request each page. Lets hour-of-day routing bucket by the
	// user's LOCAL hour instead of server/UTC time. 0 = unknown (behaves as UTC,
	// matching the previous behaviour, so there's no regression when absent).
	TZOffsetMin int `json:"tzOffsetMin,omitempty"`
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
	EnergyLevel        float64           `json:"energyLevel"`   // 0=chill, 1=intense (may be inferred for medium/unset; used by energyFit)
	EnergyLevelLabel   float64           `json:"energyLevelLabel"` // discrete label energy (energyStringToFloat); used by energyHourMatch to match EnergyByHour's train scale
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
	// === Battle vs Short ===
	// ResponseCount mirrors challenges.responseCount — > 0 means at least one
	// person accepted the duel (this is a "battle"); 0 means a "short" that
	// nobody responded to yet. The For You ranker uses this to bias the feed
	// toward battles, which is the app's core engagement surface. Always 0
	// for non-challenge content types.
	ResponseCount      int               `json:"responseCount"`
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
	// View dopamine is now PROPORTIONAL to completion (see updateSessionFromEvent):
	// delta = (completion - 0.6) * dopamineViewSlope. The old flat
	// dopamineDepletionRate (0.03 for any <0.8 view) was removed when the 0.8
	// cliff was; the slope is tuned so a low-completion view (~0.1) still drains
	// ~0.04 — preserving the "fatigue by ~30 items" calibration below — while a
	// genuinely-watched 0.79 view drains far less than a 0.05 abandon.
	dopamineViewSlope      = 0.08  // proportional view refill/drain per unit completion-from-neutral
	dopamineSkipDrain      = 0.05  // A skip/not_interested drains more — the feed missed
	                                // WHY: At ~30 items, budget ≈ 0.1 (fatigued). Average TikTok
	                                // session is 10-15 minutes ≈ 30-50 items. We want to detect
	                                // fatigue around the same threshold.

	maxItemsPerCreator     = 3     // Diversity: max 3 items from same creator in one feed page
	coldStartThreshold     = 15    // Users with <15 events are "cold start"
	contentColdThreshold   = 5     // Content with <5 views is "cold start"
	auditionViewTarget     = 300   // Until a video has this many views it is "under audition": it gets exploration impressions so its true performance can be measured before merit-ranking judges it. 5 views can't measure quality; ~hundreds can.
	profileStalenessMin    = 5     // Recompute profile if older than 5 minutes — fast cohort transitions during onboarding (TikTok-style)
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
	// Attribute the event to the authenticated user, ignoring any body userId.
	event.UserID = authUserID(r)

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

	// Update the user's two-tower embedding for reward-bearing events.
	// EMA toward content vector on positive engagement, away on negative.
	go applyEmbeddingFromEvent(event)

	// Feed the realtime trending ZSET so viral spikes surface within minutes.
	// Skip self-engagement so authors can't game their own trend score.
	// Use the by-user variant so the engagement-quality multiplier weights
	// trusted users' signals up and brand-new / high-skip accounts down.
	go func(e FeedEvent) {
		if e.UserID == "" || e.ContentID == "" {
			return
		}
		noteTrendingEventByUser(e.UserID, e.ContentType, e.ContentID, e.EventType, e.CompletionRate)
	}(event)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

// applyEmbeddingFromEvent maps an event to an embedding reward label and
// updates the user EMA. Skips events that aren't reliable reward signals
// (e.g., impressions or lifecycle events).
func applyEmbeddingFromEvent(event FeedEvent) {
	if event.UserID == "" || event.ContentID == "" {
		return
	}
	var label float64
	switch event.EventType {
	case "like", "save", "share", "rewatch", "comment":
		label = 1.0
	case "view":
		// View is only a positive signal if completion was meaningful.
		if event.CompletionRate >= 0.6 {
			label = 1.0
		} else if event.CompletionRate > 0 && event.CompletionRate < 0.2 {
			label = 0.0
		} else {
			return // ambiguous — don't move the vector
		}
	case "skip", "not_interested", "unlike", "unsave":
		label = 0.0
	default:
		return
	}
	cs := getContentScore(event.ContentID, event.ContentType)
	emotions := getContentEmotions(event.ContentID, event.ContentType)
	cv := getOrBuildContentEmbedding(cs, emotions)
	updateUserEmbedding(event.UserID, cv, label)

	// Mirror gradient: also train the CONTENT-side vector. The hash-trick
	// embedding is just the prior; after enough engagement events the
	// trained vector encodes who actually engages with this content.
	uv := getUserEmbedding(event.UserID)
	updateTrainedContentEmbedding(cs, emotions, uv, label)
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
	// Every event in the batch is attributed to the authenticated user — a
	// client can't smuggle in events authored as someone else.
	uid := authUserID(r)

	go func() {
		for _, event := range payload.Events {
			event.UserID = uid
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
			applyEmbeddingFromEvent(event)
			noteTrendingEventByUser(event.UserID, event.ContentType, event.ContentID, event.EventType, event.CompletionRate)
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

	// metadata MUST be a valid JSONB value — passing a nil []byte makes
	// lib/pq encode it as SQL NULL, and the destination column doesn't
	// accept NULL on this codepath (DEFAULT is only applied when the
	// column is OMITTED from the INSERT, not when explicit NULL is sent).
	// Without this guard every event that doesn't carry explicit metadata
	// (which is the overwhelming majority — view, like, skip, complete
	// from the Flutter client) silently fails the INSERT inside the
	// recordFeedEvent goroutine, and the whole analytics pipeline ends up
	// blind. Default to an empty JSON object so the row always lands.
	metadataJSON := []byte("{}")
	if len(event.Metadata) > 0 {
		if b, err := json.Marshal(event.Metadata); err == nil && len(b) > 0 {
			metadataJSON = b
		}
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
			LastMoodEmotions: []string{},
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
			LastMoodEmotions: []string{},
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

// applyRefreshSignal handles a pull-to-refresh from the client. Two effects:
//
//  1. Drop the per-user seen-content Redis ZSET so previously-shown items
//     can resurface. Without this, the seen filter (12h TTL) keeps removing
//     the same head-of-feed items from candidate pools, which is exactly
//     what makes a refresh "feel" like nothing changed.
//
//  2. Reset the session's dedup counters — CategoriesSeen / CreatorsSeen /
//     LastCategories / LastCreators. These accumulate as the user scrolls
//     and the ranker uses them to penalize repeats. After a refresh the
//     user's intent is "show me different stuff," so the prior fatigue
//     signal would push us back toward the same not-yet-fatigued bucket
//     and undermine the refresh.
//
// Other session signals (DopamineBudget, mood, strategy memory, lifecycle
// counters) are intentionally preserved — those represent the user's
// genuine state and should survive a refresh, just like in TikTok/IG.
func applyRefreshSignal(userID, sessionID string) {
	if rdb != nil && userID != "" {
		_ = rdb.Del(rctx, seenKey(userID)).Err()
	}
	unlock := sessionKeyLocks.lock(userID + ":" + sessionID)
	defer unlock()
	state := getSessionState(userID, sessionID)
	if state == nil {
		return
	}
	state.CategoriesSeen = make(map[string]int)
	state.CreatorsSeen = make(map[string]int)
	state.LastCategories = nil
	state.LastCreators = nil
	saveSessionState(state)
}

// recentRefreshTopKey is the Redis key holding the IDs that landed at the
// head of this user's last refreshed feed. We use it to penalize those
// same items on the next refresh so the top item is almost guaranteed to
// be different — the missing piece between "scores rotate within near-ties"
// and "the visible video on screen actually changes when I pull-to-refresh."
const recentRefreshTopKeyPrefix = "refresh_top:"
const recentRefreshTopTTL = 10 * time.Minute
const recentRefreshTopCount = 3 // remember top 3 so #1 AND #2 are demoted

func recentRefreshTopKey(userID string) string {
	return recentRefreshTopKeyPrefix + userID
}

// loadPrevRefreshTops returns the keys (contentType:contentID) that were at
// the head of the previous refresh. Returns empty when nothing recorded.
func loadPrevRefreshTops(userID string) map[string]int {
	out := make(map[string]int)
	if rdb == nil || userID == "" {
		return out
	}
	vals, err := rdb.LRange(rctx, recentRefreshTopKey(userID), 0, -1).Result()
	if err != nil {
		return out
	}
	for i, v := range vals {
		if v != "" {
			out[v] = i + 1 // rank: 1 = was #1 last time
		}
	}
	return out
}

// savePrevRefreshTops records the head of the just-served feed so the NEXT
// refresh can demote them. Only call when the request was a refresh.
func savePrevRefreshTops(userID string, items []HomeFeedItem) {
	if rdb == nil || userID == "" || len(items) == 0 {
		return
	}
	key := recentRefreshTopKey(userID)
	pipe := rdb.Pipeline()
	pipe.Del(rctx, key)
	limit := recentRefreshTopCount
	if limit > len(items) {
		limit = len(items)
	}
	for i := 0; i < limit; i++ {
		id := getItemID(items[i])
		if id == "" {
			continue
		}
		member := items[i].Type + ":" + id
		pipe.RPush(rctx, key, member)
	}
	pipe.Expire(rctx, key, recentRefreshTopTTL)
	_, _ = pipe.Exec(rctx)
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

// sessionWasPositive classifies a finished session as a good or bad outcome for
// the learned mood-transition model. "Good" = the user engaged at least once,
// wasn't skipping most of what they saw, and still had attention budget left
// (not a frustrated bounce-quit). Sessions too short to judge count as not-good.
func sessionWasPositive(s *SessionState) bool {
	if s == nil || s.ItemsSeen < 3 {
		return false
	}
	skipRate := float64(s.SkipCount) / float64(s.ItemsSeen)
	return (s.LikeCount+s.ShareCount) > 0 && skipRate < 0.6 && s.DopamineBudget > 0.2
}

// wellbeingNegativeEmotions are the emotion tags that count toward a "negative
// spiral" for the wellbeing counterweight. Package-scoped so the per-item
// representative-emotion picker and the spiral detector share one definition.
var wellbeingNegativeEmotions = map[string]bool{
	"sad": true, "scary": true, "aggressive": true, "serious": true,
}

// representativeEmotion collapses an item's emotion tags to a SINGLE label for
// per-item streak tracking: a negative tag takes priority (so one dark item
// counts as exactly one negative item), otherwise the first tag. Without this,
// flattening all of an item's tags into the history let a single video tagged
// e.g. ['sad','scary','serious'] satisfy negativeStreak>=3 on its own.
func representativeEmotion(emotions []string) string {
	if len(emotions) == 0 {
		return ""
	}
	for _, e := range emotions {
		if wellbeingNegativeEmotions[e] {
			return e
		}
	}
	return emotions[0]
}

func updateSessionFromEvent(event FeedEvent) {
	// Serialize the whole load→modify→save against other writers of this session
	// (other events for the same user, the impression goroutine) so concurrent
	// bursts don't clobber ItemsSeen / DopamineBudget / SkipStreak.
	unlock := sessionKeyLocks.lock(event.UserID + ":" + event.SessionID)
	defer unlock()
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
			// Tier 1.2: stamp a last_session_end so the next session's ranker
			// can apply the session-continuity dampener.
			go RecordSessionEnd(state.UserID)
			// Revive the learned mood-transition loop: credit the moods the user
			// moved through this session, rewarded by whether it went well.
			// Without this call recordSessionMoodOutcome had ZERO callers, so the
			// learned graph was never written and moodTransitionBonus ran on
			// hand-coded seed priors forever.
			if state.DetectedMood != "" && len(state.LastMoodEmotions) > 0 {
				// Train on LastMoodEmotions (first/dominant emotion per item) — the
				// SAME key moodTransitionBonus queries at serve time. (LastEmotions
				// is negative-priority and is for wellbeing, a different consumer.)
				recordSessionMoodOutcome(state.DetectedMood, state.LastMoodEmotions, sessionWasPositive(state))
			}
			// Credit the in-flight strategy's outcome at session end too —
			// recordStrategyOutcome otherwise only runs on the rare
			// resistance-driven mid-session switch, so the strategy-success
			// history and the Thompson bandit it feeds learned only from
			// frustrated sessions (loss-biased). Use a FRESH profile copy:
			// recordStrategyOutcome mutates+saves it, so we must never hand it
			// the shared cached profile.
			punlock := profileKeyLocks.lock(state.UserID)
			if p, perr := loadUserProfile(state.UserID); perr == nil && p != nil {
				recordStrategyOutcome(state, p)
			}
			punlock()
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

	state.TotalWatchMs += event.WatchDurationMs

	// ItemsSeen is incremented per IMPRESSION (view/skip/not_interested) in the
	// switch below — NOT here on every event. Counting it per event made one
	// engaged item (view+like+complete) bump it 2-3×, deflating the per-item
	// skipRate / engagementPerItem rates so "engaged" mood was nearly unreachable
	// and users skewed "bored". Engagement taps now bump their own counters only.

	// Dopamine budget is adjusted per IMPRESSION in the view/skip cases below —
	// NOT here on every event. Depleting on every event made a single engaged
	// item (view + like + complete) drain multiple times, and a like's refill
	// (0.02) was smaller than the per-event drain (0.03), so positive engagement
	// net-DRAINED the budget — backwards. Now dopamine tracks satisfaction: a
	// full watch refills, a partial view costs a little, a skip costs more, and
	// taps (like/share) are pure refills on top.

	// Tier 3.11: feed terminal outcome events into the online LTR model so it
	// learns which breakdown features correlate with completions for this
	// cohort. ltrObserveEvent is a no-op if no breakdown was stashed.
	// Pass watchRatio=-1 when we don't have a reliable ratio (e.g. like
	// fired without a view event preceding it); positive cases pass the
	// observed completion rate so the watch-ratio head trains on rich data.
	if label, ok := ltrLabelForEvent(event.EventType, event.CompletionRate); ok {
		watchRatio := -1.0
		switch event.EventType {
		case "view", "complete", "skip":
			if event.CompletionRate >= 0 {
				watchRatio = event.CompletionRate
			}
		}
		latencyMs := engagementLatencyFromEvent(event)
		// For positive watch-completion events the WatchDurationMs field is the
		// watch TIME, not impression-to-action latency — feeding it as latency
		// inverts the signal (a full 15s watch, the strongest positive, would
		// otherwise get the minimum 0.5× training weight, scaled inversely to
		// video length). Zero it for those so they train at a neutral weight;
		// taps (like/share/save) and skips keep their real fast-is-stronger
		// latency.
		if label >= 0.5 {
			switch event.EventType {
			case "view", "complete", "loop", "rewatch":
				latencyMs = 0
			}
		}
		go ltrObserveEventWithLatency(event.UserID, event.ContentType, event.ContentID, label, watchRatio, latencyMs)
	}

	// The watch-ratio REGRESSION head wants the full [0,1] completion
	// distribution, but ltrLabelForEvent (a BINARY classifier gate) rejects
	// mid-band views (0.2 ≤ completion < 0.8), so the regression only ever saw
	// 0.0/1.0 targets — censored into a classifier, defeating its purpose. Train
	// it directly (non-destructively) on any mid-band view the binary gate
	// rejected; the ok=true extremes already train it via the path above, so the
	// !ok guard prevents double-training.
	if event.EventType == "view" && event.CompletionRate > 0 {
		if _, ok := ltrLabelForEvent(event.EventType, event.CompletionRate); !ok {
			go wrObserveEvent(event.UserID, event.ContentType, event.ContentID, event.CompletionRate)
		}
	}

	// Mine negative feedback into UserProfile so blocks/unfollows/skips
	// don't just penalize — they also sharpen the user's preference vector.
	if isMineableNegative(event.EventType, event.CompletionRate) {
		go func(e FeedEvent) {
			unlock := profileKeyLocks.lock(e.UserID)
			defer unlock()
			profile, err := loadUserProfile(e.UserID)
			if err == nil && profile != nil {
				applyNegativeFeedbackFromEvent(profile, e)
				bumpNegativeProfileMineEpoch()
				saveUserProfile(profile)
			}
		}(event)
	}

	// Record session-trajectory transition on positive engagement events.
	// Uses the user's prior LastCategories entry as the "from" state and
	// this event's content as the "to" state. Reads session via the
	// existing getSessionState helper; skip when state isn't loaded.
	if isPositiveEngagementForTrajectory(event.EventType, event.CompletionRate) {
		go func(e FeedEvent) {
			profile, err := loadUserProfile(e.UserID)
			if err != nil || profile == nil || e.SessionID == "" {
				return
			}
			session := getSessionState(e.UserID, e.SessionID)
			if session == nil {
				return
			}
			cs := getContentScore(e.ContentID, e.ContentType)
			if cs == nil {
				return
			}
			recordSessionTrajectoryFromEvent(classifyCohort(profile), session, cs.Category, cs.EnergyLevel, true)
		}(event)
	}

	// Snapshot the budget BEFORE the per-event refills so the "second wind"
	// mechanic can fire when the user engaged WHILE critically low — the refills
	// below would otherwise lift the budget out of the <0.15 zone first.
	preBudget := state.DopamineBudget

	switch event.EventType {
	case "skip", "not_interested":
		state.ItemsSeen++ // an impression the user rejected
		state.SkipCount++
		state.SkipStreak++
		// A rejection drains more than a neutral view — the feed missed.
		state.DopamineBudget = math.Max(0, state.DopamineBudget-dopamineSkipDrain)
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
		// No dopamine refill here — the paired `view` event (proportional to
		// completion, below) is the canonical watch-satisfaction signal.
		// Refilling here too double-counted the same watch (view+complete = +0.06).
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
		// Strong negative — treat as multiple skips. Bump ItemsSeen by the same
		// amount so skipRate (SkipCount/ItemsSeen) can't exceed 1.0 and falsely
		// force L3 resistance / a 'frustrated' mood off a single report.
		state.ItemsSeen += 3
		state.SkipCount += 3
		state.SkipStreak += 2
		state.DopamineBudget = math.Max(0, state.DopamineBudget-3*dopamineSkipDrain)
	case "view":
		state.ItemsSeen++ // the impression itself; engagement taps below don't re-count it
		if event.CompletionRate >= 0.8 {
			state.SkipStreak = 0 // Watching most of content = not skipping
		}
		// Dopamine tracks watch satisfaction PROPORTIONAL to completion, and only
		// when completion is known (>0): a near-full watch refills (~+0.03 at
		// 100%), a low-completion one drains (~-0.04 at 10%, ~-0.05 at 0%), 0.6 is
		// neutral. A zero/unknown-completion view is left neutral so a measurement
		// gap — or a true bounce, which fires its own skip event — isn't double-
		// penalized. No sharp 0.8 cliff: a genuinely-watched 0.79 view drains far
		// less than a 0.05 abandon, yet low-completion views still drain near the
		// old flat rate so fatigue detection isn't blunted. This is the SOLE
		// watch-satisfaction refill now (the `complete` event no longer refills).
		if event.CompletionRate > 0 {
			delta := (event.CompletionRate - 0.6) * dopamineViewSlope
			if delta >= 0 {
				state.DopamineBudget = math.Min(1.0, state.DopamineBudget+delta)
			} else {
				state.DopamineBudget = math.Max(0, state.DopamineBudget+delta)
			}
		}
	}

	// Track content emotions for wellbeing spiral detection
	if event.EventType == "view" && event.CompletionRate > 0.5 {
		// Only track emotions for content they actually watched. Store ONE
		// representative emotion per item (negative-priority) so the spiral
		// detector counts negative ITEMS, not tags — a single multi-tagged dark
		// video must not look like a 3-item negative streak.
		emotions := getContentEmotions(event.ContentID, event.ContentType)
		if rep := representativeEmotion(emotions); rep != "" {
			state.LastEmotions = append(state.LastEmotions, rep)
			// Keep only last 10 items
			if len(state.LastEmotions) > 10 {
				state.LastEmotions = state.LastEmotions[len(state.LastEmotions)-10:]
			}
		}
		// Separately track the FIRST/dominant emotion per item for the mood-
		// transition learner. The serve path (moodTransitionBonus) keys its "to"
		// lookup on the first emotion, so the learner MUST record under the same
		// key — recording the negative-priority representative instead meant the
		// learned EMA was stored under a key serve never queries (e.g. trained
		// 'sad' for a ['happy','sad'] item but served 'happy'), silently disabling
		// the learned mood graph.
		if len(emotions) > 0 && emotions[0] != "" {
			state.LastMoodEmotions = append(state.LastMoodEmotions, emotions[0])
			if len(state.LastMoodEmotions) > 10 {
				state.LastMoodEmotions = state.LastMoodEmotions[len(state.LastMoodEmotions)-10:]
			}
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
	//
	// Gate on the PRE-event budget: the per-event refill above had usually already
	// lifted the budget out of <0.15, so gating on the post-refill value meant this
	// almost never fired — the mechanic was effectively dead.
	//
	// No `> 0` lower guard: the budget floors at exactly 0 (maximally fatigued),
	// which is precisely the rock-bottom user the "one more episode" rescue exists
	// for. Excluding 0 would repeat the inverted-edge bug already fixed for the
	// sibling anti-loop dopamine_collapse signal.
	if preBudget < 0.15 {
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
		// profile lock (taken after the session lock — consistent ordering)
		// spans load→recordStrategyOutcome(save) so it merges with other profile
		// writers instead of clobbering.
		punlock := profileKeyLocks.lock(state.UserID)
		profile, _ := loadUserProfile(state.UserID)
		newStrat := pickAlternateStrategy(state, profile)
		if newStrat != "" && newStrat != state.CurrentStrategy {
			// Record how the previous strategy performed before switching
			recordStrategyOutcome(state, profile)
			switchStrategy(state, newStrat)
		}
		punlock()
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

	// Tier 3.13: feed the same outcome into the Thompson-sampling bandit.
	// Convert delta [-1..1] to a reward [0..1] — anything above neutral is
	// treated as a partial win, anything below as a partial loss.
	reward := (delta + 1.0) / 2.0
	b := loadBandit(profile.UserID)
	b.updateArm(profile.UserID, state.CurrentStrategy, reward)
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
	// disengagement ~10s faster than the skip path. It may only ESCALATE the
	// level: previously it early-RETURNED, so a moderate bounce rate (→ level 1)
	// short-circuited and HID a severe skip-based level (→ 2/3) for exactly the
	// most disengaged users, who then never got the strategy switch / emergency.
	// We now take max(bouncePath, skipPath).
	bounceLvl := 0
	if state.ImpressionCount >= resistBounceMinSample {
		bounceRate := float64(state.BounceCount) / float64(state.ImpressionCount)
		switch {
		case state.BounceStreak >= 10 || bounceRate >= resistBounceRateL3:
			bounceLvl = 3
		case state.BounceStreak >= resistBounceStreakL2 || bounceRate >= resistBounceRateL2:
			bounceLvl = 2
		case bounceRate >= resistBounceRateL1:
			bounceLvl = 1
		}
	}

	// ─── Skip-based (LATE) resistance ────────────────────────────────
	skipLvl := 0
	if state.ItemsSeen >= 3 {
		skipRate := float64(state.SkipCount) / float64(state.ItemsSeen)
		switch {
		case state.SkipStreak >= 8 || skipRate >= resistL3SkipRate:
			skipLvl = 3
		case state.SkipStreak >= resistL2SkipStreak || skipRate >= resistL2SkipRate:
			skipLvl = 2
		case skipRate >= resistL1SkipRate:
			skipLvl = 1
		}
	}

	if bounceLvl > skipLvl {
		return bounceLvl
	}
	return skipLvl
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

	// Min-sample guards consistent with detectResistance: a bounce/skip RATE from
	// 1-2 impressions is noise and must not prematurely declare 'frustrated' and
	// trigger a strategy switch. Below the sample floor the rate stays 0.
	bounceRate := 0.0
	if state.ImpressionCount >= resistBounceMinSample {
		bounceRate = float64(state.BounceCount) / float64(state.ImpressionCount)
	}
	skipRate := 0.0
	if state.ItemsSeen >= 3 {
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

	// Tier 3.13: Thompson-sampling bandit on top of the heuristic. The bandit
	// maintains a per-user Beta posterior on each strategy and samples the
	// most promising one — naturally balancing exploration and exploitation.
	// This beats pure "pick historical best" which can lock onto a strategy
	// that used to work but no longer does.
	if profile != nil {
		b := loadBandit(profile.UserID)
		rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
		// Sample from the per-cohort soft-mix distribution rather than the single
		// Thompson argmax (sampleBest): this applies the per-cohort exploration
		// FLOOR (cold/new users explore strategies more, settled cohorts exploit),
		// which was previously dead code — only the floor-less sampleBest ran in
		// production.
		weights := b.softMixForCohort(filtered, classifyCohort(profile), rnd)
		if pick := weightedPickFrom(filtered, weights, rnd); pick != "" {
			return pick
		}
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

// userProfileCacheTTL keeps the profile in process briefly so the feed path
// doesn't read Postgres on every request. Profiles evolve slowly, so a short
// TTL is safe. The returned *UserProfile is SHARED and must be treated
// read-only (verified: scoreForUser only reads it; recordStrategyOutcome
// mutates its own freshly-loaded copy, not this one).
const userProfileCacheTTL = 60 * time.Second

var userProfileCache = NewSignalCache[*UserProfile](userProfileCacheTTL)

// disableUserProfileCache is set by tests so they see fresh per-call profiles.
var disableUserProfileCache bool

// profileRecomputing singleflights the background recompute per user so a stale
// profile under concurrent requests is rebuilt once, not once-per-request.
var profileRecomputing sync.Map // userID -> struct{}

func getOrComputeProfile(userID string) (*UserProfile, error) {
	if !disableUserProfileCache {
		if cached, ok := userProfileCache.Get(userID); ok {
			return cached, nil
		}
	}
	profile, err := loadUserProfile(userID)
	if err == nil && profile != nil {
		if time.Since(profile.LastComputedAt).Minutes() < profileStalenessMin {
			// Fresh stored profile — cache and serve.
			if !disableUserProfileCache {
				userProfileCache.Set(userID, profile)
			}
			return profile, nil
		}
		// Stale: serve it immediately (briefly cached to absorb the burst) and
		// recompute OFF the request path, so the feed never blocks on the
		// ~26-query rebuild and concurrent requests don't stampede it.
		if !disableUserProfileCache {
			userProfileCache.Set(userID, profile)
			triggerProfileRecompute(userID)
			return profile, nil
		}
	}

	// No usable stored profile (new user / read error) — must compute inline.
	fresh, ferr := computeUserProfile(userID)
	if ferr == nil && fresh != nil && !disableUserProfileCache {
		userProfileCache.Set(userID, fresh)
	}
	return fresh, ferr
}

// triggerProfileRecompute rebuilds a stale profile off the request path, at most
// once at a time per user. computeUserProfile persists to Postgres; we refresh
// the in-process cache with the rebuilt profile so the next request is current.
func triggerProfileRecompute(userID string) {
	if _, busy := profileRecomputing.LoadOrStore(userID, struct{}{}); busy {
		return
	}
	go func() {
		defer profileRecomputing.Delete(userID)
		if fresh, err := computeUserProfile(userID); err == nil && fresh != nil {
			userProfileCache.Set(userID, fresh)
		}
	}()
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
	// Serialize against the other profile writers so the rebuilt row merges with
	// (rather than clobbers) concurrent affinity / strategy / negative updates.
	unlock := profileKeyLocks.lock(userID)
	defer unlock()
	// Preserve StrategySuccessHistory across recomputes — it's updated
	// incrementally at strategy-switch time, not derived from raw events.
	// Likewise preserve incrementally-MINED negative signal (negative category
	// affinities and the avoided-categories list) that realtime negative-profile
	// mining / the impression aggregator push down between full recomputes — the
	// rebuild below clamps CategoryAffinity to >=0 from a 500-event window and
	// would otherwise silently discard committed dislike signal.
	var preservedStrategyHistory map[string]float64
	var preservedNegAffinity map[string]float64
	var preservedAvoided []string
	if existing, err := loadUserProfile(userID); err == nil && existing != nil {
		preservedStrategyHistory = existing.StrategySuccessHistory
		preservedNegAffinity = make(map[string]float64)
		for k, v := range existing.CategoryAffinity {
			if v < 0 {
				preservedNegAffinity[k] = v
			}
		}
		preservedAvoided = existing.AvoidedCategories
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

			// Count each item's completion ONCE, from its view event. 'complete'
			// and 'rewatch' rows also carry a completion_rate (=1.0) and would
			// double-count, biasing AvgCompletionRate upward (it drives cohort gates).
			if evType == "view" && completion > 0 {
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
		// Re-apply mined negative affinities the clamp above discarded: if this
		// window produced no MEANINGFUL positive evidence for a category the user
		// previously disliked, keep the negative affinity instead of resetting to
		// neutral. Require cur >= 0.15 (real re-engagement) before letting fresh
		// evidence override — a single weak positive event (cur≈0.01) must not
		// erase a sustained mined dislike like -0.5.
		const negAffinityOverrideThreshold = 0.15
		for k, negv := range preservedNegAffinity {
			if cur, ok := p.CategoryAffinity[k]; !ok || cur < negAffinityOverrideThreshold {
				p.CategoryAffinity[k] = negv
			}
		}

		// Build avoided categories — require CORROBORATED dislike. A single
		// "not_interested" tap scores -2.0, so the old -1.0 cutoff let ONE tap
		// (or one mis-categorized item via the ~70% inferCategory heuristic)
		// blacklist a whole category and force relevance to -0.3 for all of its
		// content. -2.5 needs corroboration (a not_interested PLUS a skip, or
		// several skips) before avoiding.
		for k, v := range categoryScores {
			if v < -2.5 {
				p.AvoidedCategories = append(p.AvoidedCategories, k)
			}
		}
		// Union with previously-mined avoided categories (dedup) so a recompute
		// doesn't drop dislikes the current window didn't reproduce. The realtime
		// miner can still remove an entry (it won't be in preservedAvoided then).
		{
			seen := make(map[string]bool, len(p.AvoidedCategories))
			for _, c := range p.AvoidedCategories {
				seen[c] = true
			}
			for _, c := range preservedAvoided {
				if !seen[c] {
					p.AvoidedCategories = append(p.AvoidedCategories, c)
					seen[c] = true
				}
			}
			// Bound the list like the realtime miner does (it trims to 20) so the
			// union path can't grow it without limit across recomputes. Keep the
			// most-recently-added (the window's fresh dislikes are appended last).
			if len(p.AvoidedCategories) > 20 {
				p.AvoidedCategories = p.AvoidedCategories[len(p.AvoidedCategories)-20:]
			}
		}

		// Average completion rate
		if completionCount > 0 {
			p.AvgCompletionRate = totalCompletion / float64(completionCount)
		}

		// Skip rate = skips per IMPRESSION (skip + view), NOT per total event of
		// every type. The old denominator summed likes, comments, impressions,
		// pauses, scrolls, ... which made the rate structurally tiny — the
		// at-risk cohort gate (AvgSkipRate > 0.5) could essentially never fire.
		skips := eventTypes["skip"] + eventTypes["not_interested"]
		impressions := skips + eventTypes["view"]
		if impressions > 0 {
			p.AvgSkipRate = float64(skips) / float64(impressions)
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
	// Shrink toward the 0.5 neutral prior so a single engagement on a followed
	// creator (1-of-1) doesn't read as SocialDrive=1.0 and trip the >0.6 serve
	// gate on no evidence. smoothedRate handles totalEngagement==0 → 0.5.
	p.SocialDrive = smoothedRate(float64(followedEngagement), float64(totalEngagement), 0.5, 8)

	// === Novelty Tolerance ===
	// How many unique categories does the user engage with, relative to how many
	// items they've engaged with at all?
	var uniqueCategories, totalNoveltyItems int
	db.QueryRow(`
		SELECT COUNT(DISTINCT COALESCE(c.category, p.category, 'other')), COUNT(*)
		FROM feed_events fe
		LEFT JOIN challenges c ON fe.content_type = 'challenge' AND fe.content_id = CAST(c.id AS TEXT)
		LEFT JOIN posts p ON fe.content_type = 'post' AND fe.content_id = CAST(p.id AS TEXT)
		WHERE fe.user_id = $1
		  AND (fe.event_type IN ('like','comment','share','save')
		       OR (fe.event_type = 'view' AND fe.completion_rate > 0.5))`, userID).Scan(&uniqueCategories, &totalNoveltyItems)
	// Count a 'view' only when actually watched (>50%), not on mere exposure:
	// NoveltyTolerance is meant to capture tolerance FOR novelty, but the feed
	// controls breadth of exposure, so counting passive views measured the feed's
	// diversity and fed back to show even more (novelty *= NoveltyTolerance).
	// Normalize: 1-3 categories = low novelty, 8+ = high. Then shrink toward the
	// 0.5 neutral prior by the number of engaged items — a user with only a
	// couple of events can't have sampled many categories, so the raw ratio must
	// not brand them low- (or high-) novelty until evidence accumulates.
	rawNovelty := math.Min(1.0, float64(uniqueCategories)/8.0)
	p.NoveltyTolerance = (rawNovelty*float64(totalNoveltyItems) + 0.5*8.0) / (float64(totalNoveltyItems) + 8.0)

	// === Energy Preference ===
	// Average ENERGY of the content the user actually completes (>70% watched),
	// read from each challenge's declared/inferred energy_level. The old
	// challenge-vs-post ratio pinned this near 0.8 for everyone once the home
	// feed went challenge-only, flattening the 0.20-weight EnergyFit term into a
	// constant. Deriving it from real energy (same 0.25/0.55/0.85 scale the
	// content scorer uses) restores genuine per-user variance.
	var completedEnergyN int
	var avgEnergy float64
	db.QueryRow(`
		SELECT COUNT(*),
		       COALESCE(AVG(CASE COALESCE(c.energy_level,'medium')
		            WHEN 'high' THEN 0.85 WHEN 'low' THEN 0.25 ELSE 0.55 END), 0.5)
		FROM feed_events fe
		JOIN challenges c ON fe.content_type = 'challenge'
		                 AND fe.content_id = CAST(c.id AS TEXT)
		WHERE fe.user_id = $1 AND fe.completion_rate > 0.7`, userID).Scan(&completedEnergyN, &avgEnergy)
	if completedEnergyN > 0 {
		p.EnergyPreference = avgEnergy
	}

	// === Ego Sensitivity ===
	// Recent wins/losses from REAL per-challenge vote-share. The old logic
	// counted any vote received as a "win" and only a zero-vote response as a
	// "loss", so any active responder looked like they were winning — the
	// loss-recovery ego-repair logic almost never fired for the users it targets.
	// A win = the user's response has the most votes in its challenge (and >0);
	// a loss = it has fewer than the top response. Ties at the top count as wins.
	db.QueryRow(`
		WITH my AS (
			SELECT id AS response_id, challenge_id
			FROM challenge_responses
			WHERE responder_id = CAST($1 AS INT)
			  AND created_at > NOW() - INTERVAL '7 days'
		),
		vc AS (
			SELECT cr.challenge_id, cr.id AS response_id, COUNT(cv.id) AS votes
			FROM challenge_responses cr
			LEFT JOIN challenge_votes cv ON cv.response_id = cr.id
			WHERE cr.challenge_id IN (SELECT challenge_id FROM my)
			GROUP BY cr.challenge_id, cr.id
		),
		ranked AS (
			SELECT response_id, votes,
			       MAX(votes) OVER (PARTITION BY challenge_id) AS top
			FROM vc
		)
		SELECT
			COUNT(*) FILTER (WHERE votes = top AND top > 0) AS wins,
			COUNT(*) FILTER (WHERE votes < top) AS losses
		FROM ranked
		WHERE response_id IN (SELECT response_id FROM my)`,
		userID).Scan(&p.RecentWins, &p.RecentLosses)
	// Ego sensitivity scales with battle ACTIVITY (a competitiveness proxy):
	// more battles → more ego-invested. Always assign — including 0 battles → 0 —
	// so a non-battler isn't left at the 0.5 default and thereby read as MORE
	// ego-sensitive than a 1-battle user (0.1). The ego-repair/boost patterns this
	// gates (>0.5/>0.7) are for demonstrably competitive users, not the unproven.
	p.EgoSensitivity = math.Min(1.0, float64(p.RecentWins+p.RecentLosses)/10.0)

	// === Session stats ===
	var sessionCount int
	var totalWatchMs int64
	// TotalWatchTimeMs = VIEW-only watch time. Summing watch_duration_ms across
	// ALL event types double-counted the same seconds (view + complete +
	// impression + pause each carry overlapping durations for one item).
	db.QueryRow(`
		SELECT
			COUNT(DISTINCT session_id) FILTER (WHERE session_id <> ''),
			COALESCE(SUM(watch_duration_ms) FILTER (WHERE event_type = 'view'), 0)
		FROM feed_events WHERE user_id = $1`, userID).Scan(&sessionCount, &totalWatchMs)
	p.TotalSessions = sessionCount
	p.TotalWatchTimeMs = totalWatchMs
	// Session length = average wall-clock span (first→last event) per session.
	// The old totalWatchMs/sessionCount inflated this ~2-3x via the double-count
	// above and mis-drove the cohort gates (at-risk < 90s, power > 240s).
	var avgSpanSec float64
	db.QueryRow(`
		SELECT COALESCE(AVG(span), 0) FROM (
			SELECT EXTRACT(EPOCH FROM (MAX(created_at) - MIN(created_at))) AS span
			FROM feed_events
			WHERE user_id = $1 AND session_id <> ''
			GROUP BY session_id
		) s`, userID).Scan(&avgSpanSec)
	if avgSpanSec > 0 {
		p.AvgSessionSec = int(avgSpanSec)
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
	// Bucket by the user's LOCAL hour (shift created_at by their stored UTC
	// offset) so these maps line up with the local-hour lookups in
	// categoryHourBoost/energyHourMatch. tzMin defaults to 0 (UTC) when unknown.
	tzMin := getUserTZOffset(userID)
	hourCatRows, err := db.Query(`
		SELECT EXTRACT(HOUR FROM fe.created_at + make_interval(mins => $2))::INT as h,
			COALESCE(c.category, p.category, '') as cat,
			COALESCE(c.energy_level, p.energy_level, 'medium') as energy,
			COUNT(*) as cnt
		FROM feed_events fe
		LEFT JOIN challenges c ON fe.content_type = 'challenge' AND fe.content_id = CAST(c.id AS TEXT)
		LEFT JOIN posts p ON fe.content_type = 'post' AND fe.content_id = CAST(p.id AS TEXT)
		WHERE fe.user_id = $1
		AND (fe.event_type IN ('like','comment','share','save','rewatch')
		     OR (fe.event_type = 'view' AND fe.completion_rate > 0.5))
		GROUP BY h, cat, energy
		ORDER BY h, cnt DESC`, userID, tzMin)
	if err == nil {
		defer hourCatRows.Close()
		// Accumulate per-(hour,category) TOTALS across energy levels before picking
		// the best category. The query GROUP BYs (h,cat,energy), so a single
		// category's count is FRAGMENTED across energy buckets; picking the best by
		// raw row count let a one-energy category outrank a higher-total category
		// that happened to be split across energies. Sum per (h,cat) first.
		hourCatCount := make(map[int]map[string]int) // hour → cat → total count
		hourEnergySum := make(map[int]float64)        // hour → sum of energy scores
		hourEnergyCount := make(map[int]int)          // hour → count
		for hourCatRows.Next() {
			var h, cnt int
			var cat, energy string
			hourCatRows.Scan(&h, &cat, &energy, &cnt)
			if cat != "" {
				if hourCatCount[h] == nil {
					hourCatCount[h] = make(map[string]int)
				}
				hourCatCount[h][cat] += cnt
			}
			// Same 0.25/0.55/0.85 scale the content scorer uses (energyHourMatch
			// compares the two), via the shared mapping.
			energyVal := energyStringToFloat(energy)
			hourEnergySum[h] += energyVal * float64(cnt)
			hourEnergyCount[h] += cnt
		}
		hourCatBest := make(map[int]string) // hour → highest-TOTAL category
		for h, cats := range hourCatCount {
			best, bestN := "", 0
			for cat, n := range cats {
				if n > bestN {
					best, bestN = cat, n
				}
			}
			if best != "" {
				hourCatBest[h] = best
			}
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

	// Ego-state category preferences: which categories the user engages with in
	// the hour after WINNING vs LOSING a battle. Both buckets come from ONE
	// symmetric query so they share a scale and a single definition of win/loss:
	//   win  = the user's response tied/led on votes in its challenge (top, >0)
	//   loss = the user's response had fewer votes than the challenge's top
	// — the SAME vote-share definition as RecentWins/RecentLosses above. The old
	// 'losing' query used an abandoned "loss = zero votes" definition, so its
	// bucket was populated for a different (much narrower) population than the
	// RecentLosses signal that actually gates whether this map is consulted.
	// feed_events are de-duplicated (DISTINCT fe.id): the old 'winning' query
	// JOINed challenge_votes and so counted each post-win engagement ONCE PER
	// VOTE the response received, inflating high-vote-win categories ~Nx.
	egoCatRows, err := db.Query(`
		WITH my AS (
			SELECT cr.id AS response_id, cr.challenge_id, cr.created_at
			FROM challenge_responses cr
			WHERE cr.responder_id = CAST($1 AS INT)
			  AND cr.created_at > NOW() - INTERVAL '30 days'
		),
		vc AS (
			SELECT cr.challenge_id, cr.id AS response_id, COUNT(cv.id) AS votes
			FROM challenge_responses cr
			LEFT JOIN challenge_votes cv ON cv.response_id = cr.id
			WHERE cr.challenge_id IN (SELECT challenge_id FROM my)
			GROUP BY cr.challenge_id, cr.id
		),
		ranked AS (
			SELECT response_id, votes,
			       MAX(votes) OVER (PARTITION BY challenge_id) AS top
			FROM vc
		),
		outcomes AS (
			SELECT my.response_id, my.created_at,
			       CASE WHEN r.votes = r.top AND r.top > 0 THEN 'winning'
			            WHEN r.votes < r.top THEN 'losing'
			            ELSE NULL END AS ego_state
			FROM my JOIN ranked r ON r.response_id = my.response_id
		),
		engaged AS (
			SELECT DISTINCT o.ego_state, fe.id AS event_id,
			       COALESCE(c2.category, p2.category, '') AS cat
			FROM outcomes o
			JOIN feed_events fe ON fe.user_id = $1
				AND fe.created_at BETWEEN o.created_at AND o.created_at + INTERVAL '1 hour'
				AND fe.event_type IN ('like','share','save','rewatch')
			LEFT JOIN challenges c2 ON fe.content_type = 'challenge' AND fe.content_id = CAST(c2.id AS TEXT)
			LEFT JOIN posts p2 ON fe.content_type = 'post' AND fe.content_id = CAST(p2.id AS TEXT)
			WHERE o.ego_state IS NOT NULL
		)
		SELECT ego_state, cat, COUNT(*) AS cnt
		FROM engaged
		WHERE cat <> ''
		GROUP BY ego_state, cat
		ORDER BY cnt DESC`, userID)
	if err == nil {
		defer egoCatRows.Close()
		for egoCatRows.Next() {
			var egoState, cat string
			var cnt int
			if egoCatRows.Scan(&egoState, &cat, &cnt) == nil && cat != "" {
				if _, ok := p.CategoryByEgo[egoState]; ok {
					p.CategoryByEgo[egoState][cat] = float64(cnt)
				}
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
		// Bayesian shrinkage toward the 0.5 prior so a 1-of-1 deep view doesn't
		// read as a proven deep watcher (1.0); converges to the true rate as
		// views accumulate.
		p.AttentionSpan = smoothedRate(float64(deepViews), float64(totalViews), 0.5, 8)
	}

	// --- BingeIntensity ---
	// Tail of session length distribution. If a user has at least one very long
	// session (>15min) they're a binger. Otherwise scale by avg session length.
	var maxSessionMs int64
	// Longest session by wall-clock span (first→last event), NOT a SUM of
	// overlapping per-event durations, and excluding the empty pseudo-session
	// that catches all session-less events (server-recorded / legacy rows) —
	// that bucket's SUM was huge and falsely flagged users as bingers.
	db.QueryRow(`
		SELECT COALESCE(MAX(span_ms), 0) FROM (
			SELECT EXTRACT(EPOCH FROM (MAX(created_at) - MIN(created_at))) * 1000 AS span_ms
			FROM feed_events WHERE user_id = $1 AND session_id <> ''
			GROUP BY session_id
		) s`, userID).Scan(&maxSessionMs)
	// 15min = 900,000ms = full binge. 5min = 300,000ms = casual. Below = dipper.
	if maxSessionMs > 0 {
		p.BingeIntensity = math.Min(1.0, float64(maxSessionMs)/900000.0)
	}

	// --- CreatorLoyalty ---
	// How concentrated are their positive events on their top 3 creators?
	// (share of positive engagement on top-3 creators / total positive engagement)
	var topCreatorEngagement, totalPositiveEngagement int
	// Denominator must cover the SAME population as the numerator below (events
	// that still resolve to a live creator/author). Counting deleted-content
	// engagements here but not in the numerator biased CreatorLoyalty downward
	// and made 1.0 unreachable once any engaged content was removed.
	db.QueryRow(`
		SELECT COUNT(*)
		FROM feed_events fe
		LEFT JOIN challenges c ON fe.content_type = 'challenge' AND fe.content_id = CAST(c.id AS TEXT)
		LEFT JOIN posts p ON fe.content_type = 'post' AND fe.content_id = CAST(p.id AS TEXT)
		WHERE fe.user_id = $1
		AND fe.event_type IN ('like','share','save','rewatch','complete','loop','unmute','profile_visit')
		AND COALESCE(c.creator_id::TEXT, p.author_id::TEXT) IS NOT NULL`,
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
		// Shrink toward 0.5 so a user with 2 positive events both on one creator
		// isn't scored a maxed-out loyalist (1.0) on no real evidence.
		// priorStrength 8 to match SocialDrive/AttentionSpan — the >0.6 serve gate
		// sits just 0.1 above the 0.5 prior, so a weaker k=5 left small-sample
		// users still tripping strategyCreatorFocus.
		p.CreatorLoyalty = smoothedRate(float64(topCreatorEngagement), float64(totalPositiveEngagement), 0.5, 8)
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

// contentScoreCacheTTL bounds how stale a cached content score may get. 60s is
// immaterial to ranking — engagement counts and the 2h trending window move
// slowly relative to a feed request — and it collapses the dominant feed-path
// DB cost at scale.
const contentScoreCacheTTL = 60 * time.Second

// contentScoreCache memoizes the expensive (~3-query) per-content score. A
// single feed request references the same ~100 content IDs 2-4× (scoring loop,
// MMR, session-diversity, post-composition stash, tail) and many concurrent
// users request overlapping content — without this that was ~450-750 redundant
// Postgres round-trips per page. Scores are user-independent (keyed only by
// type:id) so the cache is global and shared across all requests; at high QPS
// it collapses to ~1 compute per item per TTL window.
var contentScoreCache = NewSignalCache[*ContentScore](contentScoreCacheTTL)

// disableContentScoreCache is set by tests (TestMain) so unit tests see fresh
// per-call computation and never leak a cached score across cases.
var disableContentScoreCache bool

// getContentScore returns the per-content score, served from a short-TTL cache
// when possible. The returned *ContentScore is SHARED and MUST be treated as
// read-only by callers (verified: no call site mutates it). The actual DB
// aggregation lives in computeContentScore.
func getContentScore(contentID, contentType string) *ContentScore {
	if disableContentScoreCache {
		return computeContentScore(contentID, contentType)
	}
	key := contentType + ":" + contentID
	if cached, ok := contentScoreCache.Get(key); ok {
		return cached
	}
	cs := computeContentScore(contentID, contentType)
	contentScoreCache.Set(key, cs)
	return cs
}

// Bayesian priors for engagement rates. A new item's rate is shrunk toward
// these platform averages until it has enough impressions to speak for itself,
// so a 1-like-from-1-view item isn't mistaken for a guaranteed hit, nor a
// 0-like-from-3-views item for a dud. This is how TikTok/IG/YT judge content —
// by RATE with confidence — instead of absolute counts that bury low-volume
// content. ratePriorStrength is measured in pseudo-impressions.
const (
	priorLikeRate     = 0.05
	priorShareRate    = 0.01
	priorCommentRate  = 0.02
	priorRewatchRate  = 0.05
	ratePriorStrength = 20.0
)

// smoothedRate is the posterior mean of a positive-rate under a Beta prior
// (add-(α,β) smoothing): (positives + priorRate·strength) / (trials + strength).
// Small samples sit near the prior; large samples converge to the true rate.
func smoothedRate(positives, trials, priorRate, priorStrength float64) float64 {
	if trials < 0 {
		trials = 0
	}
	return (positives + priorRate*priorStrength) / (trials + priorStrength)
}

// computeContentScore does the actual per-content DB aggregation. Prefer the
// cached getContentScore unless a deliberately fresh read is required.
func computeContentScore(contentID, contentType string) *ContentScore {
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
		WHERE content_id = $1 AND content_type = $2
		  AND created_at > NOW() - INTERVAL '90 days'`,
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

	// Quality score: engagement RATES (per view) with Bayesian shrinkage toward
	// platform priors, so small samples are neither over- nor under-trusted — the
	// fix for "low-view content gets buried / a 1-view fluke looks viral". Computed
	// for ALL content (even 0 views → a fair prior-based baseline, never 0), so a
	// brand-new upload starts on a level field and the audition + breakout boost
	// carry it from there.
	trials := float64(viewCount)
	likeRate := smoothedRate(float64(likeCount), trials, priorLikeRate, ratePriorStrength)
	shareRate := smoothedRate(float64(shareCount), trials, priorShareRate, ratePriorStrength)
	commentRate := smoothedRate(float64(commentCount), trials, priorCommentRate, ratePriorStrength)
	rewatchRate := smoothedRate(float64(rewatchCount), trials, priorRewatchRate, ratePriorStrength)

	// Completion is neutral until we actually have watch data — don't punish a
	// video for having no views yet.
	completionScore := 0.4
	if viewCount > 0 {
		if avgCompletion >= 0.9 {
			completionScore = 1.0 // Strong: almost everyone finishes
		} else if avgCompletion >= 0.7 {
			completionScore = 0.7 // Good: most people watch most of it
		} else if avgCompletion >= 0.5 {
			completionScore = 0.4 // Neutral: half the content
		} else {
			completionScore = 0.1 // Weak: people leave early
		}
	}

	// Normalize the rate terms by their priors so a merely-average item lands
	// near the middle (a raw 5% like-rate shouldn't read as "0.05 quality").
	likeQ := math.Min(1.0, likeRate/(priorLikeRate*3))
	shareQ := math.Min(1.0, shareRate/(priorShareRate*3))
	commentQ := math.Min(1.0, commentRate/(priorCommentRate*3))
	rewatchQ := math.Min(1.0, rewatchRate/(priorRewatchRate*3))
	cs.QualityScore = (completionScore*0.25 + likeQ*0.15 + shareQ*0.25 +
		rewatchQ*0.20 + commentQ*0.15) * (1.0 - cs.SkipRate*0.5)

	// Trending: blends absolute VELOCITY (engagement in the last 2h — catches
	// established viral) with engagement RATE (Wilson lower bound of
	// engagement-per-view — lets a NEW video with a strong rate break out from a
	// small audience, the way TikTok escalates a high-performing test). Either
	// path can trend an item, so capable content isn't gated behind raw volume.
	var recentEng, recentViews int
	db.QueryRow(`
		SELECT
			COUNT(*) FILTER (WHERE event_type IN ('like','comment','share','save')),
			COUNT(*) FILTER (WHERE event_type = 'view')
		FROM feed_events
		WHERE content_id = $1 AND content_type = $2
		  AND created_at > NOW() - INTERVAL '2 hours'`,
		contentID, contentType).Scan(&recentEng, &recentViews)
	velocity := math.Min(1.0, float64(recentEng)/15.0)
	rate := 0.0
	if recentViews >= 3 {
		rate = wilsonLowerBound(float64(recentEng), float64(recentViews))
	}
	cs.TrendingScore = math.Max(velocity, rate)

	// Energy level inference (rewatch/share/completion-modulated; the engagement
	// stats it reads are already populated above). Captured here so it can serve
	// as the fallback when a creator hasn't declared an energy_level
	// ('medium'/unset) — otherwise this computed signal was always discarded by
	// the flat 0.55 default in the branches below, leaving energyFit nearly
	// constant. An explicit 'low'/'high' declaration is still trusted as-is.
	cs.EnergyLevel = inferContentEnergy(contentType, cs)
	inferredEnergy := cs.EnergyLevel

	// Category, creator info, and created_at — single query per content type
	if contentType == "challenge" {
		var subject, prefix, dbCategory, dbEnergy string
		var emotionJSON []byte
		var creatorID, league string
		var followers, wins, losses, respCount, chViews, chLikes int
		var createdAt time.Time
		db.QueryRow(`
			SELECT COALESCE(c.subject,''), COALESCE(c.prefix,''),
				COALESCE(c.category,'other'), COALESCE(c.energy_level,'medium'),
				COALESCE(c.emotion_tags,'[]'::JSONB),
				CAST(u.id AS TEXT), u.league,
				(SELECT COUNT(*) FROM follows WHERE following_id = u.id),
				u.wins, u.losses, c.created_at,
				(SELECT COUNT(*) FROM challenge_responses WHERE challenge_id = c.id),
				COALESCE(c.views, 0),
				(SELECT COUNT(*) FROM challenge_likes WHERE challenge_id = c.id)
			FROM challenges c
			JOIN users u ON c.creator_id = u.id
			WHERE c.id = $1`, contentID).Scan(
			&subject, &prefix, &dbCategory, &dbEnergy, &emotionJSON,
			&creatorID, &league, &followers, &wins, &losses, &createdAt, &respCount,
			&chViews, &chLikes)
		cs.ResponseCount = respCount

		// ── Bootstrap counters from raw challenge counts when feed_events is sparse ──
		//
		// cs.ViewCount and cs.LikeCount above were filled from feed_events. In
		// production those fill quickly, but for newly-seeded content (or any
		// content created before the analytics pipeline went live) the
		// challenge already has real views/likes recorded directly on the row.
		// Without this fallback, coldContentBonus stays stuck at its maximum
		// for every item in the corpus (everything looks "cold" because no
		// feed_events view records exist), and the quality formula gates on
		// totalInteractions>0 so it produces 0 for every challenge that
		// hasn't been engaged with through the analytics layer yet.
		//
		// Take the max of the two so live traffic + analytics is preferred
		// once it arrives, but we always have a meaningful baseline.
		if chViews > cs.ViewCount {
			cs.ViewCount = chViews
		}
		if chLikes > cs.LikeCount {
			cs.LikeCount = chLikes
		}

		// Supplement quality from the challenge row's own like/view counts when
		// feed_events analytics is sparse (seeded content, or before the pipeline
		// caught up). Smoothed the SAME way as the main path so small samples
		// aren't over-trusted, capped at 0.5, and MAX'd in — so it only ever lifts
		// a low score, never lowers a real analytics-driven one. (Replaces the old
		// `QualityScore == 0` gate, which is now dead since smoothing makes quality
		// always > 0, and the absolute 50-view floor that buried sparse content.)
		if cs.ViewCount > 0 {
			rowQ := math.Min(0.5, smoothedRate(float64(cs.LikeCount), float64(cs.ViewCount), priorLikeRate, ratePriorStrength)/(priorLikeRate*3)*0.5)
			if rowQ > cs.QualityScore {
				cs.QualityScore = rowQ
			}
		}

		// Synthetic trending fallback: when there's no recent feed_events
		// engagement, use raw recent view velocity as a proxy for fresh content.
		if cs.TrendingScore == 0 && cs.ViewCount >= 20 {
			ageHours := time.Since(createdAt).Hours()
			if ageHours <= 48 && ageHours > 0 {
				viewsPerHour := float64(cs.ViewCount) / ageHours
				// 10 views/hour ≈ noteworthy for a small platform; cap at 0.3
				cs.TrendingScore = math.Min(0.3, viewsPerHour/30.0)
			}
		}

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
			// 'medium'/unset = creator didn't really declare → use the inferred
			// (engagement-modulated) energy instead of a flat constant.
			cs.EnergyLevel = inferredEnergy
		}
		// Discrete label energy on the SAME scale EnergyByHour is trained on, so
		// energyHourMatch compares like-for-like. cs.EnergyLevel above may be the
		// inferred value for medium/unset (which energyFit wants) but would skew the
		// hour-match against the label-built EnergyByHour.
		cs.EnergyLevelLabel = energyStringToFloat(dbEnergy)
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
			// 'medium'/unset = creator didn't really declare → use the inferred
			// (engagement-modulated) energy instead of a flat constant.
			cs.EnergyLevel = inferredEnergy
		}
		// Discrete label energy on the SAME scale EnergyByHour is trained on, so
		// energyHourMatch compares like-for-like. cs.EnergyLevel above may be the
		// inferred value for medium/unset (which energyFit wants) but would skew the
		// hour-match against the label-built EnergyByHour.
		cs.EnergyLevelLabel = energyStringToFloat(dbEnergy)
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

// energyStringToFloat is the ONE canonical mapping from an energy_level label to
// a 0..1 number. Both the per-hour energy preference (EnergyByHour) and the
// content scorer's cs.EnergyLevel must use it so the train-time and serve-time
// scales are identical — they previously diverged (0.2/0.5/0.8 vs 0.25/0.55/0.85
// for the same labels), putting a permanent ~0.05 offset into energyHourMatch.
func energyStringToFloat(energy string) float64 {
	switch energy {
	case "low":
		return 0.25
	case "high":
		return 0.85
	default: // "medium"/unset
		return 0.55
	}
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

	// ── COHORT + NEGATIVE SIGNAL CONTEXT ──
	// Loaded once per user at the top of SmartFeedHandler; fetched from the
	// per-request caches here. Missing ⇒ safe defaults (engaged cohort, no
	// penalties).
	cohort := classifyCohort(profile)
	cw := weightsFor(cohort)
	ns := getNegativeSignals(profile.UserID)
	breakdown["cohort"] = float64(cohortOrdinal(cohort)) // numeric handle for debug

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
	// Clamp age at 0 so a future-dated row (clock skew) can't make the exponent
	// positive and push freshness > 1 — the one unbounded base factor otherwise.
	hoursSince := math.Max(0, time.Since(cs.CreatedAt).Hours())
	freshness := math.Exp(-0.693 * hoursSince / freshnessHalfLifeHours) // ln(2) ≈ 0.693
	breakdown["freshness"] = freshness

	// ── ENERGY FIT ──
	// How well does the content's energy match the user's current state?
	// Perfect match = 1.0, complete mismatch = 0.0
	currentEnergy := profile.EnergyPreference
	if session.DopamineBudget < 0.3 {
		// Fatigued → lower their effective energy preference (want calmer content).
		currentEnergy *= session.DopamineBudget / 0.3
	} else if session.DopamineBudget > 0.7 {
		// Highly stimulated → nudge the target energy UP: they can handle (and
		// want) more intense content, so energy matching tracks current arousal,
		// not just long-term taste. Bounded so it only ever nudges.
		currentEnergy += (1.0 - currentEnergy) * 0.3 * (session.DopamineBudget - 0.7) / 0.3
	}
	currentEnergy = math.Max(0, math.Min(1.0, currentEnergy))
	energyFit := 1.0 - math.Abs(currentEnergy-cs.EnergyLevel)
	breakdown["energyFit"] = energyFit
	// Surface the absolute content energy (0=chill .. 1=intense) so slot buckets
	// can select on actual calmness. energyFit is a MATCH metric and is high for
	// a perfect match at ANY energy, so it can't stand in for "low energy".
	breakdown["energyLevel"] = cs.EnergyLevel

	// ── RELEVANCE ──
	// Category match from user profile affinity
	relevance := 0.0
	if affinity, ok := profile.CategoryAffinity[cs.Category]; ok {
		relevance = affinity
	}
	// Negative signal: avoided category. Use the MORE negative of the flat
	// avoided penalty and any already-set mined negative affinity, so the -0.3
	// doesn't MASK a stronger mined dislike (e.g. a -0.5 negative affinity).
	for _, avoided := range profile.AvoidedCategories {
		if avoided == cs.Category {
			if relevance > -0.3 {
				relevance = -0.3 // Active penalty
			}
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
	// Long-term engagement strength with this creator (loops/completions/likes),
	// read here so the "new creator" test reflects GENUINE unfamiliarity — a
	// creator the user has watched many times but not followed must not still
	// count as maximally novel.
	creatorAff := getCreatorAffinity(profile.UserID, cs.CreatorID)
	novelty := 0.0
	if _, seen := profile.CategoryAffinity[cs.Category]; !seen {
		novelty = 0.6 // New category = exploration opportunity
	}
	isNewCreator := creatorAff == 0 // no prior engagement with this creator
	for _, pc := range profile.PreferredCreators {
		if pc == cs.CreatorID {
			isNewCreator = false
			break
		}
	}
	if isNewCreator && !followingSet[cs.CreatorID] {
		novelty = math.Min(1.0, novelty+0.4) // New creator the user has never engaged
	}
	// Preserve the RAW (pre-tolerance) novelty for slot ELIGIBILITY: discovery /
	// surprise buckets gate on this so low-NoveltyTolerance users — who most need
	// bubble-breaking — still get those slots filled, instead of an empty
	// discovery slot. The SCORE-affecting term below stays tolerance-scaled, so
	// novelty-averse users get exploration in the slot but it doesn't dominate
	// their ranking.
	breakdown["noveltyRaw"] = novelty
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
	// creatorAff is read in the NOVELTY block above (so the new-creator test can
	// use it) and reused here.
	creatorAffinityBoost := creatorAff * 0.20
	breakdown["creatorAffinityBoost"] = creatorAffinityBoost

	// ── DWELL-WEIGHTED INTENT (precomputed nightly) ──
	// Users who linger on challenge-detail pages express deeper intent than
	// reflex-skimmers. Nudge challenge content up for those users.
	dwellBoost := 0.0
	if cs.ContentType == "challenge" {
		if avgMs := getPageDwellMs(profile.UserID, "challenge_detail_page"); avgMs > 4000 {
			// 4s = casual glance, 10s+ = real read. Raw boost caps at 0.10; the
			// final term is dwellBoost*cw.Affinity (added in baseScore), so a
			// high-affinity-weight cohort (e.g. at_risk cw.Affinity=1.5) can reach
			// ~0.15 — consistent with the sibling creatorAffinityBoost*cw.Affinity.
			over := float64(avgMs-4000) / 6000.0
			if over > 1 {
				over = 1
			}
			dwellBoost = 0.10 * over
		}
	}
	breakdown["dwellIntentBoost"] = dwellBoost

	// ── BATTLE BOOST (core product bias) ──
	// devf is a head-to-head challenge app. A challenge with at least one
	// response (a "battle") is the main event — two creators going at it,
	// votable, watchable end-to-end. A challenge with zero responses is a
	// "short": surfaced for content volume but not the primary surface we
	// want users to associate with the app. Without an explicit battle bias,
	// shorts dominate the feed because (a) every challenge starts as a short
	// before it gets a response and (b) shorts vastly outnumber battles in
	// any normal content library. This term makes the ranker prefer battles
	// strongly — never a hard filter (we still need shorts to fill volume),
	// but a thumb on the scale that's hard to outweigh.
	battleBoost := 0.0
	if cs.ContentType == "challenge" {
		if cs.ResponseCount > 0 {
			// Base battle bonus is large enough to lift a battle one or two
			// rank steps over a comparable short. Logarithmic scaling on
			// response count: a battle with 1 response gets +0.30; one with
			// 5 responses gets ~+0.41; with 20+ ~+0.50 cap. Coefficient 0.20 (not
			// 0.10) so the log term spans the full 0.30→0.50 range over the ~21-
			// response window; at 0.10 the boost topped out near 0.40 and the 0.50
			// clamp was effectively dead (only reached at ~442 responses).
			battleBoost = 0.30 + 0.20*math.Log1p(float64(cs.ResponseCount-1))/math.Log1p(20)
			if battleBoost > 0.50 {
				battleBoost = 0.50
			}
		} else {
			// Light penalty on shorts so two equally-scored items always tie
			// in favor of the battle. -0.10 is enough to break ties without
			// pushing shorts off the feed entirely.
			battleBoost = -0.10
		}
	}
	breakdown["battleBoost"] = battleBoost

	// ── SOCIAL-DRIVE WEIGHTING ──
	// High SocialDrive users benefit more from social signal and tie-strength;
	// low-SocialDrive users get a flatter blend (they prefer stranger content).
	// SocialDrive is owned and written to user_profiles by computeUserProfile
	// (the analytics job deliberately does NOT upsert that column — see
	// computeSocialDrive's comment — to avoid a double-writer). So
	// profile.SocialDrive is already the fresh value; if the profile hasn't been
	// computed yet, fall back to the precomputed cache (seeded from realtime
	// events) before defaulting to the neutral midpoint.
	//
	// `sd == 0` is now a RELIABLE "unset" sentinel: computeUserProfile builds
	// SocialDrive via smoothedRate toward a 0.5 prior, which always returns a
	// value strictly > 0 (≥ ~0.04 even for a genuine zero-follow user). So a real
	// low-drive user is no longer rewritten to the fallback — only a truly
	// uncomputed field (zero value) is, which is exactly the intended behavior.
	sd := profile.SocialDrive
	if sd == 0 {
		sd = getSocialDriveFallback(profile.UserID)
	}
	socialWeightMult := 0.7 + 0.6*sd // range 0.7 .. 1.3
	breakdown["socialDriveMult"] = socialWeightMult

	// ── SEARCH-INTENT BOOST (Tier 1.4) ──
	// Reads the user's last 3 search queries (captured by SearchHandler) and
	// biases the feed toward matching categories/captions for 24h.
	searchB := searchBoost(ns, cs.Category, "")
	wSearch := 0.18 // up to ~+0.18 for an exact query hit
	searchTerm := searchB * wSearch * cw.Search
	breakdown["searchBoost"] = searchTerm

	// ── EXPERIMENT WEIGHT OVERRIDES ──
	// If an A/B test is active for this user, its variant config supplies
	// per-dimension multipliers (defaults to 1.0). This lets us ship ranker
	// changes behind a variant without redeploying.
	expCfg := getExperimentConfig(profile.UserID)
	// Experiment weights are full OVERRIDES of the weight constants, keyed
	// "wSocial"/"wFreshness"/... to match experiments.go. The old xw() looked up
	// bare "social" (never present in the config → always returned 1.0) AND
	// multiplied it onto the constant, so the A/B test had ZERO effect on the
	// score and would have reported a false null result. wsel returns the
	// variant's weight when present, else the default constant — so "control"
	// (wSocial=0.25) reproduces the default exactly.
	wsel := func(key string, def float64) float64 {
		if expCfg != nil {
			if v, ok := expCfg[key]; ok {
				return v
			}
		}
		return def
	}

	// ── BASE SCORE (cohort-weighted, experiment-scaled) ──
	baseScore := social*wsel("wSocial", wSocial)*socialWeightMult*cw.Social +
		freshness*wsel("wFreshness", wFreshness)*cw.Freshness +
		energyFit*wsel("wEnergyFit", wEnergyFit)*cw.EnergyFit +
		relevance*wsel("wRelevance", wRelevance)*cw.Relevance +
		quality*wsel("wQuality", wQuality)*cw.Quality +
		novelty*wsel("wNovelty", wNovelty)*cw.Novelty +
		// tie-strength is a social-graph signal, so SocialDrive scales it too (the
		// socialWeightMult comment promises BOTH social and tie-strength; it was
		// previously applied to social only).
		tieBoost*cw.Tie*socialWeightMult +
		creatorAffinityBoost*cw.Affinity +
		// dwellBoost is a precomputed-intent signal like tie/affinity, so it's
		// cohort-weighted (reusing cw.Affinity) instead of leaking in un-gated —
		// e.g. cold_start, where cw.Affinity=0, no longer gets the full boost.
		dwellBoost*cw.Affinity +
		searchTerm

	// ── EGO BOOST (conditional) ──
	// Validating content for ego-sensitive users. Gated ONLY on EgoSensitivity,
	// not on a loss streak: the slotEgoBoost patterns are emitted for winners and
	// the default population too, but egoBonus used to be non-zero ONLY for
	// ego-sensitive LOSERS — so the ego_boost bucket was empty for everyone who
	// actually requested the slot, silently falling back to hook content. The
	// sub-signals below (high-affinity "I'm good at this" + lower-league creator
	// "I'm better than this") apply to winners as much as losers; the feed
	// PATTERN, not this gate, decides when to lean on ego content (e.g. post-loss).
	egoBonus := 0.0
	if profile.EgoSensitivity > 0.5 {
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

	// ── CREATOR FATIGUE PENALTY (Tier 1.1) ──
	// Seeing the same creator 3+ times in a session makes the feed feel like
	// a single-channel loop. Penalty grows with each repeat, capped at -0.35.
	creatorFatigue := 0.0
	if session.CreatorsSeen != nil && cs.CreatorID != "" {
		seenC := session.CreatorsSeen[cs.CreatorID]
		if seenC >= 2 {
			creatorFatigue = -0.12 * float64(seenC-1)
			if creatorFatigue < -0.35 {
				creatorFatigue = -0.35
			}
		}
	}
	breakdown["creatorFatigue"] = creatorFatigue

	// ── SEQUENCE PENALTY (Tier 3.12) ──
	// Penalise the same category/creator if it's one of the last 2 items shown
	// — keeps the feed rhythm varied even when score ties would cluster them.
	sequencePenalty := 0.0
	if n := len(session.LastCategories); n > 0 {
		if session.LastCategories[n-1] == cs.Category {
			sequencePenalty -= 0.08
		}
		if n >= 2 && session.LastCategories[n-2] == cs.Category {
			sequencePenalty -= 0.05
		}
	}
	if n := len(session.LastCreators); n > 0 && cs.CreatorID != "" {
		if session.LastCreators[n-1] == cs.CreatorID {
			sequencePenalty -= 0.10
		}
		if n >= 2 && session.LastCreators[n-2] == cs.CreatorID {
			sequencePenalty -= 0.06
		}
	}
	breakdown["sequencePenalty"] = sequencePenalty

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

	// ── COLD CONTENT / AUDITION BONUS ──
	// New content needs enough impressions before its engagement is a reliable
	// signal. Give it a modest, decaying boost across the whole audition window
	// (not just <5 views — a 5-view sample can't measure quality) so it climbs
	// above already-proven content while it gathers data, tapering to 0 once it
	// has enough views to be judged on merit. auditionEligible marks it for the
	// guaranteed exploration slot (injectAuditionContent) so it gets seen even if
	// merit-ranking would still bury it.
	coldContentBonus := 0.0
	if cs.ViewCount < auditionViewTarget {
		coldContentBonus = 0.25 * (1.0 - float64(cs.ViewCount)/float64(auditionViewTarget))
		breakdown["auditionEligible"] = 1
	}
	breakdown["coldContentBonus"] = coldContentBonus

	// ── TRENDING BONUS ──
	trendingBonus := cs.TrendingScore * 0.15
	breakdown["trendingBonus"] = trendingBonus

	// ── CONTEXT-AWARE: HOUR ROUTING ──
	// Delegated to hour_routing.go which now also rewards adjacent-hour and
	// same-bucket category matches with a tapered scale (not just exact-hour),
	// and aligns content energy with the user's typical energy at this hour.
	// Both signals are bounded so a cold profile / wrong inference can only
	// nudge the score, never capsize it.
	// Use the user's LOCAL time so hour routing matches the local-hour buckets
	// computeUserProfile builds (both shifted by the same TZOffsetMin). 0 offset
	// = UTC on both sides, so absent tz behaves exactly as before.
	now := time.Now().UTC()
	if session != nil {
		now = now.Add(time.Duration(session.TZOffsetMin) * time.Minute)
	}
	hourBonus := categoryHourBoost(profile, cs.Category, now)
	// Use the discrete LABEL energy (not the possibly-inferred cs.EnergyLevel) so
	// this matches EnergyByHour, which is trained from the same label scale.
	hourBonus += energyHourMatch(profile, cs.EnergyLevelLabel, now)
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
	// After wins/losses, boost categories the user gravitates toward in that state.
	// Bonus = (category share) × (confidence) × 0.08, where confidence shrinks the
	// boost toward 0 when the ego bucket has few total observations: a single
	// post-win engagement (1-of-1 → share 1.0) must NOT read as a confident
	// preference and earn the full boost off one event.
	egoContextBonus := 0.0
	var egoCats map[string]float64
	if profile.RecentWins > profile.RecentLosses {
		egoCats = profile.CategoryByEgo["winning"]
	} else if profile.RecentLosses > profile.RecentWins {
		egoCats = profile.CategoryByEgo["losing"]
	}
	if score, ok := egoCats[cs.Category]; ok {
		maxScore, totalObs := 0.0, 0.0
		for _, s := range egoCats {
			totalObs += s
			if s > maxScore {
				maxScore = s
			}
		}
		if maxScore > 0 {
			confidence := totalObs / (totalObs + 5.0) // shrink small samples toward 0
			egoContextBonus = (score / maxScore) * confidence * 0.08
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
	negativeEmotions := wellbeingNegativeEmotions
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
		// Pseudo-random trigger that actually VARIES per candidate. The old seed
		// used len(cs.ContentID) — the DIGIT COUNT of a SERIAL id — which is the
		// same for nearly every item on a page, and ItemsSeen is frozen during a
		// scoring pass, so a whole page got the jackpot or none did (no variable
		// ratio at all). Hash the content id so each item gets an independent draw.
		var h uint32 = 2166136261
		for i := 0; i < len(cs.ContentID); i++ {
			h ^= uint32(cs.ContentID[i])
			h *= 16777619
		}
		rewardSeed := (session.ItemsSeen*7 + int(h)) % 11
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
			if br := stats.ShrunkBounceRate(); br > bounceRateNegativeThreshold {
				// PENALTY path uses the SHRUNK rate so n=5 can't "prove" a dislike
				// and bury a whole category on noise. Capped at -0.25.
				impressionPenalty = -0.25 * ((br - bounceRateNegativeThreshold) / (1.0 - bounceRateNegativeThreshold))
			} else if raw := stats.BounceRate(); raw < bounceRatePositiveThreshold && stats.InterestCount > 0 {
				// INTEREST path uses the RAW rate: shrinking a low bounce toward the
				// 0.5 prior made the min shrunk rate 5/(n+10) ≥ 0.2 until n>15, so
				// this +0.12 bonus was unreachable for n=5..15 while the penalty
				// above could already fire at n=5 — an asymmetry that suppressed
				// genuine interest. A false small boost (also gated on InterestCount>0,
				// a real dwell signal) is far lower-risk than a false category dislike.
				impressionPenalty = 0.12 * (1.0 - raw/bounceRatePositiveThreshold)
			}
		}
	}
	breakdown["impressionBouncePenalty"] = impressionPenalty

	// ── PER-CREATOR BOUNCE PENALTY ──
	// Same signal as the category penalty, but for THIS creator — "I keep
	// scrolling straight past this creator". The aggregator already computes
	// byCreator stats; previously only the admin diagnostics endpoint read them.
	creatorBouncePenalty := 0.0
	if cs.CreatorID != "" {
		if byCr, ok := impressionCreatorStatsCache.Get(profile.UserID); ok {
			if stats, exists := byCr[cs.CreatorID]; exists && stats.Count >= minCategoryImpressions {
				if br := stats.ShrunkBounceRate(); br > bounceRateNegativeThreshold {
					creatorBouncePenalty = -0.20 * ((br - bounceRateNegativeThreshold) / (1.0 - bounceRateNegativeThreshold))
				}
			}
		}
	}
	breakdown["creatorBouncePenalty"] = creatorBouncePenalty

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

	// ── TAME THE JACKPOT BONUSES ──
	// The session-dynamics "jackpot" bonuses (variable reward, re-entry,
	// momentum, streak) can pile up and collectively dwarf the cohort-weighted
	// mood/energy/social base — turning the principled core into a minority of
	// the score. Group-cap their COMBINED contribution rather than re-tuning each
	// weight (which needs live A/B): the relative shape is preserved, only the
	// pathological total is clipped. Generous cap so normal scoring is untouched.
	const maxJackpotBonus = 0.60
	if jackpot := variableReward + reentryBonus + momentumBonus + streakBonus; jackpot > maxJackpotBonus {
		scale := maxJackpotBonus / jackpot
		variableReward *= scale
		reentryBonus *= scale
		momentumBonus *= scale
		streakBonus *= scale
		breakdown["jackpotCapScale"] = scale
	}

	// ── BREAKOUT (virality ladder) ──
	// Reward content performing well by RATE — high engagement-per-view, Wilson
	// lower-bounded so a tiny lucky sample doesn't qualify — by pushing it to a
	// wider audience regardless of absolute view count. This is how capable
	// content goes viral from a small start instead of being gated behind volume;
	// it pairs with the audition (which earns it the impressions to prove the rate).
	breakoutBonus := 0.0
	if cs.ViewCount >= 5 {
		posEng := float64(cs.LikeCount + cs.ShareCount + cs.CommentCount)
		breakoutBonus = math.Min(0.25, wilsonLowerBound(posEng, float64(cs.ViewCount))*0.6)
	}
	breakdown["breakoutBonus"] = breakoutBonus

	// ── BEHAVIORAL-BONUS GROUP CAP ──
	// The per-cohort weighting (weightsFor / cohortWeightTable) governs the six
	// baseScore terms, but the additive in-session REACTION bonuses below are
	// un-cohort-weighted; when several fire at once their sum can dwarf the
	// weighted base, leaving cohort personalization in charge of only a minority
	// of the score. Group-cap their COMBINED contribution (same philosophy as the
	// jackpot cap above), preserving relative shape rather than re-tuning each.
	// DISCOVERY bonuses (unseen/cold/trending/breakout) are deliberately EXCLUDED
	// so genuinely new content keeps its full push. Generous cap: most of these
	// are 0 on a normal item, so only pathological stacks are clipped.
	const maxBehavioralBonus = 0.70
	// Sum only the POSITIVE contributors: several of these can be negative
	// (wellbeingBonus spiral penalty -0.15, energy-mismatch hourBonus down to
	// -0.04, collabBonus has no lower clamp). Scaling a negative member by
	// scale<1 would weaken a penalty exactly when many positives stack — the
	// opposite of the cap's intent. So we cap the positive sum and scale only
	// positive members, leaving penalties intact.
	posBehavioral := 0.0
	for _, b := range []float64{egoBonus, hourBonus, emotionBonus, egoContextBonus, wellbeingBonus,
		collabBonus, scrollBackBonus, completeBonus, loopBonus, unmuteBonus, profileVisitBonus, battleBoost} {
		if b > 0 {
			posBehavioral += b
		}
	}
	if posBehavioral > maxBehavioralBonus {
		scale := maxBehavioralBonus / posBehavioral
		capPos := func(v float64) float64 {
			if v > 0 {
				return v * scale
			}
			return v
		}
		egoBonus = capPos(egoBonus)
		hourBonus = capPos(hourBonus)
		emotionBonus = capPos(emotionBonus)
		egoContextBonus = capPos(egoContextBonus)
		wellbeingBonus = capPos(wellbeingBonus)
		collabBonus = capPos(collabBonus)
		scrollBackBonus = capPos(scrollBackBonus)
		completeBonus = capPos(completeBonus)
		loopBonus = capPos(loopBonus)
		unmuteBonus = capPos(unmuteBonus)
		profileVisitBonus = capPos(profileVisitBonus)
		battleBoost = capPos(battleBoost)
		breakdown["behavioralCapScale"] = scale
	}

	// ── FINAL SCORE ──
	finalScore := baseScore + egoBonus + fatiguePenalty + creatorFatigue + sequencePenalty + dopaminePenalty +
		unseenBonus + coldContentBonus + trendingBonus + breakoutBonus +
		hourBonus + emotionBonus + egoContextBonus + wellbeingBonus +
		collabBonus + momentumBonus + variableReward + reentryBonus + streakBonus +
		impressionPenalty + creatorBouncePenalty + scrollBackBonus + completeBonus + loopBonus +
		unmuteBonus + profileVisitBonus + battleBoost

	// ── LEARNING-TO-RANK DELTA (Tier 3.11) ──
	// Small online-SGD residual that learns which score breakdowns correlate
	// with completions for this cohort. Adds a bounded correction, never more
	// than ±0.25 so the hand-tuned base score stays dominant until LTR has
	// enough evidence.
	ltrDelta := ltrScoreDelta(cohort, breakdown)
	breakdown["ltrDelta"] = ltrDelta
	finalScore += ltrDelta

	// ── PLATT-CALIBRATED LTR BONUS ──
	// The raw ltrDelta is a logit-ish correction on arbitrary scale. Passing
	// it through the fitted Platt calibrator yields a probability in (0,1)
	// that this user will positively engage. Re-scaled to ±0.15 it becomes
	// a second, well-shaped bonus that moves the needle predictably even
	// before LTR weights have converged.
	// Query the calibrator with the RAW logit z — the scale plattRecord trains
	// on. Passing the bounded ltrDelta (0.25·tanh(z)) here was a train/serve
	// mismatch that pinned calibBonus near a constant σ(B), wasting its budget.
	if z, ok := ltrRawLogit(cohort, breakdown); ok {
		p := plattCalibrate(z)
		// Centre around 0.5 so p≈0.5 contributes nothing, p≈1 adds ~+0.15.
		calibBonus := (p - 0.5) * 0.30
		breakdown["calibBonus"] = calibBonus
		finalScore += calibBonus
	}

	// ── WATCH-RATIO PREDICTION BONUS ──
	// Separate per-cohort regression head trained on observed completion
	// fraction. Predicts the % of duration this user is likely to watch and
	// converts (centered on 0.5) to a ±0.18 ranking bonus. Returns 0 until
	// the cohort accumulates wrMinSamples so the model doesn't add noise.
	wrBonus := wrPredictBonus(cohort, breakdown)
	if wrBonus != 0 {
		breakdown["watchRatioBonus"] = wrBonus
		finalScore += wrBonus
	}

	// ── BAYESIAN UNCERTAINTY BONUS (active learning) ──
	// Thompson-sample noise scaled by the current cohort's predictive
	// stddev. Uncertain items get a noisy boost (we want to learn);
	// confident items move less. Native exploration without bandit overhead.
	//
	// The per-item modulation 4·σ(x)·(1-σ(x)) peaks at x=0, so x must be a
	// LOGIT-centered quantity. Pass the LTR raw logit z (centered), NOT finalScore
	// — finalScore is a strictly-positive additive sum sitting on the sigmoid's
	// right shoulder, which made the exploration bell peak at the LOWEST-scoring
	// items instead of the genuinely mid-confidence ones. Fallback when the LTR
	// head isn't warm: center finalScore by its rough median so the bell at least
	// sits in the populated score region rather than at 0.
	uncArg, okLogit := ltrRawLogit(cohort, breakdown)
	if !okLogit {
		uncArg = finalScore - 1.2 // rough median of the ~[0,3] finalScore range
	}
	uncBonus := bayesianUncertaintyBonus(cohort, uncArg, nil)
	if uncBonus != 0 {
		breakdown["uncertaintyBonus"] = uncBonus
		finalScore += uncBonus
	}

	// ── CREATOR RESIDUAL CALIBRATION ──
	// Self-correcting bias: if this creator is consistently over-served vs
	// actual engagement, adjust their score down. Bounded ±0.20 and gated
	// on creator having enough served items to estimate the bias reliably.
	if cs.CreatorID != "" {
		residualAdj := creatorResidualAdjustment(cs.CreatorID)
		if residualAdj != 0 {
			breakdown["creatorResidualAdj"] = residualAdj
			finalScore += residualAdj
		}
	}

	// ── SESSION-TRAJECTORY BONUS ──
	// Markov-style: predict what (category × energy) bucket comes next given
	// the user's most recent positively-engaged item. Bounded ±0.10. Active
	// only when the cohort has enough observed transitions to make a useful
	// prediction (cold model returns 0).
	if session != nil && cs.Category != "" && len(session.LastCategories) > 0 {
		fromKey := strings.ToLower(session.LastCategories[len(session.LastCategories)-1]) + ":med"
		toKey := trajectoryStateKey(cs.Category, cs.EnergyLevel)
		trajBonus := trajectoryBonus(cohort, fromKey, toKey)
		if trajBonus != 0 {
			breakdown["trajectoryBonus"] = trajBonus
			finalScore += trajBonus
		}
	}

	// ── MOOD TRANSITION BONUS ──
	// Operationalizes "feel better after 20 min" — boosts content whose
	// mood is the empirically-healthy next step from the user's current
	// detected mood. Cold model uses sensible priors; learned overrides
	// once we have enough observations.
	if session != nil && session.DetectedMood != "" {
		emotions := getContentEmotions(cs.ContentID, cs.ContentType)
		if len(emotions) > 0 {
			moodBonus := moodTransitionBonus(session.DetectedMood, emotions)
			if moodBonus != 0 {
				breakdown["moodTransitionBonus"] = moodBonus
				finalScore += moodBonus
			}
		}
	}

	// ── SESSION CONTINUITY FACTOR (Tier 1.5) ──
	// Short gap since last session → dampen the "fresh-start" boosts (unseen,
	// cold content, novelty) so we don't shake up a still-warm feed.
	//
	// Applied BEFORE the negative-signal multiplier below: this subtraction must
	// not run AFTER a hard block forces the score to 0, or it would drive a
	// blocked item negative and break the "blocked = exactly 0" flat-floor
	// invariant (making blocked items rank by their explore terms — meaningless
	// ordering that can interleave with legitimately low-scored visible content).
	if ns != nil {
		contFactor := sessionContinuityFactor(ns) // 0.2 .. 1.0
		// Keep at least 20% of the exploration bonuses so curiosity isn't killed.
		damp := 0.4 + 0.6*contFactor // 0.52 .. 1.0
		// novelty term must use the experiment-overridable weight (wsel), matching
		// how baseScore added it — otherwise an active wNovelty experiment damps a
		// portion computed from the default weight, not the served one.
		exploreTerms := unseenBonus + coldContentBonus + novelty*wsel("wNovelty", wNovelty)*cw.Novelty
		finalScore -= (1.0 - damp) * exploreTerms
		breakdown["continuityFactor"] = contFactor
	}

	// ── NEGATIVE-SIGNAL MULTIPLIERS (Tier 1.2) ──
	// Creator block/unfollow penalty multiplies the whole score (blocked = 0).
	// Recent-bounce on this exact content zeros it out so we never re-serve
	// a just-bounced item.
	negMult := negativeCreatorPenalty(ns, cs.CreatorID) * bouncePenalty(ns, cs.ContentType, cs.ContentID)
	breakdown["negativeMult"] = negMult
	// Clamp to non-negative BEFORE the multiplicative attenuator — and this is the
	// LAST flooring op, so it also absorbs a continuity subtraction that dipped
	// below 0. The additive sum can go negative (stacked fatigue/sequence/bounce
	// penalties); multiplying a NEGATIVE score by negMult<1 makes it LESS negative
	// — the negative signal would BOOST a blocked/unfollowed creator's bad item
	// above an identically-bad non-penalized one. A penalty multiplier must only
	// ever attenuate a magnitude toward 0.
	if finalScore < 0 {
		finalScore = 0
	}
	finalScore *= negMult

	breakdown["baseScore"] = baseScore
	breakdown["finalScore"] = finalScore

	return finalScore, breakdown
}

// cohortOrdinal returns a stable int handle for a cohort — used for the
// debug breakdown ("cohort": N) so clients can render without a string mapping.
func cohortOrdinal(c Cohort) int {
	switch c {
	case CohortColdStart:
		return 0
	case CohortNew:
		return 1
	case CohortEngaged:
		return 2
	case CohortPower:
		return 3
	case CohortAtRisk:
		return 4
	}
	return 2
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
		default:
			// Strategy didn't resolve to a resisting rhythm (strategyStandard/
			// Emergency/""). Because this outer RL>=2 case already matched, the
			// fatigue branches below won't be reached — so honor dopamine state
			// here, or a fatigued+resisting user gets the default HIGH-energy
			// pattern, the opposite of what their state calls for.
			if session.DopamineBudget < 0.2 {
				basePattern = []string{
					slotHook, slotCooldown, slotSocial, slotCooldown,
					slotCliffhang, slotCooldown, slotHook, slotCooldown,
					slotSurprise, slotCooldown, slotEgoBoost, slotCooldown,
				}
			} else if session.DopamineBudget < 0.5 {
				basePattern = []string{
					slotHook, slotSocial, slotCooldown, slotCliffhang,
					slotHook, slotDiscovery, slotCooldown, slotSurprise,
					slotEgoBoost, slotSocial, slotCooldown, slotHook,
				}
			}
			// else: keep the default energetic pattern set before the switch.
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
		slotHook:       {},
		slotSocial:     {},
		slotDiscovery:  {},
		slotTrending:   {},
		slotChallenge:  {},
		slotCooldown:   {},
		slotEgoBoost:   {},
		slotCliffhang:  {},
		slotSurprise:   {},
		slotRival:      {},
		slotNostalgic:  {},
		slotFavCreator: {},
		slotFreshBlood: {},
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

		// Discovery: new category/creator for user. Gate on RAW (pre-tolerance)
		// novelty so low-NoveltyTolerance users still get a non-empty discovery
		// slot (bubble-breaking); their ranking still reflects the scaled novelty.
		if bd["noveltyRaw"] > 0.3 {
			buckets[slotDiscovery] = append(buckets[slotDiscovery], item)
		}

		// Trending: high trending score
		if bd["trendingBonus"] > 0.05 {
			buckets[slotTrending] = append(buckets[slotTrending], item)
		}

		// Challenge: genuinely COMPETITIVE content (an active battle with
		// responses), signalled by battleBoost>0 — not merely Type=="challenge",
		// which in a challenge-only corpus matched 100% of items and made this
		// slot identical to the hook bucket, so the competitive-personality /
		// win-streak / energetic-mood patterns got no real differentiation.
		if bd["battleBoost"] > 0 {
			buckets[slotChallenge] = append(buckets[slotChallenge], item)
		}

		// Cooldown: gentle, relaxing items. The feed is challenge-only now, so
		// the old `Type == "post"` gate left this bucket permanently empty (and
		// with it the calming mood patterns). Select on ABSOLUTE low content
		// energy OR an explicitly calming emotion tag so challenges can fill it.
		// (Was bd["energyFit"] > 0.7 — but energyFit is a match-to-user metric
		// that's high for a perfect match at ANY energy, so it would flood the
		// "palette cleanser" slot with the most intense content for a high-energy
		// user — the exact opposite of the slot's purpose.)
		if bd["energyLevel"] < 0.4 || hasEmotionTag(item, "chill") ||
			hasEmotionTag(item, "satisfying") || hasEmotionTag(item, "wholesome") {
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
		// Opposite of their usual preferences but objectively engaging. Gate on RAW
		// novelty (see discovery) so the slot isn't dead for low-tolerance users.
		if bd["noveltyRaw"] > 0.5 && bd["quality"] > 0.5 {
			buckets[slotSurprise] = append(buckets[slotSurprise], item)
		} else if bd["collabBonus"] > 0.1 && bd["relevance"] < 0.3 {
			// Similar users liked it but it's outside this user's normal categories
			buckets[slotSurprise] = append(buckets[slotSurprise], item)
		}

		// Rival: content from competitors — non-followed challenge creators, the
		// genuine "potential rival" signal. The bucket is score-sorted, so a
		// rival's active battle (higher battleBoost) naturally rises within it.
		// (Was gated on egoContextBonus, an ego-STATE CATEGORY-preference signal
		// that has nothing to do with rivalry/opponents.)
		if item.Item.Type == "challenge" && bd["social"] < 0.1 {
			buckets[slotRival] = append(buckets[slotRival], item)
		}

		// Nostalgic: proven personal favorites — content from creators/categories
		// the user has completed, looped, scrolled back to, or unmuted before.
		// Without this bucket the calming/nostalgic strategies and the
		// frustrated/bored mood patterns silently degraded to generic hook content.
		if bd["completeBonus"] > 0 || bd["scrollBackBonus"] > 0 ||
			bd["loopBonus"] > 0 || bd["unmuteBonus"] > 0 {
			buckets[slotNostalgic] = append(buckets[slotNostalgic], item)
		}

		// Fav creator: the user's top creators — strong creator affinity or a
		// recent profile visit. Powers the creator_focus strategy.
		if bd["creatorAffinityBoost"] > 0.1 || bd["profileVisitBonus"] > 0 {
			buckets[slotFavCreator] = append(buckets[slotFavCreator], item)
		}

		// Fresh blood: brand-new content the user has never seen — unseen AND
		// either recent or globally cold. Powers the fresh_blood discovery
		// strategy (used by the bored/at-risk recovery patterns).
		if bd["unseenBonus"] > 0 && (bd["freshness"] > 0.5 || bd["coldContentBonus"] > 0.2) {
			buckets[slotFreshBlood] = append(buckets[slotFreshBlood], item)
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
	// Prefer the tags already on the struct, but the For You candidate fetches
	// (fetchChallengesWindowedByKind and the candidate_sources.go sources) do NOT
	// SELECT emotion_tags, so these are usually empty — which left the cliffhang
	// slot permanently empty and the cooldown emotion fallback dead. Fall back to
	// getContentEmotions (Redis-cached) so emotion-tag slotting actually works.
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
	for _, t := range getContentEmotions(getItemID(item.Item), item.Item.Type) {
		if t == tag {
			return true
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

	// Tier 2.10 — cold-start QUALITY FLOOR (with widening fallback).
	// Brand-new users have no personal signal, so everything rides on "good
	// first impression." The strict pass requires recent + engaged content;
	// fallback passes progressively widen the recency window AND lower the
	// engagement threshold. Final pass returns ANY content rather than an
	// empty feed — "Nothing to play just yet" is a worse first impression
	// than imperfect content.
	type qualityTier struct {
		window   string
		minViews int
		minLikes int
	}
	tiers := []qualityTier{
		{"14 days", 10, 1}, // strict
		{"60 days", 5, 0},  // wider, looser
		{"180 days", 1, 0}, // very wide, minimal
		{"365 days", 0, 0}, // last resort
	}

	// Post entity retired — cold-start now mixes "battles" (challenges that
	// have at least one response) with "shorts" (challenges nobody has
	// responded to yet) at roughly 70/30. Both are challenge items; the split
	// just biases what kind of challenge a brand-new user sees first.
	battleLimit := (limit * 7) / 10
	shortLimit := limit - battleLimit
	if battleLimit < 1 {
		battleLimit = 1
	}
	if shortLimit < 1 {
		shortLimit = 1
	}
	battleOffset := (offset * 7) / 10
	shortOffset := offset - battleOffset

	// Walk tiers from strictest to widest. Each kind is independent: we
	// keep widening for battles even after shorts are full, and vice versa,
	// so we don't end up with 1 battle + 9 shorts just because shorts were
	// abundant in the strict window. The break condition is "both budgets
	// met" — without that, the strict tier exiting early caused the cold-
	// start feed to skew almost entirely to shorts in any test corpus where
	// most battles happened to be older than the strict 14-day window.
	//
	// Re-fetching on each tier is safe: filters are monotonically inclusive
	// (wider window + lower minViews/minLikes), so re-running with a wider
	// tier returns a superset ordered by the same (views + likes*3) DESC
	// scoring. We always keep the strongest results.
	// Over-fetch by one item per kind so hasMore can be true. Without this,
	// battleLimit+shortLimit == limit exactly and every cold-start query uses
	// LIMIT == its budget, so len(items) can never exceed limit and hasMore is
	// structurally always false — a brand-new user (the cohort we most need to
	// retain) could never paginate past page 1. We trim back to limit below.
	battleFetch := battleLimit + 1
	shortFetch := shortLimit + 1

	var battleItems, shortItems []HomeFeedItem
	for _, tier := range tiers {
		if len(battleItems) < battleFetch {
			battleItems = coldStartChallengesTiered("battle", battleFetch, battleOffset, tier.window, tier.minViews, tier.minLikes)
		}
		if len(shortItems) < shortFetch {
			shortItems = coldStartChallengesTiered("short", shortFetch, shortOffset, tier.window, tier.minViews, tier.minLikes)
		}
		if len(battleItems) >= battleFetch && len(shortItems) >= shortFetch {
			break
		}
	}

	items := append(battleItems, shortItems...)

	// Shuffle to mix battles and shorts so the user doesn't see one big block
	// of either kind.
	rand.Shuffle(len(items), func(i, j int) {
		items[i], items[j] = items[j], items[i]
	})

	hasMore := len(items) > limit
	if hasMore {
		items = items[:limit]
	}

	return items, hasMore, nil
}

// coldStartChallengesTiered runs one tier of the cold-start challenge query.
// kind is "battle" (challenges with at least one response) or "short"
// (challenges nobody has responded to yet) — anything else means "all".
func coldStartChallengesTiered(kind string, limit, offset int, window string, minViews, minLikes int) []HomeFeedItem {
	if db == nil {
		return nil
	}
	responseFilter := ""
	switch kind {
	case "battle":
		responseFilter = "AND (SELECT COUNT(*) FROM challenge_responses WHERE challenge_id = c.id) > 0"
	case "short":
		responseFilter = "AND (SELECT COUNT(*) FROM challenge_responses WHERE challenge_id = c.id) = 0"
	}
	rows, err := db.Query(`
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
		AND c.created_at > NOW() - ($3::text)::interval
		AND c.views >= $4
		AND COALESCE(cl.likes,0) >= $5
		`+responseFilter+`
		ORDER BY (c.views + COALESCE(cl.likes,0) * 3) DESC, c.created_at DESC
		LIMIT $1 OFFSET $2`, limit, offset, window, minViews, minLikes)
	if err != nil {
		return nil
	}
	defer rows.Close()
	var items []HomeFeedItem
	for rows.Next() {
		var ch Challenge
		var creatorID, views, likes, responseCount int
		var createdAt, expiresAt time.Time
		rows.Scan(&ch.ID, &creatorID, &ch.CreatorUsername, &ch.CreatorLeague,
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
	return items
}

// (coldStartPostsTiered retired — the home reels feed is challenge-only now.
// What used to be the "post" cold-start branch is replaced by "shorts": the
// kind="short" call into coldStartChallengesTiered above.)

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

	userID := authUserID(r)
	sessionID := r.URL.Query().Get("sessionId")
	pageStr := r.URL.Query().Get("page")
	limitStr := r.URL.Query().Get("limit")
	debug := r.URL.Query().Get("debug") == "true"
	// refresh=true is the user's pull-to-refresh signal. Treat the same way
	// TikTok/Instagram do: drop the seen-content filter, reset session dedup
	// counters (so categoriesSeen/creatorsSeen fatigue doesn't drag from the
	// pre-refresh feed), and let the candidate sources re-shuffle freely.
	// Only honored on page=1 — refresh on a later page would either show a
	// duplicate of page 1 or be confusing UI behavior.
	refresh := r.URL.Query().Get("refresh") == "true"

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

	// Apply the refresh signal up-front so every downstream stage sees a
	// clean slate. We do NOT clear DopamineBudget, mood, or strategy memory
	// — those are session-state useful even across a refresh; we only wipe
	// the "what have I already shown / are we fatigued on this category"
	// signals that would otherwise re-bias the new feed toward the old one.
	if refresh && page == 1 {
		applyRefreshSignal(userID, sessionID)
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

	// Capture the client's UTC offset (minutes east of UTC) so hour-of-day
	// routing buckets by the user's LOCAL hour. Absent/invalid → 0 (UTC), which
	// matches the previous behaviour. Stored for the profile-build side too.
	if tz := r.URL.Query().Get("tzOffset"); tz != "" {
		if tzMin, err := strconv.Atoi(tz); err == nil && tzMin >= -840 && tzMin <= 840 {
			session.TZOffsetMin = tzMin
			go storeUserTZOffset(userID, tzMin)
		}
	}

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
	warmNegativeSignals(userID)

	// Step 5: Fetch candidates — multi-source retrieval (recency, trending,
	// follow-graph, collaborative, embedding-neighbors) merged with weighted
	// round-robin interleave. Per-cohort learned weights via
	// effectiveSourceWeights so cold/new/engaged/power/at_risk cohorts get
	// different mixes that reflect what's worked for them. Falls back
	// gracefully per-source on error.
	preCohort := classifyCohort(profile)
	// Scale the candidate pool with the page so deep in-session pagination keeps
	// finding fresh content. A fixed pool re-served the same top-N on every page,
	// and the seen-filter then dropped them — leaving thin pages and re-watches
	// mid-session even when fresh catalog existed. Capped at 3 pages' worth so a
	// high page number can't fetch an unbounded pool (scoring is O(candidates)).
	poolPages := page
	if poolPages > 3 {
		poolPages = 3
	}
	candidateLimit := limit * candidateMultiplier * poolPages
	candidates, candidateSourceMap := multiSourceFetchForCohort(userID, candidateLimit, preCohort)
	if len(candidates) == 0 {
		// Safety net: if every source failed, use the legacy single path.
		candidates = fetchCandidates(userID, candidateLimit)
		candidateSourceMap = nil
	}

	// Load the user's two-tower embedding ONCE per request. If cold, cosine
	// term is skipped (returns 0 below) so new users don't get a noisy signal.
	userVec := getUserEmbedding(userID)
	userCold := userEmbeddingIsCold(userVec)

	// Step 5.9: Anti-loop diagnosis — runs BEFORE scoring so a loop detected at
	// resistance 0-1 (category monoculture, creator flood, dopamine collapse,
	// skip streak) breaks on THIS page. It sets the loop-breaking strategy AND
	// escalates ResistanceLevel to >=2 so getFeedPattern actually honors the
	// override (its strategy switch is gated on RL>=2) and so any resistance-
	// sensitive scoring sees it too. Previously this ran after scoring+MMR and
	// the override was ignored until resistance climbed on its own — so the loop
	// only got fixed on the NEXT page, defeating the 30-sec retention goal.
	if diag := detectLoop(session); diag.Stuck && diag.SuggestedStrat != "" {
		session.CurrentStrategy = diag.SuggestedStrat
		session.TriedStrategies = append(session.TriedStrategies, diag.SuggestedStrat)
		if session.ResistanceLevel < 2 {
			session.ResistanceLevel = 2
		}
		if metricSignalCapture != nil {
			metricSignalCapture.WithLabelValues("loop_" + diag.Reason).Inc()
		}
	}

	// Step 6: Score each candidate
	scored := make([]ScoredItem, 0, len(candidates))
	for _, item := range candidates {
		contentID := getItemID(item)
		contentType := item.Type
		cs := getContentScore(contentID, contentType)

		score, breakdown := scoreForUser(cs, profile, session, followingSet, fofSet, interactedIDs)

		// Two-tower cosine bonus: ± up to 0.20 for strong matches. Gated on
		// having a warm user vector; cold users keep base scoring as-is.
		// Uses the *trained* two-tower vector when it has enough updates;
		// otherwise falls back to the Redis-cached hash-trick prior. The
		// trained vector encodes who actually engages with each item, so
		// this is closer to "people like you also liked" than pure
		// "categories you like."
		if !userCold {
			emotions := getContentEmotions(contentID, contentType)
			cv := getTrainedContentEmbedding(cs, emotions)
			sim := cosineSim(userVec, cv)
			// Attenuate by the SAME negative multiplier scoreForUser applied to the
			// rest of the score. Added unconditionally, this bonus would re-inflate
			// a blocked creator's item (negMult=0 → score forced to 0) back into
			// ranking, defeating the "blocked = 0" invariant for warm users; and an
			// unfollowed creator (negMult=0.5) would keep full embed weight. Default
			// 1.0 if the key is somehow absent.
			negMult := 1.0
			if nm, ok := breakdown["negativeMult"]; ok {
				negMult = nm
			}
			embedBonus := sim * 0.20 * negMult
			breakdown["embedSim"] = sim
			breakdown["embedBonus"] = embedBonus
			score += embedBonus
		}

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

	// REFRESH JITTER + ANTI-REPEAT — TikTok/IG-style "the head should look
	// different on pull-to-refresh." Two effects when refresh=true on page 1:
	//
	//   * Uniform ±0.10 perturbation on every score so near-ties (most of
	//     the head, given how tightly bunched scores get) reorder visibly
	//     while a clearly-better item still beats a clearly-worse one.
	//
	//   * Demotion of the items that landed at the head of the previous
	//     refresh: top1 -0.15, top2 -0.10, top3 -0.05. Big enough to
	//     guarantee a different #1 most of the time, small enough that an
	//     item that's dramatically better than every other candidate can
	//     still keep its top spot if it actually deserves it.
	if refresh && page == 1 {
		prevTops := loadPrevRefreshTops(userID)
		for i := range scored {
			scored[i].Score += (rand.Float64() - 0.5) * 0.20
			id := getItemID(scored[i].Item)
			if id == "" {
				continue
			}
			key := scored[i].Item.Type + ":" + id
			if rank, ok := prevTops[key]; ok {
				// Heavier demotion than the score-jitter range so a
				// merely-above-average item that won last time loses to
				// the next tier on the next refresh. With a small content
				// corpus (where score gaps are tight) anything below
				// -0.20 lets the same item keep winning by luck.
				switch rank {
				case 1:
					scored[i].Score -= 0.30
				case 2:
					scored[i].Score -= 0.20
				case 3:
					scored[i].Score -= 0.10
				}
			}
		}
	}

	// Sort by score for initial ranking. SliceStable so equal-scored items
	// (e.g. penalized tail items the clamp floors to 0) keep a deterministic
	// candidate order instead of being shuffled arbitrarily by the sort.
	sort.SliceStable(scored, func(i, j int) bool {
		return scored[i].Score > scored[j].Score
	})

	// Step 6.5: Drop items the user has already been shown in the last TTL
	// window (impression dedup — stronger than interactedIDs, which only
	// covered active engagement).
	scored = filterUnseenScored(userID, scored)

	// Step 6.6: Diversity re-rank (MMR) on the top-K so near-duplicates
	// don't stack next to each other in the feed.
	scored = applyMMRDefault(scored)

	// (Anti-loop diagnosis moved to Step 5.9, before scoring — see above.)

	// Cohort already computed above (preCohort) for the per-cohort source
	// blending step. Reuse to avoid recomputing.
	cohort := preCohort

	// Step 6.8: Cold-start bootstrap mix — for users with very few events,
	// sprinkle high-Wilson-score "known bangers" into the head of the feed
	// so first impressions land before personalized signals can warm up.
	// No-op for non-cold users; safe to call unconditionally.
	scored = applyBootstrapMixIfCold(userID, scored, getUserEventCount(userID))

	// Step 6.9: Cross-page session diversity penalty — if this user has
	// already seen this category multiple times this session (page 2+),
	// downweight repeats so successive pages stay varied even when MMR
	// said "fine within this page". Penalty is superlinear so a category
	// that's already appeared 3 times gets hit hard, while a first repeat
	// is barely affected.
	if session != nil && session.SessionID != "" {
		sessionCats := loadSessionCategoryCounts(r.Context(), session.SessionID)
		if len(sessionCats) > 0 {
			for i := range scored {
				cs := getContentScore(getItemID(scored[i].Item), scored[i].Item.Type)
				if cs != nil && cs.Category != "" {
					if n := sessionCats[strings.ToLower(cs.Category)]; n > 0 {
						scored[i].Score -= diversityPenaltyForCount(n + 1)
					}
				}
			}
			// Re-sort after penalty application so the scored slice is
			// still in decreasing-score order before composition.
			sort.SliceStable(scored, func(i, j int) bool { return scored[i].Score > scored[j].Score })
		}
	}

	// Step 7: Compose feed with slot pattern
	pattern := getFeedPattern(profile, session, limit)
	composed := composeFeed(scored, pattern, followingSet)

	// Step 7.1: Surprise injection — at most 1 wildcard from a category the user
	// has zero affinity for. Filter-bubble defense, gated by a small probability
	// and skipped for at-risk users. Done AFTER composeFeed (on the final ordered
	// list, like injectSuggestedAccountsCard): when it ran on the pre-composition
	// `scored`, composeFeed re-bucketed from scratch and the wildcard — lacking a
	// slotSurprise breakdown — fell into the hook bucket and got outscored, so the
	// defense was a no-op on For You. Injecting here preserves its position, and
	// it's still recorded as shown by the block below.
	composed = applySurpriseInjection(composed, profile, cohort, nil)

	// Step 7.2: Audition — guarantee a fresh under-audition upload one impression
	// per page even if merit-ranking buried it, so new content reliably gathers
	// the views it needs to prove itself (item-level exploration). Bounded to one
	// per page; items graduate out automatically once they pass auditionViewTarget.
	composed = injectAuditionContent(scored, composed)

	// Step 7.5: Remember the tail of what we just served so the NEXT page's
	// ranker can apply sequence-awareness penalties against it, AND stash the
	// score breakdown of each served item so LTR can learn from the outcome.
	if len(composed) > 0 {
		// Seen-filter: record impressions so subsequent pages don't repeat.
		items := make([]HomeFeedItem, 0, len(composed))
		for _, it := range composed {
			items = append(items, it.Item)
		}
		// Synchronous now (the ZADD is one cheap round-trip; trim/expire are
		// deferred inside markShownBatch) so a page-2 prefetch can't race the
		// seen-set write and re-serve page-1 content.
		markShownBatch(userID, items)
		// Refresh anti-repeat memory: remember the head of THIS refresh so
		// the next refresh can demote them and surface different content
		// at the top. Only saved on actual refresh requests.
		if refresh && page == 1 {
			go savePrevRefreshTops(userID, items)
		}
		// LTR stash with 1-based position so IPW can down-weight top-slot bias.
		// Also stash the creator ID + source for per-creator residual
		// calibration AND per-cohort source-blending reward at terminal-event
		// time, both without an extra DB lookup.
		for idx, it := range composed {
			cid := getItemID(it.Item)
			if it.ScoreBreakdown != nil {
				cs := getContentScore(cid, it.Item.Type)
				creatorID := ""
				if cs != nil {
					creatorID = cs.CreatorID
				}
				source := ""
				if candidateSourceMap != nil {
					source = candidateSourceMap[it.Item.Type+":"+cid]
				}
				go ltrStashBreakdownAll(userID, it.Item.Type, cid, cohort, it.ScoreBreakdown, idx+1, creatorID, source)
			}
		}
		// Cross-page session diversity: tally every served category against
		// this session's hash so the next page can see the distribution and
		// penalize repeats.
		if session != nil && session.SessionID != "" {
			servedCats := make([]string, 0, len(composed))
			for _, it := range composed {
				cscore := getContentScore(getItemID(it.Item), it.Item.Type)
				if cscore != nil && cscore.Category != "" {
					servedCats = append(servedCats, cscore.Category)
				}
			}
			go noteSessionCategories(session.SessionID, servedCats)
		}
		tail := composed
		if len(tail) > 6 {
			tail = tail[len(tail)-6:]
		}
		for _, it := range tail {
			cid := getItemID(it.Item)
			cscore := getContentScore(cid, it.Item.Type)
			if cscore.Category != "" {
				session.LastCategories = append(session.LastCategories, cscore.Category)
			}
			if cscore.CreatorID != "" {
				session.LastCreators = append(session.LastCreators, cscore.CreatorID)
			}
		}
		if n := len(session.LastCategories); n > 6 {
			session.LastCategories = session.LastCategories[n-6:]
		}
		if n := len(session.LastCreators); n > 6 {
			session.LastCreators = session.LastCreators[n-6:]
		}
		saveSessionState(session)
	}

	// Pagination
	hasMore := len(composed) >= limit

	// Attach top-response data so the client can render the opponent's
	// video on a left-swipe for any challenge with responseCount > 0.
	// One DB round-trip per page; safely no-ops on plain shorts.
	populateTopResponsesScored(composed)
	// Same boundary-call pattern for comment counts so the right-rail
	// number on every challenge tile matches the comment sheet's truth.
	populateChallengeCommentCountsScored(composed)
	// And the HLS manifest URL — once the transcode worker has produced
	// the segmented ladder, the client switches to HLS playback and
	// gets sub-500ms time-to-first-frame + adaptive bitrate. Falls back
	// to MP4 cleanly when the column is empty (worker not deployed, or
	// challenge uploaded before the worker existed).
	populateHLSManifestURLsScored(composed)

	// Interleave a "Suggested accounts" card into the feed. TikTok-style:
	// one card injected at index 4 of page 1, and again every 8 items so
	// long sessions see a fresh card per page without spam. Building the
	// card costs ~3 DB round trips; we skip when the composed slice is too
	// small to need an injection at all (cold-start safety net).
	if len(composed) >= 5 {
		composed = injectSuggestedAccountsCard(userID, page, composed)
	}

	// Strip debug info if not requested. Note: the entry includes every
	// possible inner pointer, but Go's JSON encoder skips nil pointers when
	// the struct field is omitempty — except map values are never omitted.
	// To match HomeFeedItem's omitempty semantics we conditionally include
	// the keys that are populated and leave the rest out.
	responseItems := make([]interface{}, 0, len(composed))
	for _, item := range composed {
		if debug {
			responseItems = append(responseItems, item)
			continue
		}
		entry := map[string]interface{}{
			"type":     item.Item.Type,
			"slotType": item.SlotType,
		}
		if item.Item.Challenge != nil {
			entry["challenge"] = item.Item.Challenge
		}
		if item.Item.Post != nil {
			entry["post"] = item.Item.Post
		}
		if item.Item.SuggestedAccounts != nil {
			entry["suggestedAccounts"] = item.Item.SuggestedAccounts
		}
		responseItems = append(responseItems, entry)
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

	userID := authUserID(r)
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

	// (Posts-from-followed branch retired — the Following feed is now
	// challenge-only just like the For You feed. If a followed creator only
	// posted plain content historically, those rows simply don't show up.)

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

	// Attach opponent video data for any challenge with responseCount > 0
	// — same boundary call as SmartFeedHandler so the swipe-left UX works
	// identically across For You / Following / Explore.
	populateTopResponses(items)
	populateChallengeCommentCounts(items)
	populateHLSManifestURLs(items)

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

	userID := authUserID(r)
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

// populateTopResponses batch-fills the TopResponse* fields on every Challenge
// in items that has at least one response. One DB round-trip per page, instead
// of touching every candidate-source SQL query. Called at the feed-handler
// boundary (SmartFeed / Following / Explore) right before JSON encoding.
//
// Picks the *newest* response per challenge as the "opponent" the client
// shows when the user swipes left. Picking by "most-voted" is a future
// upgrade — for now newest is the simplest signal that maps to the
// "battle just heated up" mental model users expect.
func populateTopResponses(items []HomeFeedItem) {
	if db == nil || len(items) == 0 {
		return
	}
	// Collect every challenge ID — we used to skip when ResponseCount<=0,
	// but candidate sources outside the recency lane don't populate that
	// field, so genuine battles were silently treated as shorts. Letting
	// the JOIN drive truth means a candidate from any lane (trending /
	// follow / collab / embedding / searchAffinity) gets enriched if it
	// actually has a response, and we self-correct ResponseCount from the
	// row count below.
	wantIDs := make([]int, 0, len(items))
	idToIdx := make(map[int][]int) // challenge ID → indices in items (handles duplicates)
	for i, it := range items {
		if it.Type != "challenge" || it.Challenge == nil {
			continue
		}
		cid, err := strconv.Atoi(it.Challenge.ID)
		if err != nil {
			continue
		}
		wantIDs = append(wantIDs, cid)
		idToIdx[cid] = append(idToIdx[cid], i)
	}
	if len(wantIDs) == 0 {
		return
	}

	// Build $1,$2,... placeholder list for the IN clause. Avoids pulling
	// pq.Array as a dependency; the list size is bounded by page size (~30).
	placeholders := make([]string, len(wantIDs))
	args := make([]interface{}, len(wantIDs))
	for i, id := range wantIDs {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
		args[i] = id
	}
	// Pull both the newest opponent (via DISTINCT ON) AND the total response
	// count in one go. The total count makes ResponseCount self-healing for
	// any item whose source SQL didn't populate it — battles from every lane
	// land at the client tagged correctly.
	// COALESCE(cr.video_variants, '{}'::jsonb) keeps Scan happy on rows
	// that pre-date the multi-bitrate column (any older challenge_response
	// row has NULL there). We unmarshal into a fresh VideoVariants per
	// row so empty maps stay empty rather than carrying state between
	// iterations of the loop.
	// Pulls cr.id alongside the rest so the client can vote on this opponent
	// directly from the home reels without a follow-up /challenges/{id}
	// fetch — the vote endpoint's contract is (challengeId, responseId,
	// voterId), and the response id is otherwise only available behind the
	// detail page.
	query := `
		SELECT DISTINCT ON (cr.challenge_id)
			cr.challenge_id, cr.id, cr.video_url, COALESCE(cr.thumbnail_url, ''),
			COALESCE(cr.video_variants, '{}'::jsonb),
			ru.username, ru.league,
			(SELECT COUNT(*) FROM challenge_responses WHERE challenge_id = cr.challenge_id)
		FROM challenge_responses cr
		JOIN users ru ON cr.responder_id = ru.id
		WHERE cr.challenge_id IN (` + strings.Join(placeholders, ",") + `)
		ORDER BY cr.challenge_id, cr.created_at DESC`
	rows, err := db.Query(query, args...)
	if err != nil {
		log.Printf("populateTopResponses query error: %v", err)
		return
	}
	defer rows.Close()
	for rows.Next() {
		var cid, rid, totalCount int
		var videoURL, thumbURL, username, league string
		var variantsRaw []byte
		if err := rows.Scan(&cid, &rid, &videoURL, &thumbURL, &variantsRaw, &username, &league, &totalCount); err != nil {
			continue
		}
		// Decode variants leniently — a malformed payload should NOT
		// drop the whole row, the client falls back to TopResponseVideoUrl
		// when the variants map is empty.
		var variants VideoVariants
		if len(variantsRaw) > 0 {
			if err := json.Unmarshal(variantsRaw, &variants); err != nil {
				log.Printf("populateTopResponses: variants decode failed for cid=%d: %v", cid, err)
				variants = nil
			}
		}
		ridStr := strconv.Itoa(rid)
		for _, idx := range idToIdx[cid] {
			ch := items[idx].Challenge
			if ch == nil {
				continue
			}
			ch.TopResponseID = ridStr
			ch.TopResponseVideoUrl = videoURL
			ch.TopResponseThumbnailUrl = thumbURL
			ch.TopResponseUsername = username
			ch.TopResponseLeague = league
			ch.TopResponseVideoVariants = variants
			// Self-heal ResponseCount: if the candidate source didn't fetch
			// it but the JOIN proves there's a response, surface the truth
			// so the client renders the battle UI instead of a short.
			if ch.ResponseCount < totalCount {
				ch.ResponseCount = totalCount
			}
		}
	}
}

// injectAuditionContent guarantees one under-audition item (recent, below the
// audition view target) an impression per page when merit-ranking didn't already
// surface it. This is item-level exploration: a new upload's engagement isn't a
// measurable signal until it has had enough impressions, so the ranker can't
// fairly judge it before then — without a guaranteed slot a fresh 0-view video
// can be buried under proven content forever and never get its audition. Picks
// the FRESHEST eligible item from the scored pool that isn't already on this
// page; no-op when every eligible item already made the page on merit (or none
// exist). Bounded to one injection per page (an ~8% exploration budget); items
// graduate automatically once their view count passes auditionViewTarget.
func injectAuditionContent(scored []ScoredItem, composed []ScoredItem) []ScoredItem {
	if len(composed) == 0 {
		return composed
	}
	inFeed := make(map[string]bool, len(composed))
	for _, it := range composed {
		inFeed[it.Item.Type+":"+getItemID(it.Item)] = true
	}
	best := -1
	bestFresh := -1.0
	for i := range scored {
		bd := scored[i].ScoreBreakdown
		if bd == nil || bd["auditionEligible"] <= 0 {
			continue
		}
		key := scored[i].Item.Type + ":" + getItemID(scored[i].Item)
		if inFeed[key] {
			continue // already surfaced on merit — no need to force it
		}
		if f := bd["freshness"]; f > bestFresh {
			bestFresh = f
			best = i
		}
	}
	if best < 0 {
		return composed
	}
	aud := scored[best]
	aud.SlotType = "audition"
	// Insert just after the head so it's actually seen, not at position 0 (which
	// would feel jarring and displace the strongest hook).
	pos := 3
	if pos > len(composed) {
		pos = len(composed)
	}
	out := make([]ScoredItem, 0, len(composed)+1)
	out = append(out, composed[:pos]...)
	out = append(out, aud)
	out = append(out, composed[pos:]...)
	return out
}

// injectSuggestedAccountsCard builds an "Accounts you might like" card for
// this user and splices it into the composed slice at index 4 (TikTok's
// canonical "after the user has had a chance to engage with content" slot).
// On page 2+ we inject at index 6 so users who scroll deep keep getting fresh
// suggestions without seeing the card in the same place every page. Returns
// the composed slice unchanged if there are no suggestions to surface.
func injectSuggestedAccountsCard(userID string, page int, composed []ScoredItem) []ScoredItem {
	card := BuildSuggestedAccountsCard(userID, page)
	if card == nil || len(card.Users) == 0 {
		return composed
	}
	// Wrap the card in a ScoredItem so it travels through the same
	// serialization path. Score field is unused for non-content items.
	wrapped := ScoredItem{
		Item: HomeFeedItem{
			Type:              "suggestedAccounts",
			SuggestedAccounts: card,
		},
	}
	insertAt := 4
	if page > 1 {
		insertAt = 6
	}
	if insertAt > len(composed) {
		insertAt = len(composed)
	}
	out := make([]ScoredItem, 0, len(composed)+1)
	out = append(out, composed[:insertAt]...)
	out = append(out, wrapped)
	out = append(out, composed[insertAt:]...)
	return out
}

// populateTopResponsesScored is the ScoredItem flavor used by handlers that
// pass scored slices around (explore feed). Same DB hit, just unwraps the
// underlying HomeFeedItem.
func populateTopResponsesScored(items []ScoredItem) {
	if len(items) == 0 {
		return
	}
	plain := make([]HomeFeedItem, len(items))
	for i, si := range items {
		plain[i] = si.Item
	}
	populateTopResponses(plain)
	// plain[i].Challenge points at the same struct as items[i].Item.Challenge,
	// so the in-place mutation above is already visible to the caller — no
	// copy-back needed. Listed explicitly to make that intent obvious.
}

// populateChallengeCommentCounts batch-fills CommentCount on every Challenge
// in items. Same one-round-trip pattern as populateTopResponses — challenges
// don't carry a denormalized counter on their own row, so without this the
// reels right-rail comment number would always read "0" and mislead users
// into thinking nobody has commented.
//
// Called immediately after populateTopResponses at the feed-handler
// boundary so both enrichments land before JSON encoding.
func populateChallengeCommentCounts(items []HomeFeedItem) {
	if db == nil || len(items) == 0 {
		return
	}
	wantIDs := make([]int, 0, len(items))
	idToIdx := make(map[int][]int)
	for i, it := range items {
		if it.Type != "challenge" || it.Challenge == nil {
			continue
		}
		cid, err := strconv.Atoi(it.Challenge.ID)
		if err != nil {
			continue
		}
		wantIDs = append(wantIDs, cid)
		idToIdx[cid] = append(idToIdx[cid], i)
	}
	if len(wantIDs) == 0 {
		return
	}
	placeholders := make([]string, len(wantIDs))
	args := make([]interface{}, len(wantIDs))
	for i, id := range wantIDs {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
		args[i] = id
	}
	rows, err := db.Query(`
		SELECT challenge_id, COUNT(*)
		FROM challenge_comments
		WHERE challenge_id IN (`+strings.Join(placeholders, ",")+`)
		GROUP BY challenge_id`, args...)
	if err != nil {
		log.Printf("populateChallengeCommentCounts query error: %v", err)
		return
	}
	defer rows.Close()
	for rows.Next() {
		var cid, n int
		if err := rows.Scan(&cid, &n); err != nil {
			continue
		}
		for _, idx := range idToIdx[cid] {
			ch := items[idx].Challenge
			if ch == nil {
				continue
			}
			ch.CommentCount = n
		}
	}
}

// populateChallengeCommentCountsScored mirrors populateTopResponsesScored —
// it takes a ScoredItem slice (explore / smart) and runs the same
// enrichment via the in-place HomeFeedItem trick.
func populateChallengeCommentCountsScored(items []ScoredItem) {
	if len(items) == 0 {
		return
	}
	plain := make([]HomeFeedItem, len(items))
	for i, si := range items {
		plain[i] = si.Item
	}
	populateChallengeCommentCounts(plain)
}

// populateHLSManifestURLs batch-fills Challenge.HLSManifestURL on every
// challenge in `items` whose row in the DB has a non-empty
// hls_manifest_url column (i.e. the transcode worker has finished).
//
// Why it's a separate populate step (vs. just SELECTing the column in
// every candidate-source query): there are 8+ candidate-source queries
// across candidate_sources.go, explore_feed.go, and the smart feed
// pipeline that build raw Challenge structs. Hand-editing all of them
// would be a ton of churn and a bug magnet (one missed query = silent
// HLS-not-used in that surface). One enrichment hop at the
// feed-handler boundary is the same pattern populateTopResponses and
// populateChallengeCommentCounts already use — and means new candidate
// sources added later automatically get HLS for free.
//
// Safe-by-construction: if hls_manifest_url is empty in the DB (worker
// hasn't finished, or this is a legacy challenge), we leave the
// struct's field as the zero value (""). Client sees omitempty drop the
// key and falls back to videoUrl / videoVariants.
func populateHLSManifestURLs(items []HomeFeedItem) {
	if db == nil || len(items) == 0 {
		return
	}
	wantIDs := make([]int, 0, len(items))
	idToIdx := make(map[int][]int)
	for i, it := range items {
		if it.Type != "challenge" || it.Challenge == nil {
			continue
		}
		cid, err := strconv.Atoi(it.Challenge.ID)
		if err != nil {
			continue
		}
		wantIDs = append(wantIDs, cid)
		idToIdx[cid] = append(idToIdx[cid], i)
	}
	if len(wantIDs) == 0 {
		return
	}
	placeholders := make([]string, len(wantIDs))
	args := make([]interface{}, len(wantIDs))
	for i, id := range wantIDs {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
		args[i] = id
	}
	rows, err := db.Query(`
		SELECT id, COALESCE(hls_manifest_url, '')
		FROM challenges
		WHERE id IN (`+strings.Join(placeholders, ",")+`)
		  AND hls_manifest_url <> ''`, args...)
	if err != nil {
		log.Printf("populateHLSManifestURLs query error: %v", err)
		return
	}
	defer rows.Close()
	for rows.Next() {
		var cid int
		var url string
		if err := rows.Scan(&cid, &url); err != nil {
			continue
		}
		for _, idx := range idToIdx[cid] {
			ch := items[idx].Challenge
			if ch == nil {
				continue
			}
			ch.HLSManifestURL = url
		}
	}
}

// populateHLSManifestURLsScored is the ScoredItem-slice flavor used by
// the smart/explore pipelines. Same in-place HomeFeedItem trick as
// populateChallengeCommentCountsScored.
func populateHLSManifestURLsScored(items []ScoredItem) {
	if len(items) == 0 {
		return
	}
	plain := make([]HomeFeedItem, len(items))
	for i, si := range items {
		plain[i] = si.Item
	}
	populateHLSManifestURLs(plain)
}

// populateTopResponsesChallenges is the []Challenge flavor used by handlers
// that return raw challenge slices (e.g. /search). Wraps each element in a
// throwaway HomeFeedItem whose Challenge pointer aims at the slice element,
// so the mutations populateTopResponses performs land directly in the
// caller's slice without a copy-back step.
//
// This exists so /search results render the battle indicator on the seed
// video when the user taps a challenge thumbnail and lands inside the
// fullscreen reels viewer — the viewer reads opponent fields off the
// ChallengeModel passed in as the seed, and without this enrichment the
// seed always looked like a plain short.
func populateTopResponsesChallenges(challenges []Challenge) {
	if len(challenges) == 0 {
		return
	}
	plain := make([]HomeFeedItem, len(challenges))
	for i := range challenges {
		plain[i] = HomeFeedItem{
			Type:      "challenge",
			Challenge: &challenges[i],
		}
	}
	populateTopResponses(plain)
	populateChallengeCommentCounts(plain)
	populateHLSManifestURLs(plain)
	// &challenges[i] is a stable address into the slice's backing array, so
	// populateTopResponses' writes through plain[i].Challenge land in the
	// caller's slice. Same pattern as populateTopResponsesScored above.
}

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

// fetchCandidates is the recency-source backing query. Uses a widening
// fallback ladder so a sparse-data window (stale seed, quiet weekend, new
// region) never produces an empty feed.
func fetchCandidates(userID string, limit int) []HomeFeedItem {
	for _, window := range candidateSourceWindows["recency"] {
		items := fetchCandidatesWindowed(userID, limit, window)
		if len(items) > 0 {
			return items
		}
	}
	return nil
}

func fetchCandidatesWindowed(userID string, limit int, window string) []HomeFeedItem {
	// Post entity retired — the home reels feed is challenge-only. Within the
	// challenge pool we split candidates into two flavors:
	//   * battles — challenges that have at least one response, i.e. someone
	//     accepted the duel. These are the "main event" content.
	//   * shorts  — challenges nobody has responded to yet. We surface these
	//     so creators get traction (and so the feed has volume while the
	//     content library is still small).
	// Mix is biased ~70% battles / 30% shorts: battles are richer engagement
	// surface, but shorts keep the catalog large enough to fill a session
	// without repeating creators.
	battleLimit := (limit * 7) / 10
	shortLimit := limit - battleLimit
	if battleLimit < 1 {
		battleLimit = 1
	}
	if shortLimit < 1 {
		shortLimit = 1
	}

	items := fetchChallengesWindowedByKind(userID, "battle", battleLimit, window)
	items = append(items, fetchChallengesWindowedByKind(userID, "short", shortLimit, window)...)
	return items
}

// fetchChallengesWindowedByKind pulls recent challenges of a given kind
// ("battle" = has responses, "short" = zero responses) for the recency
// candidate source. Same projection as the old combined query, just split so
// we can independently cap the two pools.
func fetchChallengesWindowedByKind(userID, kind string, limit int, window string) []HomeFeedItem {
	if limit <= 0 {
		return nil
	}
	responseFilter := "(SELECT COUNT(*) FROM challenge_responses WHERE challenge_id = c.id) > 0"
	if kind == "short" {
		responseFilter = "(SELECT COUNT(*) FROM challenge_responses WHERE challenge_id = c.id) = 0"
	}
	rows, err := db.Query(`
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
		AND c.created_at > NOW() - ($3::text)::interval
		AND c.creator_id != CAST($1 AS INT)
		AND `+responseFilter+`
		ORDER BY c.created_at DESC
		LIMIT $2`, userID, limit, window)
	if err != nil {
		return nil
	}
	defer rows.Close()
	var items []HomeFeedItem
	for rows.Next() {
		var ch Challenge
		var creatorID, views, likes, rc int
		var createdAt, expiresAt time.Time
		rows.Scan(&ch.ID, &creatorID, &ch.CreatorUsername, &ch.CreatorLeague,
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
// Results are cached in Redis (6h TTL) — this function is called once per
// candidate during feed ranking so the DB round-trip cost adds up fast.
func getContentEmotions(contentID, contentType string) []string {
	if contentID == "" {
		return nil
	}

	cacheKey := contentEmotionRedisKey + contentType + ":" + contentID
	if rdb != nil {
		if s, err := rdb.Get(rctx, cacheKey).Result(); err == nil && s != "" {
			var cached []string
			if json.Unmarshal([]byte(s), &cached) == nil {
				return cached
			}
		}
	}

	var emotionJSON []byte
	if contentType == "challenge" {
		db.QueryRow(`SELECT COALESCE(emotion_tags, '[]'::JSONB) FROM challenges WHERE id = $1`, contentID).Scan(&emotionJSON)
	} else {
		db.QueryRow(`SELECT COALESCE(emotion_tags, '[]'::JSONB) FROM posts WHERE id = $1`, contentID).Scan(&emotionJSON)
	}
	var emotions []string
	json.Unmarshal(emotionJSON, &emotions)

	if rdb != nil {
		if js, err := json.Marshal(emotions); err == nil {
			_ = rdb.Set(rctx, cacheKey, js, contentEmotionTTL).Err()
		}
	}
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
