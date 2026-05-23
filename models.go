package main

import "time"

// User represents a user's profile data, with the Posts field corrected.
type User struct {
	ID            string   `json:"id"` // Example for MongoDB
	Username      string   `json:"username"`
	password      string   `json:"-"` // The json: "-" tag prevents the password from being serialized into JSON.
	FullName      string   `json:"fullName"`
	Followers     int      `json:"followers"`
	FollowingList []string `json:"followingList"`
	Wins          int      `json:"wins"`
	Losses        int      `json:"losses"`
	League        string   `json:"league"`
	// Bio is shown on the profile page. Empty string means "user
	// hasn't set one" — render an "Add a bio" CTA on own profile,
	// nothing on others. omitempty keeps the wire format tight for
	// the (eventually rare) no-bio case.
	Bio string `json:"bio,omitempty"`
	// Account visibility — "public" (default) or "friends". When
	// "friends" the user's profile detail and challenge content
	// should only be returned to viewers in the user's follower
	// list. Filtering is enforced at the handler boundary so the
	// recommender doesn't see private content either.
	Visibility string `json:"visibility,omitempty"`
	// User-level settings — theme, language, etc. Free-form so we
	// can add toggles without a schema migration per feature.
	// Auth/security state (TOTP secret, recovery codes) lives in a
	// separate table for least-privilege access — don't put it here.
	Settings map[string]any `json:"settings,omitempty"`
	// True iff the user has finished TOTP 2FA enrollment. Surfaces
	// in /profile so the client can render the "2FA on" badge in
	// the settings sheet without a second round-trip.
	TwoFactorEnabled bool `json:"twoFactorEnabled,omitempty"`
}

// SearchResponse wraps search results with total count for pagination.
type SearchResponse struct {
	Results []User `json:"results"`
	Total   int    `json:"total"`
}

// Represents the incoming payload for a /follow request
type FollowEventPayload struct {
	FollowerID        string    `json:"followerId"`
	FollowerUsername  string    `json:"followerUsername"`
	FollowingID       string    `json:"followingId"`
	FollowingUsername string    `json:"followingUsername"`
	ClientTimestamp   time.Time `json:"clientTimestamp"` // Use time.Time for proper parsing
}

// Represents the incoming payload for an /unfollow request
type UnfollowEventPayload struct {
	UnfollowerID       string    `json:"unfollowerId"`
	UnfollowerUsername string    `json:"unfollowerUsername"`
	UnfollowedID       string    `json:"unfollowedId"`
	UnfollowedUsername string    `json:"unfollowedUsername"`
	ClientTimestamp    time.Time `json:"clientTimestamp"`
}

// Notification represents a message to be sent to a user.
type Notification struct {
	Type      string `json:"type"`
	Message   string `json:"message"`
	Timestamp string `json:"timestamp"`
	// VideoURL carries a content URL for invisible "prefetch hint"
	// notifications (Type == "next_reel_hint"). Omitted on all
	// human-visible notifications. The mobile WebSocket wrapper sees
	// this type, hands the URL to VideoPlayerService.prefetch(), and
	// suppresses surfacing it to the user.
	VideoURL string `json:"videoUrl,omitempty"`
}

// ChatMessage represents a direct message between two users.
type ChatMessage struct {
	ID              string `json:"id"`
	SenderID        string `json:"senderId"`
	SenderUsername  string `json:"senderUsername"`
	ReceiverID      string `json:"receiverId"`
	ReceiverUsername string `json:"receiverUsername"`
	Message         string `json:"message"`
	IsRead          bool   `json:"isRead"`
	Status          string `json:"status"`
	ReplyToID       string `json:"replyToId,omitempty"`
	ReplyToText     string `json:"replyToText,omitempty"`
	IsEdited        bool   `json:"isEdited"`
	IsDeleted       bool   `json:"isDeleted"`
	CreatedAt       string `json:"createdAt"`
}

// Conversation represents a chat thread between two users (for the list view).
type Conversation struct {
	UserID       string `json:"userId"`
	Username     string `json:"username"`
	League       string `json:"league"`
	LastMessage  string `json:"lastMessage"`
	LastTime     string `json:"lastTime"`
	UnreadCount  int    `json:"unreadCount"`
}

// Post represents a piece of content (video/image short) uploaded by a user.
type Post struct {
	ID             string   `json:"id"`
	AuthorID       string   `json:"authorId"`
	AuthorUsername string   `json:"authorUsername"`
	AuthorLeague   string   `json:"authorLeague"`
	Type           string   `json:"type"` //"video" or "image"
	ContentURL     string   `json:"contentUrl"`
	ThumbnailURL   string   `json:"thumbnailUrl,omitempty"`
	Caption        string   `json:"caption"`
	Likes          int      `json:"likes"`
	Views          int      `json:"views"`
	Comments       int      `json:"comments"`
	CreatedAt      string   `json:"createdAt"`
	IsLiked        bool     `json:"isLiked"`
	LikedBy        []string `json:"likedBy,omitempty"`
	// Content understanding fields
	Category       string   `json:"category"`              // Primary category
	EmotionTags    []string `json:"emotionTags,omitempty"`  // Emotion labels
	EnergyLevel    string   `json:"energyLevel"`            // "low","medium","high"
}

// FeedResponse wraps the paginated feed.
type FeedResponse struct {
	Posts   []Post `json:"posts"`
	Page    int    `json:"page"`
	HasMore bool   `json:"hasMore"`
}

// Comment represents a comment on a post.
type Comment struct {
	ID             string `json:"id"`
	PostID         string `json:"postId"`
	AuthorID       string `json:"authorId"`
	AuthorUsername string `json:"authorUsername"`
	Text           string `json:"text"`
	CreatedAt      string `json:"createdAt"`
}

// LikePayload is the request body for liking/unliking a post.
type LikePayload struct {
	PostID   string `json:"postId"`
	UserID   string `json:"userId"`
	Username string `json:"username"`
}

// CommentPayload is the request body for adding a comment.
type CommentPayload struct {
	PostID   string `json:"postId"`
	UserID   string `json:"userId"`
	Username string `json:"username"`
	Text     string `json:"text"`
}

// -------------------------------------------------------------------------------------
// challenge system
// -------------------------------------------------------------------------------------

// VideoVariants maps a quality label ("480p","720p","1080p") to its
// fully-qualified CDN URL. Stored as JSONB in Postgres and serialized as a
// plain JSON object on the wire so the client can pick the right quality
// for the user's current network speed without an extra round-trip.
//
// Empty / nil means "no variants encoded yet" — fall back to VideoURL,
// which is the canonical/default-quality URL kept for backward compat with
// every reader that predates the multi-bitrate feature.
type VideoVariants map[string]string

// challenge represents an open challenge created by a user.
type Challenge struct {
	ID              string         `json:"id"`
	CreatorID       string         `json:"creatorId"`
	CreatorUsername string         `json:"creatorUsername"`
	CreatorLeague   string         `json:"creatorLeague"`
	VideoURL        string         `json:"videoUrl"`
	VideoVariants   VideoVariants  `json:"videoVariants,omitempty"`
	// HLS master manifest URL (.m3u8). Set by the background transcode
	// worker once it has produced the segmented bitrate ladder for this
	// challenge. When non-empty, the client should prefer this over
	// VideoURL/VideoVariants — HLS gives sub-500ms time-to-first frame
	// and mid-stream adaptive bitrate. Empty means the worker hasn't
	// finished yet (or isn't deployed in this env) — clients fall back
	// to the per-bitrate MP4 path automatically. omitempty keeps the
	// payload tight for the (eventually rare) legacy case.
	HLSManifestURL  string         `json:"hlsManifestUrl,omitempty"`
	ThumbnailURL    string         `json:"thumbnailUrl,omitempty"`
	Prefix          string   `json:"prefix"`              // "Who is better", "Which is best", etc.
	Subject         string   `json:"subject"`             // "Dancer", "Painting", etc.
	Visibility      string   `json:"visibility"`          // "arena" or "friends"
	VisibleTo       []string `json:"visibleTo,omitempty"` // friends IDs (empty = all friends)
	Status          string   `json:"status"`              // "open", "active", "completed", "expired"
	Likes           int      `json:"likes"`
	Views           int      `json:"views"`
	// Live count of comments on this challenge. Computed from
	// challenge_comments at the feed-handler boundary so the reels right-rail
	// can render the same digit the comment sheet shows. Omitempty keeps the
	// payload tight for legacy callers that haven't started reading it.
	CommentCount    int      `json:"commentCount,omitempty"`
	CreatedAt       string   `json:"createdAt"`
	ExpiresAt       string   `json:"expiresAt"`
	ResponseCount   int      `json:"responseCount"`
	// Content understanding fields (creator-declared + system-inferred)
	Category        string   `json:"category"`             // Primary: "comedy","motivation","sports","dance","music",etc.
	EmotionTags     []string `json:"emotionTags,omitempty"` // ["happy","intense","inspiring"]
	EnergyLevel     string   `json:"energyLevel"`           // "low","medium","high"

	// Top response fields — populated by populateTopResponses() at the
	// feed-handler boundary for any challenge with responseCount > 0. Lets
	// the client render the opponent's video on a left-swipe without an
	// extra round-trip. All omitempty so plain shorts (no responses) don't
	// carry empty strings in the JSON payload.
	//
	// TopResponseID is needed by the client's vote button — the vote
	// endpoint takes (challengeId, responseId, voterId) and without the ID
	// the home reels can't cast a vote without first fetching the
	// challenge detail. Surfacing it inline keeps the vote tap one-shot.
	TopResponseID           string `json:"topResponseId,omitempty"`
	TopResponseVideoUrl     string `json:"topResponseVideoUrl,omitempty"`
	TopResponseThumbnailUrl string `json:"topResponseThumbnailUrl,omitempty"`
	TopResponseUsername     string `json:"topResponseUsername,omitempty"`
	TopResponseLeague       string `json:"topResponseLeague,omitempty"`
	// Adaptive-bitrate variants for the top response, mirroring the
	// primary VideoVariants map. Empty when the response was uploaded
	// before the multi-bitrate feature shipped — the client should
	// fall back to TopResponseVideoUrl in that case (which is already
	// the canonical/720p URL by convention).
	TopResponseVideoVariants VideoVariants `json:"topResponseVideoVariants,omitempty"`
}

// HomeFeedItem wraps a post, challenge, or suggested-accounts card for the
// mixed home feed. Type discriminates which inner pointer is populated.
//
// Type values:
//   - "challenge"         → Challenge populated
//   - "post"              → Post populated (legacy; the home reels feed no
//                            longer emits these but the type is kept so test
//                            fixtures and any external callers still parse)
//   - "suggestedAccounts" → SuggestedAccounts populated. Rendered by the
//                            client as a non-video card showing 3–5 user
//                            follow suggestions, interleaved into the feed
//                            every ~8 items in TikTok / Instagram style.
type HomeFeedItem struct {
	Type              string                 `json:"type"`
	Challenge         *Challenge             `json:"challenge,omitempty"`
	Post              *Post                  `json:"post,omitempty"`
	SuggestedAccounts *SuggestedAccountsCard `json:"suggestedAccounts,omitempty"`
}

// SuggestedAccount is a slim user shape carried inside a SuggestedAccountsCard.
// Slimmer than full User so the JSON payload stays small even when several
// cards are interleaved across a page.
type SuggestedAccount struct {
	ID        string `json:"id"`
	Username  string `json:"username"`
	FullName  string `json:"fullName,omitempty"`
	League    string `json:"league"`
	Followers int    `json:"followers"`
	Wins      int    `json:"wins"`
	Losses    int    `json:"losses"`
	// Why this user surfaced — one of "fof", "category", "popular", "league".
	// Lets the client surface a small reason badge per row without the
	// backend having to re-rank or look it up again.
	Reason string `json:"reason,omitempty"`
	// Number of accounts the recipient already follows who follow this user
	// (0 if not driven by FoF). Powers a "Followed by 3 friends" badge.
	FollowedByFriends int `json:"followedByFriends,omitempty"`
}

// SuggestedAccountsCard is one "Accounts to follow" card injected into the
// home reels feed. The client renders it as a special tile (no video) with
// a list of follow suggestions and an inline Follow button per row.
type SuggestedAccountsCard struct {
	// Stable card ID so the client can dedupe across pages and so analytics
	// can attribute card-level events. Built as "sa_<userID>_<page>".
	ID      string             `json:"id"`
	Title   string             `json:"title"`  // e.g. "Accounts you might like"
	Reason  string             `json:"reason"` // e.g. "Based on who you follow"
	Users   []SuggestedAccount `json:"users"`
}

// ChallengeResponse represents someone accepting and responding to a challenge.
type ChallengeResponse struct {
	ID                string         `json:"id"`
	ChallengeID       string         `json:"challengeId"`
	ResponderID       string         `json:"responderld"`
	ResponderUsername string         `json:"responderUsername"`
	ResponderLeague   string         `json:"responderLeague"`
	VideoURL          string         `json:"videoUrl"`
	VideoVariants     VideoVariants  `json:"videoVariants,omitempty"`
	ThumbnailURL      string         `json:"thumbnailUrl,omitempty"`
	Likes             int     `json:"likes"`
	Views             int     `json:"views"`
	CreatedAt         string  `json:"createdAt"`
	// Validation + community moderation fields
	DurationMs        int     `json:"durationMs,omitempty"`
	Caption           string  `json:"caption,omitempty"`
	RelevanceScore    float64 `json:"relevanceScore,omitempty"`
	OffTopicFlags     int     `json:"offTopicFlags,omitempty"`
	IsHidden          bool    `json:"isHidden,omitempty"`
}

// CreateChallengePayload is the request body for creating a challenge.
type CreateChallengePayload struct {
	CreatorID     string        `json:"creatorId"`
	VideoURL      string        `json:"videoUrl"`
	VideoVariants VideoVariants `json:"videoVariants,omitempty"` // optional multi-bitrate variants from device-side transcode
	ThumbnailURL  string        `json:"thumbnailUrl"`
	Prefix        string        `json:"prefix"`
	Subject       string        `json:"subject"`
	Visibility    string        `json:"visibility"`   // "arena" or "friends"
	VisibleTo     []string      `json:"visibleTo"`    // friend IDs (empty = all)
	Category      string        `json:"category"`     // "comedy","motivation","sports","dance",etc.
	EmotionTags   []string      `json:"emotionTags"`  // ["happy","intense","inspiring"]
	EnergyLevel   string        `json:"energyLevel"`  // "low","medium","high"
}

// ContentCategory defines the available categories for content.
// Categories help the recommendation engine understand what kind of content
// a video is, so it can match it to users who enjoy that type.
var ContentCategories = []string{
	"comedy",       // Funny, humor, roasts, pranks, memes
	"motivation",   // Inspirational, discipline, success, hustle
	"sports",       // Athletic skills, sports challenges, fitness
	"dance",        // Choreography, dance battles, freestyle
	"music",        // Singing, instruments, beatbox, rap
	"gaming",       // Gameplay, esports, game challenges
	"art",          // Drawing, painting, creative crafts
	"education",    // Tutorials, how-to, skill teaching
	"story",        // Vlogs, storytime, personal experiences
	"fashion",      // Style, beauty, outfit challenges
	"food",         // Cooking, food challenges, recipes
	"horror",       // Scary, thriller, creepy content
	"emotional",    // Sad, heartfelt, deep emotional content
	"lifestyle",    // Day in life, routines, wellness
	"tech",         // Technology, coding, gadgets
	"prank",        // Pranks, social experiments
	"news",         // Commentary, opinions, current events
	"other",        // Anything that doesn't fit above
}

// EmotionLabels are the available emotion tags for content.
// Multiple can be selected. These help the algorithm match content
// to the user's current mood/state.
var EmotionLabels = []string{
	"happy",       // Fun, joyful, cheerful
	"sad",         // Emotional, tearful, melancholic
	"intense",     // Hype, adrenaline, competitive
	"chill",       // Relaxing, calm, easy-going
	"inspiring",   // Motivating, uplifting, empowering
	"scary",       // Thrilling, frightening, suspenseful
	"funny",       // Comedic, humorous, witty
	"serious",     // Deep, thoughtful, meaningful
	"aggressive",  // Bold, fierce, confrontational
	"romantic",    // Love, affection, relationship content
	"nostalgic",   // Throwback, memories, retro
	"satisfying",  // ASMR, oddly satisfying, visual pleasure
	"cringe",      // Awkward, uncomfortable, embarrassing
	"wholesome",   // Heartwarming, kind, pure
	"suspenseful", // Edge of seat, cliffhanger, mystery
	"empowering",  // Confidence boost, self-love, growth
}

// MoodTags represent the user's mood context — used to match
// content emotion to what the user NEEDS right now.
var MoodTags = []string{
	"bored",       // Needs stimulation — serve high-energy, funny, surprising
	"stressed",    // Needs relief — serve chill, funny, satisfying
	"confident",   // Riding high — serve challenges, intense, competitive
	"lonely",      // Needs connection — serve social, wholesome, romantic
	"motivated",   // In the zone — serve inspiring, intense, empowering
	"relaxed",     // Taking it easy — serve chill, funny, satisfying
}

// EnergyLevels define how stimulating the content is.
// Matched against user's current dopamine budget / energy state.
var EnergyLevels = []string{"low", "medium", "high"}

// CaptionKeywordTags auto-extracts tags from caption/subject text.
// Maps keywords to the tags they imply.
var CaptionKeywordTags = map[string][]string{
	// Emotion keywords
	"lol": {"funny"}, "lmao": {"funny"}, "haha": {"funny"}, "dead": {"funny"},
	"crying": {"sad", "funny"}, "insane": {"intense"}, "crazy": {"intense"},
	"wild": {"intense"}, "fire": {"intense"}, "lit": {"intense"},
	"wholesome": {"wholesome"}, "cute": {"wholesome", "happy"},
	"scary": {"scary"}, "creepy": {"scary"}, "spooky": {"scary"},
	"satisfying": {"satisfying"}, "asmr": {"satisfying", "chill"},
	"relax": {"chill"}, "calm": {"chill"}, "peace": {"chill"},
	"cringe": {"cringe"}, "awkward": {"cringe"},
	"nostalgic": {"nostalgic"}, "throwback": {"nostalgic"}, "memories": {"nostalgic"},
	"love": {"romantic", "wholesome"}, "crush": {"romantic"},
	"grind": {"inspiring", "empowering"}, "hustle": {"inspiring", "motivated"},
	"believe": {"inspiring"}, "never give up": {"inspiring", "empowering"},
	"confidence": {"empowering"}, "glow up": {"empowering"},
	"suspense": {"suspenseful"}, "wait for it": {"suspenseful"},
	"plot twist": {"suspenseful"}, "unexpected": {"suspenseful"},
	// Category keywords that also imply emotion
	"battle": {"intense", "aggressive"}, "vs": {"intense"},
	"challenge": {"intense"}, "dare": {"intense", "scary"},
	"prank": {"funny"}, "roast": {"funny", "aggressive"},
	"workout": {"intense", "empowering"}, "gym": {"intense"},
}

// AcceptChallengePayload is sent when a user accepts a challenge.
type AcceptChallengePayload struct {
	ChallengeID   string        `json:"challengeId"`
	ResponderID   string        `json:"responderId"`
	VideoURL      string        `json:"videoUrl"`
	VideoVariants VideoVariants `json:"videoVariants,omitempty"` // optional multi-bitrate variants from device-side transcode
	ThumbnailURL  string        `json:"thumbnailUrl"`
	// Tier-1 validation fields — required so the server can enforce length limits
	// and store metadata for downstream relevance scoring.
	DurationMs   int    `json:"durationMs"`
	Caption      string `json:"caption,omitempty"`
}

// FlagResponsePayload is the body for community-moderation off-topic flagging.
type FlagResponsePayload struct {
	UserID string `json:"userId"`
	Reason string `json:"reason,omitempty"` // defaults to "off_topic" if empty
}

// ChallengeVotePayload is the request body for voting on a challenge response.
type ChallengeVotePayload struct {
	ChallengeID string `json:"challengeId"`
	ResponseID  string `json:"responseId"` // the response being voted for
	VoterID     string `json:"voterId"`
}

// ChallengeVote represents a user's vote on a challenge matchup.
type ChallengeVote struct {
	ID          string `json:"id"`
	ChallengeID string `json:"challengeId"`
	ResponseID  string `json:"responseId"`
	VoterID     string `json:"voterId"`
	CreatedAt   string `json:"createdAt"`
}

// VoteSummary holds vote counts for the challenge detail view.
type VoteSummary struct {
	ResponseID string `json:"responseId"`
	Username   string `json:"username"`
	Votes      int    `json:"votes"`
}

// WatchEvent tracks how long a user watched a post or challenge response.
type WatchEvent struct {
	ID         string `json:"id"`
	UserID     string `json:"userId"`
	ContentID  string `json:"contentId"`
	ContentType string `json:"contentType"` // "post", "challenge", "response"
	WatchTime  int    `json:"watchTime"`    // milliseconds
	Completed  bool   `json:"completed"`    // watched to end
	CreatedAt  string `json:"createdAt"`
}

// WatchEventPayload is the request body for recording a watch event.
type WatchEventPayload struct {
	UserID      string `json:"userId"`
	ContentID   string `json:"contentId"`
	ContentType string `json:"contentType"`
	WatchTime   int    `json:"watchTime"`
	Completed   bool   `json:"completed"`
}

// Report represents a user report on content or another user.
type Report struct {
	ID           string `json:"id"`
	ReporterID   string `json:"reporterId"`
	TargetID     string `json:"targetId"`
	TargetType   string `json:"targetType"` // "post", "challenge", "response", "user"
	Reason       string `json:"reason"`
	Description  string `json:"description"`
	Status       string `json:"status"` // "pending", "reviewed", "resolved"
	CreatedAt    string `json:"createdAt"`
}

// ReportPayload is the request body for creating a report.
type ReportPayload struct {
	ReporterID  string `json:"reporterId"`
	TargetID    string `json:"targetId"`
	TargetType  string `json:"targetType"`
	Reason      string `json:"reason"`
	Description string `json:"description"`
}

// ChallengeComment represents a comment on a challenge.
type ChallengeComment struct {
	ID             string `json:"id"`
	ChallengeID    string `json:"challengeId"`
	AuthorID       string `json:"authorId"`
	AuthorUsername string `json:"authorUsername"`
	Text           string `json:"text"`
	CreatedAt      string `json:"createdAt"`
}

// ChallengeCommentPayload is the request body for adding a challenge comment.
type ChallengeCommentPayload struct {
	ChallengeID string `json:"challengeId"`
	UserID      string `json:"userId"`
	Username    string `json:"username"`
	Text        string `json:"text"`
}

// ChallengeDetailResponse bundles the challenge + all responses for the detail view.
type ChallengeDetailResponse struct {
	Challenge Challenge           `json:"challenge"`
	Responses []ChallengeResponse `json:"responses"`
	Votes     []VoteSummary       `json:"votes,omitempty"`
}
