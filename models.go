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

// challenge represents an open challenge created by a user.
type Challenge struct {
	ID              string   `json:"id"`
	CreatorID       string   `json:"creatorId"`
	CreatorUsername string   `json:"creatorUsername"`
	CreatorLeague   string   `json:"creatorLeague"`
	VideoURL        string   `json:"videoUrl"`
	ThumbnailURL    string   `json:"thumbnailUrl,omitempty"`
	Prefix          string   `json:"prefix"`              // "Who is better", "Which is best", etc.
	Subject         string   `json:"subject"`             // "Dancer", "Painting", etc.
	Visibility      string   `json:"visibility"`          // "arena" or "friends"
	VisibleTo       []string `json:"visibleTo,omitempty"` // friends IDs (empty = all friends)
	Status          string   `json:"status"`              // "open", "active", "completed", "expired"
	Likes           int      `json:"likes"`
	Views           int      `json:"views"`
	CreatedAt       string   `json:"createdAt"`
	ExpiresAt       string   `json:"expiresAt"`
	ResponseCount   int      `json:"responseCount"`
	// Content understanding fields (creator-declared + system-inferred)
	Category        string   `json:"category"`             // Primary: "comedy","motivation","sports","dance","music",etc.
	EmotionTags     []string `json:"emotionTags,omitempty"` // ["happy","intense","inspiring"]
	EnergyLevel     string   `json:"energyLevel"`           // "low","medium","high"
}

// HomeFeedItem wraps a post or challenge for the mixed home feed.
type HomeFeedItem struct {
	Type      string     `json:"type"` // "challenge" or "post"
	Challenge *Challenge `json:"challenge,omitempty"`
	Post      *Post      `json:"post,omitempty"`
}

// ChallengeResponse represents someone accepting and responding to a challenge.
type ChallengeResponse struct {
	ID                string  `json:"id"`
	ChallengeID       string  `json:"challengeId"`
	ResponderID       string  `json:"responderld"`
	ResponderUsername string  `json:"responderUsername"`
	ResponderLeague   string  `json:"responderLeague"`
	VideoURL          string  `json:"videoUrl"`
	ThumbnailURL      string  `json:"thumbnailUrl,omitempty"`
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
	CreatorID    string   `json:"creatorId"`
	VideoURL     string   `json:"videoUrl"`
	ThumbnailURL string   `json:"thumbnailUrl"`
	Prefix       string   `json:"prefix"`
	Subject      string   `json:"subject"`
	Visibility   string   `json:"visibility"`   // "arena" or "friends"
	VisibleTo    []string `json:"visibleTo"`    // friend IDs (empty = all)
	Category     string   `json:"category"`     // "comedy","motivation","sports","dance",etc.
	EmotionTags  []string `json:"emotionTags"`  // ["happy","intense","inspiring"]
	EnergyLevel  string   `json:"energyLevel"`  // "low","medium","high"
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
	ChallengeID  string `json:"challengeId"`
	ResponderID  string `json:"responderId"`
	VideoURL     string `json:"videoUrl"`
	ThumbnailURL string `json:"thumbnailUrl"`
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
