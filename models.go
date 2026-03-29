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
}

// HomeFeedItem wraps a post or challenge for the mixed home feed.
type HomeFeedItem struct {
	Type      string     `json:"type"` // "challenge" or "post"
	Challenge *Challenge `json:"challenge,omitempty"`
	Post      *Post      `json:"post,omitempty"`
}

// ChallengeResponse represents someone accepting and responding to a challenge.
type ChallengeResponse struct {
	ID                string `json:"id"`
	ChallengeID       string `json:"challengeId"`
	ResponderID       string `json:"responderld"`
	ResponderUsername string `json:"responderUsername"`
	ResponderLeague   string `json:"responderLeague"`
	VideoURL          string `json:"videoUrl"`
	ThumbnailURL      string `json:"thumbnailUrl,omitempty"`
	Likes             int    `json:"likes"`
	Views             int    `json:"views"`
	CreatedAt         string `json:"createdAt"`
}

// CreateChallengePayload is the request body for creating a challenge.
type CreateChallengePayload struct {
	CreatorID    string   `json:"creatorId"`
	VideoURL     string   `json:"videoUrl"`
	ThumbnailURL string   `json:"thumbnailUrl"`
	Prefix       string   `json:"prefix"`
	Subject      string   `json:"subject"`
	Visibility   string   `json:"visibility"` // "arena" or "friends'
	VisibleTo    []string `json:"visibleTo"`  // friend IDs (empty = all)
}

// AcceptChallengePayload is sent when a user accepts a challenge.
type AcceptChallengePayload struct {
	ChallengeID  string `json:"challengeId"`
	ResponderID  string `json:"responderId"`
	VideoURL     string `json:"videoUrl"`
	ThumbnailURL string `json:"thumbnailUrl"`
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

// ChallengeDetailResponse bundles the challenge + all responses for the detail view.
type ChallengeDetailResponse struct {
	Challenge Challenge           `json:"challenge"`
	Responses []ChallengeResponse `json:"responses"`
	Votes     []VoteSummary       `json:"votes,omitempty"`
}
