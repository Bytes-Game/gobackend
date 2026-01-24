package main

import "time"

// Post represents a single post made by a user, matching the client's expectations.
type Post struct {
	ID        string    `json:"id"`
	URL       string    `json:"url"`
	Caption   string    `json:"caption"`
	Timestamp time.Time `json:"timestamp"`
}

// ContentPreferenceProfile stores the user's content tastes.
type ContentPreferenceProfile struct {
	FavoriteCategories []string `json:"favorite_categories"`
}

// UserSessionBehavior tracks habits related to app usage sessions.
type UserSessionBehavior struct {
	AvgSessionDuration float64 `json:"avg_session_duration"` // in minutes
	BingeWatchingScore float64 `json:"binge_watching_score"`
}

// UserSocialProfile tracks the user's social interaction patterns.
type UserSocialProfile struct {
	FollowRate   float64 `json:"follow_rate"`   // follows per session
	UnfollowRate float64 `json:"unfollow_rate"` // unfollows per session
}

// AlgorithmInteractionProfile tracks how the user interacts with the feed's algorithm.
type AlgorithmInteractionProfile struct {
	FeedbackFrequency float64 `json:"feedback_frequency"` // e.g., how often they use "show less like this"
}


// User represents a user's profile data, with the Posts field corrected.
type User struct {
	Username             string                      `json:"username"`
	password             string                      `json:"-"` // The '-' tag prevents the password from being serialized into JSON.
	FullName             string                      `json:"fullName"`
	Caption              string                      `json:"caption"`
	Followers            int                         `json:"followers"`
	Following            int                         `json:"following"`
	Posts                []Post                      `json:"posts"` // Corrected: This is now a slice of Post objects.
	Wins                 int                         `json:"wins"`
	Losses               int                         `json:"losses"`
	League               string                      `json:"league"`
	FollowingList        []string                    `json:"followingList"`
	Engagement           UserEngagementMetrics       `json:"engagement"`
	Psychological        UserPsychologicalProfile    `json:"psychological"`
	ContentPreference    ContentPreferenceProfile    `json:"content_preference"`
	SessionBehavior      UserSessionBehavior         `json:"session_behavior"`
	Social               UserSocialProfile           `json:"social"`
	AlgorithmInteraction AlgorithmInteractionProfile `json:"algorithm_interaction"`
}

// SearchResponse is a temporary struct for the search handler.
type SearchResponse struct {
	Results []User `json:"results"`
	Total   int    `json:"total"`
}
