package main

// UserEngagementMetrics tracks how a user interacts with content.
type UserEngagementMetrics struct {
	AvgWatchTime           float64  `json:"avg_watch_time"`
	WatchCompletionRate    float64  `json:"watch_completion_rate"`
	RewatchRate            float64  `json:"rewatch_rate"`
	LikeBehavior           string   `json:"like_behavior"`
	CommentBehavior        string   `json:"comment_behavior"`
	ShareBehavior          string   `json:"share_behavior"`
	DislikeBehavior        string   `json:"dislike_behavior"`
	SavesBehavior          string   `json:"saves_behavior"`
	SkipSpeed              float64  `json:"skip_speed"`
	FastSwipeRate          float64  `json:"fast_swipe_rate"`
	RageQuitTriggers       []string `json:"rage_quit_triggers"`
	PostLossEngagementDrop float64  `json:"post_loss_engagement_drop"`
	RevengePlayProbability float64  `json:"revenge_play_probability"`
}

// UserPsychologicalProfile infers a user's mental state from their behavior.
type UserPsychologicalProfile struct {
	CurrentMood     string  `json:"current_mood"`
	LastEmotion     string  `json:"last_emotion"`
	FatigueScore    float64 `json:"fatigue_score"`
	SentimentTrend  string  `json:"sentiment_trend"`
	BurnoutRisk     string  `json:"burnout_risk"`
	CognitiveLoad   float64 `json:"cognitive_load"`
	EmotionalVolatily float64 `json:"emotional_volatility"`
}
