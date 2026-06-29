package main

import (
	"strings"
	"sync/atomic"
	"time"
)

// ─────────────────────────────────────────────────────────────────────────────
// NEGATIVE-FEEDBACK PROFILE MINING
//
// Right now blocks/unfollows/skips/not_interested are treated as PENALTIES
// — they multiply the score down. That throws away half the signal.
//
// Each negative event also tells us something specific about the user:
//   - Block on a creator's "loud comedy" content → user dislikes loud comedy
//   - Unfollow a "high-energy fitness" creator → EnergyPreference should drift down
//   - Skip 5 cooking videos in a row → CategoryAffinity[cooking] should decline
//   - Not-interested on a "sad emotional" video → EmotionPreference["sad"] should drop
//
// This module mines those signals and applies them to UserProfile in a
// bounded way: each negative event nudges the relevant profile field,
// with magnitude proportional to how strong the negative was. Repeated
// negatives compound; isolated ones barely register.
//
// Output: same profile struct the ranker reads. Effect: the user's
// preferences sharpen over time even from rejections, not just acceptances.
// ─────────────────────────────────────────────────────────────────────────────

const (
	// Magnitude of a single profile nudge from one negative event. Small —
	// we don't want one accidental skip to flip a whole preference.
	negFeedbackBlockNudge        = 0.15  // strongest signal
	negFeedbackUnfollowNudge     = 0.10
	negFeedbackNotInterestedNudge = 0.08
	negFeedbackSkipNudge         = 0.03  // weakest — skips are noisy
	// (no bounce nudge — bounce isn't a feed_event on this path; see nudgeForEvent)
	// Cap on per-field deltas applied per call so a burst of one event type
	// can't completely overwrite the profile in a single tick.
	negFeedbackMaxDeltaPerTick = 0.30
)

// applyNegativeFeedbackToProfile takes one negative event and updates the
// in-memory UserProfile in place. Caller is responsible for persisting
// (the existing analytics-job flush handles that). Best-effort: missing
// data is silently skipped.
//
// We update FOUR fields based on the event type and content metadata:
//   - CategoryAffinity[content.Category] decreases
//   - EmotionPreference[emotion] decreases for each content emotion
//   - EnergyPreference moves AWAY from content.EnergyLevel
//   - AvoidedCategories list grows when affinity crosses a threshold
//
// Magnitude is scaled by the event type (block > unfollow > not_interested
// > bounce > skip).
func applyNegativeFeedbackToProfile(profile *UserProfile, eventType string, cs *ContentScore, emotions []string) {
	if profile == nil || cs == nil {
		return
	}
	nudge := nudgeForEvent(eventType)
	if nudge <= 0 {
		return
	}

	// Initialize maps if cold.
	if profile.CategoryAffinity == nil {
		profile.CategoryAffinity = make(map[string]float64)
	}
	if profile.EmotionPreference == nil {
		profile.EmotionPreference = make(map[string]float64)
	}

	// 1. Category affinity nudge.
	if cs.Category != "" {
		cat := strings.ToLower(cs.Category)
		current := profile.CategoryAffinity[cat]
		// Asymmetric: pulling DOWN is half-magnitude when already low,
		// to avoid driving deep into negative territory on noisy skips.
		drop := nudge
		if current < 0.20 {
			drop *= 0.5
		}
		if drop > negFeedbackMaxDeltaPerTick {
			drop = negFeedbackMaxDeltaPerTick
		}
		profile.CategoryAffinity[cat] = current - drop
		if profile.CategoryAffinity[cat] < -0.5 {
			profile.CategoryAffinity[cat] = -0.5
		}
		// Promote to AvoidedCategories list when it crosses the dislike threshold.
		if profile.CategoryAffinity[cat] <= -0.30 && !containsCI(profile.AvoidedCategories, cat) {
			profile.AvoidedCategories = append(profile.AvoidedCategories, cat)
			if len(profile.AvoidedCategories) > 20 {
				profile.AvoidedCategories = profile.AvoidedCategories[1:]
			}
		}
	}

	// 2. Emotion preference nudge — per emotion in the content.
	for _, e := range emotions {
		if e == "" {
			continue
		}
		em := strings.ToLower(e)
		profile.EmotionPreference[em] -= nudge * 0.6 // emotions are weaker signal than category
		if profile.EmotionPreference[em] < -0.5 {
			profile.EmotionPreference[em] = -0.5
		}
	}

	// 3. EnergyPreference drift away from content energy.
	// If user negs high-energy content, push pref toward low-energy.
	gap := cs.EnergyLevel - profile.EnergyPreference
	if gap == 0 {
		return
	}
	step := -nudge * 0.3 // small per-event nudge
	if gap > 0 {
		profile.EnergyPreference -= -step // move away from high → lower pref
	} else {
		profile.EnergyPreference += -step // move away from low → higher pref
	}
	if profile.EnergyPreference < 0 {
		profile.EnergyPreference = 0
	}
	if profile.EnergyPreference > 1 {
		profile.EnergyPreference = 1
	}

	if metricNegProfileMine != nil {
		metricNegProfileMine.WithLabelValues(eventType).Inc()
	}
}

func nudgeForEvent(eventType string) float64 {
	switch eventType {
	case "block":
		return negFeedbackBlockNudge
	case "unfollow":
		return negFeedbackUnfollowNudge
	case "not_interested":
		return negFeedbackNotInterestedNudge
	case "skip":
		return negFeedbackSkipNudge
	}
	// No "bounce" case: bounce is not a feed_event type that reaches this path
	// (MarkBounce writes recent_bounces directly), and the bounce signal is
	// already captured via bouncePenalty + the aggregator's CategoryAffinity
	// decay — mining it here too would triple-count it.
	return 0
}

func containsCI(xs []string, target string) bool {
	target = strings.ToLower(target)
	for _, x := range xs {
		if strings.EqualFold(x, target) {
			return true
		}
	}
	return false
}

// applyNegativeFeedbackFromEvent is the convenience wrapper called from
// the event handler: looks up the content + emotions, mutates profile,
// schedules persistence. Cheap: profile is in-memory and the persist
// happens on the next normal flush.
func applyNegativeFeedbackFromEvent(profile *UserProfile, event FeedEvent) {
	if profile == nil {
		return
	}
	if !isMineableNegative(event.EventType, event.CompletionRate) {
		return
	}
	cs := getContentScore(event.ContentID, event.ContentType)
	if cs == nil {
		return
	}
	emotions := getContentEmotions(event.ContentID, event.ContentType)
	applyNegativeFeedbackToProfile(profile, event.EventType, cs, emotions)
}

// isMineableNegative decides whether an event carries usable negative
// signal. Skips with VERY low completion (<10%) count; longer-watched
// skips might just be "fine but not great" — too noisy to mine.
func isMineableNegative(eventType string, completionRate float64) bool {
	switch eventType {
	case "block", "unfollow", "not_interested":
		return true
	case "skip":
		return completionRate >= 0 && completionRate < 0.10
	}
	return false
}

// negativeProfileMineEpoch is bumped each time we apply a negative-feedback
// nudge so the analytics job knows there's dirty profile state to flush.
// Bumped from the per-event mining goroutine (many concurrent) and read by
// the diagnostics endpoint, so it must be atomic — a plain int64++ from
// multiple goroutines is a data race that `go test -race` flags.
var negativeProfileMineEpoch atomic.Int64

func bumpNegativeProfileMineEpoch() {
	negativeProfileMineEpoch.Add(1)
}

// negativeProfileMineDiag returns the current epoch — useful for the
// admin diagnostics endpoint to show that mining is happening.
func negativeProfileMineDiag() int64 {
	return negativeProfileMineEpoch.Load()
}

// init guard so unused vars/imports don't get flagged.
var _ = time.Now
