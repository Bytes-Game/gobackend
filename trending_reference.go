package main

import (
	"math"
	"time"
)

// ════════════════════════════════════════════════════════════════════════════
// WHAT "TRENDING" MEANS WHEN THE PLATFORM IS NOT THE SIZE IT IS TODAY
// ════════════════════════════════════════════════════════════════════════════
//
// Trending used to be measured against a number somebody typed in:
//
//	velocity := math.Min(1.0, float64(recentEng)/15.0)
//
// Fifteen likes, comments, shares or saves in two hours meant "as trending as
// it gets". That is a sensible number for a platform this size and a
// meaningless one for a platform ten times bigger, where an ordinary video
// clears fifteen before lunch. Every video would score the maximum, they would
// all tie, and trending would quietly stop telling the feed anything at all.
//
// Nothing would break. No error, no empty page. The number would just go flat,
// and the only symptom would be a feed that had lost one of its signals
// without anybody being told.
//
// So the question is no longer "did this video pass fifteen". It is "is this
// video moving faster than the videos around it". That question means exactly
// the same thing on a hundred videos and on a hundred million, because it is
// asked against whatever the platform's own pace happens to be at the time.

// trendingReferenceTTL is how long the platform's current pace is reused
// before it is measured again. Trending is a two-hour window, so a pace that
// is a few minutes stale is still describing the same window.
const trendingReferenceTTL = 5 * time.Minute

// trendingReferencePercentile picks which video counts as "the pace to beat".
//
// The busiest quarter of everything that is moving right now. Not the median,
// which would hand full marks to half the catalogue, and not the very top,
// which would mean only the single biggest video of the moment ever counts as
// trending. A quarter is a judgement about what the word should mean, not a
// measurement of this catalogue, so it does not go stale as the platform
// grows.
const trendingReferencePercentile = 0.75

// trendingConfidenceFull is how many separate engagements it takes before a
// burst is believed in full.
//
// One person liking something is one person. It is not a trend, however quiet
// the platform is around them — and when the platform IS quiet, dividing by a
// tiny pace would otherwise turn that single like into a maximum score. A
// handful of independent people is the least that can be called a pattern at
// all, at any size. Below it the signal is held back rather than refused, the
// same way topicConfidence treats a thin match.
const trendingConfidenceFull = 5.0

// trendingReferenceCache holds one number for the whole platform, so this is a
// single cheap query every few minutes rather than one per video.
var trendingReferenceCache = NewSignalCache[float64](trendingReferenceTTL)

const trendingReferenceKey = "platform"

// trendingReference reports how much engagement a busy-but-ordinary piece of
// content is collecting in the trending window at this moment.
//
// Returns 0 when there is nothing to compare against — a brand-new platform, a
// quiet night, or no database. Callers must treat 0 as "no pace known" and
// lean on the confidence ramp instead of dividing by it.
func trendingReference() float64 {
	if v, ok := trendingReferenceCache.Get(trendingReferenceKey); ok {
		return v
	}
	ref := measureTrendingReference()
	trendingReferenceCache.Set(trendingReferenceKey, ref)
	return ref
}

// measureTrendingReference asks the database what the current pace is.
//
// Counts engagement per piece of content over the same two-hour window
// trending itself uses, then takes the percentile above. Content with no
// engagement at all is not in the sample: including it would drag the pace
// towards zero and make anything with a single like look like a phenomenon.
func measureTrendingReference() float64 {
	if db == nil {
		return 0
	}
	var ref float64
	err := db.QueryRow(`
		SELECT COALESCE(PERCENTILE_CONT($1) WITHIN GROUP (ORDER BY n), 0)
		FROM (
			SELECT COUNT(*) AS n
			FROM feed_events
			WHERE created_at > NOW() - INTERVAL '2 hours'
			  AND event_type IN ('like','comment','share','save')
			GROUP BY content_type, content_id
		) AS engaged`, trendingReferencePercentile).Scan(&ref)
	if err != nil {
		// Fail quiet, not loud. A missing pace costs some sharpness in one
		// term of the score; it must never cost the viewer their feed.
		return 0
	}
	return ref
}

// trendingVelocity scores how fast a piece of content is moving, relative to
// how fast things are moving generally.
//
// Two things decide it:
//
//   - How it compares to the pace. At or above the pace to beat is full marks.
//     Half the pace is half.
//   - How much it rests on. A burst of two is damped even if two is,
//     technically, ahead of a very quiet field.
//
// Both are ratios, so both mean the same thing at any size.
func trendingVelocity(recentEng int, reference float64) float64 {
	if recentEng <= 0 {
		return 0
	}
	share := 1.0
	if reference > 0 {
		share = math.Min(1, float64(recentEng)/reference)
	}
	return share * trendingConfidence(recentEng)
}

// trendingConfidence ramps from nothing to full over a handful of engagements.
func trendingConfidence(recentEng int) float64 {
	if recentEng <= 0 {
		return 0
	}
	return math.Min(1, float64(recentEng)/trendingConfidenceFull)
}

// ════════════════════════════════════════════════════════════════════════════
// THE SAME PROBLEM ON THE OTHER PATH
// ════════════════════════════════════════════════════════════════════════════
//
// Content that predates the analytics pipeline has view counts on its own row
// and no feed_events at all. That path had its own typed-in numbers — twenty
// views to qualify, thirty views an hour for full marks, capped at 0.3 — under
// a comment that said "10 views/hour ≈ noteworthy for a small platform".
//
// Same failure, same fix: ask how this video compares to the others, not
// whether it passed a number.

// viewVelocityReferenceCache holds the typical views-per-hour of recent
// content, measured the same way and for the same reason as the pace above.
var viewVelocityReferenceCache = NewSignalCache[float64](trendingReferenceTTL)

// rowTrendingWindowHours is how recent a video must be for its lifetime view
// count to still describe a burst rather than a long slow accumulation.
const rowTrendingWindowHours = 48.0

// rowTrendingConfidenceFull is how many views it takes before a rate computed
// from them is believed in full.
//
// Higher than the engagement ramp above, and deliberately so. A share is a
// decision; a view is barely one, so it takes many more of them to say the
// same amount. Three views in an hour is three views per hour arithmetically
// and nothing at all as evidence.
//
// Like the engagement ramp, this is about how much a count can be trusted,
// which is a fact about counting rather than about how big the platform is.
const rowTrendingConfidenceFull = 20.0

func rowTrendingConfidence(views int) float64 {
	if views <= 0 {
		return 0
	}
	return math.Min(1, float64(views)/rowTrendingConfidenceFull)
}

// rowTrendingCap is the most this fallback will ever claim.
//
// It is deliberately less than a full score. This path is inferring a burst
// from a lifetime view count, which is a much weaker thing to know than the
// engagement the real path measures, and it should never outrank it.
const rowTrendingCap = 0.3

// viewVelocityReference reports the typical views-per-hour among content young
// enough for the fallback to apply. Returns 0 when there is nothing to compare.
func viewVelocityReference() float64 {
	if v, ok := viewVelocityReferenceCache.Get(trendingReferenceKey); ok {
		return v
	}
	ref := measureViewVelocityReference()
	viewVelocityReferenceCache.Set(trendingReferenceKey, ref)
	return ref
}

func measureViewVelocityReference() float64 {
	if db == nil {
		return 0
	}
	var ref float64
	err := db.QueryRow(`
		SELECT COALESCE(PERCENTILE_CONT($1) WITHIN GROUP (ORDER BY vph), 0)
		FROM (
			SELECT views / GREATEST(EXTRACT(EPOCH FROM (NOW() - created_at)) / 3600.0, 1) AS vph
			FROM challenges
			WHERE created_at > NOW() - INTERVAL '1 hour' * $2
			  AND views > 0
		) AS young`, trendingReferencePercentile, rowTrendingWindowHours).Scan(&ref)
	if err != nil {
		return 0
	}
	return ref
}

// rowTrendingScore scores a burst from a view count and an age, for content
// the analytics pipeline never saw.
//
// Same shape as trendingVelocity: compare to the field, damp what rests on
// very little, and cap the whole thing because this is the weaker signal.
func rowTrendingScore(views int, ageHours float64, reference float64) float64 {
	if views <= 0 || ageHours <= 0 || ageHours > rowTrendingWindowHours {
		return 0
	}
	viewsPerHour := float64(views) / ageHours
	share := 1.0
	if reference > 0 {
		share = math.Min(1, viewsPerHour/reference)
	}
	return share * rowTrendingConfidence(views) * rowTrendingCap
}
