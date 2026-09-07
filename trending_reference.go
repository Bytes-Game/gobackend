package main

import (
	"math"
	"time"
)

// ════════════════════════════════════════════════════════════════════════════
// HOW A NEW VIDEO IS ALLOWED TO BREAK OUT
// ════════════════════════════════════════════════════════════════════════════
//
// This has been wrong twice, and the second time is the interesting one.
//
// FIRST it was measured against a number somebody typed in:
//
//	velocity := math.Min(1.0, float64(recentEng)/15.0)
//
// Fifteen likes, comments, shares or saves in two hours meant "as trending as
// it gets". Sensible for a platform this size, meaningless for one ten times
// bigger where an ordinary video clears fifteen before lunch. Every video
// would score the maximum, they would all tie, and the signal would go flat
// with no error to notice.
//
// SECOND it was measured against what other videos were counting:
//
//	share := recentEng / p75(recentEng across everything moving)
//
// That removed the typed-in number but kept the real problem, just better
// hidden. It compares COUNTS. A count depends on how many people have already
// been shown the video, so the videos with the biggest audiences set the bar
// that everything else has to clear. A new video shown to two hundred people
// cannot out-count an established one shown to five hundred thousand, however
// much better it is. The rich get richer and nothing new ever surfaces —
// which is the exact opposite of what this signal is for.
//
// So the question is not "how many" at all. It is:
//
//	Of the people who saw it, how many acted — and is that better than
//	other videos being shown to about as many people right now?
//
// That is how a short-video feed actually escalates content. A video is shown
// to a small audience. If it does better with them than its peers at that same
// stage, it earns a bigger audience, and is then judged against the videos at
// THAT stage. Winning is always relative to where you are, so something with
// two hundred views and something with five hundred thousand can both be
// having a good day, and both can be told so.
//
// Two consequences worth being explicit about:
//
//   - A brand-new video can reach the top of this signal on its first few
//     hundred views. That is the point, not a bug.
//   - A huge video coasting on its back catalogue scores LOW here, because
//     its rate is ordinary for its stage. Its size is already rewarded
//     elsewhere in the score; it does not also get to be "trending".

// trendingReferenceTTL is how long the benchmarks are reused before they are
// measured again. Trending looks at a two-hour window, so benchmarks a few
// minutes old still describe the same window.
const trendingReferenceTTL = 5 * time.Minute

// trendingReferencePercentile is the bar within a stage: the busiest quarter.
//
// Not the median, which would hand full marks to half of everything, and not
// the very top, which would mean only one video at a time is ever trending. A
// quarter is a judgement about what the word should mean, not a measurement of
// this catalogue, so it does not go stale as the platform grows.
const trendingReferencePercentile = 0.75

// trendingMinExposure is how many views a video needs in the window before its
// rate is treated as a measurement rather than noise.
//
// Two people out of three is not a 67% engagement rate, it is two people. This
// is a fact about how much a proportion can be trusted from a small sample,
// not a fact about how big the platform is, so it means the same thing at
// every size. Videos below it are not judged badly — they are not judged yet,
// exactly like a video still inside its first test audience.
const trendingMinExposure = 20

// exposureTierBase is how much bigger each stage is than the one below it.
//
// Ten. So stages are roughly 20-99 views, 100-999, 1000-9999, and so on. The
// point of a stage is to group videos that have had a comparable chance, and
// audiences grow by multiples rather than by fixed amounts, so the stages do
// too. This is a shape, not a size, and it does not need revisiting as the
// platform grows — there are simply more stages above.
const exposureTierBase = 10.0

// exposureTier says which stage a video is at, from how many people have seen
// it in the window.
func exposureTier(views int) int {
	if views < 1 {
		return 0
	}
	// The nudge matters. Without it an exact power of ten comes back as
	// 5.999999999999999 and a video on precisely a million views is filed one
	// stage too low, where it is judged against easier company than it
	// deserves. Nothing about that would ever look like an error.
	t := int(math.Floor(math.Log(float64(views))/math.Log(exposureTierBase) + 1e-9))
	if t < 0 {
		return 0
	}
	return t
}

// trendingBenchmarks is the rate to beat at each stage.
type trendingBenchmarks struct {
	byTier map[int]float64
}

// forTier returns the bar for a stage, falling back to the nearest stage that
// has one.
//
// A stage can be empty — nothing on the platform happens to have that many
// views this hour — and an empty stage must not mean "everything here is
// trending". Looking down to a busier neighbouring stage keeps the comparison
// honest rather than vacuous.
func (b *trendingBenchmarks) forTier(t int) float64 {
	if b == nil || len(b.byTier) == 0 {
		return 0
	}
	if v, ok := b.byTier[t]; ok && v > 0 {
		return v
	}
	best, bestDist := 0.0, 1<<30
	for k, v := range b.byTier {
		if v <= 0 {
			continue
		}
		d := k - t
		if d < 0 {
			d = -d
		}
		if d < bestDist {
			best, bestDist = v, d
		}
	}
	return best
}

// trendingBenchmarkCache holds one small table for the whole platform, so this
// is a single cheap query every few minutes rather than one per video.
var trendingBenchmarkCache = NewSignalCache[*trendingBenchmarks](trendingReferenceTTL)

const trendingReferenceKey = "platform"

// trendingBenchmarksNow returns the current rate-to-beat for each stage.
func trendingBenchmarksNow() *trendingBenchmarks {
	if v, ok := trendingBenchmarkCache.Get(trendingReferenceKey); ok && v != nil {
		return v
	}
	b := measureTrendingBenchmarks()
	trendingBenchmarkCache.Set(trendingReferenceKey, b)
	return b
}

// measureTrendingBenchmarks asks the database how well videos are doing at
// each stage right now.
//
// One row per stage. Content below the exposure floor is excluded rather than
// counted: a handful of views produces a wild rate, and letting those into the
// sample would drag the bar somewhere no real video could reach.
func measureTrendingBenchmarks() *trendingBenchmarks {
	b := &trendingBenchmarks{byTier: map[int]float64{}}
	if db == nil {
		return b
	}
	rows, err := db.Query(`
		SELECT FLOOR(LN(views) / LN($1))::int AS tier,
		       PERCENTILE_CONT($2) WITHIN GROUP (ORDER BY acted::float / views) AS bar
		FROM (
			SELECT COUNT(*) FILTER (
			           WHERE event_type IN ('like','comment','share','save')) AS acted,
			       COUNT(*) FILTER (WHERE event_type = 'view') AS views
			FROM feed_events
			WHERE created_at > NOW() - INTERVAL '2 hours'
			GROUP BY content_type, content_id
		) AS per_video
		WHERE views >= $3
		GROUP BY tier`, exposureTierBase, trendingReferencePercentile, trendingMinExposure)
	if err != nil {
		// Fail quiet, not loud. Missing benchmarks cost some sharpness in one
		// term of the score; they must never cost the viewer their feed.
		return b
	}
	defer rows.Close()
	for rows.Next() {
		var tier int
		var bar float64
		if rows.Scan(&tier, &bar) != nil {
			continue
		}
		b.byTier[tier] = bar
	}
	return b
}

// trendingBreakout scores how well a video is doing with the people who have
// actually seen it, against other videos at the same stage.
//
// The rate is deliberately the CONSERVATIVE one — a lower bound that already
// accounts for how few views it might be based on — while the bar is the plain
// rate its peers achieved. So the comparison leans slightly against the video
// being scored. On a signal that decides who gets a bigger audience, erring
// towards "not yet proven" is the right direction to err.
func trendingBreakout(recentEng, recentViews int, b *trendingBenchmarks) float64 {
	if recentEng <= 0 || recentViews < trendingMinExposure {
		// Not doing badly — not judged yet.
		return 0
	}
	bar := b.forTier(exposureTier(recentViews))
	if bar <= 0 {
		// Nothing to compare against. "Trending" is a comparison, so with no
		// peers there is no answer, and inventing one would mean the first
		// video of a quiet hour is always a phenomenon.
		return 0
	}
	rate := wilsonLowerBound(float64(recentEng), float64(recentViews))
	return math.Min(1, rate/bar)
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
