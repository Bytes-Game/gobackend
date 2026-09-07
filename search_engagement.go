package main

import "log"

// ════════════════════════════════════════════════════════════════════════════
// HOW WELL A VIDEO IS DOING, FROM EVERYTHING PEOPLE DID WITH IT
// ════════════════════════════════════════════════════════════════════════════
//
// Search used to answer that with:
//
//	logSafe(views + 5*likes + 1) / 8
//
// Two things wrong with it.
//
// FIRST, it could only see views and likes. Shares, comments, rewatches and —
// most importantly — whether anybody actually WATCHED THE THING TO THE END
// were all invisible. On a short-video app, finishing a video is the strongest
// thing a viewer can tell you short of sharing it, and search was throwing
// that away.
//
// SECOND, it invented its own exchange rate: a like is worth five views.
// Nothing else in the app agrees. engagementWeight already prices every action
// — a share is 3, a rewatch 2.5, a comment 1.2, a like 1 — and it is what the
// feed and the taste profiles both use. Search having a second opinion means
// the app disagrees with itself about what a share means, and the two would
// drift apart the first time anybody tuned either one.
//
// So this asks engagementWeight, the same as everything else.

// searchViewSquash controls how much louder a big audience is than a small one.
//
// Counts are compressed by a logarithm first, because the gap between 10 and
// 100 matters far more than the gap between 10,000 and 10,090 — otherwise one
// runaway video flattens the entire rest of the ranking. This divides that
// compressed number down to something that sits sensibly beside the other
// terms.
//
// It was 8, which made the whole range from a two-view video to a
// three-thousand-view one worth 0.076 — less than half of what freshness is
// worth, and less than twice the flat bonus a battle gets. Popularity was
// nearly a rounding error.
//
// At 4, roughly: a video needs about ten times the engagement of another to
// gain what being a battle gives it outright. Battles stay clearly preferred,
// which is deliberate, but being watched now counts for something real.
//
// This is a product judgement — how much should being popular matter — not a
// measurement of anything, so it is one line to change.
const searchViewSquash = 4.0

// searchEngagementScore turns everything people did with a video into one
// number, on roughly a 0..1 scale.
//
// Two separate questions, because mixing them buries the interesting one:
//
//	HOW MANY saw it        — the audience, compressed by a logarithm
//	WHAT THEY DID about it — as a rate, which then lifts or lowers that
//
// Adding the actions to the view count instead, which is the obvious thing to
// write, makes them invisible. A thousand views with twenty shares, twenty
// comments and twenty likes on top is a genuinely exceptional video — and
// counted that way it scored 2% above the same thousand views with nothing.
// Views outnumber everything else so heavily that anything summed beside them
// disappears.
//
// As a rate it is the other way round: twenty shares out of a thousand views
// is a two percent share rate, that is remarkable, and it reads as remarkable.
//
// agg may be nil. Plenty of the catalogue predates the analytics pipeline and
// has its totals on its own row instead, so the row's counts are used as a
// floor: whichever source knows more about a video is the one that decides.
// Without that, every video uploaded before events were recorded would look
// like nobody had ever watched it.
func searchEngagementScore(agg *contentEventAggregates, views, likes int) float64 {
	audience, reaction := 0.0, 0.0
	if agg != nil {
		audience = float64(agg.ViewCount) * searchViewValue(agg.AvgCompletion)

		// Each action priced by the one table the whole app uses.
		reaction += float64(agg.ShareCount) * engagementWeight("share", 0)
		reaction += float64(agg.RewatchCount) * engagementWeight("rewatch", 0)
		reaction += float64(agg.CommentCount) * engagementWeight("comment", 0)
		reaction += float64(agg.LikeCount) * engagementWeight("like", 0)
		// And what people did that was not positive. A video most people
		// skip is not a popular video, however many times it was served.
		reaction += float64(agg.SkipCount) * engagementWeight("skip", 0)
		reaction += float64(agg.NotInterestedCount) * engagementWeight("not_interested", 0)
	}

	// The row's own totals as a floor. Row views carry no completion figure,
	// so they count at the value of a view somebody stayed for — the honest
	// reading of a recorded view when nothing says otherwise.
	//
	// This is also what stops a stale event log from hiding a real audience:
	// a video whose recorded views have all gone missing still scores from
	// its raw count.
	if rowAudience := float64(views) * searchViewValue(1); rowAudience > audience {
		audience = rowAudience
		reaction = float64(likes) * engagementWeight("like", 0)
	}
	if audience <= 0 {
		if reaction <= 0 {
			return 0
		}
		// Actions recorded with no views to go with them. Somebody plainly
		// saw it — you cannot share what you have not watched — the view
		// events just are not there. Counting the actions as their own
		// audience keeps a real signal instead of discarding it because the
		// log is incomplete.
		audience = reaction
	}

	rate := reaction / audience
	if rate > searchReactionCap {
		rate = searchReactionCap
	}
	if rate < -searchReactionCap {
		rate = -searchReactionCap
	}
	return logSafe(audience+1) / searchViewSquash * (1 + rate)
}

// searchReactionCap bounds how far what people DID can move a video from where
// its audience alone would put it.
//
// Half, either way. So a video people love can climb by half again, one they
// push back on can lose a third, and neither can run away with the ranking on
// a handful of events — five shares from ten viewers is a fifty percent rate
// and is also five people.
//
// A proportion, so it means the same thing on any size of platform.
const searchReactionCap = 0.5

// searchEngagementAggregates loads what people did with a set of search hits,
// in one query, reusing whatever the feed has already cached this minute.
//
// Fail-open by design: on any error, or with no database, every video simply
// falls back to the totals on its own row. A search that ranks slightly worse
// is a bad minute; a search that returns nothing is a broken app.
func searchEngagementAggregates(hits []challengeHit) map[string]*contentEventAggregates {
	out := make(map[string]*contentEventAggregates, len(hits))
	if db == nil || len(hits) == 0 {
		return out
	}
	missing := make([]string, 0, len(hits))
	for _, h := range hits {
		id := h.Ch.ID
		if id == "" {
			continue
		}
		if a, ok := contentAggCache.Get(contentAggKey("challenge", id)); ok {
			out[id] = a
			continue
		}
		missing = append(missing, id)
	}
	if len(missing) == 0 {
		return out
	}
	fetched := make(map[string]*contentEventAggregates, len(missing))
	for _, id := range missing {
		fetched[id] = &contentEventAggregates{}
	}
	if err := loadEngagementAggregates("challenge", missing, fetched); err != nil {
		log.Printf("search engagement aggregates: %v", err)
		return out
	}
	for id, a := range fetched {
		contentAggCache.Set(contentAggKey("challenge", id), a)
		out[id] = a
	}
	return out
}

// searchViewFloor is what one view is worth when hardly anybody stayed.
//
// Not nothing. Somebody chose to open it, and that is real exposure whatever
// they did next. A quarter, so a view people abandon counts clearly less than
// one they watch through without counting as if it never happened.
const searchViewFloor = 0.25

// searchViewValue is what a view is worth, given how much of the video the
// average viewer actually watched.
//
// This is the one place that does NOT defer to engagementWeight, and the
// reason matters. engagementWeight answers "what does this tell me about this
// viewer's taste", and for that a half-watched video is genuinely a small
// negative — they gave it a chance and left. Here the question is "how is this
// video doing", and for that the same event is a positive: a person watched
// half a video.
//
// Prices a view at roughly the fraction that got watched, so finishing is
// worth about three times abandoning rather than infinitely more.
func searchViewValue(completion float64) float64 {
	if completion > 1 {
		completion = 1
	}
	if completion < searchViewFloor {
		return searchViewFloor
	}
	return completion
}
