package main

// watch_history.go — the long memory of what somebody has already watched.
//
// ════════════════════════════════════════════════════════════════════════════
// WHY NOT JUST FILTER IT OUT, THE WAY THE BIG APPS APPEAR TO
// ════════════════════════════════════════════════════════════════════════════
//
// Because this app tried that three times and it broke the feed three times.
// The list is in seen_filter.go and it is worth reading before touching this
// file: drop everything seen and the feed empties on a small catalogue; drop
// it with a floor and a request for thirty comes back as eight; split into an
// unseen tier and a backfill tier and the page that fills by backfill reports
// "no more" and ends the feed outright.
//
// The apps that hard-filter can afford to. They have more video than any one
// person could exhaust in several lifetimes, so "never show a repeat" and
// "always have something to show" never collide. Here they collide constantly
// — a live log had the Battles tab down to eight items.
//
// So the OUTCOME is copied, not the mechanism. What a viewer experiences on a
// large app is: things you have watched essentially never come back, and the
// longer ago it was the less that holds. That is reproducible as a score:
// make the handicap big enough that a repeat loses to any reasonable unseen
// candidate, and let it fade with age. When there is genuinely nothing else,
// repeats still rank among themselves and the feed keeps moving instead of
// running dry.
//
// Nothing here withholds anything. That is the invariant the three broken
// versions each violated.
//
// ════════════════════════════════════════════════════════════════════════════
// TWO MEMORIES
// ════════════════════════════════════════════════════════════════════════════
//
//	seen_filter.go   twelve hours, everything SERVED, decaying from 0.6.
//	                 Stops a video coming back two swipes later.
//	this file        ninety days, everything INTERACTED WITH, decaying from
//	                 rewatchPenaltyMax. Stops yesterday's video beating
//	                 content nobody has shown you yet.
//
// The two curves are sized to hand off to each other: the short one reaches
// zero at twelve hours, and the long one is still near full there, so there
// is no gap where a recently-watched video is suddenly unprotected. Before
// this file existed that gap was the whole problem — past twelve hours the
// only thing between a viewer and a repeat was a flat 0.15 bonus on the
// content it was competing against.
//
// ════════════════════════════════════════════════════════════════════════════
// NOT EVERY VIEW MEANS THE SAME THING
// ════════════════════════════════════════════════════════════════════════════
//
// A video somebody finished, liked and rewatched is definitively watched. A
// video that scrolled past at speed while they were hunting for something
// else barely registered, and holding it back for three months on that basis
// would quietly shrink the catalogue for no reason.
//
// So each item carries a strength as well as a time, and the handicap is
// scaled by it — the same idea the big apps lean on hardest, where finishing
// a video is the single most informative thing a viewer does.

import (
	"log"
	"math"
	"time"
)

const (
	// How far back the memory reaches, and how many recent events it reads to
	// build itself. Both bound the cost of a query that runs on every feed
	// request; the day count is also the honest answer to "how long does this
	// app remember that you watched something".
	//
	// Distinct items come out of the events, so somebody who rewatches things
	// remembers slightly fewer videos than the event count — which is the
	// right way round, since a rewatched video is one they clearly do not mind
	// seeing again.
	watchMemoryEvents = 15000
	watchMemoryDays   = 90

	// The handicap on something watched moments ago, fading to nothing across
	// the memory window.
	//
	// Deliberately equal to seenPenaltyMax so the two curves join smoothly:
	// the twelve-hour curve is at zero exactly where this one is still near
	// full. Decisive but finite — a repeat loses to any reasonable unseen
	// candidate, and cannot lose to nothing at all, because there is no floor
	// below which content stops being served.
	rewatchPenaltyMax = 0.6
)

// watchMemory is what the long memory knows about one video.
type watchMemory struct {
	LastAt   int64   // unix seconds, most recent interaction
	Strength float64 // 0..1 — how definitely this was actually consumed
}

// watchHistory maps "type:id" to what is remembered about it.
type watchHistory map[string]watchMemory

// seen reports whether there is any memory of this item at all.
func (h watchHistory) seen(key string) bool {
	_, ok := h[key]
	return ok
}

// suppression is the handicap this item carries now, on the same scale as the
// rest of the score. Zero for anything never watched.
//
// Linear fade across the window, scaled by how definitely it was consumed.
// Linear rather than exponential on purpose: it is the shape already used for
// the twelve-hour curve, and two handicaps that behave the same way are two
// fewer things to hold in your head when a ranking looks wrong.
func (h watchHistory) suppression(key string, now int64) float64 {
	m, ok := h[key]
	if !ok || m.LastAt <= 0 {
		return 0
	}
	age := float64(now - m.LastAt)
	if age < 0 {
		age = 0
	}
	window := (time.Duration(watchMemoryDays) * 24 * time.Hour).Seconds()
	if age >= window {
		return 0
	}
	strength := math.Max(0, math.Min(1, m.Strength))
	return rewatchPenaltyMax * (1 - age/window) * strength
}

// buildWatchHistory reads the long memory for one viewer.
//
// ════════════════════════════════════════════════════════════════════════════
// THE SHAPE OF THIS QUERY IS LOad-BEARING
// ════════════════════════════════════════════════════════════════════════════
//
// It runs on every feed request, so it has to be bounded and it has to use
// the index.
//
// The inner select walks idx_feed_events_user (user_id, created_at DESC)
// backwards from the newest event and stops after a fixed number of rows.
// Only those get grouped. Grouping the whole history instead and ordering by
// MAX(created_at) reads better and costs four times as much — measured at
// 51ms against 13ms on a fifty-thousand-event history — because it has to
// touch every row a viewer ever generated before it can pick a top slice.
//
// This replaced a `SELECT DISTINCT … LIMIT 1000` with NO ordering at all. SQL
// promises nothing about which rows an unordered LIMIT returns, and against a
// real Postgres that query forgot a video watched one minute ago and
// remembered 332 of the most recent thousand. Everything it forgot scored as
// never-watched, which is exactly how a video from yesterday ended up
// outranking content nobody had been shown.
//
// The strength CASE is the "not every view means the same thing" rule:
//
//	finished, liked, shared, saved, rewatched, commented → 1.0, definitive
//	skipped, marked not-interested                       → 1.0, they said no
//	a plain view                                         → how much they
//	                                                       watched, floored
//	anything else (a bare impression)                    → 0.25, it may only
//	                                                       have flown past
func buildWatchHistory(userID string) watchHistory {
	hist := make(watchHistory)
	if db == nil {
		return hist
	}
	rows, err := db.Query(`
		SELECT content_type || ':' || content_id AS content_key,
		       EXTRACT(EPOCH FROM MAX(created_at))::bigint AS last_at,
		       MAX(CASE
		             WHEN event_type IN ('complete','rewatch','loop','like',
		                                 'share','save','comment') THEN 1.0
		             WHEN event_type IN ('skip','not_interested')   THEN 1.0
		             WHEN event_type = 'view'
		               THEN GREATEST(COALESCE(completion_rate, 0)::float8, 0.3)
		             ELSE 0.25
		           END) AS strength
		  FROM (
		    SELECT content_type, content_id, event_type, completion_rate, created_at
		      FROM feed_events
		     WHERE user_id = $1
		       AND created_at > NOW() - ($2 || ' days')::interval
		     ORDER BY created_at DESC
		     LIMIT $3
		  ) recent
		 GROUP BY content_key`,
		userID, watchMemoryDays, watchMemoryEvents)
	if err != nil {
		// An empty memory reads as "never watched", which hands out the unseen
		// bonus freely and applies no handicap. Wrong in the harmless
		// direction: the twelve-hour seen-set is a separate mechanism and
		// still holds, so a failure here cannot resurface something watched
		// minutes ago.
		log.Printf("watch history: could not read for %s: %v", userID, err)
		return hist
	}
	defer rows.Close()
	for rows.Next() {
		var key string
		var lastAt int64
		var strength float64
		if err := rows.Scan(&key, &lastAt, &strength); err == nil {
			hist[key] = watchMemory{LastAt: lastAt, Strength: strength}
		}
	}
	return hist
}
