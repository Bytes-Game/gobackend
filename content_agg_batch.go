package main

// content_agg_batch.go — batched feed_events aggregates for content scoring.
//
// computeContentScore runs two feed_events aggregate queries per item
// (90-day engagement counts + 2-hour trending window). On a cold page
// that's ~2 queries × 150-250 candidates before the challenge-row query
// even runs — the single biggest DB cost of a feed request and the main
// blocker to deepening the candidate pool.
//
// This module batches BOTH queries across the whole candidate set with
// one GROUP BY each (id = ANY($1)) and parks the results in a short-TTL
// cache that computeContentScore consults before falling back to its
// own per-item queries. The scoring math itself stays untouched in
// computeContentScore — single source of truth, zero behavior change;
// this is purely a fetch-plan optimization.
//
// Zero-row candidates matter: the warm pass writes an EMPTY aggregate
// entry for every requested key that returned no rows, so cold content
// gets a cache HIT (empty) instead of falling back to two pointless
// queries.

import (
	"log"

	"github.com/lib/pq"
)

// contentEventAggregates carries exactly the fields the two per-item
// queries in computeContentScore produce.
type contentEventAggregates struct {
	ViewCount, LikeCount, CommentCount        int
	SkipCount, RewatchCount, ShareCount       int
	NotInterestedCount                        int
	AvgCompletion, AvgWatchMs                 float64
	RecentEng, RecentViews                    int
}

var contentAggCache = NewSignalCache[*contentEventAggregates](contentScoreCacheTTL)

func contentAggKey(contentType, contentID string) string {
	return contentType + ":" + contentID
}

// warmContentAggregates batch-loads feed_events aggregates for every
// candidate that isn't already score-cached. Call once per feed request
// right after retrieval. Fail-open: on any error the per-item fallback
// path in computeContentScore still works.
func warmContentAggregates(items []HomeFeedItem) {
	if db == nil || len(items) == 0 || disableContentScoreCache {
		return
	}
	// Collect ids per type, skipping items whose full ContentScore is
	// already cached (their aggregates would go unread).
	byType := map[string][]string{}
	for _, it := range items {
		var id string
		if it.Challenge != nil {
			id = it.Challenge.ID
		} else if it.Post != nil {
			id = it.Post.ID
		}
		if id == "" {
			continue
		}
		key := it.Type + ":" + id
		if _, ok := contentScoreCache.Get(key); ok {
			continue
		}
		if _, ok := contentAggCache.Get(key); ok {
			continue
		}
		byType[it.Type] = append(byType[it.Type], id)
	}

	for typ, ids := range byType {
		if len(ids) == 0 {
			continue
		}
		aggs := make(map[string]*contentEventAggregates, len(ids))
		for _, id := range ids {
			aggs[id] = &contentEventAggregates{} // zero entry = cache hit for cold content
		}

		// Batch #1: the 90-day engagement aggregates.
		rows, err := db.Query(`
			SELECT content_id,
				COUNT(*) FILTER (WHERE event_type = 'view'),
				COUNT(*) FILTER (WHERE event_type = 'like'),
				COUNT(*) FILTER (WHERE event_type = 'comment'),
				COUNT(*) FILTER (WHERE event_type = 'skip'),
				COUNT(*) FILTER (WHERE event_type = 'rewatch'),
				COUNT(*) FILTER (WHERE event_type = 'share'),
				COUNT(*) FILTER (WHERE event_type = 'not_interested'),
				COALESCE(AVG(completion_rate) FILTER (WHERE event_type = 'view'), 0),
				COALESCE(AVG(watch_duration_ms) FILTER (WHERE event_type = 'view'), 0)
			FROM feed_events
			WHERE content_id = ANY($1) AND content_type = $2
			  AND created_at > NOW() - INTERVAL '90 days'
			GROUP BY content_id`, pq.Array(ids), typ)
		if err != nil {
			log.Printf("warmContentAggregates 90d batch error: %v", err)
			return // fall back to per-item queries for everything
		}
		for rows.Next() {
			var id string
			a := &contentEventAggregates{}
			if err := rows.Scan(&id, &a.ViewCount, &a.LikeCount, &a.CommentCount,
				&a.SkipCount, &a.RewatchCount, &a.ShareCount, &a.NotInterestedCount,
				&a.AvgCompletion, &a.AvgWatchMs); err != nil {
				continue
			}
			if prev, ok := aggs[id]; ok {
				*prev = *a
			}
		}
		rows.Close()

		// Batch #2: the 2-hour trending window.
		rows, err = db.Query(`
			SELECT content_id,
				COUNT(*) FILTER (WHERE event_type IN ('like','comment','share','save')),
				COUNT(*) FILTER (WHERE event_type = 'view')
			FROM feed_events
			WHERE content_id = ANY($1) AND content_type = $2
			  AND created_at > NOW() - INTERVAL '2 hours'
			GROUP BY content_id`, pq.Array(ids), typ)
		if err != nil {
			log.Printf("warmContentAggregates 2h batch error: %v", err)
			return
		}
		for rows.Next() {
			var id string
			var eng, views int
			if err := rows.Scan(&id, &eng, &views); err != nil {
				continue
			}
			if a, ok := aggs[id]; ok {
				a.RecentEng, a.RecentViews = eng, views
			}
		}
		rows.Close()

		for id, a := range aggs {
			contentAggCache.Set(contentAggKey(typ, id), a)
		}
	}
}
