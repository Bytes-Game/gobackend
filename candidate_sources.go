package main

import (
	"fmt"
	"strconv"
	"sync"
	"time"
)

// ─────────────────────────────────────────────────────────────────────────────
// MULTI-SOURCE CANDIDATE GENERATION
//
// The old fetchCandidates pulled one bucket (recent challenges + recent
// posts) and passed it to the ranker. That worked, but the ranker could only
// rank what retrieval fed it — so users never saw things the single SQL
// recency query didn't surface. (Posts have since been retired; the recency
// source now splits its budget ~70/30 across battles and shorts.)
//
// This module runs SEVERAL retrievers in parallel (recency, trending,
// following-graph, collaborative, embedding-neighbors) and merges their
// outputs with weighted interleaving. The ranker then scores the union.
//
// Sources are fail-open: any source that errors contributes zero candidates
// but does not abort the whole feed. Total candidate budget is distributed
// across sources by weight.
// ─────────────────────────────────────────────────────────────────────────────

// candidateSource is a single retrieval strategy. `weight` is its share of
// the total candidate budget (sum of all active weights should be ~1.0).
type candidateSource struct {
	name   string
	weight float64
	fetch  func(userID string, limit int) []HomeFeedItem
}

// defaultSourceWeights is the production mix. Tuned so recency dominates
// (our freshness decay expects recent candidates in pool) but other sources
// get enough budget to surface non-obvious picks.
//
// The realtime trending source catches viral spikes within minutes (vs
// the 48h SQL trending which is daily-batch); it gets a meaningful slice
// at the expense of pulling 5pp from each of recency / trending / collab.
var defaultSourceWeights = map[string]float64{
	"recency":          0.25,
	"trending":         0.10,
	"trendingRealtime": 0.15,
	"follow":           0.15,
	"collab":           0.10,
	"embedding":        0.15,
	// searchAffinity uses the user's recent search history (captured by
	// RecordSearchQuery) as a query against Meilisearch, then surfaces the
	// best-matching live challenges as candidates. Closes the loop between
	// "user explicitly asked for X" and "X shows up in their feed". Pulls
	// 5pp from recency and 5pp from trending — those two bands have plenty
	// of redundancy with the other sources, the search lane has none.
	"searchAffinity": 0.10,
}

// multiSourceFetch runs all default sources in parallel and merges results,
// deduping by (type, id). Per-source budget = totalLimit * weight.
//
// This wrapper preserves the legacy signature for callers that don't carry
// cohort context. Production should call multiSourceFetchForCohort so the
// learned per-cohort blending weights are applied.
func multiSourceFetch(userID string, totalLimit int) []HomeFeedItem {
	items, _ := multiSourceFetchForCohort(userID, totalLimit, "")
	return items
}

// multiSourceFetchForCohort is the cohort-aware variant. Returns items AND
// a per-item source-attribution map (keyed by item type+id) so the LTR
// stash can record which source produced each impression. Reward observation
// at outcome time then credits the right source.
//
// When cohort=="" we fall back to defaultSourceWeights (legacy behavior).
// Otherwise effectiveSourceWeights(cohort) wraps the learned blending.
func multiSourceFetchForCohort(userID string, totalLimit int, cohort Cohort) ([]HomeFeedItem, map[string]string) {
	if totalLimit <= 0 {
		return nil, nil
	}

	sources := buildSourcesForCohort(cohort)
	type result struct {
		name  string
		items []HomeFeedItem
	}
	resCh := make(chan result, len(sources))
	var wg sync.WaitGroup

	for _, s := range sources {
		budget := int(float64(totalLimit) * s.weight)
		if budget < 2 {
			budget = 2
		}
		wg.Add(1)
		go func(src candidateSource, n int) {
			defer wg.Done()
			defer func() {
				// Never let a retriever panic take down the feed.
				if r := recover(); r != nil {
					if metricCandidateSource != nil {
						metricCandidateSource.WithLabelValues(src.name, "panic").Inc()
					}
					resCh <- result{name: src.name}
				}
			}()
			items := src.fetch(userID, n)
			if metricCandidateSource != nil {
				status := "ok"
				if len(items) == 0 {
					status = "empty"
				}
				metricCandidateSource.WithLabelValues(src.name, status).Add(float64(len(items)))
			}
			resCh <- result{name: src.name, items: items}
		}(s, budget)
	}

	go func() { wg.Wait(); close(resCh) }()

	// Dedup by member key.
	seen := make(map[string]bool, totalLimit)
	bySource := make(map[string][]HomeFeedItem, len(sources))
	itemSource := make(map[string]string, totalLimit)
	for r := range resCh {
		for _, it := range r.items {
			id := getItemID(it)
			if id == "" {
				continue
			}
			key := it.Type + ":" + id
			if seen[key] {
				continue
			}
			seen[key] = true
			bySource[r.name] = append(bySource[r.name], it)
			itemSource[key] = r.name
		}
	}

	// Weighted round-robin interleave so no single source dominates the head.
	merged := interleaveBySource(bySource, sources, totalLimit)
	return merged, itemSource
}

// buildSourcesForCohort returns the source list with per-cohort weights
// applied when cohort is non-empty; falls back to defaults otherwise.
func buildSourcesForCohort(cohort Cohort) []candidateSource {
	if cohort == "" {
		return buildDefaultSources()
	}
	weights := effectiveSourceWeights(cohort)
	return []candidateSource{
		{name: "recency", weight: weights["recency"], fetch: sourceRecency},
		{name: "trending", weight: weights["trending"], fetch: sourceTrending},
		{name: "trendingRealtime", weight: weights["trendingRealtime"], fetch: sourceTrendingRealtime},
		{name: "follow", weight: weights["follow"], fetch: sourceFollowGraph},
		{name: "collab", weight: weights["collab"], fetch: sourceCollaborative},
		{name: "embedding", weight: weights["embedding"], fetch: sourceEmbeddingNeighbors},
		{name: "searchAffinity", weight: weights["searchAffinity"], fetch: sourceSearchAffinity},
	}
}

// interleaveBySource pulls from each source in a round-robin, respecting
// per-source weight so recency still leads but other sources get early slots.
func interleaveBySource(bySource map[string][]HomeFeedItem, sources []candidateSource, limit int) []HomeFeedItem {
	out := make([]HomeFeedItem, 0, limit)
	idx := make(map[string]int, len(sources))
	// Repeat: each round, each source gets ceil(weight * roundSize) picks.
	const roundSize = 10
	for len(out) < limit {
		progress := false
		for _, s := range sources {
			takePerRound := int(s.weight*roundSize + 0.5)
			if takePerRound < 1 {
				takePerRound = 1
			}
			for k := 0; k < takePerRound && len(out) < limit; k++ {
				arr := bySource[s.name]
				if idx[s.name] >= len(arr) {
					break
				}
				out = append(out, arr[idx[s.name]])
				idx[s.name]++
				progress = true
			}
		}
		if !progress {
			break
		}
	}
	return out
}

// buildDefaultSources wires the default retriever set. Kept as a builder so
// tests can inject fakes.
func buildDefaultSources() []candidateSource {
	return []candidateSource{
		{name: "recency", weight: defaultSourceWeights["recency"], fetch: sourceRecency},
		{name: "trending", weight: defaultSourceWeights["trending"], fetch: sourceTrending},
		{name: "trendingRealtime", weight: defaultSourceWeights["trendingRealtime"], fetch: sourceTrendingRealtime},
		{name: "follow", weight: defaultSourceWeights["follow"], fetch: sourceFollowGraph},
		{name: "collab", weight: defaultSourceWeights["collab"], fetch: sourceCollaborative},
		{name: "embedding", weight: defaultSourceWeights["embedding"], fetch: sourceEmbeddingNeighbors},
		{name: "searchAffinity", weight: defaultSourceWeights["searchAffinity"], fetch: sourceSearchAffinity},
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// SOURCE 1: Recency — the legacy path, kept as default.
// ─────────────────────────────────────────────────────────────────────────────

func sourceRecency(userID string, limit int) []HomeFeedItem {
	return fetchCandidates(userID, limit)
}

// ─────────────────────────────────────────────────────────────────────────────
// SOURCE 2: Trending — engagement-weighted within the last 48h.
// ─────────────────────────────────────────────────────────────────────────────

// candidateSourceWindows holds the strict-then-widening fallback ladder
// each source tries when the prior window returned nothing. This is the
// architectural defense against any kind of sparse-data bug — seed data
// going stale, a quiet weekend, a brand-new region, etc. The strict
// window stays first for the typical case; wider windows kick in only
// when the strict one has zero results.
var candidateSourceWindows = map[string][]string{
	"trending": {"48 hours", "7 days", "30 days", "365 days"},
	"follow":   {"7 days", "30 days", "180 days", "365 days"},
	"collab":   {"14 days", "60 days", "180 days", "365 days"},
	"recency":  {"14 days", "60 days", "180 days", "365 days"},
}

func sourceTrending(userID string, limit int) []HomeFeedItem {
	if db == nil {
		return nil
	}
	for _, window := range candidateSourceWindows["trending"] {
		items := sourceTrendingWindowed(userID, limit, window)
		if len(items) > 0 {
			return items
		}
	}
	return nil
}

// sourceTrendingWindowed parametrizes the recency cutoff so the wrapper
// can try progressively wider windows. Returns immediately when the query
// has any rows.
func sourceTrendingWindowed(userID string, limit int, window string) []HomeFeedItem {
	items := make([]HomeFeedItem, 0, limit)
	// ResponseCount is fetched via correlated subquery so the ranker's
	// battleBoost can correctly favor battles over shorts. Without this
	// the field defaults to zero and trending battles look like shorts.
	rows, err := db.Query(`
		SELECT c.id, c.creator_id, u.username, u.league, c.video_url,
			c.thumbnail_url, c.prefix, c.subject, c.visibility, c.status,
			c.views, COALESCE(cl.likes, 0), c.created_at,
			(SELECT COUNT(*) FROM challenge_responses WHERE challenge_id = c.id)
		FROM challenges c
		JOIN users u ON c.creator_id = u.id
		LEFT JOIN (SELECT challenge_id, COUNT(*) AS likes FROM challenge_likes GROUP BY challenge_id) cl
			ON cl.challenge_id = c.id
		WHERE c.visibility = 'arena'
		  AND c.status IN ('open','active','completed')
		  AND c.created_at > NOW() - ($3::text)::interval
		  AND c.creator_id != CAST($1 AS INT)
		ORDER BY (c.views + COALESCE(cl.likes,0)*5) DESC
		LIMIT $2`, userID, limit, window)
	if err != nil {
		return nil
	}
	defer rows.Close()
	for rows.Next() {
		var ch Challenge
		var creatorID, views, likes, respCount int
		var createdAt time.Time
		if err := rows.Scan(&ch.ID, &creatorID, &ch.CreatorUsername, &ch.CreatorLeague,
			&ch.VideoURL, &ch.ThumbnailURL, &ch.Prefix, &ch.Subject,
			&ch.Visibility, &ch.Status, &views, &likes, &createdAt, &respCount); err != nil {
			continue
		}
		ch.CreatorID = strconv.Itoa(creatorID)
		ch.Views = views
		ch.Likes = likes
		ch.CreatedAt = createdAt.Format(time.RFC3339)
		ch.ResponseCount = respCount
		items = append(items, HomeFeedItem{Type: "challenge", Challenge: &ch})
	}
	return items
}

// ─────────────────────────────────────────────────────────────────────────────
// SOURCE 3: Follow-graph — recent content from creators the user follows.
// ─────────────────────────────────────────────────────────────────────────────

func sourceFollowGraph(userID string, limit int) []HomeFeedItem {
	if db == nil {
		return nil
	}
	for _, window := range candidateSourceWindows["follow"] {
		items := sourceFollowGraphWindowed(userID, limit, window)
		if len(items) > 0 {
			return items
		}
	}
	return nil
}

func sourceFollowGraphWindowed(userID string, limit int, window string) []HomeFeedItem {
	items := make([]HomeFeedItem, 0, limit)
	// ResponseCount via correlated subquery — same reason as sourceTrendingWindowed:
	// the ranker's battleBoost needs accurate response counts on every candidate.
	rows, err := db.Query(`
		SELECT c.id, c.creator_id, u.username, u.league, c.video_url,
			c.thumbnail_url, c.prefix, c.subject, c.visibility, c.status,
			c.views, COALESCE(cl.likes, 0), c.created_at,
			(SELECT COUNT(*) FROM challenge_responses WHERE challenge_id = c.id)
		FROM challenges c
		JOIN users u ON c.creator_id = u.id
		JOIN follows f ON f.followed_id = c.creator_id
		LEFT JOIN (SELECT challenge_id, COUNT(*) AS likes FROM challenge_likes GROUP BY challenge_id) cl
			ON cl.challenge_id = c.id
		WHERE f.follower_id = CAST($1 AS INT)
		  AND c.created_at > NOW() - ($3::text)::interval
		  AND c.visibility IN ('arena','friends')
		ORDER BY c.created_at DESC
		LIMIT $2`, userID, limit, window)
	if err != nil {
		return nil
	}
	defer rows.Close()
	for rows.Next() {
		var ch Challenge
		var creatorID, views, likes, respCount int
		var createdAt time.Time
		if err := rows.Scan(&ch.ID, &creatorID, &ch.CreatorUsername, &ch.CreatorLeague,
			&ch.VideoURL, &ch.ThumbnailURL, &ch.Prefix, &ch.Subject,
			&ch.Visibility, &ch.Status, &views, &likes, &createdAt, &respCount); err != nil {
			continue
		}
		ch.CreatorID = strconv.Itoa(creatorID)
		ch.Views = views
		ch.Likes = likes
		ch.CreatedAt = createdAt.Format(time.RFC3339)
		ch.ResponseCount = respCount
		items = append(items, HomeFeedItem{Type: "challenge", Challenge: &ch})
	}
	return items
}

// ─────────────────────────────────────────────────────────────────────────────
// SOURCE 4: Collaborative — content engaged by users similar to this user.
// Relies on user_similarities table populated by computeAllSimilarities().
// ─────────────────────────────────────────────────────────────────────────────

func sourceCollaborative(userID string, limit int) []HomeFeedItem {
	if db == nil {
		return nil
	}
	for _, window := range candidateSourceWindows["collab"] {
		items := sourceCollaborativeWindowed(userID, limit, window)
		if len(items) > 0 {
			return items
		}
	}
	return nil
}

func sourceCollaborativeWindowed(userID string, limit int, window string) []HomeFeedItem {
	items := make([]HomeFeedItem, 0, limit)
	// ResponseCount via correlated subquery — same reason as the other sources.
	rows, err := db.Query(`
		SELECT DISTINCT ON (c.id)
			c.id, c.creator_id, u.username, u.league, c.video_url,
			c.thumbnail_url, c.prefix, c.subject, c.visibility, c.status,
			c.views, COALESCE(cl.likes, 0), c.created_at,
			(SELECT COUNT(*) FROM challenge_responses WHERE challenge_id = c.id)
		FROM user_similarities us
		JOIN feed_events fe ON fe.user_id = us.similar_user_id::text
		JOIN challenges c ON c.id = fe.content_id::int AND fe.content_type = 'challenge'
		JOIN users u ON c.creator_id = u.id
		LEFT JOIN (SELECT challenge_id, COUNT(*) AS likes FROM challenge_likes GROUP BY challenge_id) cl
			ON cl.challenge_id = c.id
		WHERE us.user_id = $1
		  AND us.similarity_score > 0.3
		  AND fe.event_type IN ('like','complete','share','save')
		  AND c.created_at > NOW() - ($3::text)::interval
		  AND c.creator_id != CAST($1 AS INT)
		ORDER BY c.id, c.created_at DESC
		LIMIT $2`, userID, limit, window)
	if err != nil {
		return nil
	}
	defer rows.Close()
	for rows.Next() {
		var ch Challenge
		var creatorID, views, likes, respCount int
		var createdAt time.Time
		if err := rows.Scan(&ch.ID, &creatorID, &ch.CreatorUsername, &ch.CreatorLeague,
			&ch.VideoURL, &ch.ThumbnailURL, &ch.Prefix, &ch.Subject,
			&ch.Visibility, &ch.Status, &views, &likes, &createdAt, &respCount); err != nil {
			continue
		}
		ch.CreatorID = strconv.Itoa(creatorID)
		ch.Views = views
		ch.Likes = likes
		ch.CreatedAt = createdAt.Format(time.RFC3339)
		ch.ResponseCount = respCount
		items = append(items, HomeFeedItem{Type: "challenge", Challenge: &ch})
	}
	return items
}

// ─────────────────────────────────────────────────────────────────────────────
// SOURCE 5: Embedding neighbors — items whose vector is most similar to the
// user's EMA embedding. Done as a two-step: pull a wider candidate pool
// with a coarse SQL filter (recent, not by self), score by cosine, take top-K.
// Graceful degrade: cold user (zero vector) returns nothing so other sources
// fill the budget.
// ─────────────────────────────────────────────────────────────────────────────

func sourceEmbeddingNeighbors(userID string, limit int) []HomeFeedItem {
	if db == nil {
		return nil
	}
	uv := getUserEmbedding(userID)
	if userEmbeddingIsCold(uv) {
		return nil
	}
	// Pull a coarse 10x pool to score.
	pool := fetchCandidates(userID, limit*10)
	if len(pool) == 0 {
		return nil
	}
	type scored struct {
		item HomeFeedItem
		s    float64
	}
	out := make([]scored, 0, len(pool))
	for _, it := range pool {
		id := getItemID(it)
		if id == "" {
			continue
		}
		cs := getContentScore(id, it.Type)
		emotions := getContentEmotions(id, it.Type)
		cv := getOrBuildContentEmbedding(cs, emotions)
		out = append(out, scored{item: it, s: cosineSim(uv, cv)})
	}
	// Partial-sort: bubble the top-limit to the front. For small limit this
	// is cheaper than full sort.
	for i := 0; i < limit && i < len(out); i++ {
		best := i
		for j := i + 1; j < len(out); j++ {
			if out[j].s > out[best].s {
				best = j
			}
		}
		out[i], out[best] = out[best], out[i]
	}
	if limit > len(out) {
		limit = len(out)
	}
	res := make([]HomeFeedItem, 0, limit)
	for i := 0; i < limit; i++ {
		if out[i].s <= 0 {
			continue // only keep meaningful matches
		}
		res = append(res, out[i].item)
	}
	return res
}

// unused placeholder to reserve the symbol for future ANN indexes
// (e.g. pgvector). Keeps the build stable if callers are wired ahead of
// a pluggable backend.
var _ = fmt.Sprintf

// ─────────────────────────────────────────────────────────────────────────────
// SOURCE 7: Search-affinity — Meilisearch-driven personalization signal.
//
// Closes the loop between explicit user intent (search queries they typed
// into the Search tab) and the For You feed. Without this lane, a user who
// searched "kickflip" yesterday only sees kickflip content if recency /
// trending / follow-graph happen to surface it. This lane *guarantees* the
// signal lands.
//
// Implementation:
//   1. Read recent queries from the request-local negative-signal cache —
//      already warmed by warmNegativeSignals at the top of SmartFeedHandler.
//   2. For each of the top 2 most-recent queries, query Meilisearch's
//      challenges index with the same multi-section pool size we use in the
//      explicit search handler.
//   3. Convert hits into HomeFeedItem and dedupe by challenge ID.
//
// Failure modes (all silent — this is one source of seven, never a hard
// dependency):
//   - Cache cold (no warm call): returns nil
//   - No recent queries: returns nil
//   - Meilisearch unconfigured: returns nil; the rest of the pipeline carries
//   - DB row missing (challenge deleted between index write and request):
//     skipped from results
// ─────────────────────────────────────────────────────────────────────────────

func sourceSearchAffinity(userID string, limit int) []HomeFeedItem {
	if userID == "" || limit <= 0 {
		return nil
	}
	ns := getNegativeSignals(userID)
	if ns == nil || len(ns.recentQueries) == 0 {
		return nil
	}
	// Take only the freshest 2 queries — older intent has decayed and
	// produces stale candidates that compete poorly in the ranker.
	queries := ns.recentQueries
	if len(queries) > 2 {
		queries = queries[:2]
	}

	// Per-query budget — half of the source's slot, rounded up so a single
	// remaining query still pulls full pool size.
	perQuery := (limit / len(queries)) + 1

	seen := make(map[string]bool)
	out := make([]HomeFeedItem, 0, limit)
	for _, q := range queries {
		if q == "" {
			continue
		}
		hits := meiliSearchChallenges(q, perQuery)
		for _, h := range hits {
			if h.Ch.ID == "" || seen[h.Ch.ID] {
				continue
			}
			seen[h.Ch.ID] = true
			ch := h.Ch
			out = append(out, HomeFeedItem{Type: "challenge", Challenge: &ch})
			if len(out) >= limit {
				return out
			}
		}
	}
	return out
}
