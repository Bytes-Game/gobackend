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
// The old fetchCandidates pulled one bucket (50/50 recent challenges+posts)
// and passed it to the ranker. That worked, but the ranker could only rank
// what retrieval fed it — so users never saw things the single SQL recency
// query didn't surface.
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
	"recency":         0.30,
	"trending":        0.15,
	"trendingRealtime": 0.15,
	"follow":          0.15,
	"collab":          0.10,
	"embedding":       0.15,
}

// multiSourceFetch runs all default sources in parallel and merges results,
// deduping by (type, id). Per-source budget = totalLimit * weight.
func multiSourceFetch(userID string, totalLimit int) []HomeFeedItem {
	if totalLimit <= 0 {
		return nil
	}

	sources := buildDefaultSources()
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
		}
	}

	// Weighted round-robin interleave so no single source dominates the head.
	return interleaveBySource(bySource, sources, totalLimit)
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

func sourceTrending(userID string, limit int) []HomeFeedItem {
	if db == nil {
		return nil
	}
	items := make([]HomeFeedItem, 0, limit)
	rows, err := db.Query(`
		SELECT c.id, c.creator_id, u.username, u.league, c.video_url,
			c.thumbnail_url, c.prefix, c.subject, c.visibility, c.status,
			c.views, COALESCE(cl.likes, 0), c.created_at
		FROM challenges c
		JOIN users u ON c.creator_id = u.id
		LEFT JOIN (SELECT challenge_id, COUNT(*) AS likes FROM challenge_likes GROUP BY challenge_id) cl
			ON cl.challenge_id = c.id
		WHERE c.visibility = 'arena'
		  AND c.status IN ('open','active','completed')
		  AND c.created_at > NOW() - INTERVAL '48 hours'
		  AND c.creator_id != CAST($1 AS INT)
		ORDER BY (c.views + COALESCE(cl.likes,0)*5) DESC
		LIMIT $2`, userID, limit)
	if err != nil {
		return nil
	}
	defer rows.Close()
	for rows.Next() {
		var ch Challenge
		var creatorID, views, likes int
		var createdAt time.Time
		if err := rows.Scan(&ch.ID, &creatorID, &ch.CreatorUsername, &ch.CreatorLeague,
			&ch.VideoURL, &ch.ThumbnailURL, &ch.Prefix, &ch.Subject,
			&ch.Visibility, &ch.Status, &views, &likes, &createdAt); err != nil {
			continue
		}
		ch.CreatorID = strconv.Itoa(creatorID)
		ch.Views = views
		ch.Likes = likes
		ch.CreatedAt = createdAt.Format(time.RFC3339)
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
	items := make([]HomeFeedItem, 0, limit)
	rows, err := db.Query(`
		SELECT c.id, c.creator_id, u.username, u.league, c.video_url,
			c.thumbnail_url, c.prefix, c.subject, c.visibility, c.status,
			c.views, COALESCE(cl.likes, 0), c.created_at
		FROM challenges c
		JOIN users u ON c.creator_id = u.id
		JOIN follows f ON f.followed_id = c.creator_id
		LEFT JOIN (SELECT challenge_id, COUNT(*) AS likes FROM challenge_likes GROUP BY challenge_id) cl
			ON cl.challenge_id = c.id
		WHERE f.follower_id = CAST($1 AS INT)
		  AND c.created_at > NOW() - INTERVAL '7 days'
		  AND c.visibility IN ('arena','friends')
		ORDER BY c.created_at DESC
		LIMIT $2`, userID, limit)
	if err != nil {
		return nil
	}
	defer rows.Close()
	for rows.Next() {
		var ch Challenge
		var creatorID, views, likes int
		var createdAt time.Time
		if err := rows.Scan(&ch.ID, &creatorID, &ch.CreatorUsername, &ch.CreatorLeague,
			&ch.VideoURL, &ch.ThumbnailURL, &ch.Prefix, &ch.Subject,
			&ch.Visibility, &ch.Status, &views, &likes, &createdAt); err != nil {
			continue
		}
		ch.CreatorID = strconv.Itoa(creatorID)
		ch.Views = views
		ch.Likes = likes
		ch.CreatedAt = createdAt.Format(time.RFC3339)
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
	items := make([]HomeFeedItem, 0, limit)
	rows, err := db.Query(`
		SELECT DISTINCT ON (c.id)
			c.id, c.creator_id, u.username, u.league, c.video_url,
			c.thumbnail_url, c.prefix, c.subject, c.visibility, c.status,
			c.views, COALESCE(cl.likes, 0), c.created_at
		FROM user_similarities us
		JOIN feed_events fe ON fe.user_id = us.similar_user_id::text
		JOIN challenges c ON c.id = fe.content_id::int AND fe.content_type = 'challenge'
		JOIN users u ON c.creator_id = u.id
		LEFT JOIN (SELECT challenge_id, COUNT(*) AS likes FROM challenge_likes GROUP BY challenge_id) cl
			ON cl.challenge_id = c.id
		WHERE us.user_id = $1
		  AND us.similarity_score > 0.3
		  AND fe.event_type IN ('like','complete','share','save')
		  AND c.created_at > NOW() - INTERVAL '14 days'
		  AND c.creator_id != CAST($1 AS INT)
		ORDER BY c.id, c.created_at DESC
		LIMIT $2`, userID, limit)
	if err != nil {
		return nil
	}
	defer rows.Close()
	for rows.Next() {
		var ch Challenge
		var creatorID, views, likes int
		var createdAt time.Time
		if err := rows.Scan(&ch.ID, &creatorID, &ch.CreatorUsername, &ch.CreatorLeague,
			&ch.VideoURL, &ch.ThumbnailURL, &ch.Prefix, &ch.Subject,
			&ch.Visibility, &ch.Status, &views, &likes, &createdAt); err != nil {
			continue
		}
		ch.CreatorID = strconv.Itoa(creatorID)
		ch.Views = views
		ch.Likes = likes
		ch.CreatedAt = createdAt.Format(time.RFC3339)
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
