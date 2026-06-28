package main

import (
	"strconv"
	"strings"
	"time"
)

// ─────────────────────────────────────────────────────────────────────────────
// pgvector ANN retrieval (optional acceleration)
//
// The legacy embedding source (sourceEmbeddingNeighbors) cosine-reranks the
// RECENCY pool, so it can never surface a semantically-similar OLDER item — the
// whole point of embedding retrieval — and overlaps almost entirely with the
// recency lane. With pgvector we store each challenge's stable content vector in
// a `vector(32)` column and let Postgres do a true nearest-neighbor scan over
// the WHOLE catalog: `ORDER BY embedding <=> $userVec`.
//
// Everything here is OPTIONAL and fault-tolerant. If the `vector` extension
// can't be created (e.g. the DB role lacks permission), pgvectorAvailable stays
// false and sourceEmbeddingNeighbors transparently falls back to the in-process
// cosine rerank — nothing breaks.
//
// At small catalog sizes a sequential scan over a few thousand 32-d vectors is
// fast, so we deliberately skip an IVFFlat/HNSW index for now; add one
// (USING ivfflat (embedding vector_cosine_ops)) once the catalog grows.
// ─────────────────────────────────────────────────────────────────────────────

// pgvectorAvailable is set by runMigrations() once the extension + column are
// confirmed. Read by the ANN source and the backfill worker to no-op cleanly.
var pgvectorAvailable bool

// vecLiteral formats a float vector as a pgvector text literal: "[v1,v2,...]".
func vecLiteral(v []float64) string {
	var b strings.Builder
	b.Grow(len(v)*9 + 2)
	b.WriteByte('[')
	for i, x := range v {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(strconv.FormatFloat(x, 'f', 6, 64))
	}
	b.WriteByte(']')
	return b.String()
}

// startEmbeddingBackfillWorker populates challenges.embedding for any rows that
// don't have one yet — both existing content (one-time) and newly created
// challenges (picked up within a tick). Runs once on boot, then every 5 min.
func startEmbeddingBackfillWorker() {
	if !pgvectorAvailable {
		return
	}
	go func() {
		backfillChallengeEmbeddings(500)
		tk := time.NewTicker(5 * time.Minute)
		defer tk.Stop()
		for range tk.C {
			backfillChallengeEmbeddings(500)
		}
	}()
}

// backfillChallengeEmbeddings computes + stores the stable content vector for up
// to maxRows challenges that are missing one. Bounded per call so a large cold
// catalog is filled over several ticks rather than one long transaction.
func backfillChallengeEmbeddings(maxRows int) {
	if !pgvectorAvailable || db == nil {
		return
	}
	rows, err := db.Query(`SELECT id FROM challenges WHERE embedding IS NULL LIMIT $1`, maxRows)
	if err != nil {
		return
	}
	var ids []string
	for rows.Next() {
		var id string
		if rows.Scan(&id) == nil {
			ids = append(ids, id)
		}
	}
	rows.Close()

	for _, id := range ids {
		cs := getContentScore(id, "challenge")
		emotions := getContentEmotions(id, "challenge")
		vec := l2norm(buildContentEmbeddingStable(cs, emotions))
		// Skip all-zero vectors: cosine distance (<=>) is undefined against a
		// zero vector. Leaving embedding NULL excludes the row from ANN until it
		// has a usable fingerprint.
		if userEmbeddingIsCold(vec) {
			continue
		}
		if _, err := db.Exec(`UPDATE challenges SET embedding = $1::vector WHERE id = $2`,
			vecLiteral(vec), id); err != nil {
			// Likely the column/extension vanished — stop hammering and let the
			// next tick re-evaluate.
			return
		}
	}
}

// sourceEmbeddingANN returns the K challenges whose stored vector is nearest to
// the user's EMA embedding, across the whole catalog. Returns nil when pgvector
// is unavailable or the user vector is cold (callers fall back).
func sourceEmbeddingANN(userID string, limit int) []HomeFeedItem {
	if !pgvectorAvailable || db == nil || limit <= 0 {
		return nil
	}
	uv := getUserEmbedding(userID)
	if userEmbeddingIsCold(uv) {
		return nil
	}
	// The stored ANN vectors are the STABLE embedding (buildContentEmbeddingStable
	// — dynamic recency/popularity dims excluded), but the user EMA was trained on
	// full vectors that INCLUDE those dims (applyDynamicEmbedFeatures sets v[2]
	// recency, v[3] popularity). Project the query into the same stable subspace
	// (zero dims 2-3, re-normalize) so the cosine ANN compares like dimensions;
	// otherwise the user's recency/popularity components match absent counterparts
	// and distort similarity. Recency/popularity already enter ranking via the
	// freshness/trending bonuses. Copy first — never mutate the cached EMA.
	if len(uv) > 3 {
		q := append([]float64(nil), uv...)
		q[2] = 0
		q[3] = 0
		uv = l2norm(q)
	}
	rows, err := db.Query(`
		SELECT c.id, c.creator_id, u.username, u.league, c.video_url,
			c.thumbnail_url, c.prefix, c.subject, c.visibility, c.status,
			c.views, c.likes_count, c.created_at, c.response_count
		FROM challenges c
		JOIN users u ON c.creator_id = u.id
		WHERE c.embedding IS NOT NULL
		  AND c.visibility = 'arena'
		  AND c.status IN ('open','active','completed')
		  AND c.creator_id != CAST($1 AS INT)
		ORDER BY c.embedding <=> $2::vector
		LIMIT $3`, userID, vecLiteral(uv), limit)
	if err != nil {
		return nil
	}
	defer rows.Close()
	items := make([]HomeFeedItem, 0, limit)
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
		ch.ResponseCount = respCount
		ch.CreatedAt = createdAt.Format(time.RFC3339)
		items = append(items, HomeFeedItem{Type: "challenge", Challenge: &ch})
	}
	return items
}
