package main

import (
	"encoding/json"
	"log"
	"math"
	"net/http"
	"time"
)

// ════════════════════════════════════════════════════════════════════════════════
// COLLABORATIVE FILTERING
// ════════════════════════════════════════════════════════════════════════════════
//
// WHY: Our current algorithm is "content-based" — it matches user preferences to
// content attributes. Collaborative filtering adds "users like you also liked X."
// This catches recommendations that content-based misses.
//
// Example: User A and User B both love comedy+sports and skip horror. Content-based
// knows this. But User A also loves a specific creator that User B hasn't seen.
// Collaborative filtering says "User B, you're similar to User A, try this creator."
//
// HOW:
// 1. Compute user similarity using cosine similarity of category affinity vectors
// 2. Store top-K similar users per user in a table
// 3. When scoring content, add a bonus if similar users engaged with it
//
// SCALING:
// - At 100 users: O(100²) = 10,000 comparisons (instant)
// - At 10,000 users: O(100M) — needs batching but still manageable
// - At 1M users: Need approximate nearest neighbors (ANN). Switch to vector DB.
// - The table structure supports all scales. Only the computation method changes.

// UserSimilarity stores the precomputed similarity between two users.
type UserSimilarity struct {
	UserID    string  `json:"userId"`
	SimilarID string  `json:"similarId"`
	Score     float64 `json:"score"` // 0-1, cosine similarity
}

// computeAllSimilarities recomputes the user similarity table.
// Called periodically (e.g., every 6 hours) as a background job.
// At <10K users this runs in seconds. At scale, switch to batch/async.
func computeAllSimilarities() {
	start := time.Now()

	// Load all user profiles with category affinity
	rows, err := db.Query(`
		SELECT user_id, category_affinity FROM user_profiles
		WHERE event_count >= $1`, coldStartThreshold)
	if err != nil {
		log.Printf("Collaborative: failed to load profiles: %v", err)
		return
	}
	defer rows.Close()

	type userVector struct {
		id       string
		affinity map[string]float64
	}

	var users []userVector
	for rows.Next() {
		var id string
		var catJSON []byte
		rows.Scan(&id, &catJSON)
		var affinity map[string]float64
		json.Unmarshal(catJSON, &affinity)
		if len(affinity) > 0 {
			users = append(users, userVector{id: id, affinity: affinity})
		}
	}

	if len(users) < 2 {
		log.Printf("Collaborative: only %d users with profiles, skipping", len(users))
		return
	}

	// Compute pairwise cosine similarity
	// For each user, keep top 20 most similar users
	const maxSimilar = 20

	// Clear old similarities
	db.Exec(`DELETE FROM user_similarities`)

	for i, userA := range users {
		type scored struct {
			id    string
			score float64
		}
		var sims []scored

		for j, userB := range users {
			if i == j {
				continue
			}
			sim := cosineSimilarity(userA.affinity, userB.affinity)
			if sim > 0.1 { // Only store meaningful similarities
				sims = append(sims, scored{id: userB.id, score: sim})
			}
		}

		// Sort by similarity (descending) and keep top K
		for k := 0; k < len(sims) && k < maxSimilar; k++ {
			for l := k + 1; l < len(sims); l++ {
				if sims[l].score > sims[k].score {
					sims[k], sims[l] = sims[l], sims[k]
				}
			}
		}

		limit := maxSimilar
		if len(sims) < limit {
			limit = len(sims)
		}

		for _, s := range sims[:limit] {
			db.Exec(`
				INSERT INTO user_similarities (user_id, similar_user_id, similarity_score, computed_at)
				VALUES ($1, $2, $3, NOW())
				ON CONFLICT (user_id, similar_user_id) DO UPDATE SET
					similarity_score = $3, computed_at = NOW()`,
				userA.id, s.id, s.score)
		}
	}

	log.Printf("Collaborative: computed similarities for %d users in %v", len(users), time.Since(start))
}

// cosineSimilarity computes the cosine similarity between two category affinity maps.
// Returns 0-1 where 1 = identical preferences, 0 = completely different.
func cosineSimilarity(a, b map[string]float64) float64 {
	// Get all categories from both users
	allCats := make(map[string]bool)
	for k := range a {
		allCats[k] = true
	}
	for k := range b {
		allCats[k] = true
	}

	var dotProduct, normA, normB float64
	for cat := range allCats {
		va := a[cat]
		vb := b[cat]
		dotProduct += va * vb
		normA += va * va
		normB += vb * vb
	}

	if normA == 0 || normB == 0 {
		return 0
	}
	return dotProduct / (math.Sqrt(normA) * math.Sqrt(normB))
}

// getSimilarUsers returns the top similar users for a given user.
func getSimilarUsers(userID string) []UserSimilarity {
	rows, err := db.Query(`
		SELECT similar_user_id, similarity_score
		FROM user_similarities
		WHERE user_id = $1
		ORDER BY similarity_score DESC
		LIMIT 20`, userID)
	if err != nil {
		return nil
	}
	defer rows.Close()

	var result []UserSimilarity
	for rows.Next() {
		var sim UserSimilarity
		sim.UserID = userID
		rows.Scan(&sim.SimilarID, &sim.Score)
		result = append(result, sim)
	}
	return result
}

// collabBonusCache memoizes the per-(user, content) collaborative bonus. This is
// called once PER CANDIDATE in the scoring hot loop, so the raw SQL was N queries
// per feed; a 2-min cache collapses repeat scoring of the same content across
// pages/refreshes (and popular content across users' re-requests).
var collabBonusCache = NewSignalCache[float64](2 * time.Minute)

// getCollaborativeBonus returns a score bonus for content that similar users engaged with.
// This is the "users like you also liked this" signal.
func getCollaborativeBonus(userID, contentID, contentType string) float64 {
	if db == nil {
		return 0
	}
	ck := userID + ":" + contentType + ":" + contentID
	if v, ok := collabBonusCache.Get(ck); ok {
		return v
	}
	// Check if similar users engaged with this content
	var weightedEngagement float64
	err := db.QueryRow(`
		SELECT COALESCE(SUM(us.similarity_score * sub.engagement), 0)
		FROM user_similarities us
		JOIN LATERAL (
			SELECT CASE fe.event_type
				WHEN 'share' THEN 1.0
				WHEN 'rewatch' THEN 0.8
				WHEN 'save' THEN 0.6
				WHEN 'like' THEN 0.4
				WHEN 'comment' THEN 0.4
				WHEN 'view' THEN CASE WHEN fe.completion_rate > 0.7 THEN 0.2 ELSE 0 END
				ELSE 0
			END as engagement
			FROM feed_events fe
			WHERE fe.user_id = us.similar_user_id
			AND fe.content_id = $2 AND fe.content_type = $3
			AND fe.event_type IN ('share','rewatch','save','like','comment','view')
			ORDER BY fe.created_at DESC LIMIT 1
		) sub ON true
		WHERE us.user_id = $1`, userID, contentID, contentType).Scan(&weightedEngagement)

	if err != nil {
		return 0
	}
	// Normalize: cap at 0.15 bonus
	bonus := math.Min(0.15, weightedEngagement*0.1)
	collabBonusCache.Set(ck, bonus)
	return bonus
}

// startSimilarityWorker runs the similarity computation periodically.
func startSimilarityWorker() {
	go func() {
		// Initial computation after 1 minute (let DB settle)
		time.Sleep(1 * time.Minute)
		computeAllSimilarities()

		// Then every 6 hours
		ticker := time.NewTicker(6 * time.Hour)
		defer ticker.Stop()
		for range ticker.C {
			computeAllSimilarities()
		}
	}()
}

// SimilarUsersHandler returns the similar users for debugging/admin.
// GET /api/v1/users/similar?userId=X
func SimilarUsersHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "OPTIONS" {
		w.WriteHeader(http.StatusOK)
		return
	}

	userID := authUserID(r)
	if userID == "" {
		http.Error(w, `{"error":"userId required"}`, http.StatusBadRequest)
		return
	}

	sims := getSimilarUsers(userID)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"userId":       userID,
		"similarUsers": sims,
	})
}
