package main

// ─────────────────────────────────────────────────────────────────────────────
// MMR (Maximal Marginal Relevance) — diversity re-ranker.
//
// After primary scoring, a naive sort-desc can put 5 cooking posts in a row.
// MMR walks the ranked list and, at each position, picks the item that best
// balances its score with dissimilarity from what's already been chosen.
//
//   pick = argmax_i  ( λ · score_i  −  (1−λ) · max_j∈chosen sim(i, j) )
//
// λ ∈ [0,1] controls the tradeoff. λ=1 ⇒ pure score (status quo); λ=0.7
// is a solid default for feeds — the top-ranked item always wins, but
// subsequent positions prefer variety.
//
// We reuse content embeddings as the similarity signal so the re-ranker
// doesn't need a separate feature pipeline.
// ─────────────────────────────────────────────────────────────────────────────

const (
	mmrLambda  = 0.72 // how much we still reward raw score
	mmrTopK    = 40   // only re-rank the top-K — tail is left as-is
	// mmrCreatorPenalty is subtracted from a candidate's MMR score per
	// already-selected item from the same creator. Stops creator A's five
	// wildly-different videos from sweeping the head of the feed even when
	// their content embeddings are far apart (which would otherwise let
	// embedding-based MMR pass them through). Anti-loop catches the
	// runaway case AFTER three hits — this prevents it ever happening.
	mmrCreatorPenalty = 0.18
)

// applyMMR returns items re-ordered so the top of the list is both
// high-scoring AND diverse. Items beyond mmrTopK are passed through untouched.
//
// embedOf returns the content embedding for a ScoredItem — injected so tests
// can stub it without touching Redis/DB. creatorOf returns the creator ID
// for a ScoredItem (empty string if unknown); MMR uses it to penalize
// repeated creators in the chosen window.
func applyMMR(items []ScoredItem, lambda float64, topK int, embedOf func(ScoredItem) []float64) []ScoredItem {
	return applyMMRWithCreator(items, lambda, topK, embedOf, defaultCreatorOf)
}

// applyMMRWithCreator is the full-featured re-ranker that also penalizes
// repeated creators in the head window. Existing callers using applyMMR
// get the default creator extractor.
func applyMMRWithCreator(items []ScoredItem, lambda float64, topK int, embedOf func(ScoredItem) []float64, creatorOf func(ScoredItem) string) []ScoredItem {
	if len(items) <= 1 {
		return items
	}
	if topK <= 0 || topK > len(items) {
		topK = len(items)
	}
	if lambda < 0 {
		lambda = 0
	}
	if lambda > 1 {
		lambda = 1
	}

	head := items[:topK]
	tail := items[topK:]

	// Pre-compute embeddings + creator IDs once per head item.
	vecs := make([][]float64, len(head))
	creators := make([]string, len(head))
	for i := range head {
		vecs[i] = embedOf(head[i])
		if creatorOf != nil {
			creators[i] = creatorOf(head[i])
		}
	}

	// Greedy re-selection. Track creator counts in the chosen set so we can
	// soft-penalize candidates whose creator is already represented.
	chosen := make([]int, 0, topK)
	taken := make([]bool, len(head))
	creatorCount := make(map[string]int, len(head))

	// Seed with the highest-scoring item.
	bestIdx := 0
	for i := 1; i < len(head); i++ {
		if head[i].Score > head[bestIdx].Score {
			bestIdx = i
		}
	}
	chosen = append(chosen, bestIdx)
	taken[bestIdx] = true
	if c := creators[bestIdx]; c != "" {
		creatorCount[c]++
	}

	for len(chosen) < len(head) {
		bestI := -1
		bestMMR := -1e18
		for i := 0; i < len(head); i++ {
			if taken[i] {
				continue
			}
			// Max similarity to anything already chosen.
			var maxSim float64
			for _, j := range chosen {
				s := cosineSim(vecs[i], vecs[j])
				if s > maxSim {
					maxSim = s
				}
			}
			// Per-creator soft penalty: each prior pick from this creator
			// shaves mmrCreatorPenalty off the candidate's MMR score.
			creatorPen := 0.0
			if creators[i] != "" {
				creatorPen = mmrCreatorPenalty * float64(creatorCount[creators[i]])
			}
			mmr := lambda*head[i].Score - (1-lambda)*maxSim - creatorPen
			if mmr > bestMMR {
				bestMMR = mmr
				bestI = i
			}
		}
		if bestI < 0 {
			break
		}
		chosen = append(chosen, bestI)
		taken[bestI] = true
		if c := creators[bestI]; c != "" {
			creatorCount[c]++
		}
	}

	out := make([]ScoredItem, 0, len(items))
	for _, idx := range chosen {
		out = append(out, head[idx])
	}
	out = append(out, tail...)

	if metricMMRReranks != nil {
		metricMMRReranks.Inc()
	}
	return out
}

// defaultCreatorOf extracts a creator ID from a ScoredItem regardless of
// whether the underlying payload is a Post or a Challenge. Returns ""
// when the item carries no creator info (legacy or malformed entries).
func defaultCreatorOf(si ScoredItem) string {
	if si.Item.Post != nil && si.Item.Post.AuthorID != "" {
		return si.Item.Post.AuthorID
	}
	if si.Item.Challenge != nil && si.Item.Challenge.CreatorID != "" {
		return si.Item.Challenge.CreatorID
	}
	return ""
}

// applyMMRDefault uses the Redis-cached content embedding for each item.
// This is the production call site — getOrBuildContentEmbedding hits Redis
// for already-warm items so the per-rerank cost stays bounded at scale.
func applyMMRDefault(items []ScoredItem) []ScoredItem {
	return applyMMR(items, mmrLambda, mmrTopK, func(si ScoredItem) []float64 {
		id := getItemID(si.Item)
		cs := getContentScore(id, si.Item.Type)
		emotions := getContentEmotions(id, si.Item.Type)
		return getOrBuildContentEmbedding(cs, emotions)
	})
}
