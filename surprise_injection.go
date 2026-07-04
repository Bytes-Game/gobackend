package main

import (
	"math/rand"
	"strings"
)

// ─────────────────────────────────────────────────────────────────────────────
// SURPRISE INJECTION (filter-bubble defense)
//
// Personalized feeds tend to converge: the more we predict accurately, the
// more we feed users what they've already shown they want, the more their
// profile narrows, the more we predict the same. This is the filter-bubble
// trap — it boosts short-term engagement metrics but kills long-term
// retention because the world stops feeling fresh.
//
// Surprise injection breaks the loop by occasionally inserting a "wildcard"
// item: high-quality content from a category the user has zero affinity
// for. The wildcard goes deep enough that engagement isn't expected to be
// high, but it broadens the user's profile if they DO engage — and tells us
// (via skip vs. engage) whether their model needs updating.
//
// Frequency: at most 1 wildcard per ~10 head positions, gated by a small
// random probability so consecutive sessions look different. Never inject
// at position 0 (would feel jarring); always after the first 2 personalized
// items where the user is committed to the session.
//
// SAFETY:
//   - Never replace a chosen item, only inject between them
//   - Skip injection entirely if the user is in at_risk cohort (they're
//     already on the verge of churning — don't risk a wrong wildcard
//     pushing them out)
//   - Wildcard is always pulled from the bootstrap pool (high Wilson-LB
//     score) so even a "random" pick is high-quality
// ─────────────────────────────────────────────────────────────────────────────

const (
	// Probability of injecting a wildcard at any eligible position.
	surpriseInjectProbability = 0.10
	// Minimum head index where a wildcard is allowed (don't disrupt the
	// first impressions).
	surpriseMinPosition = 2
	// Maximum number of wildcards we'll inject in one feed page.
	surpriseMaxPerPage = 1
)

// applySurpriseInjection walks the composed feed and, with small probability
// at each eligible position, inserts a wildcard from the bootstrap pool.
// Wildcard categories are filtered to ones the user has near-zero affinity
// for — those are the high-info picks (a positive engagement here moves
// the model the most).
//
// userCohort gates whether to inject at all (at_risk users are spared).
// Pass a deterministic rand for testing; nil uses a session-derived seed.
func applySurpriseInjection(items []ScoredItem, profile *UserProfile, cohort Cohort, rnd *rand.Rand) []ScoredItem {
	if len(items) < surpriseMinPosition+2 {
		return items
	}
	if cohort == CohortAtRisk {
		// At-risk users get nothing surprising — they need familiar comfort
		// to recover, not novelty experiments.
		return items
	}
	if rnd == nil {
		// Cheap session-stable seed: hash of first item ID + len. Same input
		// produces same output → injection is reproducible per request, but
		// varies across requests as the input pool changes.
		seed := int64(len(items))
		if id := getItemID(items[0].Item); id != "" {
			for _, c := range id {
				seed = seed*131 + int64(c)
			}
		}
		rnd = rand.New(rand.NewSource(seed))
	}

	if rnd.Float64() > surpriseInjectProbability {
		return items
	}

	// Pull bootstrap pool (already-Wilson-LB-ranked).
	pool := fetchBootstrapPool(50)
	if len(pool) == 0 {
		return items
	}

	// Build set of categories the user already engages with strongly so we
	// can EXCLUDE them — wildcard's whole point is to surface unfamiliar
	// ground.
	// NOTE: no len(profile.CategoryAffinity) capacity hint here — that dereferences
	// profile BEFORE the nil-guard below and panics when profile is nil.
	familiarCats := make(map[string]bool)
	if profile != nil {
		for cat, score := range profile.CategoryAffinity {
			if score > 0.20 {
				familiarCats[strings.ToLower(cat)] = true
			}
		}
	}
	// Also exclude categories present in the current feed so the wildcard
	// genuinely contrasts with nearby items.
	for _, si := range items {
		id := getItemID(si.Item)
		cs := getContentScore(id, si.Item.Type)
		if cs != nil && cs.Category != "" {
			familiarCats[strings.ToLower(cs.Category)] = true
		}
	}

	// Find a wildcard pool member whose category is unfamiliar.
	var wild ScoredItem
	found := false
	for _, e := range pool {
		item, ok := loadHomeFeedItemByID(e.Type, e.ID)
		if !ok {
			continue
		}
		// Self-content guard.
		if item.Post != nil && profile != nil && item.Post.AuthorID == profile.UserID {
			continue
		}
		if item.Challenge != nil && profile != nil && item.Challenge.CreatorID == profile.UserID {
			continue
		}
		cs := getContentScore(e.ID, e.Type)
		if cs == nil || cs.Category == "" {
			continue
		}
		if familiarCats[strings.ToLower(cs.Category)] {
			continue
		}
		wild = ScoredItem{Item: item, Score: e.Score}
		found = true
		break
	}
	if !found {
		return items
	}

	// Insert the wildcard at a random eligible position (after surpriseMinPosition).
	maxPos := len(items)
	if maxPos > 12 {
		maxPos = 12 // keep wildcards near the head where user is likely to see them
	}
	insertPos := surpriseMinPosition + rnd.Intn(maxPos-surpriseMinPosition)
	out := make([]ScoredItem, 0, len(items)+surpriseMaxPerPage)
	out = append(out, items[:insertPos]...)
	out = append(out, wild)
	out = append(out, items[insertPos:]...)

	if metricSurpriseInject != nil {
		metricSurpriseInject.WithLabelValues("ok").Inc()
	}
	return out
}
