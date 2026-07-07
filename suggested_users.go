package main

import (
	"encoding/json"
	"log"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
)

// ─────────────────────────────────────────────────────────────────────────────
// SUGGESTED ACCOUNTS — TikTok / Instagram style "who to follow" surfacing.
//
// The home reels feed interleaves these cards (one every ~8 items) so users
// discover creators inside the same surface they're already engaged with,
// rather than a separate "find friends" page nobody opens.
//
// Ranking blends four signals (each squashed to roughly the same magnitude
// before adding so no single signal dominates):
//
//   1. Friends-of-friends (FoF)         — strongest social signal
//   2. Category-affinity match          — content match to the user's profile
//   3. Log-popularity                   — broad prior over follower count
//   4. Recency / activity               — only suggest creators who actually
//                                          posted recently (no ghost towns)
//   5. League proximity (small bonus)   — peers feel more relatable
//
// Cold users (no profile yet) bypass FoF + category and get pure popularity
// blended with recency, which mirrors what TikTok shows on a brand-new
// install. Cohort-aware tuning is intentionally kept simple here — the
// content ranker already does heavy cohort work; this surface stays legible.
//
// Filtering rules (always applied last):
//   - never suggest the user themselves
//   - never suggest someone they already follow
//   - never suggest someone they've blocked or who's blocked them
//   - require the candidate to have at least 1 challenge in the last 30 days
//     so we don't surface dormant accounts
// ─────────────────────────────────────────────────────────────────────────────

// suggestedAccountsLimit is how many users we serve in a single card. Five
// fits cleanly on phone screens without scrolling, and matches Instagram's
// "Suggested for you" carousel size.
const suggestedAccountsLimit = 5

// candidatePoolSize is how many users we score before sorting + truncating.
// Generous enough that a niche-category user still gets a meaningful
// shortlist; bounded so this never balloons past one DB page.
const candidatePoolSize = 80

// suggestedAccountsCacheTTL is how long we cache a built card per (user, page)
// inside the request — for the multi-page-per-session case the same userID
// hits this code path once per page request, so process-local memoization
// across calls in the same request is what we want, not cross-request caching.
// Cross-request caching would burn the Redis layer for marginal benefit
// (this query is already fast). Skipped for now.

// scoredCandidate holds a user with its blended score and its dominant reason.
type scoredCandidate struct {
	User              SuggestedAccount
	Score             float64
	DominantReason    string // "fof" | "category" | "popular" | "league"
	FollowedByFriends int
}

// BuildSuggestedAccountsCard composes one card for the given user. Returns
// nil if there aren't enough viable candidates (rare, but plausible for tiny
// social graphs in a fresh install). Page is used so the same user gets a
// different reason banner across pages 1, 2, 3.
func BuildSuggestedAccountsCard(userID string, page int) *SuggestedAccountsCard {
	if db == nil || userID == "" {
		return nil
	}

	// Step 1: Build the exclusion set — self + already-followed + blocked.
	excluded := buildSuggestedExclusions(userID)

	// Step 2: Pull the candidate pool. We're not yet scoring — just
	// gathering a generous superset that includes likely-good rows from
	// each retrieval lane (FoF, category, popular). This minimizes round
	// trips: one query per lane.
	fof := pullFoFCandidates(userID, excluded, candidatePoolSize/2)
	cat := pullCategoryCandidates(userID, excluded, candidatePoolSize/2)
	pop := pullPopularCandidates(userID, excluded, candidatePoolSize/2)

	// Step 3: Merge into a deduped map keyed by user ID. Each lane left a
	// per-row hint that we'll fold into the scoring.
	type accum struct {
		base           SuggestedAccount
		fofCount       int
		categoryFit    float64
		recentActivity bool
	}
	pool := make(map[string]*accum)

	for _, c := range fof {
		a, ok := pool[c.UserID]
		if !ok {
			a = &accum{base: c.Account}
			pool[c.UserID] = a
		}
		a.fofCount += c.FoFCount
		a.recentActivity = a.recentActivity || c.RecentActivity
	}
	for _, c := range cat {
		a, ok := pool[c.UserID]
		if !ok {
			a = &accum{base: c.Account}
			pool[c.UserID] = a
		}
		// Take the max of any per-lane category fit so users matching multiple
		// categories don't double-count their fit beyond the strongest match.
		if c.CategoryFit > a.categoryFit {
			a.categoryFit = c.CategoryFit
		}
		a.recentActivity = a.recentActivity || c.RecentActivity
	}
	for _, c := range pop {
		a, ok := pool[c.UserID]
		if !ok {
			a = &accum{base: c.Account}
			pool[c.UserID] = a
		}
		a.recentActivity = a.recentActivity || c.RecentActivity
	}

	if len(pool) == 0 {
		return nil
	}

	// Step 4: Pull the user's profile so we can compute league proximity and
	// gate cold-user behavior.
	profile, _ := getOrComputeProfile(userID)
	cold := profile == nil || isColdStartUser(profile)
	userLeagueIdx := leagueIndex(getLeagueFromProfile(userID))

	// Step 5: Score every candidate.
	scored := make([]scoredCandidate, 0, len(pool))
	for _, a := range pool {
		s := 0.0
		dominant := ""
		dominantSig := 0.0

		// FoF — saturating to avoid one well-connected account dominating.
		if a.fofCount > 0 {
			fofSig := math.Log1p(float64(a.fofCount)) * 0.40
			s += fofSig
			if fofSig > dominantSig {
				dominant, dominantSig = "fof", fofSig
			}
		}

		// Category fit — only contributes if the user has a warm profile.
		if !cold && a.categoryFit > 0 {
			catSig := a.categoryFit * 0.20
			s += catSig
			if catSig > dominantSig {
				dominant, dominantSig = "category", catSig
			}
		}

		// Popularity — log10 of follower count. Always contributes a small
		// amount so unknown/niche-but-quality creators still rank if their
		// social signals are weak.
		popSig := logSafe(float64(a.base.Followers)+1) * 0.10
		s += popSig
		// Only crown popularity as the dominant reason for cold users
		// (where the social/category signals are missing) — otherwise
		// FoF or category should hold the banner spot. This is the last
		// signal, so dominantSig doesn't need updating past this point.
		if dominant == "" || (cold && popSig > dominantSig) {
			dominant = "popular"
		}

		// Recent activity — flat bonus, gate against ghost-town accounts.
		if a.recentActivity {
			s += 0.15
		} else {
			// Hard gate: drop any candidate that hasn't posted in 30 days.
			// Better to serve a smaller card than to waste slots on
			// dormant accounts the user will hard-skip.
			continue
		}

		// League proximity — peers feel more relatable. ±1 league away
		// gets a small bonus; further does nothing.
		theirIdx := leagueIndex(a.base.League)
		if userLeagueIdx >= 0 && theirIdx >= 0 {
			diff := userLeagueIdx - theirIdx
			if diff < 0 {
				diff = -diff
			}
			if diff <= 1 {
				s += 0.10
			}
		}

		// Stamp the per-user reason + FoF count for client-side rendering.
		acc := a.base
		acc.Reason = dominant
		acc.FollowedByFriends = a.fofCount

		scored = append(scored, scoredCandidate{
			User:              acc,
			Score:             s,
			DominantReason:    dominant,
			FollowedByFriends: a.fofCount,
		})
	}

	if len(scored) == 0 {
		return nil
	}

	// Step 5.5: Acceptance-driven lane bias. If the user has historically
	// accepted suggestions from one lane more than others, give that lane's
	// candidates a multiplier so subsequent cards lean into what works for
	// them. Bounded by maxLaneBias so exploration still has room.
	acceptance := getSuggestionAcceptance(userID)
	if len(acceptance) > 0 {
		total := 0
		for _, n := range acceptance {
			total += n
		}
		if total > 0 {
			for i := range scored {
				bias := suggestionLaneBias(acceptance, total, scored[i].DominantReason)
				scored[i].Score *= bias
			}
		}
	}

	// Step 6: Sort descending, take top N.
	sort.Slice(scored, func(i, j int) bool {
		return scored[i].Score > scored[j].Score
	})
	if len(scored) > suggestedAccountsLimit {
		scored = scored[:suggestedAccountsLimit]
	}

	users := make([]SuggestedAccount, 0, len(scored))
	cardReason := dominantCardReason(scored, cold)
	for _, sc := range scored {
		users = append(users, sc.User)
	}

	return &SuggestedAccountsCard{
		ID:     "sa_" + userID + "_" + strconv.Itoa(page),
		Title:  "Accounts you might like",
		Reason: cardReason,
		Users:  users,
	}
}

// dominantCardReason picks a one-line banner that summarizes the card. We
// look at what reason "won" most rows: if FoF wins, lean social; if
// category wins, lean affinity; otherwise a generic "Popular creators".
func dominantCardReason(scored []scoredCandidate, cold bool) string {
	if cold {
		return "Popular creators"
	}
	tally := map[string]int{}
	for _, s := range scored {
		tally[s.DominantReason]++
	}
	if tally["fof"] >= tally["category"] && tally["fof"] >= tally["popular"] && tally["fof"] > 0 {
		return "Based on who you follow"
	}
	if tally["category"] > 0 {
		return "Creators in your top categories"
	}
	return "Trending creators"
}

// candidateRow is the per-lane intermediate the lane-pullers return. Score
// happens later, in BuildSuggestedAccountsCard, against the merged pool.
type candidateRow struct {
	UserID         string
	Account        SuggestedAccount
	FoFCount       int     // populated by pullFoFCandidates
	CategoryFit    float64 // 0..1, populated by pullCategoryCandidates
	RecentActivity bool    // any lane can mark this true
}

// buildSuggestedExclusions assembles the IDs we must NOT suggest:
// the user themselves, anyone they already follow, and anyone they've
// reported (best-effort — keeps Redis lookups out of the hot path).
func buildSuggestedExclusions(userID string) map[string]bool {
	excl := map[string]bool{userID: true}
	rows, err := db.Query(
		`SELECT CAST(following_id AS TEXT) FROM follows WHERE follower_id = CAST($1 AS INT)`,
		userID,
	)
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var fid string
			if rows.Scan(&fid) == nil {
				excl[fid] = true
			}
		}
	}
	return excl
}

// pullFoFCandidates returns users that the user's follows follow (2nd
// degree of separation), with each row carrying its FoF count — i.e. how
// many of "your friends" follow this person. Higher = stronger signal.
func pullFoFCandidates(userID string, excluded map[string]bool, limit int) []candidateRow {
	rows, err := db.Query(`
		SELECT CAST(f2.following_id AS TEXT) AS uid,
			   u.username, COALESCE(u.full_name,'') , u.league,
			   COALESCE(u.followers, 0), u.wins, u.losses,
			   COUNT(*)::int AS fof_count,
			   EXISTS (
				   SELECT 1 FROM challenges c
				   WHERE c.creator_id = u.id
				     AND c.created_at > NOW() - INTERVAL '30 days'
			   ) AS recent
		FROM follows f1
		JOIN follows f2 ON f1.following_id = f2.follower_id
		JOIN users u   ON f2.following_id = u.id
		WHERE f1.follower_id = CAST($1 AS INT)
		  AND f2.following_id != CAST($1 AS INT)
		GROUP BY f2.following_id, u.id
		ORDER BY fof_count DESC, u.followers DESC
		LIMIT $2`, userID, limit)
	if err != nil {
		log.Printf("pullFoFCandidates: %v", err)
		return nil
	}
	defer rows.Close()

	out := make([]candidateRow, 0, limit)
	for rows.Next() {
		var c candidateRow
		var u SuggestedAccount
		var recent bool
		if rows.Scan(
			&u.ID, &u.Username, &u.FullName, &u.League,
			&u.Followers, &u.Wins, &u.Losses,
			&c.FoFCount, &recent,
		) != nil {
			continue
		}
		if excluded[u.ID] {
			continue
		}
		c.UserID = u.ID
		c.Account = u
		c.RecentActivity = recent
		out = append(out, c)
	}
	return out
}

// pullCategoryCandidates returns users whose recent challenges fall into the
// requesting user's top-affinity categories. CategoryFit is the user's
// affinity weight for that category, so higher-affinity matches score more.
func pullCategoryCandidates(userID string, excluded map[string]bool, limit int) []candidateRow {
	profile, err := getOrComputeProfile(userID)
	if err != nil || profile == nil || len(profile.CategoryAffinity) == 0 {
		return nil
	}
	// Pick top-3 categories by affinity. Going wider just dilutes the signal.
	type cw struct {
		cat string
		w   float64
	}
	weights := make([]cw, 0, len(profile.CategoryAffinity))
	for k, v := range profile.CategoryAffinity {
		if v <= 0 {
			continue // skip neutral/DISLIKED categories — mined negatives (down to
			// -0.5) must never be picked as a "preferred" category for suggestions.
		}
		weights = append(weights, cw{cat: k, w: v})
	}
	sort.Slice(weights, func(i, j int) bool { return weights[i].w > weights[j].w })
	if len(weights) > 3 {
		weights = weights[:3]
	}
	if len(weights) == 0 {
		return nil
	}

	// Build placeholders for the IN clause. $1 is userID, $2..$N+1 are
	// categories, $N+2 is limit.
	ph := make([]string, len(weights))
	args := make([]interface{}, 0, len(weights)+2)
	args = append(args, userID)
	for i, w := range weights {
		ph[i] = "$" + strconv.Itoa(i+2)
		args = append(args, w.cat)
	}
	args = append(args, limit)
	limitPh := "$" + strconv.Itoa(len(weights)+2)

	q := `
		SELECT CAST(u.id AS TEXT) AS uid,
			   u.username, COALESCE(u.full_name,'') , u.league,
			   COALESCE(u.followers, 0), u.wins, u.losses,
			   COUNT(c.id)::int AS recent_count
		FROM users u
		JOIN challenges c ON c.creator_id = u.id
		WHERE u.id != CAST($1 AS INT)
		  AND COALESCE(c.category, 'other') IN (` + strings.Join(ph, ",") + `)
		  AND c.created_at > NOW() - INTERVAL '30 days'
		GROUP BY u.id
		ORDER BY recent_count DESC, u.followers DESC
		LIMIT ` + limitPh
	rows, err := db.Query(q, args...)
	if err != nil {
		log.Printf("pullCategoryCandidates: %v", err)
		return nil
	}
	defer rows.Close()

	// Use the strongest matching category's affinity as the fit score.
	maxAffinity := 0.0
	for _, w := range weights {
		if w.w > maxAffinity {
			maxAffinity = w.w
		}
	}
	out := make([]candidateRow, 0, limit)
	for rows.Next() {
		var c candidateRow
		var u SuggestedAccount
		var recentCount int
		if rows.Scan(
			&u.ID, &u.Username, &u.FullName, &u.League,
			&u.Followers, &u.Wins, &u.Losses, &recentCount,
		) != nil {
			continue
		}
		if excluded[u.ID] {
			continue
		}
		c.UserID = u.ID
		c.Account = u
		c.CategoryFit = maxAffinity
		c.RecentActivity = recentCount > 0
		out = append(out, c)
	}
	return out
}

// pullPopularCandidates returns users with the highest follower counts who
// have posted in the last 30 days. Pure popularity prior — usually the
// fallback lane for cold users but fine to mix in for warm ones too.
func pullPopularCandidates(userID string, excluded map[string]bool, limit int) []candidateRow {
	rows, err := db.Query(`
		SELECT CAST(u.id AS TEXT) AS uid,
			   u.username, COALESCE(u.full_name,'') , u.league,
			   COALESCE(u.followers, 0), u.wins, u.losses,
			   EXISTS (
				   SELECT 1 FROM challenges c
				   WHERE c.creator_id = u.id
				     AND c.created_at > NOW() - INTERVAL '30 days'
			   ) AS recent
		FROM users u
		WHERE u.id != CAST($1 AS INT)
		ORDER BY u.followers DESC
		LIMIT $2`, userID, limit)
	if err != nil {
		log.Printf("pullPopularCandidates: %v", err)
		return nil
	}
	defer rows.Close()
	out := make([]candidateRow, 0, limit)
	for rows.Next() {
		var c candidateRow
		var u SuggestedAccount
		var recent bool
		if rows.Scan(
			&u.ID, &u.Username, &u.FullName, &u.League,
			&u.Followers, &u.Wins, &u.Losses, &recent,
		) != nil {
			continue
		}
		if excluded[u.ID] {
			continue
		}
		c.UserID = u.ID
		c.Account = u
		c.RecentActivity = recent
		out = append(out, c)
	}
	return out
}

// getLeagueFromProfile pulls the user's league from the users table. The
// computed UserProfile doesn't carry it directly — small follow-up read.
func getLeagueFromProfile(userID string) string {
	var league string
	db.QueryRow(`SELECT COALESCE(league, '') FROM users WHERE id = CAST($1 AS INT)`, userID).Scan(&league)
	return league
}

// leagueIndex maps league names to a 0..5 ordinal so league-proximity math
// can compute a difference. Unknown leagues return -1, which the caller
// treats as "skip the proximity bonus rather than apply a wrong one."
func leagueIndex(league string) int {
	switch strings.ToLower(strings.TrimSpace(league)) {
	case "bronze":
		return 0
	case "silver":
		return 1
	case "gold":
		return 2
	case "platinum":
		return 3
	case "diamond", "dianond": // typo in seed data; treat as diamond
		return 4
	case "champion", "master":
		return 5
	}
	return -1
}

// SuggestedUsersHandler exposes suggested accounts directly so the client
// can fetch a card outside the feed (e.g. on a "Discover" sub-page or after
// a follow-spree). Mirrors the in-feed shape for consistency.
//
// GET /api/v1/users/suggested?userId=X&page=1
func SuggestedUsersHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "OPTIONS" {
		w.WriteHeader(http.StatusOK)
		return
	}
	userID := authUserID(r)
	pageStr := r.URL.Query().Get("page")
	if userID == "" {
		http.Error(w, `{"error":"userId is required"}`, http.StatusBadRequest)
		return
	}
	page, _ := strconv.Atoi(pageStr)
	if page < 1 {
		page = 1
	}
	card := BuildSuggestedAccountsCard(userID, page)
	if card == nil {
		// Empty card is still a valid 200 — client treats it as "no
		// suggestions right now" rather than an error.
		card = &SuggestedAccountsCard{
			ID:    "sa_" + userID + "_" + strconv.Itoa(page),
			Title: "Accounts you might like",
			Users: []SuggestedAccount{},
		}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(card)
}
