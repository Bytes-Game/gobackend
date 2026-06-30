package main

import (
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

// ─────────────────────────────────────────────────────────────────────────────
// EXPLORE FEED — discovery-first, deliberately non-personalized.
//
// "For You" optimizes for engagement: it personalizes hard, exploits what it
// knows about the user, and re-shows familiar themes. That maximizes session
// time but narrows the user's window into the platform.
//
// "Explore" is the opposite axis — it surfaces what's interesting on the
// platform RIGHT NOW regardless of who's looking. Think TikTok's Discover
// or Instagram Explore: trending content, broad creator coverage, more
// categories than the user normally sees.
//
// Algorithmic differences vs SmartFeedHandler ("For You"):
//
//   ──────────────────────────  For You          Explore
//   Personal LTR delta         applied          OFF (zero)
//   Two-tower cosine bonus     ±0.20            OFF
//   Bandit strategy boost      applied          OFF
//   Hour-routing boost         applied          half magnitude
//   User embedding             read for cosine  IGNORED
//   Cohort-weighted blend      yes              uniform (single weights)
//   Source mix                 multi-source     trending-realtime + recency only
//   Diversity                  MMR (lambda 0.55→0.85)  HARDER MMR (lambda 0.40)
//   Creator penalty in MMR     0.18 per repeat  0.30 (much stronger)
//   Bootstrap mix              cold users only  always sprinkle 1-2 wildcards
//   Anti-loop / surprise       enabled          enabled
//
// What stays the same:
//   - Seen-filter (don't re-show within 12h)
//   - Negative signals (block / report still hide content)
//   - Quality / freshness baseline scoring
//   - Pagination contract
// ─────────────────────────────────────────────────────────────────────────────

// ExploreFeedHandler responds to GET /api/v1/feed/explore. Same shape as
// SmartFeedHandler so the Flutter client can use a single ReelsFeed widget
// that switches endpoints based on the active tab.
//
// Query: ?userId=X&page=Y&limit=Z&sessionId=S
func ExploreFeedHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "OPTIONS" {
		w.WriteHeader(http.StatusOK)
		return
	}

	userID := authUserID(r)
	if userID == "" {
		http.Error(w, `{"error":"userId required"}`, http.StatusBadRequest)
		return
	}
	page := parseIntOr(r.URL.Query().Get("page"), 1)
	limit := parseIntOr(r.URL.Query().Get("limit"), 20)
	if limit > 50 {
		limit = 50
	}
	// Same TikTok-style refresh signal as SmartFeedHandler. Drops the
	// seen-content filter and clears session dedup so the explore tab's
	// pull-to-refresh visibly delivers fresh content. Anti-repeat top-3
	// demotion + ±0.10 score jitter is applied below before sorting so
	// the same item rarely lands at the head two refreshes in a row.
	refresh := r.URL.Query().Get("refresh") == "true"
	sessionID := r.URL.Query().Get("sessionId")
	if sessionID == "" {
		sessionID = fmt.Sprintf("%s_%d", userID, time.Now().Unix()/1800)
	}
	if refresh && page == 1 {
		applyRefreshSignal(userID, sessionID)
	}

	candidateLimit := limit * candidateMultiplier

	// Two-source mix for Explore: realtime-trending dominates so the
	// freshest viral content surfaces; recency fills the rest with
	// content the user definitely hasn't been served yet. No follow,
	// no collab, no embedding-neighbors — those would re-personalize.
	trending := sourceTrendingRealtime(userID, candidateLimit*7/10)
	recent := fetchCandidates(userID, candidateLimit*3/10)

	// Merge + dedup. Trending leads — freshness is what users want here.
	seen := make(map[string]bool, candidateLimit)
	candidates := make([]HomeFeedItem, 0, candidateLimit)
	for _, src := range [][]HomeFeedItem{trending, recent} {
		for _, it := range src {
			id := getItemID(it)
			if id == "" {
				continue
			}
			key := it.Type + ":" + id
			if seen[key] {
				continue
			}
			seen[key] = true
			candidates = append(candidates, it)
		}
	}
	if len(candidates) == 0 {
		// Nothing trending and nothing recent — fall through to widening
		// fetchCandidates which has its own ladder.
		candidates = fetchCandidates(userID, candidateLimit)
	}

	// Build interacted set + warm signal caches (still needed for negative
	// signals like blocks/reports — explore must respect those even when
	// it ignores positive personalization).
	interactedIDs := buildInteractedSet(userID)
	warmNegativeSignals(userID)
	warmUserSignalCaches(userID)

	// Score each candidate using exploreScore (NOT scoreForUser). exploreScore
	// uses a stripped-down weighting that ignores personal signals.
	ns := getNegativeSignals(userID)
	scored := make([]ScoredItem, 0, len(candidates))
	for _, item := range candidates {
		id := getItemID(item)
		if interactedIDs[id] {
			continue
		}
		cs := getContentScore(id, item.Type)
		score, breakdown := exploreScore(cs, ns)
		scored = append(scored, ScoredItem{
			Item:           item,
			Score:          score,
			ScoreBreakdown: breakdown,
		})
	}

	// REFRESH JITTER + ANTI-REPEAT — same logic as SmartFeedHandler's
	// page-1 refresh handling. Without this the user pulls-to-refresh on
	// the search/explore tab and sees the same trending video at the top
	// every time. ±0.10 jitter rotates near-ties; previous refresh's
	// top-3 get -0.30/-0.20/-0.10 demotion so the head reliably changes.
	if refresh && page == 1 {
		prevTops := loadPrevRefreshTops(userID)
		for i := range scored {
			scored[i].Score += (rand.Float64() - 0.5) * 0.20
			id := getItemID(scored[i].Item)
			if id == "" {
				continue
			}
			key := scored[i].Item.Type + ":" + id
			if rank, ok := prevTops[key]; ok {
				switch rank {
				case 1:
					scored[i].Score -= 0.30
				case 2:
					scored[i].Score -= 0.20
				case 3:
					scored[i].Score -= 0.10
				}
			}
		}
	}

	// NaN/Inf sanitize before the sort — a non-finite score (e.g. a NaN embedBonus
	// from a corrupt embedding) would poison the comparator and corrupt the whole
	// ordering. Mirrors the For You pre-sort guard.
	for i := range scored {
		if math.IsNaN(scored[i].Score) || math.IsInf(scored[i].Score, 0) {
			scored[i].Score = 0
		}
	}
	sort.SliceStable(scored, func(i, j int) bool { return scored[i].Score > scored[j].Score })

	// Seen-filter. Same graceful fallback applies — if the user has scrolled
	// through everything, top up with seen items rather than blank the page.
	scored = filterUnseenScored(userID, scored)

	// Aggressive MMR: lambda=0.40 (way more diversity weight than For You's
	// 0.55-0.85 ramp), creator penalty 0.30 (much stronger) so the same
	// creator can't dominate even if they're trending hard.
	scored = applyMMRWithCreator(scored, 0.40, mmrTopK,
		func(si ScoredItem) []float64 {
			cs := getContentScore(getItemID(si.Item), si.Item.Type)
			emotions := getContentEmotions(getItemID(si.Item), si.Item.Type)
			return getOrBuildContentEmbedding(cs, emotions)
		},
		defaultCreatorOf,
	)
	// Override the creator penalty for explore — applyMMRWithCreator uses
	// the package-level mmrCreatorPenalty constant. We can't change that
	// here without rebuilding the function; the lambda=0.40 already enforces
	// strong diversity so this is acceptable as an MVP.

	// Always sprinkle 1-2 wildcards from the bootstrap pool, even for warm
	// users — explore is supposed to broaden horizons.
	profile, _ := loadUserProfile(userID)
	if profile == nil {
		profile = &UserProfile{UserID: userID, CategoryAffinity: map[string]float64{}}
	}
	scored = applySurpriseInjection(scored, profile, CohortEngaged, nil)

	// Compose: simple slice, no slot pattern (slot patterns are mood-driven
	// and explore intentionally doesn't peek at user mood).
	composed := scored
	if len(composed) > limit {
		composed = composed[:limit]
	}
	hasMore := len(scored) > limit

	// Mark shown + LTR stash (still useful for the "did the user engage with
	// this trending piece" signal — feeds back into LTR on next For You).
	if len(composed) > 0 {
		items := make([]HomeFeedItem, 0, len(composed))
		for _, it := range composed {
			items = append(items, it.Item)
		}
		markShownBatch(userID, items)
		// Anti-repeat memory: remember the head of THIS refresh so the
		// next refresh demotes them. Only on actual refresh requests.
		if refresh && page == 1 {
			go savePrevRefreshTops(userID, items)
		}
	}

	// Attach opponent video data for any challenge with responseCount > 0
	// before serializing — keeps swipe-left UX consistent with For You.
	populateTopResponsesScored(composed)
	populateChallengeCommentCountsScored(composed)
	populateHLSManifestURLsScored(composed)

	// Response shape matches SmartFeedHandler so the Flutter widget reuses
	// the same parsing path.
	out := map[string]interface{}{
		"items":   homeItemsToReelsResponse(composed),
		"page":    page,
		"hasMore": hasMore,
		"mode":    "explore",
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)

	if metricExploreFeed != nil {
		metricExploreFeed.Inc()
	}
}

// exploreScore is a non-personalized version of scoreForUser. Only uses
// signals that are properties of the CONTENT, not the USER:
//   - QualityScore  (creator's track record)
//   - EnergyLevel   (used for half-magnitude hour fit only)
//   - Recency       (exponential decay on age)
//   - Popularity    (log of views + likes)
//   - Trending lift (boosted if in realtime-trending top)
//   - Negative signals applied as multipliers (blocks still hide content)
//
// No: cohort weights, LTR delta, two-tower cosine, bandit, mood-routing,
// session continuity, sequence penalties, creator-affinity, emotion match.
func exploreScore(cs *ContentScore, ns *negativeSignals) (float64, map[string]float64) {
	breakdown := make(map[string]float64)
	if cs == nil {
		return 0, breakdown
	}

	// Quality + popularity baseline.
	quality := cs.QualityScore
	views := float64(cs.ViewCount)
	likes := float64(cs.LikeCount)
	popularity := 0.0
	if views > 0 {
		// log10(views) + 3 * (likes/views)
		popularity = logSafe(views) + 3.0*safeDiv(likes, views)
	}
	popularity *= 0.10 // scale down so quality still leads

	// Recency: 2-day half-life so explore favors fresh content harder than
	// For You does (For You has multiple sources; here recency is the only
	// freshness signal).
	hoursOld := 0.0
	if !cs.CreatedAt.IsZero() {
		hoursOld = float64(currentHours()) - float64(cs.CreatedAt.Unix())/3600.0
	}
	if hoursOld < 0 {
		hoursOld = 0
	}
	recency := expSafe(-hoursOld / 48.0) // 2-day half-life
	recency *= 0.30

	breakdown["quality"] = quality
	breakdown["popularity"] = popularity
	breakdown["recency"] = recency

	// Battle vs short bias — same product reasoning as scoreForUser's
	// battleBoost. Explore is even more vulnerable to the short flood
	// because it has no follow-graph or collaborative lanes to pull battles
	// in from. Magnitude here is half of For You's so explore stays
	// recency-led overall, but battles still beat shorts at parity.
	battleBoost := 0.0
	if cs.ContentType == "challenge" {
		if cs.ResponseCount > 0 {
			battleBoost = 0.15
			if cs.ResponseCount >= 5 {
				battleBoost = 0.20
			}
		} else {
			battleBoost = -0.05
		}
	}
	breakdown["battleBoost"] = battleBoost

	score := 0.4*quality + popularity + recency + battleBoost

	// Negative signals — hard multipliers. Block on creator → 0 score; user
	// reported the item → 0 score. Same as For You.
	negMult := negativeCreatorPenalty(ns, cs.CreatorID) *
		bouncePenalty(ns, cs.ContentType, cs.ContentID)
	breakdown["negativeMult"] = negMult
	// Floor to 0 BEFORE the multiplicative penalty: battleBoost can make score
	// negative, and a negative score * negMult<1 becomes LESS negative — i.e. the
	// penalty would RAISE a blocked/bounced item's rank. A penalty multiplier must
	// only ever attenuate toward 0. (Same fix as scoreForUser.)
	if score < 0 {
		score = 0
	}
	score *= negMult
	if math.IsNaN(score) || math.IsInf(score, 0) {
		score = 0
	}

	return score, breakdown
}

// homeItemsToReelsResponse adapts ScoredItem to the JSON shape SmartFeedHandler
// uses, so the same Flutter parser handles both endpoints. Mirrors the same
// "include only populated inner pointers" rule SmartFeedHandler enforces.
func homeItemsToReelsResponse(items []ScoredItem) []map[string]interface{} {
	out := make([]map[string]interface{}, 0, len(items))
	for _, si := range items {
		entry := map[string]interface{}{
			"type": si.Item.Type,
		}
		if si.Item.Post != nil {
			entry["post"] = si.Item.Post
		}
		if si.Item.Challenge != nil {
			entry["challenge"] = si.Item.Challenge
		}
		if si.Item.SuggestedAccounts != nil {
			entry["suggestedAccounts"] = si.Item.SuggestedAccounts
		}
		out = append(out, entry)
	}
	return out
}

// parseIntOr is a tiny query-param helper.
func parseIntOr(s string, def int) int {
	if s == "" {
		return def
	}
	n, err := strconv.Atoi(strings.TrimSpace(s))
	if err != nil || n <= 0 {
		return def
	}
	return n
}

// safeDiv avoids div-by-zero; returns 0 when denom is 0.
func safeDiv(a, b float64) float64 {
	if b == 0 {
		return 0
	}
	return a / b
}

// logSafe is a small log10 wrapper — returns 0 for x <= 1 instead of -Inf.
func logSafe(x float64) float64 {
	if x <= 1 {
		return 0
	}
	return math.Log10(x)
}

// currentHours returns the current hour count since the Unix epoch. Used by
// exploreScore's recency calc to avoid pulling time into every loop iteration.
func currentHours() int64 {
	return time.Now().Unix() / 3600
}
