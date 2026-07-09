package main

import (
	"encoding/json"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/meilisearch/meilisearch-go"
)

// ─────────────────────────────────────────────────────────────────────────────
// SEARCH RANKER — TikTok / Instagram style.
//
// The previous handler dumped raw Meilisearch hits into a single list.
// This version does what the big platforms actually do:
//
//   1. Hit Meilisearch for a generous shortlist per index (challenges + users).
//   2. Apply a multi-signal RE-RANK on top of the lexical score:
//        - Lexical relevance         (Meilisearch's _rankingScore)
//        - Engagement                (log-normalized views + likes)
//        - Recency                   (exponential decay, 14-day half-life)
//        - Personalization           (does the user already follow / favor it)
//        - Social proof              (FoF count for users; creator FoF for content)
//        - Quality                   (followers for users, response count for challenges)
//   3. Split challenges into BATTLES (responseCount > 0) and SHORTS
//      (responseCount = 0) so the client can render distinct tabs that mirror
//      the For You feed taxonomy.
//   4. Capture the query into the user's recent-search list (already in place).
//
// Personalization is OPTIONAL — when userID is empty (anonymous) we skip the
// user-relative signals and the response is still well-ranked by the
// objective signals alone.
// ─────────────────────────────────────────────────────────────────────────────

// UnifiedSearchResponse is the public shape returned by /search. The legacy
// "challenges" + "users" keys are kept for backward compatibility with any
// older client; new clients should consume "accounts" / "battles" / "shorts".
//
// (There's a separate `SearchResponse` in models.go used elsewhere for an
// older user-only search response — kept distinct so we don't break callers
// of that one.)
type UnifiedSearchResponse struct {
	Accounts   []User      `json:"accounts"`
	Battles    []Challenge `json:"battles"`
	Shorts     []Challenge `json:"shorts"`
	Challenges []Challenge `json:"challenges"` // legacy: battles + shorts merged
	Users      []User      `json:"users"`      // legacy alias for accounts
	// Intent is the server's query classification: "user",
	// "category:<name>", or "general". Clients may use it to order
	// sections (e.g. accounts first for username-shaped queries).
	Intent string `json:"intent,omitempty"`
	// Related is true when the query itself had zero matches and the
	// results are a trending fallback — client should render a "no
	// exact matches, trending now" header instead of the plain list.
	Related bool `json:"related,omitempty"`
}

// per-section caps. Generous enough for an Instagram-style "Top" tab to
// have something to interleave; bounded so the client doesn't have to
// paginate inside the search response.
const (
	searchAccountCap   = 12
	searchBattleCap    = 18
	searchShortCap     = 18
	searchMeiliPoolCap = 40 // pool we ask Meilisearch for, per index
)

// SearchHandler handles requests to the /search endpoint.
//   GET /search?q=query[&type=all|accounts|battles|shorts][&userId=X]
//
// The 'type' param narrows the response to a single section but leaves the
// JSON shape stable — empty arrays for the sections you didn't ask for.
func SearchHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "OPTIONS" {
		w.WriteHeader(http.StatusOK)
		return
	}

	query := strings.TrimSpace(r.URL.Query().Get("q"))
	searchType := r.URL.Query().Get("type")
	userID := r.URL.Query().Get("userId")

	if query == "" {
		http.Error(w, `{"error":"missing search query parameter 'q'"}`, http.StatusBadRequest)
		return
	}

	if searchType == "" {
		searchType = "all"
	}

	// Best effort — record the query into the user's recent-search LIST so
	// the For You ranker can bias toward this category for ~24h. Async to
	// keep the search response latency down.
	if userID != "" {
		go RecordSearchQuery(userID, query)
	}

	// Pull personalization context once if we have a userID. All four lookups
	// are cheap and request-local-cached.
	var (
		profile     *UserProfile
		followingSet map[string]bool
		fofSet      map[string]bool
		viewerLeague string
	)
	if userID != "" {
		profile, _ = getOrComputeProfile(userID)
		followingSet, fofSet = buildSocialSets(userID)
		if u, ok := GetUserByID(userID); ok {
			viewerLeague = u.League
		}
	}

	resp := UnifiedSearchResponse{
		Accounts: []User{},
		Battles:  []Challenge{},
		Shorts:   []Challenge{},
	}

	// Fetch + rerank ACCOUNTS.
	if searchType == "all" || searchType == "accounts" || searchType == "users" {
		resp.Accounts = rankSearchAccounts(query, userID, followingSet, fofSet, viewerLeague)
	}

	// Fetch + rerank CHALLENGES, then split into battles + shorts. We fetch
	// once and split, rather than running two queries, since Meilisearch's
	// lexical rank is the same either way.
	if searchType == "all" || searchType == "battles" || searchType == "shorts" || searchType == "challenges" {
		all := rankSearchChallenges(query, userID, profile, followingSet)
		for _, ch := range all {
			if ch.ResponseCount > 0 {
				if len(resp.Battles) < searchBattleCap {
					resp.Battles = append(resp.Battles, ch)
				}
			} else {
				if len(resp.Shorts) < searchShortCap {
					resp.Shorts = append(resp.Shorts, ch)
				}
			}
		}
		// Enrich battles with opponent (top-response) fields so the client
		// can render the battle-indicator pill the moment the user taps a
		// search result and lands inside the fullscreen reels viewer. Shorts
		// have no responses by definition, so we only enrich battles. Same
		// path the SmartFeed and Explore handlers use — keeps the JSON shape
		// consistent across every surface that returns a Challenge.
		if len(resp.Battles) > 0 {
			populateTopResponsesChallenges(resp.Battles)
		}
		// Also surface the merged list under the legacy "challenges" key so
		// old clients keep working without a server-side breaking change.
		// Built AFTER enrichment so the legacy slice carries opponent fields
		// too; otherwise old paths would silently lose the battle UI.
		merged := make([]Challenge, 0, len(resp.Battles)+len(resp.Shorts))
		merged = append(merged, resp.Battles...)
		merged = append(merged, resp.Shorts...)
		resp.Challenges = merged
	}

	// Legacy alias.
	resp.Users = resp.Accounts

	// Query-intent classification for the client's section ordering
	// ("user" only when the accounts lane actually found something —
	// a username-shaped query with zero account hits is just a word).
	resp.Intent = searchIntent(query, len(resp.Accounts) > 0)

	// Zero-result rescue: never render an empty search page. Fall back
	// to realtime-trending content (category-filtered when the query
	// smells like a topic), flagged so the client can label it honestly.
	if len(resp.Accounts) == 0 && len(resp.Battles) == 0 && len(resp.Shorts) == 0 {
		if rescued := searchZeroResultRescue(resp.Intent); len(rescued) > 0 {
			resp.Related = true
			for _, ch := range rescued {
				if ch.ResponseCount > 0 && len(resp.Battles) < searchBattleCap {
					resp.Battles = append(resp.Battles, ch)
				} else if len(resp.Shorts) < searchShortCap {
					resp.Shorts = append(resp.Shorts, ch)
				}
			}
			resp.Challenges = append(append([]Challenge{}, resp.Battles...), resp.Shorts...)
		}
	}

	// Log impressions for the click-through learner — exactly what the
	// client will render (skip rescued results: clicks on trending
	// fallbacks shouldn't teach the ORIGINAL query anything). Top-10
	// per section keeps write volume and hash growth bounded.
	if !resp.Related {
		keys := make([]string, 0, 20)
		for i, u := range resp.Accounts {
			if i >= 10 {
				break
			}
			keys = append(keys, "user:"+u.ID)
		}
		shown := 0
		for _, ch := range resp.Challenges {
			if shown >= 10 {
				break
			}
			keys = append(keys, "challenge:"+ch.ID)
			shown++
		}
		go searchLogImpressions(query, keys)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// searchZeroResultRescue returns trending challenges as a fallback for
// queries with no matches. Category-filtered when the intent carries
// one; falls back to unfiltered trending, then recency, so the rescue
// itself never comes back empty on a healthy catalog.
func searchZeroResultRescue(intent string) []Challenge {
	wantCat := searchIntentCategory(intent)
	entries := fetchTrendingRealtime(30)
	out := make([]Challenge, 0, 10)
	backup := make([]Challenge, 0, 10)
	for _, e := range entries {
		if e.Type != "challenge" {
			continue
		}
		item, ok := loadHomeFeedItemByID(e.Type, e.ID)
		if !ok || item.Challenge == nil {
			continue
		}
		ch := *item.Challenge
		if wantCat != "" && strings.EqualFold(ch.Category, wantCat) {
			out = append(out, ch)
		} else if len(backup) < 10 {
			backup = append(backup, ch)
		}
		if len(out) >= 10 {
			break
		}
	}
	if len(out) == 0 {
		out = backup
	}
	if len(out) > 0 {
		populateTopResponsesChallenges(out)
	}
	return out
}

// ─────────────────────────────────────────────────────────────────────────────
// ACCOUNT RANKING
// ─────────────────────────────────────────────────────────────────────────────

// rankSearchAccounts fetches a generous Meilisearch shortlist of users
// matching the query, then re-ranks with social + quality signals.
func rankSearchAccounts(query, userID string, following, fof map[string]bool, viewerLeague string) []User {
	hits := meiliSearchUsers(query, searchMeiliPoolCap)
	if len(hits) == 0 {
		hits = postgresSearchUsersFallback(query, searchMeiliPoolCap)
	}
	if len(hits) == 0 {
		return []User{}
	}

	// leagueTier returns 0 for empty/unknown leagues; valid tiers are 1..5.
	viewerLeagueIdx := leagueTier[viewerLeague]

	type scored struct {
		User  User
		Score float64
	}
	// Learned click-through prior for this query (see search_ctr.go).
	ctrBoosts := searchCTRBoosts(query)
	out := make([]scored, 0, len(hits))
	for i, hit := range hits {
		// Skip the viewer themselves — surfacing your own account is just
		// noise in a search context.
		if hit.User.ID == userID {
			continue
		}

		// Lexical: the position-in-shortlist proxy decays exponentially.
		// Meilisearch's _rankingScore would be ideal but the Go SDK doesn't
		// expose it cleanly across all versions, so we use rank order
		// (which is what _rankingScore drives anyway).
		lex := math.Exp(-float64(i) / 8.0) // 1.0 at top, ~0.45 at i=8, ~0.20 at i=12

		// Quality: log-popularity prior on follower count. Saturates fast so
		// a 1M-follower account doesn't crush a 5k-follower exact match.
		quality := logSafe(float64(hit.User.Followers)+1) / 6.0 // 0..1 over [1,1e6]

		// Win-rate as a signal (battle accounts the user has a track record).
		winRate := 0.0
		total := hit.User.Wins + hit.User.Losses
		if total > 0 {
			winRate = float64(hit.User.Wins) / float64(total)
		}

		// Social proof — only when we know the viewer.
		social := 0.0
		if userID != "" {
			if following[hit.User.ID] {
				social = 0.30 // already following → top of list
			} else if fof[hit.User.ID] {
				social = 0.15 // friend-of-friend → strong signal
			}
		}

		// League proximity — peers feel more relevant than super-elite
		// accounts. Same/±1 league bonus.
		leagueBonus := 0.0
		if viewerLeagueIdx > 0 {
			theirIdx := leagueTier[hit.User.League]
			if theirIdx > 0 {
				diff := viewerLeagueIdx - theirIdx
				if diff < 0 {
					diff = -diff
				}
				if diff <= 1 {
					leagueBonus = 0.05
				}
			}
		}

		score := lex + 0.20*quality + 0.10*winRate + social + leagueBonus +
			ctrBoosts["user:"+hit.User.ID]
		out = append(out, scored{User: hit.User, Score: score})
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Score > out[j].Score })
	if len(out) > searchAccountCap {
		out = out[:searchAccountCap]
	}
	users := make([]User, len(out))
	for i, s := range out {
		users[i] = s.User
	}
	return users
}

// userHit is a Meilisearch hit normalized into a User struct + the original
// rank position.
type userHit struct {
	User User
	// Rank position from Meilisearch (0-indexed). Used as a lexical-score
	// proxy in the re-ranker.
	Rank int
}

// meiliSearchUsers calls the users index. Returns nil on any error so the
// fallback path can take over.
func meiliSearchUsers(query string, limit int) []userHit {
	if meili == nil {
		return nil
	}
	res, err := meili.Index("users").Search(query, &meilisearch.SearchRequest{
		Limit: int64(limit),
	})
	if err != nil {
		return nil
	}
	out := make([]userHit, 0, len(res.Hits))
	for i, hit := range res.Hits {
		doc := decodeHit(hit)
		out = append(out, userHit{User: meiliDocToUser(doc), Rank: i})
	}
	return out
}

// postgresSearchUsersFallback runs the legacy Levenshtein scorer when
// Meilisearch is unconfigured. Same shape as the meili path.
func postgresSearchUsersFallback(query string, limit int) []userHit {
	scoredUsers := make([]scoredUser, 0)
	allUsers := GetAllUsers()
	for _, user := range allUsers {
		s := calculateScore(user, query)
		if s > 0 {
			scoredUsers = append(scoredUsers, scoredUser{User: user, Score: s})
		}
	}
	sort.Slice(scoredUsers, func(i, j int) bool { return scoredUsers[i].Score > scoredUsers[j].Score })
	if len(scoredUsers) > limit {
		scoredUsers = scoredUsers[:limit]
	}
	out := make([]userHit, len(scoredUsers))
	for i, su := range scoredUsers {
		out[i] = userHit{User: su.User, Rank: i}
	}
	return out
}

// ─────────────────────────────────────────────────────────────────────────────
// CHALLENGE RANKING
// ─────────────────────────────────────────────────────────────────────────────

// rankSearchChallenges does the same lex + multi-signal re-rank for
// challenges. Returned list is mixed (battles + shorts); caller splits.
func rankSearchChallenges(query, userID string, profile *UserProfile, following map[string]bool) []Challenge {
	hits := meiliSearchChallenges(query, searchMeiliPoolCap)
	if len(hits) == 0 {
		hits = postgresSearchChallengesFallback(query, searchMeiliPoolCap)
	}
	if len(hits) == 0 {
		return nil
	}

	type scored struct {
		Ch    Challenge
		Score float64
	}
	out := make([]scored, 0, len(hits))
	now := time.Now()

	// Pre-compute the user's top categories from profile affinity so we don't
	// loop through the map for every hit.
	topCats := topCategories(profile, 3)

	// Click-through prior: what users who searched this exact query
	// actually tapped before (Wilson-smoothed, position-debiased).
	// One Redis read per query; empty map when the query is unseen.
	ctrBoosts := searchCTRBoosts(query)
	// Intent hint: queries that look like a content topic give matching
	// categories a nudge (accounts-shaped intent is handled by the
	// handler via the response's intent field).
	intentCat := searchIntentCategory(searchIntent(query, false))

	for i, hit := range hits {
		ch := hit.Ch

		// Lexical (rank-position decay).
		lex := math.Exp(-float64(i) / 8.0)

		// Engagement — log-normalized views + likes. Likes weighted 5x because
		// they're an explicit positive signal vs. passive views.
		eng := logSafe(float64(ch.Views)+5*float64(ch.Likes)+1) / 8.0 // 0..~1

		// Recency — 14-day half-life. Search expects fresher results than For
		// You does (you typically search for a current trend, not an
		// evergreen video).
		recency := 0.0
		if t, err := time.Parse(time.RFC3339, ch.CreatedAt); err == nil {
			ageDays := now.Sub(t).Hours() / 24.0
			if ageDays < 0 {
				ageDays = 0
			}
			recency = math.Exp(-ageDays / 14.0) // 1.0 at age=0, ~0.50 at 10d, ~0.14 at 28d
		}

		// Personalization — only when we have a profile.
		personalBoost := 0.0
		if profile != nil {
			// Lowercase to match the canonical casing CategoryAffinity / topCategories
			// are stored under (all writers force lowercase). Reading the raw DB
			// ch.Category would deterministically miss a mixed-case category and zero
			// the personalization.
			cat := strings.ToLower(ch.Category)
			// Category-affinity match.
			if cat != "" {
				if w, ok := profile.CategoryAffinity[cat]; ok && w > 0 {
					personalBoost += w * 0.15
				}
			}
			// Following the creator? Strong personal signal.
			if following != nil && following[ch.CreatorID] {
				personalBoost += 0.10
			}
			// Top-category override — even if the affinity weight is small,
			// reward results in the user's known interests.
			if cat != "" {
				for _, tc := range topCats {
					if tc == cat {
						personalBoost += 0.05
						break
					}
				}
			}
		}
		_ = userID // per-user CTR could key on (user, query) later; global CTR ships first

		// Quality nudge — a battle (responseCount > 0) is more interactive
		// content than a static short. Tiny bonus so battles edge out shorts
		// at near-tie lexical score, mirroring TikTok's preference for
		// interactive results in search.
		qualityNudge := 0.0
		if ch.ResponseCount > 0 {
			qualityNudge = 0.05
		}

		// Learned click-through prior for THIS query + intent-category
		// nudge — the two additions that make search self-correct.
		ctr := ctrBoosts["challenge:"+ch.ID]
		intentBoost := 0.0
		if intentCat != "" && strings.EqualFold(ch.Category, intentCat) {
			intentBoost = 0.15
		}

		score := lex + 0.20*eng + 0.20*recency + personalBoost + qualityNudge + ctr + intentBoost
		out = append(out, scored{Ch: ch, Score: score})
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Score > out[j].Score })
	chs := make([]Challenge, len(out))
	for i, s := range out {
		chs[i] = s.Ch
	}
	return chs
}

// challengeHit is a Meilisearch hit normalized into a Challenge.
type challengeHit struct {
	Ch   Challenge
	Rank int
}

func meiliSearchChallenges(query string, limit int) []challengeHit {
	if meili == nil {
		return nil
	}
	res, err := meili.Index("challenges").Search(query, &meilisearch.SearchRequest{
		Limit: int64(limit),
	})
	if err != nil {
		return nil
	}
	out := make([]challengeHit, 0, len(res.Hits))
	for i, hit := range res.Hits {
		doc := decodeHit(hit)
		out = append(out, challengeHit{Ch: meiliDocToChallenge(doc), Rank: i})
	}
	return out
}

func postgresSearchChallengesFallback(query string, limit int) []challengeHit {
	q := strings.ToLower(strings.TrimSpace(query))
	allChallenges := GetArenaChallenges()
	out := make([]challengeHit, 0)
	for _, c := range allChallenges {
		title := strings.ToLower(c.Prefix + " " + c.Subject)
		creator := strings.ToLower(c.CreatorUsername)
		if strings.Contains(title, q) || strings.Contains(creator, q) {
			out = append(out, challengeHit{Ch: c, Rank: len(out)})
			if len(out) >= limit {
				break
			}
		}
	}
	return out
}

// topCategories returns the user's top-N categories by affinity weight.
// Returns nil when the profile is missing or has no affinity data — the
// caller treats that as "skip the top-cat boost".
func topCategories(profile *UserProfile, n int) []string {
	if profile == nil || len(profile.CategoryAffinity) == 0 {
		return nil
	}
	type cw struct {
		cat string
		w   float64
	}
	weights := make([]cw, 0, len(profile.CategoryAffinity))
	for k, v := range profile.CategoryAffinity {
		if v <= 0 {
			continue // skip neutral/mined-negative categories — never a "preferred" cat
		}
		weights = append(weights, cw{cat: k, w: v})
	}
	sort.Slice(weights, func(i, j int) bool { return weights[i].w > weights[j].w })
	if len(weights) > n {
		weights = weights[:n]
	}
	out := make([]string, len(weights))
	for i, w := range weights {
		out[i] = w.cat
	}
	return out
}

// ─────────────────────────────────────────────────────────────────────────────
// LEGACY HELPERS (kept so existing callers keep compiling — they're used by
// the fallback path above and by external code paths.)
// ─────────────────────────────────────────────────────────────────────────────

// scoredUser is a helper struct just for sorting users with their search score.
type scoredUser struct {
	User  User
	Score float64
}

// meiliDocToChallenge converts a Meilisearch hit to a Challenge.
func meiliDocToChallenge(doc map[string]interface{}) Challenge {
	return Challenge{
		ID:              toString(doc["id"]),
		CreatorID:       toString(doc["creatorId"]),
		CreatorUsername: toString(doc["creatorUsername"]),
		CreatorLeague:   toString(doc["creatorLeague"]),
		Prefix:          toString(doc["prefix"]),
		Subject:         toString(doc["subject"]),
		Visibility:      toString(doc["visibility"]),
		Status:          toString(doc["status"]),
		Likes:           toInt(doc["likes"]),
		Views:           toInt(doc["views"]),
		ResponseCount:   toInt(doc["responseCount"]),
		VideoURL:        toString(doc["videoUrl"]),
		ThumbnailURL:    toString(doc["thumbnailUrl"]),
		CreatedAt:       toString(doc["createdAt"]),
	}
}

func meiliDocToUser(doc map[string]interface{}) User {
	return User{
		ID:        toString(doc["id"]),
		Username:  toString(doc["username"]),
		FullName:  toString(doc["fullName"]),
		League:    toString(doc["league"]),
		Followers: toInt(doc["followers"]),
		Wins:      toInt(doc["wins"]),
		Losses:    toInt(doc["losses"]),
	}
}

func toString(v interface{}) string {
	if v == nil {
		return ""
	}
	switch val := v.(type) {
	case string:
		return val
	case float64:
		if val == float64(int(val)) {
			return strconv.Itoa(int(val))
		}
		return strconv.FormatFloat(val, 'f', -1, 64)
	default:
		return ""
	}
}

func toInt(v interface{}) int {
	if v == nil {
		return 0
	}
	switch val := v.(type) {
	case float64:
		return int(val)
	case int:
		return val
	default:
		return 0
	}
}

// calculateScore is the legacy Levenshtein-based user scorer. Kept for the
// fallback path when Meilisearch is unavailable.
func calculateScore(user User, query string) float64 {
	var score float64
	lowerQuery := strings.ToLower(strings.TrimSpace(query))
	lowerUsername := strings.ToLower(user.Username)
	lowerFullName := strings.ToLower(user.FullName)
	lowerLeague := strings.ToLower(user.League)

	if lowerUsername == lowerQuery {
		score += 100.0
	}
	if strings.HasPrefix(lowerUsername, lowerQuery) {
		score += 50.0
	}

	dist := levenshteinDistance(lowerQuery, lowerUsername)
	maxLen := len(lowerQuery)
	if len(lowerUsername) > maxLen {
		maxLen = len(lowerUsername)
	}
	if maxLen > 0 {
		similarity := 1.0 - float64(dist)/float64(maxLen)
		if similarity >= 0.45 {
			score += similarity * 35.0
		}
	}

	queryTokens := strings.Fields(lowerQuery)
	for _, token := range queryTokens {
		if strings.Contains(lowerUsername, token) {
			score += 10.0
		}
		if strings.Contains(lowerFullName, token) {
			score += 5.0
		}
		if strings.Contains(lowerLeague, token) {
			score += 3.0
		}

		nameTokens := strings.Fields(lowerFullName)
		for _, nameToken := range nameTokens {
			tokenDist := levenshteinDistance(token, nameToken)
			tokenMaxLen := len(token)
			if len(nameToken) > tokenMaxLen {
				tokenMaxLen = len(nameToken)
			}
			if tokenMaxLen > 0 {
				sim := 1.0 - float64(tokenDist)/float64(tokenMaxLen)
				if sim >= 0.55 {
					score += sim * 8.0
				}
			}
		}

		tokenDistU := levenshteinDistance(token, lowerUsername)
		tokenMaxU := len(token)
		if len(lowerUsername) > tokenMaxU {
			tokenMaxU = len(lowerUsername)
		}
		if tokenMaxU > 0 {
			simU := 1.0 - float64(tokenDistU)/float64(tokenMaxU)
			if simU >= 0.5 {
				score += simU * 6.0
			}
		}
	}

	if strings.HasPrefix(lowerFullName, lowerQuery) {
		score += 15.0
	}

	score += float64(user.Followers) * 0.01

	totalGames := user.Wins + user.Losses
	if totalGames > 0 {
		winRate := float64(user.Wins) / float64(totalGames)
		score += winRate * 5.0
		if totalGames > 50 {
			score += 3.0
		} else if totalGames > 20 {
			score += 1.5
		}
	}

	return score
}

// levenshteinDistance computes the minimum edit distance between two strings.
func levenshteinDistance(a, b string) int {
	la, lb := len(a), len(b)
	if la == 0 {
		return lb
	}
	if lb == 0 {
		return la
	}

	matrix := make([][]int, la+1)
	for i := range matrix {
		matrix[i] = make([]int, lb+1)
		matrix[i][0] = i
	}
	for j := 0; j <= lb; j++ {
		matrix[0][j] = j
	}

	for i := 1; i <= la; i++ {
		for j := 1; j <= lb; j++ {
			cost := 1
			if a[i-1] == b[j-1] {
				cost = 0
			}
			del := matrix[i-1][j] + 1
			ins := matrix[i][j-1] + 1
			sub := matrix[i-1][j-1] + cost
			best := del
			if ins < best {
				best = ins
			}
			if sub < best {
				best = sub
			}
			matrix[i][j] = best
		}
	}
	return matrix[la][lb]
}
