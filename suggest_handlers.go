package main

import (
	"encoding/json"
	"log"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/meilisearch/meilisearch-go"
)

// addDocumentsChunked uploads docs to a Meilisearch index in bounded chunks with
// retry + backoff. Meilisearch's free tier rate-limits bulk uploads (HTTP 429),
// which previously left the challenge-subjects autocomplete index unseeded on
// boot (a single ~hundreds-of-docs AddDocuments burst). Chunking keeps each
// request small and a short exponential backoff rides out a transient 429.
// Idempotent (Meilisearch dedupes by primary key); a failed chunk doesn't abort
// the rest — a partial seed beats none.
func addDocumentsChunked(indexName string, docs []map[string]any, chunkSize int) error {
	if meili == nil {
		return nil
	}
	if chunkSize <= 0 {
		chunkSize = 100
	}
	idx := meili.Index(indexName)
	var lastErr error
	for start := 0; start < len(docs); start += chunkSize {
		end := start + chunkSize
		if end > len(docs) {
			end = len(docs)
		}
		chunk := docs[start:end]
		var err error
		for attempt := 0; attempt < 4; attempt++ {
			if attempt > 0 {
				// 0.25s, 1s, 2.25s — eases off Meilisearch between retries.
				time.Sleep(time.Duration(attempt*attempt) * 250 * time.Millisecond)
			}
			if _, err = idx.AddDocuments(chunk, nil); err == nil {
				break
			}
		}
		if err != nil {
			lastErr = err
		}
		// Brief gap between chunks so a large seed doesn't burst the rate limit.
		time.Sleep(150 * time.Millisecond)
	}
	return lastErr
}

// ════════════════════════════════════════════════════════════════════
// Challenge-creation autocomplete
// ════════════════════════════════════════════════════════════════════
//
// Two surfaces:
//   * /api/v1/suggest/challenge-prefix — picks from the small curated
//     prefix list in vocabulary.go. No Meilisearch dependency since the
//     corpus is tiny and never grows from user input — we own every
//     phrase.
//   * /api/v1/suggest/challenge-subject — picks from the much larger
//     subject pool. Backed by a dedicated Meilisearch index that
//     blends:
//       1) The curated seed list (vocabulary.go)
//       2) Subjects extracted from existing challenges (real user
//          signal)
//       3) Per-subject usage counters that grow on every new challenge
//
// Ranking blends:
//   - Meilisearch's typo-tolerant prefix match (handles "danc" →
//     "dancing", "kpoo" → "kpop dancing")
//   - usage_count (popularity)
//   - user_affinity (boost subjects in categories the user already
//     engages with — see user_profiles.category_affinity)
//
// All of this is free: we already run Meilisearch for /search, and the
// seed list ships in the binary. Zero new infra, zero recurring cost.

// challengeSubjectsIndexName is the Meilisearch index ID. Kept as a
// constant so the index name only lives in one place — if we ever
// rename it (e.g. shard by language) every reference moves together.
const challengeSubjectsIndexName = "challenge_subjects"

// subjectUsageCache mirrors the usage_count column we maintain so the
// suggest endpoint can apply a popularity boost without an extra DB
// round-trip per request. Refreshed lazily on writes; not strongly
// consistent but more than good enough for ranking.
var (
	subjectUsageMu    sync.RWMutex
	subjectUsageCache = map[string]int{}
)

// initChallengeSubjectsIndex configures the Meilisearch index used for
// the subject autocomplete. Safe to call on every boot — Meilisearch's
// CreateIndex / UpdateXAttributes are idempotent and cheap.
//
// Why a separate index from `challenges`: the searchable surface is
// different. `challenges` indexes full posts (with creator, status,
// likes, etc.) and is tuned for "find me a challenge to engage with."
// `challenge_subjects` is a word-list autocomplete tuned for typing
// speed — different searchable attrs, different ranking rules,
// different ideal size. Conflating them would force ranker compromises.
func initChallengeSubjectsIndex() {
	if meili == nil {
		return
	}
	meili.CreateIndex(&meilisearch.IndexConfig{
		Uid:        challengeSubjectsIndexName,
		PrimaryKey: "id",
	})
	idx := meili.Index(challengeSubjectsIndexName)
	idx.UpdateSearchableAttributes(&[]string{"subject"})
	idx.UpdateSortableAttributes(&[]string{"usageCount", "lastUsedAt"})
	// Custom ranking: typo + words proximity first (Meilisearch
	// default for relevance), then break ties by popularity. Without
	// the usageCount sort, two equally-relevant entries fall back to
	// document ID which is meaningless to the user.
	idx.UpdateRankingRules(&[]string{
		"words", "typo", "proximity", "attribute", "exactness",
		"usageCount:desc",
	})
}

// seedChallengeSubjects pushes the curated seed list AND every distinct
// subject from existing challenges into Meilisearch. Runs once on boot
// (called from main.go after InitMeilisearch). Idempotent: documents
// keyed by lowercase subject — a re-run just bumps usageCount via the
// upsert without creating duplicates.
func seedChallengeSubjects() {
	if meili == nil {
		return
	}
	initChallengeSubjectsIndex()

	// Build the doc set: every curated subject + every distinct
	// subject already in the challenges table. Distinct because the
	// real signal lives in the usage count, not in row duplicates.
	combined := map[string]int{}
	for _, s := range challengeSubjectsRaw {
		key := strings.ToLower(strings.TrimSpace(s))
		if key == "" {
			continue
		}
		// Seed entries start at usage 1 so they outrank brand-new
		// (count 0) entries on tie but lose to anything with real
		// engagement.
		if _, ok := combined[key]; !ok {
			combined[key] = 1
		}
	}

	// Fold in DB-known subjects. Best-effort — if the query errors
	// (cold start before tables exist, etc.) we still seed the
	// curated list.
	if db != nil {
		rows, err := db.Query(
			`SELECT lower(trim(subject)) AS s, COUNT(*) AS n
			   FROM challenges
			  WHERE subject IS NOT NULL AND length(trim(subject)) > 0
			  GROUP BY lower(trim(subject))`,
		)
		if err == nil {
			defer rows.Close()
			for rows.Next() {
				var s string
				var n int
				if rows.Scan(&s, &n) != nil {
					continue
				}
				if combined[s] < n {
					combined[s] = n
				}
			}
		}
	}

	docs := make([]map[string]any, 0, len(combined))
	subjectUsageMu.Lock()
	for s, n := range combined {
		subjectUsageCache[s] = n
		docs = append(docs, map[string]any{
			"id":         subjectDocID(s),
			"subject":    s,
			"usageCount": n,
		})
	}
	subjectUsageMu.Unlock()

	// Push in bounded chunks with retry+backoff so the free-tier rate limit
	// (429) doesn't leave the index unseeded. Idempotent across reboots.
	if err := addDocumentsChunked(challengeSubjectsIndexName, docs, 100); err != nil {
		log.Printf("seedChallengeSubjects: AddDocuments partial/failed: %v", err)
		return
	}
	log.Printf("seeded %d challenge subjects into Meilisearch", len(docs))
}

// recordSubjectUsage bumps the usage_count for `subject` after a new
// challenge is created. Called from CreateChallenge so the autocomplete
// learns from real submissions without a nightly batch.
//
// Best-effort and async-safe: callers can fire-and-forget. We don't
// fail the parent create on an indexer hiccup; Meilisearch will catch
// up on the next boot's seedChallengeSubjects pass anyway.
func recordSubjectUsage(subject string) {
	subject = strings.ToLower(strings.TrimSpace(subject))
	if subject == "" {
		return
	}
	subjectUsageMu.Lock()
	subjectUsageCache[subject]++
	count := subjectUsageCache[subject]
	subjectUsageMu.Unlock()

	if meili == nil {
		return
	}
	meili.Index(challengeSubjectsIndexName).AddDocuments(
		[]map[string]any{{
			"id":         subjectDocID(subject),
			"subject":    subject,
			"usageCount": count,
		}},
		nil,
	)
}

// subjectDocID turns a free-text subject into a stable Meilisearch
// primary key. Meilisearch requires IDs that are alphanumeric or hyphens/
// underscores, so we drop everything else and prefix with "s_" to keep
// pure-numeric strings from colliding with auto-IDs elsewhere.
func subjectDocID(subject string) string {
	var b strings.Builder
	b.WriteString("s_")
	for _, r := range subject {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			b.WriteRune(r)
		case r >= 'A' && r <= 'Z':
			b.WriteRune(r + ('a' - 'A'))
		default:
			b.WriteByte('_')
		}
	}
	return b.String()
}

// SuggestChallengePrefixHandler — GET /api/v1/suggest/challenge-prefix
//
// Query params:
//   q     — partial prefix the user has typed (optional)
//   limit — max results (default 8, cap 20)
//
// Returns JSON: { "items": ["Who is better at", ...] }
//
// No Meilisearch hop — the curated list lives in vocabulary.go and is
// small enough that an in-memory match is faster than any network call.
func SuggestChallengePrefixHandler(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query().Get("q")
	limit := parseIntOrDefault(r.URL.Query().Get("limit"), 8, 20)
	items := SuggestPrefixes(q, limit)
	writeJSON(w, http.StatusOK, map[string]any{
		"items": items,
	})
}

// SuggestChallengeSubjectHandler — GET /api/v1/suggest/challenge-subject
//
// Query params:
//   q      — partial subject the user has typed (required for filtered
//            ranking; empty returns most popular)
//   userId — viewer id (optional). When present, results are re-ranked
//            so categories the user already engages with float higher.
//   limit  — max results (default 10, cap 30)
//
// Returns JSON: { "items": [{"subject":"dancing","usageCount":42}, ...] }
//
// Ranking layers:
//   1. Meilisearch typo+prefix relevance — base order
//   2. usageCount custom sort — popularity tiebreaker
//   3. User affinity boost (when userId given) — adds a per-row score
//      that lifts subjects in the user's top categories
//
// If Meilisearch is unavailable we fall back to a linear scan of the
// curated vocabulary. The UX degrades (no typo tolerance, no global
// popularity) but autocomplete keeps working — important for offline-
// development and emergency redeploys.
func SuggestChallengeSubjectHandler(w http.ResponseWriter, r *http.Request) {
	q := strings.TrimSpace(r.URL.Query().Get("q"))
	userID := r.URL.Query().Get("userId")
	limit := parseIntOrDefault(r.URL.Query().Get("limit"), 10, 30)

	items := suggestSubjectsRanked(q, userID, limit)
	writeJSON(w, http.StatusOK, map[string]any{
		"items": items,
	})
}

// suggestSubjectsRanked is the core ranker shared by the HTTP handler
// and tests. Returns a slice of {subject, usageCount} maps already
// sorted by final score.
func suggestSubjectsRanked(q, userID string, limit int) []map[string]any {
	// 1. Pull a candidate pool from Meilisearch (or fall back to the
	//    in-binary vocabulary when Meili is down).
	candidates := fetchSubjectCandidates(q, limit*3) // overfetch — we re-sort

	// 2. Affinity boost (cheap — we only compute it once per call).
	affinity := map[string]float64{}
	if userID != "" {
		affinity = loadCategoryAffinity(userID)
	}

	// 3. Re-rank with a composite score.
	type scored struct {
		subject    string
		usageCount int
		score      float64
	}
	pool := make([]scored, 0, len(candidates))
	for _, c := range candidates {
		score := float64(c.usageCount)
		// Affinity boost: if the subject "feels like" one of the
		// user's top categories, add a multiplicative kicker. The
		// mapping is intentionally fuzzy — we don't need a perfect
		// classifier here, just a "this looks like comedy/sport/etc."
		// hint to break ties in the user's favor.
		if len(affinity) > 0 {
			cat := guessSubjectCategory(c.subject)
			if cat != "" {
				score += 5.0 * affinity[cat]
			}
		}
		// Prefix match bonus: candidates whose first chars match q
		// jump ahead of substring/typo matches with the same usage.
		// Catches the "I typed 'dan' and want 'dancing' before
		// 'cake decorating'" case.
		if q != "" && strings.HasPrefix(c.subject, strings.ToLower(q)) {
			score += 10.0
		}
		pool = append(pool, scored{
			subject:    c.subject,
			usageCount: c.usageCount,
			score:      score,
		})
	}
	sort.SliceStable(pool, func(i, j int) bool {
		return pool[i].score > pool[j].score
	})
	if len(pool) > limit {
		pool = pool[:limit]
	}
	out := make([]map[string]any, len(pool))
	for i, p := range pool {
		out[i] = map[string]any{
			"subject":    p.subject,
			"usageCount": p.usageCount,
		}
	}
	return out
}

// subjectCandidate is the shape returned by fetchSubjectCandidates
// regardless of whether the data came from Meilisearch or the in-memory
// fallback.
type subjectCandidate struct {
	subject    string
	usageCount int
}

// fetchSubjectCandidates returns up to `limit` raw matches against `q`.
// Routes through Meilisearch when available; degrades to a linear scan
// of the curated vocabulary otherwise.
func fetchSubjectCandidates(q string, limit int) []subjectCandidate {
	if meili != nil {
		res, err := meili.Index(challengeSubjectsIndexName).Search(q,
			&meilisearch.SearchRequest{
				Limit: int64(limit),
			},
		)
		if err == nil && len(res.Hits) > 0 {
			out := make([]subjectCandidate, 0, len(res.Hits))
			for _, h := range res.Hits {
				doc := decodeHit(h)
				s, _ := doc["subject"].(string)
				if s == "" {
					continue
				}
				count := 0
				switch n := doc["usageCount"].(type) {
				case float64:
					count = int(n)
				case int:
					count = n
				}
				out = append(out, subjectCandidate{subject: s, usageCount: count})
			}
			return out
		}
	}
	// Fallback path. We scan the curated list and grab anything
	// matching q either as prefix or substring. Sorted by curated
	// order so the most evergreen entries float up first.
	q = strings.ToLower(strings.TrimSpace(q))
	subjectUsageMu.RLock()
	defer subjectUsageMu.RUnlock()
	out := make([]subjectCandidate, 0, limit)
	for _, s := range challengeSubjectsRaw {
		ls := strings.ToLower(s)
		if q == "" || strings.HasPrefix(ls, q) || strings.Contains(ls, q) {
			out = append(out, subjectCandidate{
				subject:    ls,
				usageCount: subjectUsageCache[ls],
			})
			if len(out) >= limit {
				break
			}
		}
	}
	return out
}

// loadCategoryAffinity reads the user's per-category affinity map from
// the user_profiles table. Used to boost subjects in categories the
// user already engages with.
//
// Returns an empty map (rather than an error) on any failure so the
// caller can treat "no data" identically to "user has no profile yet."
func loadCategoryAffinity(userID string) map[string]float64 {
	if userID == "" || db == nil {
		return map[string]float64{}
	}
	var raw string
	err := db.QueryRow(
		`SELECT COALESCE(category_affinity::text, '{}')
		   FROM user_profiles WHERE user_id = $1`,
		userID,
	).Scan(&raw)
	if err != nil {
		return map[string]float64{}
	}
	out := map[string]float64{}
	// A bad blob shouldn't poison the rank — drop the row and return
	// empty so we fall through to no-affinity ranking.
	_ = json.Unmarshal([]byte(raw), &out)
	return out
}

// guessSubjectCategory does a cheap keyword classification of a
// free-text subject into the existing category taxonomy. Not a model,
// just a rule table — but it's enough signal to break ties on the
// affinity boost.
//
// Adding new categories is a code-only change. Keep the keyword lists
// small (5-15 entries) so the linear scan stays fast.
func guessSubjectCategory(subject string) string {
	s := strings.ToLower(subject)
	contains := func(needles ...string) bool {
		for _, n := range needles {
			if strings.Contains(s, n) {
				return true
			}
		}
		return false
	}
	switch {
	case contains("prank", "joke", "roast", "meme", "comedy", "impression", "satire", "parody"):
		return "comedy"
	case contains("danc", "ballet", "salsa", "bachata", "kpop", "freestyle"):
		return "dance"
	case contains("sing", "rap", "beatbox", "karaoke", "music", "song", "guitar", "piano", "drum", "violin", "djing", "produc"):
		return "music"
	case contains("basketball", "football", "soccer", "tennis", "boxing", "mma", "swim", "run", "sprint", "marathon", "weightlift", "cycle", "skateboard", "surf", "ski"):
		return "sports"
	case contains("fortnite", "minecraft", "valorant", "league of legends", "csgo", "apex", "warzone", "pubg", "fifa", "speedrun"):
		return "gaming"
	case contains("draw", "paint", "sketch", "art", "anim", "sculpt", "calligraph", "photograph", "videograph", "cinemat", "edit", "vfx", "poetry"):
		return "art"
	case contains("cook", "bak", "pastry", "coffee", "barista", "cocktail", "bbq", "pizza", "ramen", "sushi"):
		return "food"
	case contains("outfit", "thrift", "ootd", "fashion", "streetwear", "sneaker", "makeup", "skincare", "nail"):
		return "fashion"
	case contains("yoga", "pilates", "meditat", "mindful", "stretch", "calisthenic", "wellness", "breath"):
		return "lifestyle"
	case contains("cod", "leetcode", "hack", "design", "uiux", "ui design", "ux flow", "3d print", "arduino", "raspberry", "robot", "drone"):
		return "tech"
	case contains("scary", "horror", "ghost", "haunt", "paranormal", "true crime"):
		return "horror"
	}
	return ""
}
