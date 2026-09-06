package main

import (
	"encoding/json"
	"log"
	"os"
	"strings"

	"github.com/meilisearch/meilisearch-go"
)

// meili is the global Meilisearch client. Nil if not configured.
var meili meilisearch.ServiceManager

// InitMeilisearch sets up the Meilisearch client and creates indexes.
// Falls back gracefully if MEILISEARCH_URL is not set.
func InitMeilisearch() {
	url := os.Getenv("MEILISEARCH_URL")
	key := os.Getenv("MEILI_MASTER_KEY")
	if url == "" {
		log.Println("MEILISEARCH_URL not set — search will use PostgreSQL fallback")
		return
	}

	meili = meilisearch.New(url, meilisearch.WithAPIKey(key))

	// Create indexes
	meili.CreateIndex(&meilisearch.IndexConfig{Uid: "challenges", PrimaryKey: "id"})
	meili.CreateIndex(&meilisearch.IndexConfig{Uid: "users", PrimaryKey: "id"})

	// Configure challenges index. Searchable order matters — Meili
	// weights earlier attributes higher, so the title parts stay first
	// and the enrichment fields (category, emotion tags) act as
	// secondary match surfaces ("funny" finds emotion-tagged content
	// even when no title contains the word).
	ci := meili.Index("challenges")
	// Order is weight: Meili scores an earlier attribute higher.
	//
	// "topics" sits right behind the title because it is what the video is
	// ACTUALLY about — a jellyfish clip has the word "jellyfish" nowhere else,
	// and before this it could not be found at all. "spoken" is last on
	// purpose: a word said once in passing should never outrank a video whose
	// title is that word.
	ci.UpdateSearchableAttributes(&[]string{
		"prefix", "subject", "topics", "creatorUsername",
		"tags", "category", "emotionTags", "spoken",
	})
	filterAttrs := []interface{}{"visibility", "status"}
	ci.UpdateFilterableAttributes(&filterAttrs)
	ci.UpdateSortableAttributes(&[]string{"views", "likes", "engagementScore"})
	// Custom ranking rule: after lexical relevance (words/typo/proximity/
	// attribute), break ties by engagement instead of arbitrary order —
	// the SHORTLIST Meili returns is what the Go reranker sees, so a
	// better-ordered shortlist raises the ceiling of the whole pipeline.
	ci.UpdateRankingRules(&[]string{
		"words", "typo", "proximity", "attribute",
		"engagementScore:desc", "sort", "exactness",
	})

	// Configure users index
	ui := meili.Index("users")
	ui.UpdateSearchableAttributes(&[]string{"username", "fullName", "league"})
	ui.UpdateSortableAttributes(&[]string{"followers"})

	log.Println("Meilisearch initialized")

	// Seed existing data into indexes
	go seedMeilisearchData()
}

// seedMeilisearchData loads all existing challenges and users into Meilisearch.
func seedMeilisearchData() {
	// Index all users
	users := GetAllUsers()
	if len(users) > 0 {
		docs := make([]map[string]interface{}, len(users))
		for i, u := range users {
			docs[i] = map[string]interface{}{
				"id":        u.ID,
				"username":  u.Username,
				"fullName":  u.FullName,
				"league":    u.League,
				"followers": u.Followers,
				"wins":      u.Wins,
				"losses":    u.Losses,
			}
		}
		meili.Index("users").AddDocuments(docs, nil)
		log.Printf("Indexed %d users in Meilisearch", len(users))
	}

	// Index all arena challenges
	challenges := GetSearchableChallenges()
	if len(challenges) > 0 {
		docs := make([]map[string]interface{}, len(challenges))
		for i, c := range challenges {
			docs[i] = challengeToMeiliDoc(c)
		}
		meili.Index("challenges").AddDocuments(docs, nil)
		log.Printf("Indexed %d challenges in Meilisearch", len(challenges))
	}
}

// challengeToMeiliDoc converts a Challenge to a Meilisearch document.
func challengeToMeiliDoc(c Challenge) map[string]interface{} {
	topics, spoken := challengeSearchText(c.ID)
	return map[string]interface{}{
		"id":              c.ID,
		"creatorId":       c.CreatorID,
		"creatorUsername":  c.CreatorUsername,
		"creatorLeague":   c.CreatorLeague,
		"prefix":          c.Prefix,
		"subject":         c.Subject,
		"title":           c.Prefix + " " + c.Subject,
		"visibility":      c.Visibility,
		"status":          c.Status,
		"likes":           c.Likes,
		"views":           c.Views,
		"responseCount":   c.ResponseCount,
		"videoUrl":        c.VideoURL,
		"thumbnailUrl":    c.ThumbnailURL,
		"createdAt":       c.CreatedAt,
		// Enrichment: secondary match surfaces + the ranking-rule metric.
		// Backfill is free — seedMeilisearchData re-upserts every boot.
		"category":        c.Category,
		"emotionTags":     c.EmotionTags,
		"engagementScore": c.Views + 5*c.Likes,
		// What the video is about and what is said in it. Everything the
		// worker learned by reading, listening and looking was stored and
		// then unreachable from search: somebody who says "biryani" out loud
		// could not be found by searching biryani, and the jellyfish clip had
		// that word in no searchable field at all.
		"topics": topics,
		"tags":   c.Tags,
		"spoken": spoken,
	}
}

// challengeSearchText fetches what a video is about and what is said in it.
//
// Read at index time rather than carried on the Challenge struct, because
// every query that builds a Challenge would otherwise have to load two more
// columns for a value only search uses.
//
// Silent on every failure. A video that has not been analysed yet — which is
// most of them, briefly — simply has nothing extra to match on, and search
// must not break because a description is missing.
func challengeSearchText(id string) (topics []string, spoken string) {
	if db == nil {
		return nil, ""
	}
	var topicsJSON, analysisJSON []byte
	err := db.QueryRow(`
		SELECT COALESCE(content_topics, '[]'::jsonb), video_analysis
		  FROM challenges WHERE CAST(id AS TEXT) = $1`, id).Scan(&topicsJSON, &analysisJSON)
	if err != nil {
		return nil, ""
	}
	_ = json.Unmarshal(topicsJSON, &topics)
	if len(analysisJSON) > 0 {
		var a VideoAnalysis
		if json.Unmarshal(analysisJSON, &a) == nil {
			// Screen text as well as speech: a caption burned onto the video
			// is often the clearest statement of what it is, and plenty of
			// videos have one and no sound at all.
			spoken = strings.TrimSpace(a.ScreenText + " " + a.Speech)
		}
	}
	return topics, spoken
}

// IndexChallenge adds or updates a challenge in Meilisearch.
func IndexChallenge(c Challenge) {
	if meili == nil {
		return
	}
	meili.Index("challenges").AddDocuments([]map[string]interface{}{challengeToMeiliDoc(c)}, nil)
}

// IndexUser adds or updates a user in Meilisearch.
func IndexUser(u User) {
	if meili == nil {
		return
	}
	meili.Index("users").AddDocuments([]map[string]interface{}{{
		"id":        u.ID,
		"username":  u.Username,
		"fullName":  u.FullName,
		"league":    u.League,
		"followers": u.Followers,
		"wins":      u.Wins,
		"losses":    u.Losses,
	}}, nil)
}

// decodeHit converts a meilisearch.Hit (map[string]json.RawMessage) to map[string]interface{}.
func decodeHit(hit meilisearch.Hit) map[string]interface{} {
	doc := make(map[string]interface{}, len(hit))
	for k, raw := range hit {
		var val interface{}
		if err := json.Unmarshal(raw, &val); err == nil {
			doc[k] = val
		}
	}
	return doc
}

// MeiliSearchAll performs a unified search across challenges and users.
// Returns results as maps with a "_type" field ("challenge" or "user").
func MeiliSearchAll(query string, searchType string) []map[string]interface{} {
	if meili == nil {
		return nil
	}

	var results []map[string]interface{}

	if searchType == "" || searchType == "all" || searchType == "challenges" {
		res, err := meili.Index("challenges").Search(query, &meilisearch.SearchRequest{
			Limit: 20,
		})
		if err == nil {
			for _, hit := range res.Hits {
				doc := decodeHit(hit)
				doc["_type"] = "challenge"
				results = append(results, doc)
			}
		}
	}

	if searchType == "" || searchType == "all" || searchType == "users" {
		res, err := meili.Index("users").Search(query, &meilisearch.SearchRequest{
			Limit: 20,
		})
		if err == nil {
			for _, hit := range res.Hits {
				doc := decodeHit(hit)
				doc["_type"] = "user"
				results = append(results, doc)
			}
		}
	}

	return results
}
