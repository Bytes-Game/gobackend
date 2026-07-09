package main

import (
	"encoding/json"
	"log"
	"os"

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
	ci.UpdateSearchableAttributes(&[]string{"prefix", "subject", "creatorUsername", "category", "emotionTags"})
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
	challenges := GetArenaChallenges()
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
	}
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
