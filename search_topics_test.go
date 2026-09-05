package main

import (
	"os"
	"strings"
	"testing"
)

// ════════════════════════════════════════════════════════════════════════════
// EVERYTHING THE WORKER LEARNED, REACHABLE FROM SEARCH
// ════════════════════════════════════════════════════════════════════════════
//
// The worker reads what is said, reads what is on screen, and looks at the
// frames. All of it was stored and none of it was searchable: the index held
// prefix, subject, creator name, category and emotion tags, so a jellyfish
// clip had the word "jellyfish" in no searchable field at all and somebody who
// said "biryani" out loud could not be found by searching biryani.

func TestSearch_IndexesWhatTheVideoIsAbout(t *testing.T) {
	src, err := os.ReadFile("meilisearch.go")
	if err != nil {
		t.Fatalf("read meilisearch.go: %v", err)
	}
	s := string(src)
	for _, field := range []string{`"topics"`, `"tags"`, `"spoken"`} {
		if !strings.Contains(s, "\t\t"+field[:len(field)-1]+`":`) &&
			!strings.Contains(s, field+",") {
			t.Errorf("%s is not in the search index, so nothing the worker "+
				"learned about a video can be searched for", field)
		}
	}
}

func TestSearch_TitleStillOutranksAPassingMention(t *testing.T) {
	// Meili weights earlier searchable attributes higher. A word said once in
	// passing must not beat a video whose TITLE is that word, or every search
	// returns whatever happens to have the longest transcript.
	src, err := os.ReadFile("meilisearch.go")
	if err != nil {
		t.Fatalf("read meilisearch.go: %v", err)
	}
	s := string(src)
	start := strings.Index(s, "UpdateSearchableAttributes(&[]string{\n\t\t\"prefix\"")
	if start < 0 {
		t.Fatal("could not find the challenges searchable-attribute order")
	}
	order := s[start : start+220]
	posOf := func(w string) int { return strings.Index(order, `"`+w+`"`) }
	subject, topics, spoken := posOf("subject"), posOf("topics"), posOf("spoken")
	if subject < 0 || topics < 0 || spoken < 0 {
		t.Fatalf("missing attributes in order: %q", order)
	}
	if !(subject < topics && topics < spoken) {
		t.Errorf("attribute order is wrong. Wanted title, then what it is "+
			"about, then what was said. Got: %q", order)
	}
}

func TestSearch_ReindexesAfterAnalysis(t *testing.T) {
	// THE BUG THIS FILE EXISTS FOR.
	//
	// A challenge is indexed once, at upload — minutes BEFORE it is
	// transcribed, read and looked at. So the copy search holds never had any
	// of it. Adding topics to the index does nothing on its own; something has
	// to put the video back in once the topics exist.
	src, err := os.ReadFile("video_analysis.go")
	if err != nil {
		t.Fatalf("read video_analysis.go: %v", err)
	}
	s := string(src)
	if !strings.Contains(s, "reindexChallengeForSearch") {
		t.Error("nothing re-indexes a video after its analysis is stored. " +
			"Search keeps the copy made at upload time, which has no topics, " +
			"no machine tags and nothing that was said — so the entire " +
			"reading and looking pipeline stays invisible to search forever.")
	}
	// After the write, not before: indexing a row we then failed to save would
	// make search advertise a description the database does not have.
	save := strings.Index(s, "content_topics = $4")
	reindex := strings.Index(s, "go reindexChallengeForSearch")
	if save < 0 || reindex < 0 || reindex < save {
		t.Error("the re-index does not happen after the row is written")
	}
}

func TestSearch_FallbackFindsSubjectsToo(t *testing.T) {
	// When Meilisearch is down the fallback is the whole search. It could only
	// match a title or a username, so "jellyfish" returned nothing while the
	// app knew exactly which video that was.
	src, err := os.ReadFile("search.go")
	if err != nil {
		t.Fatalf("read search.go: %v", err)
	}
	s := string(src)
	if !strings.Contains(s, "challengeIDsAboutTopic") {
		t.Error("the Postgres fallback still matches only titles and usernames")
	}
	if !strings.Contains(s, "ILIKE '%' || $1 || '%'") {
		t.Error("topic matching is exact rather than substring. Topics are " +
			"phrases, so \"food\" would never find \"street food\".")
	}
}

func TestSearch_FallbackAsksForColumnsThatExist(t *testing.T) {
	// The #56 lesson, again. A missing column does not return blank — Postgres
	// rejects the statement, and here that means search silently finds nothing
	// whenever Meilisearch is also down.
	src, err := os.ReadFile("search.go")
	if err != nil {
		t.Fatalf("read search.go: %v", err)
	}
	s := string(src)
	if strings.Contains(s, "FROM challenges") && !strings.Contains(s, "content_topics") {
		t.Error("the fallback query does not name content_topics")
	}
	schema, err := os.ReadFile("migrations/006_topics_become_queryable.sql")
	if err != nil {
		t.Fatalf("read migration 006: %v", err)
	}
	if !strings.Contains(string(schema), "ALTER TABLE challenges") {
		t.Error("challenges never gets content_topics, so the search fallback " +
			"asks for a column that does not exist")
	}
}

// ════════════════════════════════════════════════════════════════════════════
// AND WHO COUNTS AS A SIMILAR PERSON
// ════════════════════════════════════════════════════════════════════════════

func TestCollaborative_ComparesPeopleBySubjectNotCategory(t *testing.T) {
	// Eighteen categories is far too coarse to say two people share a taste.
	// Somebody who watches only wildlife and somebody who watches only stand-up
	// both register as "comedy" if that is what the uploads happened to be
	// filed under — and the app then recommends each of them the other's
	// videos on the strength of it.
	src, err := os.ReadFile("collaborative.go")
	if err != nil {
		t.Fatalf("read collaborative.go: %v", err)
	}
	s := string(src)
	if !strings.Contains(s, "topic_affinity") {
		t.Error("similar users are still found by comparing eighteen " +
			"categories, so \"you both like videos\" counts as the same taste")
	}
	if !strings.Contains(s, "ELSE category_affinity END") {
		t.Error("no fallback to category affinity. A user whose profile has " +
			"not rebuilt since topics existed has an empty topic map and " +
			"would drop out of collaborative filtering entirely.")
	}
}

// ════════════════════════════════════════════════════════════════════════════
// THE TRANSCRIPT IS REAL EVIDENCE, NOT DECORATION
// ════════════════════════════════════════════════════════════════════════════

func TestSpeech_ReachesTheThingsThatJudgeAVideo(t *testing.T) {
	// Speech matters more the longer a video is. Today's clips are short so it
	// contributes little, but the path has to be complete now or a longer
	// upload later would still be judged on nothing.
	//
	// Three consumers, and all three must see it.
	for _, c := range []struct{ file, needs, why string }{
		{"cmd/hls-worker/understand.go", "a.ScreenText + \"\\n\" + a.Speech",
			"the model that decides what a video is about is not shown what " +
				"was said in it"},
		{"video_analysis.go", "a.ScreenText + \" \" + a.Speech",
			"the backend's own text view of a video drops the transcript"},
		{"meilisearch.go", "a.ScreenText + \" \" + a.Speech",
			"what somebody said out loud is not searchable, so a video about " +
				"biryani cannot be found by searching biryani"},
	} {
		src, err := os.ReadFile(c.file)
		if err != nil {
			t.Fatalf("read %s: %v", c.file, err)
		}
		if !strings.Contains(string(src), c.needs) {
			t.Errorf("%s: %s", c.file, c.why)
		}
	}
}

func TestCreatorTagsStillCount(t *testing.T) {
	// The machine is preferred, not trusted alone. When it cannot tell, the
	// creator's word is what is left — and dropping it would empty the
	// catalogue's categories, since the model declines on most videos.
	got := categoryFromEvidence(
		[]string{"talking"}, // model ran, no category among its answers
		nil, "food", "", "", "")
	if got.Category != "food" || got.Source != "creator" {
		t.Errorf("got %q from %q; the creator's word has to survive the model "+
			"having no opinion", got.Category, got.Source)
	}
}
