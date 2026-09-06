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

// ════════════════════════════════════════════════════════════════════════════
// TELLING THE TRUTH ABOUT WHERE RESULTS CAME FROM
// ════════════════════════════════════════════════════════════════════════════

func TestSearch_SaysWhichKindOfFallbackItGave(t *testing.T) {
	// A search with no exact hits can now be answered two very different ways,
	// and the app renders a banner from the answer.
	//
	// "aquarium" finding the jellyfish video has genuinely answered the
	// question. Calling that "trending now" — which is what the one shared
	// flag used to make the app say — tells the user the app did not
	// understand them, when it understood perfectly.
	src, err := os.ReadFile("search.go")
	if err != nil {
		t.Fatalf("read search.go: %v", err)
	}
	s := string(src)
	if !strings.Contains(s, `resp.RelatedKind = "subjects"`) {
		t.Error("a near-subject rescue is not labelled, so the app cannot " +
			"tell it apart from a trending fallback and will claim these " +
			"genuinely related videos are merely popular")
	}
	if !strings.Contains(s, `resp.RelatedKind = "trending"`) {
		t.Error("the trending rescue is not labelled")
	}
	// Both must set Related too, or the app renders no banner at all and
	// presents a fallback as if it were an exact match.
	if strings.Count(s, "resp.Related = true") != 2 {
		t.Errorf("expected exactly two rescue paths to set Related, found %d — "+
			"a rescue that does not set it is presented as a real match",
			strings.Count(s, "resp.Related = true"))
	}
}

func TestSearch_SubjectRescueIsTriedBeforeTrending(t *testing.T) {
	// Order matters and is easy to get backwards. Trending always returns
	// something, so if it runs first the near-subject path can never fire and
	// "aquarium" gets popular videos forever.
	src, err := os.ReadFile("search.go")
	if err != nil {
		t.Fatalf("read search.go: %v", err)
	}
	s := string(src)
	subjects := strings.Index(s, "searchNearbySubjects(query)")
	trending := strings.Index(s, "searchZeroResultRescue(resp.Intent)")
	if subjects < 0 || trending < 0 {
		t.Fatal("could not find both rescue paths")
	}
	if subjects > trending {
		t.Error("trending is tried before near subjects. Trending always " +
			"returns something, so the subject path would never run.")
	}
}

func TestSearch_SuggestionsOnlyNameThingsTheAppHas(t *testing.T) {
	// A suggestion chip that leads to an empty page is worse than no chip.
	// These come from the topic graph, which is built from videos that exist,
	// so every suggestion is by construction something the catalogue has.
	src, err := os.ReadFile("topic_graph.go")
	if err != nil {
		t.Fatalf("read topic_graph.go: %v", err)
	}
	if !strings.Contains(string(src), "FROM challenges") {
		t.Error("suggestions are not derived from real videos, so a chip " +
			"could name a subject nothing is about")
	}
}

func TestSearch_AnAccountHitDoesNotHideTheVideos(t *testing.T) {
	// Found by running the live search rather than by reading the code.
	//
	//   /search?q=jellyfish
	//   accounts: 10  (cyberking, stormchaser, shadowstrike, frostbyte, ...)
	//   videos:    0
	//
	// None of those usernames contains "jellyfish". The accounts lane returns
	// ten users for any query, including pure nonsense — a separate problem in
	// code this change does not touch. But its effect here was total: the
	// rescue was gated on accounts being empty too, so it never ran, and a
	// person asking about jellyfish got ten strangers and none of the four
	// jellyfish videos this app is holding.
	//
	// Somebody searching a word is asking about that word. Whether an account
	// also turned up says nothing about whether we should answer.
	src, err := os.ReadFile("search.go")
	if err != nil {
		t.Fatalf("read search.go: %v", err)
	}
	s := string(src)
	if strings.Contains(s, "if len(resp.Accounts) == 0 && len(resp.Battles) == 0 && len(resp.Shorts) == 0 {") {
		t.Error("the video rescue is gated on the accounts lane being empty. " +
			"One irrelevant account hit and the app refuses to answer a " +
			"question it can answer.")
	}
	if !strings.Contains(s, "if len(resp.Battles) == 0 && len(resp.Shorts) == 0 {") {
		t.Error("the video rescue is no longer gated on there being no videos")
	}
}

func TestSearch_TrendingStaysTheLastResort(t *testing.T) {
	// The other half of the same decision. Near subjects are a real answer and
	// should fire freely; trending is "here is what is popular", and padding a
	// perfectly good username search with unrelated videos is just noise.
	//
	// So trending keeps the stricter gate: nothing matched at all.
	src, err := os.ReadFile("search.go")
	if err != nil {
		t.Fatalf("read search.go: %v", err)
	}
	if !strings.Contains(string(src),
		"if len(resp.Accounts) == 0 && len(resp.Challenges) == 0 {") {
		t.Error("the trending fallback no longer requires that nothing at all " +
			"matched, so a successful account search now gets unrelated " +
			"videos stapled underneath it")
	}
}
