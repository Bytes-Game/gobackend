package main

import (
	"os"
	"strings"
	"testing"
)

// ════════════════════════════════════════════════════════════════════════════
// MAKING "WHAT IS THIS VIDEO ABOUT" A QUESTION THE DATABASE CAN ANSWER
// ════════════════════════════════════════════════════════════════════════════
//
// The model writes what a video is about in its own words. Those went into the
// video_analysis JSON blob, where nothing could query them — so the richest
// thing known about a video was write-only, and the feed decided what to show
// you from one word out of a list of eighteen.
//
// These guard the column that changes that, and the one mistake that would
// make it useless without looking broken.

func TestTopics_AreWrittenToTheirOwnColumn(t *testing.T) {
	// Inside the JSON blob, "which videos share this topic" means parsing every
	// row. That is the question the whole tag-matching approach is built on, so
	// it has to be answerable in SQL.
	src, err := os.ReadFile("video_analysis.go")
	if err != nil {
		t.Fatalf("read video_analysis.go: %v", err)
	}
	s := string(src)
	if !strings.Contains(s, "content_topics = $4") {
		t.Error("storeVideoAnalysis no longer writes content_topics. Topics " +
			"stay buried in the JSON blob, where nothing can match on them.")
	}
	if !strings.Contains(s, "normalizeTags(a.Topics)") {
		t.Error("topics are stored without being folded. Then \"Close-Up\" and " +
			"\"close up\" are two different topics and the two videos never " +
			"look related.")
	}
}

func TestTopics_ShareOneDefinitionOfShape(t *testing.T) {
	// THE TRAP THIS FILE EXISTS FOR.
	//
	// The natural thing for the migration to do is lift topics out of
	// video_analysis for rows that already have them. Doing that means writing
	// the folding rules a SECOND time, in SQL — and anything slightly off
	// produces topics that look correct and match nothing.
	//
	// "close-up" is a real topic on video 126 today. Folded in Go it becomes
	// "close up". A SQL backfill that only lowercased would store "close-up",
	// and those two videos would look unrelated forever, for a reason invisible
	// from either the data or the code.
	//
	// Migrations here are also fatal on failure: a bad data transform stops the
	// backend booting rather than just skipping.
	body, err := os.ReadFile("migrations/006_topics_become_queryable.sql")
	if err != nil {
		t.Fatalf("read migration: %v", err)
	}
	sql := strings.ToUpper(string(body))
	if strings.Contains(sql, "UPDATE CHALLENGES") || strings.Contains(sql, "UPDATE CHALLENGE_RESPONSES") {
		t.Error("the migration backfills topics, which means the folding rules " +
			"now exist in both Go and SQL. Two definitions of the same rule " +
			"disagree on the first topic containing a hyphen, and the result " +
			"is topics that silently match nothing. Let the worker fill the " +
			"column through normalizeTags instead.")
	}
}

func TestTopics_AreIndexedForTheQuestionTheyExistToAnswer(t *testing.T) {
	// "Which videos share this topic" over a JSONB column without a GIN index
	// reads every row. That is the query the feed will run for every video it
	// scores.
	body, err := os.ReadFile("migrations/006_topics_become_queryable.sql")
	if err != nil {
		t.Fatalf("read migration: %v", err)
	}
	s := string(body)
	for _, want := range []string{
		"ADD COLUMN IF NOT EXISTS content_topics JSONB",
		"USING GIN (content_topics)",
	} {
		if !strings.Contains(s, want) {
			t.Errorf("migration is missing %q", want)
		}
	}
	// Both tables, or responses can never be matched on what they are about.
	if !strings.Contains(s, "ALTER TABLE challenge_responses") {
		t.Error("challenge_responses never gets the column, so a response can " +
			"never be matched to anything by what it is about")
	}
}

func TestTopics_SurviveFoldingIntact(t *testing.T) {
	// Topics are phrases, not single words, and the folding was written for
	// single-word tags. A rule that dropped spaces would turn every topic into
	// one unmatchable run of letters.
	for in, want := range map[string]string{
		"street food":                "street food",
		"long distance relationship": "long distance relationship",
		"dark fantasy":               "dark fantasy",
		"hanuman chalisa":            "hanuman chalisa",
		"close-up":                   "close up",
		"thistle":                    "thistle",
	} {
		if got := normalizeOneTag(in); got != want {
			t.Errorf("normalizeOneTag(%q) = %q, want %q — a topic that does not "+
				"survive folding is one no two videos can ever share", in, got, want)
		}
	}
}

func TestTopics_AreNotFilteredAgainstTheCategoryList(t *testing.T) {
	// The whole point. auto_tags is a CLOSED vocabulary — anything the backend
	// does not define is dropped, which is what stops a model inventing tags.
	// Topics are the opposite: open vocabulary, no list, whatever fits.
	//
	// Running them through that filter would delete "thistle" and "jellyfish"
	// and leave nothing, which is exactly the eighteen-box problem this is
	// meant to escape.
	open := []string{"thistle", "jellyfish", "dark fantasy", "hanuman chalisa"}
	got := normalizeTags(open)
	if len(got) != len(open) {
		t.Fatalf("got %v, want all %d kept. Topics are not checked against the "+
			"category list — if they were, everything specific enough to be "+
			"useful would be dropped.", got, len(open))
	}
	for _, tag := range got {
		if categoryFromTags([]string{tag}) != "" && tag != "dark fantasy" {
			continue // an incidental category match is fine, it just isn't required
		}
	}
}
