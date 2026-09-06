package main

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

// The bug this guards against did not look like a bug. Search "pollination"
// and you got a page of unrelated videos — which reads as bad ranking, not as
// the search answering a different question than the suggestion asked.
//
// The cause was three places disagreeing about which videos exist.
func TestSearchable_TheThreePlacesAgreeOnWhatIsFindable(t *testing.T) {
	callers := []struct {
		file, what string
	}{
		{"database.go", "the list of results search returns"},
		{"search_relevance.go", "the text index search ranks with"},
		{"topic_graph.go", "the graph that suggests related subjects"},
	}
	for _, c := range callers {
		src, err := os.ReadFile(c.file)
		if err != nil {
			t.Fatalf("cannot read %s: %v", c.file, err)
		}
		if !strings.Contains(string(src), "searchableWhere(") {
			t.Errorf("%s (%s) no longer asks searchableWhere which videos are "+
				"findable. When these drift apart the app suggests a subject "+
				"and then cannot find it, which looks like broken ranking "+
				"rather than a disagreement about the catalogue.", c.file, c.what)
		}
	}
}

// A hand-written arena filter anywhere in the search path is the drift
// starting again.
func TestSearchable_NobodyWritesTheirOwnArenaFilter(t *testing.T) {
	// Matches visibility = 'arena' however it is spaced or aliased.
	own := regexp.MustCompile(`(?i)\w*\.?visibility\s*=\s*'arena'`)
	for _, f := range []string{"database.go", "search_relevance.go", "topic_graph.go", "search.go"} {
		src, err := os.ReadFile(f)
		if err != nil {
			t.Fatalf("cannot read %s: %v", f, err)
		}
		lines := strings.Split(string(src), "\n")
		for i, line := range lines {
			if !own.MatchString(line) {
				continue
			}
			trimmed := strings.TrimSpace(line)
			// A comment explaining the rule is fine.
			if strings.HasPrefix(trimmed, "//") {
				continue
			}
			// A query that deliberately asks a different question is fine
			// too, but it has to say so in words, near the query, so the
			// next person reads a decision instead of guessing at one.
			if statesItIsNotTheSearchPopulation(lines, i) {
				continue
			}
			t.Errorf("%s:%d writes its own arena filter:\n\t%s\nUse "+
				"searchableWhere() so this cannot disagree with the others.",
				f, i+1, trimmed)
		}
	}
}

// The graph's population must not be wider than what search can return. If it
// is, every extra video is a subject the app can suggest and then fail to find.
func TestSearchable_TheGraphIsNotWiderThanTheResults(t *testing.T) {
	src, err := os.ReadFile("topic_graph.go")
	if err != nil {
		t.Fatalf("cannot read topic_graph.go: %v", err)
	}
	s := string(src)

	// The graph query must be filtered at all — the original bug was that it
	// read every row in the table.
	q := s[strings.Index(s, "SELECT COALESCE(content_topics"):]
	if end := strings.Index(q, "`)"); end > 0 {
		q = q[:end]
	}
	if !strings.Contains(q, "searchableWhere") {
		t.Error("the related-subject graph reads challenges without limiting " +
			"itself to findable ones. It will learn subjects from drafts and " +
			"private uploads, offer them as suggestions, and send people to " +
			"an empty or irrelevant page when they tap one.")
	}
}

func TestSearchable_TheDefinitionSurvivesAnAlias(t *testing.T) {
	plain := searchableWhere("")
	aliased := searchableWhere("c")

	if strings.Contains(plain, "c.") {
		t.Errorf("unaliased form leaked an alias: %q", plain)
	}
	for _, col := range []string{"visibility", "status"} {
		if !strings.Contains(aliased, "c."+col) {
			t.Errorf("aliased form does not qualify %q, so it will be "+
				"ambiguous in a joined query: %q", col, aliased)
		}
	}
	// Same rule either way, just qualified.
	if strings.Count(plain, "AND") != strings.Count(aliased, "AND") {
		t.Error("the aliased and unaliased forms are not the same rule")
	}
}

// statesItIsNotTheSearchPopulation looks back for a comment saying, in words,
// that this query means to differ from what search can find.
func statesItIsNotTheSearchPopulation(lines []string, at int) bool {
	from := at - 12
	if from < 0 {
		from = 0
	}
	for _, l := range lines[from:at] {
		if strings.Contains(l, "not the search population") {
			return true
		}
	}
	return false
}
