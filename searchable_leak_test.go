package main

import (
	"os"
	"strings"
	"testing"
)

// ════════════════════════════════════════════════════════════════════════════
// A PRIVATE VIDEO'S TITLE MUST NOT REACH A STRANGER
// ════════════════════════════════════════════════════════════════════════════
//
// A challenge posted with visibility "friends" is titled "who is better at
// <something the person chose to keep private>". Three places were handing
// that text, or the video itself, to people who are not their friends:
//
//	seedChallengeSubjects   every subject in the table into the autocomplete
//	                        index behind /suggest/challenge-subject, which is
//	                        a PUBLIC route — no sign-in required
//	recordSubjectUsage      the same, immediately, on every create; the caller
//	                        checks the visibility two lines earlier to decide
//	                        who to notify, then ignored it here
//	meiliSearchChallenges   search hits with no filter at all, feeding both
//	                        the results page and the feed
//
// The third was dormant, not harmless: the deployed server runs without
// Meilisearch, so that path returns nothing today and would have started
// leaking the day somebody switched it on.
//
// All three are the same bug searchable_population.go was written to end. That
// fix reached three callers and these were not among them, which is why the
// count is checked here rather than remembered.

func TestLeak_TheAutocompleteOnlyLearnsFindableSubjects(t *testing.T) {
	src := readSourceFile(t, "suggest_handlers.go")

	// The seed query, up to its GROUP BY.
	//
	// Not "up to the next backtick" — the filter is CONCATENATED in, so it
	// sits after the first raw string ends. A first version of this check cut
	// there and reported the leak still open after it had been closed.
	start := strings.Index(src, "SELECT lower(trim(subject))")
	if start < 0 {
		t.Fatal("the subject seed query is no longer in this file")
	}
	q := src[start:]
	if end := strings.Index(q, "GROUP BY lower(trim(subject))"); end > 0 {
		q = q[:end]
	}
	if !strings.Contains(q, "searchableWhere") {
		t.Error("seedChallengeSubjects reads every subject in the table, " +
			"including friends-only ones, and pushes them into the index " +
			"behind a public route. Somebody's private title becomes an " +
			"autocomplete suggestion for strangers.")
	}

	// The per-create path.
	if !strings.Contains(src, "func recordSubjectUsage(subject, visibility, status string)") {
		t.Error("recordSubjectUsage no longer takes the visibility, so it " +
			"cannot tell a private challenge from a public one and will " +
			"index both")
	}
	body := bodyOfGo(t, src, "func recordSubjectUsage(")
	if !strings.Contains(body, "isSearchable(visibility, status)") {
		t.Error("recordSubjectUsage does not check isSearchable, so a " +
			"friends-only challenge's title reaches the public autocomplete " +
			"the moment it is posted")
	}
}

func TestLeak_TheCallerHandsOverTheVisibility(t *testing.T) {
	src := readSourceFile(t, "challenge_handler.go")
	if !strings.Contains(src,
		"recordSubjectUsage(challenge.Subject, challenge.Visibility, challenge.Status)") {
		t.Error("the create handler does not pass the visibility to the " +
			"autocomplete indexer. It checks the same field two lines earlier " +
			"to decide who to notify, so the information is right there.")
	}
}

func TestLeak_SearchHitsAreFilteredBeforeAnybodySeesThem(t *testing.T) {
	src := readSourceFile(t, "search.go")
	body := bodyOfGo(t, src, "func meiliSearchChallenges(")
	if !strings.Contains(body, "isSearchable(") {
		t.Error("Meilisearch hits are returned unfiltered. The index holds " +
			"every challenge including private ones and the request carries " +
			"no filter, so a friends-only video can come back as a search " +
			"result and reach a feed through candidate_sources.go.")
	}
}

// ── one rule, two shapes ────────────────────────────────────────────────────

func TestSearchable_TheGoCheckAndTheSQLAgree(t *testing.T) {
	where := searchableWhere("")

	// Everything the Go check accepts must be in the SQL, and vice versa.
	for _, st := range searchableStatuses {
		if !isSearchable(searchableVisibility, st) {
			t.Errorf("status %q is in the SQL but the Go check rejects it", st)
		}
		if !strings.Contains(where, "'"+st+"'") {
			t.Errorf("status %q is accepted by the Go check but is not in "+
				"the SQL: %q", st, where)
		}
	}
	if !strings.Contains(where, "'"+searchableVisibility+"'") {
		t.Errorf("the SQL does not name the visibility the Go check uses: %q", where)
	}

	// And the cases that matter.
	for _, tc := range []struct {
		visibility, status string
		want               bool
		why                string
	}{
		{"arena", "open", true, "an ordinary public challenge"},
		{"arena", "active", true, "a battle in progress"},
		{"arena", "completed", true, "a finished battle"},
		{"friends", "open", false, "SOMEBODY'S PRIVATE UPLOAD"},
		{"friends", "active", false, "a private battle in progress"},
		{"arena", "expired", false, "an expired challenge is not a result"},
		{"arena", "draft", false, "a draft is not a result"},
		{"", "", false, "an unset row says nothing, so it is not findable"},
	} {
		if got := isSearchable(tc.visibility, tc.status); got != tc.want {
			t.Errorf("isSearchable(%q, %q) = %v, want %v — %s",
				tc.visibility, tc.status, got, tc.want, tc.why)
		}
	}
}

// The SQL is built from the same two values the Go check reads, so neither
// can be edited without the other following.
func TestSearchable_NeitherShapeIsHandWritten(t *testing.T) {
	src := readSourceFile(t, "searchable_population.go")
	body := bodyOfGo(t, src, "func searchableWhere(")
	if !strings.Contains(body, "searchableVisibility") ||
		!strings.Contains(body, "searchableStatuses") {
		t.Error("searchableWhere spells the rule out itself instead of " +
			"building it from the shared values. That is how it drifts from " +
			"isSearchable, and a privacy rule that has two versions has the " +
			"weaker one.")
	}
	gb := bodyOfGo(t, src, "func isSearchable(")
	if !strings.Contains(gb, "searchableVisibility") ||
		!strings.Contains(gb, "searchableStatuses") {
		t.Error("isSearchable spells the rule out itself instead of reading " +
			"the shared values")
	}
}

// bodyOfGo returns one function's body, braces balanced.
func bodyOfGo(t *testing.T, src, signature string) string {
	t.Helper()
	at := strings.Index(src, signature)
	if at < 0 {
		t.Fatalf("could not find %s", signature)
	}
	open := strings.Index(src[at:], "{")
	if open < 0 {
		t.Fatalf("no body for %s", signature)
	}
	depth := 0
	for i := at + open; i < len(src); i++ {
		switch src[i] {
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				return src[at+open : i]
			}
		}
	}
	t.Fatalf("unbalanced body for %s", signature)
	return ""
}

var _ = os.ReadFile

// ── against a real database ─────────────────────────────────────────────────
//
// The checks above read the source, which proves the filter is written. This
// runs the query and proves it WORKS: a private subject in the table, and the
// list that feeds the public autocomplete coming back without it.

func TestLeak_APrivateSubjectNeverReachesTheSuggestionList(t *testing.T) {
	defer withDB(t)()

	// One of each, same creator, differing only in who may see it.
	for _, c := range []struct{ subject, visibility, status string }{
		{"a public dance off", "arena", "open"},
		{"a private thing nobody else should read", "friends", "open"},
		{"an expired public one", "arena", "expired"},
	} {
		if _, err := db.Exec(`
			INSERT INTO challenges (creator_id, video_url, prefix, subject, visibility, status)
			VALUES (1, 'https://v/a.mp4', 'who is better at', $1, $2, $3)`,
			c.subject, c.visibility, c.status); err != nil {
			t.Fatalf("seed %q: %v", c.subject, err)
		}
	}

	// The real function, not a copy of its query.
	//
	// An earlier version of this test held its own copy of the statement,
	// which meant the copy was what got tested and the real one could drift
	// away from it without anything noticing. There is one copy now and this
	// calls it.
	used := subjectsPeopleHaveUsed()
	got := map[string]bool{}
	for subject := range used {
		got[subject] = true
	}

	if !got["a public dance off"] {
		t.Error("a public subject is missing from the autocomplete — the " +
			"filter is too tight and the feature suggests nothing")
	}
	if got["a private thing nobody else should read"] {
		t.Error("A FRIENDS-ONLY SUBJECT IS IN THE PUBLIC AUTOCOMPLETE. " +
			"/suggest/challenge-subject needs no sign-in, so this text is " +
			"typed back to anybody who starts typing near it.")
	}
	if got["an expired public one"] {
		t.Error("an expired challenge is being suggested. Tapping it finds " +
			"nothing, which is the usability half of the same bug.")
	}
}
