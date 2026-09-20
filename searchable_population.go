package main

import "strings"

// ════════════════════════════════════════════════════════════════════════════
// ONE ANSWER TO "WHICH VIDEOS CAN SEARCH RETURN"
// ════════════════════════════════════════════════════════════════════════════
//
// This existed in FIVE places and they did not agree:
//
//   * the list of results   — arena, and open/active/completed
//   * the text index        — arena, any status
//   * the related-subject graph — every challenge in the table, no filter
//   * the subject autocomplete  — every challenge in the table, no filter,
//                                 served on a route that needs no sign-in
//   * the Meilisearch hits      — no filter at all, feeding both results and
//                                 the feed
//
// The first three were found together and fixed together. The last two were
// found later, doing the same thing for the same reason, which is the honest
// lesson here: a rule written once is not a rule applied everywhere, and
// nothing in the first fix would have pointed at either of them.
//
// The third one is the damaging one, and it produced a bug that looked like
// bad ranking rather than a bad question.
//
// The graph is what powers "people also searched for". Because it read every
// row, it learned subjects from videos search cannot return — drafts, private
// uploads, anything not in the arena. So the app would offer somebody the word
// "pollination", they would tap it, and land on a page of results that had
// nothing to do with pollination, because the only video about it was one they
// were never allowed to see.
//
// The suggestion was not wrong about the catalogue. It was wrong about THEIR
// catalogue. From the outside that is indistinguishable from the search being
// broken.
//
// And the two found later were worse than a bad suggestion. A challenge posted
// as "friends" is titled "who is better at <something private>". The
// autocomplete was typing that text back to anybody who started typing near
// it, signed in or not. Same missing rule; a different kind of damage.
//
// So there is now one definition, and everything that has an opinion about
// what is findable asks it. Two of these are Go and one is SQL text, which is
// the usual way this kind of thing drifts apart again — so a test reads this
// file and the three callers and fails if any of them starts saying something
// of its own.

// ════════════════════════════════════════════════════════════════════════════
// THE RULE ITSELF, ONCE
// ════════════════════════════════════════════════════════════════════════════
//
// Two shapes need it: SQL, for the queries that read the table, and Go, for
// the places that already hold a challenge and have to decide whether it may
// be shown. Both are built from these, so a second hand-written copy is the
// only way they can disagree — and a hand-written copy is what this file
// exists to stop.

// searchableVisibility is the one visibility a search may return. "friends"
// is somebody's private upload and must never appear in a result, a
// suggestion, or an autocomplete.
const searchableVisibility = "arena"

// searchableStatuses are the states a video can be found in. A draft or an
// expired challenge is not a result.
var searchableStatuses = []string{"open", "active", "completed"}

// searchableWhere returns the WHERE conditions for "a video search may return".
//
// alias is the table alias used by the surrounding query ("c" for the joined
// challenge query, "" when selecting from challenges directly).
func searchableWhere(alias string) string {
	p := ""
	if alias != "" {
		p = alias + "."
	}
	quoted := make([]string, len(searchableStatuses))
	for i, st := range searchableStatuses {
		quoted[i] = "'" + st + "'"
	}
	return p + "visibility = '" + searchableVisibility + "'" +
		"\n\t    AND " + p + "status IN (" + strings.Join(quoted, ",") + ")"
}

// isSearchable is the same rule for something already in hand.
//
// Needed because two readers cannot use the SQL: the autocomplete, which
// decides whether a newly created challenge's subject goes into a public
// index, and the Meilisearch path, which gets documents back from an index
// rather than rows from a table.
//
// Both were doing it without any rule at all. A friends-only challenge's
// subject line went straight into an autocomplete served to every visitor,
// signed in or not — which is the same bug this file was written for, in two
// places the original fix did not reach.
func isSearchable(visibility, status string) bool {
	if visibility != searchableVisibility {
		return false
	}
	for _, st := range searchableStatuses {
		if status == st {
			return true
		}
	}
	return false
}
