package main

// ════════════════════════════════════════════════════════════════════════════
// ONE ANSWER TO "WHICH VIDEOS CAN SEARCH RETURN"
// ════════════════════════════════════════════════════════════════════════════
//
// This existed in three places and all three said something different:
//
//   * the list of results   — arena, and open/active/completed
//   * the text index        — arena, any status
//   * the related-subject graph — every challenge in the table, no filter
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
// So there is now one definition, and everything that has an opinion about
// what is findable asks it. Two of these are Go and one is SQL text, which is
// the usual way this kind of thing drifts apart again — so a test reads this
// file and the three callers and fails if any of them starts saying something
// of its own.

// searchableWhere returns the WHERE conditions for "a video search may return".
//
// alias is the table alias used by the surrounding query ("c" for the joined
// challenge query, "" when selecting from challenges directly).
func searchableWhere(alias string) string {
	p := ""
	if alias != "" {
		p = alias + "."
	}
	return p + "visibility = 'arena'\n\t    AND " + p + "status IN ('open','active','completed')"
}
