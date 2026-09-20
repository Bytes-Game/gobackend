package main

// query_failures.go — saying so when a query did not run.
//
// ════════════════════════════════════════════════════════════════════════════
// THE SHAPE THIS EXISTS TO REPLACE
// ════════════════════════════════════════════════════════════════════════════
//
//	rows, err := db.Query(...)
//	if err == nil {
//	    ... all the work ...
//	}
//
// Read it again. If the query works, do the work. If it FAILS, do nothing —
// and say nothing.
//
// The output of a failed query and the output of an empty database are then
// identical. A broken lane looks like a user who follows nobody. A broken
// mood query looks like somebody who has watched nothing tagged. There is no
// error, no log line, and nothing to search for, so the only way to find one
// is to already suspect it.
//
// This is not hypothetical here. Twelve queries in this repo named a column
// that does not exist, and Postgres refuses a statement outright over one
// wrong word. Every one of them had been failing on every request since it
// was written:
//
//	the follow-graph feed lane        following somebody fed you nothing
//	two account-suggestion lanes      returned no suggestions, ever
//	the mood half of taste profiles   never populated for anybody
//	the engagement-quality score      a flat, identical score for everyone
//	the creator dashboard             failed for every creator
//	trending posts                    silently dropped from the feed
//
// Not one produced a single line of output in all that time. That is what
// this file is for.
//
// ════════════════════════════════════════════════════════════════════════════
// WHAT IT DOES NOT DO
// ════════════════════════════════════════════════════════════════════════════
//
// It does not make anything fail. Every caller here is a signal the ranker
// would like to have and can live without, and taking a feed request down
// because one of eleven signals could not be read would be a much worse
// trade. The behaviour is unchanged — what changes is that the app now says
// which signal it is missing and why.

import (
	"database/sql"
	"errors"
	"log"
)

// queryFailed reports whether err is a real failure, and says so in the log
// when it is.
//
// what names the thing the caller was trying to work out, in the words
// somebody reading a log at 2am would want: "the follow-graph lane for user
// 41", not "sourceFollowGraphWindowed". consequence says what the app is
// doing instead, because "query failed" without it leaves the reader to guess
// whether anything is actually wrong.
//
// It returns true for ANY error, so that
//
//	if !queryFailed(...) { ... }
//
// means exactly what
//
//	if err == nil { ... }
//
// meant, and swapping one for the other cannot change behaviour. Only the
// LOGGING skips no-rows: nobody has done this yet is a perfectly ordinary
// answer and printing it would bury the real ones.
func queryFailed(what, consequence string, err error) bool {
	if err == nil {
		return false
	}
	if !errors.Is(err, sql.ErrNoRows) {
		log.Printf("%s: %v — %s", what, err, consequence)
	}
	return true
}

// scanFailed is the same for a row that could not be read.
//
// Kept apart from queryFailed because the two fail for different reasons and
// one of them is far nastier. A Scan fails for the WHOLE result set at once:
// if the column list and the destination list do not line up, it fails on row
// one and on all five hundred. The loop then finishes normally having built
// its answer out of zero values, which is indistinguishable from a database
// with nothing in it.
//
// Callers pass a counter so one broken query is one line rather than five
// hundred. The count is what makes it obvious which of the two happened: a
// single bad row is a bad row, and every row failing is a broken query.
func scanFailed(what string, err error, seen *int) bool {
	if err == nil {
		return false
	}
	*seen++
	if *seen == 1 {
		log.Printf("%s: could not read a row, so this is being built from "+
			"less than the database holds: %v", what, err)
	}
	return true
}
