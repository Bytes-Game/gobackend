package main

import (
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

// Turning a dislike ON must, in ONE transaction, insert the dislike row and
// delete any like row — otherwise the same user counts as both a positive
// and a negative signal on one challenge.
func TestToggleChallengeDislike_OnInsertsAndClearsLike(t *testing.T) {
	mock, cleanup := withMockDB(t)
	defer cleanup()

	mock.ExpectQuery(regexp.QuoteMeta("SELECT EXISTS")).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))
	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO challenge_dislikes")).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta("DELETE FROM challenge_likes")).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	mock.ExpectQuery(regexp.QuoteMeta("FROM challenge_dislikes")).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(4))
	mock.ExpectQuery(regexp.QuoteMeta("FROM challenge_likes")).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(9))

	disliked, dislikes, likes := ToggleChallengeDislike("12", "7")
	if !disliked {
		t.Fatalf("disliked = false, want true on a fresh dislike")
	}
	if dislikes != 4 || likes != 9 {
		t.Fatalf("counts = (%d,%d), want (4,9)", dislikes, likes)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

// Turning a dislike OFF deletes only the dislike row — it must NOT
// resurrect a like the user never re-pressed.
func TestToggleChallengeDislike_OffDeletesOnlyDislike(t *testing.T) {
	mock, cleanup := withMockDB(t)
	defer cleanup()

	mock.ExpectQuery(regexp.QuoteMeta("SELECT EXISTS")).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta("DELETE FROM challenge_dislikes")).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	mock.ExpectQuery(regexp.QuoteMeta("FROM challenge_dislikes")).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(3))
	mock.ExpectQuery(regexp.QuoteMeta("FROM challenge_likes")).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(9))

	disliked, dislikes, likes := ToggleChallengeDislike("12", "7")
	if disliked {
		t.Fatalf("disliked = true, want false when un-disliking")
	}
	if dislikes != 3 || likes != 9 {
		t.Fatalf("counts = (%d,%d), want (3,9)", dislikes, likes)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

// A failed like-clear must roll the whole thing back rather than leaving
// the challenge with both a like and a dislike from one user.
func TestToggleChallengeDislike_RollsBackOnPartialFailure(t *testing.T) {
	mock, cleanup := withMockDB(t)
	defer cleanup()

	mock.ExpectQuery(regexp.QuoteMeta("SELECT EXISTS")).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))
	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO challenge_dislikes")).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta("DELETE FROM challenge_likes")).
		WillReturnError(errDislikeTest)
	mock.ExpectRollback()

	disliked, dislikes, likes := ToggleChallengeDislike("12", "7")
	if disliked || dislikes != 0 || likes != 0 {
		t.Fatalf("got (%v,%d,%d), want (false,0,0) on rollback", disliked, dislikes, likes)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

// Non-numeric ids are rejected before any query runs.
func TestToggleChallengeDislike_RejectsNonNumericIDs(t *testing.T) {
	mock, cleanup := withMockDB(t)
	defer cleanup()

	disliked, dislikes, likes := ToggleChallengeDislike("abc", "7")
	if disliked || dislikes != 0 || likes != 0 {
		t.Fatalf("got (%v,%d,%d), want (false,0,0)", disliked, dislikes, likes)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expected no queries: %v", err)
	}
}

// The dislike action must carry its own rate-limit budget; a missing entry
// would silently fall back to whatever allowAction does for unknown actions.
func TestDislikeHasActionLimit(t *testing.T) {
	cfg, ok := actionLimitTable["dislike"]
	if !ok {
		t.Fatal(`actionLimitTable has no "dislike" entry`)
	}
	if cfg.burst <= 0 || cfg.tokensPerSecond <= 0 {
		t.Fatalf("dislike limit misconfigured: %+v", cfg)
	}
}

var errDislikeTest = sqlmock.ErrCancelled
