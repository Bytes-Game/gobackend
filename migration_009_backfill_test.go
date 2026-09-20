package main

import (
	"strings"
	"testing"
	"time"
)

// ════════════════════════════════════════════════════════════════════════════
// THE BACKFILL IN MIGRATION 009, AGAINST ROWS THAT ARE ALREADY THERE
// ════════════════════════════════════════════════════════════════════════════
//
// A migration on a fresh database proves it parses. It proves nothing about
// what it does to data, and this one has a data step: every existing account
// is lowered from "stamped today" to the earliest trace of it doing anything.
//
// Getting that wrong is quiet and expensive in both directions. Too late and
// every long-standing account reads as brand new, which is exactly the state
// the migration exists to avoid. Too early — a MIN over the wrong table, or a
// COALESCE that lets a NULL win — and an account looks older than the app.
//
// Production has data. This is the path production will take.

func TestMigration009_BackfillUsesTheEarliestTrace(t *testing.T) {
	defer withDB(t)()

	// An account whose oldest trace is a challenge from a year ago, with
	// newer traces after it. The oldest one has to win.
	yearAgo := time.Now().Add(-365 * 24 * time.Hour)
	if _, err := db.Exec(`
		INSERT INTO users (id, username, password, created_at)
		VALUES (900, 'oldtimer', 'x', NOW()) ON CONFLICT (id) DO NOTHING`); err != nil {
		t.Fatalf("seed user: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO challenges (creator_id, video_url, prefix, subject, created_at)
		VALUES (900, 'https://v/a.mp4', 'can you', 'dance', $1)`, yearAgo); err != nil {
		t.Fatalf("seed challenge: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO feed_events (user_id, content_id, content_type, event_type, metadata)
		VALUES ('900', '1', 'challenge', 'view', '{}'::jsonb)`); err != nil {
		t.Fatalf("seed event: %v", err)
	}

	// Re-run the backfill exactly as the migration file has it. Reading the
	// statement out of the file rather than repeating it here is the point:
	// a copy in a test drifts from the migration and then tests nothing.
	if _, err := db.Exec(backfillFromMigration009(t)); err != nil {
		t.Fatalf("backfill: %v", err)
	}

	var got time.Time
	if err := db.QueryRow(
		`SELECT created_at FROM users WHERE id = 900`).Scan(&got); err != nil {
		t.Fatalf("read back: %v", err)
	}

	// Within a minute of the challenge, not of now.
	if diff := got.Sub(yearAgo); diff > time.Minute || diff < -time.Minute {
		t.Errorf("an account whose first challenge was a year ago is dated "+
			"%s — the backfill did not reach back to it, so it reads as a "+
			"brand-new account and its signals get the lowest trust there is",
			got.Format(time.RFC3339))
	}
	if time.Since(got) < 24*time.Hour {
		t.Error("the account was left with today's stamp, which is the exact " +
			"state this migration exists to avoid")
	}
}

// LEAST ignores NULLs, and the backfill depends on that.
//
// A MIN over a table this account has never touched comes back NULL and drops
// out of the comparison; NULL only wins if every argument is NULL, and
// u.created_at never is. This is why there is no COALESCE around the
// sub-selects — an earlier draft had them and they implied a danger that does
// not exist.
//
// Pinned against the database rather than remembered, because "does NULL win
// here" is exactly the kind of thing somebody will helpfully "fix" back.
func TestMigration009_LeastIgnoresNulls(t *testing.T) {
	defer withDB(t)()

	var ignoresNulls time.Time
	var allNullIsNull bool
	if err := db.QueryRow(`
		SELECT LEAST(NULL::timestamptz, '2020-01-01'::timestamptz, NULL::timestamptz),
		       LEAST(NULL::timestamptz, NULL::timestamptz) IS NULL`).
		Scan(&ignoresNulls, &allNullIsNull); err != nil {
		t.Fatalf("ask the database how LEAST behaves: %v", err)
	}
	if ignoresNulls.Year() != 2020 {
		t.Errorf("LEAST returned %s with nulls in the list. The backfill "+
			"leans on nulls dropping out; if they do not, every account that "+
			"has never posted gets a null date.", ignoresNulls)
	}
	if !allNullIsNull {
		t.Error("LEAST of all nulls is not null, which contradicts the " +
			"reasoning written into migration 009")
	}
}

// An account with nothing to go on keeps today's date. That is the honest
// answer, and it must not become NULL or the year zero.
func TestMigration009_AnAccountWithNoTraceKeepsToday(t *testing.T) {
	defer withDB(t)()

	if _, err := db.Exec(`
		INSERT INTO users (id, username, password, created_at)
		VALUES (901, 'ghost', 'x', NOW()) ON CONFLICT (id) DO NOTHING`); err != nil {
		t.Fatalf("seed user: %v", err)
	}
	if _, err := db.Exec(backfillFromMigration009(t)); err != nil {
		t.Fatalf("backfill: %v", err)
	}

	var got time.Time
	if err := db.QueryRow(
		`SELECT created_at FROM users WHERE id = 901`).Scan(&got); err != nil {
		t.Fatalf("read back — a NULL here would fail this scan, which is the "+
			"point: %v", err)
	}
	if time.Since(got) > time.Hour {
		t.Errorf("an account with no history was dated %s. A COALESCE that "+
			"lets a NULL through would drag it to the year zero and make it "+
			"the most trusted account on the platform", got.Format(time.RFC3339))
	}
}

// And the whole reason any of this exists: the score that reads the column.
func TestMigration009_TheTrustScoreCanNowBeComputed(t *testing.T) {
	defer withDB(t)()

	if _, err := db.Exec(`
		INSERT INTO users (id, username, password, created_at)
		VALUES (902, 'scored', 'x', NOW() - INTERVAL '200 days')
		ON CONFLICT (id) DO NOTHING`); err != nil {
		t.Fatalf("seed user: %v", err)
	}

	// Before the column existed this query was refused outright and the
	// function returned a flat 1.0 for everybody, with the error discarded.
	got := computeEngagementQuality("902")
	if got == 1.0 {
		t.Error("the trust score came back at exactly 1.0, which is what the " +
			"error path returns — the query is still failing")
	}
	if got <= 0 {
		t.Errorf("trust score of %v is not a usable weight", got)
	}
}

// backfillFromMigration009 lifts the UPDATE out of the migration file.
func backfillFromMigration009(t *testing.T) string {
	t.Helper()
	body := readSourceFile(t, "migrations/009_users_remember_when_they_arrived.sql")
	i := strings.Index(body, "UPDATE users u")
	if i < 0 {
		t.Fatal("migration 009 no longer contains the backfill this test is about")
	}
	j := strings.Index(body[i:], ";")
	if j < 0 {
		t.Fatal("could not find the end of the backfill statement")
	}
	return body[i : i+j+1]
}
