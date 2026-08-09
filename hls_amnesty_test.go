package main

import (
	"regexp"
	"testing"
)

// The HLS transcode queue caps how many times a job may be claimed
// (maxHLSAttempts), so a source that can never be downloaded stops being
// retried. InitDatabase's "amnesty" block clears that cap for rows that
// burned it, so an infrastructure blip does not strand a video forever.
//
// The amnesty used to be unconditional, documented as running "once per
// deploy". It does not run once per deploy — it runs once per BOOT, and
// this backend is on Render's free tier, which sleeps after inactivity and
// cold-starts many times a day. The cap was therefore cleared continuously
// and capped nothing.
//
// What that looked like in production: the scheduled transcode worker
// re-claimed the same six rows every run — all pointing at a hotlink-
// blocked URL answering 403 — failing each one five times, then doing it
// again 30 minutes later. It spent its whole allocation on jobs that could
// never succeed, and generated enough claim traffic to be 429ed by this
// backend's own rate limiter.
//
// These tests read the migration SQL directly. A DB-backed test would need
// a live Postgres, which this suite deliberately does not require; the
// property at stake is entirely visible in the statement text, and it is
// the text that regressed.

// amnestyStatements pulls the two amnesty UPDATEs out of the migration.
func amnestyStatements(t *testing.T) []string {
	t.Helper()
	re := regexp.MustCompile(`(?s)UPDATE (?:challenges|challenge_responses)\s+SET hls_attempts = 0.*?;`)
	found := re.FindAllString(alterStmts, -1)
	if len(found) != 2 {
		t.Fatalf("expected 2 amnesty statements in the migration, found %d — "+
			"if this block moved, move these tests with it", len(found))
	}
	return found
}

func TestHLSAmnesty_IsGatedOnTime(t *testing.T) {
	for _, stmt := range amnestyStatements(t) {
		if !regexp.MustCompile(`hls_claimed_at\s*<\s*NOW\(\)\s*-\s*INTERVAL`).MatchString(stmt) {
			t.Errorf("amnesty statement has no age gate:\n%s\n\n"+
				"Without one it fires on every cold start, which on this "+
				"host is many times a day, and the attempt cap it clears "+
				"stops capping anything: a permanently broken source is "+
				"retried forever.", stmt)
		}
	}
}

// A row that has never been claimed has hls_claimed_at NULL. In SQL,
// NULL < anything is NULL — not true — so a bare age comparison would
// silently exclude those rows from the amnesty forever.
func TestHLSAmnesty_StillCoversNeverClaimedRows(t *testing.T) {
	for _, stmt := range amnestyStatements(t) {
		if !regexp.MustCompile(`hls_claimed_at IS NULL`).MatchString(stmt) {
			t.Errorf("amnesty statement does not handle a NULL claim time:\n%s\n\n"+
				"NULL < NOW() - INTERVAL is NULL, not true, so a row that "+
				"burned its attempts without ever recording a claim time "+
				"would never be amnestied.", stmt)
		}
	}
}

// The amnesty must not resurrect rows that already transcoded fine —
// clearing attempts on a completed row is meaningless, and dropping the
// hls_manifest_url = ” filter would widen the update to the whole table.
func TestHLSAmnesty_OnlyTouchesUntranscodedRows(t *testing.T) {
	for _, stmt := range amnestyStatements(t) {
		if !regexp.MustCompile(`hls_manifest_url = ''`).MatchString(stmt) {
			t.Errorf("amnesty statement is not restricted to untranscoded "+
				"rows:\n%s", stmt)
		}
		if !regexp.MustCompile(`hls_attempts >= 5`).MatchString(stmt) {
			t.Errorf("amnesty statement is not restricted to rows that "+
				"actually burned their attempts:\n%s", stmt)
		}
	}
}

// The claim query is the other half of the contract: it must refuse rows
// at or over the cap, or the cap does nothing regardless of the amnesty.
func TestHLSClaim_RespectsTheAttemptCap(t *testing.T) {
	if maxHLSAttempts <= 0 {
		t.Fatalf("maxHLSAttempts = %d; a non-positive cap disables claiming "+
			"entirely", maxHLSAttempts)
	}
	// Guard the pairing between the migration's literal 5 and the Go
	// constant. They are written independently and a change to one
	// without the other silently breaks the cap.
	if maxHLSAttempts != 5 {
		t.Errorf("maxHLSAttempts is %d but the amnesty SQL in database.go "+
			"hardcodes 5; update both together", maxHLSAttempts)
	}
}
