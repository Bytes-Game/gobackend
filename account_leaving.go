package main

// account_leaving.go — deleting an account waits 30 days, the way TikTok
// does it.
//
// ════════════════════════════════════════════════════════════════════════
// WHAT HAPPENS
// ════════════════════════════════════════════════════════════════════════
//
// When somebody deletes their account (DeleteAccountHandler):
//
//   - it is hidden from everybody straight away: profile, posts, answers,
//     comments, chats, places in lists. Nothing is shown and nothing of
//     theirs is scored — the feed ranks only accounts that are staying.
//   - every phone it is signed in on is signed out, and no phone gets its
//     notifications any more.
//   - any battle it is in that is still being voted on ends with no result.
//     Nobody wins or loses points: an opponent should not lose to somebody
//     who left, nor win by default.
//
// For the next 30 days nothing is deleted. Signing in again shows when the
// account will go and asks whether to keep it (LoginHandler). Keeping it
// brings everything back as it was, except those battles.
//
// After 30 days a background job deletes it for good (purgeDueAccounts →
// purgeAccount): every row, and only the files the database lists for it.
// Its username is then free for anybody.
//
// "Waiting to be deleted" is one column, users.deletion_requested_at. Lists
// leave those accounts out with leavingUsersSQL in their query, or, where
// the list is put together in Go, with leavingIDs().

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/lib/pq"
)

// deletionWait is how long a deleted account is kept, hidden, before it is
// deleted for good. TikTok's and Instagram's number.
const deletionWait = 30 * 24 * time.Hour

// leavingUsersSQL is every account waiting to be deleted, written to follow
// NOT IN in a query:
//
//	AND c.creator_id NOT IN ` + leavingUsersSQL + `
//
// A handful of rows at most, read through idx_users_waiting_deletion, so it
// costs next to nothing on any list. users.id is never NULL, which is what
// makes NOT IN safe here.
const leavingUsersSQL = `(SELECT id FROM users WHERE deletion_requested_at IS NOT NULL)`

// visibleAnswersOfC is how many answers challenge `c` has that anybody can
// see: not those from an account waiting to be deleted. What decides whether
// a video is a battle or a short, and the count on its card. A battle whose
// only answer is from a leaving account is a short for those 30 days — not
// a battle with nothing to show on its other side.
const visibleAnswersOfC = `(SELECT COUNT(*) FROM challenge_responses
	WHERE challenge_id = c.id AND responder_id NOT IN ` + leavingUsersSQL + `)`

// leavingUserTextIDsSQL is the same list for the tables that keep user ids
// as text (user_similarities, feed_events).
const leavingUserTextIDsSQL = `(SELECT id::text FROM users WHERE deletion_requested_at IS NOT NULL)`

// leavingFresh is how long the remembered list of leaving accounts is
// trusted before it is read again: how long a deletion, or keeping an
// account, takes to reach every other server. This server forgets its copy
// at once (forgetLeaving).
const leavingFresh = 30 * time.Second

var (
	leavingMu     sync.Mutex
	leavingSet    map[int]bool
	leavingLoaded time.Time
)

// leavingIDs is every account waiting to be deleted. The map is shared:
// read it, never write to it.
//
// When the database cannot answer, the last list read is kept, and with
// none it is empty: a list that shows somebody it should not is the lesser
// fault than a feed that fails.
func leavingIDs() map[int]bool {
	leavingMu.Lock()
	set, loaded := leavingSet, leavingLoaded
	leavingMu.Unlock()
	if set != nil && time.Since(loaded) < leavingFresh {
		return set
	}
	if db == nil {
		return map[int]bool{}
	}
	rows, err := db.Query(`SELECT id FROM users WHERE deletion_requested_at IS NOT NULL`)
	if queryFailed("the accounts waiting to be deleted",
		"lists keep the last ones known, and show any new ones", err) {
		if set != nil {
			return set
		}
		return map[int]bool{}
	}
	fresh := map[int]bool{}
	bad := 0
	for rows.Next() {
		var id int
		if scanFailed("an account waiting to be deleted", rows.Scan(&id), &bad) {
			continue
		}
		fresh[id] = true
	}
	rows.Close()
	leavingMu.Lock()
	leavingSet, leavingLoaded = fresh, time.Now()
	leavingMu.Unlock()
	return fresh
}

// isLeaving is whether the account [id] (a user id, as the app sends them)
// is waiting to be deleted.
func isLeaving(id string) bool {
	return inLeaving(leavingIDs(), id)
}

// inLeaving is isLeaving against a list already read, for a loop.
func inLeaving(set map[int]bool, id string) bool {
	n, err := strconv.Atoi(id)
	return err == nil && set[n]
}

// forgetLeaving drops this server's copy of the list, after a change.
func forgetLeaving() {
	leavingMu.Lock()
	leavingSet = nil
	leavingMu.Unlock()
}

// deletionDate is when [userID]'s account will be deleted for good, and
// whether it is waiting to be.
func deletionDate(userID string) (time.Time, bool) {
	var at *time.Time
	err := db.QueryRow(`SELECT deletion_requested_at FROM users WHERE id::text = $1`,
		userID).Scan(&at)
	if queryFailed("whether account "+userID+" is waiting to be deleted",
		"treating it as not", err) || at == nil {
		return time.Time{}, false
	}
	return at.Add(deletionWait), true
}

// requestAccountDeletion starts the 30 days for [userID] and says when the
// account will be deleted for good. Asking again keeps the first date.
func requestAccountDeletion(ctx context.Context, userID string) (time.Time, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return time.Time{}, err
	}
	defer func() { _ = tx.Rollback() }()
	var requested time.Time
	err = tx.QueryRowContext(ctx, `
		UPDATE users SET deletion_requested_at = COALESCE(deletion_requested_at, NOW())
		 WHERE id::text = $1
		 RETURNING deletion_requested_at`, userID).Scan(&requested)
	if err != nil {
		return time.Time{}, err
	}
	// Their likes, votes, comments, follows, saves and shares leave other
	// people's things in the same step: hidden and set aside together, or
	// the request fails and nothing changed.
	if err := moveTraces(ctx, tx, userID, false); err != nil {
		return time.Time{}, fmt.Errorf("setting aside what they did on other people's things: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return time.Time{}, err
	}
	// Signed out on this server now; on the others within cutoffFresh.
	forgetCutoff(userID)
	forgetLeaving()

	if n, err := cancelBattlesOf(ctx, userID); err != nil {
		log.Printf("account %s is waiting to be deleted, but its battles could not all "+
			"be cancelled: %v — the rest are decided as normal when their time is up",
			userID, err)
	} else if n > 0 {
		log.Printf("account %s is waiting to be deleted: %d battle(s) cancelled", userID, n)
	}

	// No more notifications on their phones. Signing in again to keep the
	// account registers the phone again, as any sign-in does.
	if _, err := db.ExecContext(ctx, `DELETE FROM device_tokens WHERE user_id = $1`, userID); err != nil {
		log.Printf("account %s is waiting to be deleted, but its phones are still "+
			"registered for notifications: %v", userID, err)
	}

	removeFromSearch(userID)
	return requested.Add(deletionWait), nil
}

// keepAccount ends the wait: [userID] signed in during the 30 days and chose
// to keep the account. Everything comes back. Every other sign-in from before
// now stays signed out; the caller signs this one in fresh.
//
// The sign-out moment is to the second, from this server's clock — the same
// clock that stamps the fresh sign-in, so that sign-in can never look older.
func keepAccount(ctx context.Context, userID string) error {
	at := time.Now().Truncate(time.Second)
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	res, err := tx.ExecContext(ctx, `
		UPDATE users SET deletion_requested_at = NULL, signed_out_at = $2
		 WHERE id::text = $1 AND deletion_requested_at IS NOT NULL`, userID, at)
	if err != nil {
		return err
	}
	kept, _ := res.RowsAffected()
	if kept > 0 {
		// Their likes, comments, follows and the rest go back with them.
		if err := moveTraces(ctx, tx, userID, true); err != nil {
			return fmt.Errorf("putting back what they did on other people's things: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	forgetCutoff(userID)
	forgetLeaving()
	if kept > 0 {
		log.Printf("account %s is kept: it is no longer waiting to be deleted", userID)
		addBackToSearch(userID)
	}
	return nil
}

// ════════════════════════════════════════════════════════════════════════
// THE JOB THAT FINISHES THE DELETIONS
// ════════════════════════════════════════════════════════════════════════

// accountPurgeInterval is how often the job looks for accounts whose 30 days
// are up. A few hours late is harmless; nobody is waiting on it.
const accountPurgeInterval = time.Hour

// accountPurgeBatch bounds one round, so a backlog is spread out.
const accountPurgeBatch = 20

// startAccountPurger runs purgeDueAccounts forever.
func startAccountPurger() {
	go func() {
		t := time.NewTicker(accountPurgeInterval)
		defer t.Stop()
		for range t.C {
			accountPurgeRound(context.Background())
		}
	}()
}

// accountPurgeRound is one tick of the job, apart from the ticker so a test
// can run exactly what the job runs.
func accountPurgeRound(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, 20*time.Minute)
	defer cancel()
	if n, err := purgeDueAccounts(ctx, time.Now()); err != nil {
		log.Printf("deleting accounts whose 30 days are up: %v — tried again next hour", err)
	} else if n > 0 {
		log.Printf("deleted %d account(s) for good: their 30 days were up", n)
	}
}

// purgeDueAccounts deletes for good every account that asked to be deleted
// at least deletionWait before [now], and says how many.
func purgeDueAccounts(ctx context.Context, now time.Time) (int, error) {
	if db == nil {
		return 0, nil
	}
	rows, err := db.QueryContext(ctx, `
		SELECT id::text FROM users
		 WHERE deletion_requested_at IS NOT NULL AND deletion_requested_at <= $1
		 ORDER BY deletion_requested_at
		 LIMIT $2`, now.Add(-deletionWait), accountPurgeBatch)
	if err != nil {
		return 0, err
	}
	var due []string
	bad := 0
	for rows.Next() {
		var id string
		if scanFailed("an account whose 30 days are up", rows.Scan(&id), &bad) {
			continue
		}
		due = append(due, id)
	}
	rows.Close()
	done := 0
	for _, id := range due {
		if err := purgeAccount(id); err != nil {
			log.Printf("account %s: deleting it for good failed: %v — tried again next round", id, err)
			continue
		}
		done++
	}
	return done, nil
}

// ════════════════════════════════════════════════════════════════════════
// WHAT THEY DID TO OTHER PEOPLE'S THINGS
// ════════════════════════════════════════════════════════════════════════

// The marks a leaving account left on other people's things — likes on
// videos and on answers, votes, dislikes, comments, saves, shares, and
// follows both ways — are set aside while it waits, and put back if it is
// kept.
//
// Taken out of their tables rather than filtered: the like count on a video
// is read in more than twenty places, and every one of them would have to
// remember. Out of the table, none has to. The likes_count trigger moves
// that count down and back up with them.
//
// Nothing points at any of these tables, so taking rows out of one cannot
// take anything of anybody else's with it.
// TestAccountLeaving_NothingPointsAtWhatIsSetAside keeps that true.

// traceTables is every table their marks are set aside from. For each: which
// rows are theirs, and what must still be there for a row to go back — the
// video, answer or person it was on. A like on a video deleted during the 30
// days has nowhere to go back to, and trying would make keeping the account
// fail. In stillThere, `r` is the row as it was set aside.
//
// One list for both directions, so the two cannot drift apart: a table set
// aside is always a table put back.
var traceTables = []struct{ table, theirs, stillThere string }{
	{"challenge_likes", "user_id = $1",
		"EXISTS (SELECT 1 FROM challenges c WHERE c.id = r.challenge_id)"},
	{"challenge_response_likes", "user_id = $1",
		"EXISTS (SELECT 1 FROM challenge_responses x WHERE x.id = r.response_id)"},
	{"challenge_votes", "voter_id = $1",
		"EXISTS (SELECT 1 FROM challenges c WHERE c.id = r.challenge_id) AND " +
			"(r.response_id IS NULL OR EXISTS (SELECT 1 FROM challenge_responses x WHERE x.id = r.response_id))"},
	{"challenge_dislikes", "user_id = $1",
		"EXISTS (SELECT 1 FROM challenges c WHERE c.id = r.challenge_id)"},
	{"challenge_comments", "author_id = $1",
		"EXISTS (SELECT 1 FROM challenges c WHERE c.id = r.challenge_id)"},
	{"saved_challenges", "user_id = $1",
		"EXISTS (SELECT 1 FROM challenges c WHERE c.id = r.challenge_id)"},
	{"video_shares", "user_id = $1",
		"EXISTS (SELECT 1 FROM challenges c WHERE c.id = r.challenge_id)"},
	{"follows", "follower_id = $1 OR following_id = $1",
		"EXISTS (SELECT 1 FROM users u WHERE u.id = r.follower_id) AND " +
			"EXISTS (SELECT 1 FROM users u WHERE u.id = r.following_id)"},
}

// moveTraces sets [userID]'s marks on other people's things aside, or puts
// them back, inside [tx] — so it happens together with the change to the
// account, or not at all.
func moveTraces(ctx context.Context, tx *sql.Tx, userID string, back bool) error {
	id, err := strconv.Atoi(userID)
	if err != nil {
		return fmt.Errorf("user id %q is not a number", userID)
	}
	if back {
		// A follow with somebody who is ALSO waiting to be deleted waits on
		// with them instead: it goes back when they keep their account, and
		// goes with their account if they do not. Put back now, it would show
		// them in this person's followers while they are hidden everywhere
		// else — follower lists and counts leave a leaving account out only
		// because its follows are set aside.
		if _, err := tx.ExecContext(ctx, `
			UPDATE leaving_set_aside SET user_id = f.other
			  FROM (SELECT s.id,
			               CASE WHEN (s.row_data->>'follower_id')::int = $1
			                    THEN (s.row_data->>'following_id')::int
			                    ELSE (s.row_data->>'follower_id')::int END AS other
			          FROM leaving_set_aside s
			         WHERE s.user_id = $1 AND s.table_name = 'follows') f
			 WHERE leaving_set_aside.id = f.id
			   AND f.other IN `+leavingUsersSQL, id); err != nil {
			return fmt.Errorf("follows with somebody else who is leaving: %w", err)
		}
	}
	for _, t := range traceTables {
		if back {
			err = putTracesBack(ctx, tx, id, t.table, t.stillThere)
		} else {
			_, err = tx.ExecContext(ctx, fmt.Sprintf(`
				WITH moved AS (DELETE FROM %s WHERE (%s) RETURNING *)
				INSERT INTO leaving_set_aside (user_id, table_name, row_data)
				SELECT $1, $2, to_jsonb(moved) FROM moved`, t.table, t.theirs), id, t.table)
		}
		if err != nil {
			return fmt.Errorf("%s: %w", t.table, err)
		}
	}
	return nil
}

// putTracesBack puts [userID]'s rows set aside from [table] back, those whose
// video, answer or person is [stillThere]. Every one leaves the holding table
// either way: one whose thing has gone has nowhere to go back to.
//
// What goes back is each column the saved rows have and the table still has.
// The table can change during the 30 days: a column added since takes its
// default, one dropped since is left out. Putting back every column the table
// has now would write nothing into a column added since — and a column added
// with NOT NULL, as this repo often adds them, would then refuse every row,
// and with them the person trying to keep their account. (Their rows from one
// table were all saved by one statement, so they all have the same columns.)
//
// A row already back is left as it is. That happens: another server can let
// somebody follow them again in the few seconds before it hears they left.
func putTracesBack(ctx context.Context, tx *sql.Tx, userID int, table, stillThere string) error {
	rows, err := tx.QueryContext(ctx, `
		SELECT DISTINCT k.key
		  FROM leaving_set_aside s
		 CROSS JOIN LATERAL jsonb_object_keys(s.row_data) AS k(key)
		 WHERE s.user_id = $1 AND s.table_name = $2
		   AND EXISTS (SELECT 1 FROM pg_attribute a
		                WHERE a.attrelid = to_regclass($2) AND a.attname = k.key
		                  AND a.attnum > 0 AND NOT a.attisdropped)`, userID, table)
	if err != nil {
		return err
	}
	var cols []string
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			rows.Close()
			return err
		}
		cols = append(cols, pq.QuoteIdentifier(col))
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	if len(cols) == 0 {
		return nil // nothing of theirs was set aside from this table
	}
	_, err = tx.ExecContext(ctx, fmt.Sprintf(`
		WITH back AS (DELETE FROM leaving_set_aside WHERE user_id = $1 AND table_name = $2
		              RETURNING row_data)
		INSERT INTO %[1]s (%[2]s)
		SELECT r.%[3]s
		  FROM back b, jsonb_populate_record(NULL::%[1]s, b.row_data) r
		 WHERE %[4]s
		ON CONFLICT DO NOTHING`,
		table, strings.Join(cols, ", "), strings.Join(cols, ", r."), stillThere),
		userID, table)
	return err
}
