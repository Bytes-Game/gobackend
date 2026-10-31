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
	"sync"
	"time"
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
// kept. Each statement below has its partner in the other list, in the same
// order.
//
// Taken out of their tables rather than filtered: the like count on a video
// is read in more than twenty places, and every one of them would have to
// remember. Out of the table, none has to. The likes_count trigger moves
// that count down and back up with them.
//
// Nothing points at any of these tables, so taking rows out of one cannot
// take anything of anybody else's with it. A row whose video, answer or
// person has gone in the meantime is not put back.

// moveTraces sets [userID]'s marks on other people's things aside, or puts
// them back, inside [tx] — so it happens together with the change to the
// account, or not at all.
func moveTraces(ctx context.Context, tx *sql.Tx, userID string, back bool) error {
	id, err := strconv.Atoi(userID)
	if err != nil {
		return fmt.Errorf("user id %q is not a number", userID)
	}
	if back {
		for _, q := range []string{
			`WITH back AS (DELETE FROM leaving_set_aside WHERE user_id = $1 AND table_name = 'challenge_likes'
		               RETURNING row_data)
		 INSERT INTO challenge_likes SELECT (jsonb_populate_record(NULL::challenge_likes, b.row_data)).*
		   FROM back b
		  WHERE EXISTS (SELECT 1 FROM challenges c WHERE c.id = (b.row_data->>'challenge_id')::int)
		 ON CONFLICT DO NOTHING`,
			`WITH back AS (DELETE FROM leaving_set_aside WHERE user_id = $1 AND table_name = 'challenge_response_likes'
		               RETURNING row_data)
		 INSERT INTO challenge_response_likes
		 SELECT (jsonb_populate_record(NULL::challenge_response_likes, b.row_data)).*
		   FROM back b
		  WHERE EXISTS (SELECT 1 FROM challenge_responses r WHERE r.id = (b.row_data->>'response_id')::int)
		 ON CONFLICT DO NOTHING`,
			`WITH back AS (DELETE FROM leaving_set_aside WHERE user_id = $1 AND table_name = 'challenge_votes'
		               RETURNING row_data)
		 INSERT INTO challenge_votes SELECT (jsonb_populate_record(NULL::challenge_votes, b.row_data)).*
		   FROM back b
		  WHERE EXISTS (SELECT 1 FROM challenges c WHERE c.id = (b.row_data->>'challenge_id')::int)
		    AND (b.row_data->>'response_id' IS NULL
		         OR EXISTS (SELECT 1 FROM challenge_responses r WHERE r.id = (b.row_data->>'response_id')::int))
		 ON CONFLICT DO NOTHING`,
			`WITH back AS (DELETE FROM leaving_set_aside WHERE user_id = $1 AND table_name = 'challenge_dislikes'
		               RETURNING row_data)
		 INSERT INTO challenge_dislikes SELECT (jsonb_populate_record(NULL::challenge_dislikes, b.row_data)).*
		   FROM back b
		  WHERE EXISTS (SELECT 1 FROM challenges c WHERE c.id = (b.row_data->>'challenge_id')::int)
		 ON CONFLICT DO NOTHING`,
			`WITH back AS (DELETE FROM leaving_set_aside WHERE user_id = $1 AND table_name = 'challenge_comments'
		               RETURNING row_data)
		 INSERT INTO challenge_comments SELECT (jsonb_populate_record(NULL::challenge_comments, b.row_data)).*
		   FROM back b
		  WHERE EXISTS (SELECT 1 FROM challenges c WHERE c.id = (b.row_data->>'challenge_id')::int)
		 ON CONFLICT DO NOTHING`,
			`WITH back AS (DELETE FROM leaving_set_aside WHERE user_id = $1 AND table_name = 'saved_challenges'
		               RETURNING row_data)
		 INSERT INTO saved_challenges SELECT (jsonb_populate_record(NULL::saved_challenges, b.row_data)).*
		   FROM back b
		  WHERE EXISTS (SELECT 1 FROM challenges c WHERE c.id = (b.row_data->>'challenge_id')::int)
		 ON CONFLICT DO NOTHING`,
			`WITH back AS (DELETE FROM leaving_set_aside WHERE user_id = $1 AND table_name = 'video_shares'
		               RETURNING row_data)
		 INSERT INTO video_shares SELECT (jsonb_populate_record(NULL::video_shares, b.row_data)).*
		   FROM back b
		  WHERE EXISTS (SELECT 1 FROM challenges c WHERE c.id = (b.row_data->>'challenge_id')::int)
		 ON CONFLICT DO NOTHING`,
			`WITH back AS (DELETE FROM leaving_set_aside WHERE user_id = $1 AND table_name = 'follows'
		               RETURNING row_data)
		 INSERT INTO follows SELECT (jsonb_populate_record(NULL::follows, b.row_data)).*
		   FROM back b
		  WHERE EXISTS (SELECT 1 FROM users u WHERE u.id = (b.row_data->>'follower_id')::int)
		    AND EXISTS (SELECT 1 FROM users u WHERE u.id = (b.row_data->>'following_id')::int)
		 ON CONFLICT DO NOTHING`,
		} {
			if _, err := tx.ExecContext(ctx, q, id); err != nil {
				return err
			}
		}
		return nil
	}
	for _, q := range []string{
		`WITH moved AS (DELETE FROM challenge_likes WHERE user_id = $1 RETURNING *)
		 INSERT INTO leaving_set_aside (user_id, table_name, row_data)
		 SELECT $1, 'challenge_likes', to_jsonb(moved) FROM moved`,
		`WITH moved AS (DELETE FROM challenge_response_likes WHERE user_id = $1 RETURNING *)
		 INSERT INTO leaving_set_aside (user_id, table_name, row_data)
		 SELECT $1, 'challenge_response_likes', to_jsonb(moved) FROM moved`,
		`WITH moved AS (DELETE FROM challenge_votes WHERE voter_id = $1 RETURNING *)
		 INSERT INTO leaving_set_aside (user_id, table_name, row_data)
		 SELECT $1, 'challenge_votes', to_jsonb(moved) FROM moved`,
		`WITH moved AS (DELETE FROM challenge_dislikes WHERE user_id = $1 RETURNING *)
		 INSERT INTO leaving_set_aside (user_id, table_name, row_data)
		 SELECT $1, 'challenge_dislikes', to_jsonb(moved) FROM moved`,
		`WITH moved AS (DELETE FROM challenge_comments WHERE author_id = $1 RETURNING *)
		 INSERT INTO leaving_set_aside (user_id, table_name, row_data)
		 SELECT $1, 'challenge_comments', to_jsonb(moved) FROM moved`,
		`WITH moved AS (DELETE FROM saved_challenges WHERE user_id = $1 RETURNING *)
		 INSERT INTO leaving_set_aside (user_id, table_name, row_data)
		 SELECT $1, 'saved_challenges', to_jsonb(moved) FROM moved`,
		`WITH moved AS (DELETE FROM video_shares WHERE user_id = $1 RETURNING *)
		 INSERT INTO leaving_set_aside (user_id, table_name, row_data)
		 SELECT $1, 'video_shares', to_jsonb(moved) FROM moved`,
		`WITH moved AS (DELETE FROM follows WHERE follower_id = $1 OR following_id = $1 RETURNING *)
		 INSERT INTO leaving_set_aside (user_id, table_name, row_data)
		 SELECT $1, 'follows', to_jsonb(moved) FROM moved`,
	} {
		if _, err := tx.ExecContext(ctx, q, id); err != nil {
			return err
		}
	}
	return nil
}
