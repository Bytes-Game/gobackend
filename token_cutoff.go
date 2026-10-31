package main

// token_cutoff.go — changing your password signs out every other phone.
//
// A sign-in is a token that is good for seven days. Until now nothing could
// take one back: if somebody got your password and signed in, changing it
// did not stop them — their sign-in kept working until it ran out on its
// own. And there was no way to change a password at all.
//
// Now each account remembers when its password last changed
// (users.password_changed_at), and a sign-in from before that moment is
// refused everywhere a sign-in is read (parseToken). The phone that made
// the change gets a fresh sign-in in the same answer, so it stays in.
//
// Looked up at most once every cutoffFresh per person, so this is not a
// database read on every request. A database that cannot answer refuses
// nothing — the same sign-ins keep working, and the failure is logged.
//
// Deleting an account signs it out the same way, more firmly: while it
// waits to be deleted (account_leaving.go) no sign-in of it works at all.
// Keeping the account afterwards sets users.signed_out_at, so the phones
// signed in before stay signed out; only the one that kept it is let in.

import (
	"database/sql"
	"strconv"
	"sync"
	"time"
)

// cutoffFresh is how long a looked-up cutoff is trusted before it is read
// again. A sign-out takes at most this long to reach every other server.
const cutoffFresh = 30 * time.Second

type cutoffEntry struct {
	at      time.Time // zero: password never changed, never signed out
	leaving bool      // waiting to be deleted: no sign-in works
	checked time.Time
}

var cutoffCache sync.Map // user id → cutoffEntry

// tokenCutoff is when [userID] last changed their password or was signed
// out everywhere (zero if never), and whether the account is waiting to be
// deleted.
func tokenCutoff(userID string) (time.Time, bool) {
	if v, ok := cutoffCache.Load(userID); ok {
		if e := v.(cutoffEntry); time.Since(e.checked) < cutoffFresh {
			return e.at, e.leaving
		}
	}
	id, err := strconv.Atoi(userID)
	if db == nil || err != nil {
		return time.Time{}, false
	}
	var at sql.NullTime
	var leaving bool
	err = db.QueryRow(`
		SELECT GREATEST(password_changed_at, signed_out_at),
		       deletion_requested_at IS NOT NULL
		  FROM users WHERE id = $1`, id).Scan(&at, &leaving)
	if err != nil && err != sql.ErrNoRows {
		queryFailed("reading when user "+userID+" last changed their password or was signed out",
			"letting their sign-in through", err)
		return time.Time{}, false
	}
	e := cutoffEntry{checked: time.Now(), leaving: leaving}
	if at.Valid {
		e.at = at.Time
	}
	cutoffCache.Store(userID, e)
	return e.at, e.leaving
}

// forgetCutoff drops what is remembered for [userID], after a change.
func forgetCutoff(userID string) { cutoffCache.Delete(userID) }

// signedInBeforeCutoff is whether a sign-in made at [issued] by [userID]
// is no longer good: from before their last password change or sign-out,
// or any sign-in at all while the account waits to be deleted.
func signedInBeforeCutoff(userID string, issued time.Time) bool {
	cut, leaving := tokenCutoff(userID)
	if leaving {
		return true
	}
	if cut.IsZero() {
		return false
	}
	return issued.Before(cut)
}
