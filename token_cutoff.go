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
	at      time.Time // zero: password never changed
	checked time.Time
}

var cutoffCache sync.Map // user id → cutoffEntry

// tokenCutoff is when [userID] last changed their password, or zero.
func tokenCutoff(userID string) time.Time {
	if v, ok := cutoffCache.Load(userID); ok {
		if e := v.(cutoffEntry); time.Since(e.checked) < cutoffFresh {
			return e.at
		}
	}
	id, err := strconv.Atoi(userID)
	if db == nil || err != nil {
		return time.Time{}
	}
	var at sql.NullTime
	err = db.QueryRow(`SELECT password_changed_at FROM users WHERE id = $1`, id).Scan(&at)
	if err != nil && err != sql.ErrNoRows {
		queryFailed("reading when user "+userID+" last changed their password",
			"letting their sign-in through", err)
		return time.Time{}
	}
	e := cutoffEntry{checked: time.Now()}
	if at.Valid {
		e.at = at.Time
	}
	cutoffCache.Store(userID, e)
	return e.at
}

// forgetCutoff drops what is remembered for [userID], after a change.
func forgetCutoff(userID string) { cutoffCache.Delete(userID) }

// signedInBeforeCutoff is whether a sign-in made at [issued] by [userID]
// is from before their last password change, and so no longer good.
func signedInBeforeCutoff(userID string, issued time.Time) bool {
	cut := tokenCutoff(userID)
	if cut.IsZero() {
		return false
	}
	return issued.Before(cut)
}
