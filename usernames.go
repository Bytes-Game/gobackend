package main

// usernames.go — a username is one name, whatever its capital letters.
//
// "Karan", "karan" and "KARAN" are the same name. Sign-up refuses a name
// taken in any capitals (UserExists). The database refuses one too, once its
// lock is on (ensureUsernameLock). And signing in finds the account whatever
// capitals are typed (storedUsername).
//
// Usernames cannot be changed after sign-up (the app says so on Edit
// profile), so sign-up is the only way a name is chosen. The lock is what
// keeps that true for anything added later: it refuses a taken name however
// the row is written.

import (
	"log"
	"strings"
)

// usernameLock is the database's own refusal of a name taken in other
// capitals. Sign-up checks first, but two sign-ups at the same moment can
// both pass that check before either is saved; the lock stops the second.
const usernameLock = "users_username_lower_key"

// ensureUsernameLock switches the lock on, at every start until it is on.
//
// The database cannot build it while two accounts already have names that
// differ only in capitals. Until then, every start names each such group in
// the log, and the first start after all but one of a group are renamed or
// deleted switches the lock on. Migration 023 tried once, but a migration
// never runs twice: without this, a clash on the day it ran would have left
// the lock off for good, with nothing in the log to say so.
func ensureUsernameLock() {
	var on bool
	err := db.QueryRow(`SELECT to_regclass($1) IS NOT NULL`, usernameLock).Scan(&on)
	if queryFailed("whether the database refuses usernames taken in other capitals",
		"looked at again on the next start", err) || on {
		return
	}
	rows, err := db.Query(`
		SELECT string_agg(username, ', ' ORDER BY id)
		  FROM users
		 GROUP BY lower(username)
		HAVING COUNT(*) > 1`)
	if queryFailed("which usernames differ only in capital letters",
		"the database does not refuse them yet; looked at again on the next start", err) {
		return
	}
	clashes, bad := 0, 0
	for rows.Next() {
		var names string
		if scanFailed("usernames that differ only in capital letters", rows.Scan(&names), &bad) {
			continue
		}
		clashes++
		log.Printf("usernames: %s differ only in capital letters. Sign-up refuses new names "+
			"like these, but the database cannot refuse them until all but one are renamed "+
			"or deleted", names)
	}
	rows.Close()
	if clashes > 0 || bad > 0 {
		return
	}
	if _, err := db.Exec(`CREATE UNIQUE INDEX IF NOT EXISTS ` + usernameLock +
		` ON users (lower(username))`); err != nil {
		log.Printf("usernames: the database could not be set to refuse names taken in other "+
			"capital letters: %v — tried again on the next start", err)
		return
	}
	log.Printf("usernames: the database now refuses a name taken in other capital letters")
}

// storedUsername is the name of the account [typed] signs in to whatever its
// capital letters: the one account whose name differs from it at most in
// capitals. "" when there is none, and the name is then tried as typed.
//
// "" too when two older accounts differ only in capitals: guessing between
// them could sign somebody in to the wrong one. Each still signs in by its
// exact name, and ensureUsernameLock names them in the log until one is
// renamed.
func storedUsername(typed string) string {
	rows, err := db.Query(`SELECT username FROM users WHERE lower(username) = lower($1) LIMIT 2`,
		strings.TrimSpace(typed))
	if queryFailed("which account a username signs in to, whatever its capitals",
		"signing in with the name exactly as typed", err) {
		return ""
	}
	var names []string
	bad := 0
	for rows.Next() {
		var name string
		if scanFailed("an account a username signs in to", rows.Scan(&name), &bad) {
			continue
		}
		names = append(names, name)
	}
	rows.Close()
	if len(names) == 1 {
		return names[0]
	}
	return ""
}
