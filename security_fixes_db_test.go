package main

// The before-release security fixes that change accounts, on a real
// database: demo accounts with public passwords are locked, and no
// password stays kept as plain text.

import (
	"database/sql"
	"strconv"
	"testing"
	"time"

	"golang.org/x/crypto/bcrypt"
)

// fastScrambling makes the start-up jobs scramble at bcrypt's lowest
// strength for the rest of the test. Checking a password is the same code
// at any strength.
func fastScrambling(t *testing.T) {
	t.Helper()
	prev := scramble
	scramble = func(p string) (string, error) {
		h, err := bcrypt.GenerateFromPassword([]byte(p), bcrypt.MinCost)
		return string(h), err
	}
	t.Cleanup(func() { scramble = prev })
}

// fastHash is bcrypt at its lowest strength: a test account needs no
// protection, and checking it takes the same code as any other password.
func fastHash(t *testing.T, password string) string {
	t.Helper()
	raw, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

func passwordColumns(t *testing.T, id int) (plain, hash string, changed sql.NullTime) {
	t.Helper()
	if err := db.QueryRow(`SELECT COALESCE(password,''), COALESCE(password_hash,''), password_changed_at
	                         FROM users WHERE id = $1`, id).Scan(&plain, &hash, &changed); err != nil {
		t.Fatal(err)
	}
	return
}

func TestDemoAccounts_PublicPasswordsStopWorking(t *testing.T) {
	defer withDB(t)()
	t.Setenv("TEST_SERVER", "")
	fastScrambling(t)

	const (
		withHash  = 9951 // player1: scrambled public password
		withPlain = 9952 // player2: very old row, public password as plain text
		ownPass   = 9953 // player3: someone's own password under a demo name
		notDemo   = 9954 // not a demo name, but the same password as player1
	)
	names := map[int]string{withHash: "player1", withPlain: "player2", ownPass: "player3", notDemo: "not_a_demo_9954"}
	clean := func() {
		for id, name := range names {
			db.Exec(`DELETE FROM users WHERE id = $1 OR LOWER(username) = LOWER($2)`, id, name)
			forgetCutoff(strconv.Itoa(id))
		}
	}
	clean()
	defer clean()
	mustExec(t, `INSERT INTO users (id, username, password, password_hash) VALUES ($1, 'player1', '', $2)`, withHash, fastHash(t, "pass1"))
	mustExec(t, `INSERT INTO users (id, username, password, password_hash) VALUES ($1, 'player2', 'pass2', '')`, withPlain)
	mustExec(t, `INSERT INTO users (id, username, password, password_hash) VALUES ($1, 'player3', '', $2)`, ownPass, fastHash(t, "player three's own password"))
	mustExec(t, `INSERT INTO users (id, username, password, password_hash) VALUES ($1, 'not_a_demo_9954', '', $2)`, notDemo, fastHash(t, "pass1"))

	// Signed in as player1 an hour ago, as anyone could have.
	oldSignIn := signedInAt(t, strconv.Itoa(withHash), "player1", time.Now().Add(-time.Hour))
	if _, err := parseToken(oldSignIn); err != nil {
		t.Fatalf("the sign-in from before should work before the lock: %v", err)
	}

	if got := lockDemoAccounts(); got != 2 {
		t.Errorf("locked %d demo accounts, want 2 (player1 and player2)", got)
	}

	if IsValidUser("player1", "pass1") {
		t.Error("player1 still signs in with the password written in the public code")
	}
	if IsValidUser("player2", "pass2") {
		t.Error("player2 (an old plain-text row) still signs in with its public password")
	}
	if plain, _, _ := passwordColumns(t, withPlain); plain != "" {
		t.Errorf("player2's public password is still kept as plain text: %q", plain)
	}
	if _, err := parseToken(oldSignIn); err == nil {
		t.Error("someone signed in as player1 before the lock is still signed in")
	}

	if !IsValidUser("player3", "player three's own password") {
		t.Error("an account with a demo name but its own password was locked")
	}
	if _, _, changed := passwordColumns(t, ownPass); changed.Valid {
		t.Error("an account with its own password was signed out")
	}
	if !IsValidUser("not_a_demo_9954", "pass1") {
		t.Error("an account that is not a demo account was locked")
	}

	if got := lockDemoAccounts(); got != 0 {
		t.Errorf("a second start locked %d more; the first should have done them all", got)
	}

	// On a server for testing, nothing is locked.
	t.Setenv("TEST_SERVER", "true")
	mustExec(t, `UPDATE users SET password_hash = $1 WHERE id = $2`, fastHash(t, "pass1"), withHash)
	if got := lockDemoAccounts(); got != 0 {
		t.Errorf("on a server for testing, %d demo accounts were locked", got)
	}
	if !IsValidUser("player1", "pass1") {
		t.Error("on a server for testing, player1 cannot sign in with its demo password")
	}
}

func TestLeftoverPasswords_NoneStaysPlainText(t *testing.T) {
	defer withDB(t)()
	fastScrambling(t)
	// withDB's fixture accounts (900 of them) carry a plain password too.
	// Clear those, so this test counts exactly what it made, and make more
	// than one round's worth (leftoverBatch) of its own.
	mustExec(t, `UPDATE users SET password = '' WHERE id < 9000`)
	const many = leftoverBatch + 50
	db.Exec(`DELETE FROM users WHERE id >= 9970000 AND id < 9980000`)
	defer db.Exec(`DELETE FROM users WHERE id >= 9970000 AND id < 9980000`)
	mustExec(t, `INSERT INTO users (id, username, password, password_hash)
	             SELECT 9970000 + g, 'leftover_many_' || g, 'plain ' || g, ''
	               FROM generate_series(1, $1) AS g`, many)

	const (
		plainOnly = 9955 // an account nobody has signed in to since passwords were scrambled
		bothKept  = 9956 // scrambled, with an old plain copy beside it
		normal    = 9957 // an ordinary account
	)
	clean := func() {
		for _, id := range []int{plainOnly, bothKept, normal} {
			db.Exec(`DELETE FROM users WHERE id = $1`, id)
			forgetCutoff(strconv.Itoa(id))
		}
	}
	clean()
	defer clean()
	mustExec(t, `INSERT INTO users (id, username, password, password_hash) VALUES ($1, 'leftover_9955', 'typed and kept', '')`, plainOnly)
	mustExec(t, `INSERT INTO users (id, username, password, password_hash) VALUES ($1, 'leftover_9956', 'an old copy', $2)`, bothKept, fastHash(t, "the real one"))
	mustExec(t, `INSERT INTO users (id, username, password, password_hash) VALUES ($1, 'leftover_9957', '', $2)`, normal, fastHash(t, "ordinary"))

	hashed, cleared := hashLeftoverPasswords()
	if hashed != many+1 || cleared != 1 {
		t.Errorf("scrambled %d and cleared %d; want %d (more than one round) and 1", hashed, cleared, many+1)
	}
	if !IsValidUser("leftover_many_237", "plain 237") {
		t.Error("an account from the second round cannot sign in with its own password")
	}

	plain, hash, changed := passwordColumns(t, plainOnly)
	if plain != "" || hash == "" {
		t.Errorf("the plain-text account: plain %q, scrambled %q; want only a scrambled one", plain, hash)
	}
	if !IsValidUser("leftover_9955", "typed and kept") {
		t.Error("the plain-text account can no longer sign in with its own password")
	}
	if changed.Valid {
		t.Error("scrambling how a password is kept signed its owner out; the password did not change")
	}

	if plain, _, _ := passwordColumns(t, bothKept); plain != "" {
		t.Errorf("the old plain copy beside a scrambled password is still there: %q", plain)
	}
	if !IsValidUser("leftover_9956", "the real one") || IsValidUser("leftover_9956", "an old copy") {
		t.Error("the account with both: its real password must work and the old copy must not")
	}
	if !IsValidUser("leftover_9957", "ordinary") {
		t.Error("an ordinary account stopped signing in")
	}

	var left int
	if err := db.QueryRow(`SELECT COUNT(*) FROM users WHERE COALESCE(password, '') <> ''`).Scan(&left); err != nil {
		t.Fatal(err)
	}
	if left != 0 {
		t.Errorf("%d passwords are still kept as plain text", left)
	}
	if h, c := hashLeftoverPasswords(); h != 0 || c != 0 {
		t.Errorf("a second run found %d to scramble and %d to clear; the first should have done them all", h, c)
	}
}
