package main

// The before-release security fixes that change accounts, on a real
// database: demo accounts with public passwords are locked, and no
// password stays kept as plain text.

import (
	"context"
	"database/sql"
	"strconv"
	"testing"

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

func TestDemoAccounts_AreDeletedForGood(t *testing.T) {
	defer withDB(t)()
	resetRedis(t)

	const (
		seeded     = 9951 // player1, as the old start-up data made it
		toolMade   = 9952 // maya, as the old seed tool made it
		renamed    = 9953 // omar with another name, but the tool's password still opens it
		oldPlain   = 9954 // player2, a very old row with its password as plain text
		ownAccount = 9955 // nina: a real person who signed up under a free demo name
		ownOld     = 9956 // deven: a real early account (no email, no terms), own name and password
		notDemo    = 9957 // not a demo name, though its password is a demo one
	)
	ids := []int{seeded, toolMade, renamed, oldPlain, ownAccount, ownOld, notDemo}
	names := []string{"player1", "maya", "omar", "player2", "nina", "deven", "not_a_demo_9957"}
	clean := func() {
		for i, id := range ids {
			db.Exec(`DELETE FROM challenges WHERE creator_id = $1`, id)
			db.Exec(`DELETE FROM users WHERE id = $1 OR LOWER(username) = $2`, id, names[i])
			forgetCutoff(strconv.Itoa(id))
		}
		forgetLeaving()
	}
	clean()
	defer clean()
	add := func(id int, username, fullName, plain, password string) {
		t.Helper()
		hash := ""
		if password != "" {
			hash = fastHash(t, password)
		}
		mustExec(t, `INSERT INTO users (id, username, full_name, password, password_hash) VALUES ($1, $2, $3, $4, $5)`,
			id, username, fullName, plain, hash)
	}
	add(seeded, "player1", "Player One", "", "pass1")
	add(toolMade, "maya", "Maya Rivera", "", "password123")
	add(renamed, "omar", "Somebody Else", "", "password123")
	add(oldPlain, "player2", "Renamed Too", "pass2", "")
	add(ownOld, "deven", "Deven Someone", "", "deven's own password")
	add(notDemo, "not_a_demo_9957", "Player One", "", "pass1")
	// A real sign-up under a demo name records an email and the terms.
	mustExec(t, `INSERT INTO users (id, username, full_name, password, password_hash, email, terms_accepted_at)
	             VALUES ($1, 'nina', 'Nina Kapoor', '', $2, 'nina@example.com', NOW())`, ownAccount, fastHash(t, "password123"))

	video := mustID(t, `INSERT INTO challenges (creator_id, prefix, subject) VALUES ($1, 'Show', 'a demo trick') RETURNING id`, seeded)
	answerTo := mustID(t, `INSERT INTO challenges (creator_id, prefix, subject) VALUES ($1, 'Sing', 'a song') RETURNING id`, ownOld)
	mustExec(t, `INSERT INTO challenge_responses (challenge_id, responder_id, video_url) VALUES ($1, $2, '')`, answerTo, toolMade)
	mustExec(t, `INSERT INTO follows (follower_id, following_id) VALUES ($1, $2)`, seeded, ownOld)

	exists := func(id int) bool {
		var n int
		if err := db.QueryRow(`SELECT COUNT(*) FROM users WHERE id = $1`, id).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n == 1
	}

	if got := removeDemoAccounts(context.Background()); got != 4 {
		t.Errorf("deleted %d demo accounts, want 4 (player1, maya, omar, player2)", got)
	}
	for _, id := range []int{seeded, toolMade, renamed, oldPlain} {
		if exists(id) {
			t.Errorf("demo account %d is still there", id)
		}
	}
	for _, id := range []int{ownAccount, ownOld, notDemo} {
		if !exists(id) {
			t.Errorf("account %d is somebody's own, and was deleted", id)
		}
	}
	if IsValidUser("player1", "pass1") || IsValidUser("maya", "password123") {
		t.Error("a demo account still signs in with a password from the public code")
	}
	var left int
	db.QueryRow(`SELECT COUNT(*) FROM challenges WHERE id = $1`, video).Scan(&left)
	if left != 0 {
		t.Error("the demo account's video is still there")
	}
	db.QueryRow(`SELECT COUNT(*) FROM challenge_responses WHERE challenge_id = $1`, answerTo).Scan(&left)
	if left != 0 {
		t.Error("the demo account's answer to a real person's post is still there")
	}
	db.QueryRow(`SELECT COUNT(*) FROM challenges WHERE id = $1`, answerTo).Scan(&left)
	if left != 1 {
		t.Error("a real person's post was deleted along with a demo account's answer to it")
	}
	db.QueryRow(`SELECT COUNT(*) FROM follows WHERE following_id = $1`, ownOld).Scan(&left)
	if left != 0 {
		t.Error("a real person is still followed by a deleted demo account")
	}

	if got := removeDemoAccounts(context.Background()); got != 0 {
		t.Errorf("a second start deleted %d more; the first should have done them all", got)
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
