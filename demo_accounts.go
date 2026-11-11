package main

// demo_accounts.go — the demo accounts are deleted, and stay deleted.
//
// This server used to fill a new, empty database with demo data: ten
// accounts (player1 … cyberking) with the passwords pass1 … pass10. A test
// tool (cmd/seed, now gone) made six more (maya, deven, nina, omar, and
// player1 and player2 again) with the password "password123". Every one of
// those passwords was written in code anyone can read on GitHub, so anyone
// could sign in as them and post whatever they liked under those names.
//
// The owner asked for all of it to go before release:
//
//   - a new database starts empty. No demo data, on any server;
//   - the "wipe everything and reload demo data" route is gone;
//   - the tools that made demo accounts are gone;
//   - and at every start, an account that is still one of those demo
//     accounts is deleted for good, the same way an account whose owner
//     deleted it is: its videos, the answers to them, its likes, follows
//     and comments, and its files in storage.
//
// "Still one of those demo accounts" is checked carefully, because a real
// person can have signed up under a free demo name since:
//
//   - the username is one of the demo names, AND
//   - it never went through sign-up: no email and no agreement to the
//     terms (sign-up records both; the demo data recorded neither), AND
//   - it still has the demo's own full name, or a demo password still
//     opens it.
//
// Anything else under a demo name is somebody's own account, and is left
// alone.

import (
	"context"
	"database/sql"
	"log"
	"strconv"
	"time"
)

// demoAccount is one account the demo data made.
type demoAccount struct {
	username, fullName string
	// passwords it was made with, all public.
	passwords []string
}

// demoAccounts are every account the old demo data and the old seed tool
// made. Kept only so they can be found and deleted.
var demoAccounts = []demoAccount{
	{"player1", "Player One", []string{"pass1", "password123"}},
	{"player2", "Player Two", []string{"pass2", "password123"}},
	{"player3", "Player Three", []string{"pass3"}},
	{"shadowstrike", "Shadow Strike", []string{"pass4"}},
	{"blazerunner", "Blaze Runner", []string{"pass5"}},
	{"stormchaser", "Storm Chaser", []string{"pass6"}},
	{"frostbyte", "Frost Byte", []string{"pass7"}},
	{"nightowl", "Night Owl", []string{"pass8"}},
	{"thunderbolt", "Thunder Bolt", []string{"pass9"}},
	{"cyberking", "Cyber King", []string{"pass10"}},
	{"maya", "Maya Rivera", []string{"password123"}},
	{"deven", "Deven Shah", []string{"password123"}},
	{"nina", "Nina Kapoor", []string{"password123"}},
	{"omar", "Omar Haddad", []string{"password123"}},
}

// opensWithDemoPassword is whether one of the demo's public passwords
// still signs in to a row with this plain password or this scrambled one.
func (d demoAccount) opensWithDemoPassword(plain, hash string) bool {
	for _, p := range d.passwords {
		if plain == p || (hash != "" && checkPassword(hash, p)) {
			return true
		}
	}
	return false
}

// removeDemoAccounts deletes for good every account that is still a demo
// account (see the top of this file for what that means), and says how
// many it deleted. Finding none is the normal case after the first start.
func removeDemoAccounts(ctx context.Context) int {
	if db == nil {
		return 0
	}
	removed := 0
	for _, d := range demoAccounts {
		var id int
		var fullName, plain, hash string
		err := db.QueryRowContext(ctx, `
			SELECT id, COALESCE(full_name, ''), COALESCE(password, ''), COALESCE(password_hash, '')
			  FROM users
			 WHERE LOWER(username) = $1
			   AND COALESCE(email, '') = ''
			   AND terms_accepted_at IS NULL`, d.username).Scan(&id, &fullName, &plain, &hash)
		if err == sql.ErrNoRows {
			continue
		}
		if queryFailed("whether demo account "+d.username+" is still there",
			"leaving it until the next start", err) {
			continue
		}
		if fullName != d.fullName && !d.opensWithDemoPassword(plain, hash) {
			// Somebody's own account under a demo name.
			continue
		}
		uid := strconv.Itoa(id)
		// The same two steps as an account whose owner deleted it: hidden
		// and signed out first, then deleted for good — without the 30 days,
		// because nobody is coming back for a demo account.
		if _, err := requestAccountDeletion(ctx, uid); err != nil {
			log.Printf("demo accounts: %s could not be hidden and signed out (%v); tried again at the next start", d.username, err)
			continue
		}
		if err := purgeAccount(uid); err != nil {
			log.Printf("demo accounts: %s is hidden and signed out, but deleting it for good failed (%v); "+
				"tried again at the next start", d.username, err)
			continue
		}
		removed++
	}
	if removed > 0 {
		log.Printf("demo accounts: %d deleted for good, with their videos. Their passwords were written in the public code.", removed)
	}
	return removed
}

// startupAccountChecks runs, once the database and the cache are up, the
// two jobs that close old doors: no password kept as plain text
// (leftover_passwords.go), and no demo account.
func startupAccountChecks() {
	hashLeftoverPasswords()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	removeDemoAccounts(ctx)
}
