package main

// test_server.go — what only a server for testing may do.
//
// Two things in this code are fine on a server for testing and dangerous
// on the live one:
//
//   - wiping the whole database and filling it with demo data
//     (POST /api/v1/admin/reseed, and the demo data a new, empty database
//     used to get on its first start);
//   - ten demo accounts (player1 … cyberking) whose passwords are written
//     below, in code anyone can read on GitHub. Anyone could sign in as
//     them and post whatever they liked under those names.
//
// Both now need TEST_SERVER=true on the server. Without it:
//
//   - the wipe is refused;
//   - a new, empty database is left empty;
//   - the demo accounts cannot sign in. At every start, a demo account
//     whose password is still the one written here gets a random password
//     nobody knows, and everyone already signed in as it is signed out.
//     Their posts stay. An account that only shares a demo name, with a
//     password of its own, is left alone.
//
// With it, the wipe works and a wipe brings the demo accounts back with
// the passwords below.

import (
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
)

// testServer is whether this server is one for testing (TEST_SERVER=true).
func testServer() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("TEST_SERVER"))) {
	case "true", "1", "yes", "on":
		return true
	}
	return false
}

// demoAccount is one of the accounts the demo data creates.
type demoAccount struct {
	username, password, fullName, league string
	wins, losses                         int
}

// demoAccounts are the demo data's accounts. Their passwords are public:
// they are right here. seedUsers creates them; lockDemoAccounts makes sure
// they cannot sign in anywhere but a server for testing.
var demoAccounts = []demoAccount{
	{"player1", "pass1", "Player One", "Gold", 32, 18},
	{"player2", "pass2", "Player Two", "Diamond", 120, 45},
	{"player3", "pass3", "Player Three", "Bronze", 10, 5},
	{"shadowstrike", "pass4", "Shadow Strike", "Platinum", 95, 30},
	{"blazerunner", "pass5", "Blaze Runner", "Silver", 55, 40},
	{"stormchaser", "pass6", "Storm Chaser", "Gold", 78, 22},
	{"frostbyte", "pass7", "Frost Byte", "Dianond", 140, 50},
	{"nightowl", "pass8", "Night Owl", "Bronze", 15, 10},
	{"thunderbolt", "pass9", "Thunder Bolt", "Silver", 62, 38},
	{"cyberking", "pass10", "Cyber King", "Platinum", 100, 25},
}

// reseedRefusal is what the wipe says on a server that is not for testing.
const reseedRefusal = "Wiping the database is switched off on this server. " +
	"It is only allowed on a server for testing, with TEST_SERVER=true."

// lockDemoAccounts gives every demo account that still has its public
// password a random one nobody knows, and signs out everyone signed in as
// it. Nothing is done on a server for testing. It returns how many
// accounts it locked.
//
// "Still has its public password" is checked on both places a password can
// be: the scrambled one, and the plain one very old rows still carry.
func lockDemoAccounts() int {
	if testServer() {
		return 0
	}
	locked := 0
	for _, a := range demoAccounts {
		var id int
		var plain, hash string
		err := db.QueryRow(`
			SELECT id, COALESCE(password, ''), COALESCE(password_hash, '')
			  FROM users WHERE username = $1`, a.username).Scan(&id, &plain, &hash)
		if err == sql.ErrNoRows {
			continue
		}
		if err != nil {
			queryFailed("checking whether demo account "+a.username+" still has its public password",
				"leaving it as it is until the next start", err)
			continue
		}
		public := plain == a.password || (hash != "" && checkPassword(hash, a.password))
		if !public {
			continue
		}
		secret := make([]byte, 32)
		if _, err := rand.Read(secret); err != nil {
			log.Printf("demo accounts: no random password for %s (%v); it can still sign in with its public one", a.username, err)
			continue
		}
		newHash, err := scramble(base64.RawStdEncoding.EncodeToString(secret))
		if err != nil {
			log.Printf("demo accounts: could not scramble a password for %s (%v); it can still sign in with its public one", a.username, err)
			continue
		}
		if _, err := db.Exec(`
			UPDATE users
			   SET password_hash = $1, password = '', password_changed_at = NOW()
			 WHERE id = $2`, newHash, id); err != nil {
			log.Printf("demo accounts: could not lock %s (%v); it can still sign in with its public one", a.username, err)
			continue
		}
		forgetCutoff(strconv.Itoa(id))
		locked++
	}
	if locked > 0 {
		log.Printf("demo accounts: %d could sign in with a password written in the public code; "+
			"each now has a random one and everyone signed in as them is signed out. "+
			"They work again only on a server for testing (TEST_SERVER=true), after a wipe.", locked)
	}
	return locked
}

// reseedLock keeps two wipes from running over each other.
var reseedLock sync.Mutex

// ReseedHandler wipes the database and fills it with demo data — on a
// server for testing only (TEST_SERVER=true). Anywhere else it is refused,
// and nothing is touched.
func ReseedHandler(w http.ResponseWriter, r *http.Request) {
	if !testServer() {
		log.Printf("admin: a wipe of the database was asked for and refused: this is not a server for testing")
		writeJSON(w, http.StatusForbidden, map[string]string{"error": reseedRefusal})
		return
	}
	reseedLock.Lock()
	defer reseedLock.Unlock()
	wipeAndReseed()
	writeJSON(w, http.StatusOK, map[string]string{"status": "reseeded"})
}

// wipeAndReseed is the wipe itself, a variable so a test can see whether
// the wipe was reached without wiping its database.
var wipeAndReseed = ReseedDatabase

// shouldSeedDemoData is whether a database holding [users] accounts gets
// the demo data at start: only an empty one, on a server for testing.
func shouldSeedDemoData(users int) bool {
	if users > 0 {
		log.Println("Database already has data - skipping seed")
		return false
	}
	if !testServer() {
		log.Println("Database is empty and stays empty: demo data is only for a server for testing (TEST_SERVER=true)")
		return false
	}
	return true
}
