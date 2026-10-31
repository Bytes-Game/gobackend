package main

// One name, one person, whatever the capital letters.
//
// Sign-up lowers every new name, but the database can hold older ones with
// capitals in them. "CaseKaran" and "casekaran" must not be two accounts:
// they look the same to everybody reading them. Through the real sign-up
// handler against a real database.

import (
	"bytes"
	"log"
	"os"
	"strings"
	"testing"
)

func TestUsernames_OneNameWhateverTheCapitalLetters(t *testing.T) {
	defer withDB(t)()
	resetRedis(t)
	resetActionLimiters(t)
	clean := func() {
		db.Exec(`DELETE FROM users WHERE lower(username) = 'casekaran'
		                             OR lower(email) = 'new31@example.com'`)
	}
	clean()
	defer clean()
	mustExec(t, `INSERT INTO users (id, username, password) VALUES (9931, 'CaseKaran', 'x')`)

	if !UserExists("casekaran") {
		t.Error("the name in lower case reads as free while 'CaseKaran' has it")
	}
	body := goodSignup(31)
	body["username"] = "casekaran"
	if code, got := signUp(t, body, "203.0.113.31"); code != 409 || got["field"] != "username" {
		t.Errorf("signing up as 'casekaran' while 'CaseKaran' exists answered %d %v", code, got)
	}

	// Two sign-ups racing past the check are the database's to refuse.
	if _, err := db.Exec(`INSERT INTO users (username, password) VALUES ('CASEKARAN', 'x')`); err == nil {
		t.Error("the database took a second account named the same in other capital letters")
	}
}

// signedInAsWho signs in through the real handler, and says which account it
// signed in to and the name its sign-in carries — the name the chat
// connection is opened with, so it has to be the stored one, not the typed
// one.
func signedInAsWho(t *testing.T, typed, password string) (code int, id, nameInSignIn string) {
	t.Helper()
	code, got := signInAs(t, typed, password, false)
	if code != 200 {
		return code, "", ""
	}
	user, _ := got["user"].(map[string]any)
	tok, _ := got["token"].(string)
	claims, err := parseToken(tok)
	if err != nil {
		t.Fatalf("signing in as %q handed out a sign-in that does not work: %v", typed, err)
	}
	if user["username"] != claims.Username {
		t.Errorf("signing in as %q: the account says %v but its sign-in says %q",
			typed, user["username"], claims.Username)
	}
	id, _ = user["id"].(string)
	return code, id, claims.Username
}

func TestUsernames_SigningInWorksWhateverTheCapitals(t *testing.T) {
	defer withDB(t)()
	resetRedis(t)
	// Sign-up stores new names in lower case; older accounts can have capitals.
	defer leaver(t, 9851, "lower_name", "sunset42go")()
	defer leaver(t, 9852, "Old_Caps", "sunset42go")()

	for typed, want := range map[string]string{
		"lower_name":   "lower_name",
		"Lower_Name":   "lower_name",
		"LOWER_NAME":   "lower_name",
		" lower_name ": "lower_name",
		"Old_Caps":     "Old_Caps",
		"old_caps":     "Old_Caps",
	} {
		code, _, name := signedInAsWho(t, typed, "sunset42go")
		if code != 200 || name != want {
			t.Errorf("signing in as %q answered %d as %q, want 200 as %q", typed, code, name, want)
		}
	}
	if code, _ := signInAs(t, "Lower_Name", "wrong-password", false); code != 401 {
		t.Errorf("a wrong password in other capitals answered %d, want 401", code)
	}
	if code, _ := signInAs(t, "lower_namez", "sunset42go", false); code != 401 {
		t.Errorf("a name nobody has answered %d, want 401", code)
	}
}

// Two older accounts whose names differ only in capitals: the database lock
// cannot be switched on, and sign-in must never guess between them. Each
// start names them in the log; the first start after one is renamed
// switches the lock on. Through runMigrations, which is what every start
// runs.
func TestUsernames_TheLockComesOnAtStartOnceNoTwoNamesClash(t *testing.T) {
	defer withDB(t)()
	resetRedis(t)
	// Last of all: the lock back as the other tests expect it.
	defer db.Exec(`CREATE UNIQUE INDEX IF NOT EXISTS users_username_lower_key ON users (lower(username))`)
	mustExec(t, `DROP INDEX IF EXISTS users_username_lower_key`)
	defer leaver(t, 9861, "Twin_Name", "sunset42go")()
	defer leaver(t, 9862, "twin_name", "moonrise42go")()

	lockOn := func() bool {
		t.Helper()
		var on bool
		if err := db.QueryRow(`SELECT to_regclass('users_username_lower_key') IS NOT NULL`).Scan(&on); err != nil {
			t.Fatal(err)
		}
		return on
	}
	start := func() string {
		t.Helper()
		var buf bytes.Buffer
		log.SetOutput(&buf)
		defer log.SetOutput(os.Stderr)
		if err := runMigrations(); err != nil {
			t.Fatalf("starting up failed: %v", err)
		}
		return buf.String()
	}

	// Each signs in by its exact name; other capitals are nobody's.
	if code, id, _ := signedInAsWho(t, "Twin_Name", "sunset42go"); code != 200 || id != "9861" {
		t.Errorf("'Twin_Name' signed in to %q (%d), want 9861", id, code)
	}
	if code, id, _ := signedInAsWho(t, "twin_name", "moonrise42go"); code != 200 || id != "9862" {
		t.Errorf("'twin_name' signed in to %q (%d), want 9862", id, code)
	}
	for _, password := range []string{"sunset42go", "moonrise42go"} {
		if code, id, _ := signedInAsWho(t, "TWIN_NAME", password); code != 401 {
			t.Errorf("'TWIN_NAME' signed in to %q (%d): it guessed between two accounts", id, code)
		}
	}

	logged := start()
	if lockOn() {
		t.Fatal("the lock was switched on while two names clash")
	}
	if !strings.Contains(logged, "Twin_Name, twin_name differ only in capital letters") {
		t.Errorf("starting up did not name the clash in the log:\n%s", logged)
	}
	// Just that: not a failed attempt as well, every start, for the owner to
	// puzzle over.
	if strings.Contains(logged, "could not be set") {
		t.Errorf("starting up tried to switch the lock on while names clash:\n%s", logged)
	}

	// One is renamed: the next start switches the lock on, and says so.
	mustExec(t, `UPDATE users SET username = 'twin_name_two' WHERE id = 9862`)
	logged = start()
	if !lockOn() {
		t.Fatal("the lock is still off at the first start with no clash")
	}
	if !strings.Contains(logged, "the database now refuses a name taken in other capital letters") {
		t.Errorf("switching the lock on was not logged:\n%s", logged)
	}
	if _, err := db.Exec(`INSERT INTO users (username, password) VALUES ('TWIN_NAME', '')`); err == nil {
		db.Exec(`DELETE FROM users WHERE username = 'TWIN_NAME'`)
		t.Error("the database took a name taken in other capitals after the lock came on")
	}
	// And the one left now signs in whatever the capitals.
	if code, id, _ := signedInAsWho(t, "TWIN_NAME", "sunset42go"); code != 200 || id != "9861" {
		t.Errorf("'TWIN_NAME' signed in to %q (%d) once the clash was gone, want 9861", id, code)
	}
}
