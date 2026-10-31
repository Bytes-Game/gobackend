package main

// One name, one person, whatever the capital letters.
//
// Sign-up lowers every new name, but the database can hold older ones with
// capitals in them. "CaseKaran" and "casekaran" must not be two accounts:
// they look the same to everybody reading them. Through the real sign-up
// handler against a real database.

import "testing"

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
