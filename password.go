package main

import "golang.org/x/crypto/bcrypt"

// ════════════════════════════════════════════════════════════════════════════════
// PASSWORD HASHING — bcrypt
// ════════════════════════════════════════════════════════════════════════════════
//
// Replaces the old plaintext password storage + plaintext SQL comparison. New
// and seeded passwords are stored as bcrypt hashes in users.password_hash;
// legacy rows that still carry a plaintext users.password are migrated to a
// hash on first successful login (see IsValidUser), and the rest in the
// background at every start (leftover_passwords.go) — waiting for a login
// left the password of everyone who never came back readable for ever.
//
// bcrypt silently truncates input at 72 bytes; that's far beyond any realistic
// password and matches every other bcrypt deployment, so we don't pre-hash.

// passwordCost is how hard a scrambled password, or a code sent by email,
// is to undo: bcrypt's default, 10. One step more doubles the time every
// guess takes, for the server and for anybody who ever got a copy.
//
// The tests turn it down to bcrypt's lowest (TestMain, setup_test.go).
// Under the race checker one scramble at 10 takes about a second, and a
// sign-up does three (its emailed code twice, then the password): on 15
// November the sign-up tests pushed the whole run past Go's ten-minute
// limit. Checking works the same at any strength, because the strength is
// written into the scrambled text. A test checks that the server's own
// stays 10 (TestPasswordCost_TheServerScramblesAtBcryptsDefault).
var passwordCost = bcrypt.DefaultCost

// hashPassword returns a bcrypt hash of the plaintext password at
// passwordCost.
func hashPassword(plain string) (string, error) {
	h, err := bcrypt.GenerateFromPassword([]byte(plain), passwordCost)
	if err != nil {
		return "", err
	}
	return string(h), nil
}

// checkPassword reports whether plain matches the stored bcrypt hash. Returns
// false for any error (malformed hash, mismatch) — never panics.
func checkPassword(hash, plain string) bool {
	return bcrypt.CompareHashAndPassword([]byte(hash), []byte(plain)) == nil
}
