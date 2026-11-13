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

// hashPassword returns a bcrypt hash of the plaintext password at the default
// cost (currently 10).
func hashPassword(plain string) (string, error) {
	h, err := bcrypt.GenerateFromPassword([]byte(plain), bcrypt.DefaultCost)
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
