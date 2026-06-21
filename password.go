package main

import "golang.org/x/crypto/bcrypt"

// ════════════════════════════════════════════════════════════════════════════════
// PASSWORD HASHING — bcrypt
// ════════════════════════════════════════════════════════════════════════════════
//
// Replaces the old plaintext password storage + plaintext SQL comparison. New
// and seeded passwords are stored as bcrypt hashes in users.password_hash;
// legacy rows that still carry a plaintext users.password are migrated to a
// hash on first successful login (see IsValidUser), which amortizes the rehash
// cost across logins rather than blocking startup with a bulk re-hash — the
// scalable choice for a large user base.
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
