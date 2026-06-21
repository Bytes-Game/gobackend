package main

import "testing"

func TestHashPassword_RoundTrip(t *testing.T) {
	const pw = "correct horse battery staple"
	hash, err := hashPassword(pw)
	if err != nil {
		t.Fatalf("hashPassword: %v", err)
	}
	if hash == "" || hash == pw {
		t.Fatalf("hash looks wrong: %q", hash)
	}
	if !checkPassword(hash, pw) {
		t.Error("checkPassword rejected the correct password")
	}
	if checkPassword(hash, "wrong password") {
		t.Error("checkPassword accepted a wrong password")
	}
}

// bcrypt salts per-call, so two hashes of the same input must differ yet both
// verify — confirms we're not accidentally producing deterministic digests.
func TestHashPassword_DistinctSalts(t *testing.T) {
	h1, _ := hashPassword("same")
	h2, _ := hashPassword("same")
	if h1 == h2 {
		t.Error("two hashes of the same password are identical — salting is broken")
	}
	if !checkPassword(h1, "same") || !checkPassword(h2, "same") {
		t.Error("both salted hashes should still verify")
	}
}

func TestCheckPassword_RejectsGarbageHash(t *testing.T) {
	if checkPassword("not-a-bcrypt-hash", "whatever") {
		t.Error("checkPassword should reject a malformed hash instead of panicking/allowing")
	}
}
