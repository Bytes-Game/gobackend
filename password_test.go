package main

import (
	"context"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"golang.org/x/crypto/bcrypt"
)

// serverPasswordCostEnv, when set, makes TestMain leave the server's own
// password strength alone in this test program.
const serverPasswordCostEnv = "TEST_SERVER_PASSWORD_COST"

// The server scrambles passwords and emailed codes at bcrypt's default
// strength, 10. The tests run at the lowest, to be quick (TestMain). So
// this test starts a fresh copy of the test program with the server's own
// strength left alone, and checks a scramble made by hashPassword there.
//
// The copy is told to run only "server_strength", so it never starts a
// copy of its own, even if the setting stopped reaching it: that part
// would then skip, and "fresh_copy" here fails.
func TestPasswordCost_TheServerScramblesAtBcryptsDefault(t *testing.T) {
	const name = "TestPasswordCost_TheServerScramblesAtBcryptsDefault"
	inCopy := os.Getenv(serverPasswordCostEnv) != ""

	t.Run("server_strength", func(t *testing.T) {
		if !inCopy {
			t.Skip("runs in the fresh copy of the test program")
		}
		if got := strengthOf(t, "a password"); got != bcrypt.DefaultCost {
			t.Fatalf("the server scrambles passwords at strength %d, want bcrypt's default %d", got, bcrypt.DefaultCost)
		}
	})

	t.Run("fresh_copy", func(t *testing.T) {
		if inCopy {
			t.Skip("this is the fresh copy")
		}
		// Here the tests' own strength is in force.
		if got := strengthOf(t, "a password"); got != bcrypt.MinCost {
			t.Errorf("the tests scramble at strength %d, want bcrypt's lowest %d: at the server's own, the sign-up tests take minutes", got, bcrypt.MinCost)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		run := exec.CommandContext(ctx, os.Args[0],
			"-test.run=^"+name+"$/^server_strength$", "-test.count=1", "-test.v")
		run.Env = append(os.Environ(), serverPasswordCostEnv+"=1")
		out, err := run.CombinedOutput()
		if err != nil {
			t.Fatalf("the fresh copy failed: %v\n%s", err, out)
		}
		if !strings.Contains(string(out), "--- PASS: "+name+"/server_strength") {
			t.Fatalf("the check at the server's own strength did not run:\n%s", out)
		}
	})
}

// strengthOf is the strength hashPassword scrambles [plain] at.
func strengthOf(t *testing.T, plain string) int {
	t.Helper()
	h, err := hashPassword(plain)
	if err != nil {
		t.Fatalf("hashPassword: %v", err)
	}
	cost, err := bcrypt.Cost([]byte(h))
	if err != nil {
		t.Fatalf("not a bcrypt scramble: %q (%v)", h, err)
	}
	return cost
}

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
