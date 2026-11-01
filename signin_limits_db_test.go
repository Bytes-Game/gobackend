package main

// How many tries one internet address gets at the new ways in: the
// two-step code, Google and Apple, and finishing a Google or Apple
// account. Each limit is checked for what it lets through AND for where it
// stops, through the real handlers.
//
// The other tests here clear every limit before each request (signInAs,
// socialPost), so without this file none of these limits would be checked
// at all. And a limit whose name is misspelt lets everything through (an
// unknown name has no limit, action_limits.go), which looks exactly like
// working code.

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// tries sends [body] to [h] from [addr], [n] times, and says the answers.
func tries(h http.HandlerFunc, path, addr, body string, n int) []int {
	codes := make([]int, n)
	for i := range codes {
		req := httptest.NewRequest("POST", path, strings.NewReader(body))
		req.RemoteAddr = addr + ":4000"
		rec := httptest.NewRecorder()
		h(rec, req)
		codes[i] = rec.Code
	}
	return codes
}

func TestSignInLimits_OneAddressCannotTryWithoutEnd(t *testing.T) {
	defer withDB(t)()
	defer resetActionLimiters(t)
	// Google not set up: each try that gets past the limit is told so.
	t.Setenv(googleProvider.audienceEnv, "")

	cases := []struct {
		name    string
		h       http.HandlerFunc
		path    string
		body    string
		allowed int
		answer  int // what each try within the limit gets
	}{
		{"the two-step code", TwoFactorLoginHandler, "/login/2fa",
			`{"ticket":"not-a-ticket","code":"123456"}`, 10, http.StatusUnauthorized},
		{"Google", socialSignIn(googleProvider), "/auth/google",
			`{"idToken":"not-a-token"}`, 20, http.StatusServiceUnavailable},
		{"finishing a Google or Apple account", SocialSignupHandler, "/auth/social/signup",
			`{"signupTicket":"not-a-ticket"}`, 10, http.StatusUnauthorized},
	}
	for _, c := range cases {
		resetActionLimiters(t)
		got := tries(c.h, c.path, "198.51.100.20", c.body, c.allowed+1)
		for i, code := range got[:c.allowed] {
			if code != c.answer {
				t.Fatalf("%s: try %d answered %d, want %d", c.name, i+1, code, c.answer)
			}
		}
		if last := got[c.allowed]; last != http.StatusTooManyRequests {
			t.Errorf("%s: try %d answered %d, want 429 (too many tries)", c.name, c.allowed+1, last)
		}
		if other := tries(c.h, c.path, "198.51.100.21", c.body, 1)[0]; other != c.answer {
			t.Errorf("%s: another address answered %d, want %d", c.name, other, c.answer)
		}
	}

	// Finishing a Google or Apple account takes from the same budget as
	// making one with a password: ten an hour from one address, either way.
	resetActionLimiters(t)
	tries(SocialSignupHandler, "/auth/social/signup", "198.51.100.22", `{"signupTicket":"not-a-ticket"}`, 10)
	if code := tries(SignupHandler, "/signup", "198.51.100.22", `{}`, 1)[0]; code != http.StatusTooManyRequests {
		t.Errorf("after ten Google/Apple sign-ups, a password sign-up answered %d, want 429", code)
	}
}
