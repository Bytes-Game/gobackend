package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	jwt "github.com/golang-jwt/jwt/v5"
)

// issueToken → parseToken round-trips the subject and username.
func TestToken_RoundTrip(t *testing.T) {
	tok, err := issueToken("u42", "alice")
	if err != nil {
		t.Fatalf("issueToken: %v", err)
	}
	claims, err := parseToken(tok)
	if err != nil {
		t.Fatalf("parseToken: %v", err)
	}
	if claims.Subject != "u42" {
		t.Errorf("subject = %q, want u42", claims.Subject)
	}
	if claims.Username != "alice" {
		t.Errorf("username = %q, want alice", claims.Username)
	}
}

// A token signed with a different key must be rejected.
func TestToken_RejectsWrongSignature(t *testing.T) {
	claims := authClaims{RegisteredClaims: jwt.RegisteredClaims{
		Subject:   "u1",
		ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
	}}
	forged, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString([]byte("attacker-key"))
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	if _, err := parseToken(forged); err == nil {
		t.Error("expected wrong-signature token to be rejected")
	}
}

// The classic alg=none downgrade must be rejected.
func TestToken_RejectsAlgNone(t *testing.T) {
	claims := authClaims{RegisteredClaims: jwt.RegisteredClaims{
		Subject:   "u1",
		ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
	}}
	unsigned, err := jwt.NewWithClaims(jwt.SigningMethodNone, claims).SignedString(jwt.UnsafeAllowNoneSignatureType)
	if err != nil {
		t.Fatalf("sign none: %v", err)
	}
	if _, err := parseToken(unsigned); err == nil {
		t.Error("expected alg=none token to be rejected")
	}
}

// An expired token must be rejected.
func TestToken_RejectsExpired(t *testing.T) {
	secret, _ := authSecret()
	claims := authClaims{RegisteredClaims: jwt.RegisteredClaims{
		Subject:   "u1",
		ExpiresAt: jwt.NewNumericDate(time.Now().Add(-time.Minute)),
	}}
	expired, _ := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(secret)
	if _, err := parseToken(expired); err == nil {
		t.Error("expected expired token to be rejected")
	}
}

func TestBearerToken_Parsing(t *testing.T) {
	cases := map[string]string{
		"Bearer abc":  "abc",
		"bearer abc":  "abc", // case-insensitive scheme
		"Bearer  abc": "abc", // trims
		"abc":         "",    // no scheme
		"Basic abc":   "",    // wrong scheme
		"":            "",
	}
	for header, want := range cases {
		req := httptest.NewRequest("GET", "/", nil)
		if header != "" {
			req.Header.Set("Authorization", header)
		}
		if got := bearerToken(req); got != want {
			t.Errorf("bearerToken(%q) = %q, want %q", header, got, want)
		}
	}
}

// authed() rejects a request with no token and passes the trusted id through to
// the wrapped handler when the token is valid.
func TestAuthed_RejectsAndInjects(t *testing.T) {
	var seen string
	h := authed(func(w http.ResponseWriter, r *http.Request) {
		seen = authUserID(r)
		w.WriteHeader(http.StatusOK)
	})

	// No token → 401, handler never runs.
	w := httptest.NewRecorder()
	h(w, httptest.NewRequest("POST", "/x", nil))
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("no token: got %d, want 401", w.Code)
	}
	if seen != "" {
		t.Fatalf("handler ran despite missing token (saw %q)", seen)
	}

	// Valid token → handler runs with the trusted id from the token.
	tok, _ := issueToken("u_trusted", "bob")
	req := httptest.NewRequest("POST", "/x", nil)
	req.Header.Set("Authorization", "Bearer "+tok)
	w = httptest.NewRecorder()
	h(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("valid token: got %d, want 200", w.Code)
	}
	if seen != "u_trusted" {
		t.Errorf("authUserID = %q, want u_trusted", seen)
	}
}

// The core authorization guarantee: when a request carries a spoofed body
// userId, the trusted identity a handler reads is the token's subject — the
// body is irrelevant. (DB-free: we assert on authUserID directly.)
func TestAuthed_IgnoresSpoofedBodyUserID(t *testing.T) {
	tok, _ := issueToken("real_user", "real")
	body := strings.NewReader(`{"challengeId":"ch1","userId":"victim"}`)
	req := httptest.NewRequest("POST", "/api/v1/challenges/like", body)
	req.Header.Set("Authorization", "Bearer "+tok)
	w := httptest.NewRecorder()

	var trusted string
	authed(func(w http.ResponseWriter, r *http.Request) {
		// A handler that wrongly trusted the body would see "victim".
		trusted = authUserID(r)
		w.WriteHeader(http.StatusOK)
	})(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("valid token rejected: %d", w.Code)
	}
	if trusted != "real_user" {
		t.Errorf("trusted identity = %q, want real_user (body said 'victim')", trusted)
	}
}
