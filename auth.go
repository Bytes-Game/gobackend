package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	jwt "github.com/golang-jwt/jwt/v5"
)

// ════════════════════════════════════════════════════════════════════════════════
// SESSION AUTH — stateless HS256 bearer tokens
// ════════════════════════════════════════════════════════════════════════════════
//
// Replaces the old "trust the userId in the request body/query" model. On login
// the server mints a signed JWT carrying the user's id (sub) and username. Every
// mutating or per-user endpoint is wrapped in authed(), which:
//
//   1. pulls the bearer token out of the Authorization header,
//   2. verifies the HS256 signature + expiry against JWT_SECRET,
//   3. stashes the *trusted* user id in the request context.
//
// Handlers then call authUserID(r) instead of reading a client-supplied id, so a
// caller can never act on behalf of another user by lying in the payload.
//
// Stateless by design: HMAC verification is deterministic, so every Render replica
// validates the same token with zero shared state. (Redis is available if we ever
// want a revocation list — see redis.go.) Tokens are short-lived to bound the blast
// radius of a leak in lieu of server-side revocation.

// tokenTTL is how long an issued session token stays valid. Kept short-ish because
// we have no revocation list yet; the client transparently re-logs in on a 401.
const tokenTTL = 7 * 24 * time.Hour

// authClaims is the JWT payload. Subject carries the canonical user id (the same
// stringified SERIAL the rest of the code compares on); Username is carried so the
// WebSocket handler can authorize the /ws/{username} path without a DB round-trip.
type authClaims struct {
	Username string `json:"username,omitempty"`
	jwt.RegisteredClaims
}

// ctxKey is an unexported type so our context key can never collide with a key set
// by another package (the std-lib-recommended pattern).
type ctxKey string

const (
	userIDContextKey   ctxKey = "authUserID"
	usernameContextKey ctxKey = "authUsername"
)

// authSecret returns the signing key, or an error if JWT_SECRET is unset. We read
// it per-call (a cheap map lookup) so tests can set it in TestMain and so a missing
// secret fails closed — login and every protected route error rather than silently
// signing/accepting tokens with an empty key.
func authSecret() ([]byte, error) {
	s := os.Getenv("JWT_SECRET")
	if s == "" {
		return nil, errors.New("JWT_SECRET not configured")
	}
	return []byte(s), nil
}

// checkAuthConfig logs a loud warning at startup if JWT_SECRET is missing, so an
// operator notices before the first login 500s instead of after. We don't os.Exit
// here (unlike DATABASE_URL) so local tooling that never logs in can still boot.
func checkAuthConfig() {
	if _, err := authSecret(); err != nil {
		log.Println("WARNING: JWT_SECRET is not set — login and all authenticated " +
			"routes will fail with 500/401 until it is configured.")
	}
}

// issueToken mints a signed session token for the given user.
func issueToken(userID, username string) (string, error) {
	secret, err := authSecret()
	if err != nil {
		return "", err
	}
	now := time.Now()
	claims := authClaims{
		Username: username,
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   userID,
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(tokenTTL)),
		},
	}
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return tok.SignedString(secret)
}

// parseToken verifies a token string and returns its claims. It rejects any token
// not signed with HMAC (defends against the classic alg=none / RS256-confusion
// downgrade) and any token missing a subject.
func parseToken(tokenStr string) (*authClaims, error) {
	secret, err := authSecret()
	if err != nil {
		return nil, err
	}
	claims := &authClaims{}
	_, err = jwt.ParseWithClaims(tokenStr, claims, func(t *jwt.Token) (interface{}, error) {
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, errors.New("unexpected signing method")
		}
		return secret, nil
	})
	if err != nil {
		return nil, err
	}
	if claims.Subject == "" {
		return nil, errors.New("token missing subject")
	}
	return claims, nil
}

// bearerToken extracts the raw token from an "Authorization: Bearer <token>"
// header, or "" if absent/malformed.
func bearerToken(r *http.Request) string {
	h := r.Header.Get("Authorization")
	if h == "" {
		return ""
	}
	parts := strings.SplitN(h, " ", 2)
	if len(parts) == 2 && strings.EqualFold(parts[0], "Bearer") {
		return strings.TrimSpace(parts[1])
	}
	return ""
}

// authed wraps a handler so it only runs for a request bearing a valid session
// token. The trusted user id is injected into the request context; handlers read
// it via authUserID(r). CORS preflights never reach here (corsMiddleware answers
// OPTIONS before routing), so no OPTIONS bypass is needed.
func authed(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		tok := bearerToken(r)
		if tok == "" {
			http.Error(w, "missing bearer token", http.StatusUnauthorized)
			return
		}
		claims, err := parseToken(tok)
		if err != nil {
			http.Error(w, "invalid or expired token", http.StatusUnauthorized)
			return
		}
		ctx := context.WithValue(r.Context(), userIDContextKey, claims.Subject)
		ctx = context.WithValue(ctx, usernameContextKey, claims.Username)
		h(w, r.WithContext(ctx))
	}
}

// authUserID returns the trusted user id established by authed(), or "" if the
// handler was (mistakenly) not wrapped. Handlers on authed routes can rely on it
// being non-empty.
func authUserID(r *http.Request) string {
	v, _ := r.Context().Value(userIDContextKey).(string)
	return v
}

// authUsername returns the trusted username from the token, or "" if absent.
// Used where a display name is persisted (comments, chat) so a client can't
// attach someone else's handle to its content.
func authUsername(r *http.Request) string {
	v, _ := r.Context().Value(usernameContextKey).(string)
	return v
}

// requirePathUser is a guard for path-scoped resources (/users/{id}/...,
// /chat/conversations/{userId}, /saved/{userId}). It returns the trusted user id
// and writes a 403 (returning ok=false) if the path segment names a different
// user than the token's subject. Callers should return immediately when ok=false.
func requirePathUser(w http.ResponseWriter, r *http.Request, pathUserID string) (string, bool) {
	uid := authUserID(r)
	if pathUserID != "" && pathUserID != uid {
		http.Error(w, "forbidden", http.StatusForbidden)
		return "", false
	}
	return uid, true
}
