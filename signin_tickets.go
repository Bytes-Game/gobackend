package main

// signin_tickets.go — proof that one step of signing in is done, so the
// next step can finish it.
//
// Signing in can take more than one request:
//
//   - the password (or Google, or Apple) was right, but the account has
//     two-step verification on, so the code is still needed;
//   - everything was right, but the account is waiting to be deleted, and
//     the person still has to choose to keep it.
//
// Between those requests the server keeps nothing. It hands back a ticket
// instead: a short signed note saying who, which step is done, and when.
// The next request brings it back.
//
// A ticket is NOT a sign-in. It is signed with a different key from the
// session tokens (auth.go), so the server never mistakes one for the other,
// in either direction. It lasts five minutes, and a password change since
// it was made cancels it, the same as it signs out every session.
//
// Unlike a session, a ticket still works while the account waits to be
// deleted: that is when it is needed, to reach the question "keep it?".

import (
	"crypto/hmac"
	"crypto/sha256"
	"errors"
	"time"

	jwt "github.com/golang-jwt/jwt/v5"
)

// What a ticket says is done.
const (
	// The password (or Google/Apple) was right; the two-step code is next.
	stageSecondStep = "second-step"
	// Everything was checked; only the choice to keep an account that is
	// waiting to be deleted is left.
	stageVerified = "verified"
)

// ticketTTL is how long a ticket works: time to open an authenticator app
// and type a code, not long enough to be worth stealing.
const ticketTTL = 5 * time.Minute

const ticketAudience = "flex-sign-in-ticket"

type ticketClaims struct {
	Stage string `json:"stage"`
	jwt.RegisteredClaims
}

// ticketKey is the key tickets are signed with: derived from JWT_SECRET,
// but not equal to it, so a ticket can never pass for a session token.
func ticketKey() ([]byte, error) {
	secret, err := authSecret()
	if err != nil {
		return nil, err
	}
	m := hmac.New(sha256.New, secret)
	m.Write([]byte("flex sign-in ticket"))
	return m.Sum(nil), nil
}

// issueTicket says that [stage] is done for account [userID].
func issueTicket(userID, stage string) (string, error) {
	key, err := ticketKey()
	if err != nil {
		return "", err
	}
	now := time.Now()
	claims := ticketClaims{
		Stage: stage,
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   userID,
			Audience:  jwt.ClaimStrings{ticketAudience},
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(ticketTTL)),
		},
	}
	return jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(key)
}

var errTicketSpent = errors.New("the ticket ran out, or the password changed since it was made")

// readTicket checks a ticket and says whose it is and which step is done.
func readTicket(tok string) (userID, stage string, err error) {
	key, err := ticketKey()
	if err != nil {
		return "", "", err
	}
	claims := &ticketClaims{}
	_, err = jwt.ParseWithClaims(tok, claims, func(t *jwt.Token) (interface{}, error) {
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, errors.New("unexpected signing method")
		}
		return key, nil
	}, jwt.WithAudience(ticketAudience), jwt.WithExpirationRequired())
	if err != nil {
		return "", "", errTicketSpent
	}
	if claims.Subject == "" || (claims.Stage != stageSecondStep && claims.Stage != stageVerified) {
		return "", "", errTicketSpent
	}
	issued := time.Time{}
	if claims.IssuedAt != nil {
		issued = claims.IssuedAt.Time
	}
	// From before the password last changed, or the account was signed out
	// everywhere: spent. Not signedInBeforeCutoff, which also refuses every
	// sign-in of an account waiting to be deleted — a ticket is exactly how
	// such an account, with two-step verification on, gets far enough to be
	// asked whether to keep it.
	if cut, _ := tokenCutoff(claims.Subject); !cut.IsZero() && issued.Before(cut) {
		return "", "", errTicketSpent
	}
	return claims.Subject, claims.Stage, nil
}
