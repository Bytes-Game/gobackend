package main

// two_step_signin.go — the two-step code at sign-in.
//
// Two-step verification could be turned on (profile_handlers.go) but
// signing in never asked for the code, so it protected nothing. Now:
//
//  1. The password is right and the account has two-step verification on:
//     no sign-in yet. The answer is a ticket (signin_tickets.go) and
//     "enter the code". Nothing else is said, not even whether the account
//     is waiting to be deleted: the password alone is not enough to learn
//     anything more.
//  2. POST /login/2fa brings the ticket back with a code from the
//     authenticator app, or one of the recovery codes. Right: signed in,
//     the same answer a sign-in always gives.
//
// Each authenticator code works once (its time step is remembered), and
// each recovery code works once (crossed off in the same statement that
// checks it). Tries are limited per account and per address.
//
// Signing in with Google or Apple goes through the same step
// (social_signin.go).

import (
	"database/sql"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"strings"
	"time"
)

// twoStepOn says whether account [userID] has two-step verification on.
func twoStepOn(userID string) (bool, error) {
	var on bool
	err := db.QueryRow(`SELECT EXISTS (SELECT 1 FROM user_totp WHERE user_id = $1 AND is_active = TRUE)`,
		userID).Scan(&on)
	return on, err
}

// acceptTOTP takes authenticator code [code] for account [userID], once: a
// code from a time step no later than the last one used is refused.
func acceptTOTP(userID, secret, code string) (bool, error) {
	step, ok := matchTOTPStep(secret, code, time.Now())
	if !ok {
		return false, nil
	}
	res, err := db.Exec(`UPDATE user_totp SET last_used_step = $2, last_used_at = NOW()
		WHERE user_id = $1 AND (last_used_step IS NULL OR last_used_step < $2)`, userID, step)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n == 1, err
}

// useRecoveryCode takes one of account [userID]'s recovery codes, once.
// Checking it and crossing it off are one statement, so two sign-ins
// racing with the same code cannot both get in.
func useRecoveryCode(userID, code string) (bool, error) {
	res, err := db.Exec(`UPDATE user_totp SET recovery_codes = recovery_codes - $2::text
		WHERE user_id = $1 AND recovery_codes ? $2::text`, userID, hashRecoveryCode(code))
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n == 1, err
}

// checkCode takes a second-step code for account [userID]: six digits from
// the authenticator app, or a recovery code.
func checkCode(userID, secret, code string) (bool, error) {
	code = strings.TrimSpace(code)
	digits := strings.ReplaceAll(code, " ", "")
	if len(digits) == totpDigits && strings.Trim(digits, "0123456789") == "" {
		return acceptTOTP(userID, secret, digits)
	}
	if code == "" {
		return false, nil
	}
	return useRecoveryCode(userID, code)
}

// secondStep checks [code] for account [userID]. An account that turned
// two-step verification off since the ticket was made needs no code.
func secondStep(userID, code string) (bool, error) {
	var secret string
	err := db.QueryRow(`SELECT secret FROM user_totp WHERE user_id = $1 AND is_active = TRUE`, userID).Scan(&secret)
	if err == sql.ErrNoRows {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	return checkCode(userID, secret, code)
}

// askForSecondStep answers a right password (or Google/Apple) for an
// account with two-step verification on: not signed in yet, here is the
// ticket, enter the code.
func askForSecondStep(w http.ResponseWriter, user User) {
	ticket, err := issueTicket(user.ID, stageSecondStep)
	if err != nil {
		log.Printf("two-step sign-in: no ticket could be made for %s: %v", user.Username, err)
		http.Error(w, "Login temporarily unavailable", http.StatusInternalServerError)
		return
	}
	log.Printf("two-step sign-in: %s got the password right; asked for the code", user.Username)
	writeJSON(w, http.StatusUnauthorized, map[string]any{
		"twoFactorRequired": true,
		"ticket":            ticket,
		"error":             "Enter the 6-digit code from your authenticator app.",
	})
}

// completeSignIn finishes signing in once every check has passed: an
// account waiting to be deleted is asked about first (account_leaving.go),
// then the session token.
//
// The question comes with a ticket that says every check is done, so the
// answer ("keep it") needs nothing typed again: not the password, not a
// code that has been used already.
func completeSignIn(w http.ResponseWriter, r *http.Request, user User, keep bool) {
	if deleteOn, waiting := deletionDate(user.ID); waiting {
		if !keep {
			ticket, err := issueTicket(user.ID, stageVerified)
			if err != nil {
				log.Printf("sign-in: no ticket could be made for %s: %v", user.Username, err)
				http.Error(w, "Login temporarily unavailable", http.StatusInternalServerError)
				return
			}
			writeJSON(w, http.StatusConflict, map[string]interface{}{
				"error": "This account will be deleted on " +
					deleteOn.UTC().Format("2 January 2006") + ". Keep it to sign in.",
				"deletionPending": true,
				"deleteOn":        deleteOn.UTC().Format(time.RFC3339),
				"ticket":          ticket,
			})
			log.Printf("user %s signed in while waiting to be deleted; asked whether to keep it", user.Username)
			return
		}
		if err := keepAccount(r.Context(), user.ID); err != nil {
			log.Printf("user %s chose to keep their account, but that could not be saved: %v", user.Username, err)
			http.Error(w, "Login temporarily unavailable", http.StatusInternalServerError)
			return
		}
	}

	// Mint a signed session token. From here on the client authenticates every
	// protected request with this token (Authorization: Bearer <token>) and the
	// server derives identity from it — never from a client-supplied userId.
	token, err := issueToken(user.ID, user.Username)
	if err != nil {
		// Almost always means JWT_SECRET is unset — fail closed rather than hand
		// back a session the protected routes will reject anyway.
		log.Printf("token issuance failed for %s: %v", user.Username, err)
		http.Error(w, "Login temporarily unavailable", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"user":     user,
		"token":    token,
		"allUsers": []User{},
	})
	log.Printf("User %s logged in successfully.", user.Username)
}

// TwoFactorLoginHandler — POST /login/2fa {"ticket", "code", "keepAccount"}.
//
//	200 {user, token, allUsers} — signed in.
//	401 {"field": "code", "error"} — that code isn't right.
//	401 {"error", "expired": true} — the ticket ran out: sign in again.
//	409 {"error", "deletionPending", "deleteOn", "ticket"} — waiting to be
//	    deleted; the ticket back with keepAccount keeps it.
//	429 — too many tries.
func TwoFactorLoginHandler(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Ticket      string `json:"ticket"`
		Code        string `json:"code"`
		KeepAccount bool   `json:"keepAccount"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 4<<10)).Decode(&body); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}
	if !allowAction("ip:"+clientIP(r), "login_2fa_ip") {
		writeRateLimited(w, "login_2fa_ip")
		return
	}
	spent := func() {
		writeJSON(w, http.StatusUnauthorized, map[string]any{
			"error":   "That took too long. Sign in again.",
			"expired": true,
		})
	}
	userID, stage, err := readTicket(strings.TrimSpace(body.Ticket))
	if err != nil {
		spent()
		return
	}
	// Anything but "verified" needs the code. readTicket lets through only
	// the two kinds there are, but if that ever changed, an unknown kind of
	// ticket must not skip the code.
	if stage != stageVerified {
		if !allowAction("user:"+userID, "login_2fa") {
			log.Printf("two-step sign-in: too many codes tried for account %s", userID)
			writeRateLimited(w, "login_2fa")
			return
		}
		ok, err := secondStep(userID, body.Code)
		if queryFailed("whether a two-step code is right for account "+userID, "the sign-in is refused", err) {
			http.Error(w, "Login temporarily unavailable", http.StatusInternalServerError)
			return
		}
		if !ok {
			log.Printf("two-step sign-in: a wrong code for account %s", userID)
			writeFieldError(w, http.StatusUnauthorized,
				refuse("code", "That code isn't right. Check your authenticator app and try again."))
			return
		}
	}
	user, exists := GetUserByID(userID)
	if !exists {
		spent()
		return
	}
	completeSignIn(w, r, user, body.KeepAccount)
}
