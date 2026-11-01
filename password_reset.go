package main

// password_reset.go — "Forgot password?", the way TikTok does it: a 6-digit
// code sent to the account's email, then a new password.
//
//	POST /password/forgot {"login": username or email}
//	POST /password/reset  {"login", "code", "newPassword"}
//
// The code works for 15 minutes and for five tries. Asking again replaces
// it, at most once a minute. Only its bcrypt hash is kept.
//
// A new password signs out every sign-in, as changing it does. The reset
// itself signs nobody in: the app then signs in with the new password
// through the normal sign-in, so whatever that asks — keeping an account
// that is waiting to be deleted, for one — still applies.
//
// Usernames are public, so a username is answered plainly: there is no such
// account, or the code went to k•••n@gmail.com. An email address is never
// confirmed or denied: the answer is the same whether or not an account uses
// it.

import (
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/big"
	"net/http"
	"strings"
	"time"
)

const (
	resetCodeLife   = 15 * time.Minute
	resetCodeTries  = 5
	resetResendWait = time.Minute
)

// resetTarget is the account a forgotten password belongs to.
type resetTarget struct{ id, username, email string }

// resetAccountFor is the account [login] names: by email, or by username
// whatever its capitals. found is false when none does.
func resetAccountFor(login string) (t resetTarget, found bool, err error) {
	if strings.Contains(login, "@") {
		err = db.QueryRow(`SELECT id::text, username, COALESCE(email, '') FROM users
		                    WHERE lower(email) = lower($1)`, login).Scan(&t.id, &t.username, &t.email)
	} else {
		name := storedUsername(login)
		if name == "" {
			name = login
		}
		err = db.QueryRow(`SELECT id::text, username, COALESCE(email, '') FROM users
		                    WHERE username = $1`, name).Scan(&t.id, &t.username, &t.email)
	}
	if err == sql.ErrNoRows {
		return t, false, nil
	}
	return t, err == nil, err
}

// newResetCode is six random digits.
func newResetCode() (string, error) {
	n, err := rand.Int(rand.Reader, big.NewInt(1_000_000))
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%06d", n.Int64()), nil
}

// ForgotPasswordHandler — POST /password/forgot {"login"}.
//
//	200 {"sent": true, "to"?: "k•••n@gmail.com", "resendIn": 60}
//	    "to" only for a username: an email is never confirmed.
//	404 {"field": "login", "error"} — no account has that username.
//	409 {"field": "login", "error", "noEmail": true} — the account has no email.
//	429 — too many requests, or a code was sent less than a minute ago.
//	503 {"error", "unavailable": true} — email is not set up, or would not send.
func ForgotPasswordHandler(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Login string `json:"login"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 4<<10)).Decode(&body); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}
	login := strings.TrimSpace(body.Login)
	if login == "" {
		writeFieldError(w, http.StatusBadRequest, refuse("login", "Enter your username or email."))
		return
	}
	byEmail := strings.Contains(login, "@")
	if !allowAction("ip:"+clientIP(r), "password_reset_ip") {
		writeRateLimited(w, "password_reset_ip")
		return
	}
	unavailable := func() {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"error": "Resetting a password by email isn't working right now. Email " +
				supportEmail + " and we'll help you get back in.",
			"unavailable": true,
		})
	}
	// Asked before the account is looked up, so the answer says nothing
	// about whether an email address has an account.
	if !mailConfigured() {
		log.Printf("forgot password: asked for, but email is not set up (SMTP_HOST and MAIL_FROM)")
		unavailable()
		return
	}
	sentQuietly := func() {
		writeJSON(w, http.StatusOK, map[string]any{"sent": true, "resendIn": int(resetResendWait.Seconds())})
	}

	t, found, err := resetAccountFor(login)
	if queryFailed("which account a forgotten password belongs to", "the request is refused", err) {
		http.Error(w, "temporarily unavailable", http.StatusInternalServerError)
		return
	}
	if !found {
		if byEmail {
			sentQuietly()
			return
		}
		writeFieldError(w, http.StatusNotFound, refuse("login", "No account has that username."))
		return
	}
	if t.email == "" {
		writeJSON(w, http.StatusConflict, map[string]any{
			"field": "login",
			"error": "This account has no email address, so we can't send it a code. Email " +
				supportEmail + " from your email address and tell us your username.",
			"noEmail": true,
		})
		return
	}

	// Not more than once a minute, nor five an hour, for one account: nobody's
	// inbox can be filled with codes. An email address gets the same answer
	// as ever, so even this says nothing about whether it has an account.
	var sentAt time.Time
	err = db.QueryRow(`SELECT sent_at FROM password_reset_codes WHERE user_id::text = $1`, t.id).Scan(&sentAt)
	if err != sql.ErrNoRows && queryFailed("when account "+t.id+"'s last reset code was sent",
		"the request is refused", err) {
		http.Error(w, "temporarily unavailable", http.StatusInternalServerError)
		return
	}
	if wait := resetResendWait - time.Since(sentAt); err == nil && wait > 0 {
		if byEmail {
			sentQuietly()
			return
		}
		writeJSON(w, http.StatusTooManyRequests, map[string]any{
			"error":      "A code was just sent. You can ask for another in a minute.",
			"retryAfter": int(wait.Seconds()) + 1,
		})
		return
	}
	if !allowAction("user:"+t.id, "password_reset") {
		if byEmail {
			sentQuietly()
			return
		}
		writeRateLimited(w, "password_reset")
		return
	}

	code, err := newResetCode()
	if err != nil {
		log.Printf("forgot password: no random code for account %s: %v", t.id, err)
		http.Error(w, "temporarily unavailable", http.StatusInternalServerError)
		return
	}
	hash, err := hashPassword(code)
	if err != nil {
		log.Printf("forgot password: could not hash the code for account %s: %v", t.id, err)
		http.Error(w, "temporarily unavailable", http.StatusInternalServerError)
		return
	}
	if _, err := db.Exec(`
		INSERT INTO password_reset_codes (user_id, code_hash, expires_at, attempts, sent_at)
		VALUES ($1::int, $2, $3, 0, NOW())
		ON CONFLICT (user_id) DO UPDATE
		   SET code_hash = EXCLUDED.code_hash, expires_at = EXCLUDED.expires_at,
		       attempts = 0, sent_at = EXCLUDED.sent_at`,
		t.id, hash, time.Now().Add(resetCodeLife)); err != nil {
		log.Printf("forgot password: could not keep the code for account %s: %v", t.id, err)
		http.Error(w, "temporarily unavailable", http.StatusInternalServerError)
		return
	}

	if err := sendMail(t.email, code+" is your Flex password reset code", resetEmail(t.username, code)); err != nil {
		log.Printf("forgot password: the code for account %s could not be emailed: %v", t.id, err)
		// A code nobody received must not stop them asking again.
		if _, err := db.Exec(`DELETE FROM password_reset_codes WHERE user_id::text = $1`, t.id); err != nil {
			log.Printf("forgot password: and the unsent code for account %s could not be cleared: %v", t.id, err)
		}
		unavailable()
		return
	}
	log.Printf("forgot password: a reset code was emailed to account %s", t.id)
	if byEmail {
		sentQuietly()
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"sent":     true,
		"to":       maskEmail(t.email),
		"resendIn": int(resetResendWait.Seconds()),
	})
}

// resetEmail is what the code arrives in.
func resetEmail(username, code string) string {
	return "Hi @" + username + ",\n\n" +
		"We got a request to reset the password for your Flex account. Your code is:\n\n" +
		"    " + code + "\n\n" +
		"Enter it in the app within 15 minutes. Don't share it with anyone: " +
		"Flex will never ask you for it.\n\n" +
		"If you didn't ask for this, you can ignore this email. Your password " +
		"stays as it is.\n\n" +
		"The Flex team\n"
}

// ResetPasswordHandler — POST /password/reset {"login", "code", "newPassword"}.
//
//	200 {"reset": true, "username"} — changed. Every sign-in is signed out;
//	    the app signs in again with the new password.
//	400 {"field": "code", "error", "expired"?: true} — wrong, expired or spent.
//	400 {"field": "newPassword", "error"} — the code was right; the password
//	    is not allowed. The code still works, to fix it and try again.
func ResetPasswordHandler(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Login       string `json:"login"`
		Code        string `json:"code"`
		NewPassword string `json:"newPassword"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 4<<10)).Decode(&body); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}
	if !allowAction("ip:"+clientIP(r), "password_reset_try") {
		writeRateLimited(w, "password_reset_try")
		return
	}
	login, code := strings.TrimSpace(body.Login), strings.TrimSpace(body.Code)
	wrong := func() {
		writeFieldError(w, http.StatusBadRequest, refuse("code", "That code isn't right."))
	}
	spent := func(why string) {
		writeJSON(w, http.StatusBadRequest, map[string]any{"field": "code", "error": why, "expired": true})
	}

	t, found, err := resetAccountFor(login)
	if queryFailed("which account a password reset belongs to", "the reset is refused", err) {
		http.Error(w, "temporarily unavailable", http.StatusInternalServerError)
		return
	}
	if !found {
		wrong()
		return
	}
	var hash string
	var expires time.Time
	var attempts int
	err = db.QueryRow(`SELECT code_hash, expires_at, attempts FROM password_reset_codes
	                    WHERE user_id::text = $1`, t.id).Scan(&hash, &expires, &attempts)
	if err == sql.ErrNoRows {
		spent("That code has expired. Ask for a new one.")
		return
	}
	if queryFailed("account "+t.id+"'s reset code", "the reset is refused", err) {
		http.Error(w, "temporarily unavailable", http.StatusInternalServerError)
		return
	}
	forget := func() {
		if _, err := db.Exec(`DELETE FROM password_reset_codes WHERE user_id::text = $1`, t.id); err != nil {
			log.Printf("password reset: account %s's spent code could not be cleared: %v", t.id, err)
		}
	}
	if time.Now().After(expires) {
		forget()
		spent("That code has expired. Ask for a new one.")
		return
	}
	if !checkPassword(hash, code) {
		if attempts+1 >= resetCodeTries {
			forget()
			spent("Too many wrong codes. Ask for a new one.")
			return
		}
		if _, err := db.Exec(`UPDATE password_reset_codes SET attempts = attempts + 1
		                       WHERE user_id::text = $1`, t.id); err != nil {
			log.Printf("password reset: a wrong try at account %s's code could not be counted: %v", t.id, err)
		}
		wrong()
		return
	}
	if why := passwordProblem(body.NewPassword, t.username); why != "" {
		writeFieldError(w, http.StatusBadRequest, refuse("newPassword", why))
		return
	}
	newHash, err := hashPassword(body.NewPassword)
	if err != nil {
		log.Printf("password reset: could not hash the new password for account %s: %v", t.id, err)
		http.Error(w, "temporarily unavailable", http.StatusInternalServerError)
		return
	}

	// The new password and the end of the code, together or not at all. The
	// moment is to the second, as a password change stamps it.
	tx, err := db.Begin()
	if err != nil {
		log.Printf("password reset: account %s: %v", t.id, err)
		http.Error(w, "temporarily unavailable", http.StatusInternalServerError)
		return
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.Exec(`UPDATE users SET password_hash = $1, password = '', password_changed_at = $2
	                       WHERE id::text = $3`, newHash, time.Now().Truncate(time.Second), t.id); err != nil {
		log.Printf("password reset: account %s's new password could not be saved: %v", t.id, err)
		http.Error(w, "temporarily unavailable", http.StatusInternalServerError)
		return
	}
	if _, err := tx.Exec(`DELETE FROM password_reset_codes WHERE user_id::text = $1`, t.id); err != nil {
		log.Printf("password reset: account %s's used code could not be cleared: %v", t.id, err)
		http.Error(w, "temporarily unavailable", http.StatusInternalServerError)
		return
	}
	if err := tx.Commit(); err != nil {
		log.Printf("password reset: account %s: %v", t.id, err)
		http.Error(w, "temporarily unavailable", http.StatusInternalServerError)
		return
	}
	forgetCutoff(t.id)
	log.Printf("account %s reset its password with an emailed code; every sign-in is signed out", t.id)
	writeJSON(w, http.StatusOK, map[string]any{"reset": true, "username": t.username})
}
