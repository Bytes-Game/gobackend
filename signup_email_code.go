package main

// signup_email_code.go — a new account's email is checked before the
// account is made: a 6-digit code goes to the email, and the sign-up only
// goes through with it. Nobody can sign up with an address they cannot
// read, so every account's email is one its owner really has: the one a
// forgotten password's code goes to, and the one a parent's answer is
// tied to.
//
// It is the same POST /signup, twice:
//
//	Without "emailCode": every box is checked, as before. Then, instead of
//	    making the account, a code is emailed.
//	    202 {"emailCodeSent": true, "email", "resendIn": 60}
//	    Sending it again is the same call, at most once a minute.
//	With "emailCode": every box is checked again, then the code. Right: the
//	    account is made, or waits for a parent, as before.
//	    400 {"field": "emailCode", "error", "expired"?: true} — wrong, or no
//	    longer usable.
//
// The code works for 15 minutes and for five tries. Only its bcrypt hash is
// kept. Signing up with Google or Apple asks for no code: they have checked
// the email already.

import (
	"database/sql"
	"log"
	"net/http"
	"time"
)

const (
	signupCodeLife   = 15 * time.Minute
	signupCodeTries  = 5
	signupResendWait = time.Minute
)

// sendSignupCode emails a new code to [email], the address of a sign-up
// that passed every other check, and answers [w]: 202 when it went.
func sendSignupCode(w http.ResponseWriter, email string) {
	// Without email nobody can be checked, so nobody can sign up with an
	// email and a password. Said plainly, here and in the log: the other
	// way, an account made without the check, would look like it worked.
	if !mailConfigured() {
		log.Printf("sign-up: a code was needed to check a new account's email, but email is not set up " +
			"(MAIL_FROM, and RESEND_API_KEY or SMTP_HOST) — nobody can sign up with an email and password")
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"error": "We can't send email right now, so we can't check your email address. " +
				"Please try again later, or continue with Google or Apple.",
			"unavailable": true,
			"reason":      "notSetUp",
		})
		return
	}

	// Not more than once a minute for one address.
	var sentAt time.Time
	err := db.QueryRow(`SELECT sent_at FROM signup_email_codes WHERE email = $1`, email).Scan(&sentAt)
	if err != sql.ErrNoRows && queryFailed("when a sign-up code was last sent to "+maskEmail(email),
		"the sign-up is refused for now", err) {
		http.Error(w, "temporarily unavailable", http.StatusInternalServerError)
		return
	}
	if wait := signupResendWait - time.Since(sentAt); err == nil && wait > 0 {
		writeJSON(w, http.StatusTooManyRequests, map[string]any{
			"field":      "emailCode",
			"error":      "A code was just sent. You can ask for another in a minute.",
			"retryAfter": int(wait.Seconds()) + 1,
		})
		return
	}
	// And only a handful a day, however many sign-ups name the address:
	// this must not be a way to fill a stranger's inbox.
	if !allowAction("signupcode:"+email, "signup_code_email") {
		writeJSON(w, http.StatusTooManyRequests, map[string]any{
			"field": "email",
			"error": "We've sent this address as many codes as we can for now. Try again later.",
		})
		return
	}

	code, err := newResetCode()
	if err != nil {
		log.Printf("sign-up: no random code for %s: %v", maskEmail(email), err)
		http.Error(w, "temporarily unavailable", http.StatusInternalServerError)
		return
	}
	hash, err := hashPassword(code)
	if err != nil {
		log.Printf("sign-up: could not hash the code for %s: %v", maskEmail(email), err)
		http.Error(w, "temporarily unavailable", http.StatusInternalServerError)
		return
	}
	if _, err := db.Exec(`
		INSERT INTO signup_email_codes (email, code_hash, expires_at, attempts, sent_at)
		VALUES ($1, $2, $3, 0, NOW())
		ON CONFLICT (email) DO UPDATE
		   SET code_hash = EXCLUDED.code_hash, expires_at = EXCLUDED.expires_at,
		       attempts = 0, sent_at = EXCLUDED.sent_at`,
		email, hash, time.Now().Add(signupCodeLife)); err != nil {
		log.Printf("sign-up: could not keep the code for %s: %v", maskEmail(email), err)
		http.Error(w, "temporarily unavailable", http.StatusInternalServerError)
		return
	}

	if err := sendMail(email, code+" is your Flexete sign-up code", signupCodeEmail(code)); err != nil {
		log.Printf("sign-up: the code for %s could not be emailed: %v", maskEmail(email), err)
		// A code nobody received must not stop them asking again.
		forgetSignupCode(email)
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"error": "We couldn't send a code to that email. Check the address, " +
				"or try again in a little while.",
			"unavailable": true,
			"reason":      "notSent",
		})
		return
	}
	log.Printf("sign-up: a code was emailed to %s to check it", maskEmail(email))
	writeJSON(w, http.StatusAccepted, map[string]any{
		"emailCodeSent": true,
		"email":         email,
		"resendIn":      int(signupResendWait.Seconds()),
	})
}

// signupCodeEmail is what the code arrives in.
func signupCodeEmail(code string) string {
	return "Hi,\n\n" +
		"Somebody is making a Flexete account with this email address. " +
		"If it's you, your code is:\n\n" +
		"    " + code + "\n\n" +
		"Enter it in the app within 15 minutes. Don't share it with anyone: " +
		"Flexete will never ask you for it.\n\n" +
		"If it wasn't you, you can ignore this email. No account is made " +
		"without the code.\n\n" +
		"The Flexete team\n"
}

// checkSignupCode checks the code a sign-up for [email] came back with.
// Every try is counted before the code is compared, in one statement, so
// tries sent at the same moment cannot share a count and get more than
// five guesses between them.
//
// When the code is not right, it answers [w] itself and returns false.
func checkSignupCode(w http.ResponseWriter, email, code string) bool {
	spent := func(why string) {
		writeJSON(w, http.StatusBadRequest, map[string]any{"field": "emailCode", "error": why, "expired": true})
	}
	var hash string
	var expires time.Time
	var attempts int
	err := db.QueryRow(`
		UPDATE signup_email_codes SET attempts = attempts + 1
		 WHERE email = $1
		RETURNING code_hash, expires_at, attempts`, email).Scan(&hash, &expires, &attempts)
	if err == sql.ErrNoRows {
		spent("That code has expired. Ask for a new one.")
		return false
	}
	if queryFailed("the sign-up code sent to "+maskEmail(email), "the sign-up is refused for now", err) {
		http.Error(w, "temporarily unavailable", http.StatusInternalServerError)
		return false
	}
	if time.Now().After(expires) {
		forgetSignupCode(email)
		spent("That code has expired. Ask for a new one.")
		return false
	}
	if attempts > signupCodeTries {
		forgetSignupCode(email)
		spent("Too many wrong codes. Ask for a new one.")
		return false
	}
	if !checkPassword(hash, code) {
		if attempts >= signupCodeTries {
			forgetSignupCode(email)
			spent("Too many wrong codes. Ask for a new one.")
			return false
		}
		writeFieldError(w, http.StatusBadRequest, refuse("emailCode", "That code isn't right."))
		return false
	}
	return true
}

// purgeExpiredSignupCodes deletes every sign-up code whose 15 minutes are
// up, and with it the address it was sent to: a sign-up nobody finished
// leaves nothing behind. Run by the hourly account job (account_leaving.go),
// and says how many.
func purgeExpiredSignupCodes() int {
	if db == nil {
		return 0
	}
	var n int
	err := db.QueryRow(`
		WITH gone AS (DELETE FROM signup_email_codes WHERE expires_at <= NOW() RETURNING 1)
		SELECT COUNT(*) FROM gone`).Scan(&n)
	if queryFailed("deleting sign-up codes whose 15 minutes are up",
		"they are kept until the next hour", err) {
		return 0
	}
	if n > 0 {
		log.Printf("sign-up: %d code(s) nobody used were deleted, with the addresses they went to", n)
	}
	return n
}

// forgetSignupCode clears [email]'s code: its account is made, or it was
// never sent, or it can no longer be used.
func forgetSignupCode(email string) {
	if _, err := db.Exec(`DELETE FROM signup_email_codes WHERE email = $1`, email); err != nil {
		log.Printf("sign-up: the code for %s could not be cleared: %v", maskEmail(email), err)
	}
}
