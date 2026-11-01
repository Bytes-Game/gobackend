package main

// signup.go — registration, token refresh, and onboarding interests.
//
// Until this file the backend had no way to CREATE an account (login
// worked only for seeded users — a launch blocker), tokens hard-expired
// after 7 days with no refresh path (forcing re-login), and the
// cold-start ranker had no explicit interest signal to work from.
//
// What a new account must have is in account_rules.go.

import (
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/lib/pq"
)

// signupRequest is what the sign-up form sends.
type signupRequest struct {
	Username    string `json:"username"`
	Password    string `json:"password"`
	FullName    string `json:"fullName"`
	Email       string `json:"email"`
	BirthDate   string `json:"birthDate"` // YYYY-MM-DD
	AcceptTerms bool   `json:"acceptTerms"`
}

// newAccount is a sign-up that passed every rule, ready to store.
type newAccount struct {
	username, password, fullName, email string
	birthDate                           time.Time
}

// checkSignup applies every rule in account_rules.go to a sign-up, in the
// order the form shows them, and says which box is wrong first. [now] is
// passed in so tests can fix the day.
func checkSignup(in signupRequest, now time.Time) (newAccount, *fieldError) {
	username := strings.ToLower(strings.TrimSpace(in.Username))
	fullName, why := cleanFullName(in.FullName)
	if why != "" {
		return newAccount{}, refuse("fullName", why)
	}
	if why := usernameProblem(username); why != "" {
		return newAccount{}, refuse("username", why)
	}
	email, why := cleanEmail(in.Email)
	if why != "" {
		return newAccount{}, refuse("email", why)
	}
	born, why := parseBirthDate(in.BirthDate, now)
	if why != "" {
		return newAccount{}, refuse("birthDate", why)
	}
	if why := passwordProblem(in.Password, username); why != "" {
		return newAccount{}, refuse("password", why)
	}
	if !in.AcceptTerms {
		return newAccount{}, refuse("terms", "Please agree to the Terms of Service and the Privacy Policy.")
	}
	if fullName == "" {
		fullName = username
	}
	return newAccount{username: username, password: in.Password, fullName: fullName,
		email: email, birthDate: born}, nil
}

// writeFieldError sends a refusal the form can put under the right box.
func writeFieldError(w http.ResponseWriter, status int, e *fieldError) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(e)
}

// emailTaken is whether an account already uses [email] (already lower
// case). A database that cannot answer says no and logs it: the unique
// index still refuses a second account at the insert.
func emailTaken(email string) bool {
	var taken bool
	err := db.QueryRow(`SELECT EXISTS (SELECT 1 FROM users WHERE lower(email) = $1)`, email).
		Scan(&taken)
	if queryFailed("checking whether an account already uses an email",
		"letting the sign-up try; the database still refuses a second account", err) {
		return false
	}
	return taken
}

// SignupHandler — POST /signup.
//
// Creates the account and signs the person in: the answer has the same
// shape as /login ({user, token, allUsers}), so the app's next steps are
// the same for both. A refusal says which box is wrong ({field, error}).
func SignupHandler(w http.ResponseWriter, r *http.Request) {
	if db == nil {
		http.Error(w, "service unavailable", http.StatusServiceUnavailable)
		return
	}
	var in signupRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 16<<10)).Decode(&in); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// How many accounts may be made. Per internet address, by the hour and
	// by the day, so one machine cannot open hundreds; and for the whole
	// platform, as a ceiling that holds even if the address is faked (see
	// clientIP). This used to be one bucket shared by the whole platform:
	// three sign-ups a minute, for everybody — the fourth person to try at
	// launch would have been turned away.
	ip := clientIP(r)
	if !allowAction("ip:"+ip, "signup") || !allowAction("ip:"+ip, "signup_day") ||
		!allowAction("", "signup_all") {
		log.Printf("sign-up rate limited from %s", ip)
		writeRateLimited(w, "signup")
		return
	}

	acct, bad := checkSignup(in, time.Now())
	if bad != nil {
		writeFieldError(w, http.StatusBadRequest, bad)
		return
	}
	if UserExists(acct.username) {
		writeFieldError(w, http.StatusConflict, refuse("username", "That username is taken."))
		return
	}
	if emailTaken(acct.email) {
		writeFieldError(w, http.StatusConflict,
			refuse("email", "An account already uses this email. Try signing in."))
		return
	}

	hash, err := hashPassword(acct.password)
	if err != nil {
		log.Printf("sign-up: could not hash the password for %q: %v", acct.username, err)
		http.Error(w, "signup failed", http.StatusInternalServerError)
		return
	}
	// The plaintext password column stays '' — bcrypt only, from day one.
	var id string
	err = db.QueryRow(`
		INSERT INTO users (username, password, password_hash, full_name, email,
		                   birth_date, terms_accepted_at, wins, losses, league)
		VALUES ($1, '', $2, $3, $4, $5, NOW(), 0, 0, 'Bronze')
		RETURNING CAST(id AS TEXT)`,
		acct.username, hash, acct.fullName, acct.email, acct.birthDate).Scan(&id)
	if err != nil {
		// Two sign-ups racing for one name or one email land here.
		// Anything else is the server's problem, not the person's, and is
		// said as one: "username taken" for a fault elsewhere would send
		// them off picking name after name that can never work.
		var pqErr *pq.Error
		if errors.As(err, &pqErr) && pqErr.Code == "23505" {
			switch {
			case strings.Contains(pqErr.Constraint, "email"):
				writeFieldError(w, http.StatusConflict,
					refuse("email", "An account already uses this email. Try signing in."))
				return
			case strings.Contains(pqErr.Constraint, "username"):
				writeFieldError(w, http.StatusConflict, refuse("username", "That username is taken."))
				return
			}
		}
		log.Printf("sign-up insert failed for %q: %v", acct.username, err)
		http.Error(w, "signup failed", http.StatusInternalServerError)
		return
	}

	log.Printf("New user signed up: %s (id=%s)", acct.username, id)
	welcomeNewAccount(w, id, acct.username)
}

// welcomeNewAccount signs in an account that was just made, with a
// password or with Google or Apple (social_signin.go): 201 with the
// account and its sign-in, as /login answers.
func welcomeNewAccount(w http.ResponseWriter, id, username string) {
	token, err := issueToken(id, username)
	if err != nil {
		log.Printf("sign-up token issuance failed for %s: %v", username, err)
		http.Error(w, "Signup succeeded but login is temporarily unavailable", http.StatusInternalServerError)
		return
	}

	// A name can have had an owner before: one deleted for good, or one
	// who changed theirs. Nothing left waiting under it was meant for this
	// account (username_change.go).
	if rdb != nil {
		if err := rdb.Del(rctx, storedNotificationsKey(username)).Err(); err != nil {
			log.Printf("sign-up %q: notifications left under the name by its last owner could not be cleared: %v",
				username, err)
		}
	}

	user, _ := GetUserByUsername(username)
	go IndexUser(user) // searchable immediately

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"user":     user,
		"token":    token,
		"allUsers": []User{},
	})
}

// UsernameAvailableHandler — GET /signup/available?username=x.
// Powers the form's live check: whether the name is allowed, whether it is
// free, and if not, why, in words for the person typing.
func UsernameAvailableHandler(w http.ResponseWriter, r *http.Request) {
	name := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("username")))
	why := usernameProblem(name)
	valid := why == ""
	available := valid && db != nil && !UserExists(name)
	if valid && !available {
		why = "That username is taken."
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"valid":     valid,
		"available": available,
		"reason":    why,
	})
}

// usernameForEmail is the username of the account that uses [email], or ""
// when none does. For signing in with an email instead of a username.
func usernameForEmail(email string) string {
	var name string
	err := db.QueryRow(`SELECT username FROM users WHERE lower(email) = lower($1)`,
		strings.TrimSpace(email)).Scan(&name)
	if err == sql.ErrNoRows {
		return ""
	}
	if queryFailed("finding the account for an email at sign-in",
		"treating it as no account; the sign-in fails", err) {
		return ""
	}
	return name
}

// RefreshTokenHandler — POST /api/v1/auth/refresh (authed).
// Mints a fresh 7-day token for the already-verified caller so an
// active user never hits the hard expiry (the client refreshes when
// its stored token is older than ~3 days).
func RefreshTokenHandler(w http.ResponseWriter, r *http.Request) {
	userID, username := authUserID(r), authUsername(r)
	if userID == "" || username == "" {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	// With the name the account has now. A sign-in that somehow outlived a
	// username change would otherwise hand its old name on, refresh after
	// refresh (username_change.go). A database that cannot answer keeps the
	// name the sign-in has: refusing would sign the person out.
	now, err := currentUsername(userID)
	switch {
	case err == sql.ErrNoRows:
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	case err == nil:
		username = now
	default:
		queryFailed("account "+userID+"'s username at a sign-in refresh",
			"the refreshed sign-in keeps the name it had", err)
	}
	token, err := issueToken(userID, username)
	if err != nil {
		http.Error(w, "refresh temporarily unavailable", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"token": token})
}

// SeedInterestsHandler — POST /api/v1/profile/interests
// Body: {"categories": ["comedy", "dance", ...]}
//
// The onboarding interest picker's backend: seeds CategoryAffinity at
// 0.6 for each picked category (raise-only — never lowers an affinity
// real behavior has already earned). This is exactly the signal the
// cold-start ranker lacks for brand-new accounts: their first real
// feed page becomes relevance-ordered instead of pure popularity.
func SeedInterestsHandler(w http.ResponseWriter, r *http.Request) {
	userID := authUserID(r)
	if userID == "" {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	var body struct {
		Categories []string `json:"categories"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || len(body.Categories) == 0 {
		http.Error(w, "categories required", http.StatusBadRequest)
		return
	}

	// Validate against the canonical vocabulary; ignore unknowns.
	valid := make(map[string]bool, len(ContentCategories))
	for _, c := range ContentCategories {
		valid[c] = true
	}
	picked := make([]string, 0, 10)
	for _, c := range body.Categories {
		c = strings.ToLower(strings.TrimSpace(c))
		if valid[c] && len(picked) < 10 {
			picked = append(picked, c)
		}
	}
	if len(picked) == 0 {
		http.Error(w, "no valid categories", http.StatusBadRequest)
		return
	}

	unlock := profileKeyLocks.lock(userID)
	defer unlock()
	profile, _ := loadUserProfile(userID)
	if profile == nil {
		profile = &UserProfile{UserID: userID, CategoryAffinity: map[string]float64{}}
	}
	if profile.CategoryAffinity == nil {
		profile.CategoryAffinity = map[string]float64{}
	}
	const seedAffinity = 0.6
	for _, c := range picked {
		if profile.CategoryAffinity[c] < seedAffinity {
			profile.CategoryAffinity[c] = seedAffinity
		}
	}
	saveUserProfile(profile)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"status": "ok", "seeded": picked})
}
