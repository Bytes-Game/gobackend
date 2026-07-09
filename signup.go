package main

// signup.go — registration, token refresh, and onboarding interests.
//
// Until this file the backend had no way to CREATE an account (login
// worked only for seeded users — a launch blocker), tokens hard-expired
// after 7 days with no refresh path (forcing re-login), and the
// cold-start ranker had no explicit interest signal to work from.

import (
	"encoding/json"
	"log"
	"net/http"
	"regexp"
	"strings"
)

var usernameRe = regexp.MustCompile(`^[a-z0-9_.]{3,20}$`)

// SignupHandler — POST /signup {username, password}.
// Mirrors LoginHandler's response shape ({user, token, allUsers:[]})
// so the client's post-auth path is identical for both flows.
func SignupHandler(w http.ResponseWriter, r *http.Request) {
	if db == nil {
		http.Error(w, "service unavailable", http.StatusServiceUnavailable)
		return
	}
	var creds struct {
		Username string `json:"username"`
		Password string `json:"password"`
		FullName string `json:"fullName"`
	}
	if err := json.NewDecoder(r.Body).Decode(&creds); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}
	creds.Username = strings.ToLower(strings.TrimSpace(creds.Username))

	// Anonymous rate limit — the "signup" row has been in
	// actionLimitTable all along, unused until now.
	if !allowAction("", "signup") {
		writeRateLimited(w, "signup")
		return
	}

	if !usernameRe.MatchString(creds.Username) {
		http.Error(w, "username must be 3-20 chars: a-z, 0-9, _ or .", http.StatusBadRequest)
		return
	}
	if len(creds.Password) < 6 {
		http.Error(w, "password must be at least 6 characters", http.StatusBadRequest)
		return
	}
	if UserExists(creds.Username) {
		http.Error(w, "username already taken", http.StatusConflict)
		return
	}

	hash, err := hashPassword(creds.Password)
	if err != nil {
		http.Error(w, "signup failed", http.StatusInternalServerError)
		return
	}
	fullName := strings.TrimSpace(creds.FullName)
	if fullName == "" {
		fullName = creds.Username
	}
	// password column stays '' — bcrypt-only from day one (the plaintext
	// column exists solely for the legacy lazy-migration path).
	var id string
	err = db.QueryRow(`
		INSERT INTO users (username, password, password_hash, full_name, wins, losses, league)
		VALUES ($1, '', $2, $3, 0, 0, 'Bronze')
		RETURNING CAST(id AS TEXT)`,
		creds.Username, hash, fullName).Scan(&id)
	if err != nil {
		// Unique-violation race (two signups, same name) lands here too.
		log.Printf("signup insert failed for %q: %v", creds.Username, err)
		http.Error(w, "username already taken", http.StatusConflict)
		return
	}

	token, err := issueToken(id, creds.Username)
	if err != nil {
		log.Printf("signup token issuance failed for %s: %v", creds.Username, err)
		http.Error(w, "Signup succeeded but login is temporarily unavailable", http.StatusInternalServerError)
		return
	}

	user, _ := GetUserByUsername(creds.Username)
	go IndexUser(user) // searchable immediately

	log.Printf("New user signed up: %s (id=%s)", creds.Username, id)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"user":     user,
		"token":    token,
		"allUsers": []User{},
	})
}

// UsernameAvailableHandler — GET /signup/available?username=x.
// Powers the signup form's live availability check.
func UsernameAvailableHandler(w http.ResponseWriter, r *http.Request) {
	name := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("username")))
	valid := usernameRe.MatchString(name)
	available := valid && db != nil && !UserExists(name)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"valid":     valid,
		"available": available,
	})
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
