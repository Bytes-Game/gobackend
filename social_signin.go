package main

// social_signin.go — "Continue with Google" and "Continue with Apple".
//
// The phone gets a signed "this is me" token from Google or Apple and sends
// it here (POST /auth/google, POST /auth/apple). Once it is checked
// (identity_tokens.go):
//
//   - Somebody who has signed in this way before is signed in, through the
//     same steps as a password: the two-step code if it is on, and "keep
//     it?" if the account is waiting to be deleted.
//   - Somebody new gets a sign-up ticket and what Google or Apple said
//     about them (name, email). The app then asks for what every account
//     needs and nobody else can answer (a username, their birthday, the
//     terms) and finishes with POST /auth/social/signup.
//   - Somebody new whose email is already on an account made with a
//     password is refused, and told to sign in with the password. The two
//     are never joined on the email alone: sign-up does not prove anyone
//     owns the email they typed, so joining could hand a stranger's account
//     whatever the real owner of the email does next.
//
// An account made this way has no password. It cannot sign in with one
// until its owner sets one with "Forgot password?", which emails a code.

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"log"
	"math/big"
	"net/http"
	"os"
	"strings"
	"time"

	jwt "github.com/golang-jwt/jwt/v5"
	"github.com/lib/pq"
)

// signupTicketTTL is how long a sign-up ticket works: time to pick a
// username and a birthday.
const signupTicketTTL = 15 * time.Minute

const signupTicketAudience = "flex-sign-up-ticket"

// signupTicketClaims is a checked Google or Apple sign-in, waiting for the
// rest of the new account.
type signupTicketClaims struct {
	Provider string `json:"provider"`
	Email    string `json:"email,omitempty"`
	Name     string `json:"name,omitempty"`
	jwt.RegisteredClaims
}

// signupTicketKey is derived from JWT_SECRET, like the sign-in tickets,
// and different from both them and the session key.
func signupTicketKey() ([]byte, error) {
	secret, err := authSecret()
	if err != nil {
		return nil, err
	}
	m := hmac.New(sha256.New, secret)
	m.Write([]byte("flex sign-up ticket"))
	return m.Sum(nil), nil
}

func issueSignupTicket(provider string, who identity) (string, error) {
	key, err := signupTicketKey()
	if err != nil {
		return "", err
	}
	now := time.Now()
	return jwt.NewWithClaims(jwt.SigningMethodHS256, signupTicketClaims{
		Provider: provider,
		Email:    who.Email,
		Name:     who.Name,
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   who.Subject,
			Audience:  jwt.ClaimStrings{signupTicketAudience},
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(signupTicketTTL)),
		},
	}).SignedString(key)
}

func readSignupTicket(tok string) (*signupTicketClaims, error) {
	key, err := signupTicketKey()
	if err != nil {
		return nil, err
	}
	claims := &signupTicketClaims{}
	_, err = jwt.ParseWithClaims(tok, claims, func(t *jwt.Token) (interface{}, error) {
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, errors.New("unexpected signing method")
		}
		return key, nil
	}, jwt.WithAudience(signupTicketAudience), jwt.WithExpirationRequired())
	if err != nil || claims.Subject == "" || providerByID(claims.Provider) == nil {
		return nil, errors.New("the sign-up ticket ran out, or is not one")
	}
	return claims, nil
}

func providerByID(id string) *identityProvider {
	switch id {
	case "google":
		return googleProvider
	case "apple":
		return appleProvider
	}
	return nil
}

// socialAccount is the account joined to [provider]'s [subject], or "".
func socialAccount(provider, subject string) (string, error) {
	var id string
	err := db.QueryRow(`SELECT user_id::text FROM social_logins WHERE provider = $1 AND subject = $2`,
		provider, subject).Scan(&id)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return id, err
}

// suggestUsername is a free username made from the person's name, or their
// email when the name gives too little, for the form to start from. They
// can change it. "" when nothing free comes of it.
func suggestUsername(name, email string) string {
	clean := func(raw string) string {
		var b strings.Builder
		for _, r := range strings.ToLower(raw) {
			switch {
			case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '_', r == '.':
				b.WriteRune(r)
			case r == ' ' || r == '-':
				b.WriteRune('_')
			}
		}
		out := b.String()
		for strings.Contains(out, "..") {
			out = strings.ReplaceAll(out, "..", ".")
		}
		if len(out) > 15 {
			out = out[:15]
		}
		return strings.Trim(out, "._")
	}
	base := clean(name)
	if len(base) < 3 {
		local, _, _ := strings.Cut(email, "@")
		base = clean(local)
	}
	if len(base) < 3 {
		base = "flexer"
	}
	if usernameProblem(base) == "" && !UserExists(base) {
		return base
	}
	for i := 0; i < 6; i++ {
		n, err := rand.Int(rand.Reader, big.NewInt(9000))
		if err != nil {
			return ""
		}
		try := base + "_" + n.Add(n, big.NewInt(1000)).String()
		if usernameProblem(try) == "" && !UserExists(try) {
			return try
		}
	}
	return ""
}

// socialSignIn is POST /auth/google or /auth/apple
// {"idToken", "fullName"?}.
//
//	200 {user, token, allUsers} — signed in.
//	200 {"needsSignup": true, "signupTicket", "email", "fullName",
//	     "suggestedUsername"} — new here: finish at /auth/social/signup.
//	401 {"twoFactorRequired": true, "ticket"} — the code is next
//	    (two_step_signin.go).
//	401 {"error"} — the token was not good.
//	409 {"error", "emailTaken": true} — a password account has this email.
//	409 {"error", "deletionPending", "deleteOn", "ticket"} — see
//	    completeSignIn.
//	503 {"error", "unavailable": true} — not set up on the server.
//
// fullName is what Apple told the app: Apple gives a person's name to the
// app only, and only the first time, never in the token.
func socialSignIn(p *identityProvider) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			IDToken  string `json:"idToken"`
			FullName string `json:"fullName"`
		}
		if err := json.NewDecoder(io.LimitReader(r.Body, 16<<10)).Decode(&body); err != nil {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}
		if !allowAction("ip:"+clientIP(r), "social_signin") {
			writeRateLimited(w, "social_signin")
			return
		}
		if !p.configured() {
			log.Printf("%s sign-in: asked for, but %s is not set", p.name, p.audienceEnv)
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{
				"error":       "Signing in with " + p.name + " isn't set up yet. Sign in with your username or email.",
				"unavailable": true,
			})
			return
		}
		who, err := p.verify(strings.TrimSpace(body.IDToken))
		if err != nil {
			log.Printf("%s sign-in: a token was refused: %v", p.name, err)
			writeJSON(w, http.StatusUnauthorized, map[string]any{
				"error": "Couldn't sign you in with " + p.name + ". Please try again.",
			})
			return
		}
		// An email the provider has not checked is not trusted to be theirs:
		// it neither joins nor blocks anything, and is not kept.
		if !who.EmailVerified {
			who.Email = ""
		}

		userID, err := socialAccount(p.id, who.Subject)
		if queryFailed("which account is joined to a "+p.name+" sign-in", "the sign-in is refused", err) {
			http.Error(w, "Login temporarily unavailable", http.StatusInternalServerError)
			return
		}
		if userID != "" {
			if user, ok := GetUserByID(userID); ok {
				on, err := twoStepOn(user.ID)
				if queryFailed("whether "+user.Username+" has two-step verification on", "the sign-in is refused", err) {
					http.Error(w, "Login temporarily unavailable", http.StatusInternalServerError)
					return
				}
				if on {
					askForSecondStep(w, user)
					return
				}
				// Waiting to be deleted: asked, with a ticket that keeps it
				// (POST /login/2fa) without Google or Apple again.
				completeSignIn(w, r, user, false)
				return
			}
		}

		// New here.
		if who.Email != "" && emailTaken(who.Email) {
			log.Printf("%s sign-in: new, but a password account already uses the email", p.name)
			writeJSON(w, http.StatusConflict, map[string]any{
				"error": "An account already uses this email. Sign in with your username or email " +
					"and your password.",
				"emailTaken": true,
			})
			return
		}
		if who.Name == "" {
			who.Name, _ = cleanFullName(body.FullName)
		}
		ticket, err := issueSignupTicket(p.id, who)
		if err != nil {
			log.Printf("%s sign-in: no sign-up ticket could be made: %v", p.name, err)
			http.Error(w, "Login temporarily unavailable", http.StatusInternalServerError)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"needsSignup":       true,
			"signupTicket":      ticket,
			"email":             who.Email,
			"fullName":          who.Name,
			"suggestedUsername": suggestUsername(who.Name, who.Email),
		})
	}
}

// SocialSignupHandler — POST /auth/social/signup {"signupTicket", "username",
// "fullName", "birthDate", "acceptTerms"}.
//
// Makes the account, joined to the Google or Apple sign-in in the ticket,
// and signs it in: the answer is the same as /signup's. A refusal says
// which box it is about, as /signup's do.
func SocialSignupHandler(w http.ResponseWriter, r *http.Request) {
	if db == nil {
		http.Error(w, "service unavailable", http.StatusServiceUnavailable)
		return
	}
	var in struct {
		SignupTicket string `json:"signupTicket"`
		Username     string `json:"username"`
		FullName     string `json:"fullName"`
		BirthDate    string `json:"birthDate"`
		AcceptTerms  bool   `json:"acceptTerms"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 16<<10)).Decode(&in); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}
	// The same budget as making an account with a password.
	ip := clientIP(r)
	if !allowAction("ip:"+ip, "signup") || !allowAction("ip:"+ip, "signup_day") ||
		!allowAction("", "signup_all") {
		log.Printf("sign-up with Google/Apple rate limited from %s", ip)
		writeRateLimited(w, "signup")
		return
	}
	t, err := readSignupTicket(strings.TrimSpace(in.SignupTicket))
	p := (*identityProvider)(nil)
	if err == nil {
		p = providerByID(t.Provider)
	}
	if err != nil || p == nil {
		writeJSON(w, http.StatusUnauthorized, map[string]any{
			"error":   "That took too long. Start again.",
			"expired": true,
		})
		return
	}

	// The same rules as every account (account_rules.go), in the form's order.
	fullName, why := cleanFullName(in.FullName)
	if why != "" {
		writeFieldError(w, http.StatusBadRequest, refuse("fullName", why))
		return
	}
	username := strings.ToLower(strings.TrimSpace(in.Username))
	if why := usernameProblem(username); why != "" {
		writeFieldError(w, http.StatusBadRequest, refuse("username", why))
		return
	}
	born, why := parseBirthDate(in.BirthDate, time.Now())
	if why != "" {
		writeFieldError(w, http.StatusBadRequest, refuse("birthDate", why))
		return
	}
	if !in.AcceptTerms {
		writeFieldError(w, http.StatusBadRequest,
			refuse("terms", "Please agree to the Terms of Service and the Privacy Policy."))
		return
	}
	if fullName == "" {
		fullName = username
	}
	if UserExists(username) {
		writeFieldError(w, http.StatusConflict, refuse("username", "That username is taken."))
		return
	}
	// Already made (the button tapped twice, or the ticket used again): say
	// so before anything else, since that is the answer that helps.
	already := func() {
		writeJSON(w, http.StatusConflict, map[string]any{
			"error":         "You already have an account. Sign in with " + p.name + ".",
			"alreadyJoined": true,
		})
	}
	if id, err := socialAccount(t.Provider, t.Subject); err == nil && id != "" {
		already()
		return
	} else if queryFailed("whether a "+p.name+" sign-in already has an account", "the sign-up is refused", err) {
		http.Error(w, "signup failed", http.StatusInternalServerError)
		return
	}
	if t.Email != "" && emailTaken(t.Email) {
		writeJSON(w, http.StatusConflict, map[string]any{
			"error": "An account already uses this email. Sign in with your username or email " +
				"and your password.",
			"emailTaken": true,
		})
		return
	}

	id, err := createSocialAccount(r, t, username, fullName, born)
	if err != nil {
		var pqErr *pq.Error
		if errors.As(err, &pqErr) && pqErr.Code == "23505" {
			switch {
			case strings.Contains(pqErr.Constraint, "social_logins"):
				already()
				return
			case strings.Contains(pqErr.Constraint, "email"):
				writeJSON(w, http.StatusConflict, map[string]any{
					"error":      "An account already uses this email. Sign in with your password.",
					"emailTaken": true,
				})
				return
			case strings.Contains(pqErr.Constraint, "username"):
				writeFieldError(w, http.StatusConflict, refuse("username", "That username is taken."))
				return
			}
		}
		log.Printf("sign-up with %s failed for %q: %v", p.name, username, err)
		http.Error(w, "signup failed", http.StatusInternalServerError)
		return
	}
	log.Printf("New user signed up with %s: %s (id=%s)", p.name, username, id)
	welcomeNewAccount(w, id, username)
}

// createSocialAccount makes the account and joins it to the sign-in, both
// or neither.
func createSocialAccount(r *http.Request, t *signupTicketClaims, username, fullName string, born time.Time) (string, error) {
	tx, err := db.BeginTx(r.Context(), nil)
	if err != nil {
		return "", err
	}
	defer tx.Rollback()
	email := sql.NullString{String: t.Email, Valid: t.Email != ""}
	var id string
	// No password: '' in both columns, which IsValidUser never accepts.
	if err := tx.QueryRowContext(r.Context(), `
		INSERT INTO users (username, password, password_hash, full_name, email,
		                   birth_date, terms_accepted_at, wins, losses, league)
		VALUES ($1, '', '', $2, $3, $4, NOW(), 0, 0, 'Bronze')
		RETURNING CAST(id AS TEXT)`,
		username, fullName, email, born).Scan(&id); err != nil {
		return "", err
	}
	if _, err := tx.ExecContext(r.Context(), `
		INSERT INTO social_logins (provider, subject, user_id, email)
		VALUES ($1, $2, $3::int, $4)`, t.Provider, t.Subject, id, t.Email); err != nil {
		return "", err
	}
	return id, tx.Commit()
}

// AppleCallbackHandler — POST /auth/apple/callback.
//
// On Android, Apple's sign-in happens in a web page, not in the phone's
// own settings as on an iPhone. When the person finishes, Apple posts the
// result here, and this hands it straight back to the app, which then
// sends the token to /auth/apple like an iPhone does. Nothing is checked
// or kept here: the token is checked there.
//
// Set ANDROID_PACKAGE_NAME to the app's Android id (applicationId) when it
// changes for release; until then it is the template's com.example.devf.
func AppleCallbackHandler(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 64<<10)
	if err := r.ParseForm(); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}
	pkg := strings.TrimSpace(os.Getenv("ANDROID_PACKAGE_NAME"))
	if pkg == "" {
		pkg = "com.example.devf"
	}
	http.Redirect(w, r, "intent://callback?"+r.PostForm.Encode()+
		"#Intent;package="+pkg+";scheme=signinwithapple;end", http.StatusTemporaryRedirect)
}
