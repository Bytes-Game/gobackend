package main

// parent_consent.go — a young person joins only once a parent says yes.
//
// ════════════════════════════════════════════════════════════════════════
// WHAT HAPPENS
// ════════════════════════════════════════════════════════════════════════
//
// Somebody younger than the consent age where they are (age_rules.go: 18,
// everywhere) fills in the sign-up form and gives a parent's or guardian's
// email. Then:
//
//   1. NO ACCOUNT IS MADE. The sign-up is put aside (pending_signups). It
//      cannot sign in, be seen, be searched for, or be shown anything, and
//      nothing is learned about the young person meanwhile. It holds only
//      what is needed to ask the parent and to make the account if they
//      agree.
//   2. The parent is emailed a link. The page it opens says who is asking,
//      what Flexete is and what it keeps, and asks the parent for their
//      name, whether they are the parent or the legal guardian, and their
//      year of birth (they must be an adult). They choose "I agree" or
//      "I do not agree".
//   3. I agree: the account is made, with a record of who agreed to what,
//      when, and from where (parent_consents). The parent is emailed a
//      confirmation with a second link that closes the account and deletes
//      everything, for good, whenever they choose.
//   4. I do not agree, or no answer in seven days: the sign-up is deleted.
//
// While it waits, signing in with the same username and password (or the
// same Google or Apple account) answers "waiting for your parent" rather
// than "wrong password", with a ticket that lets the app send the email
// again or change the parent's address.
//
// ════════════════════════════════════════════════════════════════════════
// WHAT THIS IS, AND IS NOT
// ════════════════════════════════════════════════════════════════════════
//
// This is consent by email with a declaration: the usual method for an app
// with no money to spend on identity checks. It does not PROVE the person
// answering is the parent, or an adult — a determined child with a second
// email address can answer for themselves. India's rules (from about May
// 2027) ask for more: a check against an identity service such as
// DigiLocker. That needs a paid service and is not built here. The owner's
// before-release list says so.
//
// The secret in each link is kept only as its SHA-256, so somebody reading
// the database cannot answer for a parent.

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"html/template"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	jwt "github.com/golang-jwt/jwt/v5"
	"github.com/lib/pq"
)

const (
	// parentConsentWait is how long a sign-up waits for the parent.
	parentConsentWait = 7 * 24 * time.Hour
	// parentDeclinedKeep is how long a refused sign-up is kept, only so the
	// young person can be told why they cannot sign in.
	parentDeclinedKeep = 24 * time.Hour
	// parentEmailsMax is how many emails one sign-up may send a parent, so
	// this server cannot be used to fill a stranger's inbox.
	parentEmailsMax = 5
	// parentEmailGap is the shortest wait between two of them.
	parentEmailGap = time.Minute
	// consentWording names the words the parent agreed to. Change it when
	// the consent page's words change in a way that matters.
	consentWording = "2026-11"
	// pendingTicketTTL is how long the app may resend or change the
	// parent's email after a sign-in attempt, without the password again.
	pendingTicketTTL = 30 * time.Minute

	pendingTicketAudience = "flexete-parent-consent-ticket"
)

// pendingSignup is a sign-up waiting for a parent.
type pendingSignup struct {
	id                                int
	username, passwordHash, fullName  string
	email, country, provider, subject string
	parentEmail                       string
	// tokenHash is the SHA-256 of the secret in the parent's link.
	tokenHash               string
	birthDate               time.Time
	emailsSent              int
	lastEmailAt, declinedAt sql.NullTime
	expiresAt               time.Time
}

// newLinkSecret is a fresh secret for a link, and the form it is kept in.
func newLinkSecret() (secret, hash string, err error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", "", err
	}
	secret = base64.RawURLEncoding.EncodeToString(b)
	return secret, hashLinkSecret(secret), nil
}

func hashLinkSecret(secret string) string {
	sum := sha256.Sum256([]byte(secret))
	return hex.EncodeToString(sum[:])
}

// serverAddress is this server's own address, for the links in emails.
//
// A link in an email must point at this server and nowhere else, so the
// address is taken from the server's own settings when there are any:
//
//	PUBLIC_BASE_URL       set by the owner (https://flexete.com once that
//	                      points here);
//	RENDER_EXTERNAL_URL   set by Render itself, with no setup.
//
// Only without both is it the address the request came to, which is what
// the sender says it is: fine on a developer's machine, not something to
// put in an email to a stranger.
func serverAddress(r *http.Request) string {
	for _, name := range []string{"PUBLIC_BASE_URL", "RENDER_EXTERNAL_URL"} {
		base := strings.TrimRight(strings.TrimSpace(os.Getenv(name)), "/")
		if strings.HasPrefix(base, "https://") || strings.HasPrefix(base, "http://") {
			return base
		}
	}
	return publicBaseURL(r)
}

// ─── the ticket the app holds while it waits ────────────────────────────

type pendingTicketClaims struct {
	jwt.RegisteredClaims
}

func pendingTicketKey() ([]byte, error) {
	secret, err := authSecret()
	if err != nil {
		return nil, err
	}
	m := hmac.New(sha256.New, secret)
	m.Write([]byte("flexete parent consent ticket"))
	return m.Sum(nil), nil
}

func issuePendingTicket(pendingID int) (string, error) {
	key, err := pendingTicketKey()
	if err != nil {
		return "", err
	}
	now := time.Now()
	return jwt.NewWithClaims(jwt.SigningMethodHS256, pendingTicketClaims{
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   strconv.Itoa(pendingID),
			Audience:  jwt.ClaimStrings{pendingTicketAudience},
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(pendingTicketTTL)),
		},
	}).SignedString(key)
}

func readPendingTicket(tok string) (int, error) {
	key, err := pendingTicketKey()
	if err != nil {
		return 0, err
	}
	claims := &pendingTicketClaims{}
	_, err = jwt.ParseWithClaims(tok, claims, func(t *jwt.Token) (interface{}, error) {
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, errors.New("unexpected signing method")
		}
		return key, nil
	}, jwt.WithAudience(pendingTicketAudience), jwt.WithExpirationRequired())
	if err != nil {
		return 0, errors.New("the ticket ran out, or is not one")
	}
	id, err := strconv.Atoi(claims.Subject)
	if err != nil || id <= 0 {
		return 0, errors.New("the ticket ran out, or is not one")
	}
	return id, nil
}

// ─── reading and writing sign-ups that wait ─────────────────────────────

const pendingColumns = `id, username, password_hash, full_name, email, birth_date, country,
	provider, subject, parent_email, token_hash, emails_sent, last_email_at, declined_at, expires_at`

func scanPending(row interface{ Scan(...any) error }) (*pendingSignup, error) {
	p := &pendingSignup{}
	err := row.Scan(&p.id, &p.username, &p.passwordHash, &p.fullName, &p.email, &p.birthDate,
		&p.country, &p.provider, &p.subject, &p.parentEmail, &p.tokenHash, &p.emailsSent,
		&p.lastEmailAt, &p.declinedAt, &p.expiresAt)
	if err != nil {
		return nil, err
	}
	return p, nil
}

// pendingByID is the waiting sign-up [id], or nil when there is none (or
// its time is up).
func pendingByID(id int) (*pendingSignup, error) {
	p, err := scanPending(db.QueryRow(`SELECT `+pendingColumns+` FROM pending_signups
		WHERE id = $1 AND expires_at > NOW()`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return p, err
}

// pendingByLogin is the waiting sign-up whose username or email is [login]
// and whose password is [password], or nil. The password is checked so
// that nobody can learn, from a sign-in attempt, that somebody else's
// sign-up is waiting.
func pendingByLogin(login, password string) (*pendingSignup, error) {
	login = strings.ToLower(strings.TrimSpace(login))
	if login == "" || password == "" {
		return nil, nil
	}
	p, err := scanPending(db.QueryRow(`SELECT `+pendingColumns+` FROM pending_signups
		WHERE (lower(username) = $1 OR (email <> '' AND lower(email) = $1))
		  AND provider = '' AND expires_at > NOW()`, login))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if p.passwordHash == "" || !checkPassword(p.passwordHash, password) {
		return nil, nil
	}
	return p, nil
}

// pendingBySocial is the waiting sign-up made with this Google or Apple
// account, or nil.
func pendingBySocial(provider, subject string) (*pendingSignup, error) {
	if provider == "" || subject == "" {
		return nil, nil
	}
	p, err := scanPending(db.QueryRow(`SELECT `+pendingColumns+` FROM pending_signups
		WHERE provider = $1 AND subject = $2 AND expires_at > NOW()`, provider, subject))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return p, err
}

// pendingHolds is whether a waiting sign-up holds this username or this
// email, so nobody else can take either meanwhile. [except] are waiting
// sign-ups that do not count: the person's own, which a new try replaces.
// A database that cannot answer says no and logs it: the unique indexes
// still refuse a clash.
func pendingHolds(username, email string, except []int) (name, mail bool) {
	err := db.QueryRow(`
		SELECT EXISTS (SELECT 1 FROM pending_signups
		                WHERE lower(username) = lower($1) AND expires_at > NOW() AND NOT (id = ANY($3))),
		       EXISTS (SELECT 1 FROM pending_signups
		                WHERE $2 <> '' AND lower(email) = lower($2) AND expires_at > NOW() AND NOT (id = ANY($3)))`,
		username, email, pq.Array(intsOrEmpty(except))).Scan(&name, &mail)
	if queryFailed("checking whether a sign-up waiting for a parent holds a username or an email",
		"letting the sign-up try; the database still refuses a clash", err) {
		return false, false
	}
	return name, mail
}

// intsOrEmpty is [ids] as the database wants a list of numbers: never nil.
func intsOrEmpty(ids []int) []int64 {
	out := make([]int64, 0, len(ids))
	for _, id := range ids {
		out = append(out, int64(id))
	}
	return out
}

// usernameTaken is whether [name] is taken: by an account, or held by a
// sign-up that is waiting for a parent.
func usernameTaken(name string) bool {
	if UserExists(name) {
		return true
	}
	held, _ := pendingHolds(name, "", nil)
	return held
}

// ownPendingSignups are the waiting sign-ups that are this same person's
// earlier tries: the same username or the same email, AND the same
// password. A new try replaces them, which is how a mistake in the
// parent's address is put right (or a "no" pressed by mistake). The
// password is what shows it is the same person: without it, anybody could
// push out somebody else's sign-up by knowing their username.
func ownPendingSignups(username, email, password string) []int {
	rows, err := db.Query(`
		SELECT id, password_hash FROM pending_signups
		 WHERE provider = ''
		   AND (lower(username) = lower($1) OR ($2 <> '' AND lower(email) = lower($2)))`,
		username, email)
	if queryFailed("looking for the same person's earlier sign-up that is waiting for a parent",
		"it is treated as somebody else's; the new try is refused as taken", err) {
		return nil
	}
	defer rows.Close()
	var own []int
	bad := 0
	for rows.Next() {
		var id int
		var hash string
		if scanFailed("a sign-up waiting for a parent", rows.Scan(&id, &hash), &bad) {
			continue
		}
		if hash != "" && checkPassword(hash, password) {
			own = append(own, id)
		}
	}
	return own
}

// purgeExpiredPendingSignups deletes every waiting sign-up whose time is
// up, answered or not, and says how many. Run by the hourly account job.
func purgeExpiredPendingSignups() int {
	if db == nil {
		return 0
	}
	res, err := db.Exec(`DELETE FROM pending_signups WHERE expires_at <= NOW()`)
	if err != nil {
		queryFailed("deleting sign-ups whose parent did not answer in time",
			"they are kept until the next hour", err)
		return 0
	}
	n, _ := res.RowsAffected()
	if n > 0 {
		log.Printf("parent consent: %d sign-up(s) deleted: no answer from a parent in time, or the parent said no", n)
	}
	return int(n)
}

// ─── starting to wait ───────────────────────────────────────────────────

// parentEmailProblem says what is wrong with the parent's email a young
// person gave, or "". [own] is the young person's own email.
func parentEmailProblem(raw, own string) (string, string) {
	if strings.TrimSpace(raw) == "" {
		return "", "Enter your parent's or guardian's email."
	}
	email, why := cleanEmail(raw)
	if why != "" {
		return "", "Enter your parent's or guardian's email."
	}
	if own != "" && email == own {
		return "", "Use your parent's or guardian's own email, not yours."
	}
	return email, ""
}

// waitingAnswer is what the app is told while a sign-up waits: where the
// email went, and a ticket to send it again or change the address. The
// words are in "message"; a refused sign-in carries them in "error" too
// (refusedWaiting), where the app looks for why it was refused.
func waitingAnswer(p *pendingSignup) map[string]any {
	body := map[string]any{
		"parentConsentPending": true,
		"parentEmail":          maskEmail(p.parentEmail),
		"expiresAt":            p.expiresAt.UTC().Format(time.RFC3339),
		"message": "Waiting for your parent or guardian to say yes. We emailed them at " +
			maskEmail(p.parentEmail) + ".",
	}
	if p.declinedAt.Valid {
		body["declined"] = true
		body["message"] = "Your parent or guardian said no, so no account was made."
	}
	if ticket, err := issuePendingTicket(p.id); err == nil {
		body["ticket"] = ticket
	} else {
		log.Printf("parent consent: no ticket could be made for waiting sign-up %d: %v", p.id, err)
	}
	return body
}

// refusedWaiting answers a sign-in by somebody whose sign-up is still
// waiting for a parent (or was refused by one): 403, and why.
func refusedWaiting(w http.ResponseWriter, p *pendingSignup) {
	body := waitingAnswer(p)
	body["error"] = body["message"]
	writeJSON(w, http.StatusForbidden, body)
}

// errParentEmailNotSent is why a sign-up that needs a parent could not
// start: the email to the parent did not go.
var errParentEmailNotSent = errors.New("the email to the parent could not be sent")

// allowParentEmail is whether one more email may go to [address] today.
// However many sign-ups name it, and wherever they come from, one address
// gets at most a handful a day: this server must not be a way to fill a
// stranger's inbox.
func allowParentEmail(address string) bool {
	return allowAction("parent:"+strings.ToLower(address), "parent_address")
}

// startParentConsent puts a sign-up aside, emails the parent, and answers
// the app: 202 with where the email went and a ticket (waitingAnswer).
// [replace] are the person's own earlier tries, which make way.
func startParentConsent(w http.ResponseWriter, r *http.Request, p pendingSignup, replace []int) {
	// Asked first, so a sign-up that cannot be answered is not kept.
	if !mailConfigured() {
		log.Printf("parent consent: a sign-up needs a parent's email, but email is not set up " +
			"(MAIL_FROM, and RESEND_API_KEY or SMTP_HOST) — nobody under the consent age can join")
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"error": "We can't email parents right now, so we can't take sign-ups that need a parent's " +
				"permission. Please try again later.",
			"unavailable": true,
		})
		return
	}
	if !allowParentEmail(p.parentEmail) {
		writeJSON(w, http.StatusTooManyRequests, map[string]any{
			"field": "parentEmail",
			"error": "We've emailed that address as many times as we can today. Try again tomorrow.",
		})
		return
	}
	saved, err := beginParentConsent(r, p, replace)
	if err != nil {
		if errors.Is(err, errParentEmailNotSent) {
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{
				"error":       "We couldn't email your parent or guardian right now. Try again in a little while.",
				"unavailable": true,
			})
			return
		}
		// Two sign-ups racing for one name or one email land here.
		if constraint, ok := isUniqueViolation(err); ok {
			switch {
			case strings.Contains(constraint, "email"):
				writeFieldError(w, http.StatusConflict, refuse("email",
					"A sign-up with this email is waiting for a parent's answer. Sign in to send their email again."))
				return
			case strings.Contains(constraint, "username"):
				writeFieldError(w, http.StatusConflict, refuse("username", "That username is taken."))
				return
			case strings.Contains(constraint, "social"):
				writeJSON(w, http.StatusConflict, map[string]any{
					"error": "This sign-up is already waiting for a parent's answer.", "alreadyJoined": true})
				return
			}
		}
		log.Printf("parent consent: sign-up %q could not be put aside to wait for a parent: %v", p.username, err)
		http.Error(w, "signup failed", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusAccepted, waitingAnswer(saved))
}

// beginParentConsent puts a sign-up aside and emails the parent. The
// person's own earlier tries ([replace], or the same Google or Apple
// account) make way for it. Nothing is kept if the email cannot be sent: a
// sign-up nobody was asked about would wait seven days for nothing.
func beginParentConsent(r *http.Request, p pendingSignup, replace []int) (*pendingSignup, error) {
	secret, hash, err := newLinkSecret()
	if err != nil {
		return nil, err
	}
	tx, err := db.BeginTx(r.Context(), nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	// Out of the way: what has run out of time, and this person's earlier
	// tries.
	if _, err = tx.ExecContext(r.Context(), `
		DELETE FROM pending_signups
		 WHERE expires_at <= NOW() OR id = ANY($1)
		    OR ($2 <> '' AND provider = $2 AND subject = $3)`,
		pq.Array(intsOrEmpty(replace)), p.provider, p.subject); err != nil {
		return nil, err
	}
	saved, err := scanPending(tx.QueryRowContext(r.Context(), `
		INSERT INTO pending_signups (username, password_hash, full_name, email, birth_date, country,
		                             provider, subject, parent_email, token_hash, emails_sent,
		                             last_email_at, expires_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 1, NOW(), $11)
		RETURNING `+pendingColumns,
		p.username, p.passwordHash, p.fullName, p.email, p.birthDate, p.country,
		p.provider, p.subject, p.parentEmail, hash, time.Now().Add(parentConsentWait)))
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	// Sent after the row is saved, not while the database is held open:
	// an email service can take ten seconds to answer.
	if err := sendParentRequest(saved, serverAddress(r)+"/parent-consent?token="+secret); err != nil {
		log.Printf("parent consent: the email to the parent of %q could not be sent: %v", p.username, err)
		if _, delErr := db.Exec(`DELETE FROM pending_signups WHERE id = $1`, saved.id); delErr != nil {
			log.Printf("parent consent: and the sign-up %q could not be taken back out: %v — "+
				"it is deleted when its seven days are up", p.username, delErr)
		}
		return nil, errParentEmailNotSent
	}
	log.Printf("parent consent: sign-up %q (%s) is waiting for a parent at %s",
		saved.username, countryOrUnknown(saved.country), maskEmail(saved.parentEmail))
	return saved, nil
}

func countryOrUnknown(c string) string {
	if c == "" {
		return "country not known"
	}
	return c
}

// sendParentRequest emails the parent the link.
func sendParentRequest(p *pendingSignup, link string) error {
	name := p.fullName
	if name == "" || name == p.username {
		name = "@" + p.username
	} else {
		name += " (@" + p.username + ")"
	}
	body := "Hello,\n\n" +
		name + " gave this email address as their parent's or guardian's, and wants to join Flexete.\n\n" +
		"Flexete is an app where people post short videos and challenge each other to video battles.\n\n" +
		"They told us they were born on " + p.birthDate.Format("2 January 2006") + ". Because they are " +
		"under 18, they can only join if you agree.\n\n" +
		"To read what Flexete keeps about them, and to say yes or no, open this link:\n\n" +
		link + "\n\n" +
		"The link works for 7 days. If you do nothing, no account is made and their details are deleted.\n\n" +
		"If you do not know this person, choose \"I do not agree\" on that page, or ignore this email.\n\n" +
		"Flexete\n" + supportEmail + "\n"
	return sendMail(p.parentEmail, "Your permission is needed for a Flexete account", body)
}

// ─── the app: send again, or change the address ─────────────────────────

// ParentEmailHandler — POST /signup/parent {"ticket", "parentEmail"?,
// "cancel"?}.
//
// While a sign-up waits: sends the parent's email again, or, with a new
// address, sends it there instead. With "cancel": true it deletes the
// sign-up instead, at once: the young person changed their mind. The
// ticket comes with the sign-up's answer and with every "waiting for your
// parent" answer to a sign-in.
func ParentEmailHandler(w http.ResponseWriter, r *http.Request) {
	if db == nil {
		http.Error(w, "service unavailable", http.StatusServiceUnavailable)
		return
	}
	var in struct {
		Ticket      string `json:"ticket"`
		ParentEmail string `json:"parentEmail"`
		Cancel      bool   `json:"cancel"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 16<<10)).Decode(&in); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}
	if !allowAction("ip:"+clientIP(r), "parent_email") {
		writeRateLimited(w, "parent_email")
		return
	}
	id, err := readPendingTicket(strings.TrimSpace(in.Ticket))
	if err != nil {
		writeJSON(w, http.StatusUnauthorized, map[string]any{
			"error": "That took too long. Sign in again to carry on.", "expired": true})
		return
	}
	p, err := pendingByID(id)
	if queryFailed("reading a sign-up that is waiting for a parent", "the email is not sent again", err) {
		http.Error(w, "temporarily unavailable", http.StatusInternalServerError)
		return
	}
	if in.Cancel {
		// Theirs to take back, whatever state it is in.
		if p != nil {
			if _, err := db.Exec(`DELETE FROM pending_signups WHERE id = $1`, p.id); err != nil {
				log.Printf("parent consent: waiting sign-up %d could not be cancelled: %v", p.id, err)
				http.Error(w, "temporarily unavailable", http.StatusInternalServerError)
				return
			}
			log.Printf("parent consent: sign-up %q was cancelled by the person who made it", p.username)
		}
		writeJSON(w, http.StatusOK, map[string]any{"cancelled": true})
		return
	}
	if p == nil || p.declinedAt.Valid {
		writeJSON(w, http.StatusGone, map[string]any{
			"error": "This sign-up is no longer waiting. Sign up again to start over.", "gone": true})
		return
	}
	parent := p.parentEmail
	if strings.TrimSpace(in.ParentEmail) != "" {
		var why string
		if parent, why = parentEmailProblem(in.ParentEmail, p.email); why != "" {
			writeFieldError(w, http.StatusBadRequest, refuse("parentEmail", why))
			return
		}
	}
	if p.emailsSent >= parentEmailsMax {
		writeJSON(w, http.StatusTooManyRequests, map[string]any{
			"error": "We've emailed them as many times as we can. Ask them to check their inbox and " +
				"their spam folder, or write to " + supportEmail + ".", "tooMany": true})
		return
	}
	if p.lastEmailAt.Valid {
		if wait := parentEmailGap - time.Since(p.lastEmailAt.Time); wait > 0 {
			writeJSON(w, http.StatusTooManyRequests, map[string]any{
				"error":      "We emailed them a moment ago. Try again in a minute.",
				"retryAfter": int(wait.Seconds()) + 1})
			return
		}
	}
	if !allowParentEmail(parent) {
		writeJSON(w, http.StatusTooManyRequests, map[string]any{
			"error":   "We've emailed that address as many times as we can today. Try again tomorrow.",
			"tooMany": true})
		return
	}
	secret, hash, err := newLinkSecret()
	if err != nil {
		http.Error(w, "temporarily unavailable", http.StatusInternalServerError)
		return
	}
	// A new link each time: the old one stops working, so a link sent to a
	// wrong address is dead the moment the address is put right.
	if _, err := db.Exec(`
		UPDATE pending_signups
		   SET parent_email = $2, token_hash = $3, emails_sent = emails_sent + 1, last_email_at = NOW()
		 WHERE id = $1`, p.id, parent, hash); err != nil {
		log.Printf("parent consent: waiting sign-up %d could not be given a new link: %v", p.id, err)
		http.Error(w, "temporarily unavailable", http.StatusInternalServerError)
		return
	}
	was := *p
	p.parentEmail = parent
	p.emailsSent++
	if err := sendParentRequest(p, serverAddress(r)+"/parent-consent?token="+secret); err != nil {
		log.Printf("parent consent: the email to the parent of %q could not be sent again: %v", p.username, err)
		// Nothing went out, so nothing changes: the link in the email the
		// parent already has keeps working, and this try is not counted.
		if _, undoErr := db.Exec(`
			UPDATE pending_signups
			   SET parent_email = $2, token_hash = $3, emails_sent = $4, last_email_at = $5
			 WHERE id = $1`, was.id, was.parentEmail, was.tokenHash, was.emailsSent, was.lastEmailAt); undoErr != nil {
			log.Printf("parent consent: and waiting sign-up %d could not be put back as it was: %v — "+
				"the parent's earlier link no longer works; sending again makes a new one", was.id, undoErr)
		}
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"error":       "We couldn't send the email right now. Try again in a little while.",
			"unavailable": true})
		return
	}
	writeJSON(w, http.StatusOK, waitingAnswer(p))
}

// ─── the parent's page ──────────────────────────────────────────────────

// parentPageRules: no script, nothing loaded, and the form may only be
// sent back here.
const parentPageRules = "default-src 'none'; style-src 'unsafe-inline'; base-uri 'none'; " +
	"form-action 'self'; frame-ancestors 'none'"

type parentPage struct {
	// Which page: "ask", "done", "declined", "taken", "invalid", "withdraw",
	// "withdrawn".
	State string
	// The young person.
	Name, Username, Born string
	Age                  int
	// The secret, carried back in the form.
	Token string
	// What was wrong with the form, and what was typed.
	Problem, ParentName, Relation, BirthYear string
	// The link that takes the consent back, on the "done" page.
	WithdrawLink string
	Support      string
}

func writeParentPage(w http.ResponseWriter, status int, page parentPage) {
	page.Support = supportEmail
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Content-Security-Policy", parentPageRules)
	w.Header().Set("X-Frame-Options", "DENY")
	w.Header().Set("Referrer-Policy", "no-referrer")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	if err := parentTmpl.Execute(w, page); err != nil {
		log.Printf("parent consent: the page could not be written: %v", err)
	}
}

func pendingByToken(token string) (*pendingSignup, error) {
	token = strings.TrimSpace(token)
	if token == "" {
		return nil, nil
	}
	p, err := scanPending(db.QueryRow(`SELECT `+pendingColumns+` FROM pending_signups
		WHERE token_hash = $1 AND expires_at > NOW() AND declined_at IS NULL`, hashLinkSecret(token)))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return p, err
}

func askPage(p *pendingSignup, token string) parentPage {
	return parentPage{
		State: "ask", Name: p.fullName, Username: p.username,
		Born: p.birthDate.Format("2 January 2006"), Age: ageOn(p.birthDate, time.Now()), Token: token,
	}
}

// ParentConsentPageHandler — GET /parent-consent?token=…: the page the
// parent's link opens.
func ParentConsentPageHandler(w http.ResponseWriter, r *http.Request) {
	if db == nil {
		http.Error(w, "service unavailable", http.StatusServiceUnavailable)
		return
	}
	token := r.URL.Query().Get("token")
	p, err := pendingByToken(token)
	if queryFailed("reading the sign-up a parent's link is for", "the parent is told to try again", err) {
		http.Error(w, "temporarily unavailable", http.StatusInternalServerError)
		return
	}
	if p == nil {
		writeParentPage(w, http.StatusNotFound, parentPage{State: "invalid"})
		return
	}
	writeParentPage(w, http.StatusOK, askPage(p, token))
}

// ParentConsentAnswerHandler — POST /parent-consent: the parent's answer.
func ParentConsentAnswerHandler(w http.ResponseWriter, r *http.Request) {
	if db == nil {
		http.Error(w, "service unavailable", http.StatusServiceUnavailable)
		return
	}
	if !allowAction("ip:"+clientIP(r), "parent_answer") {
		http.Error(w, "Too many tries. Please wait a few minutes.", http.StatusTooManyRequests)
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Error(w, "the form could not be read", http.StatusBadRequest)
		return
	}
	token := r.PostFormValue("token")
	p, err := pendingByToken(token)
	if queryFailed("reading the sign-up a parent answered", "the parent is told to try again", err) {
		http.Error(w, "temporarily unavailable", http.StatusInternalServerError)
		return
	}
	if p == nil {
		writeParentPage(w, http.StatusNotFound, parentPage{State: "invalid"})
		return
	}

	if r.PostFormValue("answer") == "no" {
		if _, err := db.Exec(`UPDATE pending_signups SET declined_at = NOW(), expires_at = $2 WHERE id = $1`,
			p.id, time.Now().Add(parentDeclinedKeep)); err != nil {
			log.Printf("parent consent: the parent's no for %q could not be saved: %v", p.username, err)
			http.Error(w, "temporarily unavailable", http.StatusInternalServerError)
			return
		}
		log.Printf("parent consent: the parent of %q said no; the sign-up is deleted within a day", p.username)
		writeParentPage(w, http.StatusOK, parentPage{State: "declined", Username: p.username})
		return
	}

	// "I agree": who is agreeing.
	page := askPage(p, token)
	page.ParentName = strings.Join(strings.Fields(r.PostFormValue("name")), " ")
	page.Relation = r.PostFormValue("relation")
	page.BirthYear = strings.TrimSpace(r.PostFormValue("birthYear"))
	year, yearErr := strconv.Atoi(page.BirthYear)
	now := time.Now()
	switch {
	case len([]rune(page.ParentName)) < 2 || len([]rune(page.ParentName)) > 80:
		page.Problem = "Enter your full name."
	case page.Relation != "parent" && page.Relation != "guardian":
		page.Problem = "Choose whether you are their parent or their legal guardian."
	case yearErr != nil || year < 1900 || year > now.Year():
		page.Problem = "Enter the year you were born, as four digits."
	case now.Year()-year < 18:
		page.Problem = "A parent or guardian must be 18 or older."
	case r.PostFormValue("confirm") != "yes":
		page.Problem = "Tick the box to confirm."
	}
	if page.Problem != "" {
		writeParentPage(w, http.StatusBadRequest, page)
		return
	}

	consentCountry, _ := countryOf(r)
	userID, withdrawSecret, err := makeConsentedAccount(r, p, parentRecord{
		name: page.ParentName, relation: page.Relation, birthYear: year,
		ip: clientIP(r), country: consentCountry, userAgent: firstRunes(r.UserAgent(), 300),
	})
	if err != nil {
		status, failed := consentFailurePage(err, p, page)
		writeParentPage(w, status, failed)
		return
	}
	log.Printf("parent consent: the %s of %q agreed; account %s made", page.Relation, p.username, userID)

	forgetNameLeftovers(p.username)
	if user, ok := GetUserByUsername(p.username); ok {
		go IndexUser(user)
	}
	// The link that takes the consent back. Emailed, and shown on the page
	// as well: if the email does not arrive, the parent still has it.
	link := serverAddress(r) + "/parent-consent/withdraw?token=" + withdrawSecret
	parentName, parentEmail, username := page.ParentName, p.parentEmail, p.username
	go func() {
		body := "Hello " + parentName + ",\n\n" +
			"You agreed to @" + username + " using Flexete. They can now sign in.\n\n" +
			"You can change your mind at any time. This link closes their account and deletes " +
			"everything in it, for good:\n\n" + link + "\n\n" +
			"Keep this email. To see or correct what Flexete keeps about them, or for anything " +
			"else, write to " + supportEmail + ".\n\nFlexete\n"
		if err := sendMail(parentEmail, "You agreed to a Flexete account", body); err != nil {
			log.Printf("parent consent: the confirmation to the parent of %q could not be sent: %v — "+
				"the page they are on shows the same link, and they can write to %s",
				username, err, supportEmail)
		}
	}()
	writeParentPage(w, http.StatusOK, parentPage{State: "done", Username: p.username, WithdrawLink: link})
}

// consentFailurePage is what the parent is shown when they agreed and the
// account could not be made. [asked] is the page they answered, to show
// again when trying again can help.
func consentFailurePage(err error, p *pendingSignup, asked parentPage) (int, parentPage) {
	// "I agree" pressed twice: the first press made the account. This one
	// has nothing left to do, and says so rather than "something went
	// wrong".
	if errors.Is(err, errNoLongerWaiting) {
		return http.StatusNotFound, parentPage{State: "invalid"}
	}
	// Somebody took the username or the email while this waited. It
	// cannot happen through the app, which holds both for a waiting
	// sign-up; if it does, the sign-up can never succeed, so it ends here
	// and the parent is told.
	if constraint, ok := isUniqueViolation(err); ok {
		log.Printf("parent consent: the parent of %q agreed, but the account cannot be made: %q is "+
			"taken (%s) — the sign-up is deleted", p.username, p.username, constraint)
		if _, delErr := db.Exec(`DELETE FROM pending_signups WHERE id = $1`, p.id); delErr != nil {
			log.Printf("parent consent: and the sign-up %q could not be deleted: %v", p.username, delErr)
		}
		return http.StatusConflict, parentPage{State: "taken", Username: p.username}
	}
	log.Printf("parent consent: the account for %q could not be made after the parent agreed: %v", p.username, err)
	asked.Problem = "Something went wrong on our side. Please try again in a moment."
	return http.StatusInternalServerError, asked
}

// firstRunes is at most the first [max] characters of [s].
func firstRunes(s string, max int) string {
	if r := []rune(s); len(r) > max {
		return string(r[:max])
	}
	return s
}

// errNoLongerWaiting is the answer to a second "I agree" for one sign-up:
// the first one already made the account.
var errNoLongerWaiting = errors.New("the sign-up is no longer waiting")

// parentRecord is who agreed, and from where.
type parentRecord struct {
	name, relation         string
	birthYear              int
	ip, country, userAgent string
}

// makeConsentedAccount makes the account a parent agreed to, with the
// record of their consent, and takes the sign-up out of waiting: all of
// it, or none. It returns the new account's id and the secret for the
// parent's "close this account" link.
func makeConsentedAccount(r *http.Request, p *pendingSignup, who parentRecord) (string, string, error) {
	withdrawSecret, withdrawHash, err := newLinkSecret()
	if err != nil {
		return "", "", err
	}
	tx, err := db.BeginTx(r.Context(), nil)
	if err != nil {
		return "", "", err
	}
	defer func() { _ = tx.Rollback() }()

	// Taken out of waiting first, and only once: two clicks on "I agree"
	// make one account.
	res, err := tx.ExecContext(r.Context(), `DELETE FROM pending_signups WHERE id = $1`, p.id)
	if err != nil {
		return "", "", err
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return "", "", errNoLongerWaiting
	}
	email := sql.NullString{String: p.email, Valid: p.email != ""}
	var id string
	if err := tx.QueryRowContext(r.Context(), `
		INSERT INTO users (username, password, password_hash, full_name, email, birth_date,
		                   terms_accepted_at, wins, losses, league, country,
		                   parent_email, parent_consent_at)
		VALUES ($1, '', $2, $3, $4, $5, NOW(), 0, 0, 'Bronze', $6, $7, NOW())
		RETURNING CAST(id AS TEXT)`,
		p.username, p.passwordHash, p.fullName, email, p.birthDate, p.country, p.parentEmail).Scan(&id); err != nil {
		return "", "", err
	}
	if p.provider != "" {
		if _, err := tx.ExecContext(r.Context(), `
			INSERT INTO social_logins (provider, subject, user_id, email)
			VALUES ($1, $2, $3::int, $4)`, p.provider, p.subject, id, p.email); err != nil {
			return "", "", err
		}
	}
	if _, err := tx.ExecContext(r.Context(), `
		INSERT INTO parent_consents (user_id, parent_email, parent_name, relation, parent_birth_year,
		                             child_birth_date, country, consent_ip, consent_country,
		                             user_agent, wording, withdraw_hash)
		VALUES ($1::int, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`,
		id, p.parentEmail, who.name, who.relation, who.birthYear, p.birthDate, p.country,
		who.ip, who.country, who.userAgent, consentWording, withdrawHash); err != nil {
		return "", "", err
	}
	if err := tx.Commit(); err != nil {
		return "", "", err
	}
	return id, withdrawSecret, nil
}

// ─── the parent changes their mind ──────────────────────────────────────

// consentByWithdrawToken is the account a "close this account" link is
// for: its id and username, or "" when the link is not one.
func consentByWithdrawToken(token string) (id, username string, err error) {
	token = strings.TrimSpace(token)
	if token == "" {
		return "", "", nil
	}
	err = db.QueryRow(`
		SELECT u.id::text, u.username
		  FROM parent_consents pc JOIN users u ON u.id = pc.user_id
		 WHERE pc.withdraw_hash = $1`, hashLinkSecret(token)).Scan(&id, &username)
	if errors.Is(err, sql.ErrNoRows) {
		return "", "", nil
	}
	return id, username, err
}

// ParentWithdrawPageHandler — GET /parent-consent/withdraw?token=…: asks
// the parent to confirm before anything is deleted.
func ParentWithdrawPageHandler(w http.ResponseWriter, r *http.Request) {
	if db == nil {
		http.Error(w, "service unavailable", http.StatusServiceUnavailable)
		return
	}
	token := r.URL.Query().Get("token")
	id, username, err := consentByWithdrawToken(token)
	if queryFailed("reading the account a parent's close link is for", "the parent is told to try again", err) {
		http.Error(w, "temporarily unavailable", http.StatusInternalServerError)
		return
	}
	if id == "" {
		writeParentPage(w, http.StatusNotFound, parentPage{State: "invalid"})
		return
	}
	writeParentPage(w, http.StatusOK, parentPage{State: "withdraw", Username: username, Token: token})
}

// ParentWithdrawHandler — POST /parent-consent/withdraw: the parent takes
// their consent back. The account is hidden and signed out at once, and
// deleted for good, with everything in it — the same delete as an account
// whose owner deleted it, without the 30 days: the young person cannot
// keep an account their parent has closed.
func ParentWithdrawHandler(w http.ResponseWriter, r *http.Request) {
	if db == nil {
		http.Error(w, "service unavailable", http.StatusServiceUnavailable)
		return
	}
	if !allowAction("ip:"+clientIP(r), "parent_answer") {
		http.Error(w, "Too many tries. Please wait a few minutes.", http.StatusTooManyRequests)
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Error(w, "the form could not be read", http.StatusBadRequest)
		return
	}
	id, username, err := consentByWithdrawToken(r.PostFormValue("token"))
	if queryFailed("reading the account a parent is closing", "the parent is told to try again", err) {
		http.Error(w, "temporarily unavailable", http.StatusInternalServerError)
		return
	}
	if id == "" {
		writeParentPage(w, http.StatusNotFound, parentPage{State: "invalid"})
		return
	}
	if r.PostFormValue("confirm") != "yes" {
		writeParentPage(w, http.StatusBadRequest, parentPage{
			State: "withdraw", Username: username, Token: r.PostFormValue("token"),
			Problem: "Tick the box to confirm."})
		return
	}
	if _, err := requestAccountDeletion(r.Context(), id); err != nil {
		log.Printf("parent consent: account %s could not be closed for its parent: %v", id, err)
		http.Error(w, "temporarily unavailable", http.StatusInternalServerError)
		return
	}
	if err := purgeAccount(id); err != nil {
		// Hidden and signed out already; the hourly job would only delete
		// it after 30 days, and signing in could keep it. Say so loudly.
		log.Printf("parent consent: account %s (@%s) is hidden and signed out for its parent, but deleting "+
			"it for good FAILED: %v — delete it by hand", id, username, err)
		http.Error(w, "The account is closed, but we could not finish deleting it. Please write to "+
			supportEmail+".", http.StatusInternalServerError)
		return
	}
	log.Printf("parent consent: a parent took their consent back; account %s (@%s) is deleted for good", id, username)
	writeParentPage(w, http.StatusOK, parentPage{State: "withdrawn", Username: username})
}

// isUniqueViolation is whether [err] is the database refusing a second
// row with the same username, email or sign-in, and which.
func isUniqueViolation(err error) (constraint string, ok bool) {
	var pqErr *pq.Error
	if errors.As(err, &pqErr) && pqErr.Code == "23505" {
		return pqErr.Constraint, true
	}
	return "", false
}

var parentTmpl = template.Must(template.New("parent").Parse(`<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="robots" content="noindex">
<title>A parent's permission · Flexete</title>
<style>` + legalStyle + `
form{margin-top:18px}
label{display:block;font-weight:600;margin:16px 0 6px}
input[type=text],input[type=number],select{width:100%;padding:12px;border:1px solid var(--line);border-radius:10px;background:var(--card);color:var(--text);font:inherit}
.check{display:flex;gap:10px;align-items:flex-start;margin:18px 0;font-weight:400}
.check input{margin-top:5px;flex:none}
.buttons{display:flex;gap:10px;flex-wrap:wrap;margin-top:8px}
button{font:inherit;font-weight:700;padding:13px 18px;border-radius:12px;border:1px solid var(--line);background:var(--card);color:var(--text);cursor:pointer}
button.yes{background:var(--accent);border-color:var(--accent);color:#fff}
button.danger{background:#c62828;border-color:#c62828;color:#fff}
.problem{background:#fdecea;color:#8a1c14;border-radius:10px;padding:10px 14px;margin:14px 0}
.who{background:var(--card);border:1px solid var(--line);border-radius:14px;padding:14px 18px;margin:16px 0}
</style></head>
<body><main>
<a class="brand" href="/terms"><b>f</b>Flexete</a>
{{if eq .State "ask"}}
<h1>Your permission is needed</h1>
<div class="who"><strong>{{.Name}}</strong> (@{{.Username}}) wants to join Flexete.<br>
They told us they were born on {{.Born}}, which makes them {{.Age}}.</div>
<p>Flexete is an app where people post short videos and challenge each other to video battles. Because they are under 18, they can only join if their parent or legal guardian agrees.</p>
<h2>What Flexete keeps about them</h2>
<ul>
<li>Their name, username, email and date of birth.</li>
<li>The videos, photos, comments and messages they post or send.</li>
<li>What they watch, like and follow, which is used to choose the videos they are shown.</li>
<li>The country they are in, and which phone they use.</li>
</ul>
<p>Nothing is sold, and there are no advertisements. The full details are in the <a href="/privacy">Privacy Policy</a>, the <a href="/terms">Terms of Service</a> and the <a href="/community-guidelines">Community Guidelines</a>.</p>
<h2>What you can do afterwards</h2>
<p>You can close their account and delete everything in it at any time, with a link we email you. You can also ask us what we keep about them, or to correct it: {{.Support}}.</p>
{{if .Problem}}<p class="problem">{{.Problem}}</p>{{end}}
<form method="post" action="/parent-consent">
<input type="hidden" name="token" value="{{.Token}}">
<label for="name">Your full name</label>
<input id="name" type="text" name="name" maxlength="80" autocomplete="name" value="{{.ParentName}}">
<label for="relation">You are their</label>
<select id="relation" name="relation">
<option value="">Choose…</option>
<option value="parent"{{if eq .Relation "parent"}} selected{{end}}>Parent</option>
<option value="guardian"{{if eq .Relation "guardian"}} selected{{end}}>Legal guardian</option>
</select>
<label for="birthYear">The year you were born</label>
<input id="birthYear" type="number" name="birthYear" min="1900" max="2100" inputmode="numeric" value="{{.BirthYear}}">
<label class="check"><input type="checkbox" name="confirm" value="yes">
<span>I am this young person's parent or legal guardian, I am 18 or older, and I agree to them having a Flexete account and to Flexete keeping and using their information as the Privacy Policy describes.</span></label>
<div class="buttons">
<button class="yes" type="submit" name="answer" value="yes">I agree</button>
<button type="submit" name="answer" value="no" formnovalidate>I do not agree</button>
</div>
</form>
{{else if eq .State "done"}}
<h1>Thank you</h1>
<p>@{{.Username}} can now sign in to Flexete.</p>
<p>You can change your mind at any time. This link closes their account and deletes everything in it, for good:</p>
<p class="who"><a href="{{.WithdrawLink}}">Close @{{.Username}}'s account</a></p>
<p>We have emailed you the same link. Please keep that email.</p>
{{else if eq .State "declined"}}
<h1>No account was made</h1>
<p>You did not agree, so @{{.Username}} cannot join Flexete. The details they gave us are deleted within a day.</p>
{{else if eq .State "taken"}}
<h1>The account could not be made</h1>
<p>Thank you for answering. While we waited, somebody else took the username @{{.Username}} or the email address it was to use, so this account cannot be made.</p>
<p>Please ask the young person to sign up again. You will get a new email.</p>
{{else if eq .State "withdraw"}}
<h1>Close @{{.Username}}'s account?</h1>
<p>This takes back your permission. Their account is closed straight away and deleted for good: their profile, videos, comments, messages, likes and followers. It cannot be undone.</p>
{{if .Problem}}<p class="problem">{{.Problem}}</p>{{end}}
<form method="post" action="/parent-consent/withdraw">
<input type="hidden" name="token" value="{{.Token}}">
<label class="check"><input type="checkbox" name="confirm" value="yes">
<span>Yes, close the account and delete everything in it.</span></label>
<div class="buttons"><button class="danger" type="submit">Close and delete the account</button></div>
</form>
{{else if eq .State "withdrawn"}}
<h1>The account is closed</h1>
<p>@{{.Username}}'s account is closed and everything in it is deleted.</p>
{{else}}
<h1>This link no longer works</h1>
<p>It may have been used already, replaced by a newer email, or be more than 7 days old.</p>
<p>If you already answered, there is nothing more to do. Otherwise, ask the young person to sign in to Flexete and send the email again.</p>
{{end}}
<nav>
<a href="/privacy">Privacy Policy</a>
<a href="/terms">Terms of Service</a>
<a href="mailto:{{.Support}}">{{.Support}}</a>
</nav>
</main></body></html>`))
