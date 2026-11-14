package main

// A parent's consent for a young person — through the real handlers,
// against real Postgres, with a stand-in for the email service.

import (
	"bytes"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	jwt "github.com/golang-jwt/jwt/v5"
	"github.com/gorilla/mux"
)

// bornYearsAgo is a date of birth that makes somebody [years] old today.
func bornYearsAgo(years int) string {
	return time.Now().AddDate(-years, 0, -1).Format("2006-01-02")
}

// youngSignup is a good sign-up by a 15-year-old who names a parent.
func youngSignup(n int) map[string]any {
	return map[string]any{
		"username":    "teen" + strconv.Itoa(n),
		"password":    "sunset42",
		"fullName":    "Young Person",
		"email":       "teen" + strconv.Itoa(n) + "@example.com",
		"birthDate":   bornYearsAgo(15),
		"acceptTerms": true,
		"parentEmail": "Parent" + strconv.Itoa(n) + "@Example.com",
	}
}

// parentSetup is what every test here starts from: a clean database, clean
// limits, and email going to a stand-in.
func parentSetup(t *testing.T) (emails <-chan resendEmail, answer func(int, string), done func()) {
	t.Helper()
	done = withDB(t)
	resetRedis(t)
	resetActionLimiters(t)
	emails, answer = resendStandIn(t)
	return emails, answer, done
}

var parentLinkRe = regexp.MustCompile(`https?://[^\s]+/parent-consent\?token=([A-Za-z0-9_-]+)`)
var withdrawLinkRe = regexp.MustCompile(`https?://[^\s"<]+/parent-consent/withdraw\?token=([A-Za-z0-9_-]+)`)

// emailTo is the address an email went to, and its words.
func emailTo(e resendEmail) (to, subject, text string) {
	if list, ok := e.body["to"].([]any); ok && len(list) == 1 {
		to, _ = list[0].(string)
	}
	subject, _ = e.body["subject"].(string)
	text, _ = e.body["text"].(string)
	return to, subject, text
}

// consentToken is the secret in the link an email to a parent carries.
func consentToken(t *testing.T, e resendEmail) string {
	t.Helper()
	_, _, text := emailTo(e)
	m := parentLinkRe.FindStringSubmatch(text)
	if m == nil {
		t.Fatalf("the email to the parent has no link: %q", text)
	}
	return m[1]
}

// parentGet opens one of the parent's pages.
func parentGet(t *testing.T, h http.HandlerFunc, path, token string) (int, string) {
	t.Helper()
	r := httptest.NewRequest("GET", path+"?token="+url.QueryEscape(token), nil)
	w := httptest.NewRecorder()
	h(w, r)
	return w.Code, w.Body.String()
}

// parentPost sends one of the parent's forms, as a browser does.
func parentPost(t *testing.T, h http.HandlerFunc, path string, form url.Values) (int, string) {
	t.Helper()
	r := httptest.NewRequest("POST", path, strings.NewReader(form.Encode()))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	r.Header.Set("X-Forwarded-For", "198.51.100.7")
	r.Header.Set("CF-IPCountry", "IN")
	r.Header.Set("User-Agent", "A Parent's Browser")
	w := httptest.NewRecorder()
	h(w, r)
	return w.Code, w.Body.String()
}

// agreeForm is a parent's good "I agree".
func agreeForm(token string) url.Values {
	return url.Values{
		"token": {token}, "answer": {"yes"}, "name": {"  Asha   Rao "}, "relation": {"parent"},
		"birthYear": {"1980"}, "confirm": {"yes"},
	}
}

// parentEmailAgain sends POST /signup/parent.
func parentEmailAgain(t *testing.T, body map[string]any) (int, map[string]any) {
	t.Helper()
	raw, _ := json.Marshal(body)
	r := httptest.NewRequest("POST", "/signup/parent", bytes.NewReader(raw))
	r.Header.Set("X-Forwarded-For", "203.0.113.50")
	w := httptest.NewRecorder()
	ParentEmailHandler(w, r)
	var got map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &got)
	return w.Code, got
}

func countRows(t *testing.T, query string, args ...any) int {
	t.Helper()
	var n int
	if err := db.QueryRow(query, args...).Scan(&n); err != nil {
		t.Fatalf("%s: %v", strings.Join(strings.Fields(query), " "), err)
	}
	return n
}

// The whole of it: no account until the parent agrees, and then one, with
// a record of who agreed.
func TestParentConsent_NoAccountUntilAParentAgrees(t *testing.T) {
	emails, _, done := parentSetup(t)
	defer done()

	raw, _ := json.Marshal(youngSignup(1))
	r := httptest.NewRequest("POST", "/signup", bytes.NewReader(raw))
	r.Header.Set("X-Forwarded-For", "203.0.113.1")
	r.Header.Set("CF-IPCountry", "IN")
	// The sender may say the request came to any address at all. The link
	// in the email is to the server's own address, from its settings.
	r.Host = "evil.example"
	t.Setenv("PUBLIC_BASE_URL", "https://api.flexete.test/")
	w := httptest.NewRecorder()
	SignupHandler(w, r)
	var got map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &got)
	if w.Code != http.StatusAccepted || got["parentConsentPending"] != true {
		t.Fatalf("a 15-year-old's sign-up answered %d %v", w.Code, got)
	}
	if _, signedIn := got["token"]; signedIn {
		t.Fatal("a 15-year-old was signed in before any parent agreed")
	}
	ticket, _ := got["ticket"].(string)
	if ticket == "" || got["parentEmail"] == "parent1@example.com" || !strings.Contains(got["parentEmail"].(string), "@") {
		t.Fatalf("the answer should carry a ticket and the parent's address with most of it hidden: %v", got)
	}

	// Nothing that can sign in or be seen exists yet.
	if n := countRows(t, `SELECT COUNT(*) FROM users WHERE username = 'teen1'`); n != 0 {
		t.Fatal("an account was made before the parent agreed")
	}
	req := nextResendEmail(t, emails)
	to, subject, text := emailTo(req)
	if to != "parent1@example.com" || !strings.Contains(subject, "permission") {
		t.Fatalf("the email went to %q with subject %q", to, subject)
	}
	for _, want := range []string{"@teen1", "Young Person", "Flexete", "7 days", "https://api.flexete.test/parent-consent?token="} {
		if !strings.Contains(text, want) {
			t.Errorf("the email to the parent does not say %q:\n%s", want, text)
		}
	}
	token := consentToken(t, req)
	// The secret itself is never kept, only its SHA-256.
	var kept, country string
	if err := db.QueryRow(`SELECT token_hash, country FROM pending_signups WHERE username = 'teen1'`).
		Scan(&kept, &country); err != nil {
		t.Fatal(err)
	}
	if kept == token || kept != hashLinkSecret(token) {
		t.Error("the link's secret is kept as it is, not as its SHA-256")
	}
	if country != "IN" {
		t.Errorf("the country the sign-up came from is kept as %q, want IN", country)
	}

	// Signing in: the right password is told what it is waiting for; a
	// wrong one learns nothing.
	code, said := signInAs(t, "teen1", "sunset42", false)
	if code != http.StatusForbidden || said["parentConsentPending"] != true || said["ticket"] == "" ||
		!strings.Contains(said["error"].(string), "parent") {
		t.Fatalf("signing in while waiting answered %d %v", code, said)
	}
	if code, said := signInAs(t, "teen1@example.com", "sunset42", false); code != http.StatusForbidden || said["parentConsentPending"] != true {
		t.Errorf("signing in by email while waiting answered %d %v", code, said)
	}
	if code, said := signInAs(t, "teen1", "wrong-password", false); code != http.StatusUnauthorized || said["parentConsentPending"] != nil {
		t.Errorf("a wrong password while waiting answered %d %v", code, said)
	}

	// The name and the email are held meanwhile.
	rr := httptest.NewRequest("GET", "/signup/available?username=Teen1", nil)
	ww := httptest.NewRecorder()
	UsernameAvailableHandler(ww, rr)
	if !strings.Contains(ww.Body.String(), `"available":false`) {
		t.Errorf("the username of a waiting sign-up is offered to somebody else: %s", ww.Body.String())
	}
	other := goodSignup(7)
	other["username"] = "teen1"
	if code, said := signUp(t, other, "203.0.113.9"); code != http.StatusConflict || said["field"] != "username" {
		t.Errorf("somebody else took the waiting username: %d %v", code, said)
	}
	other = goodSignup(8)
	other["email"] = "TEEN1@example.com"
	if code, said := signUp(t, other, "203.0.113.9"); code != http.StatusConflict || said["field"] != "email" {
		t.Errorf("somebody else took the waiting email: %d %v", code, said)
	}

	// The parent's page.
	if code, _ := parentGet(t, ParentConsentPageHandler, "/parent-consent", "not-a-real-token"); code != http.StatusNotFound {
		t.Errorf("a made-up link opened the page: %d", code)
	}
	code, page := parentGet(t, ParentConsentPageHandler, "/parent-consent", token)
	if code != http.StatusOK || !strings.Contains(page, "@teen1") || !strings.Contains(page, "Young Person") ||
		!strings.Contains(page, "I agree") || !strings.Contains(page, "I do not agree") {
		t.Fatalf("the parent's page: %d\n%s", code, page)
	}
	if !strings.Contains(page, "makes them 15") {
		t.Errorf("the page does not say how old the young person is")
	}

	// Agreeing needs a name, a relation, an adult's year of birth and the
	// tick. Each one missing makes no account.
	for what, change := range map[string]func(url.Values){
		"no name":             func(f url.Values) { f.Set("name", " ") },
		"no relation":         func(f url.Values) { f.Del("relation") },
		"a made-up relation":  func(f url.Values) { f.Set("relation", "friend") },
		"no year of birth":    func(f url.Values) { f.Set("birthYear", "") },
		"a child's year":      func(f url.Values) { f.Set("birthYear", strconv.Itoa(time.Now().Year()-12)) },
		"a 17-year-old":       func(f url.Values) { f.Set("birthYear", strconv.Itoa(time.Now().Year()-17)) },
		"the box not ticked":  func(f url.Values) { f.Del("confirm") },
		"a year in the 1800s": func(f url.Values) { f.Set("birthYear", "1890") },
	} {
		form := agreeForm(token)
		change(form)
		if code, page := parentPost(t, ParentConsentAnswerHandler, "/parent-consent", form); code != http.StatusBadRequest ||
			!strings.Contains(page, `class="problem"`) {
			t.Errorf("%s: answered %d", what, code)
		}
		if n := countRows(t, `SELECT COUNT(*) FROM users WHERE username = 'teen1'`); n != 0 {
			t.Fatalf("%s: an account was made", what)
		}
	}

	code, page = parentPost(t, ParentConsentAnswerHandler, "/parent-consent", agreeForm(token))
	if code != http.StatusOK || !strings.Contains(page, "can now sign in") {
		t.Fatalf("the parent agreeing answered %d\n%s", code, page)
	}
	// The link that takes the consent back is on the page, and in an email.
	onPage := withdrawLinkRe.FindStringSubmatch(page)
	if onPage == nil {
		t.Fatalf("the page after agreeing does not show the link that closes the account:\n%s", page)
	}
	confirm := nextResendEmail(t, emails)
	to, _, text = emailTo(confirm)
	inEmail := withdrawLinkRe.FindStringSubmatch(text)
	if to != "parent1@example.com" || inEmail == nil || inEmail[1] != onPage[1] {
		t.Fatalf("the confirmation went to %q and says:\n%s", to, text)
	}

	// The account, and the record of the consent.
	var id int
	var hash, plain, email, parentEmail, userCountry string
	var born time.Time
	var consentAt, terms *time.Time
	if err := db.QueryRow(`SELECT id, password_hash, password, email, birth_date, country, parent_email,
		                             parent_consent_at, terms_accepted_at
		                        FROM users WHERE username = 'teen1'`).
		Scan(&id, &hash, &plain, &email, &born, &userCountry, &parentEmail, &consentAt, &terms); err != nil {
		t.Fatalf("no account after the parent agreed: %v", err)
	}
	if !strings.HasPrefix(hash, "$2") || plain != "" || email != "teen1@example.com" ||
		born.Format("2006-01-02") != bornYearsAgo(15) || userCountry != "IN" ||
		parentEmail != "parent1@example.com" || consentAt == nil || terms == nil {
		t.Errorf("the account: hash %q plain %q email %q born %v country %q parent %q consent %v terms %v",
			hash[:4], plain, email, born, userCountry, parentEmail, consentAt, terms)
	}
	var name, relation, wording, ip, consentCountry, agent, withdrawHash string
	var year int
	if err := db.QueryRow(`SELECT parent_name, relation, parent_birth_year, wording, consent_ip,
		                             consent_country, user_agent, withdraw_hash
		                        FROM parent_consents WHERE user_id = $1`, id).
		Scan(&name, &relation, &year, &wording, &ip, &consentCountry, &agent, &withdrawHash); err != nil {
		t.Fatalf("no record of the consent: %v", err)
	}
	if name != "Asha Rao" || relation != "parent" || year != 1980 || wording != consentWording ||
		ip != "198.51.100.7" || consentCountry != "IN" || agent != "A Parent's Browser" {
		t.Errorf("the record: %q %q %d %q %q %q %q", name, relation, year, wording, ip, consentCountry, agent)
	}
	if withdrawHash != hashLinkSecret(onPage[1]) || withdrawHash == onPage[1] {
		t.Error("the close-the-account secret is not kept as its SHA-256")
	}
	if n := countRows(t, `SELECT COUNT(*) FROM pending_signups`); n != 0 {
		t.Errorf("%d sign-up(s) still waiting after the parent agreed", n)
	}

	// Now they can sign in, and the link cannot be used twice.
	if code, said := signInAs(t, "teen1", "sunset42", false); code != http.StatusOK || said["token"] == nil {
		t.Fatalf("signing in after the parent agreed answered %d %v", code, said)
	}
	if code, _ := parentPost(t, ParentConsentAnswerHandler, "/parent-consent", agreeForm(token)); code != http.StatusNotFound {
		t.Errorf("the same link agreed a second time: %d", code)
	}
	if n := countRows(t, `SELECT COUNT(*) FROM users WHERE username = 'teen1'`); n != 1 {
		t.Errorf("%d accounts called teen1", n)
	}
}

// An adult is never sent to a parent, and somebody under the minimum age
// is turned away whatever parent they name.
func TestParentConsent_OnlyTheYoungAreAsked(t *testing.T) {
	emails, _, done := parentSetup(t)
	defer done()

	adult := goodSignup(1)
	adult["parentEmail"] = "somebody@example.com" // sent or not, it is not used
	if code, got := signUp(t, adult, "203.0.113.1"); code != http.StatusCreated || got["token"] == nil {
		t.Fatalf("an adult's sign-up answered %d %v", code, got)
	}
	child := youngSignup(2)
	child["birthDate"] = bornYearsAgo(12)
	if code, got := signUp(t, child, "203.0.113.1"); code != http.StatusBadRequest || got["field"] != "birthDate" {
		t.Fatalf("a 12-year-old's sign-up answered %d %v", code, got)
	}
	// 17 needs a parent; the day they are 18 they do not.
	almost := youngSignup(3)
	almost["birthDate"] = bornYearsAgo(17)
	almost["parentEmail"] = ""
	if code, got := signUp(t, almost, "203.0.113.1"); code != http.StatusBadRequest || got["field"] != "parentEmail" {
		t.Fatalf("a 17-year-old with no parent named answered %d %v", code, got)
	}
	of18 := youngSignup(4)
	of18["birthDate"] = time.Now().AddDate(-18, 0, 0).Format("2006-01-02")
	of18["parentEmail"] = ""
	if code, got := signUp(t, of18, "203.0.113.1"); code != http.StatusCreated {
		t.Fatalf("somebody 18 today answered %d %v", code, got)
	}
	select {
	case e := <-emails:
		t.Fatalf("an email went out though nobody needed a parent: %v", e.body)
	case <-time.After(150 * time.Millisecond):
	}
	if n := countRows(t, `SELECT COUNT(*) FROM pending_signups`); n != 0 {
		t.Errorf("%d sign-up(s) waiting though nobody needed a parent", n)
	}

	// Where the law says 16 (age_rules.go), a 15-year-old cannot join even
	// with a parent; a 16-year-old can, with one.
	raw, _ := json.Marshal(youngSignup(5))
	r := httptest.NewRequest("POST", "/signup", bytes.NewReader(raw))
	r.Header.Set("X-Forwarded-For", "203.0.113.2")
	r.Header.Set("CF-IPCountry", "AU")
	w := httptest.NewRecorder()
	SignupHandler(w, r)
	if w.Code != http.StatusBadRequest || !strings.Contains(w.Body.String(), "16 or older") {
		t.Fatalf("a 15-year-old in Australia answered %d %s", w.Code, w.Body.String())
	}
	sixteen := youngSignup(6)
	sixteen["birthDate"] = bornYearsAgo(16)
	raw, _ = json.Marshal(sixteen)
	r = httptest.NewRequest("POST", "/signup", bytes.NewReader(raw))
	r.Header.Set("X-Forwarded-For", "203.0.113.2")
	r.Header.Set("CF-IPCountry", "AU")
	w = httptest.NewRecorder()
	SignupHandler(w, r)
	if w.Code != http.StatusAccepted {
		t.Fatalf("a 16-year-old in Australia with a parent named answered %d %s", w.Code, w.Body.String())
	}
}

// "I agree" pressed twice at the same moment makes one account. Both
// presses read the waiting sign-up before either finishes; the second to
// finish must do nothing.
func TestParentConsent_TwoPressesMakeOneAccount(t *testing.T) {
	emails, _, done := parentSetup(t)
	defer done()
	if code, got := signUp(t, youngSignup(1), "203.0.113.1"); code != http.StatusAccepted {
		t.Fatalf("%d %v", code, got)
	}
	token := consentToken(t, nextResendEmail(t, emails))
	waiting, err := pendingByToken(token)
	if err != nil || waiting == nil {
		t.Fatalf("the waiting sign-up: %v %v", waiting, err)
	}
	r := httptest.NewRequest("POST", "/parent-consent", nil)
	who := parentRecord{name: "Asha Rao", relation: "parent", birthYear: 1980}
	first, _, err := makeConsentedAccount(r, waiting, who)
	if err != nil || first == "" {
		t.Fatalf("the first press: %q %v", first, err)
	}
	second, secret, err := makeConsentedAccount(r, waiting, who)
	if !errors.Is(err, errNoLongerWaiting) || second != "" || secret != "" {
		t.Fatalf("the second press: account %q, secret %q, %v — want nothing, and to be told it is no longer waiting",
			second, secret, err)
	}
	if n := countRows(t, `SELECT COUNT(*) FROM users WHERE username = 'teen1'`); n != 1 {
		t.Fatalf("%d accounts called teen1", n)
	}
	if n := countRows(t, `SELECT COUNT(*) FROM parent_consents`); n != 1 {
		t.Fatalf("%d consent records", n)
	}
	// The second press is told the link is finished, not that something
	// went wrong.
	status, page := consentFailurePage(err, waiting, askPage(waiting, token))
	if status != http.StatusNotFound || page.State != "invalid" {
		t.Errorf("the second press is shown %d %q, want the page for a link that is finished", status, page.State)
	}
	// And a fault that is the server's own is said as one, with the form
	// still there to send again.
	status, page = consentFailurePage(errors.New("the database went away"), waiting, askPage(waiting, token))
	if status != http.StatusInternalServerError || page.State != "ask" || page.Problem == "" || page.Token != token {
		t.Errorf("a fault is shown as %d %+v", status, page)
	}
}

// It cannot happen through the app, which holds a waiting sign-up's name:
// but if the name is somebody's by the time the parent agrees, the parent
// is told plainly, nothing is made, and the sign-up ends.
func TestParentConsent_ANameTakenMeanwhileIsSaidPlainly(t *testing.T) {
	emails, _, done := parentSetup(t)
	defer done()
	if code, got := signUp(t, youngSignup(1), "203.0.113.1"); code != http.StatusAccepted {
		t.Fatalf("%d %v", code, got)
	}
	token := consentToken(t, nextResendEmail(t, emails))
	mustExec(t, `INSERT INTO users (id, username, password) VALUES (9990, 'teen1', 'x')`)

	code, page := parentPost(t, ParentConsentAnswerHandler, "/parent-consent", agreeForm(token))
	if code != http.StatusConflict || !strings.Contains(page, "could not be made") || !strings.Contains(page, "sign up again") {
		t.Fatalf("agreeing to a name somebody took answered %d\n%s", code, page)
	}
	if n := countRows(t, `SELECT COUNT(*) FROM users WHERE username = 'teen1'`); n != 1 {
		t.Errorf("%d accounts called teen1, want only the one that took the name", n)
	}
	if n := countRows(t, `SELECT COUNT(*) FROM parent_consents`); n != 0 {
		t.Error("a consent was recorded for an account that was not made")
	}
	if n := countRows(t, `SELECT COUNT(*) FROM pending_signups`); n != 0 {
		t.Error("the sign-up that can never succeed is still waiting")
	}
}

// The parent says no: no account, the young person is told, and what they
// gave is deleted within a day.
func TestParentConsent_TheParentSaysNo(t *testing.T) {
	emails, _, done := parentSetup(t)
	defer done()
	if code, got := signUp(t, youngSignup(1), "203.0.113.1"); code != http.StatusAccepted {
		t.Fatalf("%d %v", code, got)
	}
	token := consentToken(t, nextResendEmail(t, emails))

	// "I do not agree" needs nothing else filled in.
	code, page := parentPost(t, ParentConsentAnswerHandler, "/parent-consent",
		url.Values{"token": {token}, "answer": {"no"}})
	if code != http.StatusOK || !strings.Contains(page, "No account was made") {
		t.Fatalf("the parent saying no answered %d\n%s", code, page)
	}
	if n := countRows(t, `SELECT COUNT(*) FROM users WHERE username = 'teen1'`); n != 0 {
		t.Fatal("an account was made though the parent said no")
	}
	// The link is finished: it cannot be turned into a yes.
	if code, _ := parentPost(t, ParentConsentAnswerHandler, "/parent-consent", agreeForm(token)); code != http.StatusNotFound {
		t.Errorf("a no was turned into a yes with the same link: %d", code)
	}
	if n := countRows(t, `SELECT COUNT(*) FROM users WHERE username = 'teen1'`); n != 0 {
		t.Fatal("an account was made after a no")
	}
	code, said := signInAs(t, "teen1", "sunset42", false)
	if code != http.StatusForbidden || said["declined"] != true || !strings.Contains(said["error"].(string), "said no") {
		t.Fatalf("signing in after a no answered %d %v", code, said)
	}
	// Kept a day at most, then deleted by the hourly job.
	var left time.Duration
	var expires time.Time
	if err := db.QueryRow(`SELECT expires_at FROM pending_signups WHERE username = 'teen1'`).Scan(&expires); err != nil {
		t.Fatal(err)
	}
	if left = time.Until(expires); left > parentDeclinedKeep+time.Minute || left < parentDeclinedKeep-time.Hour {
		t.Errorf("a refused sign-up is kept %v, want about %v", left, parentDeclinedKeep)
	}
	mustExec(t, `UPDATE pending_signups SET expires_at = NOW() - INTERVAL '1 minute' WHERE username = 'teen1'`)
	accountPurgeRound(t.Context())
	if n := countRows(t, `SELECT COUNT(*) FROM pending_signups`); n != 0 {
		t.Fatal("the hourly job left a refused sign-up whose day was up")
	}
	if code, _ := signInAs(t, "teen1", "sunset42", false); code != http.StatusUnauthorized {
		t.Errorf("signing in after everything was deleted answered %d", code)
	}
}

// After a no, the young person can ask again (a button pressed by
// mistake, or the wrong address the first time). Nobody else can take the
// name in the meantime.
func TestParentConsent_AskingAgainAfterANo(t *testing.T) {
	emails, _, done := parentSetup(t)
	defer done()
	if code, got := signUp(t, youngSignup(1), "203.0.113.1"); code != http.StatusAccepted {
		t.Fatalf("%d %v", code, got)
	}
	token := consentToken(t, nextResendEmail(t, emails))
	if code, _ := parentPost(t, ParentConsentAnswerHandler, "/parent-consent",
		url.Values{"token": {token}, "answer": {"no"}}); code != http.StatusOK {
		t.Fatalf("the no answered %d", code)
	}

	// Somebody else, an adult, whose account would be made on the spot:
	// the same name with their own email, then the same email under
	// another name. Neither is theirs to take while the day lasts.
	stranger := goodSignup(5)
	stranger["username"] = "teen1"
	if code, got := signUp(t, stranger, "203.0.113.9"); code != http.StatusConflict || got["field"] != "username" {
		t.Fatalf("somebody else took the name of a refused sign-up: %d %v", code, got)
	}
	stranger = goodSignup(6)
	stranger["email"] = "teen1@example.com"
	if code, got := signUp(t, stranger, "203.0.113.9"); code != http.StatusConflict || got["field"] != "email" {
		t.Fatalf("somebody else took the email of a refused sign-up: %d %v", code, got)
	}

	again := youngSignup(1)
	again["parentEmail"] = "other.parent@example.com"
	code, got := signUp(t, again, "203.0.113.1")
	if code != http.StatusAccepted || got["declined"] != nil {
		t.Fatalf("asking again after a no answered %d %v", code, got)
	}
	if to, _, _ := emailTo(nextResendEmail(t, emails)); to != "other.parent@example.com" {
		t.Errorf("the new email went to %q", to)
	}
	if n := countRows(t, `SELECT COUNT(*) FROM pending_signups WHERE declined_at IS NULL`); n != 1 {
		t.Errorf("%d sign-ups waiting, want the 1 new one", n)
	}
	if n := countRows(t, `SELECT COUNT(*) FROM pending_signups`); n != 1 {
		t.Errorf("%d rows kept, want the refused one replaced", n)
	}
}

// No answer in seven days: the link stops working, the name is free again,
// and the hourly job deletes what was given.
func TestParentConsent_NoAnswerInSevenDays(t *testing.T) {
	emails, _, done := parentSetup(t)
	defer done()
	if code, got := signUp(t, youngSignup(1), "203.0.113.1"); code != http.StatusAccepted {
		t.Fatalf("%d %v", code, got)
	}
	token := consentToken(t, nextResendEmail(t, emails))
	var expires time.Time
	if err := db.QueryRow(`SELECT expires_at FROM pending_signups WHERE username = 'teen1'`).Scan(&expires); err != nil {
		t.Fatal(err)
	}
	if left := time.Until(expires); left > parentConsentWait+time.Minute || left < parentConsentWait-time.Hour {
		t.Errorf("a sign-up waits %v for a parent, want about %v", left, parentConsentWait)
	}

	// The eighth day.
	mustExec(t, `UPDATE pending_signups SET expires_at = NOW() - INTERVAL '1 minute'`)
	if code, _ := parentGet(t, ParentConsentPageHandler, "/parent-consent", token); code != http.StatusNotFound {
		t.Errorf("the link still opens after seven days: %d", code)
	}
	if code, _ := parentPost(t, ParentConsentAnswerHandler, "/parent-consent", agreeForm(token)); code != http.StatusNotFound {
		t.Errorf("the link still agrees after seven days: %d", code)
	}
	if n := countRows(t, `SELECT COUNT(*) FROM users WHERE username = 'teen1'`); n != 0 {
		t.Fatal("an account was made from a link that ran out")
	}
	if code, said := signInAs(t, "teen1", "sunset42", false); code != http.StatusUnauthorized || said["parentConsentPending"] != nil {
		t.Errorf("signing in after it ran out answered %d %v", code, said)
	}
	if usernameTaken("teen1") {
		t.Error("the username is still held after the sign-up ran out")
	}
	// Still in the table until the hourly job comes round — and then not.
	if n := countRows(t, `SELECT COUNT(*) FROM pending_signups`); n != 1 {
		t.Fatalf("%d rows before the hourly job, want the 1 that ran out", n)
	}
	accountPurgeRound(t.Context())
	if n := countRows(t, `SELECT COUNT(*) FROM pending_signups`); n != 0 {
		t.Fatal("the hourly job left a sign-up whose seven days were up")
	}
}

// The young person can have the email sent again, to the same address or a
// corrected one, a few times and not too fast — and can cancel.
func TestParentConsent_SendingAgainChangingTheAddressAndCancelling(t *testing.T) {
	emails, answer, done := parentSetup(t)
	defer done()
	code, got := signUp(t, youngSignup(1), "203.0.113.1")
	if code != http.StatusAccepted {
		t.Fatalf("%d %v", code, got)
	}
	ticket := got["ticket"].(string)
	first := consentToken(t, nextResendEmail(t, emails))

	// Not straight away.
	if code, said := parentEmailAgain(t, map[string]any{"ticket": ticket}); code != http.StatusTooManyRequests || said["retryAfter"] == nil {
		t.Fatalf("a second email at once answered %d %v", code, said)
	}
	later := func() {
		mustExec(t, `UPDATE pending_signups SET last_email_at = NOW() - INTERVAL '2 minutes'`)
	}

	// Their own address is not a parent's.
	later()
	if code, said := parentEmailAgain(t, map[string]any{"ticket": ticket, "parentEmail": "Teen1@Example.com"}); code != http.StatusBadRequest || said["field"] != "parentEmail" {
		t.Fatalf("their own address as the parent's answered %d %v", code, said)
	}

	// The email service is down: nothing changes. The first link still
	// works and the try is not counted.
	answer(500, `{"name":"internal_server_error","message":"down"}`)
	if code, said := parentEmailAgain(t, map[string]any{"ticket": ticket, "parentEmail": "other@example.com"}); code != http.StatusServiceUnavailable || said["unavailable"] != true {
		t.Fatalf("with the email service down it answered %d %v", code, said)
	}
	<-emails // the try that failed
	answer(200, `{"id":"x"}`)
	var sent int
	var parent string
	if err := db.QueryRow(`SELECT emails_sent, parent_email FROM pending_signups`).Scan(&sent, &parent); err != nil {
		t.Fatal(err)
	}
	if sent != 1 || parent != "parent1@example.com" {
		t.Errorf("after an email that did not go: %d sent, parent %q — want 1 and the first address", sent, parent)
	}
	if code, _ := parentGet(t, ParentConsentPageHandler, "/parent-consent", first); code != http.StatusOK {
		t.Errorf("the first link died though no new one went out: %d", code)
	}

	// A corrected address: the email goes there, with a new link, and the
	// old link is dead.
	later()
	code, said := parentEmailAgain(t, map[string]any{"ticket": ticket, "parentEmail": " Mum@Example.com "})
	if code != http.StatusOK || said["parentConsentPending"] != true || !strings.HasSuffix(said["parentEmail"].(string), "@example.com") {
		t.Fatalf("changing the parent's address answered %d %v", code, said)
	}
	again := nextResendEmail(t, emails)
	if to, _, _ := emailTo(again); to != "mum@example.com" {
		t.Fatalf("the new email went to %q", to)
	}
	second := consentToken(t, again)
	if second == first {
		t.Fatal("the new email carries the old link")
	}
	if code, _ := parentGet(t, ParentConsentPageHandler, "/parent-consent", first); code != http.StatusNotFound {
		t.Errorf("the link sent to the wrong address still works: %d", code)
	}
	if code, _ := parentGet(t, ParentConsentPageHandler, "/parent-consent", second); code != http.StatusOK {
		t.Errorf("the new link does not open: %d", code)
	}

	// Five emails for one sign-up, and no more.
	for i := 3; i <= parentEmailsMax; i++ {
		later()
		if code, said := parentEmailAgain(t, map[string]any{"ticket": ticket}); code != http.StatusOK {
			t.Fatalf("email %d answered %d %v", i, code, said)
		}
		<-emails
	}
	later()
	if code, said := parentEmailAgain(t, map[string]any{"ticket": ticket}); code != http.StatusTooManyRequests || said["tooMany"] != true {
		t.Fatalf("email %d answered %d %v", parentEmailsMax+1, code, said)
	}

	// A ticket is needed, and a made-up one is not one.
	resetActionLimiters(t)
	if code, said := parentEmailAgain(t, map[string]any{"ticket": "nope"}); code != http.StatusUnauthorized || said["expired"] != true {
		t.Errorf("a made-up ticket answered %d %v", code, said)
	}
	// Nothing else this server signs is taken as one. Two things each
	// stop that on their own: the ticket has its own key, and says what
	// it is for. So: something signed with the right key that is for
	// something else, and something for the right thing signed with the
	// key that sign-ins use.
	var pendingID int
	if err := db.QueryRow(`SELECT id FROM pending_signups`).Scan(&pendingID); err != nil {
		t.Fatal(err)
	}
	ticketKey, err := pendingTicketKey()
	if err != nil {
		t.Fatal(err)
	}
	signInKey, err := authSecret()
	if err != nil {
		t.Fatal(err)
	}
	forged := func(key []byte, audience string) string {
		tok, err := jwt.NewWithClaims(jwt.SigningMethodHS256, pendingTicketClaims{
			RegisteredClaims: jwt.RegisteredClaims{
				Subject:   strconv.Itoa(pendingID),
				Audience:  jwt.ClaimStrings{audience},
				ExpiresAt: jwt.NewNumericDate(time.Now().Add(10 * time.Minute)),
			},
		}).SignedString(key)
		if err != nil {
			t.Fatal(err)
		}
		return tok
	}
	later()
	for what, tok := range map[string]string{
		"the right key, but made for something else":     forged(ticketKey, signupTicketAudience),
		"made for this, but signed with the sign-in key": forged(signInKey, pendingTicketAudience),
		"an account's own sign-in":                       testToken(t, "1", "creator"),
	} {
		if code, said := parentEmailAgain(t, map[string]any{"ticket": tok, "cancel": true}); code != http.StatusUnauthorized {
			t.Errorf("%s was taken as a ticket: %d %v", what, code, said)
		}
	}
	if n := countRows(t, `SELECT COUNT(*) FROM pending_signups`); n != 1 {
		t.Fatal("something that is not a ticket cancelled the sign-up")
	}
	// And the real thing, made the same way, is taken: the check above is
	// not passing because nothing ever is.
	if code, said := parentEmailAgain(t, map[string]any{"ticket": forged(ticketKey, pendingTicketAudience)}); code == http.StatusUnauthorized {
		t.Fatalf("a real ticket was refused: %d %v", code, said)
	}

	// Cancelling deletes it at once.
	if code, said := parentEmailAgain(t, map[string]any{"ticket": ticket, "cancel": true}); code != http.StatusOK || said["cancelled"] != true {
		t.Fatalf("cancelling answered %d %v", code, said)
	}
	if n := countRows(t, `SELECT COUNT(*) FROM pending_signups`); n != 0 {
		t.Fatal("a cancelled sign-up is still kept")
	}
	if code, _ := parentGet(t, ParentConsentPageHandler, "/parent-consent", second); code != http.StatusNotFound {
		t.Errorf("the parent's link still opens after the young person cancelled: %d", code)
	}
	if code, said := parentEmailAgain(t, map[string]any{"ticket": ticket}); code != http.StatusGone || said["gone"] != true {
		t.Errorf("sending again after cancelling answered %d %v", code, said)
	}
}

// Signing up again is how a wrong parent's address is put right: the same
// person's earlier try makes way. Nobody else's does.
func TestParentConsent_SigningUpAgainReplacesOnlyYourOwn(t *testing.T) {
	emails, _, done := parentSetup(t)
	defer done()
	if code, got := signUp(t, youngSignup(1), "203.0.113.1"); code != http.StatusAccepted {
		t.Fatalf("%d %v", code, got)
	}
	first := consentToken(t, nextResendEmail(t, emails))

	// Somebody else — a different password — cannot push it out, by the
	// username or by the email.
	thief := youngSignup(1)
	thief["password"] = "another-password9"
	thief["parentEmail"] = "thief@example.com"
	if code, got := signUp(t, thief, "203.0.113.66"); code != http.StatusConflict || got["field"] != "username" {
		t.Fatalf("somebody else replaced a waiting sign-up by its username: %d %v", code, got)
	}
	thief["username"] = "someoneelse"
	if code, got := signUp(t, thief, "203.0.113.66"); code != http.StatusConflict || got["field"] != "email" {
		t.Fatalf("somebody else replaced a waiting sign-up by its email: %d %v", code, got)
	}
	if code, _ := parentGet(t, ParentConsentPageHandler, "/parent-consent", first); code != http.StatusOK {
		t.Fatal("the first sign-up's link died though nobody replaced it")
	}

	// The same person, with the parent's address put right.
	fixed := youngSignup(1)
	fixed["parentEmail"] = "right.parent@example.com"
	if code, got := signUp(t, fixed, "203.0.113.1"); code != http.StatusAccepted {
		t.Fatalf("the same person signing up again answered %d %v", code, got)
	}
	again := nextResendEmail(t, emails)
	if to, _, _ := emailTo(again); to != "right.parent@example.com" {
		t.Fatalf("the new email went to %q", to)
	}
	if n := countRows(t, `SELECT COUNT(*) FROM pending_signups`); n != 1 {
		t.Fatalf("%d sign-ups waiting, want the 1 that replaced the other", n)
	}
	if code, _ := parentGet(t, ParentConsentPageHandler, "/parent-consent", first); code != http.StatusNotFound {
		t.Error("the replaced sign-up's link still works")
	}
	if code, _ := parentGet(t, ParentConsentPageHandler, "/parent-consent", consentToken(t, again)); code != http.StatusOK {
		t.Error("the new sign-up's link does not open")
	}
}

// The parent changes their mind: the account is closed and deleted for
// good, at once.
func TestParentConsent_TheParentTakesItBack(t *testing.T) {
	emails, _, done := parentSetup(t)
	defer done()
	if code, got := signUp(t, youngSignup(1), "203.0.113.1"); code != http.StatusAccepted {
		t.Fatalf("%d %v", code, got)
	}
	token := consentToken(t, nextResendEmail(t, emails))
	if code, page := parentPost(t, ParentConsentAnswerHandler, "/parent-consent", agreeForm(token)); code != http.StatusOK {
		t.Fatalf("%d %s", code, page)
	}
	_, _, text := emailTo(nextResendEmail(t, emails))
	m := withdrawLinkRe.FindStringSubmatch(text)
	if m == nil {
		t.Fatalf("no close-the-account link in the confirmation:\n%s", text)
	}
	withdraw := m[1]
	code, said := signInAs(t, "teen1", "sunset42", false)
	session, _ := said["token"].(string)
	if code != http.StatusOK || session == "" {
		t.Fatalf("%d %v", code, said)
	}

	// The link asks first, and a made-up one opens nothing.
	if code, _ := parentGet(t, ParentWithdrawPageHandler, "/parent-consent/withdraw", "made-up"); code != http.StatusNotFound {
		t.Errorf("a made-up close link opened: %d", code)
	}
	// The consent link is not a close link, and the other way round.
	if code, _ := parentGet(t, ParentWithdrawPageHandler, "/parent-consent/withdraw", token); code != http.StatusNotFound {
		t.Errorf("the consent link worked as a close link: %d", code)
	}
	code, page := parentGet(t, ParentWithdrawPageHandler, "/parent-consent/withdraw", withdraw)
	if code != http.StatusOK || !strings.Contains(page, "Close @teen1") {
		t.Fatalf("the close page: %d\n%s", code, page)
	}
	// Opening the page deletes nothing; neither does the form without the tick.
	if code, _ := parentPost(t, ParentWithdrawHandler, "/parent-consent/withdraw", url.Values{"token": {withdraw}}); code != http.StatusBadRequest {
		t.Errorf("closing without the tick answered %d", code)
	}
	if n := countRows(t, `SELECT COUNT(*) FROM users WHERE username = 'teen1'`); n != 1 {
		t.Fatal("the account went before the parent confirmed")
	}

	code, page = parentPost(t, ParentWithdrawHandler, "/parent-consent/withdraw",
		url.Values{"token": {withdraw}, "confirm": {"yes"}})
	if code != http.StatusOK || !strings.Contains(page, "is closed") {
		t.Fatalf("the parent closing the account answered %d\n%s", code, page)
	}
	if n := countRows(t, `SELECT COUNT(*) FROM users WHERE username = 'teen1'`); n != 0 {
		t.Fatal("the account is still there after the parent closed it")
	}
	if n := countRows(t, `SELECT COUNT(*) FROM parent_consents`); n != 0 {
		t.Error("the consent record outlived the account")
	}
	if code, _ := signInAs(t, "teen1", "sunset42", false); code != http.StatusUnauthorized {
		t.Errorf("signing in to a closed account answered %d", code)
	}
	// The link is finished.
	if code, _ := parentPost(t, ParentWithdrawHandler, "/parent-consent/withdraw",
		url.Values{"token": {withdraw}, "confirm": {"yes"}}); code != http.StatusNotFound {
		t.Errorf("the close link worked twice: %d", code)
	}
}

// The same through Google: no account until the parent agrees, and Google
// then signs them straight in.
func TestParentConsent_WithGoogle(t *testing.T) {
	emails, _, done := parentSetup(t)
	defer done()
	g := standInFor(t, googleProvider, "client-web")
	idToken := func() string {
		return g.token(t, jwt.MapClaims{"sub": "g-teen-1", "email": "teen.g@gmail.com",
			"email_verified": true, "name": "Teen G"})
	}
	code, got := socialPost(t, socialSignIn(googleProvider), "/auth/google", map[string]any{"idToken": idToken()})
	ticket, _ := got["signupTicket"].(string)
	if code != http.StatusOK || ticket == "" {
		t.Fatalf("%d %v", code, got)
	}
	form := map[string]any{"signupTicket": ticket, "username": "teen_g", "fullName": "Teen G",
		"birthDate": bornYearsAgo(14), "acceptTerms": true}
	if code, got := socialPost(t, SocialSignupHandler, "/auth/social/signup", form); code != http.StatusBadRequest || got["field"] != "parentEmail" {
		t.Fatalf("a 14-year-old with Google and no parent named answered %d %v", code, got)
	}
	form["parentEmail"] = "teen.g@gmail.com"
	if code, got := socialPost(t, SocialSignupHandler, "/auth/social/signup", form); code != http.StatusBadRequest || got["field"] != "parentEmail" {
		t.Fatalf("their own Google address as the parent's answered %d %v", code, got)
	}
	form["parentEmail"] = "dad@example.com"
	code, got = socialPost(t, SocialSignupHandler, "/auth/social/signup", form)
	if code != http.StatusAccepted || got["parentConsentPending"] != true || got["token"] != nil {
		t.Fatalf("a 14-year-old with Google answered %d %v", code, got)
	}
	if n := countRows(t, `SELECT COUNT(*) FROM users WHERE username = 'teen_g'`); n != 0 {
		t.Fatal("an account was made before the parent agreed")
	}
	req := nextResendEmail(t, emails)
	if to, _, _ := emailTo(req); to != "dad@example.com" {
		t.Fatalf("the email went to %q", to)
	}

	// Google again while waiting: told what it is waiting for, not signed
	// in, and not offered a second sign-up.
	code, got = socialPost(t, socialSignIn(googleProvider), "/auth/google", map[string]any{"idToken": idToken()})
	if code != http.StatusForbidden || got["parentConsentPending"] != true || got["token"] != nil || got["needsSignup"] != nil {
		t.Fatalf("Google while waiting answered %d %v", code, got)
	}

	if code, page := parentPost(t, ParentConsentAnswerHandler, "/parent-consent", agreeForm(consentToken(t, req))); code != http.StatusOK {
		t.Fatalf("%d %s", code, page)
	}
	code, got = socialPost(t, socialSignIn(googleProvider), "/auth/google", map[string]any{"idToken": idToken()})
	user, _ := got["user"].(map[string]any)
	if code != http.StatusOK || got["token"] == nil || user["username"] != "teen_g" {
		t.Fatalf("Google after the parent agreed answered %d %v", code, got)
	}
	if n := countRows(t, `SELECT COUNT(*) FROM social_logins s JOIN users u ON u.id = s.user_id
		                   WHERE u.username = 'teen_g' AND s.provider = 'google' AND s.subject = 'g-teen-1'`); n != 1 {
		t.Error("the account is not joined to the Google sign-in")
	}
	// No password was ever set, so none signs in.
	if code, _ := signInAs(t, "teen_g", "", false); code != http.StatusUnauthorized {
		t.Errorf("an empty password signed in: %d", code)
	}
}

// One parent's address gets a handful of emails a day at most, however
// many sign-ups name it and wherever they come from.
func TestParentConsent_OneAddressIsNotFlooded(t *testing.T) {
	emails, _, done := parentSetup(t)
	defer done()
	sent := 0
	for i := 1; i <= 9; i++ {
		body := youngSignup(i)
		body["parentEmail"] = "Victim@Example.com"
		code, got := signUp(t, body, "203.0.113."+strconv.Itoa(i)) // a different address each time
		switch code {
		case http.StatusAccepted:
			sent++
			<-emails
		case http.StatusTooManyRequests:
			if got["field"] != "parentEmail" {
				t.Fatalf("sign-up %d was refused, but not about the parent's address: %v", i, got)
			}
		default:
			t.Fatalf("sign-up %d answered %d %v", i, code, got)
		}
	}
	if sent != 6 {
		t.Fatalf("%d emails went to one address in a row, want 6 and then no more", sent)
	}
	if n := countRows(t, `SELECT COUNT(*) FROM pending_signups`); n != 6 {
		t.Errorf("%d sign-ups kept, want only the 6 whose parent was emailed", n)
	}
}

// With email not set up, or not going out, nothing is kept: a sign-up
// nobody was asked about would wait seven days for nothing.
func TestParentConsent_NothingIsKeptWhenTheParentCannotBeEmailed(t *testing.T) {
	_, answer, done := parentSetup(t)
	defer done()

	answer(500, `{"name":"internal_server_error","message":"down"}`)
	code, got := signUp(t, youngSignup(1), "203.0.113.1")
	if code != http.StatusServiceUnavailable || got["unavailable"] != true {
		t.Fatalf("with the email service down it answered %d %v", code, got)
	}
	if n := countRows(t, `SELECT COUNT(*) FROM pending_signups`); n != 0 {
		t.Fatal("a sign-up was kept though its parent was never emailed")
	}

	// Not set up at all: the same for the young person, and the server's
	// log says which setting is missing, so the owner can see why nobody
	// under 18 can join.
	t.Setenv("RESEND_API_KEY", "")
	t.Setenv("SMTP_HOST", "")
	var logged lockedLog
	log.SetOutput(&logged)
	code, got = signUp(t, youngSignup(2), "203.0.113.1")
	log.SetOutput(os.Stderr)
	if code != http.StatusServiceUnavailable || got["unavailable"] != true {
		t.Fatalf("with email not set up it answered %d %v", code, got)
	}
	// Trying again in a moment would not help, so they are not told to.
	if said, _ := got["error"].(string); !strings.Contains(said, "can't take sign-ups that need a parent's permission") {
		t.Errorf("with email not set up the young person is told %q", said)
	}
	if said := logged.String(); !strings.Contains(said, "email is not set up") || !strings.Contains(said, "RESEND_API_KEY") {
		t.Errorf("the log does not say that email is not set up, or what to set:\n%s", said)
	}
	if n := countRows(t, `SELECT COUNT(*) FROM pending_signups`) + countRows(t, `SELECT COUNT(*) FROM users WHERE username LIKE 'teen%'`); n != 0 {
		t.Fatal("something was kept though email is not set up")
	}
	// An adult is not held up by any of that.
	if code, got := signUp(t, goodSignup(3), "203.0.113.1"); code != http.StatusCreated {
		t.Fatalf("an adult with email not set up answered %d %v", code, got)
	}
}

// A username cannot be changed to one a waiting sign-up holds.
func TestParentConsent_AWaitingNameCannotBeTakenByAChange(t *testing.T) {
	emails, _, done := parentSetup(t)
	defer done()
	if code, got := signUp(t, youngSignup(1), "203.0.113.1"); code != http.StatusAccepted {
		t.Fatalf("%d %v", code, got)
	}
	<-emails
	code, got := signUp(t, goodSignup(2), "203.0.113.2")
	if code != http.StatusCreated {
		t.Fatalf("%d %v", code, got)
	}
	id := got["user"].(map[string]any)["id"].(string)
	raw, _ := json.Marshal(map[string]any{"username": "Teen1"})
	r := withAuth(httptest.NewRequest("PUT", "/api/v1/users/"+id+"/username", bytes.NewReader(raw)), id, "newbie2")
	r = mux.SetURLVars(r, map[string]string{"id": id})
	w := httptest.NewRecorder()
	ChangeUsernameHandler(w, r)
	if w.Code != http.StatusConflict || !strings.Contains(w.Body.String(), "taken") {
		t.Fatalf("changing to a waiting sign-up's name answered %d %s", w.Code, w.Body.String())
	}
}

// The address in an emailed link comes from the server's settings, in
// this order, and from the request only when there are none.
func TestParentConsent_LinksPointAtTheServersOwnAddress(t *testing.T) {
	r := httptest.NewRequest("GET", "/", nil)
	r.Host = "whatever.the.sender.says"
	r.Header.Set("X-Forwarded-Proto", "https")
	t.Setenv("PUBLIC_BASE_URL", "")
	t.Setenv("RENDER_EXTERNAL_URL", "")
	if got := serverAddress(r); got != "https://whatever.the.sender.says" {
		t.Errorf("with no settings: %q", got)
	}
	t.Setenv("RENDER_EXTERNAL_URL", "https://gobackend.onrender.test")
	if got := serverAddress(r); got != "https://gobackend.onrender.test" {
		t.Errorf("with Render's own setting: %q", got)
	}
	t.Setenv("PUBLIC_BASE_URL", " https://flexete.test/ ")
	if got := serverAddress(r); got != "https://flexete.test" {
		t.Errorf("with the owner's setting: %q", got)
	}
	// Something that is not an address is not used.
	t.Setenv("PUBLIC_BASE_URL", "flexete.test")
	if got := serverAddress(r); got != "https://gobackend.onrender.test" {
		t.Errorf("with a setting that is not an address: %q", got)
	}
}

// main.go has every route, and the hourly job deletes what ran out.
func TestParentConsent_IsWiredIn(t *testing.T) {
	src, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	code := regexp.MustCompile(`(?m)^\s*//.*$`).ReplaceAllString(string(src), "")
	for _, want := range []string{
		`"/signup/rules", SignupRulesHandler).Methods("GET"`,
		`"/signup/parent", ParentEmailHandler).Methods("POST"`,
		`"/parent-consent", ParentConsentPageHandler).Methods("GET")`,
		`"/parent-consent", ParentConsentAnswerHandler).Methods("POST")`,
		`"/parent-consent/withdraw", ParentWithdrawPageHandler).Methods("GET")`,
		`"/parent-consent/withdraw", ParentWithdrawHandler).Methods("POST")`,
	} {
		if !strings.Contains(code, want) {
			t.Errorf("main.go does not wire %s", want)
		}
	}
}

// The parent's pages load nothing from anywhere, run no script, and can
// only send their form back here.
func TestParentConsent_ThePagesAreLockedDown(t *testing.T) {
	emails, _, done := parentSetup(t)
	defer done()
	if code, got := signUp(t, youngSignup(1), "203.0.113.1"); code != http.StatusAccepted {
		t.Fatalf("%d %v", code, got)
	}
	token := consentToken(t, nextResendEmail(t, emails))
	r := httptest.NewRequest("GET", "/parent-consent?token="+token, nil)
	w := httptest.NewRecorder()
	ParentConsentPageHandler(w, r)
	h := w.Header()
	csp := h.Get("Content-Security-Policy")
	for _, want := range []string{"default-src 'none'", "form-action 'self'", "frame-ancestors 'none'"} {
		if !strings.Contains(csp, want) {
			t.Errorf("the page's rules do not have %q: %s", want, csp)
		}
	}
	if h.Get("Cache-Control") != "no-store" || h.Get("Referrer-Policy") != "no-referrer" {
		t.Errorf("the page may be kept or its address passed on: %v", h)
	}
	if strings.Contains(w.Body.String(), "<script") {
		t.Error("the page has a script")
	}
	// What the young person typed is shown as text, never as markup.
	mustExec(t, `UPDATE pending_signups SET full_name = '<img src=x onerror=alert(1)>'`)
	_, page := parentGet(t, ParentConsentPageHandler, "/parent-consent", token)
	if strings.Contains(page, "<img src=x") {
		t.Error("a name with markup in it is put on the page as markup")
	}
}
