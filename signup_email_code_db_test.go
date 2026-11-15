package main

// The code that checks a new account's email (signup_email_code.go),
// through the real sign-up handler, against real Postgres, with a stand-in
// for the email service.

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

// codeSetup is a clean database, clean limits, and email going to a
// stand-in, whose answer can be changed.
func codeSetup(t *testing.T) (answer func(int, string), done func()) {
	t.Helper()
	done = withDB(t)
	resetRedis(t)
	resetActionLimiters(t)
	_, answer = resendStandIn(t)
	return answer, done
}

// withEmailCode is [body] sent again with [code].
func withEmailCode(body map[string]any, code string) map[string]any {
	out := map[string]any{"emailCode": code}
	for k, v := range body {
		out[k] = v
	}
	return out
}

// codeRows is how many codes are kept for [email].
func codeRows(t *testing.T, email string) int {
	t.Helper()
	return countRows(t, `SELECT COUNT(*) FROM signup_email_codes WHERE email = $1`, email)
}

func TestSignupCode_NoAccountUntilTheCodeComesBack(t *testing.T) {
	_, done := codeSetup(t)
	defer done()

	body := goodSignup(1)
	code, got := postSignup(t, body, "203.0.113.1")
	if code != http.StatusAccepted || got["emailCodeSent"] != true || got["email"] != "new1@example.com" {
		t.Fatalf("a good sign-up without a code answered %d %v", code, got)
	}
	if _, signedIn := got["token"]; signedIn {
		t.Fatal("somebody was signed in before their email was checked")
	}
	if n := countRows(t, `SELECT COUNT(*) FROM users WHERE username = 'newbie1'`); n != 0 {
		t.Fatal("the account was made before the email was checked")
	}
	emailed := signupCodeFor(t, "new1@example.com")

	// Only a one-way hash of the code is kept.
	var hash string
	if err := db.QueryRow(`SELECT code_hash FROM signup_email_codes WHERE email = 'new1@example.com'`).
		Scan(&hash); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(hash, emailed) || !strings.HasPrefix(hash, "$2") {
		t.Fatalf("the code is not kept as a one-way hash: %q", hash)
	}

	// A wrong code: told so under the box, and still no account.
	wrong := "000000"
	if emailed == wrong {
		wrong = "111111"
	}
	code, got = postSignup(t, withEmailCode(body, wrong), "203.0.113.1")
	if code != http.StatusBadRequest || got["field"] != "emailCode" || got["error"] != "That code isn't right." {
		t.Fatalf("a wrong code answered %d %v", code, got)
	}
	if n := countRows(t, `SELECT COUNT(*) FROM users WHERE username = 'newbie1'`); n != 0 {
		t.Fatal("a wrong code made the account")
	}

	// The right one: the account, signed in, as a sign-up always was.
	code, got = postSignup(t, withEmailCode(body, emailed), "203.0.113.1")
	if code != http.StatusCreated || got["token"] == nil {
		t.Fatalf("the right code answered %d %v", code, got)
	}
	if n := countRows(t, `SELECT COUNT(*) FROM users WHERE username = 'newbie1' AND email = 'new1@example.com'`); n != 1 {
		t.Fatal("the right code made no account")
	}
	// Used up: it is gone with its account made.
	if n := codeRows(t, "new1@example.com"); n != 0 {
		t.Errorf("%d codes kept after the account was made", n)
	}
}

func TestSignupCode_EveryBoxIsCheckedBeforeAnyEmailGoes(t *testing.T) {
	_, done := codeSetup(t)
	defer done()
	if code, got := signUp(t, goodSignup(2), "203.0.113.2"); code != http.StatusCreated {
		t.Fatalf("the first account: %d %v", code, got)
	}
	// A name already taken, or a box filled wrongly: said at once, and
	// nobody is emailed a code for a sign-up that cannot go through.
	taken := goodSignup(3)
	taken["username"] = "newbie2"
	badBirthday := goodSignup(4)
	badBirthday["birthDate"] = time.Now().AddDate(-11, 0, 0).Format("2006-01-02")
	for _, body := range []map[string]any{taken, badBirthday} {
		code, got := postSignup(t, body, "203.0.113.2")
		if code == http.StatusAccepted || got["emailCodeSent"] == true || got["field"] == nil {
			t.Fatalf("%v answered %d %v", body, code, got)
		}
	}
	select {
	case e := <-standInEmails:
		to, subject, _ := emailTo(e)
		t.Fatalf("an email went to %s (%q) for a sign-up that could not go through", to, subject)
	case <-time.After(300 * time.Millisecond):
	}
	if n := countRows(t, `SELECT COUNT(*) FROM signup_email_codes`); n != 0 {
		t.Errorf("%d codes kept for sign-ups that could not go through", n)
	}
}

func TestSignupCode_FiveTriesThenItIsSpent(t *testing.T) {
	_, done := codeSetup(t)
	defer done()
	body := goodSignup(5)
	if code, got := postSignup(t, body, "203.0.113.5"); code != http.StatusAccepted {
		t.Fatalf("first step: %d %v", code, got)
	}
	emailed := signupCodeFor(t, "new5@example.com")
	wrong := fmt.Sprintf("%06d", (atoiOr(emailed)+1)%1_000_000)
	for i := 1; i <= 4; i++ {
		if code, got := postSignup(t, withEmailCode(body, wrong), "203.0.113.5"); code != http.StatusBadRequest ||
			got["field"] != "emailCode" || got["expired"] == true {
			t.Fatalf("wrong try %d answered %d %v", i, code, got)
		}
	}
	code, got := postSignup(t, withEmailCode(body, wrong), "203.0.113.5")
	if code != http.StatusBadRequest || got["expired"] != true ||
		!strings.Contains(fmt.Sprint(got["error"]), "Too many wrong codes") {
		t.Fatalf("the fifth wrong try answered %d %v", code, got)
	}
	// Even the right code does not work now: a new one must be asked for.
	code, got = postSignup(t, withEmailCode(body, emailed), "203.0.113.5")
	if code != http.StatusBadRequest || got["expired"] != true {
		t.Fatalf("the right code after five wrong ones answered %d %v", code, got)
	}
	if n := countRows(t, `SELECT COUNT(*) FROM users WHERE username = 'newbie5'`); n != 0 {
		t.Fatal("an account was made with a spent code")
	}
}

// atoiOr is [s] as a number, or 0.
func atoiOr(s string) int {
	n := 0
	for _, c := range s {
		if c < '0' || c > '9' {
			return 0
		}
		n = n*10 + int(c-'0')
	}
	return n
}

func TestSignupCode_TriesAtTheSameMomentGetNoMoreThanFive(t *testing.T) {
	_, done := codeSetup(t)
	defer done()
	body := goodSignup(6)
	if code, got := postSignup(t, body, "203.0.113.6"); code != http.StatusAccepted {
		t.Fatalf("first step: %d %v", code, got)
	}
	emailed := signupCodeFor(t, "new6@example.com")

	// Twenty wrong codes at once, from twenty addresses. Each try is
	// counted before its code is compared, so only five of them are ever
	// compared: four told "isn't right", one told it is spent. Counting
	// after comparing let every one of them through.
	var mu sync.Mutex
	notRight := 0
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			guess := fmt.Sprintf("%06d", (atoiOr(emailed)+1+i)%1_000_000)
			_, got := postSignup(t, withEmailCode(body, guess), fmt.Sprintf("198.51.100.%d", 10+i))
			if got["error"] == "That code isn't right." {
				mu.Lock()
				notRight++
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()
	if notRight > signupCodeTries-1 {
		t.Errorf("%d tries at the same moment were each compared with the code; at most %d may be",
			notRight, signupCodeTries-1)
	}
	if code, got := postSignup(t, withEmailCode(body, emailed), "203.0.113.6"); code != http.StatusBadRequest ||
		got["expired"] != true {
		t.Fatalf("the right code after twenty wrong ones answered %d %v", code, got)
	}
}

func TestSignupCode_ExpiresAfterFifteenMinutes(t *testing.T) {
	_, done := codeSetup(t)
	defer done()
	body := goodSignup(7)
	if code, got := postSignup(t, body, "203.0.113.7"); code != http.StatusAccepted {
		t.Fatalf("first step: %d %v", code, got)
	}
	emailed := signupCodeFor(t, "new7@example.com")
	var expires time.Time
	if err := db.QueryRow(`SELECT expires_at FROM signup_email_codes WHERE email = 'new7@example.com'`).
		Scan(&expires); err != nil {
		t.Fatal(err)
	}
	if left := time.Until(expires); left < 14*time.Minute || left > 15*time.Minute+time.Second {
		t.Errorf("the code works for %v, want 15 minutes", left)
	}
	if _, err := db.Exec(`UPDATE signup_email_codes SET expires_at = NOW() - INTERVAL '1 second'
	                       WHERE email = 'new7@example.com'`); err != nil {
		t.Fatal(err)
	}
	code, got := postSignup(t, withEmailCode(body, emailed), "203.0.113.7")
	if code != http.StatusBadRequest || got["expired"] != true || got["field"] != "emailCode" {
		t.Fatalf("an expired code answered %d %v", code, got)
	}
	if n := codeRows(t, "new7@example.com"); n != 0 {
		t.Error("an expired code is still kept")
	}
}

func TestSignupCode_OnceAMinuteAndAHandfulForOneAddress(t *testing.T) {
	_, done := codeSetup(t)
	defer done()
	body := goodSignup(8)
	if code, got := postSignup(t, body, "203.0.113.8"); code != http.StatusAccepted {
		t.Fatalf("first step: %d %v", code, got)
	}
	first := signupCodeFor(t, "new8@example.com")

	// Again at once: not sent, and told when it can be.
	code, got := postSignup(t, body, "203.0.113.8")
	if code != http.StatusTooManyRequests || got["retryAfter"] == nil {
		t.Fatalf("asking again at once answered %d %v", code, got)
	}

	// A minute on, a new one replaces it: the old one stops working.
	minuteOn := func() {
		t.Helper()
		if _, err := db.Exec(`UPDATE signup_email_codes SET sent_at = sent_at - INTERVAL '61 seconds'
		                       WHERE email = 'new8@example.com'`); err != nil {
			t.Fatal(err)
		}
	}
	minuteOn()
	if code, got := postSignup(t, body, "203.0.113.8"); code != http.StatusAccepted {
		t.Fatalf("asking again a minute on answered %d %v", code, got)
	}
	second := signupCodeFor(t, "new8@example.com")
	if first != second {
		if code, got := postSignup(t, withEmailCode(body, first), "203.0.113.8"); code != http.StatusBadRequest {
			t.Fatalf("the replaced code still answered %d %v", code, got)
		}
	}

	// However many sign-ups name it, a handful of codes and then no more.
	limit := actionLimitTable["signup_code_email"].burst
	for sent := 2; sent < limit; sent++ {
		minuteOn()
		if code, got := postSignup(t, body, fmt.Sprintf("192.0.2.%d", sent)); code != http.StatusAccepted {
			t.Fatalf("code %d to one address answered %d %v", sent+1, code, got)
		}
		signupCodeFor(t, "new8@example.com")
	}
	minuteOn()
	code, got = postSignup(t, body, "192.0.2.99")
	if code != http.StatusTooManyRequests || got["field"] != "email" {
		t.Fatalf("code %d to one address answered %d %v", limit+1, code, got)
	}
	// Another address is not held up by it.
	if code, got := postSignup(t, goodSignup(9), "192.0.2.99"); code != http.StatusAccepted {
		t.Fatalf("another address answered %d %v", code, got)
	}
}

func TestSignupCode_ACodeThatCouldNotBeSentIsNotKept(t *testing.T) {
	answer, done := codeSetup(t)
	defer done()
	answer(500, `{"name":"internal_server_error","message":"down"}`)
	code, got := postSignup(t, goodSignup(10), "203.0.113.10")
	if code != http.StatusServiceUnavailable || got["unavailable"] != true || got["reason"] != "notSent" {
		t.Fatalf("with the email service down it answered %d %v", code, got)
	}
	nextResendEmail(t, standInEmails) // the one that failed
	if n := codeRows(t, "new10@example.com"); n != 0 {
		t.Fatal("a code nobody received is kept")
	}
	// Nor does it stop them asking again straight away.
	answer(200, `{"id":"49a3999c-0ce1-4ea6-ab68-afcd6dc2e794"}`)
	if code, got := postSignup(t, goodSignup(10), "203.0.113.10"); code != http.StatusAccepted {
		t.Fatalf("asking again once email works answered %d %v", code, got)
	}
}

func TestSignupCode_ACodeIsForItsOwnEmailOnly(t *testing.T) {
	_, done := codeSetup(t)
	defer done()
	a, b := goodSignup(11), goodSignup(12)
	if code, got := postSignup(t, a, "203.0.113.11"); code != http.StatusAccepted {
		t.Fatalf("first step: %d %v", code, got)
	}
	codeForA := signupCodeFor(t, "new11@example.com")
	// b has asked for nothing: a's code does not make b's account.
	code, got := postSignup(t, withEmailCode(b, codeForA), "203.0.113.11")
	if code != http.StatusBadRequest || got["field"] != "emailCode" {
		t.Fatalf("somebody else's code answered %d %v", code, got)
	}
	if n := countRows(t, `SELECT COUNT(*) FROM users WHERE username = 'newbie12'`); n != 0 {
		t.Fatal("an account was made with a code sent to another address")
	}
}

func TestSignupCode_TriesDoNotUseUpSignUps(t *testing.T) {
	_, done := codeSetup(t)
	defer done()
	// Every sign-up one address may make in an hour, each with two wrong
	// tries before the right code: all of them go through.
	limit := actionLimitTable["signup"].burst
	for i := 0; i < limit; i++ {
		body := goodSignup(400 + i)
		if code, got := postSignup(t, body, "198.51.100.200"); code != http.StatusAccepted {
			t.Fatalf("sign-up %d's first step: %d %v", i+1, code, got)
		}
		email := fmt.Sprintf("new%d@example.com", 400+i)
		emailed := signupCodeFor(t, email)
		if i < 3 {
			wrong := fmt.Sprintf("%06d", (atoiOr(emailed)+1)%1_000_000)
			for j := 0; j < 2; j++ {
				if code, got := postSignup(t, withEmailCode(body, wrong), "198.51.100.200"); code != http.StatusBadRequest {
					t.Fatalf("sign-up %d's wrong try: %d %v", i+1, code, got)
				}
			}
		}
		if code, got := postSignup(t, withEmailCode(body, emailed), "198.51.100.200"); code != http.StatusCreated {
			t.Fatalf("sign-up %d with its code: %d %v", i+1, code, got)
		}
	}
	// And tries at codes have a limit of their own, per address.
	resetActionLimiters(t)
	tries := actionLimitTable["signup_code_try"].burst
	for i := 0; i < tries; i++ {
		postSignup(t, withEmailCode(goodSignup(500), "123456"), "198.51.100.201")
	}
	if code, _ := postSignup(t, withEmailCode(goodSignup(500), "123456"), "198.51.100.201"); code != http.StatusTooManyRequests {
		t.Errorf("try %d at a code from one address answered %d, want 429", tries+1, code)
	}
}

// A code nobody used goes within the hour, with the address it was sent
// to, through the hourly job itself; one still running stays.
func TestSignupCode_ACodeNobodyUsedIsDeletedByTheHourlyJob(t *testing.T) {
	_, done := codeSetup(t)
	defer done()
	for _, body := range []map[string]any{goodSignup(20), goodSignup(21)} {
		if code, got := postSignup(t, body, "203.0.113.20"); code != http.StatusAccepted {
			t.Fatalf("first step: %d %v", code, got)
		}
		signupCodeFor(t, strings.ToLower(body["email"].(string)))
	}
	mustExec(t, `UPDATE signup_email_codes SET expires_at = NOW() - INTERVAL '1 minute'
	              WHERE email = 'new20@example.com'`)
	accountPurgeRound(context.Background())
	if n := codeRows(t, "new20@example.com"); n != 0 {
		t.Error("an expired code, and the address it went to, is still kept")
	}
	if n := codeRows(t, "new21@example.com"); n != 1 {
		t.Error("a code still in its 15 minutes was deleted")
	}
}
