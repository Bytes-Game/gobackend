package main

// Forgot password (password_reset.go), through the real handlers against a
// real database, with a real email service on this machine: the email is
// sent over SMTP the way it is to a real one, and read back from there.

import (
	"bufio"
	"bytes"
	"encoding/json"
	"log"
	"net"
	"net/http/httptest"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"
)

// smtpStandIn is an email service on this machine, spoken to exactly as the
// server speaks to a real one, minus TLS and signing in. Every email it
// takes arrives on the channel.
func smtpStandIn(t *testing.T) <-chan string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	mails := make(chan string, 16)
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go serveSMTP(conn, mails)
		}
	}()
	host, port, _ := net.SplitHostPort(ln.Addr().String())
	t.Setenv("SMTP_HOST", host)
	t.Setenv("SMTP_PORT", port)
	t.Setenv("SMTP_USERNAME", "")
	t.Setenv("SMTP_PASSWORD", "")
	t.Setenv("MAIL_FROM", "Flex <no-reply@flex.test>")
	return mails
}

func serveSMTP(conn net.Conn, mails chan<- string) {
	defer conn.Close()
	r := bufio.NewReader(conn)
	say := func(s string) { _, _ = conn.Write([]byte(s + "\r\n")) }
	say("220 standin ESMTP")
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			return
		}
		cmd := strings.ToUpper(strings.TrimSpace(line))
		switch {
		case strings.HasPrefix(cmd, "EHLO"), strings.HasPrefix(cmd, "HELO"):
			say("250-standin")
			say("250 8BITMIME")
		case cmd == "DATA":
			say("354 go ahead")
			var b strings.Builder
			for {
				l, err := r.ReadString('\n')
				if err != nil {
					return
				}
				if l == ".\r\n" {
					break
				}
				b.WriteString(l)
			}
			mails <- b.String()
			say("250 queued")
		case cmd == "QUIT":
			say("221 bye")
			return
		default: // MAIL FROM, RCPT TO, NOOP, RSET
			say("250 OK")
		}
	}
}

func nextMail(t *testing.T, mails <-chan string) string {
	t.Helper()
	select {
	case m := <-mails:
		return m
	case <-time.After(3 * time.Second):
		t.Fatal("no email was sent")
		return ""
	}
}

func noMail(t *testing.T, mails <-chan string) {
	t.Helper()
	select {
	case m := <-mails:
		t.Errorf("an email was sent when none should be:\n%s", m)
	case <-time.After(200 * time.Millisecond):
	}
}

var codeInSubject = regexp.MustCompile(`(?m)^Subject: (\d{6}) is your Flex password reset code`)

func codeIn(t *testing.T, mail string) string {
	t.Helper()
	m := codeInSubject.FindStringSubmatch(mail)
	if m == nil {
		t.Fatalf("no code in the email's subject:\n%s", mail)
		return ""
	}
	return m[1]
}

func postJSON(h func(w *httptest.ResponseRecorder, body []byte), body map[string]any) (int, map[string]any) {
	raw, _ := json.Marshal(body)
	rec := httptest.NewRecorder()
	h(rec, raw)
	var got map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &got)
	return rec.Code, got
}

func forgot(t *testing.T, login string) (int, map[string]any) {
	t.Helper()
	return postJSON(func(w *httptest.ResponseRecorder, raw []byte) {
		ForgotPasswordHandler(w, httptest.NewRequest("POST", "/password/forgot", bytes.NewReader(raw)))
	}, map[string]any{"login": login})
}

func resetWith(t *testing.T, login, code, password string) (int, map[string]any) {
	t.Helper()
	return postJSON(func(w *httptest.ResponseRecorder, raw []byte) {
		ResetPasswordHandler(w, httptest.NewRequest("POST", "/password/reset", bytes.NewReader(raw)))
	}, map[string]any{"login": login, "code": code, "newPassword": password})
}

// resetter is somebody who forgot their password, with an email on file.
func resetter(t *testing.T, id int, name, email string) func() {
	t.Helper()
	done := leaver(t, id, name, "oldpass42go")
	mustExec(t, `UPDATE users SET email = $1 WHERE id = $2`, email, id)
	return done
}

func TestPasswordReset_ByUsername_ACodeByEmailThenANewPassword(t *testing.T) {
	defer withDB(t)()
	resetRedis(t)
	resetActionLimiters(t)
	mails := smtpStandIn(t)
	defer resetter(t, 9901, "Reset_Me", "reset.me@example.com")()
	before := signedInAt(t, "9901", "Reset_Me", time.Now().Add(-time.Hour))
	if _, err := parseToken(before); err != nil { // and this server now remembers it works
		t.Fatalf("the sign-in does not work even before the reset: %v", err)
	}

	code, got := forgot(t, "reset_me") // other capitals: usernames are one name
	if code != 200 || got["sent"] != true || got["to"] != "r•••e@example.com" {
		t.Fatalf("asking by username answered %d %v", code, got)
	}
	mail := nextMail(t, mails)
	if !strings.Contains(mail, "To: <reset.me@example.com>") || !strings.Contains(mail, "Hi @Reset_Me") ||
		!strings.Contains(mail, "Reply-To: "+supportEmail) {
		t.Errorf("the email is not addressed to the account as it should be:\n%s", mail)
	}
	sent := codeIn(t, mail)
	if !strings.Contains(mail, "    "+sent+"\r\n") {
		t.Errorf("the code is in the subject but not the body:\n%s", mail)
	}

	if code, got := resetWith(t, "Reset_Me", "000000", "newpass42go"); code != 400 || got["field"] != "code" {
		t.Errorf("a wrong code answered %d %v", code, got)
	}
	// The right code with a password that is not allowed: said, and the
	// code still works to fix it.
	if code, got := resetWith(t, "Reset_Me", sent, "short"); code != 400 || got["field"] != "newPassword" {
		t.Errorf("a password that is not allowed answered %d %v", code, got)
	}
	code, got = resetWith(t, "Reset_Me", sent, "newpass42go")
	if code != 200 || got["reset"] != true || got["username"] != "Reset_Me" {
		t.Fatalf("the right code with a good password answered %d %v", code, got)
	}
	if got["token"] != nil {
		t.Error("the reset signed somebody in: sign-in must go through the normal sign-in")
	}

	if _, err := parseToken(before); err == nil {
		t.Error("a sign-in from before the reset still works")
	}
	if code, _ := signInAs(t, "Reset_Me", "newpass42go", false); code != 200 {
		t.Errorf("signing in with the new password answered %d", code)
	}
	if code, _ := signInAs(t, "Reset_Me", "oldpass42go", false); code != 401 {
		t.Errorf("signing in with the old password answered %d", code)
	}
	if code, got := resetWith(t, "Reset_Me", sent, "another42go"); code != 400 || got["expired"] != true {
		t.Errorf("using the code a second time answered %d %v", code, got)
	}
}

func TestPasswordReset_AnEmailIsNeverConfirmedOrDenied(t *testing.T) {
	defer withDB(t)()
	resetRedis(t)
	resetActionLimiters(t)
	mails := smtpStandIn(t)
	defer resetter(t, 9902, "by_email", "by.email@example.com")()

	code, got := forgot(t, "nobody.here@example.com")
	if code != 200 || got["sent"] != true || got["to"] != nil {
		t.Errorf("an email nobody uses answered %d %v", code, got)
	}
	noMail(t, mails)

	code, got = forgot(t, "By.Email@Example.com")
	if code != 200 || got["sent"] != true || got["to"] != nil {
		t.Errorf("an email an account uses answered %d %v, want exactly what the other got", code, got)
	}
	sent := codeIn(t, nextMail(t, mails))
	if code, got := resetWith(t, "by.email@example.com", sent, "newpass42go"); code != 200 {
		t.Errorf("resetting by email answered %d %v", code, got)
	}
}

func TestPasswordReset_AUsernameIsAnsweredPlainly(t *testing.T) {
	defer withDB(t)()
	resetRedis(t)
	resetActionLimiters(t)
	mails := smtpStandIn(t)
	defer leaver(t, 9903, "no_email_here", "oldpass42go")()

	if code, got := forgot(t, "nobody_has_this"); code != 404 || got["field"] != "login" {
		t.Errorf("a username nobody has answered %d %v", code, got)
	}
	code, got := forgot(t, "no_email_here")
	words, _ := got["error"].(string)
	if code != 409 || got["noEmail"] != true || !strings.Contains(words, supportEmail) {
		t.Errorf("an account with no email answered %d %v", code, got)
	}
	noMail(t, mails)
}

func TestPasswordReset_ACodeRunsOut(t *testing.T) {
	defer withDB(t)()
	resetRedis(t)
	resetActionLimiters(t)
	mails := smtpStandIn(t)
	defer resetter(t, 9904, "runs_out", "runs.out@example.com")()

	forgot(t, "runs_out")
	sent := codeIn(t, nextMail(t, mails))
	for i := 1; i <= 4; i++ {
		if code, got := resetWith(t, "runs_out", "999999", "newpass42go"); code != 400 || got["expired"] == true {
			t.Fatalf("wrong try %d answered %d %v", i, code, got)
		}
	}
	if code, got := resetWith(t, "runs_out", "999999", "newpass42go"); code != 400 || got["expired"] != true {
		t.Errorf("the fifth wrong try answered %d %v, want the code spent", code, got)
	}
	if code, got := resetWith(t, "runs_out", sent, "newpass42go"); code != 400 || got["expired"] != true {
		t.Errorf("the right code after five wrong ones answered %d %v", code, got)
	}

	// A new one, and fifteen minutes go by.
	mustExec(t, `DELETE FROM password_reset_codes WHERE user_id = 9904`)
	forgot(t, "runs_out")
	sent = codeIn(t, nextMail(t, mails))
	mustExec(t, `UPDATE password_reset_codes SET expires_at = NOW() - INTERVAL '1 second' WHERE user_id = 9904`)
	if code, got := resetWith(t, "runs_out", sent, "newpass42go"); code != 400 || got["expired"] != true {
		t.Errorf("a code past its fifteen minutes answered %d %v", code, got)
	}
}

func TestPasswordReset_OneCodeAMinuteAndFiveAnHour(t *testing.T) {
	defer withDB(t)()
	resetRedis(t)
	resetActionLimiters(t)
	mails := smtpStandIn(t)
	defer resetter(t, 9905, "many_codes", "many.codes@example.com")()
	aMinuteOn := func() {
		mustExec(t, `UPDATE password_reset_codes SET sent_at = sent_at - INTERVAL '61 seconds' WHERE user_id = 9905`)
	}

	forgot(t, "many_codes")
	first := codeIn(t, nextMail(t, mails))
	code, got := forgot(t, "many_codes")
	if code != 429 || got["retryAfter"] == nil {
		t.Errorf("asking again at once answered %d %v", code, got)
	}
	noMail(t, mails)

	aMinuteOn()
	forgot(t, "many_codes")
	second := codeIn(t, nextMail(t, mails))
	if first != second {
		if code, _ := resetWith(t, "many_codes", first, "newpass42go"); code != 400 {
			t.Errorf("the first code still works after a second was sent (%d)", code)
		}
	}
	for i := 3; i <= 5; i++ {
		aMinuteOn()
		if code, _ := forgot(t, "many_codes"); code != 200 {
			t.Fatalf("code %d in an hour answered %d", i, code)
		}
		nextMail(t, mails)
	}
	aMinuteOn()
	if code, _ := forgot(t, "many_codes"); code != 429 {
		t.Errorf("a sixth code in an hour answered %d", code)
	}
	noMail(t, mails)
}

func TestPasswordReset_WithoutEmailSetUpItSaysSo(t *testing.T) {
	defer withDB(t)()
	resetRedis(t)
	resetActionLimiters(t)
	smtpStandIn(t)
	t.Setenv("SMTP_HOST", "")
	defer resetter(t, 9906, "no_service", "no.service@example.com")()
	for _, login := range []string{"no_service", "nobody.here@example.com"} {
		code, got := forgot(t, login)
		words, _ := got["error"].(string)
		if code != 503 || got["unavailable"] != true || !strings.Contains(words, supportEmail) {
			t.Errorf("with no email service, asking as %q answered %d %v", login, code, got)
		}
	}
}

func TestPasswordReset_AnEmailThatWillNotSendSaysSoAndCanBeAskedAgain(t *testing.T) {
	defer withDB(t)()
	resetRedis(t)
	resetActionLimiters(t)
	mails := smtpStandIn(t)
	defer resetter(t, 9907, "will_not_send", "will.not.send@example.com")()
	port := os.Getenv("SMTP_PORT")
	t.Setenv("SMTP_PORT", "1") // nothing answers there

	if code, got := forgot(t, "will_not_send"); code != 503 || got["unavailable"] != true {
		t.Errorf("an email that would not send answered %d %v", code, got)
	}
	t.Setenv("SMTP_PORT", port)
	if code, got := forgot(t, "will_not_send"); code != 200 {
		t.Errorf("asking again once email works answered %d %v: the unsent code is in the way", code, got)
	}
	nextMail(t, mails)
}

// Only a bcrypt hash of the code is kept, and the log never shows it.
func TestPasswordReset_TheCodeIsNeverKept(t *testing.T) {
	defer withDB(t)()
	resetRedis(t)
	resetActionLimiters(t)
	mails := smtpStandIn(t)
	defer resetter(t, 9908, "keeps_nothing", "keeps.nothing@example.com")()
	var logged bytes.Buffer
	log.SetOutput(&logged)
	defer log.SetOutput(os.Stderr)

	forgot(t, "keeps_nothing")
	sent := codeIn(t, nextMail(t, mails))
	var hash string
	if err := db.QueryRow(`SELECT code_hash FROM password_reset_codes WHERE user_id = 9908`).Scan(&hash); err != nil {
		t.Fatal(err)
	}
	if hash == sent || !strings.HasPrefix(hash, "$2") || !checkPassword(hash, sent) {
		t.Errorf("the kept code is %q, want a bcrypt hash of it", hash)
	}
	if strings.Contains(logged.String(), sent) {
		t.Error("the code is in the server's log")
	}
}

func TestPasswordReset_EmailsAreShownSoOnlyTheirOwnerKnowsThem(t *testing.T) {
	for in, want := range map[string]string{
		"karan.nagpal@gmail.com": "k•••l@gmail.com",
		"ab@x.io":                "a•••@x.io",
		"a@x.io":                 "a•••@x.io",
		"no-at-sign":             "your email",
	} {
		if got := maskEmail(in); got != want {
			t.Errorf("maskEmail(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestPasswordReset_MainWiresTheRoutes(t *testing.T) {
	src, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	code := regexp.MustCompile(`(?m)^\s*//.*$`).ReplaceAllString(string(src), "")
	for _, want := range []string{
		`"/password/forgot", ForgotPasswordHandler`,
		`"/password/reset", ResetPasswordHandler`,
	} {
		if !strings.Contains(code, want) {
			t.Errorf("main.go does not wire %s", want)
		}
	}
}
