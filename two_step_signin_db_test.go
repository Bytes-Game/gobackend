package main

// The two-step code at sign-in (two_step_signin.go), through the real
// handlers against a real database.
//
// Each test checks something that IS there (a sign-in, a ticket, an
// answer), not only what is refused.

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	jwt "github.com/golang-jwt/jwt/v5"
	"github.com/gorilla/mux"
)

// turnOnTwoStep gives account [id] two-step verification, on, and says its
// secret and recovery codes.
func turnOnTwoStep(t *testing.T, id int) (string, []string) {
	t.Helper()
	secret, err := generateTOTPSecret()
	if err != nil {
		t.Fatal(err)
	}
	codes, hashes, err := generateRecoveryCodes(3)
	if err != nil {
		t.Fatal(err)
	}
	hashesJSON, _ := json.Marshal(hashes)
	mustExec(t, `INSERT INTO user_totp (user_id, secret, recovery_codes, is_active)
		VALUES ($1, $2, $3::jsonb, TRUE)`, id, secret, string(hashesJSON))
	return secret, codes
}

// codeAt is the authenticator app's code at [at].
func codeAt(t *testing.T, secret string, at time.Time) string {
	t.Helper()
	c, err := generateTOTPCode(secret, at)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// secondStepWith sends the ticket and [code] to the real handler.
func secondStepWith(t *testing.T, ticket, code string, keep bool) (int, map[string]any) {
	t.Helper()
	body, _ := json.Marshal(map[string]any{"ticket": ticket, "code": code, "keepAccount": keep})
	req := httptest.NewRequest("POST", "/login/2fa", bytes.NewReader(body))
	req.RemoteAddr = "203.0.113.78:4000"
	rec := httptest.NewRecorder()
	TwoFactorLoginHandler(rec, req)
	var got map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &got)
	return rec.Code, got
}

// ticketFrom signs in with the password and says the ticket the answer
// carries; the test fails if the answer is not "enter the code".
func ticketFrom(t *testing.T, username, password string) string {
	t.Helper()
	code, got := signInAs(t, username, password, false)
	ticket, _ := got["ticket"].(string)
	if code != http.StatusUnauthorized || got["twoFactorRequired"] != true || ticket == "" {
		t.Fatalf("the password should have asked for the code: %d %v", code, got)
	}
	if _, ok := got["token"]; ok {
		t.Fatalf("the password alone gave a sign-in: %v", got)
	}
	return ticket
}

// craftedTicket is a ticket as issueTicket would make it, but made at
// [issued] and good until [until].
func craftedTicket(t *testing.T, userID, stage string, issued, until time.Time) string {
	t.Helper()
	return craftedTicketTo(t, ticketAudience, userID, stage, issued, until)
}

// craftedTicketTo is craftedTicket, made out to [audience].
func craftedTicketTo(t *testing.T, audience, userID, stage string, issued, until time.Time) string {
	t.Helper()
	key, err := ticketKey()
	if err != nil {
		t.Fatal(err)
	}
	tok, err := jwt.NewWithClaims(jwt.SigningMethodHS256, ticketClaims{
		Stage: stage,
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   userID,
			Audience:  jwt.ClaimStrings{audience},
			IssuedAt:  jwt.NewNumericDate(issued),
			ExpiresAt: jwt.NewNumericDate(until),
		},
	}).SignedString(key)
	if err != nil {
		t.Fatal(err)
	}
	return tok
}

func TestTwoStepSignIn_ThePasswordAloneIsNotEnough(t *testing.T) {
	defer withDB(t)()
	defer leaver(t, 9901, "two_step_maya", "sunset42go")()
	secret, _ := turnOnTwoStep(t, 9901)

	ticket := ticketFrom(t, "two_step_maya", "sunset42go")
	if _, err := parseToken(ticket); err == nil {
		t.Fatal("the ticket works as a sign-in: it must not")
	}

	// A wrong code: no sign-in, and said under the code box.
	code, got := secondStepWith(t, ticket, "000000", false)
	if code != http.StatusUnauthorized || got["field"] != "code" || got["token"] != nil {
		t.Fatalf("a wrong code answered %d %v", code, got)
	}

	// The right code: signed in, with a sign-in that works.
	now := time.Now()
	code, got = secondStepWith(t, ticket, codeAt(t, secret, now), false)
	token, _ := got["token"].(string)
	if code != http.StatusOK || token == "" {
		t.Fatalf("the right code answered %d %v", code, got)
	}
	claims, err := parseToken(token)
	if err != nil || claims.Subject != "9901" {
		t.Fatalf("the sign-in does not work: %v %v", claims, err)
	}
	if user, _ := got["user"].(map[string]any); user["username"] != "two_step_maya" {
		t.Fatalf("signed in as %v", got["user"])
	}

	// The same code again, by somebody who watched it typed: refused.
	code, got = secondStepWith(t, ticketFrom(t, "two_step_maya", "sunset42go"), codeAt(t, secret, now), false)
	if code != http.StatusUnauthorized || got["token"] != nil {
		t.Fatalf("a used code worked again: %d %v", code, got)
	}
	// The next code from the app works.
	code, got = secondStepWith(t, ticketFrom(t, "two_step_maya", "sunset42go"),
		codeAt(t, secret, now.Add(30*time.Second)), false)
	if code != http.StatusOK || got["token"] == nil {
		t.Fatalf("the next code answered %d %v", code, got)
	}
}

func TestTwoStepSignIn_ARecoveryCodeWorksOnce(t *testing.T) {
	defer withDB(t)()
	defer leaver(t, 9902, "two_step_rec", "sunset42go")()
	_, recovery := turnOnTwoStep(t, 9902)

	code, got := secondStepWith(t, ticketFrom(t, "two_step_rec", "sunset42go"), recovery[0], false)
	if code != http.StatusOK || got["token"] == nil {
		t.Fatalf("a recovery code answered %d %v", code, got)
	}
	code, got = secondStepWith(t, ticketFrom(t, "two_step_rec", "sunset42go"), recovery[0], false)
	if code != http.StatusUnauthorized || got["token"] != nil {
		t.Fatalf("a recovery code worked twice: %d %v", code, got)
	}
	// Typed in small letters, with spaces around: still the code.
	code, got = secondStepWith(t, ticketFrom(t, "two_step_rec", "sunset42go"),
		"  "+strings.ToLower(recovery[1])+" ", false)
	if code != http.StatusOK || got["token"] == nil {
		t.Fatalf("a recovery code in small letters answered %d %v", code, got)
	}
	var left int
	if err := db.QueryRow(`SELECT jsonb_array_length(recovery_codes) FROM user_totp WHERE user_id = 9902`).
		Scan(&left); err != nil || left != 1 {
		t.Fatalf("recovery codes left: %d (%v), want 1", left, err)
	}
}

func TestTwoStepSignIn_TicketsRunOutAndAPasswordChangeCancelsThem(t *testing.T) {
	defer withDB(t)()
	defer leaver(t, 9903, "two_step_old", "sunset42go")()
	secret, _ := turnOnTwoStep(t, 9903)
	expired := func(ticket, why string) {
		t.Helper()
		code, got := secondStepWith(t, ticket, codeAt(t, secret, time.Now()), false)
		if code != http.StatusUnauthorized || got["expired"] != true || got["token"] != nil {
			t.Fatalf("%s: answered %d %v", why, code, got)
		}
	}

	expired(craftedTicket(t, "9903", stageSecondStep, time.Now().Add(-10*time.Minute), time.Now().Add(-5*time.Minute)),
		"a ticket past its five minutes")
	expired(signedInAt(t, "9903", "two_step_old", time.Now()), "a session token passed off as a ticket")
	expired("not-a-ticket", "rubbish")
	expired(craftedTicket(t, "9903", "skip-the-code", time.Now(), time.Now().Add(4*time.Minute)),
		"a ticket of a kind there is not")
	// The right key, but made out to something else. (The key alone
	// already refuses every other kind of token there is; this keeps it
	// that way if a key is ever shared.)
	expired(craftedTicketTo(t, "flex-something-else", "9903", stageSecondStep, time.Now(), time.Now().Add(4*time.Minute)),
		"a ticket made out to something else")

	// Made before the password changed: cancelled, as every sign-in is.
	old := craftedTicket(t, "9903", stageSecondStep, time.Now().Add(-time.Minute), time.Now().Add(4*time.Minute))
	// Kept to the second, as a real change is (password_reset.go, token_cutoff.go).
	mustExec(t, `UPDATE users SET password_changed_at = date_trunc('second', NOW()) WHERE id = 9903`)
	forgetCutoff("9903")
	expired(old, "a ticket from before the password changed")

	// A ticket made now still works.
	code, got := secondStepWith(t, craftedTicket(t, "9903", stageSecondStep, time.Now(), time.Now().Add(4*time.Minute)),
		codeAt(t, secret, time.Now()), false)
	if code != http.StatusOK || got["token"] == nil {
		t.Fatalf("a fresh ticket answered %d %v", code, got)
	}
}

func TestTwoStepSignIn_AnAccountWaitingToBeDeletedIsAskedOnlyAfterTheCode(t *testing.T) {
	defer withDB(t)()
	defer leaver(t, 9904, "two_step_leaver", "sunset42go")()
	secret, _ := turnOnTwoStep(t, 9904)
	mustExec(t, `UPDATE users SET deletion_requested_at = NOW() WHERE id = 9904`)
	forgetLeaving()

	// The password alone says nothing about the account leaving.
	_, got := signInAs(t, "two_step_leaver", "sunset42go", false)
	if got["deletionPending"] != nil || got["deleteOn"] != nil {
		t.Fatalf("the password alone told the account is leaving: %v", got)
	}
	// Even asked to keep it, the password alone keeps nothing.
	signInAs(t, "two_step_leaver", "sunset42go", true)
	if !waiting(t, 9904) {
		t.Fatal("the password alone kept the account")
	}

	ticket := ticketFrom(t, "two_step_leaver", "sunset42go")
	code, got := secondStepWith(t, ticket, codeAt(t, secret, time.Now()), false)
	verified, _ := got["ticket"].(string)
	if code != http.StatusConflict || got["deletionPending"] != true || verified == "" {
		t.Fatalf("after the code, answered %d %v", code, got)
	}
	if !waiting(t, 9904) {
		t.Fatal("the account was kept without being asked")
	}

	// Not keeping it: asked again, nothing changes.
	if code, _ := secondStepWith(t, verified, "", false); code != http.StatusConflict {
		t.Fatalf("not keeping it answered %d", code)
	}
	// Keeping it: no code typed again, and signed in.
	code, got = secondStepWith(t, verified, "", true)
	if code != http.StatusOK || got["token"] == nil {
		t.Fatalf("keeping it answered %d %v", code, got)
	}
	if waiting(t, 9904) {
		t.Fatal("kept, but still waiting to be deleted")
	}
}

func TestTwoStepSignIn_FiveWrongCodesAndItWaits(t *testing.T) {
	defer withDB(t)()
	defer leaver(t, 9905, "two_step_guess", "sunset42go")()
	secret, _ := turnOnTwoStep(t, 9905)
	ticket := ticketFrom(t, "two_step_guess", "sunset42go")
	for i := 0; i < 5; i++ {
		if code, _ := secondStepWith(t, ticket, "1234"+strconv.Itoa(10+i), false); code != http.StatusUnauthorized {
			t.Fatalf("wrong code %d answered %d", i+1, code)
		}
	}
	code, got := secondStepWith(t, ticket, codeAt(t, secret, time.Now()), false)
	if code != http.StatusTooManyRequests || got["token"] != nil {
		t.Fatalf("after five wrong codes, the right one answered %d %v", code, got)
	}
}

func TestTwoStepSignIn_WithoutItSigningInIsAsBefore(t *testing.T) {
	defer withDB(t)()
	defer leaver(t, 9906, "one_step_maya", "sunset42go")()
	code, got := signInAs(t, "one_step_maya", "sunset42go", false)
	if code != http.StatusOK || got["token"] == nil || got["twoFactorRequired"] != nil {
		t.Fatalf("without two-step verification, the password answered %d %v", code, got)
	}
}

// Turning it off takes each code once too: a recovery code used to sign in
// cannot turn two-step verification off afterwards.
func TestTwoStepSignIn_TurningItOffTakesEachCodeOnce(t *testing.T) {
	defer withDB(t)()
	defer leaver(t, 9907, "two_step_off", "sunset42go")()
	secret, recovery := turnOnTwoStep(t, 9907)
	if code, _ := secondStepWith(t, ticketFrom(t, "two_step_off", "sunset42go"), recovery[0], false); code != 200 {
		t.Fatalf("signing in with a recovery code answered %d", code)
	}

	r := mux.NewRouter()
	r.HandleFunc("/api/v1/users/{id}/totp/disable", authed(DisableTOTPHandler)).Methods("POST")
	srv := httptest.NewServer(r)
	defer srv.Close()
	token := signedInAt(t, "9907", "two_step_off", time.Now())
	off := func(code string) int {
		t.Helper()
		status, _ := call(t, srv, token, "POST", "/api/v1/users/9907/totp/disable", `{"code":"`+code+`"}`)
		return status
	}
	if status := off(recovery[0]); status != http.StatusUnauthorized {
		t.Fatalf("a used recovery code turned it off: %d", status)
	}
	if on, _ := twoStepOn("9907"); !on {
		t.Fatal("two-step verification went off on a used code")
	}
	atTheCode := ticketFrom(t, "two_step_off", "sunset42go")
	if status := off(codeAt(t, secret, time.Now())); status != http.StatusOK {
		t.Fatalf("a right code answered %d", status)
	}
	if on, _ := twoStepOn("9907"); on {
		t.Fatal("still on after turning it off")
	}
	// Turned off while somebody was at the code screen: their password was
	// right, and the password is all it takes now.
	if code, got := secondStepWith(t, atTheCode, "", false); code != http.StatusOK || got["token"] == nil {
		t.Fatalf("at the code screen after it was turned off: %d %v", code, got)
	}
}

// The code typed to turn two-step verification on is used up by doing so:
// somebody who watched it typed cannot sign in with it. Through the real
// setup pages, not a row put in by hand.
func TestTwoStepSignIn_TheCodeThatTurnedItOnIsUsedUp(t *testing.T) {
	defer withDB(t)()
	defer leaver(t, 9908, "two_step_new", "sunset42go")()

	r := mux.NewRouter()
	r.HandleFunc("/api/v1/users/{id}/totp/enroll", authed(EnrollTOTPHandler)).Methods("POST")
	r.HandleFunc("/api/v1/users/{id}/totp/verify", authed(VerifyTOTPHandler)).Methods("POST")
	srv := httptest.NewServer(r)
	defer srv.Close()
	token := signedInAt(t, "9908", "two_step_new", time.Now())

	status, got := call(t, srv, token, "POST", "/api/v1/users/9908/totp/enroll", `{}`)
	secret, _ := got["secret"].(string)
	if status != http.StatusOK || secret == "" {
		t.Fatalf("starting two-step verification answered %d %v", status, got)
	}
	// Started but not confirmed with a code: the password alone still signs in.
	if code, got := signInAs(t, "two_step_new", "sunset42go", false); code != http.StatusOK || got["token"] == nil {
		t.Fatalf("before the first code, the password answered %d %v", code, got)
	}

	now := time.Now()
	first := codeAt(t, secret, now)
	if status, got := call(t, srv, token, "POST", "/api/v1/users/9908/totp/verify", `{"code":"`+first+`"}`); status != http.StatusOK {
		t.Fatalf("the first code answered %d %v", status, got)
	}

	// On now: the password asks for a code, and the first one is spent.
	code, got := secondStepWith(t, ticketFrom(t, "two_step_new", "sunset42go"), first, false)
	if code != http.StatusUnauthorized || got["token"] != nil {
		t.Fatalf("the code that turned it on signed in: %d %v", code, got)
	}
	// The app's next code signs in.
	code, got = secondStepWith(t, ticketFrom(t, "two_step_new", "sunset42go"),
		codeAt(t, secret, now.Add(30*time.Second)), false)
	if code != http.StatusOK || got["token"] == nil {
		t.Fatalf("the next code answered %d %v", code, got)
	}
}

func TestTwoStepSignIn_MainWiresTheRoute(t *testing.T) {
	src, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	code := regexp.MustCompile(`(?m)^\s*//.*$`).ReplaceAllString(string(src), "")
	if !strings.Contains(code, `"/login/2fa", TwoFactorLoginHandler`) {
		t.Error("main.go does not wire /login/2fa")
	}
}
