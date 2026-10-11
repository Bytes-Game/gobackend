package main

// Sign-up, signing in by email, and changing a password — through the real
// handlers against real Postgres.

import (
	"bytes"
	"encoding/json"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/mux"
)

// goodSignup is a sign-up that passes every rule, with [n] in its name and
// email so a test can make several.
func goodSignup(n int) map[string]any {
	return map[string]any{
		"username":    "newbie" + strconv.Itoa(n),
		"password":    "sunset42",
		"fullName":    "New Person",
		"email":       "New" + strconv.Itoa(n) + "@Example.com",
		"birthDate":   "2000-05-01",
		"acceptTerms": true,
	}
}

// signUp sends [body] to POST /signup from address [ip].
func signUp(t *testing.T, body map[string]any, ip string) (int, map[string]any) {
	t.Helper()
	// The test people are made with numbers picked by hand, which the
	// database's own counter does not know about; move it past them, as it
	// would be in a database where every account came through sign-up.
	if _, err := db.Exec(`SELECT setval(pg_get_serial_sequence('users', 'id'),
		GREATEST((SELECT MAX(id) FROM users), 1))`); err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(body)
	r := httptest.NewRequest("POST", "/signup", bytes.NewReader(raw))
	if ip != "" {
		r.Header.Set("X-Forwarded-For", ip)
	}
	w := httptest.NewRecorder()
	SignupHandler(w, r)
	var got map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &got)
	return w.Code, got
}

// logIn sends POST /login and answers the status and the token.
func logIn(t *testing.T, name, password string) (int, string) {
	t.Helper()
	raw, _ := json.Marshal(map[string]string{"username": name, "password": password})
	r := httptest.NewRequest("POST", "/login", bytes.NewReader(raw))
	w := httptest.NewRecorder()
	LoginHandler(w, r)
	var got struct {
		Token string `json:"token"`
	}
	_ = json.Unmarshal(w.Body.Bytes(), &got)
	return w.Code, got.Token
}

func TestSignup_SavesTheDetailsAndSignsYouIn(t *testing.T) {
	defer withDB(t)()
	resetRedis(t)
	resetActionLimiters(t)

	code, got := signUp(t, goodSignup(1), "203.0.113.1")
	if code != http.StatusCreated {
		t.Fatalf("a good sign-up: %d %v", code, got)
	}
	tok, _ := got["token"].(string)
	user, _ := got["user"].(map[string]any)
	if tok == "" || user["username"] != "newbie1" || user["fullName"] != "New Person" {
		t.Fatalf("the answer is missing the sign-in or the person: %v", got)
	}
	if _, err := parseToken(tok); err != nil {
		t.Errorf("the sign-in it gave does not work: %v", err)
	}

	var email, hash, plain string
	var born time.Time
	var agreed *time.Time
	if err := db.QueryRow(`SELECT email, birth_date, terms_accepted_at, password_hash, password
		                     FROM users WHERE username = 'newbie1'`).
		Scan(&email, &born, &agreed, &hash, &plain); err != nil {
		t.Fatal(err)
	}
	if email != "new1@example.com" {
		t.Errorf("email saved as %q, want it in lower case", email)
	}
	if born.Format("2006-01-02") != "2000-05-01" || agreed == nil {
		t.Errorf("birthday %v, terms agreed %v", born, agreed)
	}
	if plain != "" || !strings.HasPrefix(hash, "$2") {
		t.Errorf("the password is not stored as a one-way hash only (plain %q)", plain)
	}

	// The email and birthday are private: nobody else's view of the
	// profile carries them.
	r := mux.SetURLVars(httptest.NewRequest("GET", "/api/v1/users/newbie1", nil),
		map[string]string{"username": "newbie1"})
	w := httptest.NewRecorder()
	GetUserHandler(w, r)
	if strings.Contains(w.Body.String(), "example.com") || strings.Contains(w.Body.String(), "2000-05-01") {
		t.Errorf("the public profile hands out the email or birthday: %s", w.Body.String())
	}
}

func TestSignup_EachBoxIsCheckedAndNamed(t *testing.T) {
	defer withDB(t)()
	resetRedis(t)
	for field, change := range map[string]func(map[string]any){
		"username":  func(b map[string]any) { b["username"] = "support" },
		"email":     func(b map[string]any) { b["email"] = "not-an-email" },
		"birthDate": func(b map[string]any) { b["birthDate"] = time.Now().AddDate(-12, 0, 0).Format("2006-01-02") },
		"password":  func(b map[string]any) { b["password"] = "123456" },
		"terms":     func(b map[string]any) { b["acceptTerms"] = false },
	} {
		resetActionLimiters(t)
		body := goodSignup(2)
		change(body)
		code, got := signUp(t, body, "203.0.113.2")
		if code != http.StatusBadRequest || got["field"] != field || got["error"] == "" {
			t.Errorf("a bad %s: %d %v", field, code, got)
		}
	}
	// What the old form sent — a username and a password, nothing else —
	// is not enough any more.
	resetActionLimiters(t)
	code, _ := signUp(t, map[string]any{"username": "oldform", "password": "sunset42"}, "203.0.113.2")
	if code != http.StatusBadRequest {
		t.Errorf("a sign-up with only a username and password: %d", code)
	}
	var n int
	_ = db.QueryRow(`SELECT COUNT(*) FROM users WHERE username IN ('newbie2','oldform')`).Scan(&n)
	if n != 0 {
		t.Errorf("%d refused sign-ups were stored", n)
	}
}

func TestSignup_OneAccountPerNameAndPerEmail(t *testing.T) {
	defer withDB(t)()
	resetRedis(t)
	resetActionLimiters(t)
	if code, got := signUp(t, goodSignup(3), "203.0.113.3"); code != 201 {
		t.Fatalf("first sign-up: %d %v", code, got)
	}
	again := goodSignup(3)
	again["email"] = "someone.else@example.com"
	if code, got := signUp(t, again, "203.0.113.3"); code != 409 || got["field"] != "username" {
		t.Errorf("the same username twice: %d %v", code, got)
	}
	sameEmail := goodSignup(4)
	sameEmail["email"] = "NEW3@example.COM" // the same address, other case
	if code, got := signUp(t, sameEmail, "203.0.113.3"); code != 409 || got["field"] != "email" {
		t.Errorf("the same email twice: %d %v", code, got)
	}
}

func TestSignup_LimitsPerAddressAndForEveryone(t *testing.T) {
	defer withDB(t)()
	resetRedis(t)
	resetActionLimiters(t)
	// One address: the hourly allowance, then no more.
	limit := actionLimitTable["signup"].burst
	for i := 0; i < limit; i++ {
		if code, got := signUp(t, goodSignup(100+i), "198.51.100.7"); code != 201 {
			t.Fatalf("sign-up %d from one address: %d %v", i+1, code, got)
		}
	}
	if code, _ := signUp(t, goodSignup(200), "198.51.100.7"); code != http.StatusTooManyRequests {
		t.Errorf("sign-up %d from one address: %d, want 429", limit+1, code)
	}
	if code, got := signUp(t, goodSignup(201), "198.51.100.8"); code != 201 {
		t.Errorf("another address was held up too: %d %v", code, got)
	}

	// The whole platform: a ceiling that holds whatever address is given.
	// Made small here so the test can reach it.
	was := actionLimitTable["signup_all"]
	actionLimitTable["signup_all"] = actionLimitConfig{tokensPerSecond: was.tokensPerSecond, burst: 2}
	defer func() { actionLimitTable["signup_all"] = was }()
	resetActionLimiters(t)
	for i := 0; i < 2; i++ {
		if code, got := signUp(t, goodSignup(300+i), "192.0.2."+strconv.Itoa(10+i)); code != 201 {
			t.Fatalf("platform sign-up %d: %d %v", i+1, code, got)
		}
	}
	if code, _ := signUp(t, goodSignup(310), "192.0.2.99"); code != http.StatusTooManyRequests {
		t.Errorf("past the platform ceiling from a fresh address: %d, want 429", code)
	}
}

func TestSignup_TheLiveUsernameCheckSaysWhy(t *testing.T) {
	defer withDB(t)()
	resetRedis(t)
	resetActionLimiters(t)
	if code, got := signUp(t, goodSignup(5), ""); code != 201 {
		t.Fatalf("sign-up: %d %v", code, got)
	}
	check := func(name string) map[string]any {
		w := httptest.NewRecorder()
		UsernameAvailableHandler(w, httptest.NewRequest("GET", "/signup/available?username="+name, nil))
		var got map[string]any
		_ = json.Unmarshal(w.Body.Bytes(), &got)
		return got
	}
	if got := check("fresh_name"); got["available"] != true || got["reason"] != "" {
		t.Errorf("a free name: %v", got)
	}
	if got := check("newbie5"); got["available"] != false || got["valid"] != true ||
		!strings.Contains(got["reason"].(string), "taken") {
		t.Errorf("a taken name: %v", got)
	}
	if got := check("admin"); got["valid"] != false || !strings.Contains(got["reason"].(string), "reserved") {
		t.Errorf("a reserved name: %v", got)
	}
}

func TestLogin_WithYourEmailToo(t *testing.T) {
	defer withDB(t)()
	resetRedis(t)
	resetActionLimiters(t)
	if code, got := signUp(t, goodSignup(6), ""); code != 201 {
		t.Fatalf("sign-up: %d %v", code, got)
	}
	if code, tok := logIn(t, "newbie6", "sunset42"); code != 200 || tok == "" {
		t.Fatalf("signing in with the username: %d", code)
	}
	if code, tok := logIn(t, "NEW6@example.com", "sunset42"); code != 200 || tok == "" {
		t.Errorf("signing in with the email: %d", code)
	}
	if code, _ := logIn(t, "new6@example.com", "wrong-one9"); code != http.StatusUnauthorized {
		t.Errorf("the email with a wrong password: %d", code)
	}
	if code, _ := logIn(t, "nobody@example.com", "sunset42"); code != http.StatusUnauthorized {
		t.Errorf("an email nobody uses: %d", code)
	}
}

// changePassword sends POST /api/v1/auth/password with [token], through
// the same sign-in check every signed-in address uses.
func changePassword(t *testing.T, token, current, next string) (int, map[string]any) {
	t.Helper()
	raw, _ := json.Marshal(map[string]string{"currentPassword": current, "newPassword": next})
	r := httptest.NewRequest("POST", "/api/v1/auth/password", bytes.NewReader(raw))
	r.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	authed(ChangePasswordHandler)(w, r)
	var got map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &got)
	return w.Code, got
}

func TestChangePassword_SignsOutEveryOtherPhone(t *testing.T) {
	defer withDB(t)()
	resetRedis(t)
	resetActionLimiters(t)
	if code, got := signUp(t, goodSignup(7), ""); code != 201 {
		t.Fatalf("sign-up: %d %v", code, got)
	}
	_, otherPhone := logIn(t, "newbie7", "sunset42")
	// Every sign-in made before the change must be from an earlier second.
	time.Sleep(1100 * time.Millisecond)
	_, thisPhone := logIn(t, "newbie7", "sunset42")

	if code, got := changePassword(t, thisPhone, "not-it-123", "moonrise77"); code != 403 ||
		got["field"] != "currentPassword" {
		t.Errorf("a wrong current password: %d %v", code, got)
	}
	if code, got := changePassword(t, thisPhone, "sunset42", "weak"); code != 400 ||
		got["field"] != "newPassword" {
		t.Errorf("a weak new password: %d %v", code, got)
	}
	time.Sleep(1100 * time.Millisecond)
	code, got := changePassword(t, thisPhone, "sunset42", "moonrise77")
	if code != 200 {
		t.Fatalf("changing the password: %d %v", code, got)
	}
	fresh, _ := got["token"].(string)
	if _, err := parseToken(fresh); err != nil {
		t.Errorf("the fresh sign-in does not work: %v", err)
	}
	for name, tok := range map[string]string{"the other phone": otherPhone, "this phone's old sign-in": thisPhone} {
		if _, err := parseToken(tok); err == nil {
			t.Errorf("%s still works after the change", name)
		}
	}
	// Through a real signed-in address, too.
	r := httptest.NewRequest("POST", "/api/v1/auth/refresh", nil)
	r.Header.Set("Authorization", "Bearer "+otherPhone)
	w := httptest.NewRecorder()
	authed(RefreshTokenHandler)(w, r)
	if w.Code != http.StatusUnauthorized {
		t.Errorf("the other phone renewed its sign-in after the change: %d", w.Code)
	}
	if code, _ := logIn(t, "newbie7", "sunset42"); code != http.StatusUnauthorized {
		t.Errorf("the old password still signs in: %d", code)
	}
	if code, _ := logIn(t, "newbie7", "moonrise77"); code != 200 {
		t.Errorf("the new password does not sign in: %d", code)
	}
}

// The tests above call the handlers by hand, so they cannot notice main.go
// losing a route. This reads main.go itself, comments stripped.
func TestSignup_MainWiresTheRoutes(t *testing.T) {
	src, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	code := regexp.MustCompile(`(?m)^\s*//.*$`).ReplaceAllString(string(src), "")
	for _, want := range []string{
		`"/signup", SignupHandler`,
		`"/signup/available", UsernameAvailableHandler`,
		`"/login", LoginHandler`,
		`"/auth/password", authed(ChangePasswordHandler)`,
	} {
		if !strings.Contains(code, want) {
			t.Errorf("main.go does not wire %s", want)
		}
	}
}

func TestForwardedShape_SaysHowManyNotWhich(t *testing.T) {
	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set("X-Forwarded-For", "203.0.113.9, 10.0.0.1")
	r.Header.Set("X-Real-IP", "203.0.113.9")
	got := forwardedShape(r)
	if !strings.Contains(got, "carries 2 address") || !strings.Contains(got, "X-Real-IP") {
		t.Errorf("shape: %q", got)
	}
	if strings.Contains(got, "203.0.113.9") || strings.Contains(got, "10.0.0.1") {
		t.Errorf("the shape gives the addresses away: %q", got)
	}

	// And clientIP says it, once.
	var buf bytes.Buffer
	log.SetOutput(&buf)
	defer log.SetOutput(os.Stderr)
	forwardedShapeOnce = sync.Once{}
	clientIP(r)
	clientIP(r)
	if n := strings.Count(buf.String(), "how the host forwards"); n != 1 {
		t.Errorf("the shape was logged %d times, want once: %q", n, buf.String())
	}
}
