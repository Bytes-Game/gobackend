package main

// "Continue with Google / Apple" (social_signin.go, identity_tokens.go),
// through the real handlers against a real database. Google and Apple are
// stood in for by a key made here, published the way they publish theirs,
// so the real signature check runs.
//
// Each test checks something that IS there (an account, a sign-in, a
// ticket), not only what is refused.

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	jwt "github.com/golang-jwt/jwt/v5"
)

// standIn is a pretend Google or Apple: its signing key, published at a
// local address the provider reads its keys from for the test.
type standIn struct {
	key *rsa.PrivateKey
	kid string
	p   *identityProvider
}

// The stand-ins' keys, made once for the whole run. Making a key is slow,
// many times slower under the race detector, and CI's test step has a
// ten-minute limit: a new key in every test spent most of these tests' time.
// [0] is the stand-in's own; [1] is somebody else's, for forging.
var (
	standInKeysOnce sync.Once
	standInKeys     [2]*rsa.PrivateKey
	standInKeysErr  error
)

func standInKey(t *testing.T, which int) *rsa.PrivateKey {
	t.Helper()
	standInKeysOnce.Do(func() {
		for i := range standInKeys {
			if standInKeys[i], standInKeysErr = rsa.GenerateKey(rand.Reader, 2048); standInKeysErr != nil {
				return
			}
		}
	})
	key := standInKeys[which]
	if standInKeysErr != nil || key == nil {
		t.Fatalf("no stand-in key: %v", standInKeysErr)
		return &rsa.PrivateKey{}
	}
	return key
}

func standInFor(t *testing.T, p *identityProvider, audiences string) *standIn {
	t.Helper()
	key := standInKey(t, 0)
	s := &standIn{key: key, kid: "test-key-1", p: p}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"keys": []map[string]string{{
			"kty": "RSA", "kid": s.kid, "alg": "RS256", "use": "sig",
			"n": base64.RawURLEncoding.EncodeToString(key.N.Bytes()),
			"e": base64.RawURLEncoding.EncodeToString(big.NewInt(int64(key.E)).Bytes()),
		}}})
	}))
	was := p.keysURL
	p.mu.Lock()
	p.keysURL, p.keys, p.fetched = srv.URL, nil, time.Time{}
	p.mu.Unlock()
	t.Setenv(p.audienceEnv, audiences)
	t.Cleanup(func() {
		srv.Close()
		p.mu.Lock()
		p.keysURL, p.keys, p.fetched = was, nil, time.Time{}
		p.mu.Unlock()
	})
	return s
}

// token is a "this is me" token from the stand-in, with [claims] over a
// good default.
func (s *standIn) token(t *testing.T, claims jwt.MapClaims) string {
	t.Helper()
	all := jwt.MapClaims{
		"iss": s.p.issuers[0],
		"aud": "client-web",
		"exp": time.Now().Add(time.Hour).Unix(),
		"iat": time.Now().Unix(),
	}
	for k, v := range claims {
		all[k] = v
		if v == nil { // nil leaves the claim out
			delete(all, k)
		}
	}
	tok := jwt.NewWithClaims(jwt.SigningMethodRS256, all)
	tok.Header["kid"] = s.kid
	signed, err := tok.SignedString(s.key)
	if err != nil {
		t.Fatal(err)
	}
	return signed
}

func socialPost(t *testing.T, h http.HandlerFunc, path string, body map[string]any) (int, map[string]any) {
	t.Helper()
	resetActionLimiters(t)
	raw, _ := json.Marshal(body)
	req := httptest.NewRequest("POST", path, bytes.NewReader(raw))
	req.RemoteAddr = "203.0.113.79:4000"
	rec := httptest.NewRecorder()
	h(rec, req)
	var got map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &got)
	return rec.Code, got
}

// dropAccount removes an account made by a test, and its sign-in links.
// signupTicketTo is a sign-up ticket for Google sign-in [subject], made
// as issueSignupTicket makes one but out to [audience].
func signupTicketTo(t *testing.T, audience, subject string) string {
	t.Helper()
	key, err := signupTicketKey()
	if err != nil {
		t.Fatal(err)
	}
	tok, err := jwt.NewWithClaims(jwt.SigningMethodHS256, signupTicketClaims{
		Provider: "google",
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   subject,
			Audience:  jwt.ClaimStrings{audience},
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(10 * time.Minute)),
		},
	}).SignedString(key)
	if err != nil {
		t.Fatal(err)
	}
	return tok
}

func dropAccount(username string) {
	db.Exec(`DELETE FROM users WHERE username = $1`, username)
}

func TestSocialSignIn_SomebodyNewSignsUpWithGoogleAndComesBack(t *testing.T) {
	defer withDB(t)()
	g := standInFor(t, googleProvider, "client-web, client-ios")
	defer dropAccount("maya_g")

	code, got := socialPost(t, socialSignIn(googleProvider), "/auth/google", map[string]any{
		"idToken": g.token(t, jwt.MapClaims{"sub": "g-1001", "email": "Maya.G@Gmail.com",
			"email_verified": true, "name": "Maya G"}),
	})
	ticket, _ := got["signupTicket"].(string)
	if code != http.StatusOK || got["needsSignup"] != true || ticket == "" {
		t.Fatalf("somebody new answered %d %v", code, got)
	}
	if got["email"] != "maya.g@gmail.com" || got["fullName"] != "Maya G" || got["suggestedUsername"] != "maya_g" {
		t.Fatalf("what Google said, as the app gets it: %v", got)
	}
	if _, ok := got["token"]; ok {
		t.Fatal("signed in before the account exists")
	}

	code, got = socialPost(t, SocialSignupHandler, "/auth/social/signup", map[string]any{
		"signupTicket": ticket, "username": "Maya_G", "fullName": "Maya G",
		"birthDate": "2000-05-01", "acceptTerms": true,
	})
	token, _ := got["token"].(string)
	if code != http.StatusCreated || token == "" {
		t.Fatalf("finishing the sign-up answered %d %v", code, got)
	}
	claims, err := parseToken(token)
	if err != nil {
		t.Fatalf("the new sign-in does not work: %v", err)
	}
	var email, hash, provider string
	if err := db.QueryRow(`SELECT COALESCE(u.email, ''), u.password_hash, s.provider
		FROM users u JOIN social_logins s ON s.user_id = u.id
		WHERE u.id = $1 AND s.subject = 'g-1001'`, claims.Subject).Scan(&email, &hash, &provider); err != nil {
		t.Fatalf("the account is not joined to the Google sign-in: %v", err)
	}
	if email != "maya.g@gmail.com" || hash != "" || provider != "google" {
		t.Fatalf("the account: email %q, password %q, provider %q", email, hash, provider)
	}

	// Next time, Google signs them straight in.
	code, got = socialPost(t, socialSignIn(googleProvider), "/auth/google", map[string]any{
		"idToken": g.token(t, jwt.MapClaims{"sub": "g-1001", "email": "maya.g@gmail.com", "email_verified": true}),
	})
	user, _ := got["user"].(map[string]any)
	if code != http.StatusOK || got["token"] == nil || user["username"] != "maya_g" {
		t.Fatalf("coming back answered %d %v", code, got)
	}

	// No password was ever set, so none signs in, not even an empty one.
	if code, _ := signInAs(t, "maya_g", "", false); code != http.StatusUnauthorized {
		t.Fatalf("an empty password signed in to a Google account: %d", code)
	}
}

func TestSocialSignIn_OnlyTokensMadeForThisAppByGoogleCount(t *testing.T) {
	defer withDB(t)()
	g := standInFor(t, googleProvider, "client-web")
	refused := func(why, token string) {
		t.Helper()
		code, got := socialPost(t, socialSignIn(googleProvider), "/auth/google", map[string]any{"idToken": token})
		if code != http.StatusUnauthorized || got["needsSignup"] != nil || got["token"] != nil {
			t.Fatalf("%s: answered %d %v", why, code, got)
		}
	}
	base := jwt.MapClaims{"sub": "g-2001", "email": "x@gmail.com", "email_verified": true}
	with := func(k string, v any) jwt.MapClaims {
		c := jwt.MapClaims{}
		for a, b := range base {
			c[a] = b
		}
		c[k] = v
		return c
	}
	refused("made for another app", g.token(t, with("aud", "somebody-elses-app")))
	refused("made by somebody else", g.token(t, with("iss", "https://evil.example")))
	refused("run out", g.token(t, with("exp", time.Now().Add(-2*time.Hour).Unix())))
	refused("no expiry date", g.token(t, with("exp", nil)))
	refused("no subject", g.token(t, with("sub", "")))

	// Signed by a key that is not Google's, claiming to be Google's.
	other := standInKey(t, 1)
	forged := jwt.NewWithClaims(jwt.SigningMethodRS256, g.p.issuerClaims(base))
	forged.Header["kid"] = g.kid
	s, _ := forged.SignedString(other)
	refused("signed with another key", s)
	// Google's own key, but not the way Google signs (RS256).
	rs512 := jwt.NewWithClaims(jwt.SigningMethodRS512, g.p.issuerClaims(base))
	rs512.Header["kid"] = g.kid
	s512, _ := rs512.SignedString(g.key)
	refused("signed another way than Google signs", s512)
	// Signed with a shared secret instead of Google's key.
	hs, _ := jwt.NewWithClaims(jwt.SigningMethodHS256, g.p.issuerClaims(base)).SignedString([]byte("x"))
	refused("not signed by Google at all", hs)
	refused("rubbish", "not.a.token")

	// The real thing gets through.
	if code, got := socialPost(t, socialSignIn(googleProvider), "/auth/google",
		map[string]any{"idToken": g.token(t, base)}); code != http.StatusOK || got["needsSignup"] != true {
		t.Fatalf("a good token answered %d %v", code, got)
	}

	// Not set up on the server: said, never "anything goes".
	t.Setenv("GOOGLE_CLIENT_IDS", "")
	code, got := socialPost(t, socialSignIn(googleProvider), "/auth/google", map[string]any{"idToken": g.token(t, base)})
	if code != http.StatusServiceUnavailable || got["unavailable"] != true {
		t.Fatalf("without the setting, answered %d %v", code, got)
	}
}

// issuerClaims is [c] with this provider as the issuer and the stand-in's
// audience, for tokens built by hand.
func (p *identityProvider) issuerClaims(c jwt.MapClaims) jwt.MapClaims {
	all := jwt.MapClaims{"iss": p.issuers[0], "aud": "client-web", "exp": time.Now().Add(time.Hour).Unix()}
	for k, v := range c {
		all[k] = v
	}
	return all
}

func TestSocialSignIn_AnEmailOnAPasswordAccountIsNeverJoinedToIt(t *testing.T) {
	defer withDB(t)()
	g := standInFor(t, googleProvider, "client-web")
	defer leaver(t, 9911, "has_password", "sunset42go")()
	mustExec(t, `UPDATE users SET email = 'owner@gmail.com' WHERE id = 9911`)

	code, got := socialPost(t, socialSignIn(googleProvider), "/auth/google", map[string]any{
		"idToken": g.token(t, jwt.MapClaims{"sub": "g-3001", "email": "owner@gmail.com", "email_verified": true}),
	})
	if code != http.StatusConflict || got["emailTaken"] != true || got["token"] != nil {
		t.Fatalf("an email on a password account answered %d %v", code, got)
	}
	var joined int
	_ = db.QueryRow(`SELECT COUNT(*) FROM social_logins WHERE subject = 'g-3001'`).Scan(&joined)
	if joined != 0 {
		t.Fatal("the Google sign-in was joined to the password account")
	}

	// An email Google has not checked proves nothing: it neither blocks nor
	// is kept.
	code, got = socialPost(t, socialSignIn(googleProvider), "/auth/google", map[string]any{
		"idToken": g.token(t, jwt.MapClaims{"sub": "g-3002", "email": "owner@gmail.com", "email_verified": false}),
	})
	if code != http.StatusOK || got["needsSignup"] != true || got["email"] != "" {
		t.Fatalf("an unchecked email answered %d %v", code, got)
	}
}

func TestSocialSignIn_TheTwoStepCodeAndKeepingAnAccountStillApply(t *testing.T) {
	defer withDB(t)()
	g := standInFor(t, googleProvider, "client-web")

	defer leaver(t, 9912, "google_two_step", "sunset42go")()
	mustExec(t, `INSERT INTO social_logins (provider, subject, user_id) VALUES ('google', 'g-4001', 9912)`)
	secret, _ := turnOnTwoStep(t, 9912)
	code, got := socialPost(t, socialSignIn(googleProvider), "/auth/google",
		map[string]any{"idToken": g.token(t, jwt.MapClaims{"sub": "g-4001"})})
	ticket, _ := got["ticket"].(string)
	if code != http.StatusUnauthorized || got["twoFactorRequired"] != true || ticket == "" || got["token"] != nil {
		t.Fatalf("Google sign-in with two-step on answered %d %v", code, got)
	}
	if code, got := secondStepWith(t, ticket, codeAt(t, secret, time.Now()), false); code != http.StatusOK ||
		got["token"] == nil {
		t.Fatalf("the code after Google answered %d %v", code, got)
	}

	defer leaver(t, 9913, "google_leaver", "sunset42go")()
	mustExec(t, `INSERT INTO social_logins (provider, subject, user_id) VALUES ('google', 'g-4002', 9913)`)
	mustExec(t, `UPDATE users SET deletion_requested_at = NOW() WHERE id = 9913`)
	forgetLeaving()
	code, got = socialPost(t, socialSignIn(googleProvider), "/auth/google",
		map[string]any{"idToken": g.token(t, jwt.MapClaims{"sub": "g-4002"})})
	verified, _ := got["ticket"].(string)
	if code != http.StatusConflict || got["deletionPending"] != true || verified == "" {
		t.Fatalf("Google sign-in to a leaving account answered %d %v", code, got)
	}
	if code, got := secondStepWith(t, verified, "", true); code != http.StatusOK || got["token"] == nil {
		t.Fatalf("keeping it answered %d %v", code, got)
	}
	if waiting(t, 9913) {
		t.Fatal("kept, but still waiting to be deleted")
	}
}

func TestSocialSignup_EveryAccountRuleStillHolds(t *testing.T) {
	defer withDB(t)()
	g := standInFor(t, googleProvider, "client-web")
	defer dropAccount("rules_g")
	defer leaver(t, 9914, "taken_name", "sunset42go")()

	_, got := socialPost(t, socialSignIn(googleProvider), "/auth/google", map[string]any{
		"idToken": g.token(t, jwt.MapClaims{"sub": "g-5001", "email": "rules@gmail.com", "email_verified": true}),
	})
	ticket, _ := got["signupTicket"].(string)
	finish := func(fields map[string]any) (int, map[string]any) {
		t.Helper()
		body := map[string]any{"signupTicket": ticket, "username": "rules_g", "birthDate": "2001-01-01",
			"acceptTerms": true}
		for k, v := range fields {
			body[k] = v
		}
		return socialPost(t, SocialSignupHandler, "/auth/social/signup", body)
	}
	young := time.Now().AddDate(-12, 0, 0).Format("2006-01-02")
	for _, c := range []struct {
		fields map[string]any
		status int
		field  string
	}{
		{map[string]any{"birthDate": young}, http.StatusBadRequest, "birthDate"},
		{map[string]any{"acceptTerms": false}, http.StatusBadRequest, "terms"},
		{map[string]any{"username": "taken_name"}, http.StatusConflict, "username"},
		{map[string]any{"username": "a"}, http.StatusBadRequest, "username"},
		{map[string]any{"username": "flex"}, http.StatusBadRequest, "username"},
	} {
		if code, got := finish(c.fields); code != c.status || got["field"] != c.field {
			t.Fatalf("%v answered %d %v, want %d on %s", c.fields, code, got, c.status, c.field)
		}
	}
	if code, got := finish(nil); code != http.StatusCreated {
		t.Fatalf("the good sign-up answered %d %v", code, got)
	}
	// The same ticket again: one account per sign-in.
	if code, got := finish(map[string]any{"username": "rules_g2"}); code != http.StatusConflict || got["alreadyJoined"] != true {
		t.Fatalf("a second account for one Google sign-in answered %d %v", code, got)
	}
	defer dropAccount("rules_g2")

	// The right key, but made out to something else: refused. Made out
	// properly, the same works, so it is the address that refuses it.
	defer dropAccount("rules_odd")
	if code, got := finish(map[string]any{"signupTicket": signupTicketTo(t, "flex-something-else", "g-5002"),
		"username": "rules_odd"}); code != http.StatusUnauthorized || got["expired"] != true {
		t.Fatalf("a ticket made out to something else answered %d %v", code, got)
	}
	if code, got := finish(map[string]any{"signupTicket": signupTicketTo(t, signupTicketAudience, "g-5002"),
		"username": "rules_odd"}); code != http.StatusCreated {
		t.Fatalf("the same ticket made out properly answered %d %v", code, got)
	}

	// A ticket that is not one, or ran out.
	if code, got := socialPost(t, SocialSignupHandler, "/auth/social/signup",
		map[string]any{"signupTicket": "nope", "username": "x_y_z", "birthDate": "2000-01-01", "acceptTerms": true}); code != http.StatusUnauthorized || got["expired"] != true {
		t.Fatalf("a bad ticket answered %d %v", code, got)
	}
}

func TestSocialSignIn_AppleGivesTheNameToTheAppOnly(t *testing.T) {
	defer withDB(t)()
	a := standInFor(t, appleProvider, "com.example.devf")
	code, got := socialPost(t, socialSignIn(appleProvider), "/auth/apple", map[string]any{
		"idToken": a.token(t, jwt.MapClaims{"aud": "com.example.devf", "sub": "001234.apple",
			"email": "x7q@privaterelay.appleid.com", "email_verified": "true"}),
		"fullName": "Maya Apple",
	})
	if code != http.StatusOK || got["needsSignup"] != true || got["fullName"] != "Maya Apple" ||
		got["email"] != "x7q@privaterelay.appleid.com" {
		t.Fatalf("Apple sign-in answered %d %v", code, got)
	}
	// A Google token is not an Apple one, however good.
	g := standInFor(t, googleProvider, "client-web")
	if code, _ := socialPost(t, socialSignIn(appleProvider), "/auth/apple",
		map[string]any{"idToken": g.token(t, jwt.MapClaims{"sub": "g-1"})}); code != http.StatusUnauthorized {
		t.Fatalf("a Google token at /auth/apple answered %d", code)
	}
}

func TestAppleCallback_HandsTheResultBackToTheApp(t *testing.T) {
	form := url.Values{"code": {"c-1"}, "id_token": {"tok.en.x"}, "state": {"s-9"}}
	req := httptest.NewRequest("POST", "/auth/apple/callback", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	AppleCallbackHandler(rec, req)
	want := "intent://callback?code=c-1&id_token=tok.en.x&state=s-9#Intent;package=com.example.devf;scheme=signinwithapple;end"
	if rec.Code != http.StatusTemporaryRedirect || rec.Header().Get("Location") != want {
		t.Fatalf("answered %d to %q, want %q", rec.Code, rec.Header().Get("Location"), want)
	}
	t.Setenv("ANDROID_PACKAGE_NAME", "app.flex.video")
	rec = httptest.NewRecorder()
	req = httptest.NewRequest("POST", "/auth/apple/callback", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	AppleCallbackHandler(rec, req)
	if !strings.Contains(rec.Header().Get("Location"), ";package=app.flex.video;") {
		t.Fatalf("the release id is not used: %q", rec.Header().Get("Location"))
	}
}

func TestSuggestUsername_FreeAndAllowed(t *testing.T) {
	defer withDB(t)()
	defer leaver(t, 9915, "maya_s", "sunset42go")()
	if got := suggestUsername("Zed Q", ""); got != "zed_q" {
		t.Errorf("from a name: %q", got)
	}
	got := suggestUsername("Maya S", "")
	if !regexp.MustCompile(`^maya_s_\d{4}$`).MatchString(got) {
		t.Errorf("a taken name: %q, want maya_s_ and four digits", got)
	}
	if got := suggestUsername("🙂", "zoe.k@x.io"); got != "zoe.k" {
		t.Errorf("a name with no letters falls back to the email: %q", got)
	}
	if got := suggestUsername("Flex", ""); usernameProblem(got) != "" {
		t.Errorf("a reserved name: %q (%s)", got, usernameProblem(got))
	}
}

func TestSocialSignIn_MainWiresTheRoutes(t *testing.T) {
	src, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	code := regexp.MustCompile(`(?m)^\s*//.*$`).ReplaceAllString(string(src), "")
	for _, want := range []string{
		`"/auth/google", socialSignIn(googleProvider)`,
		`"/auth/apple", socialSignIn(appleProvider)`,
		`"/auth/social/signup", SocialSignupHandler`,
		`"/auth/apple/callback", AppleCallbackHandler`,
	} {
		if !strings.Contains(code, want) {
			t.Errorf("main.go does not wire %s", want)
		}
	}
}
