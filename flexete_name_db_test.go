package main

// The app is called Flexete. What people see of the name from the server says
// so; the public pages are checked in public_pages_db_test.go and reserved
// usernames in account_rules_test.go.

import (
	"encoding/json"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"

	"github.com/gorilla/mux"
)

// The name before Flexete, as a whole word. The web pages' layout rules say
// "flex" in lower case (display:flex); that is not a name and does not match.
var oldFlexName = regexp.MustCompile(`\bFlex\b`)

// An authenticator app shows the account under the name the server gives
// when two-step verification is set up.
func TestFlexeteName_AuthenticatorAppsShowFlexete(t *testing.T) {
	defer withDB(t)()
	resetRedis(t)
	defer leaver(t, 9911, "flexete_two_step", "sunset42go")()
	req := withUser(httptest.NewRequest("POST", "/api/v1/users/9911/totp/enroll", strings.NewReader(`{}`)),
		"9911", "flexete_two_step")
	req = mux.SetURLVars(req, map[string]string{"id": "9911"})
	rec := httptest.NewRecorder()
	EnrollTOTPHandler(rec, req)
	if rec.Code != 200 {
		t.Fatalf("setting up two-step verification answered %d: %s", rec.Code, rec.Body.String())
	}
	var got map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &got)
	uri, _ := got["otpauthUri"].(string)
	if !strings.HasPrefix(uri, "otpauth://totp/Flexete:flexete_two_step?") || !strings.Contains(uri, "issuer=Flexete") {
		t.Errorf("authenticator apps would show %q, want the account under Flexete", uri)
	}
}

// Once the store link is set, every web page has a button to get the app,
// and it says Flexete: a profile, a video, and the page for one nobody may see.
func TestFlexeteName_TheWebPagesSayGetFlexete(t *testing.T) {
	defer withDB(t)()
	seedPublicPeople(t)
	t.Setenv("APP_DOWNLOAD_URL", "https://apps.example/flexete")
	public := gridVideo(t, pubPerson, 1, "arena", "open")
	private := gridVideo(t, pubPerson, 1, "friends", "open")
	for what, page := range map[string]func() (int, string){
		"a profile": func() (int, string) {
			return publicPage(t, PublicProfilePageHandler, "/u/maya_pub", map[string]string{"username": "maya_pub"})
		},
		"a video": func() (int, string) {
			return publicPage(t, PublicVideoPageHandler, "/v/"+public, map[string]string{"id": public})
		},
		"a video nobody may see": func() (int, string) {
			return publicPage(t, PublicVideoPageHandler, "/v/"+private, map[string]string{"id": private})
		},
	} {
		_, body := page()
		if !strings.Contains(body, `href="https://apps.example/flexete">Get Flexete</a>`) {
			t.Errorf("%s has no Get Flexete button", what)
		}
		if strings.Contains(strings.ToLower(body), "devf") {
			t.Errorf("%s still says the app's old name, devf", what)
		}
		if m := oldFlexName.FindString(body); m != "" {
			t.Errorf("%s still says the name before Flexete: %q", what, m)
		}
	}
}

// The password-reset email speaks for the app under its name, and never
// under the one before.
func TestFlexeteName_TheResetEmailSaysFlexete(t *testing.T) {
	body := resetEmail("maya", "123456")
	if !strings.Contains(body, "The Flexete team") {
		t.Errorf("the reset email is not signed by Flexete:\n%s", body)
	}
	if oldFlexName.MatchString(body) {
		t.Errorf("the reset email still says Flex:\n%s", body)
	}
}

// Replies to the server's emails, and its "write to us" messages, go to the
// app's own domain: the inbox the owner forwards in Cloudflare, and the same
// address the app shows (supportEmail in the app's constants.dart).
func TestFlexeteName_HelpIsAtTheAppsOwnDomain(t *testing.T) {
	if supportEmail != "support@flexete.com" {
		t.Errorf("supportEmail is %q, want support@flexete.com", supportEmail)
	}
}
