package main

// The app is called Flex. What people see of the name from the server says
// so; the public pages are checked in public_pages_db_test.go and reserved
// usernames in account_rules_test.go.

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/mux"
)

// An authenticator app shows the account under the name the server gives
// when two-step verification is set up.
func TestFlexName_AuthenticatorAppsShowFlex(t *testing.T) {
	defer withDB(t)()
	resetRedis(t)
	defer leaver(t, 9911, "flex_two_step", "sunset42go")()
	req := withUser(httptest.NewRequest("POST", "/api/v1/users/9911/totp/enroll", strings.NewReader(`{}`)),
		"9911", "flex_two_step")
	req = mux.SetURLVars(req, map[string]string{"id": "9911"})
	rec := httptest.NewRecorder()
	EnrollTOTPHandler(rec, req)
	if rec.Code != 200 {
		t.Fatalf("setting up two-step verification answered %d: %s", rec.Code, rec.Body.String())
	}
	var got map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &got)
	uri, _ := got["otpauthUri"].(string)
	if !strings.HasPrefix(uri, "otpauth://totp/Flex:flex_two_step?") || !strings.Contains(uri, "issuer=Flex") {
		t.Errorf("authenticator apps would show %q, want the account under Flex", uri)
	}
}

// Once the store link is set, every web page has a button to get the app,
// and it says Flex: a profile, a video, and the page for one nobody may see.
func TestFlexName_TheWebPagesSayGetFlex(t *testing.T) {
	defer withDB(t)()
	seedPublicPeople(t)
	t.Setenv("APP_DOWNLOAD_URL", "https://apps.example/flex")
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
		if !strings.Contains(body, `href="https://apps.example/flex">Get Flex</a>`) {
			t.Errorf("%s has no Get Flex button", what)
		}
		if strings.Contains(strings.ToLower(body), "devf") {
			t.Errorf("%s still says the app's old name, devf", what)
		}
	}
}
