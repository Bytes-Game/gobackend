package main

// The rules a new account must meet, one at a time. See account_rules.go.

import (
	"strings"
	"testing"
	"time"
)

func TestAccountRules_Usernames(t *testing.T) {
	for name, wantOK := range map[string]bool{
		"maya":                  true,
		"leo_beats":             true,
		"dj.k2":                 true,
		"ab":                    false, // too short
		"Maya":                  false, // checked after lower-casing; upper case here is not allowed
		"maya!":                 false,
		".maya":                 false,
		"maya.":                 false,
		"ma..ya":                false,
		"1234":                  false, // no letter
		"admin":                 false, // reserved
		"_admin_":               false, // reserved with the dressing taken off
		"dev.f":                 false,
		"flexete":               false, // the app's own name
		"flexete.official":      false,
		"flexete_team":          false,
		"flexete.support":       false,
		"team_flexete":          false,
		"flexete_fan":           true,  // only the exact reserved names
		"flex":                  false, // the name before
		"flex_team":             false,
		"flexing_daily":         true,
		"official":              false,
		"support_":              false,
		"adminmaya":             true, // only the exact reserved names
		strings.Repeat("a", 21): false,
	} {
		got := usernameProblem(name) == ""
		if got != wantOK {
			t.Errorf("%q: allowed=%v, want %v (%q)", name, got, wantOK, usernameProblem(name))
		}
	}
}

func TestAccountRules_Passwords(t *testing.T) {
	for pw, wantOK := range map[string]bool{
		"sunset42":               true,
		"Long and 2 spaces":      true,
		"short1":                 false, // under 8
		"abcdefgh":               false, // no number
		"12345678":               false, // no letter
		"password1":              false, // common
		"maya2024x":              false, // has the username in it
		strings.Repeat("a1", 37): false, // over 72 bytes
	} {
		got := passwordProblem(pw, "maya") == ""
		if got != wantOK {
			t.Errorf("%q: allowed=%v, want %v (%q)", pw, got, wantOK, passwordProblem(pw, "maya"))
		}
	}
}

func TestAccountRules_Emails(t *testing.T) {
	if got, why := cleanEmail("  Maya@Example.COM "); got != "maya@example.com" || why != "" {
		t.Errorf("a real address came back %q %q", got, why)
	}
	for _, bad := range []string{"", "maya", "maya@", "@example.com", "maya@example",
		"Maya <maya@example.com>", "maya@.com", "maya@example.", "a b@example.com"} {
		if _, why := cleanEmail(bad); why == "" {
			t.Errorf("%q was taken as an email", bad)
		}
	}
}

func TestAccountRules_BirthDates(t *testing.T) {
	today := time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC)
	for raw, wantOK := range map[string]bool{
		"2013-10-11": true,  // 13 today
		"2013-10-12": false, // 13 tomorrow
		"2000-02-29": true,
		"1899-12-31": false,
		"2030-01-01": false, // not born yet
		"11/10/2000": false, // not the form the app sends
		"":           false,
	} {
		_, why := parseBirthDate(raw, today, defaultAgeRule)
		if got := why == ""; got != wantOK {
			t.Errorf("%q: allowed=%v, want %v (%q)", raw, got, wantOK, why)
		}
	}
	if _, why := parseBirthDate("2015-01-01", today, defaultAgeRule); !strings.Contains(why, "13 or older") {
		t.Errorf("somebody under 13 is told %q", why)
	}
	// Where the law says 16 (age_rules.go), a 15-year-old is turned away
	// and told the right age; a 16-year-old is not.
	sixteen := ageRuleFor("AU")
	if _, why := parseBirthDate("2010-10-12", today, sixteen); !strings.Contains(why, "16 or older") {
		t.Errorf("somebody of 15 in Australia is told %q", why)
	}
	if _, why := parseBirthDate("2010-10-11", today, sixteen); why != "" {
		t.Errorf("somebody of 16 in Australia is refused: %q", why)
	}
}

func TestAccountRules_Names(t *testing.T) {
	if got, why := cleanFullName("  Maya   Rose  "); got != "Maya Rose" || why != "" {
		t.Errorf("a name came back %q %q", got, why)
	}
	if _, why := cleanFullName(strings.Repeat("x", 51)); why == "" {
		t.Error("a 51-character name was allowed")
	}
	if _, why := cleanFullName("Maya\u0007"); why == "" {
		t.Error("a name with a control character was allowed")
	}
}

func TestAccountRules_SignupChecksEveryBoxInOrder(t *testing.T) {
	today := time.Date(2026, 10, 11, 0, 0, 0, 0, time.UTC)
	good := signupRequest{Username: " Maya ", Password: "sunset42", FullName: "",
		Email: "Maya@Example.com", BirthDate: "2000-05-01", AcceptTerms: true}
	acct, bad := checkSignup(good, today, defaultAgeRule)
	if bad != nil {
		t.Fatalf("a good sign-up was refused: %v", bad)
	}
	if acct.username != "maya" || acct.email != "maya@example.com" || acct.fullName != "maya" {
		t.Errorf("stored as %+v", acct)
	}
	if acct.parentEmail != "" {
		t.Errorf("an adult's sign-up is to wait for a parent at %q", acct.parentEmail)
	}
	for field, change := range map[string]func(*signupRequest){
		"username":  func(r *signupRequest) { r.Username = "admin" },
		"email":     func(r *signupRequest) { r.Email = "nope" },
		"birthDate": func(r *signupRequest) { r.BirthDate = "2020-01-01" },
		"password":  func(r *signupRequest) { r.Password = "short" },
		"terms":     func(r *signupRequest) { r.AcceptTerms = false },
		"fullName":  func(r *signupRequest) { r.FullName = strings.Repeat("x", 60) },
	} {
		r := good
		change(&r)
		if _, bad := checkSignup(r, today, defaultAgeRule); bad == nil || bad.Field != field {
			t.Errorf("a bad %s was refused as %+v", field, bad)
		}
	}
}

// Somebody under the consent age must name a parent, and the parent's
// address must be a real one that is not their own. An adult is never
// asked, whatever they send.
func TestAccountRules_UnderTheConsentAgeAParentIsAsked(t *testing.T) {
	today := time.Date(2026, 10, 11, 0, 0, 0, 0, time.UTC)
	young := signupRequest{Username: "maya", Password: "sunset42", Email: "maya@example.com",
		BirthDate: "2010-05-01", AcceptTerms: true} // 16
	for name, c := range map[string]struct {
		parent, wantField, wantParent string
	}{
		"none given":        {"", "parentEmail", ""},
		"not an address":    {"mum", "parentEmail", ""},
		"their own address": {"Maya@Example.com", "parentEmail", ""},
		"a parent's":        {" Mum@Example.com ", "", "mum@example.com"},
	} {
		r := young
		r.ParentEmail = c.parent
		acct, bad := checkSignup(r, today, defaultAgeRule)
		switch {
		case c.wantField != "" && (bad == nil || bad.Field != c.wantField):
			t.Errorf("%s: refused as %+v, want the %s box", name, bad, c.wantField)
		case c.wantField == "" && bad != nil:
			t.Errorf("%s: refused: %+v", name, bad)
		case c.wantField == "" && acct.parentEmail != c.wantParent:
			t.Errorf("%s: the parent to ask is %q, want %q", name, acct.parentEmail, c.wantParent)
		}
	}
	// The day they turn 18, nobody is asked.
	adult := young
	adult.BirthDate = "2008-10-11"
	if acct, bad := checkSignup(adult, today, defaultAgeRule); bad != nil || acct.parentEmail != "" {
		t.Errorf("an 18-year-old: refused %+v, parent %q", bad, acct.parentEmail)
	}
	almost := young
	almost.BirthDate = "2008-10-12" // 18 tomorrow
	if _, bad := checkSignup(almost, today, defaultAgeRule); bad == nil || bad.Field != "parentEmail" {
		t.Errorf("somebody 18 tomorrow was not asked for a parent: %+v", bad)
	}
}
