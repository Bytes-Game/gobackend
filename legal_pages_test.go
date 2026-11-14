package main

import (
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"strings"
	"testing"
)

// The copy of the app's documents is whole: all three, every section, the
// complaints officer, and none of the app's placeholders left in.
func TestLegalTexts_AreAWholeCopyOfTheApps(t *testing.T) {
	if legal.Updated == "" {
		t.Error("the documents have no last-updated date")
	}
	for name, d := range map[string]legalDocument{"terms": legal.Terms, "privacy": legal.Privacy, "guidelines": legal.Guidelines} {
		if d.Title == "" || d.Intro == "" || len(d.Summary) == 0 || len(d.Sections) < 10 || d.Contact == "" {
			t.Errorf("%s is not whole: title %q, %d summary lines, %d sections, contact %q",
				name, d.Title, len(d.Summary), len(d.Sections), d.Contact)
		}
		for _, s := range d.Sections {
			for _, p := range append([]string{s.Title}, s.Body...) {
				if strings.Contains(p, "$") || strings.TrimSpace(p) == "" {
					t.Errorf("%s, %q: a paragraph is empty or still has a placeholder: %q", name, s.Title, p)
				}
			}
		}
	}
	all, _ := os.ReadFile("legal/texts.json")
	for _, want := range []string{"Grievance Officer", "grievance@flexete.com", "within 24 hours", "15 days",
		"Grievance Appellate Committee", "press and hold"} {
		if !strings.Contains(string(all), want) {
			t.Errorf("the documents do not say %q", want)
		}
	}
}

func get(t *testing.T, path string) (int, string, http.Header) {
	t.Helper()
	rec := httptest.NewRecorder()
	LegalPageHandler(rec, httptest.NewRequest("GET", path, nil))
	return rec.Code, rec.Body.String(), rec.Header()
}

func TestLegalPages_ShowEveryDocumentInFull(t *testing.T) {
	for path, d := range map[string]legalDocument{"/terms": legal.Terms, "/privacy": legal.Privacy, "/community-guidelines": legal.Guidelines} {
		code, body, h := get(t, path)
		if code != http.StatusOK {
			t.Fatalf("%s: status %d", path, code)
		}
		if !strings.Contains(body, "<h1>"+d.Title+"</h1>") || !strings.Contains(body, "Last updated "+legal.Updated) {
			t.Errorf("%s does not show its title and date", path)
		}
		for _, s := range d.Sections {
			title := strings.ReplaceAll(s.Title, "'", "&#39;")
			if !strings.Contains(body, title) {
				t.Errorf("%s is missing the section %q", path, s.Title)
			}
		}
		if h.Get("Content-Security-Policy") != legalPageRules {
			t.Errorf("%s: served without the page rules", path)
		}
	}
}

func TestLegalPages_DeletingAnAccount(t *testing.T) {
	code, body, _ := get(t, "/delete-account")
	if code != http.StatusOK {
		t.Fatalf("status %d", code)
	}
	// What Google Play asks the page to have: the app's name, the steps,
	// what goes and what may stay, and for how long.
	for _, want := range []string{"Flexete", "Settings", "Delete account", supportEmail, "30 days",
		"deleted for good", "kept longer where the law requires", "Without the app"} {
		if !strings.Contains(body, want) {
			t.Errorf("the delete-account page does not say %q", want)
		}
	}
}

func TestLegalPages_ChildSafety(t *testing.T) {
	code, body, _ := get(t, "/child-safety")
	if code != http.StatusOK {
		t.Fatalf("status %d", code)
	}
	for _, want := range []string{"child sexual abuse", "A child may be at risk", "local emergency services", supportEmail, "13"} {
		if !strings.Contains(body, want) {
			t.Errorf("the child safety page does not say %q", want)
		}
	}
}

// The owner asked for no helpline numbers anywhere (TikTok's own pages give
// none, only "local emergency services"): a number is right in one country
// and wrong in the next.
func TestLegalPages_GiveNoHelplineNumbers(t *testing.T) {
	number := regexp.MustCompile(`(?i)\b(call|dial|helpline|childline)\b[^.<]{0,40}\b\d{3,}\b|\b(112|911|999|1098|14416)\b`)
	for path := range legalPages() {
		_, body, _ := get(t, path)
		if body == "" {
			t.Fatalf("%s is empty", path)
		}
		if m := number.FindString(body); m != "" {
			t.Errorf("%s gives a helpline number: %q", path, m)
		}
	}
}

func TestLegalPages_UnknownAddressIsNotFound(t *testing.T) {
	if code, _, _ := get(t, "/legal/whatever"); code != http.StatusNotFound {
		t.Errorf("status %d, want 404", code)
	}
}

func TestLegalPages_AreWiredIn(t *testing.T) {
	src, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	code := regexp.MustCompile(`(?m)^\s*//.*$`).ReplaceAllString(string(src), "")
	if !strings.Contains(code, `r.HandleFunc(path, LegalPageHandler)`) {
		t.Fatal("main.go does not serve the legal pages")
	}
	for path := range legalPages() {
		if !strings.Contains(code, `"`+path+`"`) {
			t.Errorf("main.go does not serve %s", path)
		}
	}
}
