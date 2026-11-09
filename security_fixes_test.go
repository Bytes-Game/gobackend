package main

// Tests for the fixes from the before-release security check: the admin
// page, the wipe, the demo data, size limits and the worker's key. The
// ones that need a real database are in security_fixes_db_test.go.

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"strings"
	"testing"
)

// ─── the admin page ─────────────────────────────────────────────────────

// adminPages are the admin's pages, each with the handler that serves it.
var adminPages = []struct {
	name    string
	html    string
	handler http.HandlerFunc
}{
	{"dashboard", adminDashboardHTML, AdminDashboardHandler},
	{"reports", adminReportsHTML, AdminReportsPageHandler},
}

// pageScript is a page's script with its comment lines removed, so a test
// reading it checks the code and not the words explaining it.
func pageScript(t *testing.T, html string) string {
	t.Helper()
	i := strings.Index(html, "<script>")
	j := strings.Index(html, "</script>")
	if i < 0 || j < i {
		t.Fatal("the admin page has no script")
	}
	return regexp.MustCompile(`(?m)^\s*//.*$`).ReplaceAllString(html[i:j], "")
}

// The pages show text that anyone signed in can send (error names from
// the app, reports, comments, usernames). Read as HTML, a name like
// <img onerror=…> ran code in the admin's browser — checked in a real
// browser before this fix. Nothing may hand such text to the browser's
// HTML reader.
func TestAdminPage_NeverReadsServerTextAsHTML(t *testing.T) {
	for _, p := range adminPages {
		script := pageScript(t, p.html)
		for _, sink := range []string{"innerHTML", "outerHTML", "insertAdjacentHTML", "document.write", "DOMParser", "createContextualFragment", "srcdoc"} {
			if strings.Contains(script, sink) {
				t.Errorf("the %s page's script uses %s, which reads text as HTML", p.name, sink)
			}
		}
	}
}

// The other half: the pages must still fill themselves in. A page that
// shows nothing would pass the test above.
func TestAdminPage_StillFillsInEverySection(t *testing.T) {
	want := map[string][]string{
		"dashboard": {
			"document.createElement(", "replaceChildren(",
			"show('health'", "show('funnels'", "show('errors'",
			"fetch('/api/v1/admin/health')", "fetch('/api/v1/admin/funnels')", "fetch('/api/v1/admin/errors')",
		},
		"reports": {
			"document.createElement(", "replaceChildren(",
			"getElementById('open').replaceChildren(", "getElementById('suspended').replaceChildren(",
			"fetch('/api/v1/admin/reports')", "'/api/v1/admin/reports/resolve'", "'/api/v1/admin/unsuspend'",
		},
	}
	for _, p := range adminPages {
		script := pageScript(t, p.html)
		for _, w := range want[p.name] {
			if !strings.Contains(script, w) {
				t.Errorf("the %s page's script no longer has %s", p.name, w)
			}
		}
	}
}

// The second lock: the browser may run the page's own script and nothing
// else, so even text that did get read as HTML could not run. The rule
// names the script by its fingerprint; a wrong fingerprint would stop the
// page's own script, so it is worked out here from the page as served.
func TestAdminPage_RunsOnlyItsOwnScript(t *testing.T) {
	for _, p := range adminPages {
		rec := httptest.NewRecorder()
		p.handler(rec, httptest.NewRequest("GET", "/admin", nil))
		body := rec.Body.String()

		i := strings.Index(body, "<script>")
		j := strings.Index(body, "</script>")
		if i < 0 || j < i {
			t.Fatalf("the served %s page has no script", p.name)
		}
		sum := sha256.Sum256([]byte(body[i+len("<script>") : j]))
		fingerprint := "'sha256-" + base64.StdEncoding.EncodeToString(sum[:]) + "'"

		rules := rec.Header().Get("Content-Security-Policy")
		if rules == "" {
			t.Errorf("the %s page is served with no rules on what may run", p.name)
			continue
		}
		scriptRule := ""
		for _, part := range strings.Split(rules, ";") {
			if r := strings.TrimSpace(part); strings.HasPrefix(r, "script-src ") {
				scriptRule = r
			}
		}
		if scriptRule != "script-src "+fingerprint {
			t.Errorf("%s page: script rule = %q, want only the page's own script %q", p.name, scriptRule, "script-src "+fingerprint)
		}
		for _, w := range []string{"default-src 'none'", "frame-ancestors 'none'", "base-uri 'none'", "connect-src 'self'"} {
			if !strings.Contains(rules, w) {
				t.Errorf("%s page: rules %q are missing %q", p.name, rules, w)
			}
		}
		if got := rec.Header().Get("X-Frame-Options"); got != "DENY" {
			t.Errorf("%s page: X-Frame-Options = %q, want DENY (no other site may show it in a frame)", p.name, got)
		}
		if strings.Count(body, "<script") != 1 {
			t.Errorf("the %s page has %d scripts; the rules allow exactly one", p.name, strings.Count(body, "<script"))
		}
	}
}

// ─── admin changes only from the admin's own pages ──────────────────────

func TestAdminChanges_RefusedFromOtherWebsites(t *testing.T) {
	t.Setenv("ADMIN_USER", "admin-for-test")
	t.Setenv("ADMIN_PASS", "a-long-password-for-this-test")
	reached := 0
	h := adminOnly(func(w http.ResponseWriter, r *http.Request) { reached++ })

	try := func(method string, headers map[string]string) int {
		req := httptest.NewRequest(method, "http://api.flexete.test/api/v1/admin/media/delete", strings.NewReader(`{}`))
		req.SetBasicAuth("admin-for-test", "a-long-password-for-this-test")
		for k, v := range headers {
			req.Header.Set(k, v)
		}
		rec := httptest.NewRecorder()
		h(rec, req)
		return rec.Code
	}

	cases := []struct {
		name    string
		method  string
		headers map[string]string
		allowed bool
	}{
		{"a change a page elsewhere made the browser send", "POST", map[string]string{"Sec-Fetch-Site": "cross-site"}, false},
		{"a change from a neighbouring site", "POST", map[string]string{"Sec-Fetch-Site": "same-site"}, false},
		{"a change from an older browser, another site", "POST", map[string]string{"Origin": "https://evil.example"}, false},
		{"a change from the admin page itself", "POST", map[string]string{"Sec-Fetch-Site": "same-origin"}, true},
		{"a change typed in the address bar", "POST", map[string]string{"Sec-Fetch-Site": "none"}, true},
		{"a change from curl (no browser headers)", "POST", nil, true},
		{"a change from an older browser, same site", "POST", map[string]string{"Origin": "http://api.flexete.test"}, true},
		{"a read from anywhere", "GET", map[string]string{"Sec-Fetch-Site": "cross-site"}, true},
	}
	for _, c := range cases {
		before := reached
		code := try(c.method, c.headers)
		got := reached > before
		if got != c.allowed {
			t.Errorf("%s: reached the handler = %v (status %d), want %v", c.name, got, code, c.allowed)
		}
		if !c.allowed && code != http.StatusForbidden {
			t.Errorf("%s: status %d, want 403", c.name, code)
		}
	}
}

// ─── the wipe ───────────────────────────────────────────────────────────

func TestWipe_RefusedUnlessThisIsATestServer(t *testing.T) {
	wiped := 0
	prev := wipeAndReseed
	wipeAndReseed = func() { wiped++ }
	defer func() { wipeAndReseed = prev }()

	for _, v := range []string{"", "false", "no", "0"} {
		t.Setenv("TEST_SERVER", v)
		rec := httptest.NewRecorder()
		ReseedHandler(rec, httptest.NewRequest("POST", "/api/v1/admin/reseed", nil))
		if rec.Code != http.StatusForbidden {
			t.Errorf("TEST_SERVER=%q: status %d, want 403", v, rec.Code)
		}
		if !strings.Contains(rec.Body.String(), "TEST_SERVER=true") {
			t.Errorf("TEST_SERVER=%q: the refusal does not say how to allow it: %s", v, rec.Body.String())
		}
	}
	if wiped != 0 {
		t.Fatalf("the database was wiped %d times on a server that is not for testing", wiped)
	}

	t.Setenv("TEST_SERVER", "true")
	rec := httptest.NewRecorder()
	ReseedHandler(rec, httptest.NewRequest("POST", "/api/v1/admin/reseed", nil))
	if rec.Code != http.StatusOK || wiped != 1 {
		t.Errorf("on a server for testing: status %d, wiped %d times; want 200 and once", rec.Code, wiped)
	}
}

func TestDemoData_OnlyOnATestServer(t *testing.T) {
	t.Setenv("TEST_SERVER", "")
	if shouldSeedDemoData(0) {
		t.Error("an empty live database got demo data, with ten accounts whose passwords are public")
	}
	t.Setenv("TEST_SERVER", "true")
	if !shouldSeedDemoData(0) {
		t.Error("an empty database on a server for testing got no demo data")
	}
	if shouldSeedDemoData(3) {
		t.Error("a database that already has accounts got demo data on top")
	}
}

// The wires: the start-up uses the decision, the start-up runs the two
// account jobs, and the wipe route is still the guarded handler.
func TestSecurityFixes_AreWiredIn(t *testing.T) {
	read := func(name string) string {
		src, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		return regexp.MustCompile(`(?m)^\s*//.*$`).ReplaceAllString(string(src), "")
	}
	database := read("database.go")
	if body := funcBody(database, "func InitDatabase() error {"); !strings.Contains(body, "go startupAccountChecks()") {
		t.Error("starting the database no longer runs the account checks (plain passwords, demo accounts)")
	}
	if body := funcBody(database, "func seedIfEmpty() {"); !strings.Contains(body, "shouldSeedDemoData(") {
		t.Error("the start-up no longer asks whether demo data is allowed")
	}
	checks := funcBody(read("leftover_passwords.go"), "func startupAccountChecks() {")
	for _, want := range []string{"hashLeftoverPasswords()", "lockDemoAccounts()"} {
		if !strings.Contains(checks, want) {
			t.Errorf("startupAccountChecks no longer runs %s", want)
		}
	}
	main := read("main.go")
	if !strings.Contains(main, `"/admin/reseed", adminOnly(ReseedHandler)`) {
		t.Error("the wipe route is not the guarded handler behind the admin sign-in")
	}
	if !strings.Contains(main, "r.Use(limitRequestBodies)") {
		t.Error("the size limit is not applied to the server's routes")
	}
	worker := funcBody(read("hls_worker_api.go"), "func workerAuthed(")
	if !strings.Contains(worker, "subtle.ConstantTimeCompare([]byte(got), []byte(want)) != 1") {
		t.Error("the worker's key is not compared in constant time")
	}
	if regexp.MustCompile(`got\s*[!=]=\s*want|want\s*[!=]=\s*got`).MatchString(worker) {
		t.Error("the worker's key is (also) compared with == or !=, which stops at the first wrong character")
	}
}

// ─── size limits ────────────────────────────────────────────────────────

func TestRequestLimits_BigBodiesAreRefused(t *testing.T) {
	var read int
	var readErr error
	h := limitRequestBodies(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, err := io.ReadAll(r.Body)
		read, readErr = len(b), err
	}))

	// Says it is too big: refused before anything reads it.
	read, readErr = -1, nil
	big := bytes.Repeat([]byte("a"), maxRequestBody+1)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("POST", "/api/v1/anything", bytes.NewReader(big)))
	if rec.Code != http.StatusRequestEntityTooLarge || read != -1 {
		t.Errorf("an oversized request: status %d, handler read %d bytes; want 413 and never reached", rec.Code, read)
	}

	// Does not say how big it is: reading stops at the limit.
	read, readErr = -1, nil
	req := httptest.NewRequest("POST", "/api/v1/anything", io.MultiReader(bytes.NewReader(big), bytes.NewReader(big)))
	req.ContentLength = -1
	h.ServeHTTP(httptest.NewRecorder(), req)
	var tooBig *http.MaxBytesError
	if readErr == nil || !errors.As(readErr, &tooBig) || read > maxRequestBody {
		t.Errorf("a request of unknown size: read %d bytes, error %v; want reading stopped at %d", read, readErr, maxRequestBody)
	}

	// An ordinary request goes through whole.
	read, readErr = -1, nil
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("POST", "/api/v1/anything", strings.NewReader(`{"ok":true}`)))
	if read != len(`{"ok":true}`) || readErr != nil {
		t.Errorf("an ordinary request: read %d bytes, error %v", read, readErr)
	}
}

func TestEventBatches_AreCapped(t *testing.T) {
	if maxEventsPerBatch < 200 {
		t.Fatalf("the cap (%d) is below the 200 reports the app may send at once", maxEventsPerBatch)
	}
	batch := func(n int) int {
		events := make([]map[string]string, n)
		for i := range events {
			events[i] = map[string]string{"eventType": "impression"}
		}
		body, _ := json.Marshal(map[string]any{"events": events})
		rec := httptest.NewRecorder()
		TrackBatchEventsHandler(rec, httptest.NewRequest("POST", "/api/v1/feed/events/batch", bytes.NewReader(body)))
		return rec.Code
	}
	if code := batch(maxEventsPerBatch + 1); code != http.StatusRequestEntityTooLarge {
		t.Errorf("a batch of %d reports: status %d, want 413", maxEventsPerBatch+1, code)
	}
	if code := batch(0); code != http.StatusOK {
		t.Errorf("an empty batch: status %d, want 200", code)
	}
}
