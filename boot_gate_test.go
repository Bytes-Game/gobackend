package main

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

// ════════════════════════════════════════════════════════════════════════════
// The four-day outage this exists to prevent
// ════════════════════════════════════════════════════════════════════════════
//
// The app used to exit if a dependency was missing — seven separate places,
// all before the HTTP server bound a port. From outside every one of them
// looked identical and looked like nothing: no status code, no page, just a
// connection that hung until the caller gave up.
//
// That happened. The service went down on 8 September and stayed down, and the
// only symptom anybody could get was a timeout. The health check could not
// help, because there was nothing alive to answer it.

func TestBootGate_SaysWhyInsteadOfHanging(t *testing.T) {
	g := newBootGate()
	g.setStatus(bootStatus{Detail: "database: cannot reach database", Attempts: 3})

	rec := httptest.NewRecorder()
	g.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health", nil))

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("health answered %d while starting; it must answer 503, "+
			"because a timeout is indistinguishable from a dead host and "+
			"tells whoever is looking nothing at all", rec.Code)
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("health did not return JSON: %v", err)
	}
	detail, _ := body["detail"].(string)
	if !strings.Contains(detail, "database") {
		t.Errorf("health said %q. It has to name the thing that is missing — "+
			"that is the entire difference between this and a timeout.", detail)
	}
	if ready, _ := body["ready"].(bool); ready {
		t.Error("health claimed ready while still starting")
	}
}

// Not just /health. Anything at all, so nothing ever hangs.
func TestBootGate_EveryRouteAnswersWhileStarting(t *testing.T) {
	g := newBootGate()
	for _, path := range []string{"/", "/search?q=x", "/api/v1/feed", "/anything"} {
		rec := httptest.NewRecorder()
		g.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if rec.Code != http.StatusServiceUnavailable {
			t.Errorf("%s answered %d during startup, want 503", path, rec.Code)
		}
	}
}

func TestBootGate_ServesTheRealRouterOnceReady(t *testing.T) {
	g := newBootGate()
	g.ready(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("real"))
	}))

	rec := httptest.NewRecorder()
	g.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health", nil))
	if rec.Code != http.StatusOK || rec.Body.String() != "real" {
		t.Errorf("after ready the gate served %d %q, want the real router",
			rec.Code, rec.Body.String())
	}
	if !g.currentStatus().Ready {
		t.Error("status still says not ready after the router was swapped in")
	}
}

// A dependency that comes back in an hour should find the app waiting, not a
// process that exited fifty-nine minutes ago.
func TestBootGate_KeepsTryingAndRecovers(t *testing.T) {
	g := newBootGate()
	calls := 0
	g.bringUp("database", func() error {
		calls++
		if calls < 3 {
			return errors.New("connection refused")
		}
		return nil
	})
	if calls != 3 {
		t.Errorf("gave up after %d attempts; it must keep retrying until the "+
			"dependency returns", calls)
	}
}

// While it is failing, the reason has to be visible — that is what turns a
// silent outage into a five-second diagnosis.
func TestBootGate_ReportsTheReasonWhileRetrying(t *testing.T) {
	g := newBootGate()
	seen := make(chan string, 1)
	tries := 0
	g.bringUp("valkey", func() error {
		tries++
		if tries == 1 {
			return errors.New("no route to host")
		}
		// By the second attempt the status should already describe the first
		// failure.
		select {
		case seen <- g.currentStatus().Detail:
		default:
		}
		return nil
	})

	got := <-seen
	if !strings.Contains(got, "valkey") || !strings.Contains(got, "no route to host") {
		t.Errorf("while retrying, the status said %q; it must name both the "+
			"dependency and the error", got)
	}
}

// ── The guard ───────────────────────────────────────────────────────────────

// The specific thing that caused the outage: exiting before anything can
// answer. If these come back, so does a four-day silent failure.
func TestBootGate_DependenciesDoNotKillTheProcess(t *testing.T) {
	for _, f := range []string{"database.go", "redis.go", "schema_migrations.go"} {
		src, err := os.ReadFile(f)
		if err != nil {
			t.Fatalf("cannot read %s: %v", f, err)
		}
		for i, line := range strings.Split(string(src), "\n") {
			trimmed := strings.TrimSpace(line)
			if strings.HasPrefix(trimmed, "//") {
				continue
			}
			if strings.Contains(trimmed, "log.Fatal") {
				t.Errorf("%s:%d exits the process:\n\t%s\nA dependency being "+
					"down must return an error so the port stays bound and "+
					"/health can say what is wrong. Exiting produces a hung "+
					"socket, which says nothing.", f, i+1, trimmed)
			}
		}
	}
}

// The port must be bound before anything that can fail is touched.
func TestBootGate_ThePortIsBoundFirst(t *testing.T) {
	src, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatalf("cannot read main.go: %v", err)
	}
	s := string(src)

	listen := strings.Index(s, "startListening(gate)")
	db := strings.Index(s, `gate.bringUp("database"`)
	if listen < 0 || db < 0 {
		t.Fatal("main.go no longer binds the port through the boot gate")
	}
	if listen > db {
		t.Error("main.go touches the database before binding the port. If the " +
			"database is down, nothing will be listening and every request " +
			"hangs — which is exactly the outage this was written for.")
	}
}
