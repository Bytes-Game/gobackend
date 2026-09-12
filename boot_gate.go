package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"sync/atomic"
	"time"
)

// ════════════════════════════════════════════════════════════════════════════
// WHY THE SERVER ANSWERS BEFORE IT IS READY
// ════════════════════════════════════════════════════════════════════════════
//
// This app used to die if anything it needed was missing. Seven separate exits
// before the HTTP server ever bound a port: no DATABASE_URL, could not open the
// database, could not ping it, a migration failed, no Redis URL, a bad Redis
// URL, could not reach Redis.
//
// Every one of those produced the same thing from outside: nothing. No status
// code, no page, no error — the process was gone before anything could answer,
// the host restarted it, and it died again. A connection to a port nobody is
// listening on just hangs until the caller gives up.
//
// That is exactly what happened. The service went down and the only symptom
// available to anybody was a timeout. The health check said nothing because
// there was nothing alive to say it. It stayed that way for four days, and the
// logs that would have explained it are on the host, which is the one place you
// cannot look from a failing curl.
//
// So the port is bound FIRST, before anything that can fail. Until the app is
// genuinely ready, every request gets a 503 that says what is missing. The
// health check gets a real answer instead of a hung socket, and whoever is
// looking gets told "cannot reach the database" instead of nothing at all.
//
// This does not keep the app running without a database. It still refuses to
// serve real traffic. The difference is that it refuses out loud.

// bootGate holds whichever handler is currently correct: the "not ready yet"
// one during startup, the real router once everything is up.
//
// Swapping a pointer rather than restarting the server means the port is bound
// exactly once, at the very beginning, with no window where a request could
// arrive and find nothing listening.
type bootGate struct {
	handler atomic.Pointer[http.Handler]
	status  atomic.Pointer[bootStatus]
}

// bootStatus is what the app can currently say about itself.
type bootStatus struct {
	// Ready is false until every dependency is up and migrations have run.
	Ready bool
	// Detail is the plain reason it is not ready, if it is not.
	Detail string
	// Attempts is how many times it has tried, so a log-free operator can
	// tell "still starting" from "stuck retrying for an hour".
	Attempts int
}

func newBootGate() *bootGate {
	g := &bootGate{}
	g.setStatus(bootStatus{Detail: "starting up"})
	return g
}

func (g *bootGate) setStatus(s bootStatus) { g.status.Store(&s) }

func (g *bootGate) currentStatus() bootStatus {
	if s := g.status.Load(); s != nil {
		return *s
	}
	return bootStatus{Detail: "starting up"}
}

// ready swaps in the real router. Everything from here on is served normally.
func (g *bootGate) ready(h http.Handler) {
	g.handler.Store(&h)
	g.setStatus(bootStatus{Ready: true})
	log.Println("boot: ready, serving normally")
}

func (g *bootGate) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h := g.handler.Load(); h != nil {
		(*h).ServeHTTP(w, r)
		return
	}

	st := g.currentStatus()
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Retry-After", "30")

	// Health answers 503 rather than hanging, and says why. This is the whole
	// point: an uptime check that times out tells you nothing, and one that
	// says "cannot reach the database" tells you where to look.
	body := map[string]any{
		"status":   "starting",
		"ready":    false,
		"detail":   st.Detail,
		"attempts": st.Attempts,
		"commit":   buildCommitID(),
	}
	w.WriteHeader(http.StatusServiceUnavailable)
	_ = json.NewEncoder(w).Encode(body)
}

// bringUp runs a dependency step, retrying, and reports what is wrong while it
// keeps failing.
//
// It never gives up. A database that comes back after an hour should find the
// app waiting for it, not a process that exited fifty-nine minutes ago. The
// wait between tries grows so a long outage is not also a denial-of-service
// attack on whatever is already struggling.
func (g *bootGate) bringUp(what string, step func() error) {
	wait := bootRetryMin
	for attempt := 1; ; attempt++ {
		err := step()
		if err == nil {
			if attempt > 1 {
				log.Printf("boot: %s came up after %d attempts", what, attempt)
			}
			return
		}
		g.setStatus(bootStatus{
			Detail:   what + ": " + err.Error(),
			Attempts: attempt,
		})
		log.Printf("boot: %s failed (attempt %d), retrying in %s: %v",
			what, attempt, wait, err)
		time.Sleep(wait)
		if wait < bootRetryMax {
			wait *= 2
			if wait > bootRetryMax {
				wait = bootRetryMax
			}
		}
	}
}

const (
	// bootRetryMin is the first wait. Short, because most startup failures are
	// a dependency that is itself still booting and will be along shortly.
	bootRetryMin = 2 * time.Second
	// bootRetryMax caps the wait. Long enough not to hammer something that is
	// down, short enough that recovery is noticed within a minute of it
	// happening rather than an hour later.
	bootRetryMax = 30 * time.Second
)

// buildCommitID is which build this is, or "unknown" where the host does not
// say. Shared so the starting-up reply and the ready one name it the same way.
func buildCommitID() string {
	if c := os.Getenv("RENDER_GIT_COMMIT"); c != "" {
		return c
	}
	return "unknown"
}

// startListening binds the port immediately and serves the gate.
//
// Called before any dependency is touched, so there is never a moment when the
// process is alive but nothing is listening. That moment was the whole problem:
// a request to a port with no listener hangs, and a hang says nothing.
func startListening(gate *bootGate) (*http.Server, string) {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8081"
	}
	srv := &http.Server{
		Addr:         ":" + port,
		Handler:      gate,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}
	go func() {
		log.Printf("boot: listening on :%s (not ready yet)", port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			// Nothing to fall back to: if the port itself cannot be bound
			// there is no way to report anything to anybody.
			log.Fatalf("could not bind :%s: %v", port, err)
		}
	}()
	return srv, port
}
