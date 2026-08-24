package main

import (
	"net/http"
	"strings"
	"testing"
	"time"
)

// The one property that matters more than anything else here: this feature is
// an optimisation, and an optimisation is never allowed to cost a video. So
// most of what follows is really "does it stay out of the way" — when it is
// off, when it is rate-limited, when GitHub says no.

func TestWakeTranscodeWorker_SilentWhenNotSwitchedOn(t *testing.T) {
	// No token means no network call, no goroutine, no panic. This is the
	// state every deployment is in until somebody deliberately changes it,
	// so it is the state that has to be boring.
	t.Setenv(githubWorkerTokenEnv, "")
	resetWakeupState()
	wakeTranscodeWorker()
	if !lastWakeup.IsZero() {
		t.Error("a poke was recorded with no token set — it should not have " +
			"got as far as claiming a slot")
	}
}

func TestWakeTranscodeWorker_BlankTokenIsNoToken(t *testing.T) {
	// A variable set to spaces in a hosting dashboard is a real way to end up
	// here, and it must read as "off" rather than as a token GitHub rejects
	// on every upload.
	t.Setenv(githubWorkerTokenEnv, "   ")
	resetWakeupState()
	wakeTranscodeWorker()
	if !lastWakeup.IsZero() {
		t.Error("whitespace was treated as a token")
	}
}

// ── The rate limit ──────────────────────────────────────────────────────────

func TestClaimWakeupSlot_FirstOneAlwaysGoesThrough(t *testing.T) {
	resetWakeupState()
	if !claimWakeupSlot(time.Now()) {
		t.Error("the very first upload did not get a poke — with nothing " +
			"recorded yet there is nothing to be too soon after")
	}
}

func TestClaimWakeupSlot_ABurstCostsOneCall(t *testing.T) {
	// Ten people posting at once is one run's worth of work, because a run
	// claims everything waiting rather than the one video that woke it.
	// Sending ten pokes would buy nothing and spend ten API calls.
	resetWakeupState()
	start := time.Now()
	allowed := 0
	for i := 0; i < 10; i++ {
		if claimWakeupSlot(start.Add(time.Duration(i) * time.Second)) {
			allowed++
		}
	}
	if allowed != 1 {
		t.Errorf("%d pokes went out for ten near-simultaneous uploads, want 1", allowed)
	}
}

func TestClaimWakeupSlot_OpensUpAgainAfterTheGap(t *testing.T) {
	// The half that must also hold: the limit is a gap, not a one-shot. A
	// video posted an hour later must still wake the worker.
	resetWakeupState()
	now := time.Now()
	if !claimWakeupSlot(now) {
		t.Fatal("first poke refused")
	}
	if claimWakeupSlot(now.Add(wakeupDebounce - time.Second)) {
		t.Error("a poke went out inside the gap")
	}
	if !claimWakeupSlot(now.Add(wakeupDebounce + time.Second)) {
		t.Error("still refusing after the gap had passed — uploads would be " +
			"back to waiting out the timer forever")
	}
}

func TestClaimWakeupSlot_GapIsShorterThanTheTimerItReplaces(t *testing.T) {
	// If the rate limit were ever set longer than the schedule, this feature
	// would be adding waiting rather than removing it.
	if wakeupDebounce >= 30*time.Minute {
		t.Errorf("the gap between pokes is %v, which is no better than the "+
			"30-minute schedule this exists to beat", wakeupDebounce)
	}
}

// ── What comes back from GitHub ─────────────────────────────────────────────

func TestWakeupError_SaysWhatToActuallyFix(t *testing.T) {
	// These messages are read at 2am by somebody who set this up once, months
	// ago. "403" is not an answer; "the token needs Actions write" is.
	cases := map[int]string{
		http.StatusUnauthorized: "token",
		http.StatusForbidden:    "token",
		http.StatusNotFound:     "find",
	}
	for status, want := range cases {
		got := (&wakeupError{status: status}).Error()
		if !strings.Contains(got, want) {
			t.Errorf("status %d said %q, which does not mention %q", status, got, want)
		}
	}
}

func TestWakeupError_NeverCarriesTheResponseBody(t *testing.T) {
	// GitHub echoes the request back in some error bodies, and the request
	// carries the token. The error is built from the status code alone so
	// there is no path by which a secret reaches a log file.
	e := &wakeupError{status: http.StatusForbidden}
	if strings.Contains(e.Error(), "Bearer") || strings.Contains(e.Error(), "ghp_") {
		t.Errorf("the error text %q looks like it could carry a credential", e.Error())
	}
}

// ── Defaults ────────────────────────────────────────────────────────────────

func TestEnvOrDefault_FallsBackAndTrims(t *testing.T) {
	t.Setenv("WAKEUP_TEST_VAR", "")
	if got := envOrDefault("WAKEUP_TEST_VAR", "fallback"); got != "fallback" {
		t.Errorf("unset gave %q", got)
	}
	t.Setenv("WAKEUP_TEST_VAR", "   ")
	if got := envOrDefault("WAKEUP_TEST_VAR", "fallback"); got != "fallback" {
		t.Errorf("whitespace gave %q, want the fallback — a variable someone "+
			"cleared by blanking it should read as unset", got)
	}
	t.Setenv("WAKEUP_TEST_VAR", "  set  ")
	if got := envOrDefault("WAKEUP_TEST_VAR", "fallback"); got != "set" {
		t.Errorf("got %q, want the trimmed value", got)
	}
}

func TestWorkerDefaults_PointAtTheWorkflowThatActuallyRuns(t *testing.T) {
	// If this drifts from .github/workflows/hls-worker.yml the poke returns
	// 404 forever and every upload silently goes back to waiting 30 minutes.
	if defaultWorkerWorkflow != "hls-worker.yml" {
		t.Errorf("default workflow is %q; the file in .github/workflows is "+
			"hls-worker.yml", defaultWorkerWorkflow)
	}
}

func resetWakeupState() {
	wakeupMu.Lock()
	lastWakeup = time.Time{}
	wakeupMu.Unlock()
}
