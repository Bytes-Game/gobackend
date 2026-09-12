package main

import (
	"os"
	"strings"
	"testing"
)

// ════════════════════════════════════════════════════════════════════════════
// A run that reached nothing must not report success
// ════════════════════════════════════════════════════════════════════════════
//
// When the backend is unreachable, claimJob returns an error, the loop logs it,
// sleeps, and goes round again. After max-runtime it used to return normally —
// exit zero, workflow green.
//
// So while the backend was down for four days, every scheduled run burned
// twelve minutes reaching nothing and reported success. The one job that talks
// to the backend every half hour said everything was fine.
//
// An empty queue is success. Never getting an answer at all is not.
//
// Verified against the real binary both ways when this was written: pointed at
// a dead port it exits 1 naming the backend and the error; pointed at a server
// answering "nothing to do" it exits 0 with "queue drained".
func TestDrain_NeverReachingTheBackendIsAFailure(t *testing.T) {
	src, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatalf("cannot read main.go: %v", err)
	}
	s := string(src)

	if !strings.Contains(s, "backendAnswered") {
		t.Fatal("the drain loop no longer tracks whether the backend ever " +
			"answered, so a run that reached nothing will report success again")
	}
	if !strings.Contains(s, "if !backendAnswered {") {
		t.Error("nothing acts on having never reached the backend")
	}
	if !strings.Contains(s, "os.Exit(1)") {
		t.Error("a run that never reached the backend no longer exits " +
			"non-zero, so CI stays green while the pipeline is dead")
	}
}

// The distinction that makes it useful: quiet is fine, unreachable is not.
// If the loop returns early instead of breaking, the check at the bottom is
// skipped and the whole thing is decorative.
func TestDrain_TheCheckIsNotSkipped(t *testing.T) {
	src, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatalf("cannot read main.go: %v", err)
	}
	s := string(src)

	loop := s[strings.Index(s, "for {"):]
	if end := strings.Index(loop, "\n\t// A run that never got"); end > 0 {
		loop = loop[:end]
	}
	for _, bad := range []string{
		"max runtime %v reached — exiting cleanly (remaining queue picked up by the next run)\", *maxRuntime)\n\t\t\treturn",
		"queue drained — exiting (drain mode)\")\n\t\t\t\t\treturn",
	} {
		if strings.Contains(loop, bad) {
			t.Error("the drain loop returns straight out instead of breaking, " +
				"so the did-we-reach-the-backend check below it never runs")
		}
	}
}
