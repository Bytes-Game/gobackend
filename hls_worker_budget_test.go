package main

import (
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// The hls-worker workflow's three time budgets are only correct in
// relation to each other:
//
//	max-runtime + job-timeout + runner setup < timeout-minutes
//
// Violating that is not a build error and not a test failure anywhere
// else — it shows up in production as a run hard-killed mid-transcode
// (2026-08-06 04:35), a failure email, and a row stranded PENDING until
// the reaper recovers it. This test reads the workflow and enforces the
// inequality so an innocuous-looking edit to one number can't silently
// reintroduce the overshoot.
func TestHLSWorkerTimeBudgetsFitInsideJobTimeout(t *testing.T) {
	const setupAllowanceMin = 2 // checkout + setup-go + apt install ffmpeg

	raw, err := os.ReadFile(".github/workflows/hls-worker.yml")
	if err != nil {
		t.Fatalf("read workflow: %v", err)
	}
	wf := string(raw)

	jobTimeoutMin := mustFindMinutes(t, wf, `timeout-minutes:\s*(\d+)`, "timeout-minutes")

	// Every worker invocation must carry BOTH bounds. -max-runtime alone
	// cannot bound a run (it is only checked before claiming a job).
	// Match only actual worker runs — anchoring on -drain skips the
	// `go build -o /tmp/hls-worker ...` line, which carries no budgets.
	invocations := regexp.MustCompile(`/tmp/hls-worker\s+-drain[^\n&]*`).FindAllString(wf, -1)
	if len(invocations) == 0 {
		t.Fatal("no hls-worker -drain invocations found in workflow")
	}

	for _, inv := range invocations {
		maxRuntime := mustFindMinutesIn(t, inv, `-max-runtime\s+(\d+)m`, "-max-runtime")
		jobBound := mustFindMinutesIn(t, inv, `-job-timeout\s+(\d+)m`, "-job-timeout")

		worst := maxRuntime + jobBound + setupAllowanceMin
		if worst >= jobTimeoutMin {
			t.Errorf("worst case %dm (max-runtime %dm + job-timeout %dm + setup %dm) "+
				"is not under timeout-minutes %dm\n  invocation: %s",
				worst, maxRuntime, jobBound, setupAllowanceMin, jobTimeoutMin, strings.TrimSpace(inv))
		}
	}
}

func mustFindMinutes(t *testing.T, s, pattern, what string) int {
	t.Helper()
	return mustFindMinutesIn(t, s, pattern, what)
}

func mustFindMinutesIn(t *testing.T, s, pattern, what string) int {
	t.Helper()
	m := regexp.MustCompile(pattern).FindStringSubmatch(s)
	if m == nil {
		t.Fatalf("could not find %s (pattern %q) in %q", what, pattern, strings.TrimSpace(s))
	}
	n, err := strconv.Atoi(m[1])
	if err != nil {
		t.Fatalf("parse %s: %v", what, err)
	}
	return n
}
