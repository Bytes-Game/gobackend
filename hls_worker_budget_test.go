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
	// Setup has to cover a whisper CACHE MISS, not just apt.
	//
	// The cache key holds the model name, so changing the model rebuilds
	// whisper.cpp and re-downloads the model on the very next run. Measured
	// at 2m09s for the 466MB small model; medium is 1.5GB. That build was
	// never counted here, which left a cache-miss run about a minute from
	// the kill switch with nothing in the repo saying so.
	const setupAllowanceMin = 5 // checkout + setup-go + apt + a whisper rebuild

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

// The worker asks whisper to work out the language from the audio, and an
// English-only model cannot do that — it ignores -l and hears everything as
// English. Those two settings live in different files, so nothing but this
// test stops them disagreeing.
//
// They already did disagree, for as long as speech had been on. Of 190
// videos, 126 had no audio and were correctly skipped; every one of the
// remaining 64 transcribed to something, but usually one, two or four words.
// That is not people saying little, it is an English listener catching
// fragments of speech it does not know. Only 12 of the 190 ever earned a
// content tag.
//
// Putting a ".en" model back is a legitimate choice — it is more accurate on
// English and a third of the size. It just has to be a decision, made with
// the -l line, rather than a default nobody reread.
func TestHLSWorkerSpeechModelUnderstandsMoreThanEnglish(t *testing.T) {
	wf, err := os.ReadFile(".github/workflows/hls-worker.yml")
	if err != nil {
		t.Fatalf("read workflow: %v", err)
	}
	m := regexp.MustCompile(`WHISPER_MODEL_NAME:\s*(\S+)`).FindStringSubmatch(string(wf))
	if m == nil {
		t.Fatal("no WHISPER_MODEL_NAME in the workflow; the speech pass has " +
			"no model to load and will silently switch itself off")
	}
	model := m[1]

	analyze, err := os.ReadFile("cmd/hls-worker/analyze.go")
	if err != nil {
		t.Fatalf("read analyze.go: %v", err)
	}
	asksForAuto := regexp.MustCompile(`"-l",\s*"auto"`).Match(analyze)

	if strings.HasSuffix(model, ".en") && asksForAuto {
		t.Errorf("the workflow installs %q, which only understands English, "+
			"while analyze.go asks whisper to detect the language. The model "+
			"wins that argument: every video is heard as English whatever "+
			"was said. Pick one — drop the \".en\", or stop passing -l auto "+
			"and say in the code that speech is English-only.", model)
	}
}
