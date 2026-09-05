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
	// Setup has to cover a CACHE MISS, not just apt — and there are two
	// caches now, either or both of which can miss on the same run.
	//
	// Each cache key holds its model name, so changing a model rebuilds that
	// program and re-downloads its weights on the very next run:
	//
	//	whisper.cpp   build ~2m + 1.5GB model   (measured 2m09s at 466MB)
	//	llama.cpp     build ~6m + 2.4GB model
	//
	// The whisper build was never counted here at all, which once left a
	// cache-miss run about a minute from the kill switch with nothing in the
	// repo saying so. Adding a second, larger download without raising this
	// would repeat that exactly — which is the whole reason this number is
	// checked by a test rather than left in a comment.
	const setupAllowanceMin = 14 // checkout + setup-go + apt + BOTH rebuilds

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

// TestHLSWorkerBuildsTheBinariesItThenRuns guards a flag that reads like a
// saving and is a total outage.
//
// llama-cli lives in tools/cli, and tools/CMakeLists.txt puts that directory
// inside `if (LLAMA_BUILD_SERVER)`. So -DLLAMA_BUILD_SERVER=OFF — which looks
// like an obvious way to skip building a web server nobody here wants —
// removes llama-cli from the build entirely. The target stops existing, the
// build step fails, and that step is not continue-on-error, so the whole job
// dies: no transcoding for any upload until a person notices.
//
// It shipped exactly that way. Three consecutive worker runs failed and the
// queue was stopped for about three hours. The cache guaranteed it: the first
// run after the change was a cache miss, so it broke on the very next run and
// stayed broken.
//
// Nothing about the failure points at this flag, which is why it is a test.
func TestHLSWorkerBuildsTheBinariesItThenRuns(t *testing.T) {
	raw, err := os.ReadFile(".github/workflows/hls-worker.yml")
	if err != nil {
		t.Fatalf("read workflow: %v", err)
	}
	wf := string(raw)

	// Comments are stripped first, because the workflow documents this exact
	// trap in a comment right above the build — and a naive search would trip
	// on its own warning, which is a confusing way for a guard to fail.
	var live strings.Builder
	for _, line := range strings.Split(wf, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "#") {
			continue
		}
		live.WriteString(line)
		live.WriteByte('\n')
	}
	if strings.Contains(live.String(), "LLAMA_BUILD_SERVER=OFF") {
		t.Error("LLAMA_BUILD_SERVER=OFF is back in the llama.cpp build.\n\n" +
			"That deletes llama-cli (tools/cli is gated on it), so the build " +
			"step fails and takes the whole drain job with it — every upload " +
			"stops transcoding. Use LLAMA_BUILD_TESTS=OFF for the saving.")
	}

	// And every binary the worker is pointed at has to be one the build was
	// actually asked for. A path exported into the environment that nothing
	// produced is the same outage wearing a different hat.
	for _, bin := range []string{"llama-cli", "llama-mtmd-cli"} {
		if !strings.Contains(wf, "--target llama-cli llama-mtmd-cli") &&
			!strings.Contains(wf, "--target "+bin) {
			t.Errorf("%s is not built by any --target line, but the workflow "+
				"copies it out of the build directory", bin)
		}
		if !strings.Contains(wf, "cp /tmp/llama-build/bin/"+bin) {
			t.Errorf("%s is built but never copied into the cached directory, "+
				"so it disappears on the next run", bin)
		}
	}
}
