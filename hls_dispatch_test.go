package main

import (
	"regexp"
	"strings"
	"testing"
	"time"
)

// ════════════════════════════════════════════════════════════════════════════
// STARTING THE WORKER WITHOUT WAITING FOR A TIMER THAT DOES NOT FIRE
// ════════════════════════════════════════════════════════════════════════════
//
// Uploads start a worker and that works. Everything else relied on a
// scheduled run asking for every thirty minutes and arriving every two to
// four hours. So a video put back by an admin, freed by the reaper, or left
// over from a run that ran out of time waited hours with nothing saying why.

func TestHLSDispatch_WaitsItsCooloffBetweenRuns(t *testing.T) {
	hlsDispatchMu.Lock()
	saved := lastHLSDispatch
	hlsDispatchMu.Unlock()
	t.Cleanup(func() {
		hlsDispatchMu.Lock()
		lastHLSDispatch = saved
		hlsDispatchMu.Unlock()
	})

	hlsDispatchMu.Lock()
	lastHLSDispatch = time.Time{}
	hlsDispatchMu.Unlock()
	now := time.Now()
	if !hlsDispatchDue(now) {
		t.Fatal("nothing has ever been started and this still will not start " +
			"a run, so the queue would never move on its own")
	}

	noteHLSDispatch()
	if hlsDispatchDue(time.Now()) {
		t.Error("a run was just started and this would start another " +
			"immediately, piling up runs GitHub then cancels")
	}
	if !hlsDispatchDue(time.Now().Add(hlsDispatchCooloff + time.Second)) {
		t.Error("past the cool-off it still will not start a run, so a " +
			"backlog would stall after one run")
	}
}

func TestHLSDispatch_EveryPathThatStartsAWorkerCountsAgainstTheCooloff(t *testing.T) {
	// An upload thirty seconds ago already put a worker on the queue. If only
	// this file's own dispatches were recorded, the timer would add a second
	// run on top of every upload.
	//
	// dispatchWorkerRun is the single place every path goes through, which is
	// why the recording lives there.
	src := readSourceFile(t, "transcode_wakeup.go")
	body := funcBody(src, "func dispatchWorkerRun(")
	if !strings.Contains(body, "noteHLSDispatch()") {
		t.Error("starting a worker is no longer recorded where every path " +
			"goes through.\n\nThe backlog timer would then add a run on top " +
			"of every upload and every admin re-queue.")
	}
}

func TestHLSDispatch_OneRunAtATime(t *testing.T) {
	// The cool-off must outlast a worker run, or this starts a second one
	// while the first is still going. The workflow serialises them so it is
	// not dangerous, just noise: runs that pile up and get cancelled, telling
	// nobody anything.
	//
	// The run length lives in the workflow, not here.
	wf := readSourceFile(t, ".github/workflows/hls-worker.yml")
	inv := regexp.MustCompile(`/tmp/hls-worker\s+-drain[^\n&]*`).FindString(wf)
	if inv == "" {
		t.Fatal("no hls-worker -drain invocation found in the workflow")
	}
	runMin := mustFindMinutesIn(t, inv, `-max-runtime\s+(\d+)m`, "-max-runtime")
	runLen := time.Duration(runMin) * time.Minute

	if hlsDispatchCooloff <= runLen {
		t.Errorf("the dispatch cool-off (%s) does not outlast a worker run "+
			"(%s), so a second run is started while the first is still "+
			"working", hlsDispatchCooloff, runLen)
	}
}

func TestHLSDispatch_ChecksOftenEnoughToBeWorthHaving(t *testing.T) {
	// The whole point is to replace a wait of hours with a wait of minutes.
	// A check interval anywhere near the cool-off would give back most of
	// that, and the check itself is one indexed EXISTS.
	if hlsDispatchCheckEvery >= hlsDispatchCooloff/2 {
		t.Errorf("checking every %s against a %s cool-off means a video can "+
			"wait most of a cool-off after becoming ready before anything "+
			"looks", hlsDispatchCheckEvery, hlsDispatchCooloff)
	}
}

func TestHLSDispatch_AsksTheSameQuestionTheWorkerAsks(t *testing.T) {
	// If these two disagree, the failure is silent and permanent in one of
	// two directions: a dispatcher that sees work the claim query does not
	// starts a run that finds nothing, every cool-off, forever; one that sees
	// less never starts a run for the videos it cannot see.
	//
	// They agree by construction — both call hlsClaimableWhere — and this
	// fails if that stops being true.
	body := funcBody(readSourceFile(t, "hls_dispatch.go"), "func hlsWorkWaiting(")
	if !strings.Contains(body, "hlsClaimableWhere()") {
		t.Error("the dispatcher no longer asks with the claim query's own " +
			"conditions, so it can disagree with the worker about whether " +
			"there is anything to do")
	}
	claim := funcBody(readSourceFile(t, "hls_worker_api.go"), "func HLSNextPendingHandler(")
	if !strings.Contains(claim, "hlsClaimableWhere()") {
		t.Error("the claim query no longer uses the shared condition")
	}
}

func TestHLSDispatch_LooksAtBothQueues(t *testing.T) {
	// Battle responses go through the same transcode queue and are just as
	// capable of having a backlog. A dispatcher that only watched challenges
	// would leave the opponent half of every battle waiting for the cron.
	body := funcBody(readSourceFile(t, "hls_dispatch.go"), "func hlsWorkWaiting(")
	if !strings.Contains(body, "challenge_responses") {
		t.Error("the dispatcher ignores the battle-response queue, so those " +
			"videos still wait for a scheduled run")
	}
}

func TestHLSDispatch_IsActuallyStarted(t *testing.T) {
	// A background loop nothing calls is decorative, and nothing else in the
	// system would report its absence — the symptom is just that videos take
	// hours again.
	if !strings.Contains(readSourceFile(t, "main.go"), "startHLSDispatcher()") {
		t.Error("startHLSDispatcher is never called, so none of this runs")
	}
}

func TestHLSDispatch_DoesNotReplaceTheCron(t *testing.T) {
	// A sleeping service checks nothing. On a free tier this one suspends
	// after fifteen idle minutes, and then only the cron can wake anything.
	// Deleting the schedule because "the backend handles it now" would leave
	// an idle app with no way back.
	wf := readSourceFile(t, ".github/workflows/hls-worker.yml")
	if !regexp.MustCompile(`(?m)^\s*-\s*cron:`).MatchString(wf) {
		t.Error("the worker's schedule is gone.\n\nThe backend dispatcher " +
			"only runs while the backend is awake, and on a free tier it " +
			"sleeps after fifteen idle minutes. The cron is the only thing " +
			"that can start a worker then.")
	}
}
