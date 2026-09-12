package main

import (
	"regexp"
	"strings"
	"testing"
	"time"
)

// ════════════════════════════════════════════════════════════════════════════
// FIVE TRIES HAS TO MEAN FIVE TRIES, NOT FIVE TRIES IN ONE SECOND
// ════════════════════════════════════════════════════════════════════════════
//
// A video gets maxHLSAttempts goes at being transcoded and is then retired.
// The number is fine. What was broken is how fast it could be spent.
//
// The claim query hands back the newest waiting video, and a failed job goes
// straight back to waiting — so the worker re-claimed the same video the
// instant it failed. Five failures inside one second, from a real run:
//
//	11:40:49 claimed job id=46 ... download: source GET status 403
//	11:40:49 claimed job id=46 ... download: source GET status 403
//	11:40:49 claimed job id=46 ... download: source GET status 403
//	11:40:49 claimed job id=46 ... download: source GET status 403
//	11:40:49 claimed job id=46 ... download: source GET status 403
//
// For a source that is permanently gone, that is only wasted effort. For a
// video that hit one bad moment it is fatal — the moment has not passed a
// millisecond later, so every try fails and a good video is retired for good.
//
// Fifteen videos in production ended up in exactly that state: no playable
// video, and out of attempts.

// claimSQL pulls the claim statement out of the worker API.
func claimSQL(t *testing.T) string {
	t.Helper()
	src := readSourceFile(t, "hls_worker_api.go")
	i := strings.Index(src, "SET hls_manifest_url = 'PENDING'")
	if i < 0 {
		t.Fatal("could not find the claim statement in hls_worker_api.go — " +
			"if it moved, move these tests with it")
	}
	j := strings.Index(src[i:], "RETURNING")
	if j < 0 {
		t.Fatal("the claim statement has no RETURNING; this test is reading " +
			"the wrong thing")
	}
	// Whitespace is collapsed so re-indenting the SQL cannot fail the test.
	return regexp.MustCompile(`\s+`).ReplaceAllString(src[i:i+j], " ")
}

func TestHLSClaim_MakesAFailedVideoWaitBeforeTheNextTry(t *testing.T) {
	sql := claimSQL(t)

	// The cap on its own is not a limit on anything but the count.
	if !strings.Contains(sql, "hls_attempts <") {
		t.Error("the claim query no longer checks the attempt cap, so a " +
			"video that can never transcode is retried forever")
	}
	// This is the part that turns the count into a real limit: a video that
	// was claimed recently is not offered again yet.
	if !strings.Contains(sql, "hls_claimed_at <") {
		t.Error("the claim query does not make a video wait after a failed " +
			"attempt.\n\nWithout that, a failure puts the video straight back " +
			"at the front of the queue and all of its attempts are spent in " +
			"the same second — which retires videos whose only problem was " +
			"one bad moment.")
	}
	// And the wait must not apply to a video nobody has tried yet, or every
	// upload would sit unwatchable for the length of the cool-off before its
	// FIRST attempt. Nothing else in the system would report that.
	if !strings.Contains(sql, "hls_claimed_at IS NULL") {
		t.Error("a video that has never been claimed is not exempt from the " +
			"wait, so every new upload is held back before its first try")
	}
}

func TestHLSRetryCooloff_OutlastsASingleWorkerRun(t *testing.T) {
	// The property that was violated, stated as a relationship rather than a
	// typed-in number: one run of the worker must not be able to use up a
	// video's whole allowance. Before the cool-off existed the left-hand side
	// was zero, so any run could.
	//
	// max-runtime is read from the workflow so raising it there cannot
	// quietly re-open the hole.
	wf := readSourceFile(t, ".github/workflows/hls-worker.yml")
	inv := regexp.MustCompile(`/tmp/hls-worker\s+-drain[^\n&]*`).FindString(wf)
	if inv == "" {
		t.Fatal("no hls-worker -drain invocation found in the workflow")
	}
	runMin := mustFindMinutesIn(t, inv, `-max-runtime\s+(\d+)m`, "-max-runtime")
	runLen := time.Duration(runMin) * time.Minute

	spread := time.Duration(maxHLSAttempts-1) * hlsRetryCooloff
	if spread <= runLen {
		t.Errorf("a video's %d attempts can all be spent inside one %s run "+
			"(they are spread over %s).\n\nThat is how fifteen videos were "+
			"retired without anything actually being wrong with them: one bad "+
			"minute, five instant retries, gone. Raise hlsRetryCooloff or "+
			"lower -max-runtime.",
			maxHLSAttempts, runLen, spread)
	}
}

func TestHLSRetryCooloff_StillFinishesWellInsideTheAmnesty(t *testing.T) {
	// InitDatabase forgives a video's attempts 24 hours after its last one,
	// so a video stranded by an infrastructure problem frees itself. If the
	// attempts were spread out further than that window, a video could reach
	// the cap and be forgiven on a loop, never settling — which is the
	// "retried forever" behaviour the cap exists to stop.
	const amnestyWindow = 24 * time.Hour
	spread := time.Duration(maxHLSAttempts-1) * hlsRetryCooloff
	if spread >= amnestyWindow {
		t.Errorf("attempts are spread over %s, which is past the %s amnesty — "+
			"a broken video would be forgiven before it ever reached the cap "+
			"and would be retried forever", spread, amnestyWindow)
	}
}
