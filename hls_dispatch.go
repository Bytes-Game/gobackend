package main

// hls_dispatch.go — the backend starts the video worker when videos are
// waiting, instead of hoping a timer does.
//
// ════════════════════════════════════════════════════════════════════════════
// WHY
// ════════════════════════════════════════════════════════════════════════════
//
// Converting a video happens in a GitHub Actions workflow, and until now
// something outside this service had to decide when to run it. Two things
// did:
//
//	an upload      starts a worker immediately, and this works
//	a cron         asks for every 30 minutes, and mostly does not happen
//
// Measured on this repository, gaps between scheduled runs against a
// requested thirty minutes:
//
//	4.1h  4.4h  2.3h  2.2h  2.5h  3.7h  4.4h  4.3h  4.6h  2.3h  2.1h
//
// GitHub says plainly that scheduled workflows are best-effort and dropped
// under load. Moving the schedule off the top of the hour was tried and did
// not measurably help.
//
// So anything that arrives in the queue WITHOUT an upload waits hours. That
// covers every case that matters once something has gone wrong: a video put
// back by an admin, a job freed by the reaper after a worker died, a video
// that failed and is due another try, and a backlog that one run could not
// finish inside its twelve minutes.
//
// This service is already running, already holds the token, and already
// starts a worker on upload — the one path that reliably works. All that was
// missing was asking the same question on a timer: is anything waiting?
//
// ════════════════════════════════════════════════════════════════════════════
// WHY IT DOES NOT REPLACE THE CRON
// ════════════════════════════════════════════════════════════════════════════
//
// A sleeping service checks nothing. On a free tier this one is suspended
// after fifteen idle minutes, and then only the cron can wake anything.
//
// The two cover each other's gap rather than duplicating each other: the cron
// is the only thing that runs while nobody is using the app, and this is the
// only thing that reacts promptly while somebody is. Neither is enough alone,
// which is why the cron stays.

import (
	"context"
	"log"
	"sync"
	"time"
)

// hlsDispatchCheckEvery is how often to ask whether anything is waiting.
//
// The question is one indexed EXISTS against a partial index, so asking often
// costs almost nothing, and asking often is the point: it is what turns
// "waits for the next scheduled run" into "starts within a couple of
// minutes".
const hlsDispatchCheckEvery = 2 * time.Minute

// hlsDispatchCooloff is the shortest gap between two runs this starts.
//
// It must exceed how long a worker run lasts, or this would start a second
// run while the first is still going. The workflow serialises them, so that
// is not dangerous — it just piles up runs that GitHub then cancels, filling
// the Actions list with noise and telling nobody anything.
//
// One run per run-length is also exactly the drain rate wanted: while a
// backlog exists there is always a worker on it, and when the queue empties
// the checks find nothing and no runs start at all.
//
// The run length lives in the workflow file as -max-runtime, not here, so a
// test reads it from there and fails if this stops clearing it.
const hlsDispatchCooloff = 15 * time.Minute

// hlsDispatchStartTimeout bounds the call to GitHub. This is a background
// loop, so a hung request would silently stop all future checks.
const hlsDispatchStartTimeout = 30 * time.Second

// lastHLSDispatch is when a worker run was last started BY ANY PATH. An
// upload thirty seconds ago already put a worker on the queue, so the timer
// below has nothing to add.
var (
	hlsDispatchMu   sync.Mutex
	lastHLSDispatch time.Time
)

// noteHLSDispatch records that a worker run was just started, whoever started
// it, so the timer below does not immediately start another.
func noteHLSDispatch() {
	hlsDispatchMu.Lock()
	lastHLSDispatch = time.Now()
	hlsDispatchMu.Unlock()
}

// hlsDispatchDue reports whether enough time has passed to start another run.
func hlsDispatchDue(now time.Time) bool {
	hlsDispatchMu.Lock()
	defer hlsDispatchMu.Unlock()
	return lastHLSDispatch.IsZero() || now.Sub(lastHLSDispatch) >= hlsDispatchCooloff
}

// hlsWorkWaiting reports whether a worker asking for a job right now would be
// given one.
//
// It asks with the same conditions the claim query uses, from the same
// function, so the two cannot drift into disagreeing. Both tables, because
// the battle-response leg of the queue is just as capable of having a backlog
// and just as invisible.
func hlsWorkWaiting() (bool, error) {
	if db == nil {
		return false, nil
	}
	for _, table := range []string{"challenges", "challenge_responses"} {
		var waiting bool
		err := db.QueryRow(
			`SELECT EXISTS (SELECT 1 FROM `+table+
				` WHERE `+hlsClaimableWhere()+`)`,
			int(hlsRetryCooloff.Seconds()),
		).Scan(&waiting)
		if err != nil {
			return false, err
		}
		if waiting {
			return true, nil
		}
	}
	return false, nil
}

// startHLSDispatcher runs the check on a timer for the life of the process.
func startHLSDispatcher() {
	go func() {
		ticker := time.NewTicker(hlsDispatchCheckEvery)
		defer ticker.Stop()
		for range ticker.C {
			hlsDispatchTick()
		}
	}()
}

// hlsDispatchTick is one pass: if a worker could take a job and we have not
// started one recently, start one.
func hlsDispatchTick() {
	if !hlsDispatchDue(time.Now()) {
		return
	}
	waiting, err := hlsWorkWaiting()
	if err != nil {
		log.Printf("hls dispatcher: could not check the queue: %v", err)
		return
	}
	if !waiting {
		return
	}

	// The cool-off is recorded inside dispatchWorkerRun, which every path that
	// starts a worker goes through — including this one. It is recorded on the
	// attempt rather than on success, so a failing call is retried once per
	// cool-off rather than every tick.
	ctx, cancel := context.WithTimeout(context.Background(), hlsDispatchStartTimeout)
	defer cancel()
	started, note := startWorkerNow(ctx)
	if started {
		log.Printf("hls dispatcher: videos are waiting, started a worker")
		return
	}
	log.Printf("hls dispatcher: videos are waiting but the worker could not "+
		"be started: %s", note)
}
