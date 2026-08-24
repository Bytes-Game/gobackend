package main

// transcode_wakeup.go — cutting the wait between "uploaded" and "playable".
//
// ════════════════════════════════════════════════════════════════════════════
// THE PROBLEM
// ════════════════════════════════════════════════════════════════════════════
//
// A video is not watchable until the worker converts it, and the worker runs
// on GitHub Actions on a timer — every 30 minutes (.github/workflows/
// hls-worker.yml). That timer is a floor on how long a new upload waits. Post
// a video one minute after a run finishes and it sits there, invisible and
// unplayable, for the next twenty-nine.
//
// For the person who just posted, that is the whole product being broken. They
// do not know about queues. They know their video is not there.
//
// ════════════════════════════════════════════════════════════════════════════
// WHAT THIS DOES
// ════════════════════════════════════════════════════════════════════════════
//
// Asks GitHub to start the worker NOW instead of at the next tick. The
// workflow already accepts being started on demand (workflow_dispatch), so
// this is one HTTPS call at the moment a video lands. Thirty minutes becomes
// roughly one.
//
// ════════════════════════════════════════════════════════════════════════════
// IT IS OFF UNLESS A TOKEN IS SET, AND SAFE EITHER WAY
// ════════════════════════════════════════════════════════════════════════════
//
// Without GITHUB_WORKER_TOKEN this does nothing at all and the timer still
// picks up every upload exactly as it does today. That is the important
// property: this can only ever make the wait SHORTER. Nothing is correct
// because of it and nothing breaks without it. A failed call, a bad token, a
// GitHub outage — all of them cost some waiting, none of them cost a video.
//
// It is therefore never allowed to fail an upload, block one, or slow one
// down. Every call runs in the background and every error is a log line.
//
// ════════════════════════════════════════════════════════════════════════════
// SETTINGS
// ════════════════════════════════════════════════════════════════════════════
//
//	GITHUB_WORKER_TOKEN     required to switch on. A fine-grained personal
//	                        access token scoped to THIS ONE repository with
//	                        Actions: read and write. Nothing else. It can start
//	                        this workflow and do nothing else anywhere.
//	GITHUB_WORKER_REPO      owner/name. Default below.
//	GITHUB_WORKER_WORKFLOW  workflow file name. Default below.
//	GITHUB_WORKER_REF       branch to run from. Default below.

import (
	"bytes"
	"context"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	githubWorkerTokenEnv    = "GITHUB_WORKER_TOKEN"
	githubWorkerRepoEnv     = "GITHUB_WORKER_REPO"
	githubWorkerWorkflowEnv = "GITHUB_WORKER_WORKFLOW"
	githubWorkerRefEnv      = "GITHUB_WORKER_REF"

	defaultWorkerRepo     = "Bytes-Game/gobackend"
	defaultWorkerWorkflow = "hls-worker.yml"
	defaultWorkerRef      = "main"
)

// wakeupDebounce is the shortest gap between two pokes.
//
// A run started now claims every video waiting, not just the one that
// triggered it — so when ten people post at once, the first poke already
// covers all ten and the other nine would buy nothing. The workflow's own
// concurrency group would collapse them anyway; this just avoids making the
// calls in the first place.
//
// One minute is well under the 30-minute timer it is replacing, so a burst
// never leaves anything waiting for long.
const wakeupDebounce = time.Minute

var wakeupHTTPClient = &http.Client{Timeout: 10 * time.Second}

var (
	wakeupMu   sync.Mutex
	lastWakeup time.Time
)

// wakeTranscodeWorker asks the transcode worker to start now.
//
// Returns immediately. The call happens on its own goroutine, because an
// upload must never wait on GitHub — if their API is slow, that is our
// problem, not the poster's.
func wakeTranscodeWorker() {
	token := strings.TrimSpace(os.Getenv(githubWorkerTokenEnv))
	if token == "" {
		return // not switched on; the 30-minute timer still covers this upload
	}
	if !claimWakeupSlot(time.Now()) {
		return
	}
	go func() {
		if err := dispatchWorkerRun(context.Background(), token); err != nil {
			// Worth a line, not worth alarm: the timer is still coming.
			log.Printf("transcode wakeup: could not start the worker early, "+
				"falling back to the scheduled run: %v", err)
		}
	}()
}

// claimWakeupSlot reports whether enough time has passed since the last poke,
// and records this one if so. Split out from wakeTranscodeWorker so the timing
// rule can be tested without a network.
func claimWakeupSlot(now time.Time) bool {
	wakeupMu.Lock()
	defer wakeupMu.Unlock()
	if !lastWakeup.IsZero() && now.Sub(lastWakeup) < wakeupDebounce {
		return false
	}
	lastWakeup = now
	return true
}

func dispatchWorkerRun(ctx context.Context, token string) error {
	repo := envOrDefault(githubWorkerRepoEnv, defaultWorkerRepo)
	workflow := envOrDefault(githubWorkerWorkflowEnv, defaultWorkerWorkflow)
	ref := envOrDefault(githubWorkerRefEnv, defaultWorkerRef)

	url := "https://api.github.com/repos/" + repo +
		"/actions/workflows/" + workflow + "/dispatches"
	body := []byte(`{"ref":"` + ref + `"}`)

	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	req.Header.Set("Content-Type", "application/json")

	resp, err := wakeupHTTPClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// 204 is the success answer for this endpoint. Anything else is reported
	// by status only — the body can carry back the request that produced it,
	// and this request carries a token.
	if resp.StatusCode != http.StatusNoContent {
		return &wakeupError{status: resp.StatusCode}
	}
	return nil
}

type wakeupError struct{ status int }

func (e *wakeupError) Error() string {
	switch e.status {
	case http.StatusUnauthorized, http.StatusForbidden:
		return "GitHub refused the token (needs Actions: read and write on this repo)"
	case http.StatusNotFound:
		return "GitHub could not find that repo, workflow file or branch"
	default:
		return "GitHub answered " + http.StatusText(e.status)
	}
}

func envOrDefault(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}
