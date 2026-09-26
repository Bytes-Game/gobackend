package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"strings"
	"testing"
)

// The worker tells the backend which renditions it settled for a video —
// made, or decided against — so the backfill can stop offering a video a
// rung it has already been considered for. See settledLabels.

func ladderLabels() []string {
	var out []string
	for _, r := range progressiveLadder {
		out = append(out, r.label)
	}
	return out
}

func TestSettled_EverythingMadeOrNotPlannedIsSettled(t *testing.T) {
	// A 480p source: the plan has no 720p rungs for it, and everything it
	// did plan came out.
	plan := planProgressive(854, 0)
	made := map[string]string{}
	for _, p := range plan {
		made[p.rung.label] = "/tmp/" + p.rung.label + ".mp4"
	}
	got := settledLabels(plan, made)
	if strings.Join(got, ",") != strings.Join(ladderLabels(), ",") {
		t.Errorf("settled %v, want the whole ladder %v: a rung the plan left "+
			"out on purpose is an answer too", got, ladderLabels())
	}
	for _, l := range got {
		if l == "720p_hevc" {
			return
		}
	}
	t.Error("720p_hevc was not settled for a 480p video, so the backfill " +
		"would offer it again every round")
}

func TestSettled_AFailedRungIsNotSettled(t *testing.T) {
	plan := planProgressive(1920, 0)
	made := map[string]string{}
	for _, p := range plan {
		if p.rung.label != "720p_hevc" {
			made[p.rung.label] = "/tmp/" + p.rung.label + ".mp4"
		}
	}
	for _, l := range settledLabels(plan, made) {
		if l == "720p_hevc" {
			t.Fatal("a rung that was planned and failed was reported as " +
				"settled, so no backfill would ever try it again")
		}
	}
}

func TestReportComplete_SendsTheSettledList(t *testing.T) {
	var got reportPayload
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if err := json.Unmarshal(body, &got); err != nil {
			t.Errorf("report was not JSON: %v", err)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	cfg := &workerConfig{BackendURL: srv.URL}
	job := pendingJob{ChallengeID: "7"}
	want := []string{"360p", "480p", "480p_hevc"}
	if err := reportComplete(cfg, job, jobResult{ManifestURL: "https://v/m.m3u8", Ladder: want}); err != nil {
		t.Fatal(err)
	}
	if strings.Join(got.Ladder, ",") != strings.Join(want, ",") {
		t.Errorf("the backend was sent ladder %v, want %v", got.Ladder, want)
	}
}

func TestProcessJob_PassesTheSettledListOn(t *testing.T) {
	// processJob needs ffmpeg and a bucket, so this reads the code. Comment
	// lines are dropped first so it can only pass on code.
	raw, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	var code []string
	for _, l := range strings.Split(string(raw), "\n") {
		if !strings.HasPrefix(strings.TrimSpace(l), "//") {
			code = append(code, l)
		}
	}
	src := strings.Join(code, "\n")
	for _, re := range []string{
		`progressive\s*:=\s*buildProgressive\(`,
		`Ladder:\s+progressive\.settled`,
	} {
		if !regexp.MustCompile(re).MatchString(src) {
			t.Errorf("main.go no longer matches %s — the settled list does "+
				"not reach the backend", re)
		}
	}
}
