package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
)

// ════════════════════════════════════════════════════════════════════════════
// A BACKFILL FOR A RUNG NOT EVERY VIDEO GETS HAS TO FINISH
// ════════════════════════════════════════════════════════════════════════════
//
// "missing: X" picks videos without rendition X, oldest first, until none are
// left. Every video gets a 360p copy, so that ran out. The H.265 rungs are
// different: a 480p video gets no 720p_hevc, and a lean one gets no H.265 copy
// at all when it would be no smaller. Those still lack X after they are
// re-encoded, so they were picked again every round — the same oldest videos,
// forty rounds running, while the ones behind them never got a turn.
//
// The worker now says which renditions it settled (made, or decided against)
// and the pick skips a video whose last conversion already considered X.
// These tests play the worker's part through the real completion endpoint,
// so the whole loop — pick, convert, report, pick again — runs on Postgres.

// fullLadder is every rendition the current worker knows about.
var fullLadder = []string{"480p", "720p", "720p_hq", "360p", "480p_hevc", "720p_hevc"}

// finishConversion reports a conversion the way the worker does.
func finishConversion(t *testing.T, id int, variants map[string]string, ladder []string) {
	t.Helper()
	body, err := json.Marshal(hlsCompleteRequest{
		ChallengeID:   strconv.Itoa(id),
		ManifestURL:   "https://v/" + strconv.Itoa(id) + "/master.m3u8",
		Kind:          "challenge",
		VideoVariants: variants,
		Ladder:        ladder,
	})
	if err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	HLSCompleteHandler(w, httptest.NewRequest("POST",
		"/api/v1/internal/hls/complete", bytes.NewReader(body)))
	if w.Code != http.StatusNoContent {
		t.Fatalf("completion for %d got %d: %s", id, w.Code, w.Body.String())
	}
}

func storedLadder(t *testing.T, id int) *string {
	t.Helper()
	var ladder *string
	if err := db.QueryRow(
		`SELECT hls_ladder::text FROM challenges WHERE id = $1`, id).Scan(&ladder); err != nil {
		t.Fatalf("read ladder of %d: %v", id, err)
	}
	return ladder
}

func TestBackfill_FinishesWhenSomeVideosRightlyGetNone(t *testing.T) {
	defer withDB(t)()

	big := requeueSeed(t, map[string]string{
		"360p": "https://cdn/big/360p.mp4", "720p": "https://cdn/big/720p.mp4"})
	small := requeueSeed(t, map[string]string{
		"360p": "https://cdn/small/360p.mp4", "480p": "https://cdn/small/480p.mp4"})

	first := decodeRequeue(t, requeuePost(t, `{"missing":"720p_hevc","limit":25}`).Body.Bytes())
	if first.Requeued != 2 {
		t.Fatalf("round 1 queued %d, want both", first.Requeued)
	}

	// The worker converts both. The big one gets its 720p H.265 copy. The
	// small one is a 480p video: the plan gives it no 720p anything, and
	// that is the right answer, not a failure.
	finishConversion(t, big, map[string]string{
		"720p": "https://cdn/big2/720p.mp4", "720p_hevc": "https://cdn/big2/720p_hevc.mp4"},
		fullLadder)
	finishConversion(t, small, map[string]string{
		"480p": "https://cdn/small2/480p.mp4", "480p_hevc": "https://cdn/small2/480p_hevc.mp4"},
		fullLadder)

	second := decodeRequeue(t, requeuePost(t, `{"missing":"720p_hevc","limit":25}`).Body.Bytes())
	if second.Requeued != 0 {
		t.Errorf("round 2 queued %d. The small video was already considered "+
			"for 720p_hevc and rightly has none; queueing it again is the loop "+
			"that never ends", second.Requeued)
	}
	if second.StillMissing == nil || *second.StillMissing != 0 {
		t.Errorf("stillMissing = %v after every video was considered, want 0 — "+
			"the answer that stops the job", second.StillMissing)
	}
	if m := requeueManifest(t, small); m == "" {
		t.Error("the small video was sent round again")
	}
}

func TestBackfill_ARungThatFailedIsTriedAgain(t *testing.T) {
	defer withDB(t)()

	id := requeueSeed(t, map[string]string{"720p": "https://cdn/a/720p.mp4"})
	requeuePost(t, `{"missing":"720p_hevc","limit":25}`)

	// The encode of 720p_hevc failed. The worker leaves a failed rung off
	// the list, because a failure is not an answer.
	finishConversion(t, id, map[string]string{"720p": "https://cdn/a2/720p.mp4"},
		[]string{"480p", "720p", "720p_hq", "360p", "480p_hevc"})

	again := decodeRequeue(t, requeuePost(t, `{"missing":"720p_hevc","limit":25}`).Body.Bytes())
	if again.Requeued != 1 {
		t.Errorf("queued %d, want 1: a rung that failed has to get another try", again.Requeued)
	}
}

func TestBackfill_OlderConversionsAreStillOffered(t *testing.T) {
	defer withDB(t)()

	// Converted before the list was recorded at all.
	unrecorded := requeueSeed(t, map[string]string{"720p": "https://cdn/a/720p.mp4"})
	// Converted by a worker whose ladder had no H.265 yet.
	older := requeueSeed(t, map[string]string{"720p": "https://cdn/b/720p.mp4"})
	if _, err := db.Exec(`UPDATE challenges SET hls_ladder = '["480p","720p","360p"]'::jsonb
		WHERE id = $1`, older); err != nil {
		t.Fatal(err)
	}
	// Converted by today's worker, which decided against it.
	considered := requeueSeed(t, map[string]string{"720p": "https://cdn/c/720p.mp4"})
	ladder, _ := json.Marshal(fullLadder)
	if _, err := db.Exec(`UPDATE challenges SET hls_ladder = $2::jsonb WHERE id = $1`,
		considered, string(ladder)); err != nil {
		t.Fatal(err)
	}

	count := decodeRequeue(t,
		requeuePost(t, `{"missing":"720p_hevc","countOnly":true}`).Body.Bytes())
	if count.StillMissing == nil || *count.StillMissing != 2 {
		t.Errorf("count = %v, want 2: the two conversions that never considered "+
			"720p_hevc", count.StillMissing)
	}

	requeuePost(t, `{"missing":"720p_hevc","limit":25}`)
	if requeueManifest(t, unrecorded) != "" || requeueManifest(t, older) != "" {
		t.Error("a video no conversion has considered for 720p_hevc was not " +
			"offered, so it would never get one")
	}
	if requeueManifest(t, considered) == "" {
		t.Error("a video already considered for 720p_hevc was offered again")
	}
}

func TestBackfill_TheListIsStoredAsTheWorkerSentIt(t *testing.T) {
	defer withDB(t)()

	id := requeueSeed(t, map[string]string{"720p": "https://cdn/a/720p.mp4"})

	// An older worker sends no list: the row must keep saying "not recorded".
	finishConversion(t, id, map[string]string{"720p": "https://cdn/a2/720p.mp4"}, nil)
	if got := storedLadder(t, id); got != nil {
		t.Errorf("a worker that sent no list left %s behind", *got)
	}

	// A label the backend does not store is dropped, the rest kept.
	finishConversion(t, id, map[string]string{"720p": "https://cdn/a3/720p.mp4"},
		[]string{"720p", "4k", "720p_hevc"})
	got := storedLadder(t, id)
	if got == nil {
		t.Fatal("the list was not stored")
	}
	var labels []string
	if err := json.Unmarshal([]byte(*got), &labels); err != nil {
		t.Fatalf("stored list is not JSON: %v", err)
	}
	if len(labels) != 2 || labels[0] != "720p" || labels[1] != "720p_hevc" {
		t.Errorf("stored %v, want [720p 720p_hevc]", labels)
	}
}
