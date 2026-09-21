package main

import (
	"encoding/json"
	"net/http"
	"testing"
)

// ════════════════════════════════════════════════════════════════════════════
// PUTTING ONLY THE VIDEOS THAT NEED IT BACK IN THE QUEUE
// ════════════════════════════════════════════════════════════════════════════
//
// A rung was added to the ladder — 360p, for people on mobile data — and every
// video already on the platform was encoded before it existed. Reaching them
// with the oldest-first sweep works but re-encodes the whole catalogue,
// including the videos that already have the rung, every time it is run.
//
// "missing" asks the question the job is about: which videos do not have this
// rendition? That is a JSONB test, and a JSONB test is exactly the kind of SQL
// a pretend database cannot check. So this runs against a real one.

// requeueSeed writes one finished video with the given renditions and returns
// its id. An empty map means "the worker ran but produced no renditions".
func requeueSeed(t *testing.T, variants map[string]string) int {
	t.Helper()
	blob, err := json.Marshal(variants)
	if err != nil {
		t.Fatalf("variants: %v", err)
	}
	var id int
	if err := db.QueryRow(`
		INSERT INTO challenges (creator_id, video_url, hls_manifest_url, video_variants)
		VALUES (1, 'https://v/source.mp4', 'https://v/done.m3u8', $1::jsonb)
		RETURNING id`, string(blob)).Scan(&id); err != nil {
		t.Fatalf("seed: %v", err)
	}
	return id
}

func requeueManifest(t *testing.T, id int) string {
	t.Helper()
	var m string
	if err := db.QueryRow(
		`SELECT hls_manifest_url FROM challenges WHERE id = $1`, id).Scan(&m); err != nil {
		t.Fatalf("read back %d: %v", id, err)
	}
	return m
}

func decodeRequeue(t *testing.T, body []byte) requeueResponse {
	t.Helper()
	var got requeueResponse
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("answer was not JSON: %v (%s)", err, body)
	}
	return got
}

func TestRequeueMissing_OnlyTouchesVideosWithoutThatRendition(t *testing.T) {
	defer withDB(t)()

	withIt := requeueSeed(t, map[string]string{
		"360p": "https://cdn/a/360p.mp4",
		"720p": "https://cdn/a/720p.mp4",
	})
	withoutIt := requeueSeed(t, map[string]string{
		"480p": "https://cdn/b/480p.mp4",
		"720p": "https://cdn/b/720p.mp4",
	})
	noneAtAll := requeueSeed(t, map[string]string{})

	w := requeuePost(t, `{"missing":"360p","limit":50}`)
	if w.Code != http.StatusOK {
		t.Fatalf("got %d: %s", w.Code, w.Body.String())
	}
	got := decodeRequeue(t, w.Body.Bytes())

	if m := requeueManifest(t, withIt); m == "" {
		t.Error("a video that ALREADY has a 360p rendition was queued again. " +
			"That is the whole cost this selection exists to avoid: the " +
			"catalogue re-encoded from scratch to add one file to some of it.")
	}
	if m := requeueManifest(t, withoutIt); m != "" {
		t.Error("a video missing its 360p rendition was left alone, so the " +
			"backlog never finishes however many times this is run")
	}
	if m := requeueManifest(t, noneAtAll); m != "" {
		t.Error("a video with no renditions at all was left alone. An empty " +
			"map has no 360p in it either.")
	}
	if got.Requeued != 2 {
		t.Errorf("requeued %d, want 2", got.Requeued)
	}
	if got.Missing != "360p" {
		t.Errorf("the answer says missing=%q, so a caller cannot tell whether "+
			"the filter was applied at all", got.Missing)
	}
}

func TestRequeueMissing_SaysWhenTheBacklogIsFinished(t *testing.T) {
	defer withDB(t)()

	// Two to do, and the answer has to count DOWN, because "requeued: 2"
	// looks identical on the first call and the last.
	requeueSeed(t, map[string]string{"480p": "https://cdn/b/480p.mp4"})
	requeueSeed(t, map[string]string{"480p": "https://cdn/c/480p.mp4"})

	w := requeuePost(t, `{"missing":"360p","limit":1}`)
	first := decodeRequeue(t, w.Body.Bytes())
	if first.StillMissing == nil {
		t.Fatal("no stillMissing in the answer, so nobody running this can " +
			"tell whether to run it again")
	}
	if *first.StillMissing != 1 {
		t.Errorf("stillMissing = %d after queueing one of two, want 1",
			*first.StillMissing)
	}

	// The one just queued is no longer "finished", so it drops out of the
	// selection until the worker gets to it. Only the untouched one is left.
	w = requeuePost(t, `{"missing":"360p","limit":50}`)
	second := decodeRequeue(t, w.Body.Bytes())
	if second.StillMissing == nil || *second.StillMissing != 0 {
		t.Errorf("stillMissing = %v after the second pass, want 0 — which is "+
			"the answer that says the job is done", second.StillMissing)
	}

	// Zero has to survive the trip through JSON. This is the answer that
	// says "stop running this", and it is the easiest one to lose: a plain
	// int field would have been indistinguishable from a sweep that counted
	// nothing, which is why the field is a pointer.
	if !jsonHasKey(t, w.Body.Bytes(), "stillMissing") {
		t.Error("stillMissing:0 was dropped from the JSON, so the one answer " +
			"that means 'stop running this' never reaches the caller")
	}
}

func jsonHasKey(t *testing.T, body []byte, key string) bool {
	t.Helper()
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(body, &raw); err != nil {
		t.Fatalf("answer was not JSON: %v", err)
	}
	_, ok := raw[key]
	return ok
}

func TestRequeueMissing_AnOrdinarySweepIsUnchanged(t *testing.T) {
	defer withDB(t)()

	withIt := requeueSeed(t, map[string]string{"360p": "https://cdn/a/360p.mp4"})
	withoutIt := requeueSeed(t, map[string]string{"480p": "https://cdn/b/480p.mp4"})

	w := requeuePost(t, `{"limit":50}`)
	if w.Code != http.StatusOK {
		t.Fatalf("got %d: %s", w.Code, w.Body.String())
	}
	got := decodeRequeue(t, w.Body.Bytes())

	// No filter named, so both go round again — the behaviour this endpoint
	// had before the filter existed.
	for _, id := range []int{withIt, withoutIt} {
		if m := requeueManifest(t, id); m != "" {
			t.Errorf("video %d was not queued by a plain sweep", id)
		}
	}
	if got.StillMissing != nil {
		t.Errorf("a plain sweep reported stillMissing=%d. It counted nothing, "+
			"so saying a number invites somebody to act on it.",
			*got.StillMissing)
	}
	if got.Missing != "" {
		t.Errorf("a plain sweep reported missing=%q", got.Missing)
	}
}

func TestRequeueMissing_RefusesARenditionItCouldNeverStore(t *testing.T) {
	// A label the backend does not store can never appear in video_variants,
	// so every video matches it and the request quietly becomes "re-encode
	// the whole catalogue" — the opposite of what naming a rendition is for.
	//
	// No database needed: this is refused before the handler looks at one.
	for _, bad := range []string{"4k", "360", "'; DROP TABLE challenges; --", "240p"} {
		body, err := json.Marshal(map[string]any{"missing": bad, "limit": 5})
		if err != nil {
			t.Fatal(err)
		}
		w := requeuePost(t, string(body))
		if w.Code != http.StatusBadRequest {
			t.Errorf("missing=%q got %d, want 400", bad, w.Code)
		}
	}
}

func TestRequeueMissing_AcceptsEveryRenditionItDoesStore(t *testing.T) {
	// The presence half of the check above. A test that only proves things
	// are refused passes just as happily against a handler that refuses
	// everything.
	//
	// db is nil here, so a label that gets past validation reaches the
	// "no database" answer — which is how this tells "allowed" from
	// "rejected" without needing a database.
	for label := range videoVariantLabels {
		body, err := json.Marshal(map[string]any{"missing": label, "limit": 5})
		if err != nil {
			t.Fatal(err)
		}
		w := requeuePost(t, string(body))
		if w.Code == http.StatusBadRequest {
			t.Errorf("missing=%q was refused, but it is a rendition this "+
				"backend stores, so the backlog for it can never be run",
				label)
		}
	}
}
