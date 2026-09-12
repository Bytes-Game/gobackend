package main

import (
	"encoding/json"
	"strings"
	"testing"
)

// ════════════════════════════════════════════════════════════════════════════
// A VIDEO NEEDS SOMETHING TO SHOW BEFORE IT CAN PLAY
// ════════════════════════════════════════════════════════════════════════════
//
// The app paints a still frame while a reel opens. The feed sends
// thumbnailUrl, the reel tile renders it, and the comments in that code talk
// about the poster holding the face until the first frame lands.
//
// Nothing made one. Measured on production: 42 videos in the arena, ONE with
// a thumbnail, and that one came from the import script. Every other reel
// showed a black rectangle until its first video frame decoded.
//
// The failure was total and silent, and it was silent because the pieces are
// in four places: the worker grabs the frame, the worker sends it, this
// backend stores it, and the feed reads it. Any one of them missing gives the
// same black screen with nothing logged. These tests hold the chain together.

func TestPoster_TheWorkerSendsItAndTheBackendReadsIt(t *testing.T) {
	// The wire contract, from the worker's side to this one. A field renamed
	// on either side quietly stops posters arriving, with no error anywhere.
	body := `{"challengeId":"7","manifestUrl":"https://cdn/x/master.m3u8",` +
		`"thumbnailUrl":"https://cdn/x/poster.jpg"}`
	var req hlsCompleteRequest
	if err := json.Unmarshal([]byte(body), &req); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if req.ThumbnailURL != "https://cdn/x/poster.jpg" {
		t.Errorf("the backend does not read thumbnailUrl off the wire; got %q",
			req.ThumbnailURL)
	}

	worker := readSourceFile(t, "cmd/hls-worker/main.go")
	if !strings.Contains(worker, `json:"thumbnailUrl,omitempty"`) {
		t.Error("the worker no longer sends thumbnailUrl, so nothing ever " +
			"arrives to store")
	}
	if !strings.Contains(worker, "ThumbnailURL:  done.ThumbnailURL") {
		t.Error("the worker builds a poster URL and then does not put it in " +
			"the report it sends")
	}
}

func TestPoster_CompletingAJobStoresIt(t *testing.T) {
	// A handler that reads the field and never writes it is the same black
	// screen, one step further along.
	src := readSourceFile(t, "hls_worker_api.go")
	body := funcBody(src, "func HLSCompleteHandler(")
	if !strings.Contains(body, "storeVideoThumbnail(") {
		t.Error("finishing a transcode no longer saves the poster, so the " +
			"feed has nothing to send the app")
	}
	if !strings.Contains(src, "SET thumbnail_url = $2") {
		t.Error("nothing writes thumbnail_url")
	}
}

func TestPoster_AnEmptyOneDoesNotWipeAnExistingPicture(t *testing.T) {
	// An older worker sends nothing, and a video too short for the grab sends
	// nothing. Neither should take away a thumbnail the import script set —
	// which is exactly what a re-queue would do if this wrote unconditionally.
	body := funcBody(readSourceFile(t, "hls_worker_api.go"),
		"func storeVideoThumbnail(")
	if !strings.Contains(body, `TrimSpace(url) == ""`) {
		t.Error("an empty thumbnail is written over the top of a real one, " +
			"so re-queuing a video can lose the picture it already had")
	}
}

func TestPoster_AFailedGrabDoesNotFailTheVideo(t *testing.T) {
	// A poster is a nicety. Returning an error from the grab would trade a
	// working video for a black rectangle, which is the wrong way round.
	worker := readSourceFile(t, "cmd/hls-worker/main.go")
	body := funcBody(worker, "func processJob(")
	if body == "" {
		// The transcode function has been renamed; fall back to the whole file
		// rather than passing vacuously.
		body = worker
	}
	i := strings.Index(body, "makePoster(")
	if i < 0 {
		t.Fatal("the worker no longer makes a poster")
	}
	after := body[i:]
	// The poster's own error must be logged and moved past, never returned.
	stop := strings.Index(after, "return jobResult{")
	if stop < 0 {
		stop = len(after)
	}
	if strings.Contains(after[:stop], "return jobResult{}, ") {
		t.Error("a failed poster aborts the whole job.\n\nThe transcode has " +
			"already been paid for by this point; throwing it away over a " +
			"still image means the video is not watchable at all.")
	}
}
