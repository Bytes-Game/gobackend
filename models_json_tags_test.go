package main

import (
	"encoding/json"
	"testing"
)

// A misspelled json tag is the quietest bug this package can ship. Nothing
// fails: the field marshals under the wrong name, the client's decoder finds
// nothing at the name it asked for, and the zero value it substitutes looks
// exactly like "this response has no responder yet".
//
// That is not hypothetical. ChallengeResponse.ResponderID carried
// `json:"responderld"` — an l where the I belongs — so every response the API
// has ever served named the field responderld. The Flutter client reads
// json['responderId'], so ChallengeResponse.responderId was the empty string
// on every battle in the app, and had been for as long as the field existed.
// No error was logged on either side.
//
// These tests pin the names the client actually reads. They are deliberately
// literal — asserting the wire spelling, not the struct field — because the
// struct field name is what looks right at a glance and is not what anyone
// consumes.
func TestChallengeResponseWireNames(t *testing.T) {
	blob, err := json.Marshal(ChallengeResponse{
		ID:                "55",
		ChallengeID:       "104",
		ResponderID:       "1",
		ResponderUsername: "player1",
		VideoURL:          "https://cdn.example/r.mp4",
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var wire map[string]any
	if err := json.Unmarshal(blob, &wire); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	for _, name := range []string{
		"id", "challengeId", "responderId", "responderUsername", "videoUrl",
	} {
		if _, ok := wire[name]; !ok {
			t.Errorf("ChallengeResponse is missing %q on the wire; the client "+
				"reads that name and will see a zero value without it", name)
		}
	}

	if got := wire["responderId"]; got != "1" {
		t.Errorf("responderId = %v, want \"1\"", got)
	}
	// The specific misspelling that shipped. Worth naming so a re-introduction
	// fails with the reason rather than just a missing key.
	if _, ok := wire["responderld"]; ok {
		t.Error("ChallengeResponse serialises responderld (lowercase L); the " +
			"client reads responderId and will always see an empty string")
	}
}

// AcceptChallengePayload is the inbound half of the same relationship and
// already spelled the field correctly. Pinned alongside so the two halves
// cannot drift apart again — they are the same identity travelling in
// opposite directions.
func TestAcceptChallengePayloadWireNames(t *testing.T) {
	var payload AcceptChallengePayload
	if err := json.Unmarshal([]byte(
		`{"challengeId":"104","responderId":"1","videoUrl":"u"}`,
	), &payload); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if payload.ResponderID != "1" {
		t.Errorf("ResponderID = %q, want \"1\" — the inbound tag no longer "+
			"matches what clients send", payload.ResponderID)
	}
}
