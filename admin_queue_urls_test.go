package main

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

// What the app would actually PLAY, on the row that says why it is not
// watchable.
//
// Before this, checking a finished video meant guessing. The worker names its
// output directory with eight random hex characters, so nothing reconstructs
// the URL from the id, and every other way to reach it falls short: the arena
// list leaves rows out, and a challenge's own page counts a view for looking.
// Verifying that four re-encoded videos came back the right length took three
// endpoints and still could not cover the fourth.

func TestParseVariantMap_ReadsTheColumn(t *testing.T) {
	got := parseVariantMap(`{"480p":"https://cdn/x/480p.mp4","720p":"https://cdn/x/720p.mp4"}`)
	if len(got) != 2 || got["480p"] != "https://cdn/x/480p.mp4" {
		t.Errorf("got %v", got)
	}
}

func TestParseVariantMap_NothingIsNothing(t *testing.T) {
	for _, raw := range []string{"", "{}", "null"} {
		if got := parseVariantMap(raw); got != nil {
			t.Errorf("%q -> %v, want nothing", raw, got)
		}
	}
}

// One malformed column must not take the whole answer down. This endpoint
// exists to explain why a video is stuck; refusing to answer because one row
// is broken would hide the other fifty-five — and a row nobody can see is
// exactly the failure this endpoint was written to end.
func TestParseVariantMap_OneBadRowDoesNotFailTheRequest(t *testing.T) {
	for _, raw := range []string{`{oops`, `["a","b"]`, `{"480p":42}`, `not json at all`} {
		if got := parseVariantMap(raw); got != nil {
			t.Errorf("%q -> %v, want nothing rather than a guess", raw, got)
		}
	}
}

// The claim marker is a sentinel sitting in a URL column. A reader that does
// not know that hands the string "PENDING" to a video player, which has
// happened before — see the guard in feed_engine.go.
func TestQueueNeverReportsTheClaimMarkerAsAURL(t *testing.T) {
	src, err := os.ReadFile("admin_hls_queue.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(src), `manifest != "" && manifest != hlsClaimMarker`) {
		t.Error("the manifest is copied out without checking it is not the " +
			"claim marker, so a row being worked on reports PENDING as its URL")
	}
}

// Every spelling of the marker has to be the same string. The SQL still
// writes it inline — rewriting eight query strings to interpolate a constant
// is more risk than it removes — so this is what holds them together.
func TestEveryClaimMarkerSpellingAgrees(t *testing.T) {
	if hlsClaimMarker != "PENDING" {
		t.Fatalf("marker is %q; every query below still says 'PENDING'", hlsClaimMarker)
	}
	// Scoped to lines that mention the column, because 'pending' is a
	// perfectly good status value on other tables and matching those would
	// make this test fail for a reason that has nothing to do with it.
	//
	// What it catches is a typo in the one place it matters: the claim
	// writing 'PENDING' and the reaper looking for 'PENDNG' would strand
	// every row a dead worker was holding, permanently and silently.
	quoted := regexp.MustCompile(`'([^']*)'`)
	checked := 0
	for _, f := range []string{
		"hls_worker_api.go", "database.go", "feed_engine.go", "media_requeue.go",
	} {
		src, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		for _, line := range strings.Split(string(src), "\n") {
			if !strings.Contains(line, "hls_manifest_url") {
				continue
			}
			for _, m := range quoted.FindAllStringSubmatch(line, -1) {
				if m[1] == "" {
					continue // the empty string is the "not started" state
				}
				checked++
				if m[1] != hlsClaimMarker {
					t.Errorf("%s spells the claim marker %q, not %q\n  %s",
						f, m[1], hlsClaimMarker, strings.TrimSpace(line))
				}
			}
		}
	}
	if checked == 0 {
		t.Error("no claim-marker literals were found at all, so this test " +
			"checked nothing — the queries must have moved")
	}
}

// The row has to actually carry the fields, and the query has to actually
// select the column they come from.
func TestQueueRowCarriesWhatThePlayerWouldUse(t *testing.T) {
	src, err := os.ReadFile("admin_hls_queue.go")
	if err != nil {
		t.Fatal(err)
	}
	body := string(src)
	for _, want := range []string{
		`ManifestURL string            ` + "`" + `json:"manifestUrl,omitempty"` + "`",
		`Variants    map[string]string ` + "`" + `json:"variants,omitempty"` + "`",
		"COALESCE(video_variants::text, '{}')",
		"Variants:  parseVariantMap(variants)",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("missing %q — the row cannot report what the app plays", want)
		}
	}
}
