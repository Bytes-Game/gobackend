package main

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

// ════════════════════════════════════════════════════════════════════════════
// A RUNG THE BACKEND REFUSES IS A FILE NOBODY EVER PLAYS
// ════════════════════════════════════════════════════════════════════════════
//
// The worker encodes a rendition, uploads it, and posts the label back here.
// storeVideoVariants drops any label that is not in videoVariantLabels — on
// purpose, because "whatever the worker sends" must not be the same thing as
// "whatever gets served to a viewer".
//
// The cost of forgetting is invisible from every side. The worker's log says
// the encode succeeded. The storage bill says the file is there. The app just
// never hears about it, so the rung exists and nothing plays it.
//
// The worker is a separate program (cmd/hls-worker), so its ladder cannot be
// imported here. This reads its source instead: one side of the comparison is
// the real map, the other is the real ladder, and neither is a list somebody
// copied by hand into a test.

var ladderLabelRe = regexp.MustCompile(`\{label:\s*"([^"]+)"`)

// progressiveLadderLabelsFromSource reads the worker's ladder out of its own
// file. It fails rather than returning nothing, because "I found no rungs" and
// "every rung is fine" must not look the same.
func progressiveLadderLabelsFromSource(t *testing.T) []string {
	t.Helper()
	const path = "cmd/hls-worker/progressive.go"
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("could not read %s, so this check is not running: %v", path, err)
	}
	src := string(raw)
	start := strings.Index(src, "var progressiveLadder = []progressiveRendition{")
	if start < 0 {
		t.Fatalf("%s no longer declares progressiveLadder the way this test "+
			"reads it. Rather than pass on nothing, fix the test.", path)
	}
	// Up to the closing brace of the slice literal, which is the first line
	// in the file after start that is exactly "}".
	rest := src[start:]
	end := strings.Index(rest, "\n}\n")
	if end < 0 {
		t.Fatalf("could not find the end of the progressiveLadder literal in %s", path)
	}
	body := rest[:end]

	var labels []string
	for _, m := range ladderLabelRe.FindAllStringSubmatch(body, -1) {
		labels = append(labels, m[1])
	}
	if len(labels) == 0 {
		t.Fatalf("found no rungs in %s. An empty list would make this test "+
			"pass while checking nothing.", path)
	}
	return labels
}

func TestWorkerLadderLabelsAreAccepted(t *testing.T) {
	for _, label := range progressiveLadderLabelsFromSource(t) {
		if !videoVariantLabels[label] {
			t.Errorf("the worker encodes a %q rendition and videoVariantLabels "+
				"does not list it. Every one of those files is encoded, "+
				"uploaded, paid for, and then thrown away on the way in — "+
				"with nothing in any log to say so.", label)
		}
	}
}

func TestWorkerLadderLabelsAreStorable(t *testing.T) {
	// The second half of the same wire. A label that passes the allow-list
	// but has no file extension cannot be given a key in the bucket, so the
	// presigned-upload path would refuse it.
	//
	// This is the path a phone uses for its own upload, and the app now names
	// that upload after the same ladder — see labelForLongSide in the app's
	// NetworkQualityService, which returns "360p" for a small file.
	for _, label := range progressiveLadderLabelsFromSource(t) {
		if variantToExt[label] == "" {
			t.Errorf("a phone uploading a %q file has nowhere to put it: "+
				"variantToExt has no extension for that label, so the "+
				"presign request is refused and the upload fails outright.",
				label)
		}
	}
}

func TestWorkerLadderIsNotEmptyToThisTest(t *testing.T) {
	// The reverse check. If the reader above ever stops finding rungs it must
	// fail loudly, not report a clean pass over an empty list — the same shape
	// as a CI run whose database never started.
	got := progressiveLadderLabelsFromSource(t)
	if len(got) < 2 {
		t.Fatalf("read only %v from the worker's ladder. It has more rungs "+
			"than that, so the reader is broken and every check above it is "+
			"checking nothing.", got)
	}
	// And it really is reading the file, not a list baked in here.
	if !containsLabel(got, "720p") {
		t.Errorf("the worker's ladder read as %v, with no 720p in it. That is "+
			"not a ladder this app could serve.", got)
	}
}

func containsLabel(list []string, want string) bool {
	for _, s := range list {
		if s == want {
			return true
		}
	}
	return false
}
