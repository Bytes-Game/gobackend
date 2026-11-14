package main

// Which address goes in front of the files a job makes. See publicBaseFor.

import (
	"os"
	"strings"
	"testing"
)

// codeWithoutComments is a source file with its comment lines taken out,
// so a check on the code cannot pass on the words explaining it.
func codeWithoutComments(t *testing.T, name string) string {
	t.Helper()
	raw, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	var code []string
	for _, l := range strings.Split(string(raw), "\n") {
		if !strings.HasPrefix(strings.TrimSpace(l), "//") {
			code = append(code, l)
		}
	}
	return strings.Join(code, "\n")
}

func TestPublicBase_TheServersAddressWins(t *testing.T) {
	own := &workerConfig{R2PublicBase: "https://left-behind.test"}
	none := &workerConfig{}

	for _, tc := range []struct {
		name string
		cfg  *workerConfig
		sent string
		want string
	}{
		// The day the videos move: the server sends the new address, and
		// the worker's settings still hold the old one.
		{"both, and they differ", own, "https://media.flexete.com", "https://media.flexete.com"},
		{"only the server's", none, "https://media.flexete.com", "https://media.flexete.com"},
		{"the server's, with a slash and spaces", own, " https://media.flexete.com/ ", "https://media.flexete.com"},
		// A server too old to send one.
		{"only the worker's own", own, "", "https://left-behind.test"},
		{"the server sent only spaces", own, "   ", "https://left-behind.test"},
	} {
		got, err := publicBaseFor(tc.cfg, pendingJob{PublicBaseURL: tc.sent})
		if err != nil || got != tc.want {
			t.Errorf("%s: %q, %v; want %q", tc.name, got, err, tc.want)
		}
	}

	// A server with no address of its own sends a guess that Cloudflare
	// does not answer at. The worker's own address beats that...
	keys := &workerConfig{R2AccountID: "acct", R2Bucket: "bucket", R2PublicBase: "https://left-behind.test"}
	const guess = "https://pub-acct.r2.dev/bucket"
	if got, err := publicBaseFor(keys, pendingJob{PublicBaseURL: guess}); err != nil || got != "https://left-behind.test" {
		t.Errorf("the server's guess against the worker's own: %q, %v", got, err)
	}
	// ...and only that: any other address the server sends still wins,
	// one that merely looks like the guess included.
	for _, sent := range []string{"https://pub-acct.r2.dev", "https://pub-other.r2.dev/bucket", "https://media.flexete.com"} {
		if got, err := publicBaseFor(keys, pendingJob{PublicBaseURL: sent}); err != nil || got != sent {
			t.Errorf("the server sent %q: got %q, %v", sent, got, err)
		}
	}
	// With nothing of its own, the worker uses the guess, as it always
	// has: there is nothing better to hand.
	bare := &workerConfig{R2AccountID: "acct", R2Bucket: "bucket"}
	if got, err := publicBaseFor(bare, pendingJob{PublicBaseURL: guess}); err != nil || got != guess {
		t.Errorf("the guess with nothing else to use: %q, %v", got, err)
	}

	// Neither: the job fails and says why, rather than guess an address.
	for _, cfg := range []*workerConfig{none, nil} {
		got, err := publicBaseFor(cfg, pendingJob{})
		if err == nil || got != "" {
			t.Fatalf("with no address anywhere: %q, %v; want an error", got, err)
		}
		if !strings.Contains(err.Error(), "R2_PUBLIC_BASE_URL") {
			t.Errorf("the error does not name the setting: %v", err)
		}
	}
}

// The job itself asks publicBaseFor, and reads the worker's own setting
// nowhere else. Checked in the code, comments left out, because the test
// that runs a whole job needs ffmpeg and is skipped where there is none.
func TestPublicBase_TheJobAsksForIt(t *testing.T) {
	src := codeWithoutComments(t, "main.go")
	start := strings.Index(src, "func processJob(")
	if start < 0 {
		t.Fatal("processJob is gone")
	}
	body := src[start:]
	if end := strings.Index(body, "\n}\n"); end > 0 {
		body = body[:end]
	}
	if !strings.Contains(body, "publicBaseFor(cfg, job)") {
		t.Error("processJob no longer asks publicBaseFor which address to use")
	}
	if strings.Contains(body, "cfg.R2PublicBase") || strings.Contains(body, "job.PublicBaseURL") {
		t.Error("processJob picks an address by itself, past publicBaseFor")
	}
}
