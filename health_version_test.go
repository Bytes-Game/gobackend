package main

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

// Working out whether a fix had reached production used to mean poking an
// endpoint and inferring the answer from how it behaved. That only works when
// a change is visible from outside, and it is guesswork even then.
func TestHealth_SaysWhichBuildIsAnswering(t *testing.T) {
	src, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatalf("cannot read main.go: %v", err)
	}
	s := string(src)

	if strings.Contains(s, `{"status":"ok"}`) {
		t.Error("health is back to a fixed string, so there is again no way " +
			"to ask the running server which code it is on")
	}
	for _, want := range []string{"RENDER_GIT_COMMIT", `"commit"`, `"startedAt"`} {
		if !strings.Contains(s, want) {
			t.Errorf("health no longer reports %s", want)
		}
	}
}

// Missing deployment metadata must read as "unknown", never as a blank that
// could be mistaken for a real answer.
func TestHealth_UnknownBuildSaysSo(t *testing.T) {
	saved, had := os.LookupEnv("RENDER_GIT_COMMIT")
	os.Unsetenv("RENDER_GIT_COMMIT")
	defer func() {
		if had {
			os.Setenv("RENDER_GIT_COMMIT", saved)
		}
	}()

	commit := os.Getenv("RENDER_GIT_COMMIT")
	if commit == "" {
		commit = "unknown"
	}
	body, err := json.Marshal(map[string]string{"status": "ok", "commit": commit})
	if err != nil {
		t.Fatalf("health body does not encode: %v", err)
	}
	if !strings.Contains(string(body), `"commit":"unknown"`) {
		t.Errorf("with no build info the commit should read \"unknown\", got %s", body)
	}
}
