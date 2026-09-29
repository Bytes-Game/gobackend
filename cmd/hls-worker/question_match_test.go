package main

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// The challenge a video was posted for travels from the job, through the
// analysis, into the model's prompt — and the model's answer comes back out
// in the reading the server stores. Run for real, with a stand-in for the
// model: a script that writes down what it was asked and answers "no".

func TestMatch_TheChallengeReachesTheModelAndItsAnswerComesBack(t *testing.T) {
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		t.Skip("needs ffmpeg to make a video to look at")
	}
	dir := t.TempDir()
	video := filepath.Join(dir, "silent.mp4")
	if out, err := exec.Command("ffmpeg", "-y", "-loglevel", "error",
		"-f", "lavfi", "-i", "testsrc=duration=3:size=160x120:rate=10",
		"-pix_fmt", "yuv420p", video).CombinedOutput(); err != nil {
		t.Fatalf("make a test video: %v\n%s", err, out)
	}

	asked := filepath.Join(dir, "asked.txt")
	model := filepath.Join(dir, "model.sh")
	script := "#!/bin/sh\nprintf '%s\\n' \"$@\" > " + asked + "\n" +
		`echo '{"categories": ["sports"], "feelings": [], "topics": ["cat"], "matches": "no"}'` + "\n"
	if err := os.WriteFile(model, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	// Nothing is said in the video, so it is the frames pass that looks.
	t.Setenv(understandBinEnv, "")
	t.Setenv(framesBinEnv, model)
	t.Setenv(understandModelEnv, filepath.Join(dir, "model.gguf"))
	t.Setenv(framesProjectorEnv, filepath.Join(dir, "mmproj.gguf"))

	a := analyzeVideo(context.Background(), video, "Who can juggle five balls?")
	prompt, err := os.ReadFile(asked)
	if err != nil {
		t.Fatalf("the model was never asked anything: %v (passes %v)", err, a.Passes)
	}
	if !strings.Contains(string(prompt), "Who can juggle five balls?") {
		t.Errorf("the prompt does not carry the challenge:\n%s", prompt)
	}
	if a.QuestionMatch != "no" {
		t.Errorf("the reading says questionMatch=%q, want the model's \"no\"", a.QuestionMatch)
	}
	if !strings.Contains(string(analysisJSON(a)), `"questionMatch":"no"`) {
		t.Errorf("what goes to the server leaves the answer out: %s", analysisJSON(a))
	}

	// With no challenge, nothing is asked, and whatever the model says
	// about a match it was never asked for is not taken.
	a = analyzeVideo(context.Background(), video, "")
	prompt, _ = os.ReadFile(asked)
	if strings.Contains(string(prompt), `"matches"`) {
		t.Errorf("asked about a match with no challenge to match:\n%s", prompt)
	}
	if a.QuestionMatch != "" {
		t.Errorf("took the model's %q about a match nobody asked for", a.QuestionMatch)
	}
}

// The same for a video somebody talks in: the words are read, not the
// pictures looked at, and the question goes with the words.
func TestMatch_AVideoThatTalksIsAskedToo(t *testing.T) {
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		t.Skip("needs ffmpeg to make a video to listen to")
	}
	dir := t.TempDir()
	video := filepath.Join(dir, "talking.mp4")
	if out, err := exec.Command("ffmpeg", "-y", "-loglevel", "error",
		"-f", "lavfi", "-i", "testsrc=duration=3:size=160x120:rate=10",
		"-f", "lavfi", "-i", "sine=frequency=440:duration=3",
		"-pix_fmt", "yuv420p", "-shortest", video).CombinedOutput(); err != nil {
		t.Fatalf("make a test video: %v\n%s", err, out)
	}
	whisper := filepath.Join(dir, "whisper.sh")
	if err := os.WriteFile(whisper, []byte("#!/bin/sh\n"+
		"echo 'today I am going to cook a big pot of rice for my family'\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	asked := filepath.Join(dir, "asked.txt")
	model := filepath.Join(dir, "model.sh")
	if err := os.WriteFile(model, []byte("#!/bin/sh\nprintf '%s\\n' \"$@\" > "+asked+"\n"+
		`echo '{"categories": ["food"], "feelings": [], "topics": ["rice"], "matches": "no"}'`+"\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv(whisperBinEnv, whisper)
	t.Setenv(whisperModelEnv, filepath.Join(dir, "whisper.bin"))
	t.Setenv(understandBinEnv, model)
	t.Setenv(understandModelEnv, filepath.Join(dir, "model.gguf"))
	t.Setenv(framesBinEnv, "")

	a := analyzeVideo(context.Background(), video, "Who can juggle five balls?")
	prompt, err := os.ReadFile(asked)
	if err != nil {
		t.Fatalf("the model was never asked: %v (passes %v, speech %q)", err, a.Passes, a.Speech)
	}
	if !strings.Contains(string(prompt), "Who can juggle five balls?") ||
		!strings.Contains(string(prompt), "pot of rice") {
		t.Errorf("the prompt should carry both the words and the challenge:\n%s", prompt)
	}
	if a.QuestionMatch != "no" {
		t.Errorf("questionMatch=%q from the reading pass, want \"no\" (passes %v)", a.QuestionMatch, a.Passes)
	}
}

func TestMatch_TheJobsQuestionIsWhatTheAnalysisIsGiven(t *testing.T) {
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
	if !strings.Contains(strings.Join(code, "\n"), "analyzeVideo(ctx, srcPath, job.Question)") {
		t.Error("the worker no longer hands the job's question to the analysis — " +
			"the server sends it and nothing asks it")
	}
}
