package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// The job's time limit, and the two things that keep a long video inside it:
// uploading several files at once, and making the H.265 copies only in the
// time that is left. See "TWO HALVES" in progressive.go and uploadParallel in
// main.go for the video that needed both.

func TestFitsInTime_CountsTheEncodeTheUploadAndTheMargin(t *testing.T) {
	now := time.Now()
	b := extrasBudget{deadline: now.Add(10 * time.Minute), uploadBytesPerSec: 1 << 20}

	// 5m to make + 60 MB at 1 MB/s (1m) + 1m margin = 7m, inside 10m.
	if !fitsInTime(now, b, 5*time.Minute, 60<<20) {
		t.Error("a file needing 7 of the 10 minutes left was refused")
	}
	// 8m + 1m + 1m = 10m: not inside.
	if fitsInTime(now, b, 8*time.Minute, 60<<20) {
		t.Error("a file needing all 10 minutes left, with nothing to spare, was allowed")
	}
	// Quick to make, slow to send: 400 MB at 1 MB/s is nearly 7 minutes.
	if fitsInTime(now, b, 3*time.Minute, 400<<20) {
		t.Error("the upload was not counted: 3m to make but ~7m to send, in 10m")
	}
	// Speed never measured: assume the slow fallback, not a free upload.
	if fitsInTime(now, extrasBudget{deadline: now.Add(5 * time.Minute)}, 0, 600<<20) {
		t.Error("an unmeasured upload of 600 MB was treated as instant")
	}
	// No deadline: everything fits.
	if !fitsInTime(now, extrasBudget{}, time.Hour, 1<<40) {
		t.Error("with no time limit a file was still refused")
	}
}

// fakeBucket stands in for putFile and records what arrived.
type fakeBucket struct {
	mu       sync.Mutex
	got      map[string]int64
	inFlight int
	most     int
	delay    time.Duration
	failKey  string
	// failSuffix fails every key ending with it.
	failSuffix string
	// onPut runs for every file before it is recorded.
	onPut func(local, key string)
}

func useFakeBucket(t *testing.T, b *fakeBucket) {
	t.Helper()
	b.got = map[string]int64{}
	old := putFile
	putFile = func(ctx context.Context, cfg *workerConfig, local, key string) error {
		b.mu.Lock()
		b.inFlight++
		if b.inFlight > b.most {
			b.most = b.inFlight
		}
		b.mu.Unlock()
		defer func() {
			b.mu.Lock()
			b.inFlight--
			b.mu.Unlock()
		}()
		if b.onPut != nil {
			b.onPut(local, key)
		}
		select {
		case <-time.After(b.delay):
		case <-ctx.Done():
			return ctx.Err()
		}
		if key == b.failKey || (b.failSuffix != "" && strings.HasSuffix(key, b.failSuffix)) {
			return errors.New("the bucket said no")
		}
		fi, err := os.Stat(local)
		if err != nil {
			return err
		}
		b.mu.Lock()
		b.got[key] = fi.Size()
		b.mu.Unlock()
		return nil
	}
	t.Cleanup(func() { putFile = old })
}

// writeTree makes a folder shaped like the converter's output: a few stream
// folders full of pieces, and some files at the top.
func writeTree(t *testing.T) (string, []string) {
	t.Helper()
	dir := t.TempDir()
	var keys []string
	for _, rung := range []string{"240p", "360p", "480p", "720p"} {
		if err := os.MkdirAll(filepath.Join(dir, rung), 0o755); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 10; i++ {
			name := "seg_" + strconv.Itoa(i) + ".ts"
			if err := os.WriteFile(filepath.Join(dir, rung, name), []byte(rung+name), 0o644); err != nil {
				t.Fatal(err)
			}
			keys = append(keys, "p/"+rung+"/"+name)
		}
	}
	for _, name := range []string{"master.m3u8", "480p.mp4", "720p.mp4"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(name), 0o644); err != nil {
			t.Fatal(err)
		}
		keys = append(keys, "p/"+name)
	}
	return dir, keys
}

func TestUploadTree_SendsEveryFileSeveralAtOnce(t *testing.T) {
	b := &fakeBucket{delay: 20 * time.Millisecond}
	useFakeBucket(t, b)
	dir, keys := writeTree(t)

	u, err := uploadTree(context.Background(), &workerConfig{}, dir, "p")
	if err != nil {
		t.Fatal(err)
	}
	if u.files != len(keys) || len(b.got) != len(keys) {
		t.Fatalf("sent %d files (bucket has %d), want %d", u.files, len(b.got), len(keys))
	}
	for _, k := range keys {
		if _, ok := b.got[k]; !ok {
			t.Errorf("%s never arrived", k)
		}
	}
	if b.most < 2 {
		t.Errorf("at most %d file at a time — one at a time is what ran "+
			"challenge 9 out of time", b.most)
	}
	if b.most > uploadParallel {
		t.Errorf("%d files at once, more than the %d allowed", b.most, uploadParallel)
	}
	if u.bytes == 0 || u.bytesPerSec() <= 0 {
		t.Errorf("the upload measured nothing (%d bytes, %.0f B/s) — the "+
			"H.265 half has no speed to plan with", u.bytes, u.bytesPerSec())
	}
}

func TestUploadTree_AFailureIsReportedAndStopsTheRest(t *testing.T) {
	b := &fakeBucket{delay: 5 * time.Millisecond, failKey: "p/360p/seg_3.ts"}
	useFakeBucket(t, b)
	dir, keys := writeTree(t)

	_, err := uploadTree(context.Background(), &workerConfig{}, dir, "p")
	if err == nil || !strings.Contains(err.Error(), "p/360p/seg_3.ts") {
		t.Fatalf("error %v, want one naming the file that failed", err)
	}
	if len(b.got) >= len(keys)-1 {
		t.Errorf("%d of %d files went up after one failed — the rest should "+
			"stop, since a job with a hole in it is failed anyway", len(b.got), len(keys))
	}
}

// makeBigLongSource is makeSource at six seconds, long enough that an encode
// cannot finish inside a fraction of a second on any machine.
func makeBigLongSource(t *testing.T, dir, name string) string {
	t.Helper()
	needFFmpeg(t)
	if out, err := exec.Command("ffmpeg", "-hide_banner", "-encoders").Output(); err != nil ||
		!strings.Contains(string(out), "libx265") {
		if os.Getenv("REQUIRE_FFMPEG") != "" {
			t.Fatal("REQUIRE_FFMPEG is set but this ffmpeg has no H.265 encoder")
		}
		t.Skip("this ffmpeg has no H.265 encoder")
	}
	path := filepath.Join(dir, name)
	target := itoa(aboveEveryCeiling()/1000) + "k"
	out, err := exec.Command("ffmpeg", "-y",
		"-f", "lavfi", "-i", "color=c=black:s=1920x1080:r=30:d=6,noise=alls=80:allf=t+u",
		"-f", "lavfi", "-i", "sine=frequency=440:duration=6",
		"-c:v", "libx264", "-preset", "ultrafast", "-pix_fmt", "yuv420p",
		"-b:v", target, "-maxrate", target, "-bufsize", target,
		"-c:a", "aac", "-shortest", "-movflags", "+faststart",
		path,
	).CombinedOutput()
	if err != nil {
		t.Fatalf("could not build a source clip: %v: %s", err, lastLine(string(out)))
	}
	return path
}

// runWholeJob runs processJob on a 1080p clip served over HTTP, into a
// pretend bucket.
func runWholeJob(t *testing.T, ctx context.Context, b *fakeBucket) jobResult {
	t.Helper()
	dir := t.TempDir()
	makeBigLongSource(t, dir, "big.mp4")
	srv := httptest.NewServer(http.FileServer(http.Dir(dir)))
	t.Cleanup(srv.Close)
	useFakeBucket(t, b)

	res, err := processJob(ctx, &workerConfig{R2PublicBase: "https://cdn.test"}, pendingJob{
		ChallengeID: "77", SourceURL: srv.URL + "/big.mp4", Kind: "challenge", MaxSeconds: 180,
	})
	if err != nil {
		t.Fatalf("processJob: %v", err)
	}
	return res
}

func uploadedLabel(b *fakeBucket, label string) bool {
	for k := range b.got {
		if strings.HasSuffix(k, "/"+label+".mp4") {
			return true
		}
	}
	return false
}

func TestProcessJob_TheH264HalfIsUpBeforeAnyH265FileIsMade(t *testing.T) {
	var early []string
	b := &fakeBucket{}
	// At the moment an H.264 file goes up, no H.265 file may exist yet
	// anywhere in the job's folder. If one does, the job went back to making
	// everything before uploading anything — the order that lost challenge 9.
	b.onPut = func(local, key string) {
		if !strings.HasSuffix(key, ".mp4") || strings.Contains(key, "_hevc") {
			return
		}
		work := filepath.Dir(filepath.Dir(local))
		if found, _ := filepath.Glob(filepath.Join(work, "*", "*_hevc.mp4")); len(found) > 0 {
			b.mu.Lock()
			early = append(early, filepath.Base(local)+" went up after "+filepath.Base(found[0])+" was made")
			b.mu.Unlock()
		}
	}
	res := runWholeJob(t, context.Background(), b)

	if len(early) > 0 {
		t.Errorf("the H.265 half ran before the H.264 half was uploaded: %v", early)
	}
	// With no time limit, the H.265 half is made, uploaded and reported.
	for _, label := range []string{"480p_hevc", "720p_hevc", "720p", "480p", "360p"} {
		if _, ok := res.VideoVariants[label]; !ok {
			t.Errorf("no %s in the result: %v", label, res.VideoVariants)
		}
		if !uploadedLabel(b, label) {
			t.Errorf("%s is in the result but never reached the bucket", label)
		}
	}
	if !strings.HasSuffix(res.ManifestURL, "/master.m3u8") {
		t.Errorf("manifest %q", res.ManifestURL)
	}
}

func TestProcessJob_NoTimeForH265StillFinishesWithEverythingElse(t *testing.T) {
	old := hevcCostFactor
	hevcCostFactor = 1e6 // every H.265 file would take for ever
	t.Cleanup(func() { hevcCostFactor = old })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	b := &fakeBucket{}
	res := runWholeJob(t, ctx, b)

	for _, label := range []string{"480p_hevc", "720p_hevc"} {
		if _, ok := res.VideoVariants[label]; ok {
			t.Errorf("%s is in the result though there was no time to make it", label)
		}
		if uploadedLabel(b, label) {
			t.Errorf("%s was uploaded though there was no time to make it", label)
		}
		settled := false
		for _, l := range res.Ladder {
			settled = settled || l == label
		}
		if !settled {
			t.Errorf("%s was left out for time but not settled (ladder %v) — "+
				"the backfill would send this video round again, to run out "+
				"of time at the same place", label, res.Ladder)
		}
	}
	for _, label := range []string{"720p", "480p", "360p"} {
		if _, ok := res.VideoVariants[label]; !ok || !uploadedLabel(b, label) {
			t.Errorf("the H.264 %s is missing — a video short of time must "+
				"still finish with everything else", label)
		}
	}
}

func TestProcessJob_AFailedH265UploadDoesNotFailTheJob(t *testing.T) {
	b := &fakeBucket{failSuffix: "_hevc.mp4"}
	res := runWholeJob(t, context.Background(), b)

	for _, label := range []string{"480p_hevc", "720p_hevc"} {
		if _, ok := res.VideoVariants[label]; ok {
			t.Errorf("%s is in the result but never arrived — a phone would "+
				"be sent to a file that is not there", label)
		}
		for _, l := range res.Ladder {
			if l == label {
				t.Errorf("%s failed to upload but was settled, so no backfill "+
					"would ever try it again", label)
			}
		}
	}
	if _, ok := res.VideoVariants["720p"]; !ok || res.ManifestURL == "" {
		t.Errorf("the job lost what had already gone up: %v", res.VideoVariants)
	}
}

func TestFinishExtras_AnEncodeThatRunsLongIsStoppedAndSettled(t *testing.T) {
	old := hevcCostFactor
	hevcCostFactor = 0 // guess every H.265 file to be instant
	t.Cleanup(func() { hevcCostFactor = old })

	dir := t.TempDir()
	src := makeBigLongSource(t, dir, "big.mp4")
	out := filepath.Join(dir, "out")
	if err := os.MkdirAll(out, 0o755); err != nil {
		t.Fatal(err)
	}
	run := startProgressive(context.Background(), src, out, 0)

	// The guess says it fits, with half a second to spare. Six seconds of
	// 1080p does not encode in half a second anywhere, so the encode's own
	// stop is what ends it.
	run.finishExtras(context.Background(), out, extrasBudget{
		deadline:          time.Now().Add(extrasMargin + 500*time.Millisecond),
		uploadBytesPerSec: 1 << 40,
	})
	for _, label := range []string{"480p_hevc", "720p_hevc"} {
		if _, ok := run.made[label]; ok {
			t.Errorf("%s was kept though its time ran out", label)
		}
		if !run.outOfTime[label] {
			t.Errorf("%s ran out of time but is not recorded as such", label)
		}
		if _, err := os.Stat(filepath.Join(out, label+".mp4")); err == nil {
			t.Errorf("a half-made %s.mp4 was left on disk", label)
		}
	}
	got := strings.Join(run.outcome().settled, ",")
	if !strings.Contains(got, "480p_hevc") || !strings.Contains(got, "720p_hevc") {
		t.Errorf("settled %s — the stopped rungs should be settled", got)
	}
}
