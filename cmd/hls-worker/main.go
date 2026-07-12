// hls-worker is a separate binary deployed alongside the main devb
// backend (e.g. as a Render Background Worker). It polls the backend
// for un-transcoded challenges, downloads each source video from R2,
// runs FFmpeg locally to produce an HLS bitrate ladder + segments,
// uploads the result back to R2, and reports completion.
//
// Architecture rationale (mirrors hls_worker_api.go on the backend):
//   * The main backend runs on Render's free tier with limited CPU.
//     FFmpeg transcoding a 60s 1080p reel can burn 30-90s of CPU,
//     which would tie up the request handler past Render's 30s
//     timeout. Keeping the transcode work on a separate process keeps
//     the API responsive.
//   * Pull-based queue: this worker polls /internal/hls/next-pending,
//     which uses Postgres' FOR UPDATE SKIP LOCKED to give exactly one
//     job to exactly one worker. Scaling means running more replicas
//     of THIS binary; no scheduler / cron / message bus.
//
// Deployment:
//   * Dockerfile in this directory installs FFmpeg + ships this binary.
//   * Render Background Worker (free tier: 750 hours/month) is the
//     intended host. Same env vars as the main backend for R2
//     (R2_ACCOUNT_ID, R2_BUCKET, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY,
//     R2_PUBLIC_BASE_URL) plus:
//       - BACKEND_URL = https://gobackend-9nd8.onrender.com
//       - HLS_WORKER_TOKEN = (must match the backend's value)
//
// Failure mode: any error during a job leaves the row marked PENDING
// in the DB; the worker calls /internal/hls/fail to reset it to ''
// so another attempt can claim it. If a source video is genuinely
// broken (corrupt mp4, unsupported codec) the row keeps getting picked
// + failed until someone notices in logs — an upgrade for later would
// be an attempt counter.

package main

import (
	"bytes"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// HLS ladder — same labels the existing client-side multi-bitrate code
// expects, with conservative bitrates tuned for short-form vertical
// video. Bitrates are video-only; audio is muxed in at 96k AAC for
// every rendition (so segments stay aligned across qualities).
type rendition struct {
	label    string // "240p" | "360p" | "480p" | "720p" | "1080p"
	width    int
	height   int
	videoBps int
	audioBps int
}

// We start the ladder lower than the legacy MP4 lineup (which capped
// at 480p) because HLS lets the player pick higher renditions per
// segment — so the cellular default just sits at 240p/360p and only
// upgrades when bandwidth proves it can sustain the next step.
var ladder = []rendition{
	{label: "240p", width: 426, height: 240, videoBps: 300_000, audioBps: 64_000},
	{label: "360p", width: 640, height: 360, videoBps: 600_000, audioBps: 96_000},
	{label: "480p", width: 854, height: 480, videoBps: 1_000_000, audioBps: 96_000},
	{label: "720p", width: 1280, height: 720, videoBps: 2_500_000, audioBps: 128_000},
	{label: "1080p", width: 1920, height: 1080, videoBps: 4_500_000, audioBps: 128_000},
}

const (
	segmentSeconds = 2 // industry standard for short-form video
)

func main() {
	pollInterval := flag.Duration("poll", 5*time.Second, "interval between empty-queue polls")
	// -drain: process everything pending, then EXIT instead of polling
	// forever. This is what makes free scheduled runners (GitHub
	// Actions cron) a viable $0 transcode fleet: each run drains the
	// backlog and terminates, so a 30-minute cron behaves like an
	// always-on worker with ≤30min latency and zero hosting cost.
	drain := flag.Bool("drain", false, "exit once the queue is empty instead of polling forever")
	flag.Parse()

	cfg, err := loadConfig()
	if err != nil {
		log.Fatalf("config error: %v", err)
	}
	log.Printf("hls-worker starting; backend=%s bucket=%s drain=%v", cfg.BackendURL, cfg.R2Bucket, *drain)

	// Single-process loop. Multiple replicas can run safely because
	// /internal/hls/next-pending uses FOR UPDATE SKIP LOCKED on the
	// backend side — two workers will never claim the same row.
	emptyPolls := 0
	for {
		job, err := claimJob(cfg)
		if err != nil {
			log.Printf("claim error: %v (sleeping)", err)
			time.Sleep(*pollInterval)
			continue
		}
		if job == nil {
			if *drain {
				// Two consecutive empty polls = genuinely drained (one
				// could race a just-failed job being reset).
				emptyPolls++
				if emptyPolls >= 2 {
					log.Println("queue drained — exiting (drain mode)")
					return
				}
			}
			time.Sleep(*pollInterval)
			continue
		}
		emptyPolls = 0
		log.Printf("claimed job kind=%s id=%s source=%s", jobKind(*job), job.ChallengeID, job.SourceURL)
		manifestURL, err := processJob(cfg, *job)
		if err != nil {
			log.Printf("process error for %s=%s: %v", jobKind(*job), job.ChallengeID, err)
			_ = reportFail(cfg, *job, err.Error())
			continue
		}
		if err := reportComplete(cfg, *job, manifestURL); err != nil {
			log.Printf("complete report error for %s=%s: %v", jobKind(*job), job.ChallengeID, err)
			continue
		}
		log.Printf("completed %s=%s manifest=%s", jobKind(*job), job.ChallengeID, manifestURL)
	}
}

// ─── Config + API DTOs ───────────────────────────────────────────────

type workerConfig struct {
	BackendURL   string
	WorkerToken  string
	R2AccountID  string
	R2Bucket     string
	R2AccessKey  string
	R2SecretKey  string
	R2PublicBase string // e.g. https://media.devf.com  (no trailing slash)
}

func loadConfig() (*workerConfig, error) {
	c := &workerConfig{
		BackendURL:   strings.TrimRight(strings.TrimSpace(os.Getenv("BACKEND_URL")), "/"),
		WorkerToken:  strings.TrimSpace(os.Getenv("HLS_WORKER_TOKEN")),
		R2AccountID:  strings.TrimSpace(os.Getenv("R2_ACCOUNT_ID")),
		R2Bucket:     strings.TrimSpace(os.Getenv("R2_BUCKET")),
		R2AccessKey:  strings.TrimSpace(os.Getenv("R2_ACCESS_KEY_ID")),
		R2SecretKey:  strings.TrimSpace(os.Getenv("R2_SECRET_ACCESS_KEY")),
		R2PublicBase: strings.TrimRight(strings.TrimSpace(os.Getenv("R2_PUBLIC_BASE_URL")), "/"),
	}
	missing := []string{}
	for _, p := range []struct {
		k string
		v string
	}{
		{"BACKEND_URL", c.BackendURL},
		{"HLS_WORKER_TOKEN", c.WorkerToken},
		{"R2_ACCOUNT_ID", c.R2AccountID},
		{"R2_BUCKET", c.R2Bucket},
		{"R2_ACCESS_KEY_ID", c.R2AccessKey},
		{"R2_SECRET_ACCESS_KEY", c.R2SecretKey},
	} {
		if p.v == "" {
			missing = append(missing, p.k)
		}
	}
	if len(missing) > 0 {
		return nil, fmt.Errorf("missing env: %s", strings.Join(missing, ","))
	}
	if c.R2PublicBase == "" {
		c.R2PublicBase = fmt.Sprintf("https://pub-%s.r2.dev/%s", c.R2AccountID, c.R2Bucket)
	}
	return c, nil
}

type pendingJob struct {
	ChallengeID string `json:"challengeId"` // row id in the kind's table
	SourceURL   string `json:"sourceUrl"`
	// Kind selects which table the job came from: "challenge" (default,
	// also what pre-kind backends send as "") or "response" for the
	// challenge_responses leg of a battle. Echoed back verbatim on
	// complete/fail so the backend updates the right row.
	Kind string `json:"kind"`
}

type reportPayload struct {
	ChallengeID string `json:"challengeId"`
	ManifestURL string `json:"manifestUrl"`
	Kind        string `json:"kind"`
}

// ─── HTTP calls to the backend ───────────────────────────────────────

func claimJob(cfg *workerConfig) (*pendingJob, error) {
	req, _ := http.NewRequest("POST", cfg.BackendURL+"/api/v1/internal/hls/next-pending", nil)
	req.Header.Set("X-Worker-Token", cfg.WorkerToken)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode == http.StatusNoContent {
		return nil, nil
	}
	if res.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(res.Body)
		return nil, fmt.Errorf("status %d: %s", res.StatusCode, string(body))
	}
	var j pendingJob
	if err := json.NewDecoder(res.Body).Decode(&j); err != nil {
		return nil, err
	}
	return &j, nil
}

// jobKind normalizes the wire kind — pre-kind backends send "" which
// means the challenges table.
func jobKind(j pendingJob) string {
	if j.Kind == "response" {
		return "response"
	}
	return "challenge"
}

func reportComplete(cfg *workerConfig, job pendingJob, manifestURL string) error {
	body, _ := json.Marshal(reportPayload{ChallengeID: job.ChallengeID, ManifestURL: manifestURL, Kind: jobKind(job)})
	req, _ := http.NewRequest("POST", cfg.BackendURL+"/api/v1/internal/hls/complete", bytes.NewReader(body))
	req.Header.Set("X-Worker-Token", cfg.WorkerToken)
	req.Header.Set("Content-Type", "application/json")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode >= 300 {
		b, _ := io.ReadAll(res.Body)
		return fmt.Errorf("status %d: %s", res.StatusCode, string(b))
	}
	return nil
}

func reportFail(cfg *workerConfig, job pendingJob, reason string) error {
	body, _ := json.Marshal(reportPayload{ChallengeID: job.ChallengeID, ManifestURL: reason, Kind: jobKind(job)})
	req, _ := http.NewRequest("POST", cfg.BackendURL+"/api/v1/internal/hls/fail", bytes.NewReader(body))
	req.Header.Set("X-Worker-Token", cfg.WorkerToken)
	req.Header.Set("Content-Type", "application/json")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	return nil
}

// ─── Core processing pipeline ────────────────────────────────────────

func processJob(cfg *workerConfig, job pendingJob) (string, error) {
	work, err := os.MkdirTemp("", "hls-"+job.ChallengeID+"-")
	if err != nil {
		return "", err
	}
	defer os.RemoveAll(work)

	// 1. Download source.
	srcPath := filepath.Join(work, "source.mp4")
	if err := downloadTo(job.SourceURL, srcPath); err != nil {
		return "", fmt.Errorf("download: %w", err)
	}

	// 2. Run ffmpeg → produces master.m3u8 + per-rendition manifests
	// + .ts segments in `work`.
	outDir := filepath.Join(work, "out")
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return "", err
	}
	if err := transcodeHLS(srcPath, outDir); err != nil {
		return "", fmt.Errorf("transcode: %w", err)
	}

	// 3. Upload everything in outDir to R2 under hls/<id>/ for
	// challenges, hls/resp/<id>/ for battle responses — the two tables
	// have independent id sequences, so without the kind segment a
	// response's output could collide with (and overwrite) an unrelated
	// challenge's ladder.
	prefix := fmt.Sprintf("hls/%s/%s", job.ChallengeID, randHex(8))
	if jobKind(job) == "response" {
		prefix = fmt.Sprintf("hls/resp/%s/%s", job.ChallengeID, randHex(8))
	}
	files, err := os.ReadDir(outDir)
	if err != nil {
		return "", err
	}
	for _, f := range files {
		if f.IsDir() {
			// nested rendition dirs: walk them
			sub, _ := os.ReadDir(filepath.Join(outDir, f.Name()))
			for _, g := range sub {
				if g.IsDir() {
					continue
				}
				local := filepath.Join(outDir, f.Name(), g.Name())
				key := prefix + "/" + f.Name() + "/" + g.Name()
				if err := uploadFile(cfg, local, key); err != nil {
					return "", fmt.Errorf("upload %s: %w", key, err)
				}
			}
			continue
		}
		local := filepath.Join(outDir, f.Name())
		key := prefix + "/" + f.Name()
		if err := uploadFile(cfg, local, key); err != nil {
			return "", fmt.Errorf("upload %s: %w", key, err)
		}
	}

	manifestURL := cfg.R2PublicBase + "/" + prefix + "/master.m3u8"
	return manifestURL, nil
}

func downloadTo(srcURL, dstPath string) error {
	res, err := http.Get(srcURL)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode >= 300 {
		return fmt.Errorf("source GET status %d", res.StatusCode)
	}
	f, err := os.Create(dstPath)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = io.Copy(f, res.Body)
	return err
}

// probeHasAudio uses ffprobe to detect whether `src` contains at least
// one audio stream. We treat probe FAILURE as "audio present" (fail
// open) — silently stripping audio from a video that actually has
// audio would be a much worse bug than declaring a missing audio
// rendition that the player skips.
//
// Why this exists: the symptom we're working around is uploaded reels
// that play back silently with the player's AudioTrack starting,
// flushing after ~60ms, stopping, and looping forever. Root cause: when
// the source MP4 has no audio (e.g. a Realme/Redmi/Oppo phone whose
// MediaCodec dropped audio during on-device compression — a known
// MediaTek SoC bug — see video_processor_service.dart), the worker
// would still declare `a:i` for every rendition in -var_stream_map.
// FFmpeg's `-map 0:a:0?` (optional) silently skips the audio output,
// but the master.m3u8 still REFERENCES audio renditions that don't
// exist. The player then initializes an AudioTrack, fetches a zero-
// byte audio segment, flushes, restarts — exactly the loop the user
// reported.
//
// Fix: probe before transcoding, and only declare audio if it's
// actually present.
func probeHasAudio(src string) bool {
	cmd := exec.Command("ffprobe",
		"-v", "error",
		"-select_streams", "a",
		"-show_entries", "stream=codec_type",
		"-of", "csv=p=0",
		src,
	)
	out, err := cmd.Output()
	if err != nil {
		log.Printf("probeHasAudio: ffprobe failed for %s (%v) — assuming audio present", src, err)
		return true
	}
	hasAudio := strings.Contains(string(out), "audio")
	log.Printf("probeHasAudio: src=%s hasAudio=%v", src, hasAudio)
	return hasAudio
}

// transcodeHLS shells out to FFmpeg to produce a master.m3u8 + per-
// rendition .m3u8 + .ts segments. We use the var_stream_map + hls flags
// to get the standard "master playlist with N variant playlists"
// layout in a single ffmpeg invocation.
//
// The -movflags +faststart equivalent isn't needed for HLS (segments
// have no moov-at-end problem) but we set -profile:v baseline + -level
// 3.1 on the smallest ladder so even ancient phones decode them
// without sweating, while upper ladders use main / high profiles for
// efficiency.
//
// Audio handling: we probe `src` first and only declare audio renditions
// if the source actually has audio. This avoids the silent-playback
// loop bug described on probeHasAudio above.
func transcodeHLS(src, outDir string) error {
	hasAudio := probeHasAudio(src)

	args := []string{
		"-y", // overwrite existing files in outDir
		"-i", src,
	}

	// One -map per rendition. We map the audio stream too, but only
	// when probe confirmed it's there — otherwise FFmpeg would emit
	// a "Stream specifier matches no streams" warning and silently
	// skip the audio output, leaving us with a video-only file but
	// an audio-declaring master playlist (the bug).
	for range ladder {
		args = append(args, "-map", "0:v:0")
		if hasAudio {
			args = append(args, "-map", "0:a:0")
		}
	}

	// Per-rendition encode settings. Three hard-won details from the
	// first production run (portrait WhatsApp-sourced upload):
	//   * force_divisible_by=2 — aspect-preserving scale of a portrait
	//     source produced 853x1080 and libx264 FATALS on odd dims
	//     ("width not divisible by 2", exit 187). Every video failed.
	//   * -filter:v:N (typed per-output-stream form) — the old -vf:N
	//     form made ffmpeg warn "Multiple -filter options ... only the
	//     last will be used" and collapse every rendition onto the last
	//     scale, so the whole ladder would have been 1080p.
	//   * fps=30 — normalizes freak sources (a real upload arrived at
	//     0.33 fps: 5 frames in 15s, which played as "frozen video +
	//     running audio"). Duplicating frames to a constant 30fps makes
	//     any source play smoothly and keeps -g 60 = exact 2s keyframes.
	for i, r := range ladder {
		idx := fmt.Sprintf("%d", i)
		args = append(args,
			"-c:v:"+idx, "libx264",
			"-b:v:"+idx, fmt.Sprintf("%dk", r.videoBps/1000),
			"-maxrate:v:"+idx, fmt.Sprintf("%dk", r.videoBps/1000),
			"-bufsize:v:"+idx, fmt.Sprintf("%dk", (r.videoBps/1000)*2),
			"-filter:v:"+idx, fmt.Sprintf("scale=w=%d:h=%d:force_original_aspect_ratio=decrease:force_divisible_by=2,fps=30", r.width, r.height),
			"-preset", "veryfast",
			"-g", fmt.Sprintf("%d", segmentSeconds*30), // keyframe every segment
			"-keyint_min", fmt.Sprintf("%d", segmentSeconds*30),
			"-sc_threshold", "0",
		)
		if hasAudio {
			args = append(args,
				"-c:a:"+idx, "aac",
				"-b:a:"+idx, fmt.Sprintf("%dk", r.audioBps/1000),
				"-ac:a:"+idx, "2",
				"-ar:a:"+idx, "48000", // normalize to 48kHz — MTK decoders
				//                       handle 48000 more reliably than the
				//                       44100 some Android cameras emit.
			)
		}
	}

	// var_stream_map ties each (v,a) pair to a named variant. When the
	// source has no audio we omit a:i so the master playlist declares
	// video-only variants — the player won't try to fetch a phantom
	// audio rendition.
	varMap := make([]string, 0, len(ladder))
	for i, r := range ladder {
		if hasAudio {
			varMap = append(varMap, fmt.Sprintf("v:%d,a:%d,name:%s", i, i, r.label))
		} else {
			varMap = append(varMap, fmt.Sprintf("v:%d,name:%s", i, r.label))
		}
	}
	args = append(args,
		"-var_stream_map", strings.Join(varMap, " "),
		"-master_pl_name", "master.m3u8",
		"-f", "hls",
		"-hls_time", fmt.Sprintf("%d", segmentSeconds),
		"-hls_playlist_type", "vod",
		"-hls_segment_filename", filepath.Join(outDir, "%v", "seg_%03d.ts"),
		filepath.Join(outDir, "%v", "index.m3u8"),
	)

	cmd := exec.Command("ffmpeg", args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

// ─── R2 upload (minimal SigV4 PUT) ───────────────────────────────────

// We re-implement the minimal PUT here instead of importing the main
// backend's media_storage.go because that lives in `package main` of
// the parent module and Go won't let us import a main package from a
// sibling binary. Future cleanup: extract to internal/r2/. For now,
// keep this in sync with media_storage.go's PresignPutURL.
func uploadFile(cfg *workerConfig, localPath, objectKey string) error {
	host := cfg.R2AccountID + ".r2.cloudflarestorage.com"
	encodedKey := encodeS3Path(objectKey)
	canonicalURI := "/" + cfg.R2Bucket + encodedKey

	now := time.Now().UTC()
	amzDate := now.Format("20060102T150405Z")
	dateStamp := now.Format("20060102")
	credScope := dateStamp + "/auto/s3/aws4_request"

	q := url.Values{}
	q.Set("X-Amz-Algorithm", "AWS4-HMAC-SHA256")
	q.Set("X-Amz-Credential", cfg.R2AccessKey+"/"+credScope)
	q.Set("X-Amz-Date", amzDate)
	q.Set("X-Amz-Expires", "3600")
	q.Set("X-Amz-SignedHeaders", "host")
	canonicalQuery := q.Encode()

	canonicalHeaders := "host:" + host + "\n"
	canonicalRequest := strings.Join([]string{
		"PUT", canonicalURI, canonicalQuery,
		canonicalHeaders, "host", "UNSIGNED-PAYLOAD",
	}, "\n")

	stringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256",
		amzDate,
		credScope,
		sha256Hex([]byte(canonicalRequest)),
	}, "\n")

	kDate := hmacSHA256([]byte("AWS4"+cfg.R2SecretKey), []byte(dateStamp))
	kRegion := hmacSHA256(kDate, []byte("auto"))
	kService := hmacSHA256(kRegion, []byte("s3"))
	kSigning := hmacSHA256(kService, []byte("aws4_request"))
	signature := hex.EncodeToString(hmacSHA256(kSigning, []byte(stringToSign)))
	q.Set("X-Amz-Signature", signature)

	uploadURL := "https://" + host + canonicalURI + "?" + q.Encode()

	f, err := os.Open(localPath)
	if err != nil {
		return err
	}
	defer f.Close()
	stat, err := f.Stat()
	if err != nil {
		return err
	}

	req, err := http.NewRequest("PUT", uploadURL, f)
	if err != nil {
		return err
	}
	req.ContentLength = stat.Size()
	req.Header.Set("Content-Type", contentTypeFor(objectKey))
	// Mirror the cache-control we set on direct-from-client uploads:
	// segments and manifests are content-addressed (path includes a
	// random prefix per challenge) so they're immutable forever.
	req.Header.Set("Cache-Control", "public, max-age=31536000, immutable")

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK && res.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(res.Body)
		return fmt.Errorf("R2 PUT %s status %d: %s", objectKey, res.StatusCode, string(body))
	}
	return nil
}

func contentTypeFor(key string) string {
	switch {
	case strings.HasSuffix(key, ".m3u8"):
		return "application/vnd.apple.mpegurl"
	case strings.HasSuffix(key, ".ts"):
		return "video/mp2t"
	case strings.HasSuffix(key, ".mp4"):
		return "video/mp4"
	default:
		return "application/octet-stream"
	}
}

func encodeS3Path(key string) string {
	parts := strings.Split(key, "/")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		out = append(out, url.PathEscape(p))
	}
	return "/" + strings.Join(out, "/")
}

func sha256Hex(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

func hmacSHA256(key, data []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write(data)
	return h.Sum(nil)
}

func randHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		// Fall back to a timestamp string; collisions don't matter
		// because the challengeID is already in the path.
		return fmt.Sprintf("ts%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
}

// (defensive) keep errors typed even though we don't use this directly
// — Go linters complain about unused imports otherwise. Not strictly
// needed, but kept for symmetry with media_storage.go's idioms.
var _ = errors.New
