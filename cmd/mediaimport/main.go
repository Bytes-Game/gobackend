// mediaimport copies the sample reel clips into our own R2 bucket and
// generates a real poster frame for each one.
//
// # WHY THIS EXISTS
//
// The seeded feed hotlinks its videos from third-party test-file hosts.
// Nine of the fourteen come from test-videos.co.uk, measured at roughly
// one full second to first byte. A reels player issues many range
// requests per video, so that second is paid over and over: the decoder
// receives a burst of data, plays it, and starves. On a device this reads
// as "the video freezes and I have to pause and play to unstick it", and
// it is visible in a profile log as a long run of
//
//	queueInputBuffer: Input time interval reaches 98Xms
//
// clustered tightly around that host's time to first byte. It also
// wrecks the prefix-warming measurement: a warm that cannot even start
// for a second loses the race against the user's thumb, so the cache hit
// rate we read off the app is really a measurement of somebody else's
// web server.
//
// The posters have the same shape of problem for a different reason: the
// seeder points thumbnail_url at picsum.photos, which returns an
// unrelated stock photo per clip. A picture of stars sitting on top of a
// jellyfish video does not read as a placeholder, it reads as the wrong
// video.
//
// This command fixes both at the source. It downloads each clip once,
// extracts a frame from the video itself, uploads both to R2, and prints
// the resulting public URLs so they can be pasted into the seeder.
//
// # USAGE
//
// Normally run from the media-import workflow, which already holds the
// R2 secrets and ships ffmpeg. Locally:
//
//	go run ./cmd/mediaimport            # download, transcode, upload
//	go run ./cmd/mediaimport -dry-run   # print the plan, touch nothing
//
// Object keys are derived from a hash of the source URL, so the command
// is idempotent: running it twice overwrites the same objects rather
// than accumulating copies, and the printed URLs do not change.
package main

import (
	"context"
	"crypto/hmac"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"mymodule/internal/mp4layout"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// sources are the clips to import. Keep in step with the clips table in
// cmd/seed/main.go: this list is the origin side, that one is the
// serving side, and after a successful run that one should point at the
// URLs this command prints.
var sources = []string{
	"https://test-videos.co.uk/vids/bigbuckbunny/mp4/h264/720/Big_Buck_Bunny_720_10s_1MB.mp4",
	"https://test-videos.co.uk/vids/jellyfish/mp4/h264/720/Jellyfish_720_10s_1MB.mp4",
	"https://test-videos.co.uk/vids/sintel/mp4/h264/720/Sintel_720_10s_1MB.mp4",
	"https://flutter.github.io/assets-for-api-docs/assets/videos/bee.mp4",
	"https://mdn.github.io/shared-assets/videos/flower.mp4",
	"https://test-videos.co.uk/vids/bigbuckbunny/mp4/h264/720/Big_Buck_Bunny_720_10s_2MB.mp4",
	"https://test-videos.co.uk/vids/jellyfish/mp4/h264/720/Jellyfish_720_10s_2MB.mp4",
	"https://test-videos.co.uk/vids/sintel/mp4/h264/720/Sintel_720_10s_2MB.mp4",
	"https://flutter.github.io/assets-for-api-docs/assets/videos/butterfly.mp4",
	"https://media.w3.org/2010/05/video/movie_300.mp4",
	"https://test-videos.co.uk/vids/bigbuckbunny/mp4/h264/720/Big_Buck_Bunny_720_10s_5MB.mp4",
	"https://test-videos.co.uk/vids/jellyfish/mp4/h264/720/Jellyfish_720_10s_5MB.mp4",
	"https://test-videos.co.uk/vids/sintel/mp4/h264/720/Sintel_720_10s_5MB.mp4",
	"https://media.w3.org/2010/05/sintel/trailer.mp4",
}

// maxReelBytes mirrors the seeder's guard. A reels feed has no business
// serving anything larger, and importing one would quietly undo the size
// cleanup that shrank the largest asset from 249 MB to 5 MB.
const maxReelBytes = 6 * 1024 * 1024

// keyPrefix namespaces these objects so they are obviously disposable
// sample data and can be deleted wholesale without touching real uploads.
const keyPrefix = "seed/"

type result struct {
	source    string
	videoURL  string
	posterURL string
	bytes     int64
}

func main() {
	dryRun := flag.Bool("dry-run", false,
		"print what would be uploaded without contacting R2")
	flag.Parse()

	cfg, err := loadConfig(*dryRun)
	if err != nil {
		fmt.Fprintf(os.Stderr, "config: %v\n", err)
		os.Exit(1)
	}

	if !*dryRun {
		if _, err := exec.LookPath("ffmpeg"); err != nil {
			fmt.Fprintln(os.Stderr,
				"ffmpeg not found on PATH; it is required to extract poster frames")
			os.Exit(1)
		}
	}

	workDir, err := os.MkdirTemp("", "mediaimport")
	if err != nil {
		fmt.Fprintf(os.Stderr, "temp dir: %v\n", err)
		os.Exit(1)
	}
	defer os.RemoveAll(workDir)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()

	results := make([]result, 0, len(sources))
	failures := 0
	for i, src := range sources {
		fmt.Printf("[%2d/%d] %s\n", i+1, len(sources), src)
		res, err := importOne(ctx, cfg, workDir, src, *dryRun)
		if err != nil {
			// Keep going: one dead source should not cost us the other
			// thirteen uploads, and the summary below reports the gap.
			fmt.Fprintf(os.Stderr, "        FAILED: %v\n", err)
			failures++
			continue
		}
		results = append(results, res)
		fmt.Printf("        video  %s\n", res.videoURL)
		fmt.Printf("        poster %s\n", res.posterURL)
	}

	fmt.Printf("\n%d imported, %d failed\n", len(results), failures)

	if !*dryRun && len(results) > 0 {
		verifyPublic(ctx, results)
	}
	printManifest(results)

	if failures > 0 {
		os.Exit(1)
	}
}

// importOne downloads a single clip, renders its poster and uploads both.
func importOne(ctx context.Context, cfg *config, workDir, src string, dryRun bool) (result, error) {
	key := objectKey(src)
	res := result{
		source:    src,
		videoURL:  cfg.publicURL(key + ".mp4"),
		posterURL: cfg.publicURL(key + ".jpg"),
	}
	if dryRun {
		return res, nil
	}

	videoPath := filepath.Join(workDir, filepath.Base(key)+".mp4")
	n, err := download(ctx, src, videoPath)
	if err != nil {
		return res, fmt.Errorf("download: %w", err)
	}
	if n > maxReelBytes {
		return res, fmt.Errorf("source is %d bytes, over the %d byte reel limit",
			n, maxReelBytes)
	}
	res.bytes = n

	// Normalise the box order before anything else touches the file. A
	// source with its index at the end is uploaded to our bucket exactly
	// as it arrived unless it is fixed here, and after that every client
	// pays for it forever — see mp4_layout.go for what it costs.
	fixed, err := ensureFastStart(ctx, videoPath)
	if err != nil {
		return res, fmt.Errorf("faststart: %w", err)
	}
	if fixed {
		fmt.Println("        remuxed to faststart (index was at the end)")
		// The remux rewrites the container, so the size reported in the
		// manifest has to be the one actually uploaded.
		if st, statErr := os.Stat(videoPath); statErr == nil {
			res.bytes = st.Size()
		}
	}

	posterPath := filepath.Join(workDir, filepath.Base(key)+".jpg")
	if err := extractPoster(ctx, videoPath, posterPath); err != nil {
		return res, fmt.Errorf("poster: %w", err)
	}

	if err := cfg.put(ctx, videoPath, key+".mp4", "video/mp4"); err != nil {
		return res, fmt.Errorf("upload video: %w", err)
	}
	if err := cfg.put(ctx, posterPath, key+".jpg", "image/jpeg"); err != nil {
		return res, fmt.Errorf("upload poster: %w", err)
	}
	return res, nil
}

// objectKey derives a stable, collision-resistant key from the source
// URL. Stable so re-running overwrites rather than duplicates; derived
// from the URL rather than the filename because several sources share a
// basename (three different "trailer.mp4"-shaped names would collide).
func objectKey(src string) string {
	sum := sha1.Sum([]byte(src))
	return keyPrefix + hex.EncodeToString(sum[:8])
}

func download(ctx context.Context, src, dst string) (int64, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", src, nil)
	if err != nil {
		return 0, err
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("status %d", res.StatusCode)
	}
	f, err := os.Create(dst)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	// Read one byte past the limit so an oversized source is detected
	// here rather than after writing hundreds of megabytes to disk.
	n, err := io.Copy(f, io.LimitReader(res.Body, maxReelBytes+1))
	if err != nil {
		return n, err
	}
	return n, nil
}

// extractPoster renders a single frame as the video's poster.
//
// Seeking to one second rather than zero avoids the fade-in-from-black
// that several of these clips open on, which would give us a poster that
// is literally a black rectangle. Clips shorter than a second fall back
// to the first frame.
func extractPoster(ctx context.Context, videoPath, posterPath string) error {
	if err := runFFmpeg(ctx, videoPath, posterPath, "1"); err == nil {
		if st, statErr := os.Stat(posterPath); statErr == nil && st.Size() > 0 {
			return nil
		}
	}
	if err := runFFmpeg(ctx, videoPath, posterPath, "0"); err != nil {
		return err
	}
	st, err := os.Stat(posterPath)
	if err != nil {
		return err
	}
	if st.Size() == 0 {
		return fmt.Errorf("ffmpeg produced an empty poster")
	}
	return nil
}

// ensureFastStart moves an MP4's index to the front if it is at the back,
// rewriting videoPath in place. Reports whether it did anything.
//
// The remux is `-c copy`: no decoding, no re-encoding, no quality change —
// ffmpeg reads the container and writes the same streams back with the
// boxes in the other order. On a reel it takes about a second.
//
// A file that is already faststart is left completely alone. That matters
// for a tool that is re-run: rewriting healthy files on every import would
// churn the bucket, change every object's bytes, and make the "remuxed"
// line above meaningless as a report of which sources were bad.
func ensureFastStart(ctx context.Context, videoPath string) (bool, error) {
	layout, err := mp4layout.OfFile(videoPath)
	if err != nil {
		return false, err
	}
	// unknown is deliberately not remuxed — see mp4_layout.go. Rewriting on
	// the strength of a read we could not interpret would touch files that
	// are very likely fine.
	if layout != mp4layout.MoovAtEnd {
		return false, nil
	}

	remuxed := videoPath + ".faststart.mp4"
	defer os.Remove(remuxed)
	cmd := exec.CommandContext(ctx, "ffmpeg",
		"-y",
		"-i", videoPath,
		// Copy both streams through untouched. Anything else here would
		// silently re-encode and cost quality for a container-level fix.
		"-c", "copy",
		"-movflags", "+faststart",
		remuxed,
	)
	if out, err := cmd.CombinedOutput(); err != nil {
		return false, fmt.Errorf("ffmpeg -movflags +faststart: %w: %s",
			err, lastLines(string(out), 3))
	}

	// Verify rather than assume. ffmpeg exits 0 on plenty of outputs that
	// are not what was asked for, and an unverified remux would upload a
	// still-broken file while printing that it had fixed it — the same
	// class of silent wrongness this whole change is about.
	switch after, err := mp4layout.OfFile(remuxed); {
	case err != nil:
		return false, err
	case after != mp4layout.FastStart:
		return false, fmt.Errorf(
			"remux produced a %s file; refusing to upload it as fixed", after)
	}

	if err := os.Rename(remuxed, videoPath); err != nil {
		return false, fmt.Errorf("replace with remuxed file: %w", err)
	}
	return true, nil
}

// mp4layout.OfFile reads just enough of a file's opening to classify it.
func runFFmpeg(ctx context.Context, videoPath, posterPath, seekSeconds string) error {
	// -ss before -i is the fast (keyframe) seek, which is what we want:
	// exact frame accuracy is irrelevant for a poster and the accurate
	// form decodes everything up to the seek point.
	cmd := exec.CommandContext(ctx, "ffmpeg",
		"-y",
		"-ss", seekSeconds,
		"-i", videoPath,
		"-frames:v", "1",
		// Width 720 keeps it sharp on a phone without making the poster
		// heavier than the first seconds of the video it stands in for.
		// -2 keeps the source aspect ratio and an even height, which the
		// JPEG encoder requires.
		"-vf", "scale=720:-2",
		"-q:v", "4",
		posterPath,
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("ffmpeg -ss %s: %w: %s", seekSeconds, err, lastLines(string(out), 3))
	}
	return nil
}

func lastLines(s string, n int) string {
	lines := strings.Split(strings.TrimSpace(s), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, " | ")
}

// verifyPublic checks that what we uploaded is actually reachable the
// way the app will reach it. Uploading to a private bucket succeeds
// quietly, and the failure would otherwise surface much later as an
// unplayable feed, so ask for the first bytes exactly as the player does
// and report what comes back.
func verifyPublic(ctx context.Context, results []result) {
	fmt.Println("\nverifying public reachability (range request, as the player issues):")
	bad := 0
	for _, r := range results {
		req, err := http.NewRequestWithContext(ctx, "GET", r.videoURL, nil)
		if err != nil {
			continue
		}
		req.Header.Set("Range", "bytes=0-1023")
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			fmt.Printf("  ERROR %v  %s\n", err, r.videoURL)
			bad++
			continue
		}
		io.Copy(io.Discard, io.LimitReader(res.Body, 4096))
		res.Body.Close()
		if res.StatusCode != http.StatusPartialContent {
			fmt.Printf("  status %d (expected 206)  %s\n", res.StatusCode, r.videoURL)
			bad++
			continue
		}
		fmt.Printf("  206 OK  %s\n", r.videoURL)
	}
	if bad > 0 {
		fmt.Printf("\n%d object(s) are not publicly readable. The upload itself\n", bad)
		fmt.Println("succeeded, so this is a bucket setting: enable public access")
		fmt.Println("on the R2 bucket (or bind the custom domain) and re-check.")
		fmt.Println("Do NOT paste these URLs into the seeder until they return 206.")
	}
}

// printManifest emits the block to paste back into the seeder. Kept as
// plain aligned text rather than JSON because a human transcribes it.
func printManifest(results []result) {
	if len(results) == 0 {
		return
	}
	fmt.Println("\n--- manifest: source, video, poster ---")
	for _, r := range results {
		fmt.Printf("%s\n\t%s\n\t%s\n", r.source, r.videoURL, r.posterURL)
	}
	fmt.Println("--- end manifest ---")
}

// ─── R2 config and upload ────────────────────────────────────────────
//
// The minimal SigV4 PUT is re-implemented here for the same reason
// cmd/hls-worker does it: media_storage.go lives in `package main` of
// the parent module, and Go will not let one main package import
// another. Keep in step with media_storage.go's PresignPutURL.

type config struct {
	accountID     string
	bucket        string
	accessKey     string
	secretKey     string
	publicBaseURL string
}

func loadConfig(dryRun bool) (*config, error) {
	c := &config{
		accountID:     strings.TrimSpace(os.Getenv("R2_ACCOUNT_ID")),
		bucket:        strings.TrimSpace(os.Getenv("R2_BUCKET")),
		accessKey:     strings.TrimSpace(os.Getenv("R2_ACCESS_KEY_ID")),
		secretKey:     strings.TrimSpace(os.Getenv("R2_SECRET_ACCESS_KEY")),
		publicBaseURL: strings.TrimRight(strings.TrimSpace(os.Getenv("R2_PUBLIC_BASE_URL")), "/"),
	}
	var missing []string
	for name, v := range map[string]string{
		"R2_ACCOUNT_ID":        c.accountID,
		"R2_BUCKET":            c.bucket,
		"R2_ACCESS_KEY_ID":     c.accessKey,
		"R2_SECRET_ACCESS_KEY": c.secretKey,
	} {
		if v == "" {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 && !dryRun {
		return nil, fmt.Errorf("missing env: %s", strings.Join(missing, ", "))
	}
	if c.publicBaseURL == "" {
		// Same fallback the backend uses, so a setup without a custom
		// domain still produces the URLs the API would have produced.
		c.publicBaseURL = fmt.Sprintf("https://pub-%s.r2.dev/%s", c.accountID, c.bucket)
		fmt.Fprintf(os.Stderr,
			"note: R2_PUBLIC_BASE_URL is unset; assuming %s\n", c.publicBaseURL)
	}
	return c, nil
}

func (c *config) publicURL(objectKey string) string {
	return c.publicBaseURL + "/" + strings.TrimLeft(objectKey, "/")
}

func (c *config) put(ctx context.Context, localPath, objectKey, contentType string) error {
	host := c.accountID + ".r2.cloudflarestorage.com"
	canonicalURI := "/" + c.bucket + "/" + strings.TrimLeft(objectKey, "/")

	now := time.Now().UTC()
	amzDate := now.Format("20060102T150405Z")
	dateStamp := now.Format("20060102")
	credScope := dateStamp + "/auto/s3/aws4_request"

	q := url.Values{}
	q.Set("X-Amz-Algorithm", "AWS4-HMAC-SHA256")
	q.Set("X-Amz-Credential", c.accessKey+"/"+credScope)
	q.Set("X-Amz-Date", amzDate)
	q.Set("X-Amz-Expires", "3600")
	q.Set("X-Amz-SignedHeaders", "host")
	canonicalQuery := q.Encode()

	canonicalRequest := strings.Join([]string{
		"PUT", canonicalURI, canonicalQuery,
		"host:" + host + "\n", "host", "UNSIGNED-PAYLOAD",
	}, "\n")
	stringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256", amzDate, credScope,
		sha256Hex([]byte(canonicalRequest)),
	}, "\n")

	kDate := hmacSHA256([]byte("AWS4"+c.secretKey), []byte(dateStamp))
	kRegion := hmacSHA256(kDate, []byte("auto"))
	kService := hmacSHA256(kRegion, []byte("s3"))
	kSigning := hmacSHA256(kService, []byte("aws4_request"))
	q.Set("X-Amz-Signature", hex.EncodeToString(hmacSHA256(kSigning, []byte(stringToSign))))

	f, err := os.Open(localPath)
	if err != nil {
		return err
	}
	defer f.Close()
	stat, err := f.Stat()
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, "PUT",
		"https://"+host+canonicalURI+"?"+q.Encode(), f)
	if err != nil {
		return err
	}
	req.ContentLength = stat.Size()
	req.Header.Set("Content-Type", contentType)
	// Keys are content-addressed by source URL, so a given key always
	// holds the same bytes and can be cached indefinitely.
	req.Header.Set("Cache-Control", "public, max-age=31536000, immutable")

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK && res.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(io.LimitReader(res.Body, 2048))
		return fmt.Errorf("R2 PUT status %d: %s", res.StatusCode, string(body))
	}
	return nil
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
