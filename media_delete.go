package main

// media_delete.go — actually removing video files from R2 when the thing
// that pointed at them is deleted.
//
// WHY THIS EXISTS
//
// Deleting a challenge removed the database row and nothing else. The video
// stayed in the bucket forever, unreachable from the app but still stored and
// still billed. Same for deleting an account. Both places said so out loud —
// "R2 media objects are not deleted here (same policy as challenge deletion —
// decoupled storage cleanup)" — but the decoupled cleanup was never written,
// so "decoupled" meant "never". Every delete since launch has leaked its
// video.
//
// HOW IT WORKS
//
// Deleting from the bucket is slow, can fail, and must not make the user's
// delete button appear broken. So the request does the fast, important part
// (drop the rows) and writes the storage paths to a queue table. A background
// worker drains that queue.
//
// The queue is a table rather than a goroutine because a goroutine dies with
// the process. Render restarts this service regularly — on deploy, on idle,
// on its own schedule — and a deletion lost to a restart is a file nobody
// will ever look at again. Rows survive restarts; goroutines do not.
//
// WHAT GETS DELETED
//
// Two shapes of path, both derivable from what the row already stored:
//
//   u/<userID>/<uploadID>/   everything uploaded for one video — every
//                            quality variant plus the thumbnail
//   hls/<challengeID>/       the streaming version, if the transcode worker
//                            ever produced one: a manifest plus hundreds of
//                            small segment files
//
// Both are deleted by listing the prefix and removing what is there, so a
// variant this code does not know the name of still goes.

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// How often the worker looks for queued deletions. Storage cleanup is never
// urgent — nobody is waiting on it — and a slow tick keeps this off the
// critical path of a small instance.
const mediaDeleteInterval = 2 * time.Minute

// How many prefixes one tick will work through. Bounded so a large backlog
// (for example the first run after this ships, against every delete that has
// already happened) is spread over many ticks instead of pinning the process.
const mediaDeleteBatch = 20

// A prefix that keeps failing is eventually abandoned, so one broken entry
// cannot block the queue forever. It stays in the table with its error for
// anyone investigating.
const mediaDeleteMaxAttempts = 8

// ---------- queue ----------
//
// The pending_media_deletions table is created with the rest of the schema in
// runMigrations.

// enqueueMediaDeletions records storage paths to be cleared later.
//
// Deliberately never returns an error. It is called just after rows have been
// deleted, and by then the user's request has succeeded; failing to write the
// queue row is a cleanup problem to log, not a reason to tell someone their
// delete did not work.
func enqueueMediaDeletions(prefixes []string) {
	if db == nil || len(prefixes) == 0 {
		return
	}
	for _, p := range prefixes {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		if !strings.HasSuffix(p, "/") {
			p += "/"
		}
		_, err := db.Exec(
			`INSERT INTO pending_media_deletions (object_prefix) VALUES ($1)
			 ON CONFLICT (object_prefix) DO NOTHING`, p)
		if err != nil {
			log.Printf("media cleanup: could not queue %q: %v", p, err)
		}
	}
}

// startMediaDeleter runs the queue drain forever. Safe to run on several
// instances at once: each row is claimed with a locked delete so two workers
// cannot pick up the same prefix.
func startMediaDeleter() {
	go func() {
		t := time.NewTicker(mediaDeleteInterval)
		defer t.Stop()
		for range t.C {
			if err := drainMediaDeletions(context.Background()); err != nil {
				log.Printf("media cleanup: %v", err)
			}
		}
	}()
}

func drainMediaDeletions(ctx context.Context) error {
	if db == nil {
		return nil
	}
	cfg, err := loadR2Config()
	if err != nil {
		// No storage credentials configured. Leave the queue alone — the
		// rows are still valid, and shouting every two minutes helps nobody.
		return nil
	}

	rows, err := db.Query(
		`SELECT id, object_prefix, attempts FROM pending_media_deletions
		 WHERE attempts < $1 ORDER BY id LIMIT $2`,
		mediaDeleteMaxAttempts, mediaDeleteBatch)
	if err != nil {
		return fmt.Errorf("reading the deletion queue: %w", err)
	}
	type job struct {
		id       int
		prefix   string
		attempts int
	}
	var jobs []job
	for rows.Next() {
		var j job
		if err := rows.Scan(&j.id, &j.prefix, &j.attempts); err == nil {
			jobs = append(jobs, j)
		}
	}
	rows.Close()

	for _, j := range jobs {
		// Claim it. If another instance got there first this deletes nothing
		// and we skip, which is how two workers stay out of each other's way.
		res, err := db.Exec(
			`DELETE FROM pending_media_deletions WHERE id = $1 AND attempts = $2`,
			j.id, j.attempts)
		if err != nil {
			continue
		}
		if n, _ := res.RowsAffected(); n == 0 {
			continue
		}

		n, err := cfg.DeletePrefix(ctx, j.prefix)
		if err == nil {
			if n > 0 {
				log.Printf("media cleanup: removed %d object(s) under %s", n, j.prefix)
			}
			continue
		}

		// Put it back with the failure recorded, so the next tick retries and
		// a permanently broken entry eventually stops being retried.
		log.Printf("media cleanup: %s failed (attempt %d): %v",
			j.prefix, j.attempts+1, err)
		_, putBack := db.Exec(
			`INSERT INTO pending_media_deletions (object_prefix, attempts, last_error)
			 VALUES ($1, $2, $3)
			 ON CONFLICT (object_prefix) DO UPDATE
			   SET attempts = EXCLUDED.attempts, last_error = EXCLUDED.last_error`,
			j.prefix, j.attempts+1, err.Error())
		if putBack != nil {
			log.Printf("media cleanup: lost %q while re-queueing: %v", j.prefix, putBack)
		}
	}
	return nil
}

// ---------- talking to R2 ----------

// mediaHTTPClient is used for the backend's own calls to R2. Separate from
// anything serving users, with a timeout long enough for a listing but short
// enough that a hung bucket cannot wedge the worker.
var mediaHTTPClient = &http.Client{Timeout: 30 * time.Second}

// DeletePrefix removes every object whose key starts with prefix, and returns
// how many were removed.
func (c *R2Config) DeletePrefix(ctx context.Context, prefix string) (int, error) {
	if c == nil {
		return 0, errors.New("R2Config is nil")
	}
	if prefix == "" || prefix == "/" {
		// Refusing this is the whole safety story: an empty prefix matches
		// every object in the bucket.
		return 0, errors.New("refusing to delete an empty prefix")
	}
	keys, err := c.ListKeys(ctx, prefix)
	if err != nil {
		return 0, err
	}
	deleted := 0
	for _, k := range keys {
		if err := c.DeleteObject(ctx, k); err != nil {
			return deleted, fmt.Errorf("deleting %s: %w", k, err)
		}
		deleted++
	}
	return deleted, nil
}

// listBucketResult is the slice of S3's ListObjectsV2 XML we care about.
type listBucketResult struct {
	XMLName               xml.Name `xml:"ListBucketResult"`
	IsTruncated           bool     `xml:"IsTruncated"`
	NextContinuationToken string   `xml:"NextContinuationToken"`
	Contents              []struct {
		Key string `xml:"Key"`
	} `xml:"Contents"`
}

// ListKeys returns every object key under prefix, following pagination.
//
// An HLS transcode produces hundreds of segment files, so the paging is not
// theoretical.
func (c *R2Config) ListKeys(ctx context.Context, prefix string) ([]string, error) {
	var out []string
	token := ""
	for page := 0; page < 100; page++ { // hard stop; 100k keys is not a real case
		q := url.Values{}
		q.Set("list-type", "2")
		q.Set("prefix", prefix)
		q.Set("max-keys", "1000")
		if token != "" {
			q.Set("continuation-token", token)
		}
		signed, err := c.presignBucketURL("GET", q, 10*time.Minute)
		if err != nil {
			return nil, err
		}
		body, err := c.doSigned(ctx, "GET", signed)
		if err != nil {
			return nil, err
		}
		var parsed listBucketResult
		if err := xml.Unmarshal(body, &parsed); err != nil {
			return nil, fmt.Errorf("could not read the bucket listing: %w", err)
		}
		for _, item := range parsed.Contents {
			out = append(out, item.Key)
		}
		if !parsed.IsTruncated || parsed.NextContinuationToken == "" {
			return out, nil
		}
		token = parsed.NextContinuationToken
	}
	return out, nil
}

// DeleteObject removes exactly one object.
func (c *R2Config) DeleteObject(ctx context.Context, objectKey string) error {
	signed, err := c.presignURL("DELETE", objectKey, nil, 10*time.Minute)
	if err != nil {
		return err
	}
	_, err = c.doSigned(ctx, "DELETE", signed)
	return err
}

// doSigned performs a request against a URL that presignURL already signed.
func (c *R2Config) doSigned(ctx context.Context, method, signedURL string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, method, signedURL, nil)
	if err != nil {
		return nil, err
	}
	res, err := mediaHTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(res.Body, 4<<20))
	// S3 answers 204 to a delete, including for a key that was already gone,
	// which is exactly the behaviour a retry wants.
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return nil, fmt.Errorf("storage answered %d: %s",
			res.StatusCode, strings.TrimSpace(string(body)))
	}
	return body, nil
}

// ---------- working out what to delete ----------

// mediaPrefixFromPublicURL turns a stored video address back into the folder
// it lives in.
//
// The app saves the public address of a video, like
//
//	https://pub-abc123.r2.dev/u/29/4ab2fd.../720p.mp4
//
// and everything uploaded alongside it — the other quality variants, the
// thumbnail — sits in the same folder. So we strip the public prefix off the
// front and the filename off the end, leaving "u/29/4ab2fd.../".
//
// Returns "" for anything that is not one of our own uploads, which is the
// safe answer: a link to somewhere else must never make us delete anything.
func mediaPrefixFromPublicURL(c *R2Config, publicURL string) string {
	if c == nil || publicURL == "" {
		return ""
	}
	base := strings.TrimRight(c.PublicBaseURL, "/")
	if base == "" || !strings.HasPrefix(publicURL, base+"/") {
		return ""
	}
	key := strings.TrimPrefix(publicURL, base+"/")
	if i := strings.IndexAny(key, "?#"); i >= 0 {
		key = key[:i]
	}
	// Only ever our upload layout: u/<user>/<upload>/<file>. Anything with a
	// different shape is left alone rather than guessed at.
	parts := strings.Split(key, "/")
	if len(parts) < 4 || parts[0] != "u" || parts[1] == "" || parts[2] == "" {
		return ""
	}
	return fmt.Sprintf("u/%s/%s/", parts[1], parts[2])
}

// mediaPrefixesForChallenge collects every storage folder belonging to one
// challenge: its own upload, the uploads of every answer to it, and the
// streaming versions of both.
//
// Must be called BEFORE the rows are deleted — afterwards there is nothing
// left to read the addresses from.
func mediaPrefixesForChallenge(challengeID int) []string {
	if db == nil {
		return nil
	}
	cfg, err := loadR2Config()
	if err != nil {
		return nil
	}

	seen := map[string]bool{}
	add := func(rawURL string) {
		if p := mediaPrefixFromPublicURL(cfg, rawURL); p != "" {
			seen[p] = true
		}
	}

	// The challenge's own media, and every answer's.
	for _, q := range []string{
		`SELECT COALESCE(video_url,''), COALESCE(thumbnail_url,''),
		        COALESCE(video_variants,'{}'::jsonb)::text
		   FROM challenges WHERE id = $1`,
		`SELECT COALESCE(video_url,''), COALESCE(thumbnail_url,''),
		        COALESCE(video_variants,'{}'::jsonb)::text
		   FROM challenge_responses WHERE challenge_id = $1`,
	} {
		rows, err := db.Query(q, challengeID)
		if err != nil {
			continue
		}
		for rows.Next() {
			var video, thumb, variants string
			if err := rows.Scan(&video, &thumb, &variants); err != nil {
				continue
			}
			add(video)
			add(thumb)
			// The variants column is a small {quality: url} object. Pulling
			// the URLs out by scanning for them avoids depending on its exact
			// shape, which has changed once already.
			for _, u := range urlsInJSON(variants) {
				add(u)
			}
		}
		rows.Close()
	}

	out := make([]string, 0, len(seen)+2)
	for p := range seen {
		out = append(out, p)
	}
	// The transcode worker writes the streaming version under its own paths,
	// keyed by challenge rather than by upload, so they are added directly.
	out = append(out,
		fmt.Sprintf("hls/%d/", challengeID),
		fmt.Sprintf("hls/resp/%d/", challengeID),
	)
	return out
}

// urlsInJSON pulls every http(s) address out of a small JSON blob without
// caring about its structure.
func urlsInJSON(blob string) []string {
	var out []string
	for _, chunk := range strings.Split(blob, `"`) {
		if strings.HasPrefix(chunk, "http://") || strings.HasPrefix(chunk, "https://") {
			out = append(out, chunk)
		}
	}
	return out
}
