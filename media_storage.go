package main

// Cloudflare R2 storage helper. R2 speaks the S3 API but with two
// quirks we care about here:
//
//   1. The region in the SigV4 credential scope is the literal string "auto"
//      (not "us-east-1" or anything geographic).
//   2. R2 charges $0/GB egress, which is why the entire video pipeline uses
//      it instead of S3/Backblaze. Keep that decision in mind before swapping
//      providers — egress is by far the biggest cost in a video app.
//
// We hand-roll AWS Signature V4 query signing here because we only ever
// perform one operation (presigned PUT URLs for client uploads). Pulling
// in aws-sdk-go-v2 would add ~30 transitive packages and ~10 MB to the
// binary for a few hundred lines of crypto we can write directly.
//
// All public functions here are pure given inputs (no goroutines, no I/O
// other than reading env once at boot) so they're trivially testable.

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"
)

// R2Config holds everything the signer needs at runtime. Loaded once from
// env via loadR2Config(); after that it's read-only and shared.
type R2Config struct {
	// AccountID is the Cloudflare R2 account id; combined with bucket it
	// forms the S3 endpoint host: <account>.r2.cloudflarestorage.com.
	AccountID string
	// Bucket name. Must already exist (we don't create-on-demand).
	Bucket string
	// AccessKeyID + SecretAccessKey are the R2 API token credentials.
	AccessKeyID     string
	SecretAccessKey string
	// PublicBaseURL is the CDN/host clients fetch from (typically the R2
	// public-bucket URL or a custom domain bound to the bucket). Trailing
	// slash optional — we trim it.
	PublicBaseURL string
}

var (
	r2Once   sync.Once
	r2Cached *R2Config
	r2Err    error
)

// loadR2Config reads R2 credentials from env on first call and caches the
// result. Returns an error if any required key is missing — callers that
// need to fail fast (handler boot) can surface that directly.
func loadR2Config() (*R2Config, error) {
	r2Once.Do(func() {
		cfg := &R2Config{
			AccountID:       strings.TrimSpace(getEnvFromOS("R2_ACCOUNT_ID")),
			Bucket:          strings.TrimSpace(getEnvFromOS("R2_BUCKET")),
			AccessKeyID:     strings.TrimSpace(getEnvFromOS("R2_ACCESS_KEY_ID")),
			SecretAccessKey: strings.TrimSpace(getEnvFromOS("R2_SECRET_ACCESS_KEY")),
			PublicBaseURL:   strings.TrimRight(strings.TrimSpace(getEnvFromOS("R2_PUBLIC_BASE_URL")), "/"),
		}
		missing := []string{}
		if cfg.AccountID == "" {
			missing = append(missing, "R2_ACCOUNT_ID")
		}
		if cfg.Bucket == "" {
			missing = append(missing, "R2_BUCKET")
		}
		if cfg.AccessKeyID == "" {
			missing = append(missing, "R2_ACCESS_KEY_ID")
		}
		if cfg.SecretAccessKey == "" {
			missing = append(missing, "R2_SECRET_ACCESS_KEY")
		}
		if cfg.PublicBaseURL == "" {
			// Fall back to the R2 default public hostname format so dev
			// setups without a custom domain still work. This won't serve
			// real traffic until the bucket is actually marked public, but
			// it lets the API respond consistently.
			cfg.PublicBaseURL = fmt.Sprintf("https://pub-%s.r2.dev/%s", cfg.AccountID, cfg.Bucket)
		}
		if len(missing) > 0 {
			r2Err = fmt.Errorf("R2 not configured: missing env %s", strings.Join(missing, ","))
			return
		}
		r2Cached = cfg
	})
	return r2Cached, r2Err
}

// r2Endpoint returns the S3-compatible https origin (no path) for this
// R2 account. The bucket is encoded in the request path, S3-style.
func (c *R2Config) endpointHost() string {
	return c.AccountID + ".r2.cloudflarestorage.com"
}

// PresignPutURL builds an AWS SigV4 query-signed URL that grants the
// holder permission to PUT exactly one object at `objectKey` for at most
// `expiry` seconds. We deliberately use UNSIGNED-PAYLOAD so the client
// can stream the body without precomputing a SHA-256 of (potentially
// hundreds of MB of) video.
//
// The returned URL is opaque to the caller — they pass it straight to
// http.PUT with the file body.
func (c *R2Config) PresignPutURL(objectKey string, expiry time.Duration) (string, error) {
	return c.presignURL("PUT", objectKey, nil, expiry)
}

// Multipart presigns — same signer, different (method, query) pairs.
// The client executes the actual S3 calls; the backend never touches
// bytes, exactly like the single-PUT path. Part size/count policy lives
// client-side; server-side authorization is the key-prefix ownership
// check in the handler.

// PresignMultipartInitURL → POST {key}?uploads (response XML carries UploadId).
func (c *R2Config) PresignMultipartInitURL(objectKey string, expiry time.Duration) (string, error) {
	q := url.Values{}
	q.Set("uploads", "")
	return c.presignURL("POST", objectKey, q, expiry)
}

// PresignUploadPartURL → PUT {key}?partNumber=N&uploadId=X (response ETag header).
func (c *R2Config) PresignUploadPartURL(objectKey, uploadID string, partNumber int, expiry time.Duration) (string, error) {
	if uploadID == "" || partNumber < 1 || partNumber > 10000 {
		return "", errors.New("invalid multipart part params")
	}
	q := url.Values{}
	q.Set("partNumber", fmt.Sprintf("%d", partNumber))
	q.Set("uploadId", uploadID)
	return c.presignURL("PUT", objectKey, q, expiry)
}

// PresignMultipartCompleteURL → POST {key}?uploadId=X with the parts XML body.
func (c *R2Config) PresignMultipartCompleteURL(objectKey, uploadID string, expiry time.Duration) (string, error) {
	if uploadID == "" {
		return "", errors.New("uploadId required")
	}
	q := url.Values{}
	q.Set("uploadId", uploadID)
	return c.presignURL("POST", objectKey, q, expiry)
}

// PresignMultipartAbortURL → DELETE {key}?uploadId=X (frees stored parts).
func (c *R2Config) PresignMultipartAbortURL(objectKey, uploadID string, expiry time.Duration) (string, error) {
	if uploadID == "" {
		return "", errors.New("uploadId required")
	}
	q := url.Values{}
	q.Set("uploadId", uploadID)
	return c.presignURL("DELETE", objectKey, q, expiry)
}

// presignURL is the shared SigV4 query signer. extraQuery entries (e.g.
// uploads/uploadId/partNumber) participate in the canonical request —
// url.Values.Encode() sorts keys, which is exactly the canonical order
// SigV4 requires.
func (c *R2Config) presignURL(method, objectKey string, extraQuery url.Values, expiry time.Duration) (string, error) {
	if c == nil {
		return "", errors.New("R2Config is nil")
	}
	if objectKey == "" {
		return "", errors.New("objectKey is empty")
	}
	if expiry <= 0 || expiry > 7*24*time.Hour {
		// SigV4 caps query expiry at 7 days, and we'd never legitimately
		// want more than ~1 hour for a client upload.
		return "", fmt.Errorf("invalid expiry %s", expiry)
	}

	// Cloudflare R2 always uses "auto" as the region in the credential scope.
	const region = "auto"
	const service = "s3"

	now := time.Now().UTC()
	amzDate := now.Format("20060102T150405Z")
	dateStamp := now.Format("20060102")
	credentialScope := fmt.Sprintf("%s/%s/%s/aws4_request", dateStamp, region, service)

	host := c.endpointHost()
	// S3 path-style: /<bucket>/<key>. URL-encode each segment of the key
	// individually so e.g. spaces or slashes inside a single segment are
	// preserved correctly. We treat objectKey as already containing slash
	// separators between path segments.
	encodedKey := encodeS3Path(objectKey)
	canonicalURI := "/" + c.Bucket + encodedKey

	// Query parameters are required to be sorted alphabetically by key in
	// the canonical request. We build them as a map and emit in order.
	q := url.Values{}
	q.Set("X-Amz-Algorithm", "AWS4-HMAC-SHA256")
	q.Set("X-Amz-Credential", c.AccessKeyID+"/"+credentialScope)
	q.Set("X-Amz-Date", amzDate)
	q.Set("X-Amz-Expires", fmt.Sprintf("%d", int(expiry.Seconds())))
	q.Set("X-Amz-SignedHeaders", "host")
	for k, vs := range extraQuery {
		for _, v := range vs {
			q.Set(k, v)
		}
	}
	canonicalQuery := q.Encode()

	// Only `host` is signed — keeping signed headers minimal means the
	// client can freely set Content-Type, Content-Length, etc. without
	// invalidating the signature.
	canonicalHeaders := "host:" + host + "\n"
	signedHeaders := "host"

	canonicalRequest := strings.Join([]string{
		method,
		canonicalURI,
		canonicalQuery,
		canonicalHeaders,
		signedHeaders,
		"UNSIGNED-PAYLOAD",
	}, "\n")

	hashedCanonical := sha256Hex([]byte(canonicalRequest))

	stringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256",
		amzDate,
		credentialScope,
		hashedCanonical,
	}, "\n")

	signingKey := deriveSigningKey(c.SecretAccessKey, dateStamp, region, service)
	signature := hex.EncodeToString(hmacSHA256(signingKey, []byte(stringToSign)))

	q.Set("X-Amz-Signature", signature)

	return "https://" + host + canonicalURI + "?" + q.Encode(), nil
}

// PublicURL returns the long-lived URL clients use to fetch an uploaded
// object via R2's public CDN binding. This is what we persist into the
// database — never the presigned URL, which expires.
func (c *R2Config) PublicURL(objectKey string) string {
	if c == nil {
		return ""
	}
	return c.PublicBaseURL + "/" + strings.TrimLeft(objectKey, "/")
}

// ---------- SigV4 primitives ----------

func sha256Hex(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

func hmacSHA256(key, data []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write(data)
	return h.Sum(nil)
}

// deriveSigningKey runs the standard SigV4 four-step HMAC chain.
// Verbose by design — easy to compare against the AWS reference impl.
func deriveSigningKey(secret, dateStamp, region, service string) []byte {
	kDate := hmacSHA256([]byte("AWS4"+secret), []byte(dateStamp))
	kRegion := hmacSHA256(kDate, []byte(region))
	kService := hmacSHA256(kRegion, []byte(service))
	return hmacSHA256(kService, []byte("aws4_request"))
}

// encodeS3Path URL-encodes each '/'-separated segment of an S3 object key
// using the same rules as AWS SigV4 (RFC 3986 unreserved chars only).
// Returns a string starting with '/' — ready to concat after the bucket.
func encodeS3Path(key string) string {
	parts := strings.Split(key, "/")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		// url.PathEscape conveniently uses RFC 3986 path-segment encoding,
		// which matches what SigV4 expects for canonical-URI segments.
		out = append(out, url.PathEscape(p))
	}
	return "/" + strings.Join(out, "/")
}

// ---------- Object key helpers ----------

// newUploadID returns 16 cryptographically random bytes hex-encoded
// (32 chars). Used as the path component grouping all variants of one
// upload — short, opaque, and not enumerable.
func newUploadID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand failure on a healthy system is essentially
		// impossible; fall back to a timestamp-derived id to avoid panic.
		return fmt.Sprintf("ts%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b[:])
}

// MediaKindAllowed is the closed set of media classifications the
// presign endpoint accepts. Keep this minimal — every new kind is a
// new place to validate file extension, content-type, and per-user
// quota.
var mediaKindAllowed = map[string]struct{}{
	"video":     {},
	"thumbnail": {},
}

// variantToExt maps the requested variant name to the file extension we
// store under. For thumbnails we use "jpg" regardless of variant since
// a single thumbnail is enough.
var variantToExt = map[string]string{
	"480p":     "mp4",
	"720p":     "mp4",
	"1080p":    "mp4",
	"original": "mp4",
	"default":  "jpg", // thumbnail
}

// buildObjectKey constructs the S3 path for one variant of one upload.
// Layout: u/<userID>/<uploadID>/<variant>.<ext>
//
// Per-user prefix means a future "delete all my media" job is a single
// prefix scan; per-upload prefix groups variants of the same source so
// we can list them with one round trip.
func buildObjectKey(userID, uploadID, kind, variant string) (string, error) {
	if userID == "" || uploadID == "" {
		return "", errors.New("userID and uploadID required")
	}
	if _, ok := mediaKindAllowed[kind]; !ok {
		return "", fmt.Errorf("invalid media kind %q", kind)
	}
	ext, ok := variantToExt[variant]
	if !ok {
		return "", fmt.Errorf("invalid variant %q", variant)
	}
	// Force thumbnail kind to use jpg, force video kind to use mp4 — this
	// catches a common client bug where the wrong (kind,variant) combo is
	// requested and would otherwise produce a misnamed file.
	if kind == "thumbnail" {
		ext = "jpg"
	} else if kind == "video" {
		ext = "mp4"
	}
	return fmt.Sprintf("u/%s/%s/%s.%s", userID, uploadID, variant, ext), nil
}
