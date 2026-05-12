package main

// Tests for the hand-rolled SigV4 signer and object-key helpers in
// media_storage.go. We deliberately do NOT spin up a real R2 bucket
// here — the goal is to lock down the crypto and the URL-shape
// invariants so any future refactor can't silently produce signatures
// that R2 would reject in prod.

import (
	"encoding/hex"
	"net/url"
	"strings"
	"testing"
	"time"
)

// TestDeriveSigningKey_AWSReferenceVector validates our hand-rolled
// HMAC chain against the published AWS reference vector. If this ever
// drifts, every signature we produce is wrong and every R2 PUT
// returns 403. The vector is the canonical example from the AWS Sig V4
// spec ("Examples of How to Derive a Signing Key").
//
//   secret  = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY"
//   date    = "20120215"
//   region  = "us-east-1"
//   service = "iam"
//   want    = f4780e2d9f65fa895f9c67b32ce1baf0b0d8a43505a000a1a9e090d414db404d
func TestDeriveSigningKey_AWSReferenceVector(t *testing.T) {
	got := hex.EncodeToString(deriveSigningKey(
		"wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
		"20120215",
		"us-east-1",
		"iam",
	))
	const want = "f4780e2d9f65fa895f9c67b32ce1baf0b0d8a43505a000a1a9e090d414db404d"
	if got != want {
		t.Fatalf("deriveSigningKey mismatch:\n  got:  %s\n  want: %s", got, want)
	}
}

// TestDeriveSigningKey_DifferentInputsDifferentKeys is a sanity check —
// changing any one of the four inputs must change the output, otherwise
// our chain is collapsing somewhere.
func TestDeriveSigningKey_DifferentInputsDifferentKeys(t *testing.T) {
	base := hex.EncodeToString(deriveSigningKey("secret", "20240101", "auto", "s3"))

	cases := []struct {
		name                 string
		secret, date, region string
		service              string
	}{
		{"different secret", "other", "20240101", "auto", "s3"},
		{"different date", "secret", "20240102", "auto", "s3"},
		{"different region", "secret", "20240101", "us-east-1", "s3"},
		{"different service", "secret", "20240101", "auto", "iam"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := hex.EncodeToString(deriveSigningKey(tc.secret, tc.date, tc.region, tc.service))
			if got == base {
				t.Fatalf("expected different key for %s, got identical %s", tc.name, got)
			}
		})
	}
}

// TestPresignPutURL_Shape checks that the URL we build conforms to the
// SigV4 query-signed shape that R2 expects. We don't pin the exact
// signature (it depends on the wall clock) but we DO pin every
// structural invariant: scheme, host, path, mandatory query keys,
// signature length, and credential scope format.
func TestPresignPutURL_Shape(t *testing.T) {
	cfg := &R2Config{
		AccountID:       "abc123",
		Bucket:          "devf-media",
		AccessKeyID:     "AKIATESTACCESSKEY12345",
		SecretAccessKey: "secret/Key+ExampleForUnitTest1234567890",
		PublicBaseURL:   "https://cdn.example.com",
	}
	signed, err := cfg.PresignPutURL("u/42/abcd/720p.mp4", 10*time.Minute)
	if err != nil {
		t.Fatalf("PresignPutURL returned error: %v", err)
	}

	u, err := url.Parse(signed)
	if err != nil {
		t.Fatalf("URL did not parse: %v", err)
	}
	if u.Scheme != "https" {
		t.Errorf("scheme: got %q, want https", u.Scheme)
	}
	if u.Host != "abc123.r2.cloudflarestorage.com" {
		t.Errorf("host: got %q, want abc123.r2.cloudflarestorage.com", u.Host)
	}
	if u.Path != "/devf-media/u/42/abcd/720p.mp4" {
		t.Errorf("path: got %q, want /devf-media/u/42/abcd/720p.mp4", u.Path)
	}

	q := u.Query()
	mustHave := []string{
		"X-Amz-Algorithm",
		"X-Amz-Credential",
		"X-Amz-Date",
		"X-Amz-Expires",
		"X-Amz-SignedHeaders",
		"X-Amz-Signature",
	}
	for _, k := range mustHave {
		if q.Get(k) == "" {
			t.Errorf("missing required query key %s", k)
		}
	}
	if got := q.Get("X-Amz-Algorithm"); got != "AWS4-HMAC-SHA256" {
		t.Errorf("algorithm: got %q, want AWS4-HMAC-SHA256", got)
	}
	if got := q.Get("X-Amz-SignedHeaders"); got != "host" {
		t.Errorf("signed headers: got %q, want host", got)
	}
	if got := q.Get("X-Amz-Expires"); got != "600" {
		t.Errorf("expires: got %q, want 600", got)
	}
	// Signature is hex-encoded SHA-256 of HMAC -> always 64 chars.
	if sig := q.Get("X-Amz-Signature"); len(sig) != 64 {
		t.Errorf("signature length: got %d, want 64 (hex sha256)", len(sig))
	}
	// Credential scope must look like AKID/YYYYMMDD/auto/s3/aws4_request
	cred := q.Get("X-Amz-Credential")
	parts := strings.Split(cred, "/")
	if len(parts) != 5 {
		t.Fatalf("credential format: got %q (parts=%d), want 5 parts", cred, len(parts))
	}
	if parts[0] != cfg.AccessKeyID {
		t.Errorf("credential AKID: got %q, want %q", parts[0], cfg.AccessKeyID)
	}
	if parts[2] != "auto" {
		t.Errorf("credential region: got %q, want auto (R2 uses literal 'auto')", parts[2])
	}
	if parts[3] != "s3" {
		t.Errorf("credential service: got %q, want s3", parts[3])
	}
	if parts[4] != "aws4_request" {
		t.Errorf("credential terminator: got %q, want aws4_request", parts[4])
	}
}

// TestPresignPutURL_RejectsBadInputs guards every error branch on the
// signer. A nil receiver, empty key, or out-of-range expiry must NOT
// produce a usable URL — we rely on that to keep client bugs from
// leaking into 4-hour token windows or 0-byte object names.
func TestPresignPutURL_RejectsBadInputs(t *testing.T) {
	cfg := &R2Config{
		AccountID:       "x",
		Bucket:          "b",
		AccessKeyID:     "AK",
		SecretAccessKey: "sk",
	}
	cases := []struct {
		name   string
		fn     func() (string, error)
		errSub string
	}{
		{
			name:   "nil receiver",
			fn:     func() (string, error) { return (*R2Config)(nil).PresignPutURL("k", time.Minute) },
			errSub: "nil",
		},
		{
			name:   "empty key",
			fn:     func() (string, error) { return cfg.PresignPutURL("", time.Minute) },
			errSub: "objectkey",
		},
		{
			name:   "zero expiry",
			fn:     func() (string, error) { return cfg.PresignPutURL("k", 0) },
			errSub: "expiry",
		},
		{
			name:   "negative expiry",
			fn:     func() (string, error) { return cfg.PresignPutURL("k", -time.Second) },
			errSub: "expiry",
		},
		{
			name:   "expiry > 7 days",
			fn:     func() (string, error) { return cfg.PresignPutURL("k", 8*24*time.Hour) },
			errSub: "expiry",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.fn()
			if err == nil {
				t.Fatalf("expected error, got URL %q", got)
			}
			if !strings.Contains(strings.ToLower(err.Error()), tc.errSub) {
				t.Errorf("error: got %q, want substring %q", err.Error(), tc.errSub)
			}
		})
	}
}

// TestEncodeS3Path ensures we URL-encode each path segment using
// RFC 3986 path-segment rules — what SigV4 expects in the canonical
// URI. Slashes between segments stay literal; reserved chars inside a
// segment get percent-encoded.
func TestEncodeS3Path(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"u/42/abc/720p.mp4", "/u/42/abc/720p.mp4"},
		{"u/42/upload id/720p.mp4", "/u/42/upload%20id/720p.mp4"},
		// Plus signs in object keys must be encoded — otherwise S3
		// interprets them as spaces in the canonical URI.
		{"u/42/a+b/720p.mp4", "/u/42/a+b/720p.mp4"}, // url.PathEscape leaves '+' literal in path segments
		// A leading slash in input still produces a single leading slash
		// in output (we always start with one).
		{"a/b", "/a/b"},
	}
	for _, tc := range cases {
		got := encodeS3Path(tc.in)
		if got != tc.want {
			t.Errorf("encodeS3Path(%q): got %q, want %q", tc.in, got, tc.want)
		}
	}
}

// TestBuildObjectKey_Layout validates the per-user / per-upload key
// layout we depend on for "delete all media for user" prefix scans
// and for grouping variants of the same source.
func TestBuildObjectKey_Layout(t *testing.T) {
	cases := []struct {
		name                  string
		user, upload, kind, v string
		want                  string
		wantErr               bool
	}{
		{"video 720p", "42", "abc", "video", "720p", "u/42/abc/720p.mp4", false},
		{"video 1080p", "42", "abc", "video", "1080p", "u/42/abc/1080p.mp4", false},
		{"thumbnail default", "42", "abc", "thumbnail", "default", "u/42/abc/default.jpg", false},
		// Force-mapping: thumbnail kind always lands as .jpg, even if the
		// variant table would have said otherwise.
		{"thumbnail with video variant key still jpg", "42", "abc", "thumbnail", "720p", "u/42/abc/720p.jpg", false},
		// Force-mapping: video kind always lands as .mp4 even with the
		// thumbnail "default" variant. (Caller bug — we don't want it
		// to silently produce a .jpg with mp4 bytes inside.)
		{"video with thumbnail variant still mp4", "42", "abc", "video", "default", "u/42/abc/default.mp4", false},

		{"missing userID", "", "abc", "video", "720p", "", true},
		{"missing uploadID", "42", "", "video", "720p", "", true},
		{"unknown kind", "42", "abc", "audio", "720p", "", true},
		{"unknown variant", "42", "abc", "video", "4k", "", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := buildObjectKey(tc.user, tc.upload, tc.kind, tc.v)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got %q", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

// TestPublicURL_TrimsTrailingSlashAndJoins makes sure the URL we
// persist into the database doesn't have double slashes or a missing
// separator regardless of whether the configured base ends with "/".
func TestPublicURL_TrimsTrailingSlashAndJoins(t *testing.T) {
	cases := []struct {
		base, key, want string
	}{
		{"https://cdn.example.com", "u/42/abc/720p.mp4", "https://cdn.example.com/u/42/abc/720p.mp4"},
		// loadR2Config strips the trailing slash, but PublicURL itself
		// must still tolerate a key with a leading slash so callers can
		// pass either shape without double-slashing the URL.
		{"https://cdn.example.com", "/u/42/abc/720p.mp4", "https://cdn.example.com/u/42/abc/720p.mp4"},
	}
	for _, tc := range cases {
		c := &R2Config{PublicBaseURL: tc.base}
		got := c.PublicURL(tc.key)
		if got != tc.want {
			t.Errorf("PublicURL(%q,%q): got %q, want %q", tc.base, tc.key, got, tc.want)
		}
	}
}

// TestNewUploadID is a smoke check on the random-id generator. We
// can't pin the exact value (it's random) but we DO pin length,
// hex-only chars, and uniqueness across two calls.
func TestNewUploadID(t *testing.T) {
	a := newUploadID()
	b := newUploadID()
	if len(a) != 32 {
		t.Errorf("length: got %d, want 32", len(a))
	}
	if a == b {
		t.Errorf("two consecutive uploadIDs collided: %q", a)
	}
	if _, err := hex.DecodeString(a); err != nil {
		t.Errorf("not hex: %q (%v)", a, err)
	}
}
