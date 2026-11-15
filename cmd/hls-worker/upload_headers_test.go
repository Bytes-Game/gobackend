package main

// What a file the worker stores says about how long a copy may be kept.
// See uploadCacheControl.
//
// The uploads here are the worker's real ones, sent through its real
// upload code. Only the last step is a stand-in: where the request would
// leave for storage, it is caught and read instead.

import (
	"context"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
)

// The line, written out here on purpose. A test that compared with the
// worker's own constant would still pass if the constant were wrong.
const expectedCacheControl = "public, max-age=31536000, s-maxage=7200, immutable"

type caughtUpload struct {
	method, path, cacheControl, contentType string
}

type roundTrip func(*http.Request) (*http.Response, error)

func (f roundTrip) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// catchUploads stands in for storage: every request the worker would send
// there is kept instead, and answered as storage answers a good upload.
func catchUploads(t *testing.T) func() []caughtUpload {
	t.Helper()
	var mu sync.Mutex
	var got []caughtUpload
	old := uploadClient
	uploadClient = &http.Client{Transport: roundTrip(func(r *http.Request) (*http.Response, error) {
		if r.Body != nil {
			_, _ = io.Copy(io.Discard, r.Body)
			_ = r.Body.Close()
		}
		mu.Lock()
		got = append(got, caughtUpload{r.Method, r.URL.Path,
			r.Header.Get("Cache-Control"), r.Header.Get("Content-Type")})
		mu.Unlock()
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{},
			Body: io.NopCloser(strings.NewReader(""))}, nil
	})}
	t.Cleanup(func() { uploadClient = old })
	return func() []caughtUpload {
		mu.Lock()
		defer mu.Unlock()
		out := append([]caughtUpload{}, got...)
		sort.Slice(out, func(i, j int) bool { return out[i].path < out[j].path })
		return out
	}
}

func TestUpload_TheLineItself(t *testing.T) {
	if uploadCacheControl != expectedCacheControl {
		t.Fatalf("the worker's line is %q, want %q", uploadCacheControl, expectedCacheControl)
	}
	// The two hours are what makes a deleted video stop opening. Without
	// them Cloudflare keeps its copy for the year the phone is given.
	for _, part := range []string{"public", "max-age=31536000", "s-maxage=7200", "immutable"} {
		if !strings.Contains(uploadCacheControl, part) {
			t.Errorf("the line lacks %q", part)
		}
	}
}

// A whole job's folder goes up the way a real job sends it: every kind of
// file in it carries the line.
func TestUpload_EveryFileOfAJobCarriesTheLine(t *testing.T) {
	caught := catchUploads(t)
	dir := t.TempDir()
	for _, name := range []string{"master.m3u8", "720p.mp4", "poster.jpg",
		"720p/index.m3u8", "720p/seg_000.ts", "720p/seg_001.ts"} {
		path := filepath.Join(dir, name)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte("a file"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	cfg := &workerConfig{R2AccountID: "acct", R2Bucket: "bucket", R2AccessKey: "key", R2SecretKey: "secret"}
	if _, err := uploadTree(context.Background(), cfg, dir, "hls/7/abc"); err != nil {
		t.Fatalf("the upload itself did not go: %v", err)
	}

	got := caught()
	var paths []string
	for _, u := range got {
		paths = append(paths, u.path)
		if u.method != http.MethodPut {
			t.Errorf("%s was sent with %s, not PUT", u.path, u.method)
		}
		if u.cacheControl != expectedCacheControl {
			t.Errorf("%s says %q about how long it may be kept, want %q",
				u.path, u.cacheControl, expectedCacheControl)
		}
	}
	want := []string{
		"/bucket/hls/7/abc/720p.mp4",
		"/bucket/hls/7/abc/720p/index.m3u8",
		"/bucket/hls/7/abc/720p/seg_000.ts",
		"/bucket/hls/7/abc/720p/seg_001.ts",
		"/bucket/hls/7/abc/master.m3u8",
		"/bucket/hls/7/abc/poster.jpg",
	}
	if strings.Join(paths, " ") != strings.Join(want, " ") {
		t.Errorf("what went to storage:\n got %q\nwant %q", paths, want)
	}
}
