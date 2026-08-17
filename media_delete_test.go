package main

// Tests for the parts of media_delete.go that decide WHAT gets deleted.
//
// These matter more than most tests in this repo. Everything else here is
// about showing the right video; this code removes files permanently. A bug
// in mediaPrefixFromPublicURL does not show a wrong thumbnail, it deletes
// somebody's content. So the cases below lean heavily on what must NOT be
// derived: anything we do not positively recognise has to come back empty,
// and an empty prefix has to be refused outright.
//
// Nothing here touches a real bucket.

import (
	"context"
	"strings"
	"testing"
)

func testR2() *R2Config {
	return &R2Config{
		AccountID:       "acct",
		Bucket:          "media",
		AccessKeyID:     "key",
		SecretAccessKey: "secret",
		PublicBaseURL:   "https://pub-abc123.r2.dev",
	}
}

func TestMediaPrefixFromPublicURL(t *testing.T) {
	c := testR2()

	cases := []struct {
		name string
		url  string
		want string
	}{
		{
			name: "a normal upload gives its folder",
			url:  "https://pub-abc123.r2.dev/u/29/4ab2fd/720p.mp4",
			want: "u/29/4ab2fd/",
		},
		{
			name: "the thumbnail beside it gives the same folder",
			url:  "https://pub-abc123.r2.dev/u/29/4ab2fd/default.jpg",
			want: "u/29/4ab2fd/",
		},
		{
			name: "a cache-busting query is ignored",
			url:  "https://pub-abc123.r2.dev/u/29/4ab2fd/720p.mp4?v=2",
			want: "u/29/4ab2fd/",
		},
		{
			name: "a fragment is ignored",
			url:  "https://pub-abc123.r2.dev/u/29/4ab2fd/720p.mp4#t=3",
			want: "u/29/4ab2fd/",
		},

		// Everything below must produce nothing.
		{
			name: "someone else's host is never ours to delete",
			url:  "https://evil.example.com/u/29/4ab2fd/720p.mp4",
			want: "",
		},
		{
			name: "a host that merely starts the same is not ours",
			url:  "https://pub-abc123.r2.dev.evil.com/u/29/4ab2fd/720p.mp4",
			want: "",
		},
		{
			name: "a streaming path is handled separately, not derived here",
			url:  "https://pub-abc123.r2.dev/hls/42/deadbeef/master.m3u8",
			want: "",
		},
		{
			name: "a key with too few parts is not enough to act on",
			url:  "https://pub-abc123.r2.dev/u/29/720p.mp4",
			want: "",
		},
		{
			name: "an empty user id would widen the prefix",
			url:  "https://pub-abc123.r2.dev/u//4ab2fd/720p.mp4",
			want: "",
		},
		{
			name: "an empty upload id would delete everything that user owns",
			url:  "https://pub-abc123.r2.dev/u/29//720p.mp4",
			want: "",
		},
		{
			name: "the bare public base is not a video",
			url:  "https://pub-abc123.r2.dev/",
			want: "",
		},
		{
			name: "an empty string",
			url:  "",
			want: "",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := mediaPrefixFromPublicURL(c, tc.url); got != tc.want {
				t.Errorf("mediaPrefixFromPublicURL(%q)\n got %q\nwant %q",
					tc.url, got, tc.want)
			}
		})
	}
}

// A trailing slash on the configured base is allowed by PublicURL, so the
// reverse direction has to cope with it too.
func TestMediaPrefixFromPublicURL_TrailingSlashOnBase(t *testing.T) {
	c := testR2()
	c.PublicBaseURL = "https://pub-abc123.r2.dev/"
	got := mediaPrefixFromPublicURL(c, "https://pub-abc123.r2.dev/u/7/xyz/480p.mp4")
	if got != "u/7/xyz/" {
		t.Fatalf("got %q, want %q", got, "u/7/xyz/")
	}
}

// Round-trip against the function that builds the keys in the first place,
// so the two cannot drift apart.
func TestMediaPrefixFromPublicURL_RoundTripsBuildObjectKey(t *testing.T) {
	c := testR2()
	for _, variant := range []string{"480p", "720p", "1080p"} {
		key, err := buildObjectKey("29", "4ab2fd", "video", variant)
		if err != nil {
			t.Fatalf("buildObjectKey(%s): %v", variant, err)
		}
		got := mediaPrefixFromPublicURL(c, c.PublicURL(key))
		if got != "u/29/4ab2fd/" {
			t.Errorf("variant %s: got %q, want %q", variant, got, "u/29/4ab2fd/")
		}
	}
	thumb, err := buildObjectKey("29", "4ab2fd", "thumbnail", "default")
	if err != nil {
		t.Fatalf("buildObjectKey(thumbnail): %v", err)
	}
	if got := mediaPrefixFromPublicURL(c, c.PublicURL(thumb)); got != "u/29/4ab2fd/" {
		t.Errorf("thumbnail: got %q, want %q", got, "u/29/4ab2fd/")
	}
}

// The last line of defence. Whatever else goes wrong upstream, a prefix that
// would match the whole bucket must be refused before any listing happens.
func TestDeletePrefix_RefusesPrefixesThatWouldMatchEverything(t *testing.T) {
	c := testR2()
	for _, prefix := range []string{"", "/"} {
		n, err := c.DeletePrefix(context.Background(), prefix)
		if err == nil {
			t.Errorf("DeletePrefix(%q) was allowed; it must be refused", prefix)
		}
		if n != 0 {
			t.Errorf("DeletePrefix(%q) reported %d deletions", prefix, n)
		}
	}
}

func TestURLsInJSON(t *testing.T) {
	blob := `{"480p":"https://pub-abc123.r2.dev/u/1/a/480p.mp4",` +
		`"720p":"https://pub-abc123.r2.dev/u/1/a/720p.mp4","note":"none"}`
	got := urlsInJSON(blob)
	if len(got) != 2 {
		t.Fatalf("got %d urls, want 2: %v", len(got), got)
	}
	for _, u := range got {
		if !strings.HasPrefix(u, "https://") {
			t.Errorf("not a url: %q", u)
		}
	}
	if len(urlsInJSON("{}")) != 0 {
		t.Error("an empty variants map should yield no urls")
	}
	if len(urlsInJSON("")) != 0 {
		t.Error("an empty string should yield no urls")
	}
}

// The listing request has to be signed against the bucket, not against an
// object, or R2 rejects it. This checks the shape without a network call.
func TestPresignBucketURL_SignsTheBucketAndKeepsQuery(t *testing.T) {
	c := testR2()
	q := map[string][]string{
		"list-type": {"2"},
		"prefix":    {"u/29/4ab2fd/"},
	}
	signed, err := c.presignBucketURL("GET", q, 600_000_000_000 /* 10m */)
	if err != nil {
		t.Fatalf("presignBucketURL: %v", err)
	}
	// The path must be exactly /<bucket>/ — no second slash. "/media//" is
	// read by S3 as an object literally named "/", so the listing comes back
	// empty and nothing is ever deleted. The first version of this did that,
	// and a looser assertion here did not catch it.
	path := signed
	if i := strings.Index(path, "?"); i >= 0 {
		path = path[:i]
	}
	if want := "https://acct.r2.cloudflarestorage.com/media/"; path != want {
		t.Errorf("bucket path\n got %q\nwant %q", path, want)
	}
	for _, want := range []string{"list-type=2", "X-Amz-Signature=", "prefix=u"} {
		if !strings.Contains(signed, want) {
			t.Errorf("signed URL is missing %q: %s", want, signed)
		}
	}
}
