package main

// The address in front of the bucket, and the files saved under an earlier
// one. See media_addresses.go.
//
// These need no database. media_addresses_db_test.go walks the whole thing
// against a real one: the address changes, the server starts again, and a
// file saved before is still cleared.

import (
	"reflect"
	"strings"
	"testing"
)

// withEarlierAddresses puts [earlier] where a start of the server would
// have put what the database has on record, and takes it away after.
func withEarlierAddresses(t *testing.T, earlier ...string) {
	t.Helper()
	set := func(list []string) {
		earlierMediaAddresses.Lock()
		earlierMediaAddresses.list = list
		earlierMediaAddresses.Unlock()
	}
	set(earlier)
	t.Cleanup(func() { set(nil) })
}

func TestMediaAddresses_AFileIsOursUnderTodaysAddressOrAnEarlierOne(t *testing.T) {
	const today = "https://media.flexete.com"
	const listed = "https://cdn.listed.example"
	const written = "https://pub-0123456789abcdef.r2.dev"
	c := &R2Config{PublicBaseURL: today, OldPublicBaseURLs: []string{listed}}
	withEarlierAddresses(t, written)

	for _, tc := range []struct {
		name, url, key string
		ours           bool
	}{
		{"today's address", today + "/u/29/4ab2/720p.mp4", "u/29/4ab2/720p.mp4", true},
		{"an earlier one the server wrote down", written + "/u/29/4ab2/720p.mp4", "u/29/4ab2/720p.mp4", true},
		{"an earlier one listed by hand", listed + "/chat/7/abcdefgh/photo.jpg", "chat/7/abcdefgh/photo.jpg", true},
		{"somewhere else", "https://elsewhere.example/u/29/4ab2/720p.mp4", "", false},
		{"an address that only starts like ours", today + ".evil.example/u/29/4ab2/720p.mp4", "", false},
		{"ours with nothing after it", today, "", false},
		{"the same host over plain http", "http://media.flexete.com/u/29/4ab2/720p.mp4", "", false},
		{"nothing", "", "", false},
	} {
		key, ours := c.keyOfStoredFile(tc.url)
		if key != tc.key || ours != tc.ours {
			t.Errorf("%s: keyOfStoredFile(%q) = %q, %v; want %q, %v",
				tc.name, tc.url, key, ours, tc.key, tc.ours)
		}
	}

	// With nothing on record and nothing listed, only today's is ours:
	// the earlier ones are known because they were written down, not
	// because any r2.dev address will do.
	withEarlierAddresses(t)
	bare := &R2Config{PublicBaseURL: today}
	if _, ours := bare.keyOfStoredFile(written + "/u/29/4ab2/720p.mp4"); ours {
		t.Error("an address nobody wrote down or listed was taken as ours")
	}
	if key, ours := bare.keyOfStoredFile(today + "/u/29/4ab2/720p.mp4"); !ours || key != "u/29/4ab2/720p.mp4" {
		t.Errorf("today's address alone: %q, %v", key, ours)
	}
	var none *R2Config
	if _, ours := none.keyOfStoredFile(today + "/u/29/4ab2/720p.mp4"); ours {
		t.Error("with no storage set up, a file was taken as ours")
	}
}

// Every way a folder is worked out for clearing goes through the list:
// videos, their pictures and other sizes, profile photos, chat files.
func TestMediaAddresses_ClearingAcceptsAnEarlierAddress(t *testing.T) {
	const today = "https://media.flexete.com"
	const earlier = "https://pub-0123456789abcdef.r2.dev"
	c := &R2Config{PublicBaseURL: today}
	before := chatMediaStorage
	chatMediaStorage = func() (*R2Config, error) { return c, nil }
	t.Cleanup(func() { chatMediaStorage = before })

	video := earlier + "/u/29/4ab2fd/720p.mp4"
	photo := earlier + "/chat/29/abcdefgh12/photo.jpg"

	// Before the server knows the earlier address, neither is ours. This
	// is what every file saved before a change of address looked like.
	withEarlierAddresses(t)
	if got := mediaPrefixFromPublicURL(c, video); got != "" {
		t.Fatalf("an unknown address gave the folder %q", got)
	}
	if got := chatFileFolder(photo); got != "" {
		t.Fatalf("an unknown address gave the chat folder %q", got)
	}

	withEarlierAddresses(t, earlier)
	if got := mediaPrefixFromPublicURL(c, video); got != "u/29/4ab2fd/" {
		t.Errorf("a video saved under the earlier address: folder %q, want u/29/4ab2fd/", got)
	}
	if got := mediaPrefixFromPublicURL(c, video+"?v=2"); got != "u/29/4ab2fd/" {
		t.Errorf("the same with a question mark after it: folder %q", got)
	}
	if got := chatFileFolder(photo); got != "chat/29/abcdefgh12/" {
		t.Errorf("a chat photo saved under the earlier address: folder %q, want chat/29/abcdefgh12/", got)
	}
	// Today's still works, and the shape of the name is still checked:
	// an earlier address is not a way round "only our own upload layout".
	if got := mediaPrefixFromPublicURL(c, today+"/u/29/4ab2fd/720p.mp4"); got != "u/29/4ab2fd/" {
		t.Errorf("today's address: folder %q", got)
	}
	for _, bad := range []string{
		earlier + "/hls/29/master.m3u8",
		earlier + "/u/29/720p.mp4",
		earlier + "/u//4ab2fd/720p.mp4",
	} {
		if got := mediaPrefixFromPublicURL(c, bad); got != "" {
			t.Errorf("%q gave the folder %q; only u/<user>/<upload>/<file> is ever cleared", bad, got)
		}
	}
	if got := chatFileFolder(earlier + "/chat//abcdefgh12/photo.jpg"); got != "" {
		t.Errorf("a chat address with no sender gave %q", got)
	}
}

// The list is for clearing what was saved. A NEW upload must be under
// today's address: an earlier address is not a way to post a file, set a
// photo or send a chat photo from there.
func TestMediaAddresses_ANewUploadMustBeUnderTodaysAddress(t *testing.T) {
	const today = "https://media.flexete.com"
	const earlier = "https://pub-0123456789abcdef.r2.dev"
	stand := func() (*R2Config, error) { return &R2Config{PublicBaseURL: today}, nil }
	beforePost, beforePhoto, beforeChat := postMediaStorage, profilePhotoStorage, chatMediaStorage
	postMediaStorage, profilePhotoStorage, chatMediaStorage = stand, stand, stand
	t.Cleanup(func() {
		postMediaStorage, profilePhotoStorage, chatMediaStorage = beforePost, beforePhoto, beforeChat
	})
	withEarlierAddresses(t, earlier)

	if !ownUpload("29", today+"/u/29/4ab2fd/720p.mp4") {
		t.Fatal("a post under today's address was refused")
	}
	if ownUpload("29", earlier+"/u/29/4ab2fd/720p.mp4") {
		t.Error("a post was accepted from an earlier address")
	}
	if !validProfilePhoto("29", today+"/u/29/4ab2fd/photo.jpg") {
		t.Fatal("a photo under today's address was refused")
	}
	if validProfilePhoto("29", earlier+"/u/29/4ab2fd/photo.jpg") {
		t.Error("a profile photo was accepted from an earlier address")
	}
	fresh := ChatMedia{Kind: chatKindPhoto, Width: 10, Height: 10,
		URL: today + "/chat/29/abcdefgh12/photo.jpg"}
	if _, err := checkChatMedia("29", "", fresh); err != nil {
		t.Fatalf("a chat photo under today's address was refused: %v", err)
	}
	fresh.URL = earlier + "/chat/29/abcdefgh12/photo.jpg"
	if _, err := checkChatMedia("29", "", fresh); err == nil {
		t.Error("a chat photo was accepted from an earlier address")
	}
}

func TestMediaAddresses_TheFullerAddressIsTriedFirst(t *testing.T) {
	// The address used when R2_PUBLIC_BASE_URL was empty had the bucket's
	// name on the end. Read under the shorter address, a file saved under
	// it would be a file called "bucket/u/29/..." and never be cleared.
	c := &R2Config{PublicBaseURL: "https://pub-acct.r2.dev"}
	withEarlierAddresses(t, "https://pub-acct.r2.dev/bucket")
	if got := mediaPrefixFromPublicURL(c, "https://pub-acct.r2.dev/bucket/u/29/4ab2fd/720p.mp4"); got != "u/29/4ab2fd/" {
		t.Errorf("folder %q, want u/29/4ab2fd/", got)
	}
	// Whichever of the two is today's.
	c = &R2Config{PublicBaseURL: "https://pub-acct.r2.dev/bucket/"}
	withEarlierAddresses(t, "https://pub-acct.r2.dev")
	if got := mediaPrefixFromPublicURL(c, "https://pub-acct.r2.dev/bucket/u/29/4ab2fd/720p.mp4"); got != "u/29/4ab2fd/" {
		t.Errorf("the other way round: folder %q, want u/29/4ab2fd/", got)
	}
	if got := mediaPrefixFromPublicURL(c, "https://pub-acct.r2.dev/u/29/4ab2fd/720p.mp4"); got != "u/29/4ab2fd/" {
		t.Errorf("under the shorter address: folder %q, want u/29/4ab2fd/", got)
	}
}

func TestMediaAddresses_EveryAddressOnceAndTidied(t *testing.T) {
	c := &R2Config{
		PublicBaseURL:     "https://media.flexete.com/",
		OldPublicBaseURLs: []string{"https://old.example/", "https://media.flexete.com"},
	}
	withEarlierAddresses(t, "https://old.example", " ", "https://older.example//")
	got := c.everyPublicAddress()
	want := []string{"https://media.flexete.com", "https://older.example", "https://old.example"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("everyPublicAddress() = %q, want %q", got, want)
	}
}

func TestMediaAddresses_TheListByHandIsRead(t *testing.T) {
	for in, want := range map[string][]string{
		"":                                nil,
		"  ,  ,":                          nil,
		"https://a.example":               {"https://a.example"},
		" https://a.example/ ,https://b":  {"https://a.example", "https://b"},
		"https://a.example//,,https://b/": {"https://a.example", "https://b"},
	} {
		if got := publicAddressList(in); !reflect.DeepEqual(got, want) {
			t.Errorf("publicAddressList(%q) = %q, want %q", in, got, want)
		}
	}
	// And it reaches the settings: R2_OLD_PUBLIC_BASE_URLS is read at start.
	withStorageSettings(t)
	t.Setenv("R2_OLD_PUBLIC_BASE_URLS", "https://pub-old.r2.dev/ , https://cdn.old.example")
	cfg, err := loadR2Config()
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"https://pub-old.r2.dev", "https://cdn.old.example"}; !reflect.DeepEqual(cfg.OldPublicBaseURLs, want) {
		t.Errorf("the settings hold %q, want %q", cfg.OldPublicBaseURLs, want)
	}
}

func TestMediaAddresses_TheLineInTheLog(t *testing.T) {
	const test = "https://pub-0123456789abcdef.r2.dev"
	const own = "https://media.flexete.com"

	line := mediaAddressLine(test, false, nil)
	for _, want := range []string{
		"media: new files are handed out at " + test + ".",
		"No earlier address is on record.",
		"R2's test address",
	} {
		if !strings.Contains(line, want) {
			t.Errorf("on the test address the line lacks %q:\n%s", want, line)
		}
	}

	line = mediaAddressLine(own, false, []string{test})
	for _, want := range []string{
		"media: new files are handed out at " + own + ".",
		"still cleared from storage when deleted: " + test + ".",
	} {
		if !strings.Contains(line, want) {
			t.Errorf("after the change the line lacks %q:\n%s", want, line)
		}
	}
	for _, not := range []string{"R2's test address", "No earlier address"} {
		if strings.Contains(line, not) {
			t.Errorf("after the change the line still says %q:\n%s", not, line)
		}
	}

	line = mediaAddressLine(own, false, []string{test, "https://cdn.old.example"})
	if !strings.Contains(line, "the 2 earlier addresses") ||
		!strings.Contains(line, test+", https://cdn.old.example.") {
		t.Errorf("with two earlier addresses:\n%s", line)
	}
	// No address set at all: the line says the one in use is a guess that
	// does not work, and does not call it R2's test address.
	line = mediaAddressLine("https://pub-acct.r2.dev/bucket", true, nil)
	for _, want := range []string{
		"media: R2_PUBLIC_BASE_URL is not set.",
		"https://pub-acct.r2.dev/bucket",
		"will not load",
	} {
		if !strings.Contains(line, want) {
			t.Errorf("with no address set the line lacks %q:\n%s", want, line)
		}
	}
	if strings.Contains(line, "R2's test address") {
		t.Errorf("a guessed address was called R2's test address:\n%s", line)
	}
	// A name that only ends like the test address is not one.
	if strings.Contains(mediaAddressLine("https://media.r2.dev.example", false, nil), "R2's test address") {
		t.Error("an address that is not R2's was called its test address")
	}
}

// Without a database the answer is what was listed by hand: all that is
// known. And nothing is asked of a database that is not there.
func TestMediaAddresses_WithNoDatabaseOnlyTheListByHand(t *testing.T) {
	before := db
	db = nil
	t.Cleanup(func() { db = before })
	listed := []string{"https://pub-old.r2.dev"}
	got, asked := recordMediaAddresses("https://media.flexete.com", listed)
	if !reflect.DeepEqual(got, listed) || !asked {
		t.Errorf("with no database: %q, %v; want %q and nothing to try again", got, asked, listed)
	}
}

// The worker knows the server's made-up address by spelling it the same
// way (guessedPublicBase in cmd/hls-worker). If one side changed its
// spelling alone, the worker would take the guess for a real address and
// put it on every new video.
func TestMediaAddresses_TheWorkerSpellsTheGuessTheSameWay(t *testing.T) {
	const shape = `"https://pub-%s.r2.dev/%s"`
	for _, file := range []string{"media_storage.go", "cmd/hls-worker/main.go"} {
		if !strings.Contains(codeOf(t, file), shape) {
			t.Errorf("%s no longer spells the made-up address as %s", file, shape)
		}
	}
}
