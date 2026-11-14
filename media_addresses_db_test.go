package main

// The day the address in front of the bucket changes, against a real
// database. See media_addresses.go.
//
// Every test here starts the server's own start-up step
// (rememberMediaAddresses) with the settings a real start would read, and
// then goes through the real handlers. Nothing is handed the earlier
// address by hand: if the start-up step stopped writing it down, or the
// clearing code stopped asking for it, these go red.

import (
	"bytes"
	"encoding/json"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/mux"
)

// withMediaAddresses gives the test an empty list of addresses and the
// real storage settings everywhere a folder is worked out. What it returns
// puts back what was there: defer it, after withDB, so that it runs while
// the database is still open.
func withMediaAddresses(t *testing.T) func() {
	t.Helper()
	// A run that was stopped while the table was out of the way (see
	// AFailedReadAtStartIsTriedAgain) left it there. Put it back.
	mustExec(t, `DO $$ BEGIN
	               IF to_regclass('media_addresses') IS NULL
	                  AND to_regclass('media_addresses_away') IS NOT NULL THEN
	                   ALTER TABLE media_addresses_away RENAME TO media_addresses;
	               END IF;
	             END $$`)
	mustExec(t, `ALTER TABLE media_addresses DROP CONSTRAINT IF EXISTS media_addresses_no_writes`)
	mustExec(t, `DELETE FROM media_addresses`)
	beforeMedia, beforeChat, beforePhoto := mediaDeleteStorage, chatMediaStorage, profilePhotoStorage
	mediaDeleteStorage, chatMediaStorage, profilePhotoStorage = loadR2Config, loadR2Config, loadR2Config
	forgetStart()
	// The settings each start set are put back by the test itself (Setenv),
	// after this has run. What was read from them must not outlive that.
	t.Cleanup(forgetStart)
	return func() {
		mediaDeleteStorage, chatMediaStorage, profilePhotoStorage = beforeMedia, beforeChat, beforePhoto
		forgetStart()
		mustExec(t, `DELETE FROM media_addresses`)
	}
}

// forgetStart drops everything a start of the server keeps in memory about
// storage: the settings it read, and the earlier addresses it read back.
func forgetStart() {
	forgetStorageSettings()
	earlierMediaAddresses.Lock()
	earlierMediaAddresses.list = nil
	earlierMediaAddresses.Unlock()
}

// startWithAddress is one start of the server with the bucket's public
// address set to [today] and R2_OLD_PUBLIC_BASE_URLS to [listed]. As after
// a restart, nothing from before is left in memory: whatever the server
// knows about earlier addresses afterwards, it read from the database.
func startWithAddress(t *testing.T, today, listed string) {
	t.Helper()
	t.Setenv("R2_ACCOUNT_ID", "acct")
	t.Setenv("R2_BUCKET", "bucket")
	t.Setenv("R2_ACCESS_KEY_ID", "key")
	t.Setenv("R2_SECRET_ACCESS_KEY", "secret")
	t.Setenv("R2_PUBLIC_BASE_URL", today)
	t.Setenv("R2_OLD_PUBLIC_BASE_URLS", listed)
	forgetStart()
	rememberMediaAddresses()
}

// earlierOnRecord is what the server holds in memory after a start.
func earlierOnRecord() []string {
	earlierMediaAddresses.RLock()
	defer earlierMediaAddresses.RUnlock()
	return append([]string{}, earlierMediaAddresses.list...)
}

// waitQueued waits for [folder] to be queued for removal: a chat file is
// queued off the response path.
func waitQueued(t *testing.T, folder string) bool {
	t.Helper()
	for i := 0; i < 100; i++ {
		if pendingDeletion(t, folder) {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return false
}

func TestMediaAddresses_FilesSavedBeforeTheAddressChangedAreStillCleared(t *testing.T) {
	defer withDB(t)()
	resetRedis(t)
	resetActionLimiters(t)
	defer withMediaAddresses(t)()

	const owner, friend = 9761, 9762
	clean := func() {
		db.Exec(`DELETE FROM chat_messages WHERE sender_id IN (9761, 9762) OR receiver_id IN (9761, 9762)`)
		db.Exec(`DELETE FROM challenges WHERE creator_id IN (9761, 9762)`)
		db.Exec(`DELETE FROM users WHERE id IN (9761, 9762)`)
		db.Exec(`DELETE FROM pending_media_deletions
		          WHERE object_prefix LIKE 'u/976_/%' OR object_prefix LIKE 'chat/976_/%'`)
	}
	clean()
	defer clean()
	people(t, map[int]string{owner: "public", friend: "public"})

	const old = "https://pub-0123456789abcdef.r2.dev"
	const now = "https://media.flexete.test"
	post := func(base, upload, small string) int {
		return mustID(t, `INSERT INTO challenges
		        (creator_id, video_url, thumbnail_url, video_variants, prefix, subject)
		      VALUES ($1, $2, $3, $4::jsonb, 'can you', 'move house') RETURNING id`,
			owner, base+"/u/9761/"+upload+"/720p.mp4", base+"/u/9761/"+upload+"/thumb.jpg",
			`{"480p":"`+base+`/u/9761/`+small+`/480p.mp4"}`)
	}
	deletePost := func(id int) {
		code, _ := asUser(t, DeleteChallengeHandler, "POST", "/api/v1/challenges/delete",
			owner, "person9761", map[string]any{"challengeId": strconv.Itoa(id)})
		if code != http.StatusOK {
			t.Fatalf("deleting post %d answered %d", id, code)
		}
	}

	// Day one. Everything is saved under R2's test address.
	startWithAddress(t, old, "")
	before := post(old, "vidold", "vidsmall")
	mustExec(t, `UPDATE users SET avatar_url = $1 WHERE id = $2`, old+"/u/9761/faceold/photo.jpg", owner)
	chatPhoto := mustID(t, `INSERT INTO chat_messages (sender_id, receiver_id, message, kind, media_url)
	                        VALUES ($1, $2, '', 'photo', $3) RETURNING id`,
		owner, friend, old+"/chat/9761/abcdefgh12/photo.jpg")

	// The address changes, and the server starts again. It says so.
	var logged lockedLog
	log.SetOutput(&logged)
	startWithAddress(t, now, "")
	log.SetOutput(os.Stderr)
	for _, want := range []string{
		"media: new files are handed out at " + now + ".",
		"still cleared from storage when deleted: " + old + ".",
	} {
		if !strings.Contains(logged.String(), want) {
			t.Errorf("the start's log lacks %q:\n%s", want, logged.String())
		}
	}

	// A video posted before the change is deleted by its owner: its folder,
	// and the folder of its other size, go to be cleared.
	deletePost(before)
	if got, want := queuedLike(t, "u/9761/vid%"), []string{"u/9761/vidold/", "u/9761/vidsmall/"}; !reflect.DeepEqual(got, want) {
		t.Errorf("after deleting a video saved under the old address, queued %q, want %q", got, want)
	}

	// The photo set before the change is replaced: the old file goes.
	raw, _ := json.Marshal(map[string]any{"avatarUrl": now + "/u/9761/facenew/photo.jpg"})
	req := withAuth(httptest.NewRequest("PATCH", "/api/v1/users/9761", bytes.NewReader(raw)), "9761", "person9761")
	req = mux.SetURLVars(req, map[string]string{"id": "9761"})
	rec := httptest.NewRecorder()
	UpdateUserProfileHandler(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("setting a new photo answered %d: %s", rec.Code, rec.Body.String())
	}
	if !pendingDeletion(t, "u/9761/faceold/") {
		t.Error("the photo saved under the old address stays in storage after it is replaced")
	}
	if pendingDeletion(t, "u/9761/facenew/") {
		t.Error("the new photo was queued for removal")
	}

	// A chat photo sent before the change is unsent: its file goes.
	code, _ := asUser(t, DeleteMessageHandler, "POST", "/api/v1/chat/delete",
		owner, "person9761", map[string]any{"messageId": strconv.Itoa(chatPhoto)})
	if code != http.StatusOK {
		t.Fatalf("unsending the photo answered %d", code)
	}
	if !waitQueued(t, "chat/9761/abcdefgh12/") {
		t.Error("the chat photo saved under the old address stays in storage after it is unsent")
	}

	// A video posted after the change is cleared as it always was.
	deletePost(post(now, "vidnew", "vidnew"))
	if !pendingDeletion(t, "u/9761/vidnew/") {
		t.Error("a video saved under today's address is not cleared")
	}

	// And an address that was never this server's still clears nothing,
	// however much the rest of it looks like one of our files.
	deletePost(post("https://elsewhere.example", "vidalien", "vidalien"))
	if pendingDeletion(t, "u/9761/vidalien/") {
		t.Error("a file at an address that was never ours was queued for removal")
	}
}

func TestMediaAddresses_AnAccountDeletedAfterTheChangeTakesItsOldFilesToo(t *testing.T) {
	defer withDB(t)()
	resetRedis(t)
	defer withMediaAddresses(t)()

	const leaving, friend = 9763, 9764
	clean := func() {
		db.Exec(`DELETE FROM chat_messages WHERE sender_id IN (9763, 9764) OR receiver_id IN (9763, 9764)`)
		db.Exec(`DELETE FROM challenges WHERE creator_id IN (9763, 9764)`)
		db.Exec(`DELETE FROM users WHERE id IN (9763, 9764)`)
		db.Exec(`DELETE FROM pending_media_deletions
		          WHERE object_prefix LIKE 'u/976_/%' OR object_prefix LIKE 'chat/976_/%'
		             OR object_prefix LIKE 'hls/%'`)
	}
	clean()
	defer clean()
	mustExec(t, `INSERT INTO users (id, username, password) VALUES ($1, 'leaving_9763', 'x')`, leaving)
	mustExec(t, `INSERT INTO users (id, username, password) VALUES ($1, 'friend_9764', 'x')`, friend)

	const old = "https://pub-0123456789abcdef.r2.dev"
	startWithAddress(t, old, "")
	mustExec(t, `UPDATE users SET avatar_url = $1 WHERE id = $2`, old+"/u/9763/face/photo.jpg", leaving)
	mustID(t, `INSERT INTO challenges (creator_id, video_url, thumbnail_url, prefix, subject)
	           VALUES ($1, $2, $3, 'can you', 'leave') RETURNING id`,
		leaving, old+"/u/9763/post/720p.mp4", old+"/u/9763/post/thumb.jpg")
	mustID(t, `INSERT INTO chat_messages (sender_id, receiver_id, message, kind, media_url)
	           VALUES ($1, $2, '', 'photo', $3) RETURNING id`,
		leaving, friend, old+"/chat/9763/abcdefgh12/photo.jpg")

	startWithAddress(t, "https://media.flexete.test", "")
	if n := deleteForGood(t, leaving, "leaving_9763"); n != 1 {
		t.Fatalf("the job deleted %d accounts, want 1", n)
	}
	got := queuedLike(t, "u/9763/%", "chat/9763/%")
	want := []string{"chat/9763/abcdefgh12/", "u/9763/face/", "u/9763/post/"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("after the account was deleted for good, queued %q, want %q", got, want)
	}
}

func TestMediaAddresses_TheListIsKeptInTheDatabase(t *testing.T) {
	defer withDB(t)()
	defer withMediaAddresses(t)()
	const first = "https://pub-0123456789abcdef.r2.dev"
	const second = "https://media.one.test"
	const third = "https://media.two.test"

	check := func(when string, want ...string) {
		t.Helper()
		if want == nil {
			want = []string{}
		}
		if got := earlierOnRecord(); !reflect.DeepEqual(got, want) {
			t.Errorf("%s: earlier addresses %q, want %q", when, got, want)
		}
	}

	startWithAddress(t, first, "")
	check("the first start")
	// The same address at the next start: nothing new, nothing lost, and
	// nothing in the log that reads as a failure. This is every ordinary
	// start, so a line here would be cried at every one of them.
	var logged lockedLog
	log.SetOutput(&logged)
	startWithAddress(t, first+"/", "")
	log.SetOutput(os.Stderr)
	check("the same address again")
	if said := logged.String(); !strings.Contains(said, "media: new files are handed out at "+first+".") ||
		strings.Contains(said, "writing down") || strings.Contains(said, "reading the earlier") {
		t.Errorf("an ordinary start's log:\n%s", said)
	}

	startWithAddress(t, second, "")
	check("after one change", first)
	startWithAddress(t, second, "")
	check("the same address after a change", first)

	// Oldest first.
	startWithAddress(t, third, "")
	check("after two changes", first, second)

	// Back to the first: it is today's again, not an earlier one, and the
	// other two are remembered.
	startWithAddress(t, first, "")
	check("back to the first", second, third)

	// What is on record is one row per address, whatever the slash.
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM media_addresses`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Errorf("%d addresses on record, want 3", n)
	}
}

func TestMediaAddresses_AnAddressListedByHandGoesOnRecord(t *testing.T) {
	defer withDB(t)()
	defer withMediaAddresses(t)()
	const today = "https://media.flexete.test"
	const never = "https://pub-fedcba9876543210.r2.dev"
	file := never + "/u/29/4ab2fd/720p.mp4"

	// The server never started with this address, so nothing wrote it
	// down: a file under it is not ours.
	startWithAddress(t, today, "")
	cfg, err := loadR2Config()
	if err != nil {
		t.Fatal(err)
	}
	if got := mediaPrefixFromPublicURL(cfg, file); got != "" {
		t.Fatalf("an address never used or listed gave the folder %q", got)
	}

	// Listed by hand, it is.
	startWithAddress(t, today, " "+never+"/ , ")
	cfg, _ = loadR2Config()
	if got := mediaPrefixFromPublicURL(cfg, file); got != "u/29/4ab2fd/" {
		t.Errorf("listed by hand: folder %q, want u/29/4ab2fd/", got)
	}

	// And it was written down: taking the setting away later loses nothing.
	startWithAddress(t, today, "")
	cfg, _ = loadR2Config()
	if got := mediaPrefixFromPublicURL(cfg, file); got != "u/29/4ab2fd/" {
		t.Errorf("after the setting was taken away: folder %q, want u/29/4ab2fd/", got)
	}
	if got, want := earlierOnRecord(), []string{never}; !reflect.DeepEqual(got, want) {
		t.Errorf("earlier addresses %q, want %q", got, want)
	}
}

// The database cannot be read at the moment the server starts. The server
// says so, and asks again until it can: it does not run for weeks without
// the list because of one bad moment.
func TestMediaAddresses_AFailedReadAtStartIsTriedAgain(t *testing.T) {
	defer withDB(t)()
	defer withMediaAddresses(t)()
	const first = "https://pub-0123456789abcdef.r2.dev"
	const second = "https://media.flexete.test"
	const byHand = "https://pub-fedcba9876543210.r2.dev"
	before := mediaAddressRetry
	mediaAddressRetry = 25 * time.Millisecond
	defer func() { mediaAddressRetry = before }()

	startWithAddress(t, first, "")

	// The table is out of reach for the next start.
	mustExec(t, `ALTER TABLE media_addresses RENAME TO media_addresses_away`)
	back := false
	putBack := func() {
		if !back {
			mustExec(t, `ALTER TABLE media_addresses_away RENAME TO media_addresses`)
			back = true
		}
	}
	defer putBack()

	var logged lockedLog
	log.SetOutput(&logged)
	defer log.SetOutput(os.Stderr)
	startWithAddress(t, second, byHand)
	// All that is known without the database is what was listed by hand.
	if got, want := earlierOnRecord(), []string{byHand}; !reflect.DeepEqual(got, want) {
		t.Fatalf("with the table out of reach, earlier addresses %q, want %q", got, want)
	}
	if said := logged.String(); !strings.Contains(said, "reading the earlier addresses") ||
		strings.Contains(said, "media: new files") {
		t.Errorf("the failed start's log:\n%s", said)
	}
	// It keeps asking while the table is away, and learns nothing wrong.
	time.Sleep(120 * time.Millisecond)
	if got, want := earlierOnRecord(), []string{byHand}; !reflect.DeepEqual(got, want) {
		t.Fatalf("while the table was still out of reach, earlier addresses %q, want %q", got, want)
	}

	putBack()
	want := []string{first, byHand}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && !reflect.DeepEqual(earlierOnRecord(), want) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := earlierOnRecord(); !reflect.DeepEqual(got, want) {
		t.Fatalf("after the table came back, earlier addresses %q, want %q", got, want)
	}
	// Give the line a moment: it is written just after the list is kept.
	for time.Now().Before(deadline) && !strings.Contains(logged.String(), "media: new files") {
		time.Sleep(10 * time.Millisecond)
	}
	if said := logged.String(); !strings.Contains(said,
		"media: new files are handed out at "+second+".") || !strings.Contains(said, first) {
		t.Errorf("once it could be read, the log:\n%s", said)
	}
	// And today's address went on record too, though the first try at
	// writing it failed.
	if n := countRows(t, `SELECT COUNT(*) FROM media_addresses WHERE base = $1`, second); n != 1 {
		t.Errorf("today's address is on record %d times, want once", n)
	}
	// Once it has the list it stops asking: the line is said once.
	time.Sleep(150 * time.Millisecond)
	if n := strings.Count(logged.String(), "media: new files"); n != 1 {
		t.Errorf("the line was said %d times after the list was read, want once", n)
	}
}

// The list can be read but today's address cannot be written. What was
// read is used at once, and the write is tried again: an address that is
// not on record is the one whose files are left behind at the NEXT change.
func TestMediaAddresses_AFailedWriteAtStartIsTriedAgain(t *testing.T) {
	defer withDB(t)()
	defer withMediaAddresses(t)()
	const first = "https://pub-0123456789abcdef.r2.dev"
	const second = "https://media.flexete.test"
	before := mediaAddressRetry
	mediaAddressRetry = 25 * time.Millisecond
	defer func() { mediaAddressRetry = before }()

	startWithAddress(t, first, "")

	// Nothing new can be written; what is there can be read.
	mustExec(t, `ALTER TABLE media_addresses
	             ADD CONSTRAINT media_addresses_no_writes CHECK (base = '') NOT VALID`)
	lifted := false
	lift := func() {
		if !lifted {
			mustExec(t, `ALTER TABLE media_addresses DROP CONSTRAINT media_addresses_no_writes`)
			lifted = true
		}
	}
	defer lift()

	var logged lockedLog
	log.SetOutput(&logged)
	defer log.SetOutput(os.Stderr)
	startWithAddress(t, second, "")
	if got, want := earlierOnRecord(), []string{first}; !reflect.DeepEqual(got, want) {
		t.Fatalf("what could be read is not used: earlier addresses %q, want %q", got, want)
	}
	if said := logged.String(); !strings.Contains(said, "writing down the address") {
		t.Errorf("the failed write is not in the log:\n%s", said)
	}
	if n := countRows(t, `SELECT COUNT(*) FROM media_addresses WHERE base = $1`, second); n != 0 {
		t.Fatalf("today's address is on record though it could not be written")
	}

	lift()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) &&
		countRows(t, `SELECT COUNT(*) FROM media_addresses WHERE base = $1`, second) != 1 {
		time.Sleep(10 * time.Millisecond)
	}
	if n := countRows(t, `SELECT COUNT(*) FROM media_addresses WHERE base = $1`, second); n != 1 {
		t.Fatalf("today's address is on record %d times after the write could go through, want once", n)
	}
	for time.Now().Before(deadline) && !strings.Contains(logged.String(), "media: new files") {
		time.Sleep(10 * time.Millisecond)
	}
	if !strings.Contains(logged.String(), "media: new files are handed out at "+second+".") {
		t.Errorf("once it was written, the log:\n%s", logged.String())
	}
}

// A server with the storage keys but no R2_PUBLIC_BASE_URL hands files
// out at a made-up address. It says so at start. And the day the real
// address is set, what was saved under the made-up one is still ours.
func TestMediaAddresses_WithNoAddressSetTheLogSaysSo(t *testing.T) {
	defer withDB(t)()
	defer withMediaAddresses(t)()
	const guess = "https://pub-acct.r2.dev/bucket"

	var logged lockedLog
	log.SetOutput(&logged)
	startWithAddress(t, "", "")
	log.SetOutput(os.Stderr)
	for _, want := range []string{"media: R2_PUBLIC_BASE_URL is not set.", guess, "will not load"} {
		if !strings.Contains(logged.String(), want) {
			t.Errorf("the start's log lacks %q:\n%s", want, logged.String())
		}
	}

	logged = lockedLog{}
	log.SetOutput(&logged)
	startWithAddress(t, "https://media.flexete.test", "")
	log.SetOutput(os.Stderr)
	if strings.Contains(logged.String(), "is not set") {
		t.Errorf("with the address set, the log still says it is not:\n%s", logged.String())
	}
	if got, want := earlierOnRecord(), []string{guess}; !reflect.DeepEqual(got, want) {
		t.Errorf("earlier addresses %q, want %q", got, want)
	}
	cfg, err := loadR2Config()
	if err != nil {
		t.Fatal(err)
	}
	if got := mediaPrefixFromPublicURL(cfg, guess+"/u/29/4ab2fd/720p.mp4"); got != "u/29/4ab2fd/" {
		t.Errorf("a file saved under the guessed address: folder %q, want u/29/4ab2fd/", got)
	}
}

// With no storage set up nothing is written: there is no address to
// remember, and an empty one must never go on record as "ours".
func TestMediaAddresses_NothingIsWrittenWhenStorageIsNotSetUp(t *testing.T) {
	defer withDB(t)()
	defer withMediaAddresses(t)()
	for _, k := range []string{"R2_ACCOUNT_ID", "R2_BUCKET", "R2_ACCESS_KEY_ID",
		"R2_SECRET_ACCESS_KEY", "R2_PUBLIC_BASE_URL", "R2_OLD_PUBLIC_BASE_URLS"} {
		t.Setenv(k, "")
	}
	forgetStart()
	rememberMediaAddresses()
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM media_addresses`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Errorf("%d addresses written down with no storage set up", n)
	}
}

// The step runs at every start, before the first thing that deletes
// (the start-up account checks delete the demo accounts and queue their
// files).
func TestMediaAddresses_AreReadAtStartBeforeAnythingIsDeleted(t *testing.T) {
	src := codeOf(t, "main.go")
	read := strings.Index(src, "\trememberMediaAddresses()\n")
	if read < 0 {
		t.Fatal("the server never reads its earlier addresses at start: after the address " +
			"changes, files saved under the old one are never cleared from storage")
	}
	for _, later := range []string{"go startupAccountChecks()", "startMediaDeleter()", "startAccountPurger()"} {
		at := strings.Index(src, later)
		if at < 0 {
			t.Fatalf("main.go no longer has %s", later)
		}
		if at < read {
			t.Errorf("%s starts before the earlier addresses are read", later)
		}
	}
	if db := strings.Index(src, `gate.bringUp("database", InitDatabase)`); db < 0 || db > read {
		t.Error("the earlier addresses are read before the database is up")
	}
}
