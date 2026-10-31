package main

// Deleting an account removes only the files the database lists for it.
//
// It used to clear the person's whole folder, u/<their number>/. Numbers are
// only unique inside one database, and the database has already been
// replaced once: numbering started again from 1 while the old uploads stayed
// in storage under the old numbers. A new account can share an old one's
// number, and clearing "its" folder wipes the old person's videos.
//
// These run against a real database. They check what is QUEUED for removal;
// nothing here touches a real bucket.

import (
	"context"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/lib/pq"
)

// withDeleteStorage makes the delete helpers work out folders for a stand-in
// storage address, and returns that address.
func withDeleteStorage(t *testing.T) string {
	t.Helper()
	const base = "https://media.example"
	stand := func() (*R2Config, error) { return &R2Config{PublicBaseURL: base}, nil }
	beforeMedia, beforeChat := mediaDeleteStorage, chatMediaStorage
	mediaDeleteStorage, chatMediaStorage = stand, stand
	t.Cleanup(func() { mediaDeleteStorage, chatMediaStorage = beforeMedia, beforeChat })
	return base
}

// mustExec runs one statement the test depends on.
func mustExec(t *testing.T, query string, args ...any) {
	t.Helper()
	if _, err := db.Exec(query, args...); err != nil {
		t.Fatalf("%s: %v", strings.Join(strings.Fields(query), " "), err)
	}
}

// mustID runs an INSERT ... RETURNING id.
func mustID(t *testing.T, query string, args ...any) int {
	t.Helper()
	var id int
	if err := db.QueryRow(query, args...).Scan(&id); err != nil {
		t.Fatalf("%s: %v", strings.Join(strings.Fields(query), " "), err)
	}
	return id
}

// queuedLike lists the queued folders matching any of the LIKE patterns.
func queuedLike(t *testing.T, patterns ...string) []string {
	t.Helper()
	var out []string
	for _, p := range patterns {
		rows, err := db.Query(`SELECT object_prefix FROM pending_media_deletions
		                        WHERE object_prefix LIKE $1`, p)
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			var s string
			if err := rows.Scan(&s); err != nil {
				t.Fatal(err)
			}
			out = append(out, s)
		}
		rows.Close()
	}
	sort.Strings(out)
	return out
}

// deleteAccount calls the real handler as that person: it asks for the
// deletion, which starts the 30 days (account_leaving.go).
func deleteAccount(t *testing.T, id int, username string) *httptest.ResponseRecorder {
	t.Helper()
	sid := strconv.Itoa(id)
	r := withAuth(httptest.NewRequest("DELETE", "/api/v1/users/"+sid, nil), sid, username)
	r = mux.SetURLVars(r, map[string]string{"id": sid})
	w := httptest.NewRecorder()
	DeleteAccountHandler(w, r)
	return w
}

// deleteForGood asks for the deletion through the real handler, lets the 30
// days pass, and runs the real job that finishes it. Says how many accounts
// the job deleted.
func deleteForGood(t *testing.T, id int, username string) int {
	t.Helper()
	if w := deleteAccount(t, id, username); w.Code != 200 {
		t.Fatalf("asking to delete answered %d: %s", w.Code, w.Body.String())
	}
	mustExec(t, `UPDATE users SET deletion_requested_at = NOW() - INTERVAL '31 days' WHERE id = $1`, id)
	n, err := purgeDueAccounts(context.Background(), time.Now())
	if err != nil {
		t.Fatalf("the job that deletes accounts for good: %v", err)
	}
	return n
}

func TestAccountDelete_OnlyWhatTheDatabaseListsIsQueued(t *testing.T) {
	defer withDB(t)()
	resetRedis(t)
	base := withDeleteStorage(t)

	const leaving, friend, third = 9711, 9712, 9713
	clean := func() {
		db.Exec(`DELETE FROM chat_messages WHERE sender_id IN (9711, 9712, 9713)
		            OR receiver_id IN (9711, 9712, 9713)`)
		db.Exec(`DELETE FROM challenges WHERE creator_id IN (9711, 9712, 9713)`)
		db.Exec(`DELETE FROM users WHERE id IN (9711, 9712, 9713)`)
		db.Exec(`DELETE FROM pending_media_deletions
		          WHERE object_prefix LIKE 'u/971_/%' OR object_prefix LIKE 'chat/971_/%'`)
	}
	clean()
	defer clean()

	for id, name := range map[int]string{leaving: "leaving_user", friend: "friend_user", third: "third_user"} {
		mustExec(t, `INSERT INTO users (id, username, password) VALUES ($1, $2, 'x')`, id, name)
	}
	mustExec(t, `UPDATE users SET avatar_url = $1 WHERE id = $2`, base+"/u/9711/face/photo.jpg", leaving)

	// Their post, and the friend's answer to it.
	post := mustID(t, `INSERT INTO challenges (creator_id, video_url, thumbnail_url, prefix, subject)
	                   VALUES ($1, $2, $3, 'can you', 'leave') RETURNING id`,
		leaving, base+"/u/9711/post1/720p.mp4", base+"/u/9711/post1/default.jpg")
	friendsAnswer := mustID(t, `INSERT INTO challenge_responses (challenge_id, responder_id, video_url)
	                            VALUES ($1, $2, $3) RETURNING id`,
		post, friend, base+"/u/9712/fans/720p.mp4")

	// The friend's post, which stays, and their answer to it, which goes.
	friendsPost := mustID(t, `INSERT INTO challenges (creator_id, video_url, prefix, subject)
	                          VALUES ($1, $2, 'can you', 'stay') RETURNING id`,
		friend, base+"/u/9712/fpost/720p.mp4")
	theirAnswer := mustID(t, `INSERT INTO challenge_responses (challenge_id, responder_id, video_url, thumbnail_url)
	                          VALUES ($1, $2, $3, $4) RETURNING id`,
		friendsPost, leaving, base+"/u/9711/ans1/720p.mp4", base+"/u/9711/ans1/default.jpg")

	// Their chats: a photo they sent, a voice note they were sent, and a
	// photo the friend forwarded on to somebody else, whose chat stays.
	chat := `INSERT INTO chat_messages (sender_id, receiver_id, message, kind, media_url)
	         VALUES ($1, $2, '', $3, $4)`
	mustExec(t, chat, leaving, friend, "photo", base+"/chat/9711/aaaaaaaa/photo.jpg")
	mustExec(t, chat, friend, leaving, "voice", base+"/chat/9712/bbbbbbbb/voice.m4a")
	mustExec(t, chat, leaving, friend, "photo", base+"/chat/9711/cccccccc/photo.jpg")
	mustExec(t, chat, friend, third, "photo", base+"/chat/9711/cccccccc/photo.jpg")

	if n := deleteForGood(t, leaving, "leaving_user"); n != 1 {
		t.Fatalf("the job deleted %d accounts, want 1", n)
	}
	if _, found := GetUserByID("9711"); found {
		t.Fatal("the account is still there")
	}

	got := queuedLike(t, "u/971_/%", "chat/971_/%")
	want := []string{
		"chat/9711/aaaaaaaa/", // the photo they sent
		"chat/9712/bbbbbbbb/", // the voice note they were sent: that chat is gone too
		"u/9711/ans1/",        // their answer to the friend's post
		"u/9711/face/",        // their profile photo
		"u/9711/post1/",       // their post
		"u/9712/fans/",        // the friend's answer to their post, which went with it
	}
	if strings.Join(got, " ") != strings.Join(want, " ") {
		t.Errorf("queued for removal:\n  %s\nwant exactly:\n  %s\n"+
			"u/9711/ on its own would clear the whole folder — and with it any old "+
			"account's uploads that share the number. chat/9711/cccccccc/ is still "+
			"shown in a chat that stays. u/9712/fpost/ is the friend's post, which stays.",
			strings.Join(got, "\n  "), strings.Join(want, "\n  "))
	}
	// The streaming copies, which are filed by row.
	copies := []string{
		"hls/" + strconv.Itoa(post) + "/",
		"hls/resp/" + strconv.Itoa(friendsAnswer) + "/",
		"hls/resp/" + strconv.Itoa(theirAnswer) + "/",
	}
	defer db.Exec(`DELETE FROM pending_media_deletions WHERE object_prefix = ANY($1)`, pq.Array(copies))
	for _, p := range copies {
		if !pendingDeletion(t, p) {
			t.Errorf("%s is not queued, so those streaming copies stay in storage", p)
		}
	}

	// And what stays, stays.
	var stays bool
	if err := db.QueryRow(`SELECT EXISTS(SELECT 1 FROM challenges WHERE id = $1)`, friendsPost).Scan(&stays); err != nil || !stays {
		t.Errorf("the friend's post went too (err %v)", err)
	}
	if err := db.QueryRow(`SELECT EXISTS(SELECT 1 FROM chat_messages
	                        WHERE sender_id = $1 AND receiver_id = $2)`, friend, third).Scan(&stays); err != nil || !stays {
		t.Errorf("the forwarded photo's message, between two people who stay, went too (err %v)", err)
	}
}

func TestAccountDelete_AProfilePhotoThatIsNotTheirsIsLeftAlone(t *testing.T) {
	// The app only takes a photo from the person's own uploads, but one
	// saved before that check existed could point at somebody else's file.
	defer withDB(t)()
	resetRedis(t)
	base := withDeleteStorage(t)
	clean := func() {
		db.Exec(`DELETE FROM users WHERE id = 9731`)
		db.Exec(`DELETE FROM pending_media_deletions WHERE object_prefix LIKE 'u/9732/%'`)
	}
	clean()
	defer clean()
	mustExec(t, `INSERT INTO users (id, username, password, avatar_url) VALUES (9731, 'borrowed_photo', 'x', $1)`,
		base+"/u/9732/someone/photo.jpg")

	if n := deleteForGood(t, 9731, "borrowed_photo"); n != 1 {
		t.Fatalf("the job deleted %d accounts, want 1", n)
	}
	if pendingDeletion(t, "u/9732/someone/") {
		t.Error("deleting one account queued another person's upload for removal")
	}
}

func TestAccountDelete_ADeleteThatFailsKeepsTheFiles(t *testing.T) {
	// The files are queued only once the account is really gone. Queued
	// first, a delete that failed half way would leave the account standing
	// with its photo and its answers removed from under it.
	defer withDB(t)()
	resetRedis(t)
	base := withDeleteStorage(t)
	clean := func() {
		db.Exec(`DROP TRIGGER IF EXISTS refuse_9721 ON users`)
		db.Exec(`DROP FUNCTION IF EXISTS refuse_9721()`)
		db.Exec(`DELETE FROM chat_messages WHERE sender_id = 9721 OR receiver_id = 9721`)
		db.Exec(`DELETE FROM challenges WHERE creator_id IN (9721, 9722)`)
		db.Exec(`DELETE FROM users WHERE id IN (9721, 9722)`)
		db.Exec(`DELETE FROM pending_media_deletions
		          WHERE object_prefix LIKE 'u/9721/%' OR object_prefix LIKE 'chat/9721/%'`)
	}
	clean()
	defer clean()

	mustExec(t, `INSERT INTO users (id, username, password, avatar_url) VALUES (9721, 'staying_put', 'x', $1)`,
		base+"/u/9721/face/photo.jpg")
	mustExec(t, `INSERT INTO users (id, username, password) VALUES (9722, 'their_friend', 'x')`)
	friendsPost := mustID(t, `INSERT INTO challenges (creator_id, video_url, prefix, subject)
	                          VALUES (9722, $1, 'can you', 'stay') RETURNING id`, base+"/u/9722/p/720p.mp4")
	answer := mustID(t, `INSERT INTO challenge_responses (challenge_id, responder_id, video_url)
	                     VALUES ($1, 9721, $2) RETURNING id`, friendsPost, base+"/u/9721/ans/720p.mp4")
	answerCopies := "hls/resp/" + strconv.Itoa(answer) + "/"
	defer db.Exec(`DELETE FROM pending_media_deletions WHERE object_prefix = $1`, answerCopies)
	mustExec(t, `INSERT INTO chat_messages (sender_id, receiver_id, message, kind, media_url)
	             VALUES (9721, 9722, '', 'photo', $1)`, base+"/chat/9721/dddddddd/photo.jpg")

	// The database refuses to remove this one person, so the delete fails
	// at its very last step.
	mustExec(t, `CREATE FUNCTION refuse_9721() RETURNS trigger AS $$
	             BEGIN
	               IF OLD.id = 9721 THEN RAISE EXCEPTION 'refused for the test'; END IF;
	               RETURN OLD;
	             END $$ LANGUAGE plpgsql`)
	mustExec(t, `CREATE TRIGGER refuse_9721 BEFORE DELETE ON users
	             FOR EACH ROW EXECUTE FUNCTION refuse_9721()`)

	if n := deleteForGood(t, 9721, "staying_put"); n != 0 {
		t.Fatalf("a delete the database refused was counted as done (%d)", n)
	}
	if _, found := GetUserByID("9721"); !found {
		t.Fatal("the account went anyway, so this test proves nothing")
	}
	if got := queuedLike(t, "u/9721/%", "chat/9721/%"); len(got) > 0 {
		t.Errorf("the account is still there, but its files were queued for removal: %v", got)
	}
	if pendingDeletion(t, answerCopies) {
		t.Error("the answer is still there, but its streaming copies were queued for removal")
	}
}

func TestChatFileFolder_OnlyOneFilesOwnFolder(t *testing.T) {
	const base = "https://media.example"
	before := chatMediaStorage
	chatMediaStorage = func() (*R2Config, error) { return &R2Config{PublicBaseURL: base}, nil }
	t.Cleanup(func() { chatMediaStorage = before })

	for url, want := range map[string]string{
		base + "/chat/7/abcdefgh/photo.jpg": "chat/7/abcdefgh/",
		base + "/chat/7/abcdefgh/voice.m4a": "chat/7/abcdefgh/",
		"":                                  "",
		"https://elsewhere.example/chat/7/abcdefgh/photo.jpg": "", // not our storage
		base + "/chat/7/abcdefgh/movie.mp4":                   "", // not a chat file
		base + "/chat/7/a/b/photo.jpg":                        "", // extra folders
		base + "/u/7/abcdefgh/photo.jpg":                      "", // an upload, not a chat file
		// No sender: tidied up, this would be chat/12345678/ — the whole
		// chat folder of user 12345678.
		base + "/chat//12345678/photo.jpg":   "",
		base + "/chat/../12345678/photo.jpg": "",
	} {
		if got := chatFileFolder(url); got != want {
			t.Errorf("chatFileFolder(%q) = %q, want %q", url, got, want)
		}
	}
}
