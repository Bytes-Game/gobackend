package main

// Keeping an account puts back what it did on other people's things
// (account_leaving.go: putTracesBack).
//
// Thirty days is long enough for the world to move on. A video they liked
// is deleted. An answer they voted for is taken down. A person they followed
// leaves for good. The tables themselves gain and lose columns. None of that
// may stop somebody keeping their account — and before putTracesBack read
// the saved columns, a column added with NOT NULL did exactly that.

import (
	"testing"
	"time"
)

func TestAccountLeaving_KeepingWorksWhateverChangedInTheMeantime(t *testing.T) {
	defer withDB(t)()
	resetRedis(t)

	// The test changes two real tables, so it puts them back however it ends,
	// and first tidies up after a run that was killed half way.
	undoColumns := func() {
		db.Exec(`ALTER TABLE challenge_comments DROP COLUMN IF EXISTS keep_test_pinned`)
		db.Exec(`ALTER TABLE follows DROP COLUMN IF EXISTS keep_test_gone`)
	}
	undoColumns()
	defer undoColumns()
	defer db.Exec(`DELETE FROM leaving_set_aside WHERE user_id IN (9821, 9822, 9823)`)
	defer leaver(t, 9821, "comes_back", "sunset42go")()
	defer leaver(t, 9822, "still_here", "sunset42go")()
	defer leaver(t, 9823, "gone_meanwhile", "sunset42go")()

	stays := mustID(t, `INSERT INTO challenges (creator_id, video_url, prefix, subject)
	                    VALUES (9822, 'https://v/stays.mp4', 'can you', 'stay put') RETURNING id`)
	goes := mustID(t, `INSERT INTO challenges (creator_id, video_url, prefix, subject)
	                   VALUES (9822, 'https://v/goes.mp4', 'can you', 'vanish') RETURNING id`)
	voted := mustID(t, `INSERT INTO challenges (creator_id, video_url, prefix, subject)
	                    VALUES (9822, 'https://v/voted.mp4', 'can you', 'win a vote') RETURNING id`)
	answerGoes := mustID(t, `INSERT INTO challenge_responses (challenge_id, responder_id, video_url)
	                         VALUES ($1, 9822, 'https://v/answer-goes.mp4') RETURNING id`, stays)
	answerStays := mustID(t, `INSERT INTO challenge_responses (challenge_id, responder_id, video_url)
	                          VALUES ($1, 9822, 'https://v/answer-stays.mp4') RETURNING id`, stays)

	// A column follows has when they leave, and loses while they wait.
	mustExec(t, `ALTER TABLE follows ADD COLUMN keep_test_gone INT DEFAULT 7`)

	// Everything they did, twice where it can be: once on something that
	// stays, once on something that will be gone by the time they come back.
	mustExec(t, `INSERT INTO challenge_likes (challenge_id, user_id) VALUES ($1, 9821), ($2, 9821)`, stays, goes)
	mustExec(t, `INSERT INTO challenge_response_likes (response_id, user_id) VALUES ($1, 9821), ($2, 9821)`,
		answerStays, answerGoes)
	mustExec(t, `INSERT INTO challenge_votes (challenge_id, response_id, voter_id)
	             VALUES ($1, NULL, 9821), ($2, $3, 9821), ($4, NULL, 9821)`, voted, stays, answerGoes, goes)
	mustExec(t, `INSERT INTO challenge_dislikes (challenge_id, user_id) VALUES ($1, 9821), ($2, 9821)`, stays, goes)
	mustExec(t, `INSERT INTO challenge_comments (challenge_id, author_id, text, created_at)
	             VALUES ($1, 9821, 'still here', '2026-01-02T03:04:05Z'),
	                    ($2, 9821, 'on a video that goes', NOW())`, stays, goes)
	mustExec(t, `INSERT INTO saved_challenges (user_id, challenge_id) VALUES (9821, $1), (9821, $2)`, stays, goes)
	mustExec(t, `INSERT INTO video_shares (user_id, challenge_id, response_id) VALUES (9821, $1, 0), (9821, $2, 0)`,
		stays, goes)
	mustExec(t, `INSERT INTO follows (follower_id, following_id)
	             VALUES (9821, 9822), (9822, 9821), (9821, 9823), (9823, 9821)`)

	if w := deleteAccount(t, 9821, "comes_back"); w.Code != 200 {
		t.Fatalf("delete answered %d: %s", w.Code, w.Body.String())
	}
	var setAside int
	if err := db.QueryRow(`SELECT COUNT(*) FROM leaving_set_aside WHERE user_id = 9821`).Scan(&setAside); err != nil {
		t.Fatal(err)
	}
	if setAside != 19 {
		t.Fatalf("%d of their 19 marks were set aside", setAside)
	}

	// The 30 days.
	mustExec(t, `DELETE FROM challenges WHERE id = $1`, goes)
	mustExec(t, `DELETE FROM challenge_responses WHERE id = $1`, answerGoes)
	mustExec(t, `DELETE FROM users WHERE id = 9823`)
	mustExec(t, `ALTER TABLE follows DROP COLUMN keep_test_gone`)
	mustExec(t, `ALTER TABLE challenge_comments ADD COLUMN keep_test_pinned BOOLEAN NOT NULL DEFAULT FALSE`)
	// Another server, for the few seconds before it hears they left, lets
	// somebody follow them again.
	mustExec(t, `INSERT INTO follows (follower_id, following_id) VALUES (9822, 9821)`)

	code, got := signInAs(t, "comes_back", "sunset42go", true)
	if code != 200 {
		t.Fatalf("keeping the account answered %d %v: something that changed during the 30 days "+
			"stopped them keeping it (the reason is in the log above)", code, got)
	}

	var left int
	if err := db.QueryRow(`SELECT COUNT(*) FROM leaving_set_aside WHERE user_id = 9821`).Scan(&left); err != nil {
		t.Fatal(err)
	}
	if left != 0 {
		t.Errorf("%d of their marks are still set aside after keeping the account", left)
	}

	// Each mark whose video, answer or person is still there is back.
	for what, q := range map[string]string{
		"their like on the video that stayed": `SELECT COUNT(*) FROM challenge_likes
		                                         WHERE user_id = 9821 AND challenge_id = $1`,
		"their dislike": `SELECT COUNT(*) FROM challenge_dislikes WHERE user_id = 9821 AND challenge_id = $1`,
		"their comment": `SELECT COUNT(*) FROM challenge_comments WHERE author_id = 9821 AND challenge_id = $1`,
		"their save":    `SELECT COUNT(*) FROM saved_challenges WHERE user_id = 9821 AND challenge_id = $1`,
		"their share":   `SELECT COUNT(*) FROM video_shares WHERE user_id = 9821 AND challenge_id = $1`,
	} {
		var n int
		if err := db.QueryRow(q, stays).Scan(&n); err != nil {
			t.Fatalf("%s: %v", what, err)
		}
		if n != 1 {
			t.Errorf("%s did not come back", what)
		}
	}
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM challenge_response_likes WHERE user_id = 9821 AND response_id = $1`,
		answerStays).Scan(&n); err != nil || n != 1 {
		t.Errorf("their like on the answer that stayed did not come back (%d, %v)", n, err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM challenge_votes WHERE voter_id = 9821 AND challenge_id = $1`,
		voted).Scan(&n); err != nil || n != 1 {
		t.Errorf("their vote on the battle that stayed did not come back (%d, %v)", n, err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM follows
	                        WHERE (follower_id = 9821 AND following_id = 9822)
	                           OR (follower_id = 9822 AND following_id = 9821)`).Scan(&n); err != nil || n != 2 {
		t.Errorf("%d of the 2 follows with somebody still here came back (%v)", n, err)
	}
	if err := db.QueryRow(`SELECT likes_count FROM challenges WHERE id = $1`, stays).Scan(&n); err != nil || n != 1 {
		t.Errorf("the video's like count is %d after their like came back, want 1 (%v)", n, err)
	}

	// Their comment comes back as it was, and the column added while they
	// waited holds its default.
	var text string
	var pinned bool
	var at time.Time
	if err := db.QueryRow(`SELECT text, keep_test_pinned, created_at FROM challenge_comments
	                        WHERE author_id = 9821`).Scan(&text, &pinned, &at); err != nil {
		t.Fatal(err)
	}
	if text != "still here" || pinned || !at.Equal(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)) {
		t.Errorf("their comment came back as %q, pinned %v, written %v — want it as it was", text, pinned, at)
	}
}

// Two people who follow each other both ask to be deleted, and one changes
// their mind. The other is still hidden everywhere, so the follow between
// them waits with the one still leaving: back if they keep their account
// too, gone with the account if they do not.
func TestAccountLeaving_AFollowWithSomebodyStillLeavingWaitsForThem(t *testing.T) {
	defer withDB(t)()
	resetRedis(t)
	defer db.Exec(`DELETE FROM leaving_set_aside WHERE user_id IN (9831, 9832, 9833)`)
	defer leaver(t, 9831, "first_back", "sunset42go")()
	defer leaver(t, 9832, "still_going", "sunset42go")()
	defer leaver(t, 9833, "never_left", "sunset42go")()
	mustExec(t, `INSERT INTO follows (follower_id, following_id) VALUES (9831, 9832), (9832, 9831), (9833, 9831)`)

	if w := deleteAccount(t, 9831, "first_back"); w.Code != 200 {
		t.Fatalf("delete answered %d", w.Code)
	}
	if w := deleteAccount(t, 9832, "still_going"); w.Code != 200 {
		t.Fatalf("delete answered %d", w.Code)
	}
	if code, got := signInAs(t, "first_back", "sunset42go", true); code != 200 {
		t.Fatalf("keeping the account answered %d %v", code, got)
	}

	u, found := GetUserByID("9831")
	if !found {
		t.Fatal("the kept account is not there")
	}
	// The follow from somebody who never left is back — so the two checks
	// after it are not passing on lists that are simply empty.
	if u.Followers != 1 {
		t.Errorf("%d followers while the other is still leaving, want 1: the one who never left", u.Followers)
	}
	if len(u.FollowingList) != 0 {
		t.Errorf("they follow %v, but the one they followed is still leaving", u.FollowingList)
	}
	for _, f := range followUserPage("9831", "following_id", "follower_id", 50, 0) {
		if f.ID == "9832" {
			t.Error("their follower list shows somebody who is still waiting to be deleted")
		}
	}

	// The other keeps theirs too: the follow both ways is back.
	if code, got := signInAs(t, "still_going", "sunset42go", true); code != 200 {
		t.Fatalf("the second keeping answered %d %v", code, got)
	}
	u, _ = GetUserByID("9831")
	if u.Followers != 2 || len(u.FollowingList) != 1 {
		t.Errorf("once both kept their accounts: %d followers and following %v, want 2 and [9832]",
			u.Followers, u.FollowingList)
	}
}

// Setting rows aside takes them out of their tables. If anything pointed at
// one of those tables, that would either fail — and nobody could delete
// their account — or take what points at those rows with them: somebody
// else's reply or like, gone for good. Nothing points at them today. This
// keeps it that way.
func TestAccountLeaving_NothingPointsAtWhatIsSetAside(t *testing.T) {
	defer withDB(t)()
	pointingAt := func(table string) []string {
		t.Helper()
		var exists bool
		if err := db.QueryRow(`SELECT to_regclass($1) IS NOT NULL`, table).Scan(&exists); err != nil {
			t.Fatal(err)
		}
		if !exists {
			t.Fatalf("there is no table %q: a name in traceTables is wrong", table)
		}
		rows, err := db.Query(`
			SELECT conrelid::regclass::text || ' (' || conname || ')'
			  FROM pg_constraint
			 WHERE contype = 'f' AND confrelid = to_regclass($1)`, table)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		var from []string
		for rows.Next() {
			var f string
			if err := rows.Scan(&f); err != nil {
				t.Fatal(err)
			}
			from = append(from, f)
		}
		return from
	}

	// It has to be able to see a table that IS pointed at, or it passes on
	// anything.
	if len(pointingAt("users")) == 0 {
		t.Fatal("found nothing pointing at users, which half the schema does: this check is blind")
	}
	for _, tt := range traceTables {
		for _, from := range pointingAt(tt.table) {
			t.Errorf("%s points at %s. Setting a leaving account's rows in %s aside would now "+
				"fail, or take the rows pointing at them with them — other people's, gone for good. "+
				"Work out what should happen to those first.", from, tt.table, tt.table)
		}
	}
}
