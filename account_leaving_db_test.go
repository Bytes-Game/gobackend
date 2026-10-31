package main

// Deleting an account waits 30 days, the TikTok way (account_leaving.go).
//
// These go through the real handlers and the real job against a real
// database: asking to delete, being signed out everywhere, signing in again
// and being asked, keeping the account, battles ending with no result, and
// the job that deletes the account for good once the 30 days are up.

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// leaver makes a person who can sign in with [password]. Defer what it
// returns: it removes them again, and has to run before withDB closes the
// database (t.Cleanup would run after, against no database at all).
func leaver(t *testing.T, id int, name, password string) func() {
	t.Helper()
	hash, err := hashPassword(password)
	if err != nil {
		t.Fatal(err)
	}
	clean := func() {
		db.Exec(`DELETE FROM device_tokens WHERE user_id = $1`, strconv.Itoa(id))
		db.Exec(`DELETE FROM challenges WHERE creator_id = $1`, id)
		db.Exec(`DELETE FROM users WHERE id = $1`, id)
	}
	clean()
	mustExec(t, `INSERT INTO users (id, username, password, password_hash) VALUES ($1, $2, '', $3)`,
		id, name, hash)
	forgetLeaving()
	forgetCutoff(strconv.Itoa(id))
	return func() {
		clean()
		forgetLeaving()
	}
}

// signedInAt is a sign-in [userID] made at [at], as issueToken would have
// made it then. Sign-in times are kept to the second, so a sign-in from the
// same second as a sign-out is not "before" it; a real old sign-in is.
func signedInAt(t *testing.T, userID, username string, at time.Time) string {
	t.Helper()
	secret, err := authSecret()
	if err != nil {
		t.Fatal(err)
	}
	claims := authClaims{
		Username: username,
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   userID,
			IssuedAt:  jwt.NewNumericDate(at),
			ExpiresAt: jwt.NewNumericDate(at.Add(tokenTTL)),
		},
	}
	tok, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(secret)
	if err != nil {
		t.Fatal(err)
	}
	return tok
}

// signInAs signs in through the real handler.
func signInAs(t *testing.T, username, password string, keep bool) (int, map[string]any) {
	t.Helper()
	resetActionLimiters(t)
	body, _ := json.Marshal(map[string]any{"username": username, "password": password, "keepAccount": keep})
	req := httptest.NewRequest("POST", "/login", bytes.NewReader(body))
	req.RemoteAddr = "203.0.113.77:4000"
	rec := httptest.NewRecorder()
	LoginHandler(rec, req)
	var got map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &got)
	return rec.Code, got
}

// waiting is whether [id] is waiting to be deleted, read from the table.
func waiting(t *testing.T, id int) bool {
	t.Helper()
	var w bool
	if err := db.QueryRow(`SELECT deletion_requested_at IS NOT NULL FROM users WHERE id = $1`, id).Scan(&w); err != nil {
		t.Fatal(err)
	}
	return w
}

func TestAccountLeaving_DeletingSignsOutEverywhereAndSaysWhen(t *testing.T) {
	defer withDB(t)()
	resetRedis(t)
	defer leaver(t, 9801, "leaving_soon", "sunset42go")()
	before := signedInAt(t, "9801", "leaving_soon", time.Now().Add(-time.Hour))
	mustExec(t, `INSERT INTO device_tokens (token, user_id, platform) VALUES ('tok-9801', '9801', 'fcm')`)
	if _, err := parseToken(before); err != nil {
		t.Fatalf("the sign-in does not work even before deleting: %v", err)
	}

	w := deleteAccount(t, 9801, "leaving_soon")
	if w.Code != 200 {
		t.Fatalf("delete answered %d: %s", w.Code, w.Body.String())
	}
	var got map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &got)
	on, err := time.Parse(time.RFC3339, got["deleteOn"].(string))
	if err != nil || on.Before(time.Now().Add(29*24*time.Hour)) || on.After(time.Now().Add(31*24*time.Hour)) {
		t.Errorf("deleteOn = %v (%v), want about 30 days from now", got["deleteOn"], err)
	}
	if got["deleted"] != true {
		t.Error(`"deleted" is gone from the answer: apps from before the wait would not sign out`)
	}
	if !waiting(t, 9801) {
		t.Fatal("the account is not marked as waiting to be deleted")
	}
	if _, found := GetUserByID("9801"); !found {
		t.Fatal("the account was deleted at once: there is no 30 days to change their mind")
	}
	if _, err := parseToken(before); err == nil {
		t.Error("a phone signed in before the delete is still signed in")
	}
	var phones int
	_ = db.QueryRow(`SELECT COUNT(*) FROM device_tokens WHERE user_id = '9801'`).Scan(&phones)
	if phones != 0 {
		t.Error("their phones still get notifications")
	}
	// Asking again keeps the first date.
	first := got["deleteOn"]
	w = deleteAccount(t, 9801, "leaving_soon")
	_ = json.Unmarshal(w.Body.Bytes(), &got)
	if got["deleteOn"] != first {
		t.Errorf("asking twice moved the date from %v to %v", first, got["deleteOn"])
	}
}

func TestAccountLeaving_SigningInAsksAndKeepingBringsItBack(t *testing.T) {
	defer withDB(t)()
	resetRedis(t)
	defer leaver(t, 9802, "changed_mind", "sunset42go")()
	before := signedInAt(t, "9802", "changed_mind", time.Now().Add(-time.Hour))
	if w := deleteAccount(t, 9802, "changed_mind"); w.Code != 200 {
		t.Fatalf("delete answered %d", w.Code)
	}

	// Signing in: told when it goes, and not signed in.
	code, got := signInAs(t, "changed_mind", "sunset42go", false)
	if code != 409 || got["deletionPending"] != true || got["deleteOn"] == nil {
		t.Fatalf("signing in while waiting answered %d %v, want 409 with the date", code, got)
	}
	if got["token"] != nil {
		t.Error("signing in while waiting handed out a sign-in without being asked to keep it")
	}
	if msg, _ := got["error"].(string); !strings.Contains(msg, "deleted on") {
		t.Errorf("the words for an older app say %q", msg)
	}
	if !waiting(t, 9802) {
		t.Fatal("just signing in kept the account: keeping it has to be chosen")
	}

	// Choosing to keep it needs the right password like any sign-in.
	if code, _ := signInAs(t, "changed_mind", "wrong-password", true); code != 401 {
		t.Errorf("keeping it with the wrong password answered %d", code)
	}
	if !waiting(t, 9802) {
		t.Fatal("a wrong password kept the account")
	}

	// Choosing to keep it: signed in, and the account is back.
	code, got = signInAs(t, "changed_mind", "sunset42go", true)
	if code != 200 || got["token"] == nil {
		t.Fatalf("keeping the account answered %d %v", code, got)
	}
	if waiting(t, 9802) {
		t.Fatal("the account is still waiting to be deleted after keeping it")
	}
	if _, err := parseToken(got["token"].(string)); err != nil {
		t.Errorf("the sign-in from keeping the account does not work: %v", err)
	}
	if _, err := parseToken(before); err == nil {
		t.Error("a phone signed in before the delete came back to life when the account was kept")
	}
	if isLeaving("9802") {
		t.Error("the kept account is still on this server's list of leaving accounts")
	}
}

func TestAccountLeaving_DeletedForGoodOnlyAfter30Days(t *testing.T) {
	defer withDB(t)()
	resetRedis(t)
	ctx := context.Background()
	defer leaver(t, 9803, "gone_for_good", "sunset42go")()
	defer leaver(t, 9804, "staying_here", "sunset42go")()
	mustExec(t, `INSERT INTO follows (follower_id, following_id) VALUES (9803, 9804)`)
	defer db.Exec(`DELETE FROM leaving_set_aside WHERE user_id = 9803`)
	if w := deleteAccount(t, 9803, "gone_for_good"); w.Code != 200 {
		t.Fatalf("delete answered %d", w.Code)
	}
	setAside := func() int {
		var n int
		_ = db.QueryRow(`SELECT COUNT(*) FROM leaving_set_aside WHERE user_id = 9803`).Scan(&n)
		return n
	}
	if setAside() != 1 {
		t.Fatalf("their follow was not set aside while they wait (%d rows)", setAside())
	}

	// Before the 30 days are up: nothing.
	if n, err := purgeDueAccounts(ctx, time.Now().Add(29*24*time.Hour)); err != nil || n != 0 {
		t.Fatalf("29 days on, the job deleted %d (err %v)", n, err)
	}
	if _, found := GetUserByID("9803"); !found {
		t.Fatal("deleted before its 30 days were up")
	}

	// After: gone, and its name is free for anybody.
	if n, err := purgeDueAccounts(ctx, time.Now().Add(deletionWait+time.Minute)); err != nil || n != 1 {
		t.Fatalf("30 days on, the job deleted %d (err %v), want 1", n, err)
	}
	if _, found := GetUserByID("9803"); found {
		t.Fatal("still there 30 days on")
	}
	if UserExists("gone_for_good") {
		t.Error("the name of a deleted account is still taken")
	}
	if n := setAside(); n != 0 {
		t.Errorf("%d of what they did is still kept after they were deleted for good", n)
	}
	if _, found := GetUserByID("9804"); !found {
		t.Fatal("the job deleted an account that never asked")
	}
	// And the deleting step itself refuses an account that did not ask.
	if err := purgeAccount("9804"); err == nil {
		t.Error("purgeAccount deleted an account that is not waiting to be deleted")
	}
	if _, found := GetUserByID("9804"); !found {
		t.Fatal("purgeAccount deleted an account that never asked")
	}
}

func TestAccountLeaving_TheJobRunsWhatTheTickerRuns(t *testing.T) {
	defer withDB(t)()
	resetRedis(t)
	defer leaver(t, 9805, "old_request", "sunset42go")()
	if w := deleteAccount(t, 9805, "old_request"); w.Code != 200 {
		t.Fatalf("delete answered %d", w.Code)
	}
	mustExec(t, `UPDATE users SET deletion_requested_at = NOW() - INTERVAL '31 days' WHERE id = 9805`)
	accountPurgeRound(context.Background())
	if _, found := GetUserByID("9805"); found {
		t.Fatal("one round of the job left an account whose 30 days were up")
	}

	// The loop itself waits an hour between rounds, so check it runs the
	// round this test ran, and that the server starts it.
	src := codeOf(t, "account_leaving.go")
	start := strings.Index(src, "func startAccountPurger() {")
	if start < 0 {
		t.Fatal("startAccountPurger is gone")
	}
	body := src[start:]
	if end := strings.Index(body, "\n}\n"); end > 0 {
		body = body[:end]
	}
	if !strings.Contains(body, "accountPurgeRound(") {
		t.Error("the job's loop no longer runs accountPurgeRound")
	}
	if !strings.Contains(codeOf(t, "main.go"), "startAccountPurger()") {
		t.Error("the server never starts the job, so no account is ever deleted for good")
	}
}

func TestAccountLeaving_BattlesInProgressEndWithNoResult(t *testing.T) {
	defer withDB(t)()
	resetRedis(t)
	defer leaver(t, 9806, "battle_leaver", "sunset42go")()
	defer leaver(t, 9807, "battle_opponent", "sunset42go")()
	defer leaver(t, 9808, "other_player", "sunset42go")()
	mustExec(t, `UPDATE users SET rating = 1500, wins = 3, losses = 2 WHERE id IN (9806, 9807, 9808)`)

	battle := func(creator, responder int) int {
		cid := mustID(t, `INSERT INTO challenges (creator_id, video_url, prefix, subject, status,
		                                          accepted_at, voting_ends_at, battle_days)
		                  VALUES ($1, 'https://v/x.mp4', 'who can', 'win', 'active',
		                          NOW() - INTERVAL '1 hour', NOW() + INTERVAL '2 days', 3)
		                  RETURNING id`, creator)
		mustExec(t, `INSERT INTO challenge_responses (challenge_id, responder_id, video_url)
		             VALUES ($1, $2, 'https://v/y.mp4')`, cid, responder)
		return cid
	}
	withLeaver := battle(9807, 9806) // the leaver answered the opponent
	without := battle(9807, 9808)    // nothing to do with them

	if w := deleteAccount(t, 9806, "battle_leaver"); w.Code != 200 {
		t.Fatalf("delete answered %d", w.Code)
	}

	var status string
	var resolved bool
	if err := db.QueryRow(`SELECT status, resolved_at IS NOT NULL FROM challenges WHERE id = $1`,
		withLeaver).Scan(&status, &resolved); err != nil {
		t.Fatal(err)
	}
	if !resolved || status != "completed" {
		t.Errorf("the battle with the leaver is %q (resolved %v): the opponent waits on a player who is gone",
			status, resolved)
	}
	rows, err := db.Query(`SELECT user_id, outcome, rating_after - rating_before
	                         FROM battle_results WHERE challenge_id = $1 ORDER BY user_id`, withLeaver)
	if err != nil {
		t.Fatal(err)
	}
	results := map[int]string{}
	for rows.Next() {
		var uid, moved int
		var outcome string
		if err := rows.Scan(&uid, &outcome, &moved); err != nil {
			t.Fatal(err)
		}
		results[uid] = outcome
		if moved != 0 {
			t.Errorf("user %d's points moved by %d in a cancelled battle", uid, moved)
		}
	}
	rows.Close()
	if results[9806] != "cancelled" || results[9807] != "cancelled" {
		t.Errorf("results %v, want both players cancelled", results)
	}
	var wins, losses, rating int
	_ = db.QueryRow(`SELECT wins, losses, rating FROM users WHERE id = 9807`).Scan(&wins, &losses, &rating)
	if wins != 3 || losses != 2 || rating != 1500 {
		t.Errorf("the opponent's record changed to %d-%d at %d", wins, losses, rating)
	}

	if err := db.QueryRow(`SELECT resolved_at IS NOT NULL FROM challenges WHERE id = $1`,
		without).Scan(&resolved); err != nil {
		t.Fatal(err)
	}
	if resolved {
		t.Error("a battle the leaver was not in was ended too")
	}
}
