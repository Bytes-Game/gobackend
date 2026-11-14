package main

// Reporting and moderation, on a real database, through the real
// handlers: a report is kept with whose it is, the admin's decisions do
// what they say, and a suspended account is out everywhere.

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"
)

const (
	modReporter  = 9961
	modCreator   = 9962
	modReporter2 = 9963
)

var modNames = map[int]string{modReporter: "mod_reporter", modCreator: "mod_creator", modReporter2: "mod_reporter2"}

// moderationFixture makes three accounts, a video and an answer and a
// comment by modCreator, and returns their ids and a clean-up.
func moderationFixture(t *testing.T) (video, answer, comment int, done func()) {
	t.Helper()
	clean := func() {
		for id := range modNames {
			db.Exec(`DELETE FROM reports WHERE reporter_id = $1 OR owner_id = $1`, id)
			db.Exec(`DELETE FROM challenges WHERE creator_id = $1`, id)
			db.Exec(`DELETE FROM users WHERE id = $1`, id)
			forgetCutoff(strconv.Itoa(id))
		}
		forgetLeaving()
	}
	clean()
	for id, name := range modNames {
		mustExec(t, `INSERT INTO users (id, username, password, password_hash) VALUES ($1, $2, '', $3)`,
			id, name, fastHash(t, "password of "+name))
	}
	video = mustID(t, `INSERT INTO challenges (creator_id, video_url, thumbnail_url, prefix, subject)
	                   VALUES ($1, 'https://media.example/v.mp4', 'https://media.example/t.jpg', 'Show me', 'your best trick')
	                   RETURNING id`, modCreator)
	other := mustID(t, `INSERT INTO challenges (creator_id, prefix, subject) VALUES ($1, 'Sing', 'a song') RETURNING id`, modReporter2)
	answer = mustID(t, `INSERT INTO challenge_responses (challenge_id, responder_id, video_url) VALUES ($1, $2, 'https://media.example/a.mp4') RETURNING id`, other, modCreator)
	comment = mustID(t, `INSERT INTO challenge_comments (challenge_id, author_id, text) VALUES ($1, $2, 'you are all idiots') RETURNING id`, other, modCreator)
	resetActionLimiters(t)
	return video, answer, comment, clean
}

// sendReport sends a report as [from] through the real route's handler.
func sendReport(t *testing.T, from int, targetType string, targetID int, reason, note string) (int, map[string]any) {
	t.Helper()
	// Three reports in a row is all one person may send (action_limits.go);
	// these tests send more, and are not about that limit.
	resetActionLimiters(t)
	body, _ := json.Marshal(map[string]any{
		"targetType": targetType, "targetId": strconv.Itoa(targetID), "reason": reason, "description": note,
		"reporterId": strconv.Itoa(modCreator), // ignored: the reporter is whoever is signed in
	})
	req := httptest.NewRequest("POST", "/api/v1/report", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+testToken(t, strconv.Itoa(from), modNames[from]))
	rec := httptest.NewRecorder()
	authed(HandleReportEvent)(rec, req)
	var got map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &got)
	return rec.Code, got
}

func decideOn(t *testing.T, targetType string, targetID int, action, confirm string) (int, string) {
	t.Helper()
	body, _ := json.Marshal(map[string]any{"targetType": targetType, "targetId": targetID, "action": action, "confirm": confirm})
	rec := httptest.NewRecorder()
	AdminResolveReportHandler(rec, httptest.NewRequest("POST", "/api/v1/admin/reports/resolve", bytes.NewReader(body)))
	return rec.Code, rec.Body.String()
}

func openGroup(t *testing.T, targetType string, targetID int) *openReport {
	t.Helper()
	open, err := listOpenReports()
	if err != nil {
		t.Fatalf("the review list: %v", err)
	}
	for i := range open {
		if open[i].TargetType == targetType && open[i].TargetID == targetID {
			return &open[i]
		}
	}
	return nil
}

func TestReports_KeptWithWhoseItIs(t *testing.T) {
	defer withDB(t)()
	resetRedis(t)
	video, answer, comment, done := moderationFixture(t)
	defer done()

	for _, c := range []struct {
		kind string
		id   int
	}{{"challenge", video}, {"response", answer}, {"comment", comment}, {"user", modCreator}} {
		code, got := sendReport(t, modReporter, c.kind, c.id, "harassment", "")
		if code != http.StatusCreated {
			t.Fatalf("reporting a %s: status %d %v", c.kind, code, got)
		}
		var owner, reporter int
		if err := db.QueryRow(`SELECT owner_id, reporter_id FROM reports WHERE target_type = $1 AND target_id = $2`,
			c.kind, c.id).Scan(&owner, &reporter); err != nil {
			t.Fatalf("the %s report was not kept: %v", c.kind, err)
		}
		if owner != modCreator {
			t.Errorf("a %s report is kept as %d's; it is %d's", c.kind, owner, modCreator)
		}
		if reporter != modReporter {
			t.Errorf("a %s report names %d as the reporter; it was %d (the body's own claim must be ignored)", c.kind, reporter, modReporter)
		}
	}

	// A serious report hides the creator from the reporter — the creator,
	// not whichever account has the video's number.
	time.Sleep(50 * time.Millisecond) // MarkBlocked runs beside the answer
	if blocked, _ := rdb.SIsMember(rctx, "blocked_creators:"+strconv.Itoa(modReporter), strconv.Itoa(modCreator)).Result(); !blocked {
		t.Error("a harassment report did not hide the creator from the reporter")
	}
	if wrong, _ := rdb.SIsMember(rctx, "blocked_creators:"+strconv.Itoa(modReporter), strconv.Itoa(video)).Result(); wrong && video != modCreator {
		t.Error("the video's own number was blocked as if it were an account")
	}

	// Twice is once.
	code, got := sendReport(t, modReporter, "challenge", video, "spam", "")
	if code != http.StatusOK || got["alreadyReported"] != true {
		t.Errorf("a second report on the same video: status %d %v; want 200 alreadyReported", code, got)
	}
	var n int
	db.QueryRow(`SELECT COUNT(*) FROM reports WHERE reporter_id = $1 AND target_type = 'challenge' AND target_id = $2`, modReporter, video).Scan(&n)
	if n != 1 {
		t.Errorf("%d reports kept from one person on one video, want 1", n)
	}

	// Refused: your own, nothing there, an unknown kind.
	if code, _ := sendReport(t, modCreator, "challenge", video, "spam", ""); code != http.StatusBadRequest {
		t.Errorf("reporting your own video: status %d, want 400", code)
	}
	if code, _ := sendReport(t, modReporter, "challenge", 987654321, "spam", ""); code != http.StatusNotFound {
		t.Errorf("reporting a video that is not there: status %d, want 404", code)
	}
	if code, _ := sendReport(t, modReporter, "post", video, "spam", ""); code != http.StatusBadRequest {
		t.Errorf("reporting an unknown kind of thing: status %d, want 400", code)
	}

	// A reason the server does not know is kept as "other"; a long note is cut.
	long := strings.Repeat("x", reportNoteMax+50)
	if code, _ := sendReport(t, modReporter2, "comment", comment, "some new reason", long); code != http.StatusCreated {
		t.Fatalf("a report with a new reason: status %d", code)
	}
	var reason, note string
	db.QueryRow(`SELECT reason, description FROM reports WHERE reporter_id = $1 AND target_type = 'comment'`, modReporter2).Scan(&reason, &note)
	if reason != "other" || len([]rune(note)) != reportNoteMax {
		t.Errorf("kept reason %q and a note of %d characters; want other and %d", reason, len([]rune(note)), reportNoteMax)
	}
}

func TestReports_ReviewListAndDecisions(t *testing.T) {
	defer withDB(t)()
	resetRedis(t)
	video, answer, comment, done := moderationFixture(t)
	defer done()

	sendReport(t, modReporter, "challenge", video, "sexual_content", "nudity at 0:05")
	sendReport(t, modReporter2, "challenge", video, "spam", "")
	sendReport(t, modReporter, "comment", comment, "harassment", "")
	sendReport(t, modReporter, "response", answer, "violence", "")

	g := openGroup(t, "challenge", video)
	if g == nil {
		t.Fatal("the reported video is not on the review list")
	}
	if g.Reports != 2 || g.Owner != "mod_creator" || g.Text != "Show me your best trick" ||
		g.VideoURL != "https://media.example/v.mp4" || g.Gone {
		t.Errorf("the video's line: %+v", *g)
	}
	if strings.Join(g.Reasons, ",") != "sexual_content,spam" || len(g.Notes) != 1 || g.Notes[0] != "nudity at 0:05" {
		t.Errorf("the video's reasons %v and notes %v", g.Reasons, g.Notes)
	}
	if c := openGroup(t, "comment", comment); c == nil || c.Text != "you are all idiots" {
		t.Errorf("the comment's line shows %+v; want its words", c)
	}

	// Nothing wrong: closed, nothing deleted.
	if code, body := decideOn(t, "comment", comment, "dismiss", ""); code != http.StatusOK {
		t.Fatalf("dismiss: %d %s", code, body)
	}
	if openGroup(t, "comment", comment) != nil {
		t.Error("a dismissed comment is still waiting")
	}
	var still int
	db.QueryRow(`SELECT COUNT(*) FROM challenge_comments WHERE id = $1`, comment).Scan(&still)
	if still != 1 {
		t.Error("dismissing deleted the comment")
	}

	// Removing needs saying yes.
	if code, _ := decideOn(t, "challenge", video, "remove", ""); code != http.StatusBadRequest {
		t.Errorf("remove without confirm: status %d, want 400", code)
	}
	db.QueryRow(`SELECT COUNT(*) FROM challenges WHERE id = $1`, video).Scan(&still)
	if still != 1 {
		t.Fatal("a remove without confirm deleted the video")
	}

	// Remove: gone for good, and both reports closed as removed.
	if code, body := decideOn(t, "challenge", video, "remove", "yes"); code != http.StatusOK {
		t.Fatalf("remove: %d %s", code, body)
	}
	db.QueryRow(`SELECT COUNT(*) FROM challenges WHERE id = $1`, video).Scan(&still)
	if still != 0 {
		t.Error("the removed video is still there")
	}
	var removed int
	db.QueryRow(`SELECT COUNT(*) FROM reports WHERE target_type = 'challenge' AND target_id = $1
	              AND status = 'resolved' AND resolution = 'removed' AND resolved_at IS NOT NULL`, video).Scan(&removed)
	if removed != 2 {
		t.Errorf("%d of the video's 2 reports were closed as removed", removed)
	}

	// Remove and suspend: the answer goes, and so does its creator.
	if code, body := decideOn(t, "response", answer, "remove_suspend", "yes"); code != http.StatusOK {
		t.Fatalf("remove + suspend: %d %s", code, body)
	}
	db.QueryRow(`SELECT COUNT(*) FROM challenge_responses WHERE id = $1`, answer).Scan(&still)
	if still != 0 {
		t.Error("the removed answer is still there")
	}
	if !accountSuspended(strconv.Itoa(modCreator)) {
		t.Error("remove + suspend did not suspend the creator")
	}

	// An account is suspended, never "removed".
	sendReport(t, modReporter2, "user", modReporter, "spam", "")
	if code, _ := decideOn(t, "user", modReporter, "remove", "yes"); code != http.StatusBadRequest {
		t.Errorf("removing an account: status %d, want 400", code)
	}
	if code, _ := decideOn(t, "challenge", video, "delete everything", "yes"); code != http.StatusBadRequest {
		t.Errorf("an unknown decision: status %d, want 400", code)
	}
}

func TestSuspension_StopsSignInAndHidesTheAccount(t *testing.T) {
	defer withDB(t)()
	resetRedis(t)
	_, answer, _, done := moderationFixture(t)
	defer done()
	pass := "password of mod_creator"
	var battle int
	if err := db.QueryRow(`SELECT challenge_id FROM challenge_responses WHERE id = $1`, answer).Scan(&battle); err != nil {
		t.Fatal(err)
	}
	answersShown := func() int { return len(GetChallengeResponses(strconv.Itoa(battle))) }
	if answersShown() != 1 {
		t.Fatalf("before: the battle shows %d answers, want the creator's 1", answersShown())
	}

	if code, got := signInAs(t, "mod_creator", pass, false); code != http.StatusOK {
		t.Fatalf("before: sign-in %d %v", code, got)
	}
	before := signedInAt(t, strconv.Itoa(modCreator), "mod_creator", time.Now().Add(-time.Minute))
	if leavingIDs()[modCreator] {
		t.Fatal("hidden before being suspended")
	}

	sendReport(t, modReporter, "user", modCreator, "impersonation", "pretends to be me")
	if code, body := decideOn(t, "user", modCreator, "suspend", "yes"); code != http.StatusOK {
		t.Fatalf("suspend: %d %s", code, body)
	}

	code, got := signInAs(t, "mod_creator", pass, false)
	if code != http.StatusForbidden || got["suspended"] != true || !strings.Contains(got["error"].(string), "suspended") {
		t.Errorf("a suspended account signing in: %d %v; want 403 saying it is suspended", code, got)
	}
	if _, err := parseToken(before); err == nil {
		t.Error("a phone signed in before the suspension is still signed in")
	}
	if !leavingIDs()[modCreator] {
		t.Error("a suspended account is not hidden from lists")
	}
	for _, u := range GetAllUsers() {
		if u.Username == "mod_creator" {
			t.Error("a suspended account is still in the list of everybody")
		}
	}
	if answersShown() != 0 {
		t.Error("a suspended account's answer video is still shown in its battle")
	}

	// Undo.
	body, _ := json.Marshal(map[string]any{"userId": modCreator})
	rec := httptest.NewRecorder()
	AdminUnsuspendHandler(rec, httptest.NewRequest("POST", "/api/v1/admin/unsuspend", bytes.NewReader(body)))
	if rec.Code != http.StatusOK {
		t.Fatalf("unsuspend: %d %s", rec.Code, rec.Body.String())
	}
	if code, got := signInAs(t, "mod_creator", pass, false); code != http.StatusOK {
		t.Errorf("after unsuspending, sign-in: %d %v", code, got)
	}
	if leavingIDs()[modCreator] || answersShown() != 1 {
		t.Error("an unsuspended account, or its answer, is still hidden")
	}
	if _, err := parseToken(before); err == nil {
		t.Error("a sign-in taken away by the suspension came back with the unsuspension")
	}
}

// The wires: the routes the review page calls, behind the admin sign-in,
// and the report route behind a sign-in.
func TestModeration_IsWiredIn(t *testing.T) {
	src, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	code := regexp.MustCompile(`(?m)^\s*//.*$`).ReplaceAllString(string(src), "")
	for _, want := range []string{
		`"/report", authed(HandleReportEvent)`,
		`"/admin/reports", adminOnly(AdminOpenReportsHandler)`,
		`"/admin/reports/resolve", adminOnly(AdminResolveReportHandler)`,
		`"/admin/unsuspend", adminOnly(AdminUnsuspendHandler)`,
		`r.HandleFunc("/admin/reports", adminOnly(AdminReportsPageHandler))`,
	} {
		if !strings.Contains(code, want) {
			t.Errorf("main.go does not wire %s", want)
		}
	}
}
