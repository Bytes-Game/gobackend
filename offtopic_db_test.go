package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/mux"
)

// Videos that don't match their challenge, against a real database: the
// model's answer arriving the way the worker sends it, reports arriving the
// way the app sends them, and what each costs — and to whom. Nothing is ever
// taken down; every test checks the video is still up.

// modelSays delivers the worker's reading of one video, with the model's
// answer to "does it match its challenge", through the endpoint the worker
// calls. kind is "challenge" or "response".
func modelSays(t *testing.T, kind, id, match string) {
	t.Helper()
	body, err := json.Marshal(hlsCompleteRequest{
		ChallengeID: id,
		ManifestURL: "https://v/" + kind + id + "/master.m3u8",
		Kind:        kind,
		Analysis:    json.RawMessage(`{"passes":["understand"],"questionMatch":"` + match + `"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	HLSCompleteHandler(w, httptest.NewRequest("POST",
		"/api/v1/internal/hls/complete", bytes.NewReader(body)))
	if w.Code != http.StatusNoContent {
		t.Fatalf("worker's report on %s %s got %d: %s", kind, id, w.Code, w.Body.String())
	}
}

// report is somebody pressing "doesn't match the challenge" in the app.
// rid "" reports the challenge's own video.
func report(t *testing.T, uid, cid, rid string) (int, map[string]any) {
	t.Helper()
	b, _ := json.Marshal(map[string]string{"responseId": rid})
	r := httptest.NewRequest("POST", "/api/v1/challenges/"+cid+"/report", bytes.NewReader(b))
	r = mux.SetURLVars(r, map[string]string{"id": cid})
	w := httptest.NewRecorder()
	ReportOffTopicHandler(w, withUser(r, uid, "reporter"+uid))
	out := map[string]any{}
	if w.Code == http.StatusOK {
		if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
			t.Fatal(err)
		}
	} else {
		out["error"] = strings.TrimSpace(w.Body.String())
	}
	return w.Code, out
}

type playerRecord struct{ Rating, Wins, Losses, Strikes int }

func recordOf(t *testing.T, uid int) playerRecord {
	t.Helper()
	var p playerRecord
	if err := db.QueryRow(`SELECT rating, wins, losses, integrity_strikes
		FROM users WHERE id = $1`, uid).Scan(&p.Rating, &p.Wins, &p.Losses, &p.Strikes); err != nil {
		t.Fatal(err)
	}
	return p
}

// charged is a record after [points] were taken off it, never below the floor.
func charged(p playerRecord, points, strikes int) playerRecord {
	p.Rating = max(p.Rating-points, ratingFloor)
	p.Strikes += strikes
	return p
}

// notesOf is what user uid has in their notification list about battle cid.
func notesOf(t *testing.T, uid int, cid, kind string) []string {
	t.Helper()
	rows, err := db.Query(`SELECT body FROM user_notifications
		WHERE user_id = $1 AND challenge_id = $2 AND kind = $3 ORDER BY id`, uid, cid, kind)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var b string
		if err := rows.Scan(&b); err != nil {
			t.Fatal(err)
		}
		out = append(out, b)
	}
	return out
}

// penaltyPushes counts the "your video was reported" pushes queued for uid.
func penaltyPushes(t *testing.T, uid int, cid, rid string, level int) int {
	t.Helper()
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM notification_outbox
		WHERE user_id = $1 AND dedupe_key = $2`, strconv.Itoa(uid),
		"off_topic_penalty:"+cid+":"+rid+":"+strconv.Itoa(uid)+":"+strconv.Itoa(level)).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// stillUp checks nothing was taken down: the answer is not hidden, the
// challenge keeps its status, and the battle has both its sides.
func stillUp(t *testing.T, cid, rid, wantStatus string) {
	t.Helper()
	var status string
	if err := db.QueryRow(`SELECT status FROM challenges WHERE id = $1`, cid).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != wantStatus {
		t.Errorf("the challenge is %q, want %q — nothing should change it", status, wantStatus)
	}
	if rid == "" {
		return
	}
	var hidden bool
	if err := db.QueryRow(`SELECT COALESCE(is_hidden, FALSE) FROM challenge_responses
		WHERE id = $1`, rid).Scan(&hidden); err != nil {
		t.Fatal(err)
	}
	if hidden {
		t.Error("the answer was hidden — videos stay up")
	}
	if ps := standingsJSON(t, cid)["participants"].([]any); len(ps) != 2 {
		t.Errorf("the score has %d sides, want both", len(ps))
	}
}

// watchAnswer is a viewer having the answer on screen for four seconds.
func watchAnswer(t *testing.T, uid int, cid, rid string) {
	t.Helper()
	watch(t, strconv.Itoa(uid), cid, map[string]any{"opponentMs": 4000, "responseId": rid})
}

// ── the owner of a video that doesn't match ────────────────────────────────

func TestOffTopic_TheModelAndAReportCostTheHigherPenalty(t *testing.T) {
	defer withDB(t)()
	cid, rid := liveBattle(t)
	before := recordOf(t, 2)

	// The model says the answer doesn't match. On its own that charges
	// nothing — it only warns the owner.
	modelSays(t, "response", rid, "no")
	if got := recordOf(t, 2); got != before {
		t.Fatalf("the model alone charged the owner: %+v → %+v", before, got)
	}
	if w := notesOf(t, 2, cid, noteOffTopicWarning); len(w) != 1 ||
		!strings.Contains(w[0], "may not match") || !strings.Contains(w[0], "stays up either way") {
		t.Errorf("the answer's owner was warned %v", w)
	}
	// The same answer again is not a second warning.
	modelSays(t, "response", rid, "no")
	if w := notesOf(t, 2, cid, noteOffTopicWarning); len(w) != 1 {
		t.Errorf("the owner has %d warnings after the same answer twice", len(w))
	}

	// The challenger reports it: the two agree. The higher charge, one
	// strike, and the video stays up with the battle going on.
	if code, out := report(t, "1", cid, rid); code != http.StatusOK || out["message"] != "Thanks for reporting." {
		t.Fatalf("the challenger's report: %d %v", code, out)
	}
	after := recordOf(t, 2)
	if want := charged(before, penaltyConfirmed, 1); after != want {
		t.Errorf("owner %+v → %+v, want %+v (the higher charge, one strike, no loss)", before, after, want)
	}
	stillUp(t, cid, rid, "active")
	if n := notesOf(t, 2, cid, noteOffTopicPenalty); len(n) != 1 ||
		!strings.Contains(n[0], "Our check and a report both say") ||
		!strings.Contains(n[0], "Your video stays up.") {
		t.Errorf("the owner was told %v", n)
	}
	if n := penaltyPushes(t, 2, cid, rid, levelConfirmed); n != 1 {
		t.Errorf("%d pushes queued, want 1", n)
	}

	// More reports charge nobody again.
	voter(t, 7001, 5)
	watchAnswer(t, 7001, cid, rid)
	report(t, "7001", cid, rid)
	if again := recordOf(t, 2); again != after {
		t.Errorf("another report charged again: %+v → %+v", after, again)
	}
}

func TestOffTopic_ReportsAloneCostTheSmallerPenaltyAndTheModelTopsItUp(t *testing.T) {
	defer withDB(t)()
	cid, rid := liveBattle(t)
	before := recordOf(t, 2)

	// Reports that don't count: an old account that never watched it, and a
	// brand-new account made after the video — the cheapest way to pile on.
	voter(t, 7101, 5)
	report(t, "7101", cid, rid)
	seedAccount(t, 7102, time.Now(), 0)
	watchAnswer(t, 7102, cid, rid)
	report(t, "7102", cid, rid)
	// Real viewers: two is not enough...
	for _, v := range []int{7103, 7104} {
		voter(t, v, 5)
		watchAnswer(t, v, cid, rid)
		report(t, strconv.Itoa(v), cid, rid)
	}
	if got := recordOf(t, 2); got != before {
		t.Fatalf("charged before three real viewers reported it: %+v → %+v", before, got)
	}
	// ...three is: the smaller charge.
	voter(t, 7105, 5)
	watchAnswer(t, 7105, cid, rid)
	report(t, "7105", cid, rid)
	reported := recordOf(t, 2)
	if want := charged(before, penaltyReported, 1); reported != want {
		t.Errorf("owner %+v → %+v, want %+v", before, reported, want)
	}
	stillUp(t, cid, rid, "active")
	if n := notesOf(t, 2, cid, noteOffTopicPenalty); len(n) != 1 ||
		!strings.Contains(n[0], "Several people reported") {
		t.Errorf("the owner was told %v", n)
	}

	// A fourth report charges nobody again.
	voter(t, 7106, 5)
	watchAnswer(t, 7106, cid, rid)
	report(t, "7106", cid, rid)
	if again := recordOf(t, 2); again != reported {
		t.Errorf("a fourth report charged again: %+v → %+v", reported, again)
	}

	// The model then agrees: the rest of the higher charge, and no second
	// strike — the whole video costs the higher charge, once.
	modelSays(t, "response", rid, "no")
	topped := recordOf(t, 2)
	if want := charged(reported, penaltyConfirmed-penaltyReported, 0); topped != want {
		t.Errorf("topped up %+v → %+v, want %+v", reported, topped, want)
	}
	stillUp(t, cid, rid, "active")
	if n := notesOf(t, 2, cid, noteOffTopicPenalty); len(n) != 2 {
		t.Errorf("the owner has %d penalty notes, want 2 (one for each charge)", len(n))
	}
}

func TestOffTopic_AChallengeThatDoesntMatchItsOwnWordsStaysUp(t *testing.T) {
	defer withDB(t)()
	voter(t, 7301, 5)
	cid := auditChallenge(t, map[string]any{"creator": 7301, "subject": "juggle five"})
	if _, err := db.Exec(`UPDATE challenges SET visibility = 'arena' WHERE id = $1`, cid); err != nil {
		t.Fatal(err)
	}
	before := recordOf(t, 7301)
	modelSays(t, "challenge", cid, "no")
	voter(t, 7302, 5)
	watch(t, "7302", cid, map[string]any{"creatorMs": 4000})
	report(t, "7302", cid, "")
	if got, want := recordOf(t, 7301), charged(before, penaltyConfirmed, 1); got != want {
		t.Errorf("creator %+v → %+v, want %+v", before, got, want)
	}
	stillUp(t, cid, "", "open")
	// Still on the profile for everybody.
	found := false
	for _, c := range GetChallengesByCreator("7301", false, 50, 0) {
		found = found || c.ID == cid
	}
	if !found {
		t.Error("a visitor no longer sees the challenge — it should stay up")
	}
}

// ── somebody using reports against the other side ─────────────────────────

func TestOffTopic_TheOtherSideAloneChargesNobody(t *testing.T) {
	defer withDB(t)()
	cid, rid := liveBattle(t)
	answerer, challenger := recordOf(t, 2), recordOf(t, 1)
	// The challenger reports the answer; the model has not said anything.
	report(t, "1", cid, rid)
	modelSays(t, "response", rid, "unsure")
	if got := recordOf(t, 2); got != answerer {
		t.Errorf("the other side's report alone charged the answerer: %+v → %+v", answerer, got)
	}
	if got := recordOf(t, 1); got != challenger {
		t.Errorf("an unsure model charged the reporter: %+v → %+v", challenger, got)
	}
}

func TestOffTopic_ReportingAVideoThatMatchesCostsTheReporter(t *testing.T) {
	defer withDB(t)()
	cid, rid := liveBattle(t)
	challenger, answerer := recordOf(t, 1), recordOf(t, 2)

	// The challenger reports the answer, and so does a viewer.
	report(t, "1", cid, rid)
	voter(t, 7401, 5)
	watchAnswer(t, 7401, cid, rid)
	report(t, "7401", cid, rid)
	viewer := recordOf(t, 7401)

	// The model finds it DOES match: the challenger was reporting to hurt
	// the other side, and pays for it. The viewer, who has no stake, doesn't.
	modelSays(t, "response", rid, "yes")
	if got, want := recordOf(t, 1), charged(challenger, falseReportPenalty, 0); got != want {
		t.Errorf("the challenger %+v → %+v, want %+v", challenger, got, want)
	}
	if got := recordOf(t, 7401); got != viewer {
		t.Errorf("a viewer was charged for reporting: %+v → %+v", viewer, got)
	}
	if got := recordOf(t, 2); got != answerer {
		t.Errorf("the answer that matches cost its owner: %+v → %+v", answerer, got)
	}
	if n := notesOf(t, 1, cid, noteFalseReport); len(n) != 1 ||
		!strings.Contains(n[0], "does match the challenge") {
		t.Errorf("the challenger was told %v", n)
	}

	// Charged once per video: reporting again, or the model saying so
	// again, costs nothing more.
	after := recordOf(t, 1)
	report(t, "1", cid, rid)
	modelSays(t, "response", rid, "unsure")
	modelSays(t, "response", rid, "yes")
	if got := recordOf(t, 1); got != after {
		t.Errorf("charged again for the same report: %+v → %+v", after, got)
	}

	// The other way round, with the model's word already in: the answerer
	// reports the challenger's video, which matches, and is told at once.
	modelSays(t, "challenge", cid, "yes")
	before := recordOf(t, 2)
	code, out := report(t, "2", cid, "")
	if code != http.StatusOK || !strings.Contains(out["message"].(string), "cost you 10 rating points") {
		t.Errorf("the answerer's report: %d %v — want to be told what it cost", code, out)
	}
	if got, want := recordOf(t, 2), charged(before, falseReportPenalty, 0); got != want {
		t.Errorf("the answerer %+v → %+v, want %+v", before, got, want)
	}
	stillUp(t, cid, rid, "active")
}

// Two reports arriving at the same moment must still charge once. What
// makes that true is the video being locked and marked in the same step as
// the check — so the charges are sent all at once, here.
func TestOffTopic_ChargesHappenOnceEvenAtTheSameMoment(t *testing.T) {
	defer withDB(t)()
	cid, rid := liveBattle(t)
	ctx := context.Background()
	for _, v := range []struct {
		owner int
		rid   int
	}{{2, mustAtoi(t, rid)}, {1, 0}} {
		before := recordOf(t, v.owner)
		results := make(chan bool, 4)
		for i := 0; i < 4; i++ {
			go func() {
				_, ok, err := chargeOwner(ctx, mustAtoi(t, cid), v.rid, levelReported)
				if err != nil {
					t.Error(err)
				}
				results <- ok
			}()
		}
		n := 0
		for i := 0; i < 4; i++ {
			if <-results {
				n++
			}
		}
		if n != 1 {
			t.Errorf("video %d: charged %d times at once, want 1", v.rid, n)
		}
		if got, want := recordOf(t, v.owner), charged(before, penaltyReported, 1); got != want {
			t.Errorf("video %d owner %+v → %+v, want %+v", v.rid, before, got, want)
		}
	}

	// The same for false reports.
	report(t, "1", cid, rid)
	if _, err := db.Exec(`UPDATE challenge_responses SET question_match = 'yes' WHERE id = $1`, rid); err != nil {
		t.Fatal(err)
	}
	before := recordOf(t, 1)
	results := make(chan int, 4)
	for i := 0; i < 4; i++ {
		go func() {
			got, err := chargeFalseReports(ctx, mustAtoi(t, cid), mustAtoi(t, rid))
			if err != nil {
				t.Error(err)
			}
			results <- len(got)
		}()
	}
	total := 0
	for i := 0; i < 4; i++ {
		total += <-results
	}
	if total != 1 {
		t.Errorf("one false report was charged %d times at once", total)
	}
	if got, want := recordOf(t, 1), charged(before, falseReportPenalty, 0); got != want {
		t.Errorf("reporter %+v → %+v, want %+v", before, got, want)
	}
}

func TestOffTopic_YouCannotReportYourOwnVideo(t *testing.T) {
	defer withDB(t)()
	cid, rid := liveBattle(t)
	if code, out := report(t, "2", cid, rid); code != http.StatusForbidden {
		t.Errorf("reporting your own answer: %d %v, want 403", code, out)
	}
	if code, out := report(t, "1", cid, ""); code != http.StatusForbidden {
		t.Errorf("reporting your own challenge: %d %v, want 403", code, out)
	}
	if code, _ := report(t, "1", cid, "999999"); code != http.StatusNotFound {
		t.Errorf("reporting an answer that isn't in this battle: %d, want 404", code)
	}
}

// ── what the rest of the app does with a video hidden or removed ──────────
//
// Nothing in this file hides or removes a video any more. These are for a
// row that is hidden or removed some other way — by hand, by moderation —
// and they hold the lists to skipping it.

func TestHiddenAnswer_IsOutOfTheBattle(t *testing.T) {
	defer withDB(t)()
	cid, rid := liveBattle(t)
	voter(t, 7201, 5)
	second, err := AcceptChallenge(AcceptChallengePayload{
		ChallengeID: cid, ResponderID: "7201", VideoURL: "https://v/second" + cid + ".mp4", DurationMs: 5000,
	})
	if err != nil {
		t.Fatal(err)
	}
	voter(t, 7202, 5)
	watchAnswer(t, 7202, cid, second.ID)
	if w := postVote(t, "7202", cid, second.ID, ""); w.Code != http.StatusOK {
		t.Fatalf("vote: %d %s", w.Code, w.Body.String())
	}
	if _, err := db.Exec(`UPDATE challenge_responses SET is_hidden = TRUE WHERE id = $1`, second.ID); err != nil {
		t.Fatal(err)
	}

	st := standingsJSON(t, cid)
	if ps := st["participants"].([]any); len(ps) != 2 || side(t, st, 1)["responseId"] != rid {
		t.Errorf("the score is %v — want the challenger and the answer still up", ps)
	}
	if w := postVote(t, "7202", cid, second.ID, ""); w.Code != http.StatusConflict {
		t.Errorf("voting for a hidden answer: %d %s", w.Code, w.Body.String())
	}
	items := []HomeFeedItem{{Type: "challenge", Challenge: &Challenge{ID: cid}}}
	populateTopResponses(items)
	if got := items[0].Challenge.TopResponseID; got != rid {
		t.Errorf("the feed shows answer %q, want %s", got, rid)
	}
	cards, _ := getBattles(t, "7201", "7201", "live")["battles"].([]any)
	if len(cards) != 0 {
		t.Errorf("the owner of the hidden answer still has %d live battles", len(cards))
	}
}

func TestRemovedChallenge_IsGoneForVisitors(t *testing.T) {
	defer withDB(t)()
	voter(t, 7501, 5)
	cid := auditChallenge(t, map[string]any{"creator": 7501, "subject": "juggle five"})
	if _, err := db.Exec(`UPDATE challenges SET visibility = 'arena', status = 'removed' WHERE id = $1`, cid); err != nil {
		t.Fatal(err)
	}
	has := func(list []Challenge) bool {
		for _, c := range list {
			if c.ID == cid {
				return true
			}
		}
		return false
	}
	if has(GetChallengesByCreator("7501", false, 50, 0)) {
		t.Error("a visitor still sees a removed challenge")
	}
	if !has(GetChallengesByCreator("7501", true, 50, 0)) {
		t.Error("the owner can't see their own removed challenge")
	}
	ch, _ := GetChallengeByID(cid)
	if err := validateChallengeResponseSubmission(AcceptChallengePayload{
		ChallengeID: cid, ResponderID: "1", VideoURL: "https://v/late" + cid + ".mp4", DurationMs: 5000,
	}, ch); err == nil || !strings.Contains(err.Error(), "removed") {
		t.Errorf("answering a removed challenge: %v", err)
	}
	voter(t, 7502, 5)
	if w := postVote(t, "7502", cid, "", "creator"); w.Code != http.StatusConflict {
		t.Errorf("voting on a removed challenge: %d %s", w.Code, w.Body.String())
	}
}

// ── the question the worker asks ──────────────────────────────────────────

func TestOffTopic_TheWorkerIsToldWhatToCheckAgainst(t *testing.T) {
	defer withDB(t)()
	cid, rid := liveBattle(t)
	if _, err := db.Exec(`UPDATE challenges SET prefix = 'Who can', subject = 'juggle "five"',
		hls_manifest_url = '', hls_attempts = 0, hls_claimed_at = NULL WHERE id = $1`, cid); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE challenge_responses SET hls_manifest_url = '', hls_attempts = 0,
		hls_claimed_at = NULL WHERE id = $1`, rid); err != nil {
		t.Fatal(err)
	}
	// Take jobs the way the worker does until both of these have come out.
	questions := map[string]string{}
	for i := 0; i < 2000 && len(questions) < 2; i++ {
		w := httptest.NewRecorder()
		HLSNextPendingHandler(w, httptest.NewRequest("GET", "/api/v1/internal/hls/next", nil))
		if w.Code == http.StatusNoContent {
			break
		}
		var job pendingHLSJob
		if err := json.Unmarshal(w.Body.Bytes(), &job); err != nil {
			t.Fatal(err)
		}
		if (job.Kind == "challenge" && job.ChallengeID == cid) ||
			(job.Kind == hlsKindResponse && job.ChallengeID == rid) {
			questions[job.Kind] = job.Question
		}
	}
	want := `Who can juggle "five"?`
	if questions["challenge"] != want {
		t.Errorf("the challenge's job asks %q, want %q", questions["challenge"], want)
	}
	if questions[hlsKindResponse] != want {
		t.Errorf("the answer's job asks %q, want the challenge it answers: %q",
			questions[hlsKindResponse], want)
	}
}

// ── what nobody can post ──────────────────────────────────────────────────

func TestCreate_PrefixAndSubjectHaveALimit(t *testing.T) {
	defer withDB(t)()
	post := func(prefix, subject string) (int, string) {
		b, _ := json.Marshal(CreateChallengePayload{
			VideoURL: "https://v/limit.mp4", Prefix: prefix, Subject: subject,
			Visibility: "arena", Category: "comedy",
		})
		r := httptest.NewRequest("POST", "/api/v1/challenges", bytes.NewReader(b))
		w := httptest.NewRecorder()
		CreateChallengeHandler(w, withUser(r, "1", "creator"))
		return w.Code, w.Body.String()
	}
	if code, body := post(strings.Repeat("a", maxPrefixChars+1), "juggling"); code != http.StatusBadRequest ||
		!strings.Contains(body, "at most 50 characters") {
		t.Errorf("a 51-character prefix: %d %s", code, body)
	}
	if code, body := post("Who can", strings.Repeat("b", maxSubjectChars+1)); code != http.StatusBadRequest ||
		!strings.Contains(body, "at most 30 characters") {
		t.Errorf("a 31-character subject: %d %s", code, body)
	}
}
