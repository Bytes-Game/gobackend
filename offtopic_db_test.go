package main

import (
	"bytes"
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
// way the app sends them, and what taking a video down does to the battle,
// the records and the lists.

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

func answerDown(t *testing.T, rid string) bool {
	t.Helper()
	var hidden, marked bool
	if err := db.QueryRow(`SELECT COALESCE(is_hidden, FALSE), off_topic_at IS NOT NULL
		FROM challenge_responses WHERE id = $1`, rid).Scan(&hidden, &marked); err != nil {
		t.Fatal(err)
	}
	if hidden != marked {
		t.Errorf("answer %s: hidden=%v but marked as taken down=%v", rid, hidden, marked)
	}
	return hidden
}

// watchAnswer is a viewer having the answer on screen for four seconds.
func watchAnswer(t *testing.T, uid int, cid, rid string) {
	t.Helper()
	watch(t, strconv.Itoa(uid), cid, map[string]any{"opponentMs": 4000, "responseId": rid})
}

// ── the rule ──────────────────────────────────────────────────────────────

func TestOffTopic_TheModelAndTheOpponentTakeDownAnAnswer(t *testing.T) {
	defer withDB(t)()
	cid, rid := liveBattle(t)
	creator0, answerer0 := recordOf(t, 1), recordOf(t, 2)

	// The model says the answer doesn't match. On its own that takes
	// nothing down — it only warns the owner.
	modelSays(t, "response", rid, "no")
	if answerDown(t, rid) {
		t.Fatal("the model alone took the answer down")
	}
	if w := notesOf(t, 2, cid, noteOffTopicWarning); len(w) != 1 ||
		!strings.Contains(w[0], "may not match") {
		t.Errorf("the answer's owner was warned %v, want one heads-up", w)
	}
	// The same answer again is not a second warning.
	modelSays(t, "response", rid, "no")
	if w := notesOf(t, 2, cid, noteOffTopicWarning); len(w) != 1 {
		t.Errorf("the owner has %d warnings after the same answer twice", len(w))
	}

	// The challenger reports it: two signals agree.
	code, out := report(t, "1", cid, rid)
	if code != http.StatusOK || out["takenDown"] != true {
		t.Fatalf("the challenger's report: %d %v — want it taken down", code, out)
	}
	if !answerDown(t, rid) {
		t.Fatal("the answer is still up")
	}

	// One side left, so the battle is over and the challenger won it.
	var status string
	var resolved bool
	if err := db.QueryRow(`SELECT status, resolved_at IS NOT NULL FROM challenges WHERE id = $1`,
		cid).Scan(&status, &resolved); err != nil {
		t.Fatal(err)
	}
	if status != "completed" || !resolved {
		t.Errorf("battle is %s, resolved=%v — want completed and decided", status, resolved)
	}
	creator, answerer := recordOf(t, 1), recordOf(t, 2)
	if creator.Wins != creator0.Wins+1 || creator.Rating <= creator0.Rating {
		t.Errorf("challenger %+v → %+v: want one more win and a higher rating", creator0, creator)
	}
	if answerer.Losses != answerer0.Losses+1 || answerer.Strikes != answerer0.Strikes+1 {
		t.Errorf("answerer %+v → %+v: want one more loss and one strike", answerer0, answerer)
	}
	if lost := answerer0.Rating - answerer.Rating; lost <= integrityPenalty && answerer.Rating > ratingFloor {
		t.Errorf("answerer lost %d rating points, want more than the %d penalty "+
			"(a lost battle costs something too)", lost, integrityPenalty)
	}
	var outcomes []string
	rows, err := db.Query(`SELECT user_id || ':' || outcome || ':' || flagged
		FROM battle_results WHERE challenge_id = $1 ORDER BY user_id`, cid)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			t.Fatal(err)
		}
		outcomes = append(outcomes, s)
	}
	rows.Close()
	if strings.Join(outcomes, " ") != "1:won:false 2:lost:true" {
		t.Errorf("battle results %v, want the challenger won and the answerer lost, marked", outcomes)
	}

	// Both are told, in words that say what happened.
	gone := notesOf(t, 2, cid, noteOffTopic)
	if len(gone) != 1 || !strings.Contains(gone[0], "was taken down") ||
		!strings.Contains(gone[0], "You lost the battle and "+strconv.Itoa(answerer0.Rating-answerer.Rating)+" rating points.") {
		t.Errorf("the answerer was told %v", gone)
	}
	var pushes int
	if err := db.QueryRow(`SELECT COUNT(*) FROM notification_outbox
		WHERE user_id = '2' AND dedupe_key = $1`, "off_topic:"+cid+":2").Scan(&pushes); err != nil {
		t.Fatal(err)
	}
	if pushes != 1 {
		t.Errorf("the answerer has %d pushes about it queued, want 1", pushes)
	}
	won := notesOf(t, 1, cid, noteBattleWon)
	if len(won) != 1 || !strings.Contains(won[0], "didn't match the challenge") {
		t.Errorf("the challenger was told %v", won)
	}

	// Reporting it again changes nothing and charges nobody twice.
	if code, out := report(t, "1", cid, rid); code != http.StatusOK || out["takenDown"] != true {
		t.Errorf("a second report: %d %v", code, out)
	}
	if again := recordOf(t, 2); again != answerer {
		t.Errorf("a second report charged the answerer again: %+v → %+v", answerer, again)
	}
}

func TestOffTopic_ReportsAloneNeedThreeRealViewers(t *testing.T) {
	defer withDB(t)()
	cid, rid := liveBattle(t)
	modelSays(t, "response", rid, "yes")

	// The challenger alone is not enough when the model says it matches.
	if _, out := report(t, "1", cid, rid); out["takenDown"] != false {
		t.Fatalf("the challenger alone took it down: %v", out)
	}
	// Reports that don't count: an old account that never watched it, and a
	// brand-new account made after the video — the cheapest way to pile on.
	voter(t, 7101, 5)
	if _, out := report(t, "7101", cid, rid); out["takenDown"] != false {
		t.Fatal("a report from somebody who never watched took it down")
	}
	seedAccount(t, 7102, time.Now(), 0)
	watchAnswer(t, 7102, cid, rid)
	if _, out := report(t, "7102", cid, rid); out["takenDown"] != false {
		t.Fatal("a report from a brand-new account took it down")
	}
	// Real viewers: two is not enough...
	for _, v := range []int{7103, 7104} {
		voter(t, v, 5)
		watchAnswer(t, v, cid, rid)
		if _, out := report(t, strconv.Itoa(v), cid, rid); out["takenDown"] != false {
			t.Fatalf("down after viewer %d", v)
		}
	}
	if answerDown(t, rid) {
		t.Fatal("down with two real viewers' reports")
	}
	// ...three is.
	voter(t, 7105, 5)
	watchAnswer(t, 7105, cid, rid)
	if _, out := report(t, "7105", cid, rid); out["takenDown"] != true {
		t.Fatalf("three real viewers reported it and it stayed up: %v", out)
	}
	if !answerDown(t, rid) {
		t.Fatal("the answer is still up")
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

func TestOffTopic_ABattleWithOtherAnswersCarriesOn(t *testing.T) {
	defer withDB(t)()
	cid, rid := liveBattle(t)
	voter(t, 7201, 5)
	second, err := AcceptChallenge(AcceptChallengePayload{
		ChallengeID: cid, ResponderID: "7201", VideoURL: "https://v/second" + cid + ".mp4", DurationMs: 5000,
	})
	if err != nil {
		t.Fatal(err)
	}
	// Somebody voted for the second answer before it came down.
	voter(t, 7202, 5)
	watchAnswer(t, 7202, cid, second.ID)
	if w := postVote(t, "7202", cid, second.ID, ""); w.Code != http.StatusOK {
		t.Fatalf("vote: %d %s", w.Code, w.Body.String())
	}
	before := recordOf(t, 7201)

	modelSays(t, "response", second.ID, "no")
	if _, out := report(t, "1", cid, second.ID); out["takenDown"] != true {
		t.Fatalf("not taken down: %v", out)
	}

	// Still two sides: the battle goes on without it.
	var status string
	var resolved bool
	if err := db.QueryRow(`SELECT status, resolved_at IS NOT NULL FROM challenges WHERE id = $1`,
		cid).Scan(&status, &resolved); err != nil {
		t.Fatal(err)
	}
	if status != "active" || resolved {
		t.Errorf("battle is %s, resolved=%v — want it still running", status, resolved)
	}
	st := standingsJSON(t, cid)
	ps := st["participants"].([]any)
	if len(ps) != 2 {
		t.Fatalf("the score has %d sides, want 2 (the challenger and the answer still up)", len(ps))
	}
	if got := side(t, st, 1)["responseId"]; got != rid {
		t.Errorf("the second side is answer %v, want %s", got, rid)
	}
	// Its owner has lost, now, and is told.
	after := recordOf(t, 7201)
	if after.Losses != before.Losses+1 || after.Strikes != before.Strikes+1 ||
		before.Rating-after.Rating <= integrityPenalty {
		t.Errorf("owner of the video taken down: %+v → %+v", before, after)
	}
	if n := notesOf(t, 7201, cid, noteOffTopic); len(n) != 1 {
		t.Errorf("the owner has %d take-down notes, want 1", len(n))
	}
	// Nobody else won anything yet.
	if n := notesOf(t, 1, cid, noteBattleWon); len(n) != 0 {
		t.Errorf("the challenger was told they won a battle still running: %v", n)
	}

	// No more votes for it, with a reason; the voter can pick again.
	w := postVote(t, "7202", cid, second.ID, "")
	if w.Code != http.StatusConflict || !strings.Contains(w.Body.String(), "taken down") {
		t.Errorf("voting for the video taken down: %d %s", w.Code, w.Body.String())
	}
	if w := postVote(t, "7202", cid, rid, ""); w.Code != http.StatusOK {
		t.Errorf("voting for the answer still up: %d %s", w.Code, w.Body.String())
	}

	// The feed shows the answer still up, and counts one.
	items := []HomeFeedItem{{Type: "challenge", Challenge: &Challenge{ID: cid}}}
	populateTopResponses(items)
	if got := items[0].Challenge; got.TopResponseID != rid {
		t.Errorf("the feed's battle shows answer %q, want %s", got.TopResponseID, rid)
	}
	// And the one who lost it has it among their lost battles, not live ones.
	tab := func(name string) []string {
		var ids []string
		cards, _ := getBattles(t, "7201", "7201", name)["battles"].([]any)
		for _, c := range cards {
			ids = append(ids, c.(map[string]any)["challengeId"].(string))
		}
		return ids
	}
	if live := tab("live"); strings.Contains(strings.Join(live, ","), cid) {
		t.Errorf("the owner still has it among live battles: %v", live)
	}
	if lost := tab("lost"); strings.Join(lost, ",") != cid {
		t.Errorf("the owner's lost battles are %v, want [%s]", lost, cid)
	}
}

func TestOffTopic_AChallengeThatDoesntMatchItsOwnWords(t *testing.T) {
	defer withDB(t)()
	// No answers yet: the challenge comes down and its owner pays the
	// penalty, but there was no battle to lose.
	voter(t, 7301, 5)
	cid := auditChallenge(t, map[string]any{"creator": 7301, "subject": "juggle five"})
	if _, err := db.Exec(`UPDATE challenges SET creator_id = 7301, visibility = 'arena' WHERE id = $1`, cid); err != nil {
		t.Fatal(err)
	}
	before := recordOf(t, 7301)
	modelSays(t, "challenge", cid, "no")
	if w := notesOf(t, 7301, cid, noteOffTopicWarning); len(w) != 1 {
		t.Errorf("the creator was warned %v, want once", w)
	}
	voter(t, 7302, 5)
	watch(t, "7302", cid, map[string]any{"creatorMs": 4000})
	if _, out := report(t, "7302", cid, ""); out["takenDown"] != true {
		t.Fatalf("not taken down: %v", out)
	}
	ch, ok := GetChallengeByID(cid)
	if !ok || ch.Status != "removed" {
		t.Fatalf("challenge status %q, want removed", ch.Status)
	}
	after := recordOf(t, 7301)
	if after.Rating != max(before.Rating-integrityPenalty, ratingFloor) ||
		after.Losses != before.Losses || after.Strikes != before.Strikes+1 {
		t.Errorf("creator %+v → %+v: want the penalty and a strike, no loss", before, after)
	}
	if n := notesOf(t, 7301, cid, noteOffTopic); len(n) != 1 ||
		!strings.Contains(n[0], "It cost you "+strconv.Itoa(integrityPenalty)+" rating points.") {
		t.Errorf("the creator was told %v", n)
	}

	// Gone for everybody else; still on the owner's own profile.
	has := func(list []Challenge) bool {
		for _, c := range list {
			if c.ID == cid {
				return true
			}
		}
		return false
	}
	if has(GetChallengesByCreator("7301", false, 50, 0)) {
		t.Error("a visitor still sees the challenge taken down on the profile")
	}
	if !has(GetChallengesByCreator("7301", true, 50, 0)) {
		t.Error("the owner can't see their own challenge taken down")
	}
	// Nobody can answer it now.
	voter(t, 7303, 5)
	b, _ := json.Marshal(AcceptChallengePayload{ChallengeID: cid, VideoURL: "https://v/late.mp4", DurationMs: 5000})
	r := httptest.NewRequest("POST", "/api/v1/challenges/accept", bytes.NewReader(b))
	w := httptest.NewRecorder()
	AcceptChallengeHandler(w, withUser(r, "7303", "voter7303"))
	if w.Code != http.StatusBadRequest || !strings.Contains(w.Body.String(), "taken down") {
		t.Errorf("answering a challenge taken down: %d %s", w.Code, w.Body.String())
	}
}

func TestOffTopic_AChallengeTakenDownMidBattleHandsTheAnswerTheWin(t *testing.T) {
	defer withDB(t)()
	cid, rid := liveBattle(t)
	creator0, answerer0 := recordOf(t, 1), recordOf(t, 2)
	modelSays(t, "challenge", cid, "no")
	// The answerer is in the battle, so their report counts with the model's.
	if _, out := report(t, "2", cid, ""); out["takenDown"] != true {
		t.Fatalf("not taken down: %v", out)
	}
	var status string
	var resolved bool
	if err := db.QueryRow(`SELECT status, resolved_at IS NOT NULL FROM challenges WHERE id = $1`,
		cid).Scan(&status, &resolved); err != nil {
		t.Fatal(err)
	}
	if status != "removed" || !resolved {
		t.Errorf("battle is %s, resolved=%v — want removed and decided", status, resolved)
	}
	creator, answerer := recordOf(t, 1), recordOf(t, 2)
	if creator.Losses != creator0.Losses+1 || creator.Strikes != creator0.Strikes+1 {
		t.Errorf("challenger %+v → %+v: want a loss and a strike", creator0, creator)
	}
	if answerer.Wins != answerer0.Wins+1 || answerer.Rating <= answerer0.Rating {
		t.Errorf("answerer %+v → %+v: want a win", answerer0, answerer)
	}
	if n := notesOf(t, 2, cid, noteBattleWon); len(n) != 1 {
		t.Errorf("the answerer was told %v, want one win note", n)
	}
	voter(t, 7401, 5)
	if w := postVote(t, "7401", cid, rid, ""); w.Code != http.StatusConflict {
		t.Errorf("a vote on a challenge taken down: %d %s", w.Code, w.Body.String())
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
