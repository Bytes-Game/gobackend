package main

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
)

// ════════════════════════════════════════════════════════════════════════════
// THE OTHER HALF OF A BATTLE, AGAINST A REAL DATABASE
// ════════════════════════════════════════════════════════════════════════════
//
// Every wire added here is a SQL change, and a mock database cannot check one.
// sqlmock replays whatever rows a test hands it without reading the statement,
// so a query naming a column that does not exist, a LATERAL join with the
// wrong arity, or a UNION whose two halves disagree all pass a mocked test and
// fail in production — where a missing column does not return blank, it
// refuses the whole statement and the feature reports "nothing here yet".
//
// So these run against real Postgres with the real schema, built by the real
// migration path. They skip cleanly when TEST_DATABASE_URL is unset, the same
// as the audit tests they borrow their fixtures from.

// answerFixture inserts one answer to a challenge and returns its id.
func answerFixture(t *testing.T, challengeID string, topics, auto, mine string) int {
	t.Helper()
	cid, err := strconv.Atoi(challengeID)
	if err != nil {
		t.Fatalf("challenge id %q: %v", challengeID, err)
	}
	var id int
	if err := db.QueryRow(`
		INSERT INTO challenge_responses
			(challenge_id, responder_id, video_url, content_topics, auto_tags, custom_tags)
		VALUES ($1, 2, 'https://v/answer.mp4', $2::jsonb, $3::jsonb, $4::jsonb)
		RETURNING id`, cid, topics, auto, mine).Scan(&id); err != nil {
		t.Fatalf("insert response: %v", err)
	}
	return id
}

// ── the loader ──────────────────────────────────────────────────────────────

func TestAnswerWords_ReadsTheAnswerTheViewerIsShown(t *testing.T) {
	defer withDB(t)()

	cid := auditChallenge(t, map[string]any{"subject": "dance battle"})
	// Two answers. populateTopResponses shows the NEWEST, so that is the one
	// this must describe — describing the other would make the ranker
	// confident about a video nobody sees.
	answerFixture(t, cid, `["old dance"]`, `["dance"]`, `[]`)
	answerFixture(t, cid, `["new dance","floor work"]`, `["dance"]`, `["breaking"]`)

	id, _ := strconv.Atoi(cid)
	got := loadAnswerWords(id)

	if !contains(got.Topics, "new dance") {
		t.Errorf("read the wrong answer: topics are %v, want the newest one", got.Topics)
	}
	if contains(got.Topics, "old dance") {
		t.Errorf("read an answer the viewer is not shown: %v", got.Topics)
	}
	// The responder's own word leads, the model's follows — the same order
	// mergeTags puts a challenge's two sets in.
	if len(got.Tags) == 0 || got.Tags[0] != "breaking" {
		t.Errorf("tags are %v; the responder's own word should lead", got.Tags)
	}
	if !contains(got.Tags, "dance") {
		t.Errorf("the model's tag was dropped: %v", got.Tags)
	}
}

func TestAnswerWords_AShortHasNoneAndSaysSoQuietly(t *testing.T) {
	defer withDB(t)()

	cid := auditChallenge(t, map[string]any{"subject": "nobody answered this"})
	id, _ := strconv.Atoi(cid)
	got := loadAnswerWords(id)
	if !got.empty() {
		t.Errorf("a challenge with no answers reported %+v", got)
	}
}

// ── the scorer ──────────────────────────────────────────────────────────────

// The wire this repo keeps cutting: a thing that works, with tests that pass,
// that nothing calls. Cut the call in computeContentScore and this fails.
func TestContentScore_BattleCarriesItsAnswersWords(t *testing.T) {
	defer withDB(t)()

	cid := auditChallenge(t, map[string]any{
		"subject":  "who cooks better",
		"category": "food",
	})
	if _, err := db.Exec(
		`UPDATE challenges SET content_topics = '["street food"]'::jsonb,
		                       auto_tags = '["food"]'::jsonb WHERE id = $1`, cid); err != nil {
		t.Fatalf("describe the challenge: %v", err)
	}
	answerFixture(t, cid, `["night market"]`, `["food"]`, `["chaat"]`)

	cs := computeContentScore(cid, "challenge")
	if cs == nil {
		t.Fatal("computeContentScore returned nil")
	}
	if !contains(cs.Topics, "street food") {
		t.Errorf("the challenge's own topic was lost: %v", cs.Topics)
	}
	if !contains(cs.Topics, "night market") {
		t.Errorf("the answer's topic never reached the battle. Topics: %v. "+
			"Half of what the viewer watches is describing nothing.", cs.Topics)
	}
	if !contains(cs.Tags, "chaat") {
		t.Errorf("the responder's own tag never reached the battle: %v", cs.Tags)
	}
	// The challenge leads. Its words are what the item IS; the answer is
	// context, and context must not push the subject out of the front.
	if len(cs.Topics) == 0 || cs.Topics[0] != "street food" {
		t.Errorf("the answer's words displaced the challenge's: %v", cs.Topics)
	}
}

// An answer must never move the battle's category.
//
// A category excludes: a viewer who asked for dance and got a battle refiled
// as comedy by one off-topic reply was not served what they asked for. The app
// already has a gentler way to say an answer does not fit, in relevance_score
// and the off-topic flags.
func TestContentScore_AnOffTopicAnswerDoesNotRefileTheBattle(t *testing.T) {
	defer withDB(t)()

	cid := auditChallenge(t, map[string]any{
		"subject":  "who dances better",
		"category": "dance",
	})
	if _, err := db.Exec(
		`UPDATE challenges SET auto_tags = '["dance"]'::jsonb WHERE id = $1`, cid); err != nil {
		t.Fatalf("describe the challenge: %v", err)
	}
	// An answer that is entirely comedy, tagged so by both the responder and
	// the model — the strongest possible pull away from dance.
	answerFixture(t, cid, `["stand up"]`, `["comedy"]`, `["comedy"]`)

	cs := computeContentScore(cid, "challenge")
	if cs == nil {
		t.Fatal("computeContentScore returned nil")
	}
	if cs.Category != "dance" {
		t.Errorf("one comedy answer refiled a dance battle as %q. A viewer "+
			"who asked for dance would stop getting this.", cs.Category)
	}
}

// The answer's mood is a separate wire from its words: tags go into cs.Tags,
// moods into the emotion vector. Cutting one leaves the other working, so it
// needs its own test.
func TestContentScore_BattleCarriesItsAnswersMood(t *testing.T) {
	defer withDB(t)()

	cid := auditChallenge(t, map[string]any{
		"subject":  "who is calmer",
		"category": "motivation",
	})
	if _, err := db.Exec(
		`UPDATE challenges SET emotion_tags = '["calm"]'::jsonb WHERE id = $1`, cid); err != nil {
		t.Fatalf("set the challenge mood: %v", err)
	}
	rid := answerFixture(t, cid, `[]`, `[]`, `[]`)
	if _, err := db.Exec(
		`UPDATE challenge_responses SET emotion_tags = '["funny"]'::jsonb WHERE id = $1`, rid); err != nil {
		t.Fatalf("set the answer mood: %v", err)
	}

	cs := computeContentScore(cid, "challenge")
	if cs == nil {
		t.Fatal("computeContentScore returned nil")
	}
	if cs.EmotionVector["calm"] == 0 {
		t.Errorf("the challenge's own mood was lost: %v", cs.EmotionVector)
	}
	if cs.EmotionVector["funny"] == 0 {
		t.Errorf("the answer's mood never reached the battle: %v. The viewer "+
			"sat through both videos.", cs.EmotionVector)
	}
}

// ── the energy baseline ─────────────────────────────────────────────────────

// Somebody who only ever answers other people's battles is a real kind of
// user — answering is the lighter act, and the accept rate limit is set three
// times higher than the create one for that reason. Their energy used to be
// guessed from the words every single time, because the baseline read the
// challenges table alone and they had never posted one.
func TestCreatorEnergy_CountsTheVideosSomebodyOnlyAnswersWith(t *testing.T) {
	defer withDB(t)()

	cid := auditChallenge(t, map[string]any{"subject": "somebody else's battle"})
	pid, _ := strconv.Atoi(cid)
	for i := 0; i < 3; i++ {
		var rid int
		if err := db.QueryRow(`
			INSERT INTO challenge_responses
				(challenge_id, responder_id, video_url, energy_level)
			VALUES ($1, 2, 'https://v/a.mp4', 'high') RETURNING id`, pid).Scan(&rid); err != nil {
			t.Fatalf("insert answer %d: %v", i, err)
		}
	}

	if got := creatorRecentEnergy("2"); got != "high" {
		t.Errorf("a responder with three high-energy answers has a baseline "+
			"of %q — their own videos are not being counted", got)
	}
}

// The other half of the same change: a creator who has only ever posted
// challenges must get exactly the baseline they got before.
//
// This is the leg that already worked, and it is the one a badly-formed union
// takes down — the query fails as a whole, this function swallows the error as
// "no opinion", and the energy baseline silently stops existing for everybody.
// That is not hypothetical: the first version of the union here was missing
// the brackets around each leg and did exactly that.
func TestCreatorEnergy_StillReadsAChallengeOnlyCreator(t *testing.T) {
	defer withDB(t)()

	for i := 0; i < 3; i++ {
		auditChallenge(t, map[string]any{
			"creator": 1,
			"subject": fmt.Sprintf("calm challenge %d", i),
			"energy":  "low",
		})
	}
	if got := creatorRecentEnergy("1"); got != "low" {
		t.Errorf("a creator with three low-energy challenges has a baseline "+
			"of %q — the path that already worked is broken", got)
	}
}

// ── the topic graph ─────────────────────────────────────────────────────────

func TestTopicGraph_CountsBothHalvesOfABattle(t *testing.T) {
	defer withDB(t)()

	// The graph needs a subject pair to appear more than once before it calls
	// it a relationship, so two battles share the same pairing — and the
	// pairing only exists at all if answers are read.
	for i := 0; i < 3; i++ {
		cid := auditChallenge(t, map[string]any{"subject": fmt.Sprintf("battle %d", i)})
		if _, err := db.Exec(
			`UPDATE challenges SET visibility = 'arena', status = 'open',
			        content_topics = '["thistle"]'::jsonb WHERE id = $1`, cid); err != nil {
			t.Fatalf("describe the challenge: %v", err)
		}
		answerFixture(t, cid, `["thistle","pollination"]`, `[]`, `[]`)
	}

	g := buildTopicGraph()
	if g == nil {
		t.Fatal("buildTopicGraph returned nil")
	}
	if g.freq["pollination"] == 0 {
		t.Error("a subject that appears only on answers was never counted, " +
			"so the graph cannot relate it to anything")
	}
	if g.freq["thistle"] < 6 {
		t.Errorf("thistle counted %d times across three challenges and three "+
			"answers that all mention it", g.freq["thistle"])
	}
}

// A hidden answer is shown to nobody, so nothing should be learned from it.
func TestTopicGraph_IgnoresAHiddenAnswer(t *testing.T) {
	defer withDB(t)()

	cid := auditChallenge(t, map[string]any{"subject": "hidden answer battle"})
	if _, err := db.Exec(
		`UPDATE challenges SET visibility = 'arena', status = 'open' WHERE id = $1`, cid); err != nil {
		t.Fatalf("publish the challenge: %v", err)
	}
	rid := answerFixture(t, cid, `["moderated subject"]`, `[]`, `[]`)
	if _, err := db.Exec(
		`UPDATE challenge_responses SET is_hidden = TRUE WHERE id = $1`, rid); err != nil {
		t.Fatalf("hide the answer: %v", err)
	}

	g := buildTopicGraph()
	if g.freq["moderated subject"] != 0 {
		t.Error("a hidden answer taught the topic graph something nobody can see")
	}
}

// ── the taste profile ───────────────────────────────────────────────────────

// The rebuild reads five hundred events through a LATERAL join added for this.
// A join with the wrong arity does not error here — rows.Scan fails silently
// and every profile rebuilds EMPTY, which looks exactly like a new user.
func TestProfileRebuild_LearnsFromTheAnswerHalfOfABattle(t *testing.T) {
	defer withDB(t)()

	cid := auditChallenge(t, map[string]any{
		"subject":  "who cooks better",
		"category": "food",
	})
	answerFixture(t, cid, `["night market"]`, `["food"]`, `[]`)

	// Watched nearly to the end, five times. 'view' with a high completion
	// rate is what engagementWeight scores at 1.0 — 'complete' is not one of
	// the event types it knows and weighs exactly nothing, which is how a
	// first draft of this test managed to accuse the join of being broken.
	for i := 0; i < 5; i++ {
		if _, err := db.Exec(`
			INSERT INTO feed_events (user_id, content_id, content_type, event_type,
				completion_rate, metadata)
			VALUES ('1', $1, 'challenge', 'view', 0.95, '{}'::jsonb)`, cid); err != nil {
			t.Fatalf("record a view: %v", err)
		}
	}

	p, err := computeUserProfile("1")
	if err != nil {
		t.Fatalf("computeUserProfile: %v", err)
	}
	if p == nil {
		t.Fatal("computeUserProfile returned nil")
	}
	if len(p.TopicAffinity) == 0 {
		t.Fatal("the rebuild learned no subjects at all — if the LATERAL " +
			"join's arity is wrong, rows.Scan fails silently and every " +
			"profile comes back empty")
	}
	if p.TopicAffinity["night market"] <= 0 {
		t.Errorf("watching a battle taught nothing from its answer. "+
			"Subjects learned: %v", p.TopicAffinity)
	}
}

// ── the write path ──────────────────────────────────────────────────────────

func TestAcceptChallenge_StoresTheRespondersOwnWords(t *testing.T) {
	defer withDB(t)()

	cid := auditChallenge(t, map[string]any{
		"subject":  "who dances better",
		"category": "dance",
	})
	got, err := AcceptChallenge(AcceptChallengePayload{
		ChallengeID: cid,
		ResponderID: "2",
		VideoURL:    "https://v/answer.mp4",
		DurationMs:  5000,
		Tags:        []string{"#Breaking", "breaking", "floor work"},
	})
	if err != nil {
		t.Fatalf("AcceptChallenge: %v", err)
	}

	var category, energy string
	var tagsJSON string
	if err := db.QueryRow(`
		SELECT COALESCE(category,''), COALESCE(energy_level,''),
		       COALESCE(custom_tags::text,'[]')
		  FROM challenge_responses WHERE id = $1`, got.ID).
		Scan(&category, &energy, &tagsJSON); err != nil {
		t.Fatalf("read back: %v", err)
	}

	// Folded on the way in, so two videos tagged the same thing compare equal.
	if tagsJSON != `["breaking", "floor work"]` && tagsJSON != `["breaking","floor work"]` {
		t.Errorf("tags stored as %s; want them folded and de-duplicated", tagsJSON)
	}
	// Nobody said what this answer was, so it inherits the question. Without
	// that it would be 'other', and 'other' means "nobody knows".
	if category != "dance" {
		t.Errorf("the answer to a dance challenge was filed as %q", category)
	}
	if energy == "" {
		t.Error("no energy level was derived; the column would sit at its " +
			"default and the hour router would compare against nothing")
	}
}

// The mood column, through the door a real answer comes in by.
//
// Setting emotion_tags with a direct UPDATE — which the scoring test above
// does, because it is testing the read — proves nothing about whether anything
// ever WRITES it. Deleting the write left every test green until this existed.
func TestAcceptChallenge_StoresTheMoodTheResponderImplied(t *testing.T) {
	defer withDB(t)()

	cid := auditChallenge(t, map[string]any{"subject": "who is funnier"})
	got, err := AcceptChallenge(AcceptChallengePayload{
		ChallengeID: cid,
		ResponderID: "2",
		VideoURL:    "https://v/answer.mp4",
		DurationMs:  5000,
		// "funny" is on the emotion list, so tagging it says how the video
		// feels as well as what it is about. See emotionsForContent.
		Tags: []string{"funny"},
	})
	if err != nil {
		t.Fatalf("AcceptChallenge: %v", err)
	}
	var moods string
	if err := db.QueryRow(
		`SELECT COALESCE(emotion_tags::text,'[]') FROM challenge_responses WHERE id = $1`,
		got.ID).Scan(&moods); err != nil {
		t.Fatalf("read back: %v", err)
	}
	if !strings.Contains(moods, "funny") {
		t.Errorf("the responder's mood was stored as %s. The column exists "+
			"and the scorer reads it; nothing was filling it.", moods)
	}
	// And it comes back on the wire, so the app can show what was recorded.
	if len(got.EmotionTags) == 0 {
		t.Error("the response returned to the client carries no mood")
	}
}

func TestAcceptChallenge_TheResponderOutranksTheChallenge(t *testing.T) {
	defer withDB(t)()

	cid := auditChallenge(t, map[string]any{
		"subject":  "who dances better",
		"category": "dance",
	})
	got, err := AcceptChallenge(AcceptChallengePayload{
		ChallengeID: cid,
		ResponderID: "2",
		VideoURL:    "https://v/answer.mp4",
		DurationMs:  5000,
		Category:    "comedy",
	})
	if err != nil {
		t.Fatalf("AcceptChallenge: %v", err)
	}
	var category string
	if err := db.QueryRow(
		`SELECT COALESCE(category,'') FROM challenge_responses WHERE id = $1`,
		got.ID).Scan(&category); err != nil {
		t.Fatalf("read back: %v", err)
	}
	if category != "comedy" {
		t.Errorf("the responder said comedy and it was stored as %q — the "+
			"challenge does not get to overrule the person who made the video",
			category)
	}
}

// ── the admin read ──────────────────────────────────────────────────────────

// The query this endpoint builds names columns by hand. Against a real
// database a wrong name returns nothing for EVERY response rather than an
// empty field on one, which reads as "nothing has been analysed yet".
func TestAdminAnalysisRead_RunsAgainstARealResponsesTable(t *testing.T) {
	defer withDB(t)()

	cid := auditChallenge(t, map[string]any{"subject": "who cooks better"})
	rid := answerFixture(t, cid, `["night market"]`, `["food"]`, `["chaat"]`)
	if _, err := db.Exec(`
		UPDATE challenge_responses
		   SET video_analysis = '{"passes":["understand"],"autoTags":["food"]}'::jsonb,
		       category = 'comedy'
		 WHERE id = $1`, rid); err != nil {
		t.Fatalf("store an analysis: %v", err)
	}

	rows := readAnalysisRows("challenge_responses", "responses", rid, 1)
	if len(rows) != 1 {
		t.Fatalf("got %d rows, want 1 — a column name the table does not "+
			"have takes down the whole listing", len(rows))
	}
	if rows[0].CreatorCategory != "comedy" {
		t.Errorf("the responder's category came back as %q, want comedy",
			rows[0].CreatorCategory)
	}
	if rows[0].MachineCategory != "food" {
		t.Errorf("the model's category came back as %q, want food",
			rows[0].MachineCategory)
	}
	if !rows[0].Disputed {
		t.Error("the responder said comedy, the model said food, and the " +
			"endpoint reported no disagreement")
	}
}

func contains(hay []string, needle string) bool {
	for _, s := range hay {
		if s == needle {
			return true
		}
	}
	return false
}
