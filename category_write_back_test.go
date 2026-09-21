package main

// What a video IS, and who said so.
//
// Three faults, one shape. The app reads, listens to and looks at every
// upload — and none of that reached the place the rest of the app asks.
//
//  1. A silent video's only evidence is what the model SAW. analysisText,
//     whose whole job is "hand the matchers every word we have about this
//     video", left those words out. So the pass built to rescue silent
//     videos rescued them into a dead end.
//
//  2. The category column was written once, at upload, before anything had
//     watched the video, and never updated. The feed got away with it by
//     recomputing on every score. Nothing else did.
//
//  3. That same column was then read back as "what the creator said" — but
//     it might be a keyword guess this server made off the title. So the
//     app could report a creator disagreeing with the model about a video
//     whose creator never said anything.

import (
	"encoding/json"
	"strconv"
	"strings"
	"testing"
)

// ── 1. the silent half ──────────────────────────────────────────────────────

func TestAnalysisText_CarriesWhatTheModelSaw(t *testing.T) {
	// The case that matters: nobody speaks, nothing is written on screen.
	// Most of this catalogue looks like this.
	silent := &VideoAnalysis{
		Passes: []string{"frames"},
		Topics: []string{"street food", "biryani plate"},
	}
	got := analysisText(silent)
	if got == "" {
		t.Fatal("a silent video that the model LOOKED at came back with no " +
			"words at all. Its topics are the only evidence it will ever " +
			"offer, and this is the function that hands evidence to the " +
			"category and mood matchers.")
	}
	for _, want := range []string{"street food", "biryani plate"} {
		if !strings.Contains(got, want) {
			t.Errorf("analysisText dropped %q: got %q", want, got)
		}
	}
}

func TestAnalysisText_StillCarriesWordsAndSpeech(t *testing.T) {
	// The presence half. A version that returned only topics would pass the
	// test above and silently lose every video that DOES say something.
	a := &VideoAnalysis{
		ScreenText: "butter chicken",
		Speech:     "today we are cooking",
		Topics:     []string{"home cooking"},
	}
	got := analysisText(a)
	for _, want := range []string{"butter chicken", "today we are cooking", "home cooking"} {
		if !strings.Contains(got, want) {
			t.Errorf("analysisText dropped %q: got %q", want, got)
		}
	}
}

func TestSilentVideo_GetsACategoryFromWhatWasSeen(t *testing.T) {
	// End to end through the real decision function, with the shape a silent
	// video actually has: no creator category, no tags, no words — and a
	// model that looked and wrote down what it saw.
	//
	// Before topics reached analysisText this fell through to keyword-
	// matching the creator's subject line, which is the guess the whole
	// understanding pipeline exists to replace.
	seen := &VideoAnalysis{
		Passes: []string{"frames"},
		Topics: []string{"recipe", "kitchen"},
	}
	got := categoryFromEvidence(nil, nil, "", "my latest", "check out", analysisText(seen))
	if got.Category != "food" {
		t.Errorf("a video the model saw a kitchen and a recipe in came back "+
			"as %q (source %q), not food. Its topics are not reaching the "+
			"matcher.", got.Category, got.Source)
	}
}

// ── 3. whose word is it ─────────────────────────────────────────────────────

func TestCategorySourceAtUpload(t *testing.T) {
	cases := []struct {
		name, creator, stored, want string
	}{
		{"creator picked it", "food", "food", "creator"},
		{"we guessed it off the title", "", "food", "guess"},
		{"creator picked, tags won anyway", "food", "comedy", "guess"},
		{"nobody knew", "", "", ""},
		{"a shrug is not an answer", "", "other", ""},
		{"nor is the other shrug", "", "general", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := categorySourceAtUpload(c.creator, c.stored); got != c.want {
				t.Errorf("categorySourceAtUpload(%q,%q) = %q, want %q",
					c.creator, c.stored, got, c.want)
			}
		})
	}
}

func TestDisputed_NeedsARealPersonOnBothSides(t *testing.T) {
	// A guess is not a disagreement. This is the thing the creator_category
	// column exists to make true.
	machineSaysDance := []string{"dance"}

	nobodySaid := categoryFromEvidence(machineSaysDance, nil, "", "", "", "")
	if nobodySaid.Disputed() {
		t.Error("reported a creator disagreeing with the model when the " +
			"creator said nothing at all")
	}
	if nobodySaid.Creator != "" {
		t.Errorf("invented a creator claim: %q", nobodySaid.Creator)
	}

	// And the presence half — a real disagreement must still be reported,
	// or this whole signal could be switched off and every test above would
	// still pass.
	realDisagreement := categoryFromEvidence(machineSaysDance, nil, "comedy", "", "", "")
	if !realDisagreement.Disputed() {
		t.Error("a creator who said comedy over a video the model reads as " +
			"dance is exactly what Disputed() is for, and it stayed quiet")
	}
}

// ── 2 + 3 against a real database ───────────────────────────────────────────

// TestSettleCategory_WritesTheModelsAnswerToTheRow is the test paired with
// settleCategory in sql_compiles_test.go's runByATest. It RUNS both of that
// function's statements, against both tables, because neither can be read
// out of the source: the table name arrives as a parameter.
func TestSettleCategory_WritesTheModelsAnswerToTheRow(t *testing.T) {
	defer withDB(t)()
	cid, rid := runtimeFixture(t)

	for _, table := range []string{"challenges", "challenge_responses"} {
		id := cid
		if table == "challenge_responses" {
			id = rid
		}

		t.Run(table+"/the model decides", func(t *testing.T) {
			// A creator who said nothing, and a model that watched the video
			// and concluded "dance".
			if _, err := db.Exec(
				`UPDATE `+table+` SET category = 'comedy', creator_category = '',
				        category_source = 'guess', machine_category = ''
				  WHERE id = $1`, id); err != nil {
				t.Fatalf("seed: %v", err)
			}

			storeVideoAnalysis(table, id, json.RawMessage(
				`{"passes":["understand"],"autoTags":["dance"],"speech":"five six seven eight"}`))

			cat, machine, source, creator := readCategoryColumns(t, table, id)
			if machine != "dance" {
				t.Errorf("machine_category = %q, want dance — the model's "+
					"conclusion never reached the row, which is the whole bug",
					machine)
			}
			if cat != "dance" {
				t.Errorf("category = %q, want dance. The feed would rank this "+
					"as dance because it recomputes; every report that reads "+
					"the column still says %q.", cat, cat)
			}
			if source != "machine" {
				t.Errorf("category_source = %q, want machine", source)
			}
			if creator != "" {
				t.Errorf("creator_category = %q — nothing the worker does may "+
					"write into the one column that holds a person's claim",
					creator)
			}
		})

		t.Run(table+"/the model cannot tell", func(t *testing.T) {
			// It looked and had no opinion. What upload decided had the
			// creator's own pick behind it, so it must survive — replacing it
			// with a fresh guess at the same title would be a downgrade.
			if _, err := db.Exec(
				`UPDATE `+table+` SET category = 'food', creator_category = 'food',
				        category_source = 'creator', machine_category = 'dance'
				  WHERE id = $1`, id); err != nil {
				t.Fatalf("seed: %v", err)
			}

			storeVideoAnalysis(table, id, json.RawMessage(
				`{"passes":["frames"],"autoTags":["fast cuts"]}`))

			cat, machine, source, _ := readCategoryColumns(t, table, id)
			if cat != "food" {
				t.Errorf("category = %q, want food kept. The model had no "+
					"opinion, so there was nothing better to replace the "+
					"creator's own answer with.", cat)
			}
			if source != "creator" {
				t.Errorf("category_source = %q, want creator kept", source)
			}
			if machine != "" {
				t.Errorf("machine_category = %q, want empty. \"It looked and "+
					"could not tell\" is a finding, and has to be tellable "+
					"apart from \"nothing has looked yet\".", machine)
			}
		})
	}
}

func mustAtoi(t *testing.T, s string) int {
	t.Helper()
	n, err := strconv.Atoi(s)
	if err != nil {
		t.Fatalf("id %q: %v", s, err)
	}
	return n
}

// codeWithoutComments strips every comment line before a source check runs.
//
// Two tests in this repo once matched the words in the comment EXPLAINING a
// trap rather than the code that avoids it, and passed against code with the
// trap wide open. See CLAUDE.md.
func codeWithoutComments(src string) string {
	var out []string
	for _, line := range strings.Split(src, "\n") {
		if t := strings.TrimSpace(line); strings.HasPrefix(t, "//") {
			continue
		}
		out = append(out, line)
	}
	return strings.Join(out, "\n")
}

func readCategoryColumns(t *testing.T, table string, id int) (cat, machine, source, creator string) {
	t.Helper()
	if err := db.QueryRow(
		`SELECT COALESCE(category,''), COALESCE(machine_category,''),
		        COALESCE(category_source,''), COALESCE(creator_category,'')
		   FROM `+table+` WHERE id = $1`, id).Scan(&cat, &machine, &source, &creator); err != nil {
		t.Fatalf("read back %s id=%d: %v", table, id, err)
	}
	return
}

// TestUploadRecordsWhoActuallyChose proves the other end of the same wire:
// the insert has to put a person's pick somewhere a guess can never reach.
func TestUploadRecordsWhoActuallyChose(t *testing.T) {
	defer withDB(t)()

	t.Run("a creator who chose", func(t *testing.T) {
		ch, err := CreateChallenge(CreateChallengePayload{
			CreatorID: "1", VideoURL: "https://v/a.mp4",
			Prefix: "who is better at", Subject: "dancing",
			Visibility: "arena", Category: "food",
		})
		if err != nil {
			t.Fatalf("create: %v", err)
		}
		id := mustAtoi(t, ch.ID)
		_, _, source, creator := readCategoryColumns(t, "challenges", id)
		if creator != "food" {
			t.Errorf("creator_category = %q, want food — a real pick by a "+
				"real person has to be recorded as one", creator)
		}
		if source != "creator" {
			t.Errorf("category_source = %q, want creator", source)
		}
	})

	t.Run("a creator who skipped", func(t *testing.T) {
		ch, err := CreateChallenge(CreateChallengePayload{
			CreatorID: "1", VideoURL: "https://v/b.mp4",
			Prefix: "who is better at", Subject: "cooking biryani",
			Visibility: "arena", Category: "",
		})
		if err != nil {
			t.Fatalf("create: %v", err)
		}
		id := mustAtoi(t, ch.ID)
		cat, _, source, creator := readCategoryColumns(t, "challenges", id)
		if creator != "" {
			t.Errorf("creator_category = %q — nobody chose anything, and "+
				"writing our own guess in here is the bug this column "+
				"exists to stop", creator)
		}
		if source != "guess" {
			t.Errorf("category_source = %q, want guess (category came out %q)",
				source, cat)
		}
	})

	t.Run("a shrug is not a choice", func(t *testing.T) {
		ch, err := CreateChallenge(CreateChallengePayload{
			CreatorID: "1", VideoURL: "https://v/c.mp4",
			Prefix: "who is better at", Subject: "pranks",
			Visibility: "arena", Category: "other",
		})
		if err != nil {
			t.Fatalf("create: %v", err)
		}
		id := mustAtoi(t, ch.ID)
		_, _, _, creator := readCategoryColumns(t, "challenges", id)
		if creator != "" {
			t.Errorf("creator_category = %q — 'other' is what gets stored "+
				"when nobody chose, so it is not a claim", creator)
		}
	})
}

// TestRankerReadsTheCreatorsOwnColumn guards the read end.
//
// Source-read, because the failure being guarded against is a column name
// quietly changing back — and a test that called categoryFromEvidence by
// hand would pass with the wire cut, which is how this class of bug got
// into this repo five times.
func TestRankerReadsTheCreatorsOwnColumn(t *testing.T) {
	src := codeWithoutComments(readSourceFile(t, "feed_engine.go"))
	if !strings.Contains(src, "c.creator_category") {
		t.Error("the ranker no longer reads creator_category. It is back to " +
			"handing the category column in as the creator's word — which " +
			"for most rows is a keyword guess this server made off a title.")
	}

	admin := codeWithoutComments(readSourceFile(t, "media_analysis_read.go"))
	if !strings.Contains(admin, "creator_category") {
		t.Error("the admin analysis view no longer reads creator_category, " +
			"so its \"do the creator and the model agree\" column is back to " +
			"comparing the model against our own guess")
	}
}
