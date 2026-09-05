package main

import (
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

// The query also returns the creator's own category and tags, so the endpoint
// can show BOTH answers to "what is this video" side by side — the creator's
// and the model's. Counting how often they disagree is the only way to know
// whether preferring the machine was the right call, and nothing else in the
// app puts the two together.
func analysisRowsFor(raw string) *sqlmock.Rows {
	return analysisRowsWithCreator(raw, "", "[]")
}

// analysisRowsWithCreator is the same row with the creator's claim filled in,
// for the cases that are about the disagreement rather than the transcript.
func analysisRowsWithCreator(raw, category, tagsJSON string) *sqlmock.Rows {
	return sqlmock.NewRows([]string{
		"id", "created_at", "video_analysis", "category", "tags",
	}).AddRow(259, time.Now(), raw, category, tagsJSON)
}

// ════════════════════════════════════════════════════════════════════════════
// THE WHOLE POINT IS THE WORDS
// ════════════════════════════════════════════════════════════════════════════
//
// The worker stored transcripts that nothing could read. The workflow log
// prints a word count and never the words, and no endpoint returned the
// column, so "is the speech pass any good?" had no answer — through a whole
// model change, which moved the first two uploads from one-to-six words to
// 32 and 39 with nobody able to see whether those were sentences or noise.

func TestReadAnalysis_ReturnsTheTranscript(t *testing.T) {
	mock, cleanup := withMockDB(t)
	defer cleanup()

	stored := `{"passes":["shape","text","speech"],` +
		`"speech":"aaj hum banayenge aloo ka paratha",` +
		`"screenText":"RECIPE","autoTags":["talking"]}`
	mock.ExpectQuery(regexp.QuoteMeta("FROM challenges")).
		WillReturnRows(analysisRowsFor(stored))

	got := readAnalysisRows("challenges", "challenges", 259, 1)
	if len(got) != 1 {
		t.Fatalf("expected one row, got %d", len(got))
	}
	if got[0].Speech != "aaj hum banayenge aloo ka paratha" {
		t.Errorf("the transcript did not come back; got %q. Reading the words "+
			"is the only reason this endpoint exists.", got[0].Speech)
	}
	if got[0].Screen != "RECIPE" {
		t.Errorf("on-screen text came back as %q", got[0].Screen)
	}
	if got[0].SpeechWords != 6 {
		t.Errorf("word count came back as %d, want 6 — it has to line up "+
			"with the number the workflow log prints", got[0].SpeechWords)
	}
}

func TestReadAnalysis_KeepsARowWhoseReadingIsCorrupt(t *testing.T) {
	// An unreadable blob IS the answer somebody is looking for. Dropping it
	// silently would make a corrupt reading look like a missing one, which
	// is a different fault with a different fix.
	mock, cleanup := withMockDB(t)
	defer cleanup()

	mock.ExpectQuery(regexp.QuoteMeta("FROM challenges")).
		WillReturnRows(analysisRowsFor(`{not json`))

	got := readAnalysisRows("challenges", "challenges", 259, 1)
	if len(got) != 1 {
		t.Fatalf("a corrupt reading vanished instead of being reported; got %d rows", len(got))
	}
	if got[0].ID != 259 {
		t.Errorf("wrong id came back: %d", got[0].ID)
	}
	if got[0].Speech != "" {
		t.Errorf("expected no transcript from unreadable JSON, got %q", got[0].Speech)
	}
}

func TestReadAnalysis_SkipsRowsNobodyEverAnalysed(t *testing.T) {
	// Most rows on a young platform have no reading at all. Listing them
	// buries the handful worth looking at, so the filter is in the SQL.
	mock, cleanup := withMockDB(t)
	defer cleanup()

	mock.ExpectQuery(regexp.QuoteMeta("WHERE video_analysis IS NOT NULL")).
		WillReturnRows(analysisRowsFor(`{"passes":["shape"]}`))

	if got := readAnalysisRows("challenges", "challenges", 0, 20); len(got) != 1 {
		t.Fatalf("expected the filtered query to run; got %d rows", len(got))
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("rows with no reading were not filtered out: %v", err)
	}
}

func TestReadAnalysis_CountsWordsTheSameWayTheWorkerLogDoes(t *testing.T) {
	// Whitespace-separated, so a row here can be lined up against the
	// "speechWords=N" line in the run that produced it.
	for in, want := range map[string]int{
		"":                       0,
		"   ":                    0,
		"one":                    1,
		"aaj hum banayenge":      3,
		"  spaced   out   words": 3,
	} {
		if got := countWords(in); got != want {
			t.Errorf("countWords(%q) = %d, want %d", in, got, want)
		}
	}
}

// ════════════════════════════════════════════════════════════════════════════
// AND WHO SAID WHAT
// ════════════════════════════════════════════════════════════════════════════
//
// The machine now outranks the creator when it has an opinion. Whether that
// was right is an empirical question, and it can only be answered by seeing
// both answers together — which is what this endpoint is now for.

func TestReadAnalysis_ShowsBothAnswersWhenTheyDisagree(t *testing.T) {
	mock, cleanup := withMockDB(t)
	defer cleanup()

	// The model read the video and called it horror. The creator said comedy.
	stored := `{"passes":["shape","speech","understand"],` +
		`"speech":"something moved behind me and I ran",` +
		`"autoTags":["horror","scary","talking"],"topics":["ghost"]}`
	mock.ExpectQuery(regexp.QuoteMeta("FROM challenges")).
		WillReturnRows(analysisRowsWithCreator(stored, "comedy", `["comedy"]`))

	got := readAnalysisRows("challenges", "challenges", 259, 1)
	if len(got) != 1 {
		t.Fatalf("expected one row, got %d", len(got))
	}
	r := got[0]
	if r.MachineCategory != "horror" || r.CreatorCategory != "comedy" {
		t.Errorf("got machine=%q creator=%q, want horror and comedy. Both have "+
			"to survive or the disagreement cannot be counted.",
			r.MachineCategory, r.CreatorCategory)
	}
	if !r.Disputed {
		t.Error("the two answers differ and it is not reported as disputed")
	}
	if r.CategorySource != "machine" {
		t.Errorf("source is %q, want machine — the model examined the video",
			r.CategorySource)
	}
	if len(r.Topics) != 1 || r.Topics[0] != "ghost" {
		t.Errorf("topics came back as %v; they are what actually says what "+
			"the video is about", r.Topics)
	}
}

// ════════════════════════════════════════════════════════════════════════════
// ASKING FOR A COLUMN THAT IS NOT THERE
// ════════════════════════════════════════════════════════════════════════════
//
// This endpoint shipped asking for a column called "tags". No table here has
// one — the creator's tags are in custom_tags. Postgres does not return an
// empty value for a column that does not exist; it refuses the whole query. So
// the endpoint answered null for every video on the platform, which reads
// exactly like "nothing has been analysed yet".
//
// The mock database cannot catch this. It replays whatever rows a test hands
// it and never looks at the column names, so every test above passed against a
// query the real database rejects. These two check the names themselves.

func TestReadAnalysis_AsksChallengesForColumnsThatExist(t *testing.T) {
	cols := analysisCreatorColumns("challenges")

	if strings.Contains(cols, "custom_tags") == false {
		t.Errorf("the creator's tags are read from %q. The column is "+
			"custom_tags — asking for anything else takes down the whole "+
			"listing, not just the tags.", cols)
	}

	// Checked against the schema rather than against a remembered name, so
	// renaming the column in database.go fails here too instead of failing in
	// production.
	schema, err := os.ReadFile("database.go")
	if err != nil {
		t.Fatalf("read database.go: %v", err)
	}
	for _, col := range []string{"custom_tags", "category"} {
		if !strings.Contains(string(schema), "ALTER TABLE challenges ADD COLUMN "+col+" ") {
			t.Errorf("the analysis query asks challenges for %q, but the "+
				"schema in database.go never adds that column to challenges",
				col)
		}
		if !strings.Contains(cols, col) {
			t.Errorf("the analysis query no longer reads %q from challenges", col)
		}
	}
}

func TestReadAnalysis_AsksResponsesForNothingItDoesNotHave(t *testing.T) {
	// challenge_responses has neither column: a response answers somebody
	// else's challenge, so it never carried a category or the responder's own
	// tags. The fragment for that table must be literals only.
	schema, err := os.ReadFile("database.go")
	if err != nil {
		t.Fatalf("read database.go: %v", err)
	}
	cols := analysisCreatorColumns("challenge_responses")
	for _, absent := range []string{"category", "custom_tags", "tags"} {
		if strings.Contains(string(schema), "ALTER TABLE challenge_responses ADD COLUMN "+absent+" ") {
			// Somebody added it. Then reading it is fine and this test is the
			// thing that is out of date.
			continue
		}
		if strings.Contains(cols, absent) {
			t.Errorf("the responses query asks for %q, which that table does "+
				"not have. One missing column returns nothing for every "+
				"response, not an empty value for that one field. Got: %s",
				absent, cols)
		}
	}
}

func TestReadAnalysis_StillWorksForResponses(t *testing.T) {
	// The end-to-end shape of the above: a response comes back with its
	// reading, and with no creator claim to disagree with.
	mock, cleanup := withMockDB(t)
	defer cleanup()

	stored := `{"passes":["understand"],"speech":"watch this","autoTags":["dance"]}`
	mock.ExpectQuery(regexp.QuoteMeta("FROM challenge_responses")).
		WillReturnRows(analysisRowsWithCreator(stored, "", "[]"))

	got := readAnalysisRows("challenge_responses", "responses", 0, 20)
	if len(got) != 1 {
		t.Fatalf("expected one row, got %d", len(got))
	}
	if got[0].Speech != "watch this" {
		t.Errorf("the transcript did not come back; got %q", got[0].Speech)
	}
	if got[0].CreatorCategory != "" {
		t.Errorf("a response reported a creator category of %q; there is no "+
			"column for one", got[0].CreatorCategory)
	}
	if got[0].Disputed {
		t.Error("a response was reported as disputed, but only one side spoke")
	}
}

func TestReadAnalysis_SaysWhenBothSidesAgree(t *testing.T) {
	mock, cleanup := withMockDB(t)
	defer cleanup()

	stored := `{"passes":["understand"],"autoTags":["food"]}`
	mock.ExpectQuery(regexp.QuoteMeta("FROM challenges")).
		WillReturnRows(analysisRowsWithCreator(stored, "food", `["food"]`))

	got := readAnalysisRows("challenges", "challenges", 259, 1)
	if len(got) != 1 {
		t.Fatalf("expected one row, got %d", len(got))
	}
	if got[0].CategorySource != "agreed" {
		t.Errorf("source is %q, want agreed", got[0].CategorySource)
	}
	if got[0].Disputed {
		t.Error("agreement reported as a dispute")
	}
}
