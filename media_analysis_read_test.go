package main

import (
	"os"
	"path/filepath"
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

// schemaAddsColumn reports whether this repo's schema adds col to table.
//
// It looks in BOTH places the schema is written, which is the whole point of
// it existing. database.go re-runs its ADD COLUMN statements on every boot;
// migrations/ holds the numbered files applied once each. A check that reads
// only the first is blind to half the schema, and the half it cannot see is
// the newer half — video_analysis, auto_tags, content_topics and every column
// migration 008 adds all live in files this used to ignore.
//
// One ALTER TABLE statement at a time, up to its semicolon, so a column added
// to challenges is never mistaken for one added to challenge_responses.
func schemaAddsColumn(t *testing.T, table, col string) bool {
	t.Helper()
	sources := []string{}
	b, err := os.ReadFile("database.go")
	if err != nil {
		t.Fatalf("read database.go: %v", err)
	}
	sources = append(sources, string(b))
	files, err := filepath.Glob("migrations/*.sql")
	if err != nil {
		t.Fatalf("list migrations: %v", err)
	}
	if len(files) == 0 {
		t.Fatal("no migrations found — this check would silently pass for " +
			"every column added by one")
	}
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatalf("read %s: %v", f, err)
		}
		sources = append(sources, string(b))
	}

	stmt := regexp.MustCompile(`(?is)ALTER TABLE\s+` + regexp.QuoteMeta(table) + `\b[^;]*;`)
	add := regexp.MustCompile(`(?is)ADD COLUMN\s+(?:IF NOT EXISTS\s+)?` + regexp.QuoteMeta(col) + `\b`)
	for _, src := range sources {
		for _, one := range stmt.FindAllString(src, -1) {
			if add.MatchString(one) {
				return true
			}
		}
	}
	return false
}

// The helper has to be able to say no, or every check built on it passes for
// a column that was never added anywhere.
func TestSchemaAddsColumn_SaysNoForSomethingNobodyAdded(t *testing.T) {
	if schemaAddsColumn(t, "challenges", "not_a_real_column_xyz") {
		t.Error("the schema check found a column that does not exist, so it " +
			"cannot be trusted to notice a missing one either")
	}
	// And it must not confuse the two tables. prefix is on challenges only.
	if schemaAddsColumn(t, "challenge_responses", "prefix") {
		t.Error("a column on challenges was credited to challenge_responses")
	}
}

func TestReadAnalysis_AsksChallengesForColumnsThatExist(t *testing.T) {
	cols := analysisCreatorColumns("challenges")

	if !strings.Contains(cols, "custom_tags") {
		t.Errorf("the creator's tags are read from %q. The column is "+
			"custom_tags — asking for anything else takes down the whole "+
			"listing, not just the tags.", cols)
	}

	// Checked against the schema rather than against a remembered name, so
	// renaming the column fails here too instead of failing in production.
	for _, col := range []string{"custom_tags", "category"} {
		if !schemaAddsColumn(t, "challenges", col) {
			t.Errorf("the analysis query asks challenges for %q, but nothing "+
				"in the schema adds that column to challenges", col)
		}
		if !strings.Contains(cols, col) {
			t.Errorf("the analysis query no longer reads %q from challenges", col)
		}
	}
}

// The mirror of the test above, and it used to be its opposite.
//
// While challenge_responses had no creator columns this asserted that the
// query asked for NEITHER. That was right at the time and it is wrong now:
// migration 008 added both, and a response whose creator claim is not read
// comes back looking like a response whose creator made no claim. The endpoint
// exists to compare the two sides, so reporting one of them as silent when it
// is not is the one failure that matters here.
func TestReadAnalysis_AsksResponsesForTheCreatorColumnsToo(t *testing.T) {
	cols := analysisCreatorColumns("challenge_responses")

	for _, col := range []string{"custom_tags", "category"} {
		if !schemaAddsColumn(t, "challenge_responses", col) {
			t.Fatalf("challenge_responses has no %q column in the schema. "+
				"Reading one that is not there returns nothing for every "+
				"response, not an empty value for that field.", col)
		}
		if !strings.Contains(cols, col) {
			t.Errorf("the analysis query does not read %q from "+
				"challenge_responses, so every answer in the app reports no "+
				"creator claim and nothing disputed. Got: %s", col, cols)
		}
	}
}

// A table this function has never heard of must be answered with literals.
//
// table is picked by hlsTableForKind from a fixed pair, so this cannot happen
// today. It is pinned because the cost of guessing wrong is not a wrong field,
// it is an empty listing: Postgres refuses the whole statement over one column
// name it does not recognise.
func TestReadAnalysis_GuessesNoColumnsForAnUnknownTable(t *testing.T) {
	cols := analysisCreatorColumns("posts")
	if strings.Contains(cols, "category") || strings.Contains(cols, "custom_tags") {
		t.Errorf("named a column for a table this function does not know "+
			"about; got %s", cols)
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
		t.Errorf("a response with an empty category column reported a creator "+
			"category of %q", got[0].CreatorCategory)
	}
	if got[0].Disputed {
		t.Error("a response was reported as disputed, but only one side spoke")
	}
}

// A response can now disagree with the model, and saying so is the entire
// reason this endpoint exists.
//
// Before migration 008 this was unreachable: the query handed the row two
// literals, so every answer in the app came back with no creator claim and
// Disputed false. That is the same output as genuine agreement, which made
// half the catalogue silently unanswerable to the one question being asked.
func TestReadAnalysis_AResponseCanDisagreeWithTheModel(t *testing.T) {
	mock, cleanup := withMockDB(t)
	defer cleanup()

	stored := `{"passes":["understand"],"autoTags":["dance"]}`
	mock.ExpectQuery(regexp.QuoteMeta("FROM challenge_responses")).
		WillReturnRows(analysisRowsWithCreator(stored, "comedy", `["comedy"]`))

	got := readAnalysisRows("challenge_responses", "responses", 0, 20)
	if len(got) != 1 {
		t.Fatalf("expected one row, got %d", len(got))
	}
	if got[0].CreatorCategory != "comedy" {
		t.Errorf("the responder said comedy; the endpoint reported %q",
			got[0].CreatorCategory)
	}
	if got[0].MachineCategory != "dance" {
		t.Errorf("the model said dance; the endpoint reported %q",
			got[0].MachineCategory)
	}
	if !got[0].Disputed {
		t.Error("the responder said comedy and the model said dance, and the " +
			"endpoint reported no disagreement")
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
