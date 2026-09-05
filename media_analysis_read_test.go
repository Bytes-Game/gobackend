package main

import (
	"regexp"
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
