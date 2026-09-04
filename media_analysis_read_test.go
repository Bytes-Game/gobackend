package main

import (
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func analysisRowsFor(raw string) *sqlmock.Rows {
	return sqlmock.NewRows([]string{"id", "created_at", "video_analysis"}).
		AddRow(259, time.Now(), raw)
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
