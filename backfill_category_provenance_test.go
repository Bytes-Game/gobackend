package main

// The videos that already exist.
//
// migrations/010 added three columns and left them empty. Empty is honest but
// it is not finished — every video uploaded before that change would carry an
// upload-time guess forever, including the ones the model had already watched.
//
// Two very different jobs, and these tests mostly exist to hold the line
// between them:
//
//   - what the MODEL concluded is fully recoverable. It is sitting in
//     auto_tags, written by the worker months ago, and nothing ever turned it
//     into a category. Carrying it the last few inches is not a guess.
//
//   - what the CREATOR said mostly is NOT recoverable, and most of the length
//     below is spent proving that this code refuses to invent it.

import (
	"strings"
	"testing"
)

// ── the rule, on its own ────────────────────────────────────────────────────

func TestProvenCreatorPick_ClaimsOnlyWhatItCanProve(t *testing.T) {
	cases := []struct {
		name   string
		stored string
		tags   []string
		want   string
		why    string
	}{
		{
			name: "tags say one thing, the row says another",
			// The only provable case. The server's derivation checks tags
			// FIRST, so if it had decided this row it would have said comedy.
			// It says food. Nothing but a person could have put that there.
			stored: "food", tags: []string{"comedy", "roast"}, want: "food",
			why: "the derivation would have stopped at the tags and said comedy",
		},
		{
			name:   "tags agree with the row",
			stored: "food", tags: []string{"food"}, want: "",
			why: "the derivation would have produced exactly this, so it proves nothing",
		},
		{
			name:   "no tags at all",
			stored: "food", tags: nil, want: "",
			why: "with no tags the keyword half decided, and that half's rules " +
				"have changed since — re-running them would invent claims",
		},
		{
			name:   "tags that name no category",
			stored: "food", tags: []string{"hyderabad", "street"}, want: "",
			why: "same as no tags: the keyword half decided",
		},
		{
			name:   "the row says other",
			stored: "other", tags: []string{"comedy"}, want: "",
			why: "'other' is what gets stored when nobody chose",
		},
		{
			name:   "the row says general",
			stored: "general", tags: []string{"comedy"}, want: "",
			why: "same shrug, different word",
		},
		{
			name:   "the row says nothing",
			stored: "", tags: []string{"comedy"}, want: "",
			why: "nothing stored, nothing to attribute",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := provenCreatorPick(c.stored, normalizeTags(c.tags))
			if got != c.want {
				t.Errorf("provenCreatorPick(%q, %v) = %q, want %q — %s",
					c.stored, c.tags, got, c.want, c.why)
			}
		})
	}
}

func TestProvenCreatorPick_NeverRunsTheKeywordHalf(t *testing.T) {
	// The whole safety argument in one check.
	//
	// "cooking biryani" is a subject the keyword matcher reads as food. If
	// this function consulted the keyword half at all, a row storing anything
	// other than food would come back "proven" — and every such row in the
	// history would be recorded as a claim by a person who never made one.
	//
	// It has no way to reach that code, and this is the test that says so: no
	// subject is even passed in.
	if got := provenCreatorPick("dance", nil); got != "" {
		t.Errorf("claimed %q from a row with no tags. The only thing that "+
			"could have decided such a row is the keyword table, whose rules "+
			"have been corrected since those rows were written.", got)
	}
}

// ── against a real database ─────────────────────────────────────────────────

// TestBackfill_SettlesVideosTheModelAlreadyWatched is the test paired with
// backfillOneTable in sql_compiles_test.go's runByATest. It RUNS both of that
// function's statements, on both tables, because neither can be read out of
// the source: the table name is chosen at runtime.
func TestBackfill_SettlesVideosTheModelAlreadyWatched(t *testing.T) {
	defer withDB(t)()
	cid, rid := runtimeFixture(t)

	for _, table := range []string{"challenges", "challenge_responses"} {
		id := cid
		if table == "challenge_responses" {
			id = rid
		}

		t.Run(table+"/the model had already decided", func(t *testing.T) {
			// Exactly the shape of a video from before this change: the
			// worker watched it and wrote auto_tags, and the category column
			// still holds whatever upload guessed.
			if _, err := db.Exec(`
				UPDATE `+table+`
				   SET category = 'comedy', creator_category = '',
				       machine_category = '', category_source = '',
				       custom_tags = '[]'::jsonb,
				       auto_tags = '["dance"]'::jsonb,
				       video_analysis = '{"passes":["frames"],"topics":["floor work"]}'::jsonb
				 WHERE id = $1`, id); err != nil {
				t.Fatalf("seed: %v", err)
			}

			if _, err := backfillOneTable(table); err != nil {
				t.Fatalf("backfill %s: %v", table, err)
			}

			cat, machine, source, creator := readCategoryColumns(t, table, id)
			if machine != "dance" {
				t.Errorf("machine_category = %q, want dance. The model's "+
					"answer has been sitting in auto_tags this whole time "+
					"and nothing carried it across.", machine)
			}
			if cat != "dance" {
				t.Errorf("category = %q, want dance", cat)
			}
			if source != "machine" {
				t.Errorf("category_source = %q, want machine", source)
			}
			if creator != "" {
				t.Errorf("creator_category = %q — nobody said anything about "+
					"this video, and this backfill may never invent that",
					creator)
			}
		})

		t.Run(table+"/nothing to go on is left alone", func(t *testing.T) {
			// No machine tags, no creator tags. Neither half of the job has
			// any evidence, so the row must come out exactly as it went in.
			if _, err := db.Exec(`
				UPDATE `+table+`
				   SET category = 'comedy', creator_category = '',
				       machine_category = '', category_source = '',
				       custom_tags = '[]'::jsonb, auto_tags = '[]'::jsonb,
				       video_analysis = NULL
				 WHERE id = $1`, id); err != nil {
				t.Fatalf("seed: %v", err)
			}

			if _, err := backfillOneTable(table); err != nil {
				t.Fatalf("backfill %s: %v", table, err)
			}

			cat, machine, source, creator := readCategoryColumns(t, table, id)
			if cat != "comedy" || machine != "" || source != "" || creator != "" {
				t.Errorf("a row with no evidence came back as "+
					"category=%q machine=%q source=%q creator=%q — it should "+
					"be untouched. Writing a source for a decision nobody "+
					"made is the fault this whole change removes.",
					cat, machine, source, creator)
			}
		})

		t.Run(table+"/a creator pick that can be proven", func(t *testing.T) {
			// Tags say comedy, the row says food. The server's derivation
			// checks tags first, so it would have said comedy. A person put
			// food there.
			if _, err := db.Exec(`
				UPDATE `+table+`
				   SET category = 'food', creator_category = '',
				       machine_category = '', category_source = '',
				       custom_tags = '["comedy"]'::jsonb, auto_tags = '[]'::jsonb,
				       video_analysis = NULL
				 WHERE id = $1`, id); err != nil {
				t.Fatalf("seed: %v", err)
			}

			if _, err := backfillOneTable(table); err != nil {
				t.Fatalf("backfill %s: %v", table, err)
			}

			cat, _, source, creator := readCategoryColumns(t, table, id)
			if creator != "food" {
				t.Errorf("creator_category = %q, want food. This is the one "+
					"case where a person's pick CAN be proven, and refusing "+
					"to record it throws away real information.", creator)
			}
			if cat != "food" || source != "creator" {
				t.Errorf("category=%q source=%q, want food/creator", cat, source)
			}
		})
	}
}

func TestBackfill_RunsOnlyOnce(t *testing.T) {
	defer withDB(t)()

	// No need to clear the marker here: withDB does it for every test now,
	// via forgetOneTimeBackfills. This test used to do it by hand, and that
	// was the clue the default was wrong — a test that has to undo its own
	// setup before it can assert anything is a setup that disarms assertions.

	// First run records itself.
	backfillCategoryProvenance()
	var recorded bool
	if err := db.QueryRow(
		`SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version = $1)`,
		backfillVersion).Scan(&recorded); err != nil {
		t.Fatalf("read schema_migrations: %v", err)
	}
	if !recorded {
		t.Fatal("the backfill did not record itself, so it would walk every " +
			"video in the database again on every single boot")
	}

	// Second run must be a no-op. Proven by giving it work it WOULD do if it
	// ran, and checking it does not.
	cid, _ := runtimeFixture(t)
	if _, err := db.Exec(`
		UPDATE challenges
		   SET category = 'comedy', creator_category = '', machine_category = '',
		       category_source = '', auto_tags = '["dance"]'::jsonb
		 WHERE id = $1`, cid); err != nil {
		t.Fatalf("seed: %v", err)
	}

	backfillCategoryProvenance()

	_, machine, _, _ := readCategoryColumns(t, "challenges", cid)
	if machine != "" {
		t.Errorf("machine_category = %q — the backfill ran a second time. "+
			"It is recorded in schema_migrations precisely so it does not.",
			machine)
	}
}

// TestBackfill_IsActuallyCalledAtBoot is the wire check.
//
// Every other test in this file calls the backfill BY HAND, so all of them
// keep passing if boot stops calling it — which is the most common bug in
// this repo and the reason CLAUDE.md says to cut the wire and re-run. Cutting
// it left every test green, so this one exists.
//
// Source-read, with comments stripped, because boot is not reachable from a
// test: runMigrations() is what withDB itself uses to build the schema.
func TestBackfill_IsActuallyCalledAtBoot(t *testing.T) {
	src := codeWithoutComments(readSourceFile(t, "database.go"))
	if !strings.Contains(src, "backfillCategoryProvenance()") {
		t.Error("nothing calls backfillCategoryProvenance at boot, so every " +
			"video that existed before the provenance columns did keeps the " +
			"category upload guessed for it, forever — and every other test " +
			"in this file still passes, because they all call it by hand")
	}
}

// TestWithDB_StartsWithTheBackfillsUnRun is the guard on the guard.
//
// forgetOneTimeBackfills is what makes every other backfill test in this repo
// mean anything, and nothing else would notice if it were deleted: the tests
// it protects would all go back to passing, quietly, against broken code.
// That is the shape it exists to stop, so it needs a check of its own.
//
// Deliberately asserts on the STATE a test body starts in, not on the source,
// so it also catches the reset being moved, reordered after something that
// re-records, or narrowed to the wrong version string.
func TestWithDB_StartsWithTheBackfillsUnRun(t *testing.T) {
	defer withDB(t)()

	var stillRecorded []string
	rows, err := db.Query(
		`SELECT version FROM schema_migrations WHERE version LIKE '%(go)'`)
	if err != nil {
		t.Fatalf("read schema_migrations: %v", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			t.Fatalf("scan: %v", err)
		}
		stillRecorded = append(stillRecorded, v)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("walking schema_migrations: %v", err)
	}

	if len(stillRecorded) > 0 {
		t.Errorf("a test body starts with these backfills already marked as "+
			"run: %v\n\nThat makes every assertion about them meaningless — "+
			"seed rows and the backfill will not touch them, check that it "+
			"recorded itself and it passes with the recording deleted. "+
			"withDB must clear these; see forgetOneTimeBackfills.",
			stillRecorded)
	}

	// And the PRESENCE half. Everything above checks that something is NOT
	// there, and a reset widened to `DELETE FROM schema_migrations` with no
	// WHERE would sail through all of it — while making every versioned
	// migration re-run on every single test.
	//
	// So: the .sql migrations must still be recorded. Only the Go backfills
	// are the reset's business.
	var sqlMigrations int
	if err := db.QueryRow(
		`SELECT COUNT(*) FROM schema_migrations WHERE version NOT LIKE '%(go)'`,
	).Scan(&sqlMigrations); err != nil {
		t.Fatalf("count the versioned migrations: %v", err)
	}
	if sqlMigrations == 0 {
		t.Error("the versioned .sql migrations are no longer recorded as " +
			"applied. forgetOneTimeBackfills is meant to clear the Go " +
			"backfills and nothing else — widened to everything, it makes " +
			"every migration re-run on every test, and the absence check " +
			"above would not notice.")
	}
}
