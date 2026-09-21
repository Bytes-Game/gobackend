package main

// backfill_category_provenance.go — giving the videos that already exist the
// answers the new columns were built to hold.
//
// ════════════════════════════════════════════════════════════════════════════
// TWO JOBS, AND ONLY ONE OF THEM IS FULLY RECOVERABLE
// ════════════════════════════════════════════════════════════════════════════
//
// migrations/010 added creator_category, machine_category and category_source
// and deliberately left them empty. Empty is honest, but it is not finished —
// it leaves every video uploaded before this change permanently describing
// itself worse than the database can already manage.
//
// This closes as much of that gap as the evidence allows, and no more.
//
// ── 1. THE MODEL HAS ALREADY WATCHED THESE VIDEOS ───────────────────────────
//
// This is the big one, and it is not a guess at all.
//
// settleCategory runs when an analysis LANDS. Every video analysed before it
// existed never got one — and those videos are not missing the evidence, they
// are sitting on it. Their auto_tags column holds what the model concluded,
// written by the worker months ago. Nothing ever turned that into a category.
//
// So for those rows the answer is already in the database and just has to be
// carried the last few inches. Recomputed with the same function the ranker
// uses, from data already stored, so the stored answer cannot disagree with
// the ranked one.
//
// ── 2. WHAT THE CREATOR SAID, WHERE IT CAN BE PROVEN ────────────────────────
//
// This one is mostly NOT recoverable, and pretending otherwise would put back
// the exact fault this work removes.
//
// The old app always sent a category — its dropdown defaulted to "other" — so
// a stored category came from one of exactly two places:
//
//	 a. the creator opened the dropdown and chose            (a real claim)
//	 b. the server derived it: their tags, else keywords      (not a claim)
//
// Nothing recorded which. But (b) is code, and code can be re-run: if the
// server's own derivation would have produced something DIFFERENT from what
// is stored, then the stored value cannot have come from (b), so it came from
// (a). That is elimination, not inference.
//
// ⚠ Only the TAGS half of the derivation is used, never the keyword half.
//
// categoryFromTags is a stable lookup against a fixed list. inferCategory is
// not: its keyword table has been corrected at least once — four of its
// answers were not categories at all, and ten real categories had no keywords
// and were unreachable. Re-running today's table over a row written under the
// old one would produce a different answer for reasons that have nothing to
// do with the creator, and every such row would be recorded as a human claim
// that no human ever made. That is worse than leaving the column empty, and
// it is worse at scale, silently, across the whole history.
//
// So a row only counts as proven when its tags name a category and the stored
// category is a different one. Then step (b) short-circuits at the tags and
// the keyword table never gets a look-in, whatever it said at the time.
//
// Everything else stays empty. Not because it is hard, but because the honest
// answer to "who said this" is "we do not know", and a column that means "a
// person said so" must never hold anything else.

import (
	"fmt"
	"log"
)

// backfillVersion is recorded in schema_migrations so this runs exactly once.
// The "(go)" is deliberate: it is not a file in migrations/, and somebody
// reading that table should not go looking for one.
const backfillVersion = "010b_category_provenance (go)"

// backfillCategoryProvenance fills in what can be established for videos that
// existed before the provenance columns did.
//
// Best effort. A failure here leaves old rows exactly as migrations/010 left
// them, which is the state the app already handles — never a reason to stop a
// deploy, unlike a schema change.
func backfillCategoryProvenance() {
	if db == nil {
		return
	}

	var done bool
	if err := db.QueryRow(
		`SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version = $1)`,
		backfillVersion).Scan(&done); err != nil {
		queryFailed("category backfill: could not check whether it has already run",
			"skipping it, so old videos keep the category upload gave them", err)
		return
	}
	if done {
		return
	}

	total := 0
	for _, table := range []string{"challenges", "challenge_responses"} {
		n, err := backfillOneTable(table)
		if err != nil {
			// Say what was reached. A half-finished backfill is safe — it is
			// row-at-a-time and idempotent — but it must NOT be recorded as
			// done, or the rest is never picked up.
			log.Printf("category backfill: stopped partway through %s after "+
				"%d rows: %v — not recording it, so the next boot finishes "+
				"the job", table, n, err)
			return
		}
		total += n
	}

	if _, err := db.Exec(
		`INSERT INTO schema_migrations (version, checksum) VALUES ($1, '')
		 ON CONFLICT (version) DO NOTHING`, backfillVersion); err != nil {
		queryFailed("category backfill: finished but could not record it",
			"it will run again on the next boot, which is wasteful but harmless", err)
		return
	}
	if total > 0 {
		log.Printf("category backfill: settled %d video(s) that were analysed "+
			"before their answer had anywhere to go", total)
	}
}

// backfillOneTable walks the rows that still have nothing recorded and gives
// them whatever can be established. Returns how many rows it changed.
func backfillOneTable(table string) (int, error) {
	// Only rows with something to gain: nothing recorded about where their
	// category came from, and either words from the model to read or a
	// creator's tags to check against.
	//
	// subject and prefix are literals on the responses table, which has
	// neither — see the same shape in settleCategory.
	sel := `SELECT id, COALESCE(category,''), COALESCE(custom_tags::text,'[]'),
	               COALESCE(auto_tags::text,'[]'), COALESCE(video_analysis::text,''),
	               '', ''
	          FROM challenge_responses
	         WHERE category_source = '' AND creator_category = '' AND machine_category = ''`
	if table == "challenges" {
		sel = `SELECT id, COALESCE(category,''), COALESCE(custom_tags::text,'[]'),
		              COALESCE(auto_tags::text,'[]'), COALESCE(video_analysis::text,''),
		              COALESCE(subject,''), COALESCE(prefix,'')
		         FROM challenges
		        WHERE category_source = '' AND creator_category = '' AND machine_category = ''`
	}

	rows, err := db.Query(sel)
	if err != nil {
		return 0, fmt.Errorf("reading %s: %w", table, err)
	}

	type pending struct {
		id                       int
		creator, machine, source string
		category                 string
	}
	var work []pending
	scanErrors := 0

	for rows.Next() {
		var id int
		var storedCategory, creatorTagsJSON, autoTagsJSON, analysisJSON, subject, prefix string
		if scanFailed(fmt.Sprintf("category backfill: a %s row", table),
			rows.Scan(&id, &storedCategory, &creatorTagsJSON, &autoTagsJSON,
				&analysisJSON, &subject, &prefix), &scanErrors) {
			continue
		}

		var creatorTags, machineTags []string
		_ = jsonUnmarshalQuiet([]byte(creatorTagsJSON), &creatorTags)
		_ = jsonUnmarshalQuiet([]byte(autoTagsJSON), &machineTags)
		creatorTags = normalizeTags(creatorTags)
		machineTags = normalizeTags(machineTags)

		// ── 2. the creator's word, only where it is PROVEN ──────────────────
		proven := provenCreatorPick(storedCategory, creatorTags)

		// ── 1. the model's answer, from what it already wrote down ──────────
		var analysis VideoAnalysis
		if analysisJSON != "" {
			_ = jsonUnmarshalQuiet([]byte(analysisJSON), &analysis)
		}
		v := categoryFromEvidence(machineTags, creatorTags, proven,
			subject, prefix, analysisText(&analysis))

		// Nothing established either way — leave the row alone rather than
		// writing a source for a decision nobody made.
		if proven == "" && v.Machine == "" {
			continue
		}

		p := pending{id: id, creator: proven, machine: v.Machine, category: storedCategory}
		if v.Machine != "" {
			p.category, p.source = v.Category, v.Source
		} else {
			// The creator's pick is now known, but the model still has no
			// opinion. The stored category stands; only its provenance
			// changes, and only when the two actually agree.
			if proven == storedCategory {
				p.source = "creator"
			} else {
				p.source = "guess"
			}
		}
		work = append(work, p)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return 0, fmt.Errorf("walking %s: %w", table, err)
	}
	if err := rows.Close(); err != nil {
		return 0, fmt.Errorf("closing %s: %w", table, err)
	}

	// Written after the read finishes, not during it: updating a table while
	// walking a cursor over it is how a backfill starts skipping rows.
	changed := 0
	for _, p := range work {
		// Every parameter cast. $3 is compared AND assigned, and Postgres
		// deduces a different type for each use and refuses the whole
		// statement — see the same note in settleCategory, where it cost a
		// round.
		if _, err := db.Exec(
			`UPDATE `+table+`
			    SET creator_category = $2::text,
			        machine_category = $3::text,
			        category         = $4::text,
			        category_source  = $5::text
			  WHERE id = $1`,
			p.id, p.creator, p.machine, p.category, p.source,
		); err != nil {
			return changed, fmt.Errorf("updating %s id=%d: %w", table, p.id, err)
		}
		changed++
	}
	return changed, nil
}

// provenCreatorPick returns the stored category when it can only have come
// from a person, and "" otherwise.
//
// The rule, in one line: if the creator's own tags name a category and the
// stored category is a DIFFERENT one, the server's derivation could not have
// produced what is stored, so the creator chose it.
//
// Everything else — no tags, tags that name nothing, tags that agree — stays
// unproven. Those are the rows where the keyword half of the derivation might
// have decided, and that half is not safe to re-run. See the file header.
func provenCreatorPick(storedCategory string, creatorTags []string) string {
	stored := usableCategory(storedCategory)
	if stored == "" {
		return "" // 'other', 'general' or empty: nobody claimed anything
	}
	fromTags := categoryFromTags(creatorTags)
	if fromTags == "" || fromTags == stored {
		return ""
	}
	return stored
}
