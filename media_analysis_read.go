package main

// media_analysis_read.go — reading back what the worker heard and read.
//
// ════════════════════════════════════════════════════════════════════════════
// WHY THIS EXISTS
// ════════════════════════════════════════════════════════════════════════════
//
// The worker transcribes speech, reads on-screen text, and stores both. Then
// nothing could look at them. The workflow log prints a word COUNT and never
// the words; no endpoint returned the column; the only reader was the ranker,
// deep inside a scoring query.
//
// So "is the speech pass any good?" was unanswerable. It stayed unanswerable
// through a whole model change: switching off the English-only model moved
// the first two uploads from the one-to-six words that model managed to 32
// and 39 — plainly better, and still nobody could see whether those 32 words
// were a sentence or noise.
//
// This is that missing window. It changes nothing about how anything is
// stored or ranked; it only lets a person read what is already there.
//
// ════════════════════════════════════════════════════════════════════════════
// WHY ADMIN-ONLY, AND NOT A LOG LINE
// ════════════════════════════════════════════════════════════════════════════
//
// The obvious fix is one log.Printf in the worker. It is the wrong fix.
//
// A transcript is what somebody said out loud into their camera. Worker logs
// live in GitHub Actions, readable by anyone with access to the repository,
// kept for months, and searchable. That is a worse home for a person's speech
// than the database it already sits in.
//
// Behind the admin login it goes to whoever already administers the app, is
// not retained anywhere new, and leaves a request trail. Same door as the
// re-queue endpoint.

import (
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// analysisReadDefaultLimit is what a request that names no size gets. Enough
// to read a session's worth of uploads in one go.
const analysisReadDefaultLimit = 20

// analysisReadMaxLimit bounds one request, so a mistyped number cannot pull
// every transcript on the platform into one response.
const analysisReadMaxLimit = 100

type analysisRow struct {
	ID       int      `json:"id"`
	Kind     string   `json:"kind"`
	Created  string   `json:"createdAt"`
	Passes   []string `json:"passes,omitempty"`
	Speech   string   `json:"speech,omitempty"`
	Screen   string   `json:"screenText,omitempty"`
	AutoTags []string `json:"autoTags,omitempty"`

	// ── WHO SAYS WHAT THIS VIDEO IS ──────────────────────────────────────
	//
	// The whole point of surfacing these together. The machine now outranks
	// the creator when it has an opinion, and the only way to know whether
	// that is the right call is to see how often the two disagree and on
	// what. Counting that needs both answers side by side, which nothing
	// else in the app shows.
	CreatorCategory string `json:"creatorCategory,omitempty"`
	MachineCategory string `json:"machineCategory,omitempty"`
	// Which one won, and whether the other was overruled: "agreed",
	// "machine", "creator" or "guess".
	CategorySource string `json:"categorySource,omitempty"`
	Disputed       bool   `json:"categoryDisputed,omitempty"`
	// The free-form description, which is what actually says what a video is
	// about. Surfaced here because it is the only way to see whether the
	// model understood a video — auto_tags can only ever say one of eighteen
	// things.
	Topics []string `json:"topics,omitempty"`
	// The counts the workflow log prints, so a row here can be lined up
	// against the run that produced it without counting words by hand.
	SpeechWords     int `json:"speechWords"`
	ScreenTextChars int `json:"screenTextChars"`
}

// AdminReadAnalysisHandler returns what the worker heard and read.
//
// GET /api/v1/admin/media/analysis?kind=challenges&id=259
// GET /api/v1/admin/media/analysis?kind=responses&limit=20
//
// Newest first. Rows the worker never analysed are left out — an empty
// column is not a reading, and padding the list with them buries the ones
// worth looking at.
func AdminReadAnalysisHandler(w http.ResponseWriter, r *http.Request) {
	if db == nil {
		http.Error(w, "no database", http.StatusServiceUnavailable)
		return
	}
	kind := r.URL.Query().Get("kind")
	if kind == "" {
		kind = "challenges"
	}
	table := hlsTableForKind(kind)
	if kind == "responses" {
		table = "challenge_responses"
	}

	// A single id wins over the listing — asking about one video is the
	// common case after an upload.
	if raw := r.URL.Query().Get("id"); raw != "" {
		id, err := strconv.Atoi(raw)
		if err != nil {
			http.Error(w, "id must be a number", http.StatusBadRequest)
			return
		}
		writeJSON(w, http.StatusOK, readAnalysisRows(table, kind, id, 1))
		return
	}

	limit := analysisReadDefaultLimit
	if raw := r.URL.Query().Get("limit"); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 {
			limit = n
		}
	}
	if limit > analysisReadMaxLimit {
		limit = analysisReadMaxLimit
	}
	writeJSON(w, http.StatusOK, readAnalysisRows(table, kind, 0, limit))
}

// analysisCreatorColumns names the two things the creator said about their own
// video: the category they picked and the tags they typed.
//
// Both live only on challenges. A response is somebody's answer to another
// person's challenge, so it never had a category or tags of its own, and those
// columns were never added to its table.
//
// This is a function rather than two words inside the query because getting a
// column name wrong here is not a small mistake. Postgres does not hand back an
// empty value for a column that does not exist — it refuses the whole
// statement. So a single wrong name returns nothing for every video at once,
// and the endpoint looks like "nothing has been analysed yet" rather than like
// a broken query. That is exactly what happened: this asked for a column called
// "tags", the creator's tags are in custom_tags, and the endpoint answered null
// for the entire catalogue.
func analysisCreatorColumns(table string) string {
	if table == "challenges" {
		return `COALESCE(category, ''), COALESCE(custom_tags::text, '[]')`
	}
	// Literals, not columns. Nothing to read, so nothing to ask for.
	return `'', '[]'`
}

// readAnalysisRows pulls stored analyses, newest first. id > 0 asks for one.
func readAnalysisRows(table, kind string, id, limit int) []analysisRow {
	where := `WHERE video_analysis IS NOT NULL`
	args := []interface{}{limit}
	if id > 0 {
		where += ` AND id = $2`
		args = append(args, id)
	}
	rows, err := db.Query(`
		SELECT id, created_at, video_analysis::text,
		       `+analysisCreatorColumns(table)+`
		  FROM `+table+`
		  `+where+`
		 ORDER BY created_at DESC
		 LIMIT $1`, args...)
	if err != nil {
		log.Printf("admin analysis read: %s: %v", table, err)
		return nil
	}
	defer rows.Close()

	out := []analysisRow{}
	for rows.Next() {
		var rowID int
		var created time.Time
		var raw, dbCategory, tagsJSON string
		if err := rows.Scan(&rowID, &created, &raw, &dbCategory, &tagsJSON); err != nil {
			continue
		}
		var creatorTags []string
		_ = json.Unmarshal([]byte(tagsJSON), &creatorTags)
		var a VideoAnalysis
		if err := json.Unmarshal([]byte(raw), &a); err != nil {
			// Keep the row. That the stored blob is unreadable is itself
			// the answer somebody is looking for, and dropping it silently
			// would make a corrupt reading look like a missing one.
			log.Printf("admin analysis read: %s id=%d is unreadable: %v", table, rowID, err)
			out = append(out, analysisRow{ID: rowID, Kind: kind})
			continue
		}
		// The same decision the ranker makes, so this view shows what
		// actually happens rather than a second opinion about it.
		//
		// Named catVerdict because `verdict` is already a function in this
		// package — shadowing it here compiles and then confuses whoever
		// reads it next.
		catVerdict := categoryFromEvidence(
			normalizeTags(a.AutoTags), normalizeTags(creatorTags),
			dbCategory, "", "", analysisText(&a))
		out = append(out, analysisRow{
			ID:              rowID,
			Kind:            kind,
			Created:         created.UTC().Format(time.RFC3339),
			Passes:          a.Passes,
			Speech:          a.Speech,
			Screen:          a.ScreenText,
			AutoTags:        a.AutoTags,
			CreatorCategory: catVerdict.Creator,
			MachineCategory: catVerdict.Machine,
			CategorySource:  catVerdict.Source,
			Disputed:        catVerdict.Disputed(),
			Topics:          a.Topics,
			SpeechWords:     countWords(a.Speech),
			ScreenTextChars: len(a.ScreenText),
		})
	}
	return out
}

// countWords is the same measure the workflow log prints, so a row here
// lines up with the run that produced it.
func countWords(s string) int { return len(strings.Fields(s)) }
