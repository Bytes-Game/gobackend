package main

// video_analysis.go — the backend half of "what is this video".
//
// The transcode worker inspects every upload while it has the file (see
// cmd/hls-worker/analyze.go) and sends the result back with the manifest.
// This is where that result is stored and turned into something the ranker
// can use.
//
// ════════════════════════════════════════════════════════════════════════════
// WHY MACHINE TAGS ARE KEPT APART FROM CREATOR TAGS
// ════════════════════════════════════════════════════════════════════════════
//
// They live in different columns and they are merged only at read time.
//
// Merging them at write time would be simpler and wrong twice over. A creator
// editing their tags would silently wipe whatever the machine found, because
// the two would be indistinguishable in one column. And the machine's guesses
// would appear on the creator's own tag list, which is putting words in
// somebody's mouth — the creator's tags are shown back to them, so anything
// stored there has to be theirs.
//
// Read together, written apart.
//
// ════════════════════════════════════════════════════════════════════════════
// WHAT IT IS USED FOR
// ════════════════════════════════════════════════════════════════════════════
//
//	CATEGORY   — a machine tag naming a category is a weaker claim than a
//	             creator's tag, so it is consulted after theirs and before the
//	             keyword guess. Real, but never louder than the person who
//	             made the thing.
//	EMBEDDING  — machine tags become features like any other, so a video whose
//	             on-screen caption says "recipe" sits near other cooking
//	             videos even if nobody tagged it.
//	ENERGY     — how fast a video cuts and how much of it is not silence is a
//	             genuine, measured statement about how stimulating it is,
//	             where the existing energy signal is inferred from engagement.
//
// Everything here is optional at every step. A worker with no OCR binary
// sends no screen text; a worker predating this sends no analysis at all;
// a video that was uploaded before any of it exists has NULL. Readers must
// treat missing as "not measured", never as "measured as zero" — a video with
// no reading is not a silent, still, dark video.

import (
	"encoding/json"
	"fmt"
	"log"
	"math"
	"strconv"
	"strings"
)

// VideoAnalysis mirrors the worker's struct. Kept as its own declaration
// rather than shared, because the worker is a separate binary that is
// deployed independently: a shared type would force the two to move together,
// and the whole point of the wire format being JSON is that they do not have
// to.
type VideoAnalysis struct {
	CutsPerMinute float64  `json:"cutsPerMinute,omitempty"`
	MotionScore   float64  `json:"motionScore,omitempty"`
	Loudness      float64  `json:"loudness,omitempty"`
	SpeechRatio   float64  `json:"speechRatio,omitempty"`
	Brightness    float64  `json:"brightness,omitempty"`
	ScreenText    string   `json:"screenText,omitempty"`
	Speech        string   `json:"speech,omitempty"`
	AutoTags      []string `json:"autoTags,omitempty"`

	// What the video is about in the model's own words — "hanuman chalisa",
	// "street food", "long distance relationship". Free-form and deliberately
	// separate from AutoTags: nothing ranks on these, so they can describe a
	// video far more precisely than the eighteen categories can. Written by
	// the worker's understanding pass; see cmd/hls-worker/understand.go.
	Topics []string `json:"topics,omitempty"`

	Passes []string `json:"passes,omitempty"`
}

// Measured reports whether anything was actually looked at.
func (v *VideoAnalysis) Measured() bool { return v != nil && len(v.Passes) > 0 }

// storeVideoAnalysis saves a worker's reading against one row.
//
// Never returns an error, by design — see the call site. Losing a reading is
// cheap; making the worker re-download and re-transcode a finished video to
// retry saving one is not.
func storeVideoAnalysis(table string, id int, raw json.RawMessage) {
	if db == nil || len(raw) == 0 {
		return
	}
	var a VideoAnalysis
	if err := json.Unmarshal(raw, &a); err != nil {
		log.Printf("video analysis: %s id=%d sent something unreadable: %v", table, id, err)
		return
	}
	if !a.Measured() {
		return
	}

	// Fold the machine's tags the same way creator tags are folded, so the two
	// sets are comparable. A machine tag of "Fast Cuts" that never matches a
	// creator's "fast cuts" would be a tag nobody can ever share.
	tags := normalizeTags(a.AutoTags)
	tagsJSON, err := json.Marshal(tags)
	if err != nil || len(tags) == 0 {
		tagsJSON = []byte("[]")
	}

	// What the video is ABOUT, lifted out of the JSON blob into its own
	// column so it can be QUERIED.
	//
	// Topics are the open-vocabulary half — "thistle", "dark fantasy",
	// "street food" — and inside video_analysis they were write-only:
	// answering "which other videos are about thistles" meant parsing every
	// row. In their own indexed column that becomes one question, which is
	// what lets the feed match videos to each other instead of sorting them
	// into eighteen boxes.
	//
	// Shaped, not filtered. Unlike auto_tags there is no list to check
	// against — that is the point of topics — so this only lowercases and
	// de-duplicates, the same normalisation both tag columns get, so that
	// "Street Food" and "street food" are one topic rather than two.
	topicsJSON, err := json.Marshal(normalizeTags(a.Topics))
	if err != nil || len(a.Topics) == 0 {
		topicsJSON = []byte("[]")
	}

	// Only these two tables have the columns, and table is chosen by
	// hlsTableForKind from a fixed pair — never from user input.
	if _, err := db.Exec(
		`UPDATE `+table+`
		    SET video_analysis = $2, auto_tags = $3, content_topics = $4
		  WHERE id = $1`,
		id, []byte(raw), tagsJSON, topicsJSON,
	); err != nil {
		log.Printf("video analysis: could not save for %s id=%d: %v", table, id, err)
		return
	}

	// Now the video has actually been watched, decide what it IS and write
	// that down. See settleCategory — this is the wire that was missing.
	settleCategory(table, id, tags, &a)

	// Tell search the video now has words attached to it.
	//
	// Without this the whole reading, listening and looking pipeline was
	// invisible to search forever. A challenge is indexed once, at upload,
	// minutes BEFORE any of this exists — so the copy search holds has no
	// topics, no tags from the model, and nothing that was said out loud.
	// Somebody could describe a jellyfish video perfectly and find nothing.
	//
	// Only challenges: responses are not in the search index at all.
	if table == "challenges" {
		go reindexChallengeForSearch(id)
	}
}

// settleCategory writes down what the video turned out to be, now that
// something has actually watched it.
//
// ════════════════════════════════════════════════════════════════════════════
// WHY THIS HAD TO EXIST
// ════════════════════════════════════════════════════════════════════════════
//
// The category column was written once, at upload, before a single frame had
// been looked at. Nothing ever updated it. The model's conclusion went into
// auto_tags and stayed there.
//
// The feed got away with that because it recomputes the answer from auto_tags
// every time it scores something — so ranking used the model. Nothing else
// did. The creator dashboard's "by category", any report that groups by it,
// the admin views: all of them read the column, and the column still held a
// keyword guess at a title from before the video existed as anything but
// bytes.
//
// So the pipeline that reads, listens to and looks at every upload was
// invisible to every part of the app that asks the database a question. A
// feature that works and that nothing calls, which is the most common bug in
// this repo — this one just took the long way round.
//
// ════════════════════════════════════════════════════════════════════════════
// WHAT IT WILL AND WILL NOT OVERWRITE
// ════════════════════════════════════════════════════════════════════════════
//
// category is replaced ONLY when the model actually had an opinion. If it
// looked and could not tell, the upload-time answer stays: that answer had
// the creator's own pick and their tags behind it, and replacing it with a
// fresh keyword guess at the same title would be a downgrade dressed up as
// an update.
//
// machine_category is written every time, including as empty. "The model
// looked and had nothing to say" is a real finding, and one somebody reading
// this row needs to be able to tell apart from "nothing has looked yet".
//
// creator_category is never touched here. It is the one column in this group
// that holds a claim by a person, and no machine gets to write to it.
func settleCategory(table string, id int, machineTags []string, a *VideoAnalysis) {
	if db == nil {
		return
	}

	// What the creator gave us, read back rather than assumed. creator_category
	// holds only a real pick — see migrations/010.
	var creatorCategory, subject, prefix, creatorTagsJSON string
	q := `SELECT COALESCE(creator_category,''), COALESCE(custom_tags::text,'[]'), '', ''
	        FROM challenge_responses WHERE id = $1`
	if table == "challenges" {
		q = `SELECT COALESCE(creator_category,''), COALESCE(custom_tags::text,'[]'),
		            COALESCE(subject,''), COALESCE(prefix,'')
		       FROM challenges WHERE id = $1`
	}
	if err := db.QueryRow(q, id).Scan(
		&creatorCategory, &creatorTagsJSON, &subject, &prefix); err != nil {
		queryFailed(
			fmt.Sprintf("settleCategory: could not read %s id=%d back", table, id),
			"leaving its category at the value upload gave it", err)
		return
	}
	var creatorTags []string
	if !jsonUnmarshalQuiet([]byte(creatorTagsJSON), &creatorTags) {
		log.Printf("settleCategory: %s id=%d has unreadable custom_tags — "+
			"deciding its category without the creator's own words", table, id)
	}

	// The same decision the ranker makes, from the same function, so the
	// stored answer and the ranked answer cannot drift apart.
	v := categoryFromEvidence(
		machineTags, normalizeTags(creatorTags),
		creatorCategory, subject, prefix, analysisText(a))

	// One statement so a row is never briefly half-updated, and so "the model
	// had no opinion" is recorded without discarding what upload decided.
	//
	// EVERY PARAMETER IS CAST. $2 appears three times — once as the value of
	// a VARCHAR column and twice compared against '' — and without the casts
	// Postgres deduces a different type each time and refuses the whole
	// statement: "inconsistent types deduced for parameter $2". That is not a
	// warning and it is not partial; nothing is written at all, for every
	// video, forever. It is the same trap as CAST($1::text AS INT) elsewhere
	// in this repo, and it got through review here once already — the test
	// that runs this statement is what caught it.
	if _, err := db.Exec(
		`UPDATE `+table+`
		    SET machine_category = $2::text,
		        category        = CASE WHEN $2::text <> '' THEN $3::text ELSE category END,
		        category_source = CASE WHEN $2::text <> '' THEN $4::text ELSE category_source END
		  WHERE id = $1`,
		id, v.Machine, v.Category, v.Source,
	); err != nil {
		queryFailed(
			fmt.Sprintf("settleCategory: could not save the verdict for %s id=%d", table, id),
			"the video keeps the category it was given at upload, so the feed "+
				"still ranks it correctly but reports about it will not", err)
		return
	}

	// Worth a line either way. A reader asking "did watching this video change
	// what we think it is" should not have to infer the answer from silence.
	if v.Machine == "" {
		log.Printf("category: %s id=%d — the model looked and could not tell; "+
			"keeping what upload decided", table, id)
		return
	}
	if v.Disputed() {
		log.Printf("category: %s id=%d is %q (the model), though its creator "+
			"said %q", table, id, v.Machine, v.Creator)
		return
	}
	log.Printf("category: %s id=%d is %q (%s)", table, id, v.Category, v.Source)
}

// reindexChallengeForSearch re-upserts one challenge into the search index
// after its analysis lands.
//
// Best effort and off the hot path. A failure here means the video is
// findable by its title but not yet by its subject, which is exactly where it
// was before — never a reason to fail the analysis that just succeeded.
func reindexChallengeForSearch(id int) {
	if meili == nil || db == nil {
		return
	}
	ch, ok := GetChallengeByID(strconv.Itoa(id))
	if !ok {
		return
	}
	IndexChallenge(ch)
}

// mergeTags combines the creator's tags with the machine's, creator first.
//
// Order carries meaning downstream — categoryFromTags takes the first tag
// that names a category — so the creator leading is what keeps their choice
// winning over a machine guess.
func mergeTags(creator, auto []string) []string {
	if len(auto) == 0 {
		return creator
	}
	seen := make(map[string]bool, len(creator)+len(auto))
	out := make([]string, 0, len(creator)+len(auto))
	for _, t := range creator {
		if t != "" && !seen[t] {
			seen[t] = true
			out = append(out, t)
		}
	}
	for _, t := range auto {
		if t != "" && !seen[t] {
			seen[t] = true
			out = append(out, t)
		}
	}
	return out
}

// analysisEnergy converts a reading into the app's 0-to-1 energy scale, and
// says whether there was enough to convert.
//
// Two measured properties, both of which really do track how stimulating a
// video is: how often it cuts, and how much of it is not silence. A rapid-cut
// video with wall-to-wall sound is high energy in a way that does not depend
// on anybody's opinion — which is worth having, because the existing energy
// signal is inferred from engagement and so cannot say anything about a video
// nobody has watched yet.
//
// Weighted toward cuts: a static video with constant background music is calm
// however loud it is, while a fast-cut one is busy even in silence.
func analysisEnergy(a *VideoAnalysis) (float64, bool) {
	if !a.Measured() || !hasAnalysisPass(a, "shape") {
		return 0, false
	}
	// 30 cuts a minute — one every two seconds — is about as fast as a real
	// edit goes, so that is treated as the top of the scale.
	cuts := math.Min(1, a.CutsPerMinute/30)
	sound := a.SpeechRatio
	return math.Min(1, 0.65*cuts+0.35*sound), true
}

func hasAnalysisPass(a *VideoAnalysis, name string) bool {
	if a == nil {
		return false
	}
	for _, p := range a.Passes {
		if p == name {
			return true
		}
	}
	return false
}

// analysisText is every word the machine has about a video — what was said
// out loud, what was written on screen, and what the model said it SAW — as
// one string for the keyword matchers to read.
//
// The existing category and emotion matchers take text and look for words in
// it. Handing them this means the machine's findings reach both without
// either needing to learn about analysis at all.
//
// ════════════════════════════════════════════════════════════════════════════
// TOPICS ARE IN HERE, AND LEAVING THEM OUT COST THE SILENT HALF EVERYTHING
// ════════════════════════════════════════════════════════════════════════════
//
// Most of this catalogue says nothing. Of 114 videos, 79 produce no
// transcript, and 62 of those have no readable text on screen either. For
// every one of them ScreenText and Speech are both empty, so this used to
// return "" — and "" is what reached the last-resort category guess and the
// mood matcher.
//
// Those videos are not evidence-free. The worker LOOKS at them (see
// understand_frames.go) and the model writes down what it saw: "street food",
// "temple doorway", "cricket bat". Those phrases were the only words in
// existence about that video, they were sitting in the same struct, and the
// one function whose whole job is "hand the matchers every word we have" did
// not pass them on.
//
// So the pass that exists precisely to rescue silent videos was rescuing them
// into a dead end. Topics went to the ranker's topic matching and nowhere
// else; the category fallback still had nothing to read and dropped back to
// keyword-matching the creator's subject line, which is the guess the whole
// understanding pipeline was built to replace.
func analysisText(a *VideoAnalysis) string {
	if a == nil {
		return ""
	}
	parts := make([]string, 0, 3)
	if a.ScreenText != "" {
		parts = append(parts, a.ScreenText)
	}
	if a.Speech != "" {
		parts = append(parts, a.Speech)
	}
	// Last, so a video that both speaks AND was looked at reads in the order
	// the evidence was gathered. Joined with a space like the other two: these
	// are separate short phrases, and running them together would invent words
	// that are in none of them.
	if len(a.Topics) > 0 {
		parts = append(parts, strings.Join(a.Topics, " "))
	}
	return strings.Join(parts, " ")
}

// jsonUnmarshalQuiet decodes into v and reports success, for the many callers
// that treat unreadable stored analysis as "not measured" rather than as an
// error worth failing a request over.
func jsonUnmarshalQuiet(raw []byte, v interface{}) bool {
	return json.Unmarshal(raw, v) == nil
}
