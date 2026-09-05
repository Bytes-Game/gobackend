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
	"log"
	"math"
	"strconv"
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

// analysisText is everything the video said, on screen and out loud, as one
// string for the keyword matchers to read.
//
// The existing category and emotion matchers take text and look for words in
// it. Handing them this means the machine's findings reach both without
// either needing to learn about analysis at all.
func analysisText(a *VideoAnalysis) string {
	if a == nil {
		return ""
	}
	if a.ScreenText == "" {
		return a.Speech
	}
	if a.Speech == "" {
		return a.ScreenText
	}
	return a.ScreenText + " " + a.Speech
}
