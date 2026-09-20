package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
)

// ════════════════════════════════════════════════════════════════════════════
// GIVING THE CREATOR WHAT THE MODEL SAW
// ════════════════════════════════════════════════════════════════════════════
//
// Every video is already read, listened to and looked at after it is posted —
// see cmd/hls-worker/understand.go. That pass produces topics in the model's
// own words ("street food", "long distance relationship") and tags from the
// list the ranker knows.
//
// All of it was for the machine. The person who made the video never saw any
// of it, and their own tags — the ones that decide who the video reaches —
// stayed whatever they typed in the thirty seconds before posting.
//
// So: show them. "We spotted surfing, wave, ocean — add these?"
//
// WHY THIS IS NOT AT POSTING TIME, WHICH IS WHERE IT LOOKS LIKE IT BELONGS.
//
// The pass runs on a build machine that takes about two minutes to start
// before it does anything — measured, with every binary and model already
// cached. Nobody spends two minutes on the title screen. The alternatives are
// a hosted API (a key, a rate limit, terms someone else can change, and the
// creator's frames leaving the building) or a model on the phone (the
// smallest useful one is half a gigabyte). Both were rejected.
//
// Waiting is not only the cheap option, it is the better answer: by the time
// this runs the model has seen the whole video rather than guessing from a
// title that has not been written yet.

// suggestionSource is what the machine noticed, in the order it is offered.
//
// Topics first. They are the open-vocabulary half — the model describing the
// video in its own words — and they are what a creator would not have thought
// to type. auto_tags come from the fixed list the ranker uses, so a creator
// is far more likely to have covered those already.
type tagSuggestions struct {
	// Suggested is what the creator has not already got and has not already
	// said no to.
	Suggested []string `json:"suggested"`
	// Yours is what is on the video now, so the client can show the whole
	// picture without a second call.
	Yours []string `json:"yours"`
}

// tagSurface is one kind of video these handlers work on.
//
// There are two, and they are the same feature: a challenge, and an answer to
// somebody else's challenge. Both are read, listened to and looked at by the
// same worker, into the same two columns, and both have an uploader who has
// never been shown any of it.
//
// The differences are three words — which table, which column names the owner,
// and whether search holds a copy — so they are three fields rather than a
// second copy of the file. A second copy is how one of them ends up with a
// fix the other never gets.
type tagSurface struct {
	// table is interpolated into SQL, so it may only ever be one of the two
	// values below. Nothing here takes a table name from a request.
	table string
	// owner is the column holding the id of the person who uploaded it.
	owner string
	// searchHasACopy is true where the search index stores its own copy of the
	// tags and has to be told they changed. Only challenges are indexed.
	searchHasACopy bool
	// notFound is the error a missing row reports, so the 404 message names
	// the thing the caller actually asked for.
	notFound error
}

var (
	// errNoSuchResponseForTags is the response half of
	// errNoSuchChallengeForTags. Separate values so a caller can tell which
	// of the two it asked about, and so the 404 body is not the wrong noun.
	errNoSuchResponseForTags = errors.New("no such response")

	challengeTagSurface = tagSurface{
		table:          "challenges",
		owner:          "creator_id",
		searchHasACopy: true,
		notFound:       errNoSuchChallengeForTags,
	}
	responseTagSurface = tagSurface{
		table: "challenge_responses",
		owner: "responder_id",
		// Responses are not in the search index at all — see
		// storeVideoAnalysis, which only reindexes challenges. Telling search
		// about a change it has no record of would be a wasted round trip on
		// every save.
		searchHasACopy: false,
		notFound:       errNoSuchResponseForTags,
	}
)

// GetResponseTagSuggestionsHandler is the response half of
// GetTagSuggestionsHandler.
//
//	GET /api/v1/challenges/responses/{id}/tag-suggestions
//
// Responder only, for the same reason: the model's reading of somebody's video
// belongs to the person who made it.
func GetResponseTagSuggestionsHandler(w http.ResponseWriter, r *http.Request) {
	serveTagSuggestions(w, r, responseTagSurface)
}

// DecideResponseTagSuggestionsHandler is the response half of
// DecideTagSuggestionsHandler.
//
//	POST /api/v1/challenges/responses/{id}/tag-suggestions
func DecideResponseTagSuggestionsHandler(w http.ResponseWriter, r *http.Request) {
	decideTagSuggestions(w, r, responseTagSurface)
}

// GetTagSuggestionsHandler answers "what did the model notice about my video?".
//
//	GET /api/v1/challenges/{id}/tag-suggestions
//
// Creator only. These are suggestions about someone's own work, and the
// machine's reading of a video is not something to hand to strangers.
func GetTagSuggestionsHandler(w http.ResponseWriter, r *http.Request) {
	serveTagSuggestions(w, r, challengeTagSurface)
}

func serveTagSuggestions(w http.ResponseWriter, r *http.Request, s tagSurface) {
	id, err := strconv.Atoi(mux.Vars(r)["id"])
	if err != nil {
		http.Error(w, "bad id", http.StatusBadRequest)
		return
	}
	own, topics, auto, mine, dismissed, err := readTagState(s, id, authUserID(r))
	if err != nil {
		writeTagStateError(w, err)
		return
	}
	if !own {
		http.Error(w, "not your video", http.StatusForbidden)
		return
	}
	writeJSON(w, http.StatusOK, tagSuggestions{
		Suggested: suggestableTags(topics, auto, mine, dismissed),
		Yours:     mine,
	})
}

// tagDecisionRequest is the creator answering.
type tagDecisionRequest struct {
	// Add is kept. Dismiss is not offered again.
	Add     []string `json:"add,omitempty"`
	Dismiss []string `json:"dismiss,omitempty"`
}

// DecideTagSuggestionsHandler records what the creator chose.
//
//	POST /api/v1/challenges/{id}/tag-suggestions  {"add":[...],"dismiss":[...]}
//
// Both lists in one call because both are the same gesture — the creator
// looked at what was offered and sorted it. Two endpoints would let a client
// do half of it and leave the rest offered forever.
func DecideTagSuggestionsHandler(w http.ResponseWriter, r *http.Request) {
	decideTagSuggestions(w, r, challengeTagSurface)
}

func decideTagSuggestions(w http.ResponseWriter, r *http.Request, s tagSurface) {
	id, err := strconv.Atoi(mux.Vars(r)["id"])
	if err != nil {
		http.Error(w, "bad id", http.StatusBadRequest)
		return
	}
	var req tagDecisionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}

	own, topics, auto, mine, dismissed, err := readTagState(s, id, authUserID(r))
	if err != nil {
		writeTagStateError(w, err)
		return
	}
	if !own {
		http.Error(w, "not your video", http.StatusForbidden)
		return
	}

	offered := suggestableTags(topics, auto, mine, dismissed)
	keep := acceptTags(mine, offered, req.Add)
	said := dismissTags(dismissed, offered, req.Dismiss)

	if err := saveTagDecision(s, id, keep, said); err != nil {
		http.Error(w, "could not save: "+err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, tagSuggestions{
		Suggested: suggestableTags(topics, auto, keep, said),
		Yours:     keep,
	})
}

// acceptTags is the creator's tag set after they keep some of what was
// offered.
//
// ONLY WHAT WAS ON OFFER. A client that sends anything else is not answering
// this question — it is editing tags through a door that was not built for
// it, skipping the shape and length rules the create path applies. This is
// the security boundary of the whole feature, which is why it is a function
// with its own tests rather than four lines inside a handler.
//
// Normalised as a whole set at the end, not per tag: that is what
// de-duplicates and applies maxTagsPerItem, so accepting can never carry a
// video past the cap the create path holds it to.
func acceptTags(mine, offered, add []string) []string {
	allowed := map[string]bool{}
	for _, t := range offered {
		allowed[t] = true
	}
	keep := make([]string, 0, len(mine)+len(add))
	keep = append(keep, mine...)
	for _, t := range normalizeTags(add) {
		if allowed[t] {
			keep = append(keep, t)
		}
	}
	return normalizeTags(keep)
}

// dismissTags is the set of "no" answers after the creator turns some down.
//
// Same boundary as [acceptTags]: only what was offered. Letting a client
// dismiss anything would let it silently poison future suggestions for tags
// the model has not even proposed yet.
func dismissTags(dismissed, offered, drop []string) []string {
	allowed := map[string]bool{}
	for _, t := range offered {
		allowed[t] = true
	}
	said := append([]string{}, dismissed...)
	for _, t := range normalizeTags(drop) {
		if allowed[t] {
			said = append(said, t)
		}
	}
	return dedupeStrings(said)
}

// suggestableTags is what is still worth offering.
//
// Everything the machine noticed, minus what the creator already has and
// minus what they have already turned down. Pure, so the rule is testable
// without a database — and it is the same rule on the read and the write, so
// a client cannot accept something that was never offered.
func suggestableTags(topics, auto, mine, dismissed []string) []string {
	skip := map[string]bool{}
	for _, t := range mine {
		skip[t] = true
	}
	for _, t := range dismissed {
		skip[t] = true
	}
	out := []string{}
	for _, t := range append(append([]string{}, topics...), auto...) {
		if t == "" || skip[t] {
			continue
		}
		skip[t] = true
		out = append(out, t)
	}
	// Room left under the cap, so the creator is never offered more than they
	// could keep. Offering eleven when ten is the limit is a list where the
	// last one silently does nothing.
	if room := maxTagsPerItem - len(mine); room >= 0 && len(out) > room {
		out = out[:room]
	}
	return out
}

func dedupeStrings(in []string) []string {
	seen := map[string]bool{}
	out := make([]string, 0, len(in))
	for _, s := range in {
		if s == "" || seen[s] {
			continue
		}
		seen[s] = true
		out = append(out, s)
	}
	return out
}

// errNoSuchChallengeForTags separates "no such video" from a database fault,
// so one is a 404 and the other is not.
var errNoSuchChallengeForTags = errors.New("no such challenge")

func writeTagStateError(w http.ResponseWriter, err error) {
	if errors.Is(err, errNoSuchChallengeForTags) {
		http.Error(w, "challenge not found", http.StatusNotFound)
		return
	}
	if errors.Is(err, errNoSuchResponseForTags) {
		http.Error(w, "response not found", http.StatusNotFound)
		return
	}
	http.Error(w, "could not read: "+err.Error(), http.StatusInternalServerError)
}

// readTagState loads everything both handlers need in one query.
//
// The table and owner column come from a tagSurface, which is one of two
// package-level values. Neither is ever built from a request, and that is the
// only thing standing between string-interpolated SQL and an injection — so if
// a third surface is ever added, it is added here as a constant, not passed in.
func readTagState(s tagSurface, id int, viewerID string) (own bool, topics, auto, mine, dismissed []string, err error) {
	if db == nil {
		return false, nil, nil, nil, nil, errors.New("no database")
	}
	var owner string
	var topicsJSON, autoJSON, mineJSON, dismissedJSON []byte
	row := db.QueryRow(
		`SELECT `+s.owner+`,
		        COALESCE(content_topics::text, '[]'),
		        COALESCE(auto_tags::text, '[]'),
		        COALESCE(custom_tags::text, '[]'),
		        COALESCE(dismissed_tags::text, '[]')
		   FROM `+s.table+` WHERE id = $1`, id)
	if err := row.Scan(&owner, &topicsJSON, &autoJSON, &mineJSON, &dismissedJSON); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil, nil, nil, nil, s.notFound
		}
		return false, nil, nil, nil, nil, err
	}
	return owner != "" && owner == viewerID,
		decodeTagList(topicsJSON), decodeTagList(autoJSON),
		decodeTagList(mineJSON), decodeTagList(dismissedJSON), nil
}

// decodeTagList reads one JSONB tag column.
//
// A column that will not parse reports no tags rather than failing the
// request: the worst case is a creator offered nothing, which is where they
// were before this existed.
func decodeTagList(raw []byte) []string {
	out := []string{}
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil
	}
	return out
}

func saveTagDecision(s tagSurface, id int, keep, dismissed []string) error {
	keepJSON, err := json.Marshal(keep)
	if err != nil {
		return err
	}
	dismissedJSON, err := json.Marshal(dismissed)
	if err != nil {
		return err
	}
	if _, err := db.Exec(
		`UPDATE `+s.table+` SET custom_tags = $2, dismissed_tags = $3 WHERE id = $1`,
		id, keepJSON, dismissedJSON); err != nil {
		return err
	}
	// Tags decide who a video reaches, and search holds its own copy — of
	// challenges. Responses were never indexed, so there is nothing to tell.
	if s.searchHasACopy {
		go reindexChallengeForSearch(id)
	}
	return nil
}
