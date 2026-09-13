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

// GetTagSuggestionsHandler answers "what did the model notice about my video?".
//
//	GET /api/v1/challenges/{id}/tag-suggestions
//
// Creator only. These are suggestions about someone's own work, and the
// machine's reading of a video is not something to hand to strangers.
func GetTagSuggestionsHandler(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.Atoi(mux.Vars(r)["id"])
	if err != nil {
		http.Error(w, "bad challenge id", http.StatusBadRequest)
		return
	}
	own, topics, auto, mine, dismissed, err := readTagState(id, authUserID(r))
	if err != nil {
		writeTagStateError(w, err)
		return
	}
	if !own {
		http.Error(w, "not your challenge", http.StatusForbidden)
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
	id, err := strconv.Atoi(mux.Vars(r)["id"])
	if err != nil {
		http.Error(w, "bad challenge id", http.StatusBadRequest)
		return
	}
	var req tagDecisionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}

	own, topics, auto, mine, dismissed, err := readTagState(id, authUserID(r))
	if err != nil {
		writeTagStateError(w, err)
		return
	}
	if !own {
		http.Error(w, "not your challenge", http.StatusForbidden)
		return
	}

	offered := suggestableTags(topics, auto, mine, dismissed)
	keep := acceptTags(mine, offered, req.Add)
	said := dismissTags(dismissed, offered, req.Dismiss)

	if err := saveTagDecision(id, keep, said); err != nil {
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
	http.Error(w, "could not read challenge: "+err.Error(), http.StatusInternalServerError)
}

// readTagState loads everything both handlers need in one query.
func readTagState(id int, viewerID string) (own bool, topics, auto, mine, dismissed []string, err error) {
	if db == nil {
		return false, nil, nil, nil, nil, errors.New("no database")
	}
	var creator string
	var topicsJSON, autoJSON, mineJSON, dismissedJSON []byte
	row := db.QueryRow(
		`SELECT creator_id,
		        COALESCE(content_topics::text, '[]'),
		        COALESCE(auto_tags::text, '[]'),
		        COALESCE(custom_tags::text, '[]'),
		        COALESCE(dismissed_tags::text, '[]')
		   FROM challenges WHERE id = $1`, id)
	if err := row.Scan(&creator, &topicsJSON, &autoJSON, &mineJSON, &dismissedJSON); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil, nil, nil, nil, errNoSuchChallengeForTags
		}
		return false, nil, nil, nil, nil, err
	}
	return creator != "" && creator == viewerID,
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

func saveTagDecision(id int, keep, dismissed []string) error {
	keepJSON, err := json.Marshal(keep)
	if err != nil {
		return err
	}
	dismissedJSON, err := json.Marshal(dismissed)
	if err != nil {
		return err
	}
	if _, err := db.Exec(
		`UPDATE challenges SET custom_tags = $2, dismissed_tags = $3 WHERE id = $1`,
		id, keepJSON, dismissedJSON); err != nil {
		return err
	}
	// Tags decide who a video reaches, and search holds its own copy.
	go reindexChallengeForSearch(id)
	return nil
}
