package main

// video_about.go — "What is this video about?"
//
// The app's answer to Instagram's AI button under a reel. The worker writes
// the answer once, while it reads and looks at every upload anyway (see
// understoodAbout in cmd/hls-worker/understand.go), and it is stored inside
// the video's analysis. This hands it to the app when somebody asks.
//
// What is NOT handed over: the transcript. That is what somebody said into
// their camera, word for word, and it stays behind the admin login (see
// media_analysis_read.go). The sentence here is a description of a video
// anyone asking may already watch — the same rule decides both (mayWatch).

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
)

// VideoAbout is the answer.
type VideoAbout struct {
	// About is a sentence or two from the model; empty when it could not
	// tell, or the video was looked at before it was asked to write one.
	About string `json:"about"`
	// Topics are the model's own short words for the video, for when there
	// is no sentence to show.
	Topics []string `json:"topics"`
	// From is what the model went on: "said" (what is said and written on
	// screen) or "shown" (the pictures, for a video that says nothing).
	// Empty when no model looked at it.
	From string `json:"from"`
	// Looked says whether the worker has looked at the video at all. False
	// for one uploaded moments ago, which the app says is still being
	// worked out rather than that there is nothing to say.
	Looked bool `json:"looked"`
}

// GET /api/v1/challenges/{id}/about?response=RID
//
// About the challenger's video, or with ?response= about that answer in the
// battle. For anyone who may watch it.
func VideoAboutHandler(w http.ResponseWriter, r *http.Request) {
	viewer := authUserID(r)
	cid, err := strconv.Atoi(mux.Vars(r)["id"])
	if err != nil || viewer == "" {
		http.Error(w, "sign in, with a challenge id", http.StatusBadRequest)
		return
	}
	rid := 0
	if s := r.URL.Query().Get("response"); s != "" {
		if rid, err = strconv.Atoi(s); err != nil || rid <= 0 {
			http.Error(w, "response must be an answer's id", http.StatusBadRequest)
			return
		}
	}
	if !mayWatchList(w, viewer, cid) {
		return
	}

	var raw, topics []byte
	if rid > 0 {
		err = db.QueryRow(`
			SELECT COALESCE(video_analysis::text, ''), COALESCE(content_topics::text, '[]')
			  FROM challenge_responses
			 WHERE id = $1 AND challenge_id = $2`, rid, cid).Scan(&raw, &topics)
	} else {
		err = db.QueryRow(`
			SELECT COALESCE(video_analysis::text, ''), COALESCE(content_topics::text, '[]')
			  FROM challenges WHERE id = $1`, cid).Scan(&raw, &topics)
	}
	if queryFailed("reading what a video is about", "answering no such video", err) {
		http.Error(w, "no such video", http.StatusNotFound)
		return
	}
	writeJSON(w, http.StatusOK, readVideoAbout(raw, topics))
}

// readVideoAbout turns a stored analysis and topics column into the answer.
func readVideoAbout(raw, topicsJSON []byte) VideoAbout {
	out := VideoAbout{Topics: []string{}}
	if err := json.Unmarshal(topicsJSON, &out.Topics); err != nil || out.Topics == nil {
		out.Topics = []string{}
	}
	if len(raw) == 0 {
		return out
	}
	var a struct {
		About     string   `json:"about"`
		AboutFrom string   `json:"aboutFrom"`
		Passes    []string `json:"passes"`
	}
	if err := json.Unmarshal(raw, &a); err != nil {
		return out
	}
	out.Looked = len(a.Passes) > 0
	out.About = a.About
	if a.About != "" {
		out.From = a.AboutFrom
	}
	return out
}
