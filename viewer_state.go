package main

// viewer_state.go — what the person asking has done to each video: liked
// it, saved it, voted in it, liked the answer.
//
// ════════════════════════════════════════════════════════════════════════
// A VIDEO YOU LIKED CAME BACK UNLIKED
// ════════════════════════════════════════════════════════════════════════
//
// Feeds carried each video's like COUNT but never whether YOU liked it. The
// app filled the gap by starting every heart empty. So a video you had
// liked came round again in your feed with an empty heart — and tapping it,
// to put the like back, took your like away.
//
// Saves and votes had the same hole: a saved video showed unsaved, and a
// battle you had voted in asked you to vote again.
//
// So every place that hands videos to the app marks them here, for the
// person asking, in one database round trip per page.
//
// ════════════════════════════════════════════════════════════════════════
// SAFE ONLY BECAUSE NOTHING SHARES THESE RECORDS
// ════════════════════════════════════════════════════════════════════════
//
// This writes per-person facts onto the challenge records of one response.
// That is only safe while no cache hands the same record to two people's
// requests — otherwise one person's like would appear on another's phone.
// Nothing caches them today (feeds are read fresh per request). A cache of
// Challenge records must copy them before this runs.

import (
	"net/http"
	"strconv"

	"github.com/lib/pq"
)

// signedInUserID is who is signed in on a route that does not require it:
// the id from a valid token, or "" for nobody. Never a userId from the
// address — anyone can put anything there.
func signedInUserID(r *http.Request) string {
	if id := authUserID(r); id != "" {
		return id
	}
	tok := bearerToken(r)
	if tok == "" {
		return ""
	}
	claims, err := parseToken(tok)
	if err != nil {
		return ""
	}
	return claims.Subject
}

// markViewerState fills the viewer fields on [chs]. Every pointer must be
// this request's own record.
func markViewerState(viewerID string, chs []*Challenge) {
	if db == nil || len(chs) == 0 {
		return
	}
	// The counts are for everyone, signed in or not. So is whether a post is
	// a photo, and the credit for its song.
	fillCounts(chs)
	fillMediaTypes(chs)
	fillMusic(chs)
	viewer, err := strconv.Atoi(viewerID)
	if err != nil || viewer <= 0 {
		return
	}
	byID := make(map[string][]*Challenge, len(chs))
	byAnswer := make(map[string][]*Challenge)
	challengeIDs := make([]int64, 0, len(chs))
	answerIDs := make([]int64, 0)
	for _, c := range chs {
		if c == nil {
			continue
		}
		id, err := strconv.ParseInt(c.ID, 10, 64)
		if err != nil {
			continue
		}
		if _, dup := byID[c.ID]; !dup {
			challengeIDs = append(challengeIDs, id)
		}
		byID[c.ID] = append(byID[c.ID], c)
		if c.TopResponseID != "" {
			if rid, err := strconv.ParseInt(c.TopResponseID, 10, 64); err == nil {
				if _, dup := byAnswer[c.TopResponseID]; !dup {
					answerIDs = append(answerIDs, rid)
				}
				byAnswer[c.TopResponseID] = append(byAnswer[c.TopResponseID], c)
			}
		}
	}
	if len(challengeIDs) == 0 {
		return
	}

	// One trip for all four. A vote with no answer id is a vote for the
	// creator's side (migration 011), so its name comes back empty here and
	// is filled from the challenge below.
	rows, err := db.Query(`
		(SELECT 'liked' AS kind, challenge_id::text AS id, '' AS who
		   FROM challenge_likes
		  WHERE user_id = $1 AND challenge_id = ANY($2))
		UNION ALL
		(SELECT 'saved', challenge_id::text, ''
		   FROM saved_challenges
		  WHERE user_id = $1 AND challenge_id = ANY($2))
		UNION ALL
		(SELECT 'voted', cv.challenge_id::text, COALESCE(u.username, '')
		   FROM challenge_votes cv
		   LEFT JOIN challenge_responses cr ON cr.id = cv.response_id
		   LEFT JOIN users u ON u.id = cr.responder_id
		  WHERE cv.voter_id = $1 AND cv.challenge_id = ANY($2))
		UNION ALL
		(SELECT 'answer_liked', response_id::text, ''
		   FROM challenge_response_likes
		  WHERE user_id = $1 AND response_id = ANY($3))`,
		viewer, pq.Array(challengeIDs), pq.Array(answerIDs))
	if queryFailed("what this person has liked, saved and voted for",
		"showing every heart empty and every vote unasked", err) {
		return
	}
	defer rows.Close()
	bad := 0
	for rows.Next() {
		var kind, id, who string
		if scanFailed("a like, save or vote of this person's",
			rows.Scan(&kind, &id, &who), &bad) {
			continue
		}
		switch kind {
		case "liked":
			for _, c := range byID[id] {
				c.ViewerLiked = true
			}
		case "saved":
			for _, c := range byID[id] {
				c.ViewerSaved = true
			}
		case "voted":
			for _, c := range byID[id] {
				c.ViewerVoted = true
				c.ViewerVotedFor = who
				if who == "" {
					c.ViewerVotedFor = c.CreatorUsername
				}
			}
		case "answer_liked":
			for _, c := range byAnswer[id] {
				c.ViewerLikedAnswer = true
			}
		}
	}
	if err := rows.Err(); err != nil {
		queryFailed("reading what this person has liked, saved and voted for",
			"some hearts may show empty", err)
	}
}

// markViewerStateItems is markViewerState for a feed page.
func markViewerStateItems(viewerID string, items []HomeFeedItem) {
	chs := make([]*Challenge, 0, len(items))
	for _, it := range items {
		if it.Type == "challenge" && it.Challenge != nil {
			chs = append(chs, it.Challenge)
		}
	}
	markViewerState(viewerID, chs)
}

// markViewerStateScored is markViewerState for a ranked feed page.
func markViewerStateScored(viewerID string, items []ScoredItem) {
	chs := make([]*Challenge, 0, len(items))
	for _, it := range items {
		if it.Item.Type == "challenge" && it.Item.Challenge != nil {
			chs = append(chs, it.Item.Challenge)
		}
	}
	markViewerState(viewerID, chs)
}

// markViewerStateChallenges is markViewerState for a plain list, such as
// search results or a profile's videos.
func markViewerStateChallenges(viewerID string, list []Challenge) {
	chs := make([]*Challenge, len(list))
	for i := range list {
		chs[i] = &list[i]
	}
	markViewerState(viewerID, chs)
}
