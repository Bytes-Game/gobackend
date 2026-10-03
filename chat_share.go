package main

// Sharing a battle or a short into a chat.
//
// A shared video used to go into the chat as plain text: a line of title
// and the raw address of the video file. The other person saw a link.
//
// Now it is a message of its own kind, "share", that keeps WHICH video was
// shared (and, for a battle, which side). When a chat is read, the video is
// attached to the message in full — the same shape every feed sends — so
// the app draws it as a video card and opens it in the player with one
// tap.
//
// Attached for whoever is reading, and only if they may watch it: a video
// that has been deleted, or a friends-only video that is not for them,
// comes back as a share with no video, and the app shows "unavailable".
// The sender is checked the same way when they share: you can only share
// a video you can watch yourself.
//
// Everywhere a message is shown as one line (the chat list, a reply's
// quote, the phone notification) a share reads "🎬 Shared a video", or its
// note.

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/lib/pq"
)

const chatKindShare = "share"

// ChatShare is the video a "share" message points at. Challenge is nil when
// the reader may not watch it any more (deleted, or friends-only and not
// for them).
type ChatShare struct {
	ChallengeID string `json:"challengeId"`
	// For a battle: the side that was shared — empty for the challenger's
	// video, the answer's id for the answer's.
	ResponseID string     `json:"responseId,omitempty"`
	Challenge  *Challenge `json:"challenge,omitempty"`
}

// mayWatchSQL is true when the person in $2 may watch challenge `c`: it is
// not friends-only, or it is theirs, or they follow its creator and it is
// for all followers or for them by name. The same rule the follow-graph
// feed uses (candidate_sources.go), for one person and one video.
const mayWatchSQL = `(c.visibility <> 'friends'
	OR c.creator_id = $2
	OR (EXISTS (SELECT 1 FROM follows f
	             WHERE f.follower_id = $2 AND f.following_id = c.creator_id)
	    AND (NOT EXISTS (SELECT 1 FROM challenge_visible_to v
	                      WHERE v.challenge_id = c.id)
	         OR EXISTS (SELECT 1 FROM challenge_visible_to v
	                     WHERE v.challenge_id = c.id AND v.user_id = $2))))`

// checkShare says whether [senderID] may share challenge [challengeID] (and
// side [responseID], 0 for the challenger's video).
func checkShare(senderID, challengeID, responseID int) error {
	if challengeID <= 0 {
		return errors.New("which video is being shared?")
	}
	ok, err := mayWatch(senderID, challengeID)
	if queryFailed(fmt.Sprintf("checking user %d may share challenge %d", senderID, challengeID),
		"refusing the share", err) {
		return errors.New("could not check the video; try again")
	}
	if !ok {
		return errors.New("that video is not available")
	}
	if responseID > 0 && !answerOf(challengeID, responseID) {
		return errors.New("that answer is not part of this battle")
	}
	return nil
}

// attachShares puts each shared video on its message, as [viewerID] may see
// it. One query for every share in [msgs].
func attachShares(viewerID int, msgs []ChatMessage) {
	ids := []int64{}
	seen := map[int]bool{}
	for i := range msgs {
		m := &msgs[i]
		if m.Kind != chatKindShare || m.sharedChallengeID <= 0 {
			continue
		}
		m.Shared = &ChatShare{ChallengeID: strconv.Itoa(m.sharedChallengeID)}
		if m.sharedResponseID > 0 {
			m.Shared.ResponseID = strconv.Itoa(m.sharedResponseID)
		}
		if !seen[m.sharedChallengeID] {
			seen[m.sharedChallengeID] = true
			ids = append(ids, int64(m.sharedChallengeID))
		}
	}
	if len(ids) == 0 || db == nil {
		return
	}
	list := queryChallenges(challengeBaseQuery+`
		WHERE c.id = ANY($1) AND `+mayWatchSQL, pq.Array(ids), viewerID)
	if len(list) == 0 {
		return
	}
	// Dressed the way every feed dresses a video: the top answer, the
	// counts, and the reader's own like, save and vote.
	populateTopResponsesChallenges(list)
	markViewerStateChallenges(strconv.Itoa(viewerID), list)
	byID := map[string]*Challenge{}
	for i := range list {
		byID[list[i].ID] = &list[i]
	}
	for i := range msgs {
		if s := msgs[i].Shared; s != nil {
			if ch, ok := byID[s.ChallengeID]; ok {
				c := *ch
				s.Challenge = &c
			}
		}
	}
}
