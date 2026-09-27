package main

// profile_videos.go — the profile's tabs as videos, not notes.
//
// The Open / Live / Won / Lost / Draw tabs were built from battle cards: a
// title, a thumbnail, and the score. Enough to write a line of text about a
// battle, not enough to play it — no encoded versions, no manifest, no
// answer video, nothing to say whether you had liked it. So the app drew
// those tabs as written notes while every other grid in the app shows
// videos, and a tap could not open the battle to scroll through.
//
// Now each card carries the whole video, the same record every feed sends,
// with the answer filled in and the viewer's own likes and votes marked.

import (
	"strconv"

	"github.com/lib/pq"
)

// challengesByIDs loads full video records, in the order of [ids]. Ids that
// no longer exist are left out.
func challengesByIDs(ids []int) []Challenge {
	if db == nil || len(ids) == 0 {
		return nil
	}
	got := queryChallenges(challengeBaseQuery+`
	 WHERE c.id = ANY($1)`, pq.Array(ids))
	byID := make(map[string]Challenge, len(got))
	for _, c := range got {
		byID[c.ID] = c
	}
	out := make([]Challenge, 0, len(ids))
	for _, id := range ids {
		if c, ok := byID[strconv.Itoa(id)]; ok {
			out = append(out, c)
		}
	}
	return out
}

// attachBattleVideos puts the playable video on each battle card, finished
// the way a feed finishes its videos, and marked for [viewerID].
func attachBattleVideos(cards []BattleCard, viewerID string) {
	ids := make([]int, 0, len(cards))
	for _, c := range cards {
		if id, err := strconv.Atoi(c.ChallengeID); err == nil {
			ids = append(ids, id)
		}
	}
	videos := challengesByIDs(ids)
	populateTopResponsesChallenges(videos)
	markViewerStateChallenges(viewerID, videos)
	byID := make(map[string]*Challenge, len(videos))
	for i := range videos {
		byID[videos[i].ID] = &videos[i]
	}
	for i := range cards {
		cards[i].Video = byID[cards[i].ChallengeID]
	}
}
