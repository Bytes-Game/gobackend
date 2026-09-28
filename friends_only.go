package main

// friends_only.go — a challenge for friends: who it is for, and telling them.
//
// "Friends" are the people who follow the creator. An "Only friends"
// challenge is for all of them, or for the ones the creator picked by name,
// held in challenge_visible_to. An empty list there means all of them.
//
// ════════════════════════════════════════════════════════════════════════
// ONLY FOLLOWERS CAN BE PICKED
// ════════════════════════════════════════════════════════════════════════
//
// The app offers the creator's followers to pick from, but the list arrives
// as ids from a phone. Taken as sent, anyone could name any account on the
// platform and push a notification at it. So the list is cut to followers
// here, whatever was sent.
//
// And a list that comes out EMPTY after that cut must not become "all
// friends" — that would widen a challenge meant for three people to
// everybody who follows the creator. It is kept to the creator alone.

import (
	"fmt"
	"log"
	"strconv"

	"github.com/lib/pq"
)

// TriggerFriendChallenge is a push for "someone you follow posted a
// challenge for you". Sent under the same setting as a friend's answer —
// both are a friend's activity aimed at you.
const TriggerFriendChallenge TriggerKind = "friend_challenge"

// saveVisibleTo records which of [requested] may see challenge [challengeID]:
// those of them who follow [creatorID].
func saveVisibleTo(challengeID, creatorID int, requested []string) {
	if db == nil || len(requested) == 0 {
		return
	}
	ids := make([]int64, 0, len(requested))
	for _, s := range requested {
		if id, err := strconv.ParseInt(s, 10, 64); err == nil && id > 0 {
			ids = append(ids, id)
		}
	}
	res, err := db.Exec(`
		INSERT INTO challenge_visible_to (challenge_id, user_id)
		SELECT $1, f.follower_id
		  FROM follows f
		 WHERE f.following_id = $2 AND f.follower_id = ANY($3)
		ON CONFLICT DO NOTHING`, challengeID, creatorID, pq.Array(ids))
	if queryFailed("saving who a friends-only challenge is for",
		"keeping it to its creator until fixed", err) {
		res = nil
	}
	if res != nil {
		if n, _ := res.RowsAffected(); n > 0 {
			return
		}
	}
	// Nobody valid (or the save failed): the creator alone, never "all".
	_, err = db.Exec(`INSERT INTO challenge_visible_to (challenge_id, user_id)
	                  VALUES ($1, $2) ON CONFLICT DO NOTHING`, challengeID, creatorID)
	queryFailed("keeping a friends-only challenge to its creator",
		"it may show to all of their followers", err)
}

type friend struct {
	id       int
	username string
}

// challengeAudience is who a friends-only challenge is for, and whether
// they were picked by name (rather than being all of the creator's
// followers).
func challengeAudience(challengeID, creatorID int) (people []friend, picked bool) {
	var n int
	err := db.QueryRow(`SELECT COUNT(*) FROM challenge_visible_to
	                     WHERE challenge_id = $1`, challengeID).Scan(&n)
	if queryFailed("reading who a friends-only challenge is for",
		"telling nobody about it", err) {
		return nil, false
	}
	picked = n > 0
	rows, err := db.Query(`
		SELECT u.id, u.username
		  FROM follows f
		  JOIN users u ON u.id = f.follower_id
		 WHERE f.following_id = $1
		   AND f.follower_id <> $1
		   AND (NOT EXISTS (SELECT 1 FROM challenge_visible_to v
		                     WHERE v.challenge_id = $2)
		        OR EXISTS (SELECT 1 FROM challenge_visible_to v
		                    WHERE v.challenge_id = $2
		                      AND v.user_id = f.follower_id))`,
		creatorID, challengeID)
	if queryFailed("listing the friends a challenge is for",
		"telling nobody about it", err) {
		return nil, picked
	}
	defer rows.Close()
	bad := 0
	for rows.Next() {
		var f friend
		if scanFailed("a friend a challenge is for",
			rows.Scan(&f.id, &f.username), &bad) {
			continue
		}
		people = append(people, f)
	}
	if err := rows.Err(); err != nil {
		queryFailed("listing the friends a challenge is for",
			"some of them will not be told", err)
	}
	return people, picked
}

// notifyFriendsOfChallenge tells everyone a friends-only challenge is for
// that it is there: in their notifications list, live if they are online,
// and as a push to their phone.
func notifyFriendsOfChallenge(c Challenge) {
	if db == nil {
		return
	}
	cid, err1 := strconv.Atoi(c.ID)
	creatorID, err2 := strconv.Atoi(c.CreatorID)
	if err1 != nil || err2 != nil {
		return
	}
	people, picked := challengeAudience(cid, creatorID)
	title := challengeTitle(c.Prefix, c.Subject)
	body := fmt.Sprintf("posted a challenge for friends: “%s”", title)
	pushTitle := fmt.Sprintf("%s posted a challenge for friends", c.CreatorUsername)
	if picked {
		body = fmt.Sprintf("challenged you: “%s”", title)
		pushTitle = fmt.Sprintf("%s challenged you", c.CreatorUsername)
	}
	for _, f := range people {
		notifyUser(InboxNote{
			UserID:      f.id,
			Username:    f.username,
			Kind:        noteFriendChallenge,
			ActorID:     creatorID,
			ActorName:   c.CreatorUsername,
			ChallengeID: cid,
			Body:        body,
		})
		if _, _, err := enqueueNotification(EnqueueParams{
			UserID:      strconv.Itoa(f.id),
			TriggerKind: TriggerFriendChallenge,
			DedupeKey:   "friend_challenge:" + c.ID,
			Title:       pushTitle,
			Body:        title,
			Deeplink:    fmt.Sprintf("devf://challenge/%s", c.ID),
		}); err != nil {
			log.Printf("friend challenge %s: could not queue a push for "+
				"user %d: %v — they still have it in their list", c.ID, f.id, err)
		}
	}
}
