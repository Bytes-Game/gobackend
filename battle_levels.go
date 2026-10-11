package main

// battle_levels.go — who may battle whom.
//
// Battles are between people of about the same level, so they stay fair:
// your own league, or the one right next to it. A Bronze player can take on
// Bronze and Silver, a Gold player Silver, Gold and Platinum. A Diamond
// player cannot pick off beginners, and a beginner is not thrown against a
// Diamond.
//
// Except between friends. Friends can battle at any level. "Friends" means
// both people chose it, so a stranger cannot make themselves your friend to
// get round the rule:
//
//   - you follow each other, or
//   - the challenge is friends-only and its creator picked you by name.
//
// A one-way follow is not enough. Anybody can follow anybody, so it would be
// a way round the rule for anyone who wanted one.
//
// This used to allow two leagues apart (Bronze against Gold), had no friends
// exception, and read the league from a column that says "Bronze" for
// everybody who has not had a battle decided yet — including people whose
// rating says otherwise. The league here is worked out from the rating and
// the record, the same way the profile shows it (leagueFor).

import (
	"fmt"
	"strconv"
	"strings"
)

// leagueTier puts the leagues in order. Somebody with no decided battles is
// "Unranked" and counts as Bronze: everybody starts at the bottom.
var leagueTier = map[string]int{
	"Bronze":   1,
	"Silver":   2,
	"Gold":     3,
	"Platinum": 4,
	"Diamond":  5,
}

// leagueOrder is leagueTier the other way round.
var leagueOrder = []string{"Bronze", "Silver", "Gold", "Platinum", "Diamond"}

// maxLeagueDiff is how many leagues apart two people may be and still
// battle: 1 is your own league and the ones on either side.
const maxLeagueDiff = 1

// tierOf is a league's place in the order, Bronze for Unranked or anything
// unknown.
func tierOf(league string) int {
	if t, ok := leagueTier[league]; ok {
		return t
	}
	return 1
}

// battleLeague is the league somebody is in now, from their rating and
// record.
func battleLeague(u User) string {
	return leagueFor(u.Rating, u.Wins+u.Losses+u.Draws)
}

// leaguesInReach names the leagues somebody in [tier] may battle, in words:
// "Bronze and Silver", "Silver, Gold and Platinum".
func leaguesInReach(tier int) string {
	names := make([]string, 0, 2*maxLeagueDiff+1)
	for t := tier - maxLeagueDiff; t <= tier+maxLeagueDiff; t++ {
		if t >= 1 && t <= len(leagueOrder) {
			names = append(names, leagueOrder[t-1])
		}
	}
	if len(names) == 1 {
		return names[0]
	}
	return strings.Join(names[:len(names)-1], ", ") + " and " + names[len(names)-1]
}

// friendsForBattle is whether [responderID] counts as [challenge]'s
// creator's friend for this battle: they follow each other, or it is
// friends-only and the creator picked them by name.
func friendsForBattle(challenge Challenge, responderID string) (bool, error) {
	creator, err1 := strconv.Atoi(challenge.CreatorID)
	responder, err2 := strconv.Atoi(responderID)
	cid, err3 := strconv.Atoi(challenge.ID)
	if err1 != nil || err2 != nil || err3 != nil {
		return false, nil
	}
	var friends bool
	err := db.QueryRow(`
		SELECT (EXISTS (SELECT 1 FROM follows WHERE follower_id = $1 AND following_id = $2)
		        AND EXISTS (SELECT 1 FROM follows WHERE follower_id = $2 AND following_id = $1))
		    OR EXISTS (SELECT 1 FROM challenges c
		                 JOIN challenge_visible_to v ON v.challenge_id = c.id
		                WHERE c.id = $3 AND c.visibility = 'friends' AND v.user_id = $2)`,
		creator, responder, cid).Scan(&friends)
	return friends, err
}

// battleRefusal says why [responderID] may not answer [challenge], in words
// for them, or "" when they may. Only the battle rules: whether they may
// see it at all is viewerMayWatch, and whether the post takes answers is
// the challenge's own business (validateChallengeResponseSubmission).
func battleRefusal(challenge Challenge, responderID string) string {
	if responderID == "" {
		return ""
	}
	if responderID == challenge.CreatorID {
		return "This is your own challenge. Somebody else has to answer it."
	}
	creator, found1 := GetUserByID(challenge.CreatorID)
	responder, found2 := GetUserByID(responderID)
	if !found1 || !found2 {
		return "Could not check your level right now. Try again in a moment."
	}
	theirs, yours := battleLeague(creator), battleLeague(responder)
	gap := tierOf(theirs) - tierOf(yours)
	if gap < 0 {
		gap = -gap
	}
	if gap <= maxLeagueDiff {
		return ""
	}
	friends, err := friendsForBattle(challenge, responderID)
	if queryFailed("checking whether user "+responderID+" and the creator of challenge "+
		challenge.ID+" are friends", "holding them to the level rule", err) {
		friends = false
	}
	if friends {
		return ""
	}
	you := "You're in " + yours
	if yours == "Unranked" {
		you = "You haven't had a battle decided yet, so you start in Bronze"
	}
	return fmt.Sprintf("This challenge is from a %s player. %s, so you can battle %s "+
		"players, or friends at any level. Win battles to move up.",
		theirs, you, leaguesInReach(tierOf(yours)))
}
