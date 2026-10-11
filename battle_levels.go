package main

// battle_levels.go — who may battle whom.
//
// Battles are between people with about the same number of points, so they
// stay fair: you can answer a challenge from anybody within 50 points of
// you, above or below. Somebody on 1,000 points can take on anybody from
// 950 to 1,050. As your points climb towards the next league, the people
// just inside it come within reach — and when you cross its line you are
// in it (leagueFor, after every battle).
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
// The rule is worked out in one place (levelRefusals) for both of its uses:
// the battle page and the answer step ask about one challenge, and every
// feed asks about a whole page at once so the app can switch each Accept
// button on or off before anybody taps it.

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/lib/pq"
)

// leagueTier puts the leagues in order. Somebody with no decided battles is
// "Unranked" and counts as Bronze: everybody starts at the bottom.
var leagueTier = map[string]int{
	"Bronze":   1,
	"Silver":   2,
	"Gold":     3,
	"Platinum": 4,
	"Diamond":  5,
	"Ace":      6,
}

// battleRange is how many points apart two players may be and still battle,
// either way.
const battleRange = 50

// pointsOf is somebody's points: their rating, or the starting rating for a
// row that has none yet.
func pointsOf(rating int) int {
	if rating <= 0 {
		return ratingStart
	}
	return rating
}

// inRange is whether two players' points are close enough to battle.
func inRange(theirs, yours int) bool {
	gap := theirs - yours
	if gap < 0 {
		gap = -gap
	}
	return gap <= battleRange
}

// withCommas writes 1180 as "1,180".
func withCommas(n int) string {
	s := strconv.Itoa(n)
	neg := strings.HasPrefix(s, "-")
	s = strings.TrimPrefix(s, "-")
	for i := len(s) - 3; i > 0; i -= 3 {
		s = s[:i] + "," + s[i:]
	}
	if neg {
		return "-" + s
	}
	return s
}

// aPlayerIn is "a Gold player", "an Ace player", or "a new player" for
// somebody with no battle decided yet.
func aPlayerIn(league string) string {
	switch league {
	case "", "Unranked":
		return "a new player"
	case "Ace":
		return "an Ace player"
	}
	return "a " + league + " player"
}

// outOfRangeWords says, for the person trying to answer, why they cannot
// and what they can do instead.
func outOfRangeWords(theirLeague string, theirs, yours int) string {
	return fmt.Sprintf("This challenge is from %s with %s points. You have %s points, "+
		"so you can battle players from %s to %s points, or friends at any level. "+
		"Win battles to climb.",
		aPlayerIn(theirLeague), withCommas(theirs), withCommas(yours),
		withCommas(yours-battleRange), withCommas(yours+battleRange))
}

// errNoPlayer is a level check for somebody the database does not have.
var errNoPlayer = errors.New("no such player")

// levelRefusals says, for each of [chs], why [viewerID] may not answer it
// by the level rule, in words for them; a challenge they may answer is not
// in the map. Only the level rule and the friends exception: whether it is
// their own, whether they may see it and whether it takes answers at all
// are asked elsewhere.
func levelRefusals(viewerID string, chs []*Challenge) (map[string]string, error) {
	out := map[string]string{}
	viewer, err := strconv.Atoi(viewerID)
	if err != nil || db == nil || len(chs) == 0 {
		return out, nil
	}
	ids := []int64{int64(viewer)}
	for _, c := range chs {
		if n, err := strconv.ParseInt(c.CreatorID, 10, 64); err == nil {
			ids = append(ids, n)
		}
	}
	type player struct{ points, decided int }
	players := map[string]player{}
	rows, err := db.Query(`
		SELECT id, rating, wins + losses + draws FROM users WHERE id = ANY($1::int[])`,
		pq.Array(ids))
	if err != nil {
		return nil, err
	}
	bad := 0
	for rows.Next() {
		var id, rating, decided int
		if scanFailed("a player's points for the level rule",
			rows.Scan(&id, &rating, &decided), &bad) {
			continue
		}
		players[strconv.Itoa(id)] = player{pointsOf(rating), decided}
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	you, ok := players[viewerID]
	if !ok {
		return nil, errNoPlayer
	}

	// Who is out of range, before asking who is a friend: most of a page
	// usually is not, and then there is nothing more to ask.
	far := map[string]*Challenge{}
	farIDs := []int64{}
	for _, c := range chs {
		them, ok := players[c.CreatorID]
		if !ok || c.CreatorID == viewerID || inRange(them.points, you.points) {
			continue
		}
		if n, err := strconv.ParseInt(c.ID, 10, 64); err == nil {
			if _, dup := far[c.ID]; !dup {
				farIDs = append(farIDs, n)
			}
			far[c.ID] = c
		}
	}
	if len(far) == 0 {
		return out, nil
	}
	friends := map[string]bool{}
	frows, err := db.Query(`
		SELECT c.id FROM challenges c
		 WHERE c.id = ANY($2::int[])
		   AND ((EXISTS (SELECT 1 FROM follows WHERE follower_id = $1 AND following_id = c.creator_id)
		         AND EXISTS (SELECT 1 FROM follows WHERE follower_id = c.creator_id AND following_id = $1))
		     OR (c.visibility = 'friends'
		         AND EXISTS (SELECT 1 FROM challenge_visible_to v
		                      WHERE v.challenge_id = c.id AND v.user_id = $1)))`,
		viewer, pq.Array(farIDs))
	if queryFailed("checking which far-off challengers are friends of user "+viewerID,
		"holding them all to the level rule", err) {
		frows = nil
	}
	if frows != nil {
		fbad := 0
		for frows.Next() {
			var id int
			if scanFailed("a challenge from a friend", frows.Scan(&id), &fbad) {
				continue
			}
			friends[strconv.Itoa(id)] = true
		}
		if err := frows.Close(); err != nil {
			queryFailed("reading which far-off challengers are friends of user "+viewerID,
				"holding the rest to the level rule", err)
		}
	}
	for id, c := range far {
		if friends[id] {
			continue
		}
		them := players[c.CreatorID]
		out[id] = outOfRangeWords(leagueFor(them.points, them.decided), them.points, you.points)
	}
	return out, nil
}

// battleRefusal says why [responderID] may not answer [challenge], in words
// for them, or "" when they may. The battle rules only: whether they may
// see it at all is viewerMayWatch, and whether the post takes answers is
// the challenge's own business (validateChallengeResponseSubmission).
func battleRefusal(challenge Challenge, responderID string) string {
	if responderID == "" {
		return ""
	}
	if responderID == challenge.CreatorID {
		return "This is your own challenge. Somebody else has to answer it."
	}
	refusals, err := levelRefusals(responderID, []*Challenge{&challenge})
	if err != nil {
		if !errors.Is(err, errNoPlayer) {
			queryFailed("reading points for the level rule (user "+responderID+")",
				"asking them to try again", err)
		}
		return "Could not check your level right now. Try again in a moment."
	}
	return refusals[challenge.ID]
}

// markAnswerBlocks puts on each challenge of a page that [viewerID] may not
// answer why not (Challenge.AnswerBlocked), so the app shows its Accept
// button switched off — with the reason a tap away — instead of letting
// somebody record a whole video to be turned away. Their own posts and
// posts nobody can answer are left alone: the app shows no Accept button
// on those at all.
func markAnswerBlocks(viewerID string, chs []*Challenge) {
	ask := make([]*Challenge, 0, len(chs))
	for _, c := range chs {
		if c != nil && c.CreatorID != viewerID && !c.ClosedToBattles {
			ask = append(ask, c)
		}
	}
	refusals, err := levelRefusals(viewerID, ask)
	if err != nil {
		if !errors.Is(err, errNoPlayer) {
			queryFailed("reading points for the level rule on a page (user "+viewerID+")",
				"leaving every Accept button on; the answer step still checks", err)
		}
		return
	}
	for _, c := range ask {
		c.AnswerBlocked = refusals[c.ID]
	}
}
