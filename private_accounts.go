package main

// private_accounts.go — a private account's videos are for its followers.
//
// A private account (users.visibility = 'friends') showed "This account is
// private" on its profile to anybody who did not follow it, and a lock next
// to its name. But that was the app being polite: the server handed the
// same videos out everywhere else. They came up in For You, in the Search
// grid, in search results, in trending, on the profile's own video list for
// anyone who asked the server directly, and on the battle page for anyone
// with the number — signed in or not.
//
// So the rule is now the server's, once: a private account's videos are
// seen by that person and by the people who follow them, and by nobody
// else. A video shared with friends only keeps its own, narrower rule on
// top (mayWatchSQL).

import (
	"strconv"

	"github.com/lib/pq"
)

// hiddenCreators is which of [creatorIDs] are private accounts that
// [viewerID] may not see: not themselves, and not somebody they follow. An
// empty viewer is nobody signed in, who sees no private account.
//
// When the database cannot say, it says nothing is hidden and logs why, the
// same way every other read here fails: a feed that empties while the
// database is unwell would look exactly like a platform with no videos.
func hiddenCreators(viewerID string, creatorIDs []string) map[string]bool {
	if db == nil || len(creatorIDs) == 0 {
		return nil
	}
	seen := map[int]bool{}
	ids := make([]int, 0, len(creatorIDs))
	for _, s := range creatorIDs {
		if n, err := strconv.Atoi(s); err == nil && !seen[n] {
			seen[n] = true
			ids = append(ids, n)
		}
	}
	if len(ids) == 0 {
		return nil
	}
	viewer, _ := strconv.Atoi(viewerID) // 0: nobody signed in
	rows, err := db.Query(`
		SELECT u.id FROM users u
		 WHERE u.id = ANY($1::int[])
		   AND u.visibility = 'friends'
		   AND u.id <> $2
		   AND NOT EXISTS (SELECT 1 FROM follows f
		                    WHERE f.follower_id = $2 AND f.following_id = u.id)`,
		pq.Array(ids), viewer)
	if queryFailed("finding private accounts among the creators shown to user "+viewerID,
		"showing every video as though its account were public", err) {
		return nil
	}
	defer rows.Close()
	out := map[string]bool{}
	bad := 0
	for rows.Next() {
		var id int
		if scanFailed("a private account to leave out", rows.Scan(&id), &bad) {
			continue
		}
		out[strconv.Itoa(id)] = true
	}
	if err := rows.Err(); err != nil {
		queryFailed("reading private accounts among the creators shown to user "+viewerID,
			"showing some of their videos", err)
	}
	return out
}

// dropHiddenCreatorItems takes out the videos of private accounts the
// viewer does not follow.
func dropHiddenCreatorItems(viewerID string, items []HomeFeedItem) []HomeFeedItem {
	ids := make([]string, 0, len(items))
	for _, it := range items {
		ids = append(ids, getItemCreatorID(it))
	}
	hidden := hiddenCreators(viewerID, ids)
	if len(hidden) == 0 {
		return items
	}
	out := make([]HomeFeedItem, 0, len(items))
	for _, it := range items {
		if !hidden[getItemCreatorID(it)] {
			out = append(out, it)
		}
	}
	return out
}

// dropHiddenCreatorScored is the same for a ranked page.
func dropHiddenCreatorScored(viewerID string, items []ScoredItem) []ScoredItem {
	ids := make([]string, 0, len(items))
	for _, si := range items {
		ids = append(ids, defaultCreatorOf(si))
	}
	hidden := hiddenCreators(viewerID, ids)
	if len(hidden) == 0 {
		return items
	}
	out := make([]ScoredItem, 0, len(items))
	for _, si := range items {
		if !hidden[defaultCreatorOf(si)] {
			out = append(out, si)
		}
	}
	return out
}

// dropHiddenCreatorChallenges is the same for a list of videos.
func dropHiddenCreatorChallenges(viewerID string, chs []Challenge) []Challenge {
	ids := make([]string, 0, len(chs))
	for _, c := range chs {
		ids = append(ids, c.CreatorID)
	}
	hidden := hiddenCreators(viewerID, ids)
	if len(hidden) == 0 {
		return chs
	}
	out := make([]Challenge, 0, len(chs))
	for _, c := range chs {
		if !hidden[c.CreatorID] {
			out = append(out, c)
		}
	}
	return out
}

// accountHiddenFrom is whether [ownerID]'s account is private to [viewerID].
func accountHiddenFrom(viewerID, ownerID string) bool {
	return hiddenCreators(viewerID, []string{ownerID})[ownerID]
}

// viewerMayWatch is mayWatchSQL for one person ("" for nobody signed in)
// and one video. A database that cannot answer says no and logs why: a
// page that will not open is better than a private video that does.
func viewerMayWatch(viewerID, challengeID string) bool {
	cid, err := strconv.Atoi(challengeID)
	if err != nil {
		return false
	}
	viewer, _ := strconv.Atoi(viewerID)
	ok, err := mayWatch(viewer, cid)
	if queryFailed("checking whether user "+viewerID+" may watch challenge "+challengeID,
		"not showing it", err) {
		return false
	}
	return ok
}
