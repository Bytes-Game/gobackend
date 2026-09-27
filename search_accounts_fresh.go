package main

import (
	"strconv"

	"github.com/lib/pq"
)

// freshenAccounts fills in the people a search found with what the database
// says about them NOW: whether their account is private, their league, their
// rating and their record.
//
// ════════════════════════════════════════════════════════════════════════════
// WHY SEARCH CANNOT JUST USE WHAT IT FOUND
// ════════════════════════════════════════════════════════════════════════════
//
// The people come back from the search index, which holds a copy of each
// account made when it was indexed. Two things were wrong with that copy:
//
//   - It never had the account's privacy setting at all, so the app could
//     not show a lock on a private account or offer the right button.
//   - Its league goes stale. A league now follows a rating that moves with
//     every decided battle and drains away at the top, and nothing re-indexes
//     an account when that happens. Search would show last month's league.
//
// So the index decides WHO matches and in what order, and the database says
// what they look like. This is at most searchAccountCap rows by primary key,
// one query.
//
// If the query fails, the people are returned as the index had them — a
// search with a stale league is better than no people at all — and the
// failure is logged, so it is not mistaken for accounts that happen to have
// no privacy setting.
func freshenAccounts(users []User) []User {
	if len(users) == 0 || db == nil {
		return users
	}
	ids := make([]int64, 0, len(users))
	for _, u := range users {
		if n, err := strconv.ParseInt(u.ID, 10, 64); err == nil {
			ids = append(ids, n)
		}
	}
	if len(ids) == 0 {
		return users
	}
	rows, err := db.Query(`
		SELECT id, COALESCE(visibility, 'public'), COALESCE(league, ''),
		       COALESCE(rating, 1000), wins, losses, COALESCE(full_name, '')
		  FROM users
		 WHERE id = ANY($1)`, pq.Array(ids))
	if queryFailed("the privacy and league of the people search found",
		"showing them as the search index has them", err) {
		return users
	}
	defer rows.Close()

	type fresh struct {
		visibility, league, fullName string
		rating, wins, losses         int
	}
	byID := make(map[string]fresh, len(ids))
	bad := 0
	for rows.Next() {
		var id int64
		var f fresh
		if scanFailed("a person search found",
			rows.Scan(&id, &f.visibility, &f.league, &f.rating, &f.wins,
				&f.losses, &f.fullName), &bad) {
			continue
		}
		byID[strconv.FormatInt(id, 10)] = f
	}
	// Whatever was read before a failure is still worth using.
	queryFailed("reading the people search found",
		"showing the ones not read as the search index has them", rows.Err())

	out := make([]User, len(users))
	for i, u := range users {
		if f, ok := byID[u.ID]; ok {
			u.Visibility = f.visibility
			u.League = f.league
			u.Rating = f.rating
			u.Wins = f.wins
			u.Losses = f.losses
			if f.fullName != "" {
				u.FullName = f.fullName
			}
		}
		out[i] = u
	}
	return out
}
