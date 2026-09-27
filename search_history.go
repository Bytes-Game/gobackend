package main

// search_history.go — what the search bar remembers, and letting people
// take it back.
//
// The search bar shows your recent searches when you tap it. Two things were
// missing:
//
//   - There was no way to delete one. The list lives here on the server, so
//     hiding it in the app would only have hidden it until the next load.
//     And it is not only a list: the For You ranker reads the same key and
//     leans your feed towards what you searched for (signals_negative.go).
//     "Delete this search" has to mean the feed stops using it too, or the
//     button is a lie.
//   - Opening an account from search left no trace. People expect the
//     account they looked at to be in the history, the way it is in every
//     app they know.
//
// Accounts are kept in their own list, not mixed into the searches: the
// ranker reads the searches as topics, and a user id is not a topic.

import (
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/lib/pq"
)

const (
	recentSearchAccountsPrefix = "recent_search_accounts:"

	// How many opened accounts the history keeps.
	recentSearchAccountCap = 10

	// Longer than the searches' 24 hours: those are also a feed signal and
	// fade with it, while an account you looked at is simply history.
	recentSearchAccountTTL = 30 * 24 * time.Hour
)

func recentSearchAccountsKey(userID string) string {
	return recentSearchAccountsPrefix + userID
}

// storedSearchForm is the form a search is kept in. RecordSearchQuery stores
// through it and deleting looks up through it, so "Dance " typed into the
// bar and "dance" in the history are the same entry.
func storedSearchForm(q string) string {
	q = strings.ToLower(strings.TrimSpace(q))
	if len(q) > 64 {
		q = q[:64]
	}
	return q
}

// dedupeKeepOrder drops later copies. The same search made twice is stored
// twice (the ranker counts repetition), but the bar should show it once.
func dedupeKeepOrder(in []string) []string {
	seen := make(map[string]bool, len(in))
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

// RecordSearchedAccountHandler — POST /api/v1/search/recent/account
// body {"userId": "..."}. The account moves to the top of the history.
func RecordSearchedAccountHandler(w http.ResponseWriter, r *http.Request) {
	userID := authUserID(r)
	var body struct {
		UserID string `json:"userId"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil ||
		strings.TrimSpace(body.UserID) == "" {
		http.Error(w, `{"error":"userId required"}`, http.StatusBadRequest)
		return
	}
	other := strings.TrimSpace(body.UserID)
	if _, err := strconv.ParseInt(other, 10, 64); err != nil {
		http.Error(w, `{"error":"userId must be a number"}`, http.StatusBadRequest)
		return
	}
	// Your own profile is not something you searched for.
	if userID == "" || other == userID || rdb == nil {
		writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
		return
	}
	key := recentSearchAccountsKey(userID)
	pipe := rdb.TxPipeline()
	pipe.LRem(rctx, key, 0, other)
	pipe.LPush(rctx, key, other)
	pipe.LTrim(rctx, key, 0, recentSearchAccountCap-1)
	pipe.Expire(rctx, key, recentSearchAccountTTL)
	if _, err := pipe.Exec(rctx); err != nil {
		log.Printf("search history: could not remember that %s opened %s; "+
			"it will not show in their history: %v", userID, other, err)
		http.Error(w, `{"error":"could not save"}`, http.StatusServiceUnavailable)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

// DeleteRecentSearchHandler — DELETE /api/v1/search/recent
//
//	?q=<query>        removes that search, every copy of it
//	?userId=<id>      removes that account
//	neither           clears the whole history, searches and accounts
func DeleteRecentSearchHandler(w http.ResponseWriter, r *http.Request) {
	userID := authUserID(r)
	if userID == "" || rdb == nil {
		writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
		return
	}
	q := storedSearchForm(r.URL.Query().Get("q"))
	other := strings.TrimSpace(r.URL.Query().Get("userId"))
	var err error
	switch {
	case q != "":
		err = rdb.LRem(rctx, "recent_searches:"+userID, 0, q).Err()
	case other != "":
		err = rdb.LRem(rctx, recentSearchAccountsKey(userID), 0, other).Err()
	default:
		err = rdb.Del(rctx, "recent_searches:"+userID,
			recentSearchAccountsKey(userID)).Err()
	}
	if err != nil {
		log.Printf("search history: could not delete for %s; it is still "+
			"there, and still steering their feed: %v", userID, err)
		http.Error(w, `{"error":"could not delete"}`, http.StatusServiceUnavailable)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

// recentSearchAccounts reads the opened-accounts history, newest first, with
// the name and league as they are now. Accounts that no longer exist drop
// out.
func recentSearchAccounts(userID string) []User {
	out := []User{}
	if rdb == nil || db == nil || userID == "" {
		return out
	}
	idStrs, err := rdb.LRange(rctx, recentSearchAccountsKey(userID), 0,
		recentSearchAccountCap-1).Result()
	if err != nil {
		log.Printf("search history: could not read %s's opened accounts; "+
			"showing the history without them: %v", userID, err)
		return out
	}
	ids := make([]int64, 0, len(idStrs))
	for _, s := range idStrs {
		if n, err := strconv.ParseInt(s, 10, 64); err == nil {
			ids = append(ids, n)
		}
	}
	if len(ids) == 0 {
		return out
	}
	rows, err := db.Query(`
		SELECT id, username, COALESCE(full_name, ''), COALESCE(league, ''),
		       COALESCE(visibility, 'public'), wins, losses
		  FROM users
		 WHERE id = ANY($1)`, pq.Array(ids))
	if queryFailed("the accounts in the search history",
		"showing the history without them", err) {
		return out
	}
	defer rows.Close()
	byID := make(map[string]User, len(ids))
	bad := 0
	for rows.Next() {
		var id int64
		var u User
		if scanFailed("an account in the search history",
			rows.Scan(&id, &u.Username, &u.FullName, &u.League,
				&u.Visibility, &u.Wins, &u.Losses), &bad) {
			continue
		}
		u.ID = strconv.FormatInt(id, 10)
		byID[u.ID] = u
	}
	// Newest first, the order they were opened in.
	for _, s := range idStrs {
		if u, ok := byID[s]; ok {
			out = append(out, u)
		}
	}
	return out
}
