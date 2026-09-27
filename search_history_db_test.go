package main

// Search history: deleting from it, and accounts opened from search.
//
// Deleting has to reach the key the For You ranker reads, or the search is
// gone from the bar and still steering the feed. These go through the real
// handlers and read the key the ranker reads.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

func historyOf(t *testing.T, userID string) (recent []string, accounts []User) {
	t.Helper()
	rec := httptest.NewRecorder()
	RecentSearchesHandler(rec, withAuth(
		httptest.NewRequest("GET", "/api/v1/search/recent", nil), userID, "me"))
	if rec.Code != 200 {
		t.Fatalf("history answered %d: %s", rec.Code, rec.Body.String())
	}
	var body struct {
		Recent   []string `json:"recent"`
		Accounts []User   `json:"accounts"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("unreadable history: %v", err)
	}
	return body.Recent, body.Accounts
}

func openedFromSearch(t *testing.T, userID, other string) {
	t.Helper()
	rec := httptest.NewRecorder()
	RecordSearchedAccountHandler(rec, withAuth(
		httptest.NewRequest("POST", "/api/v1/search/recent/account",
			bytes.NewBufferString(fmt.Sprintf(`{"userId":%q}`, other))),
		userID, "me"))
	if rec.Code != 200 {
		t.Fatalf("recording %s answered %d: %s", other, rec.Code,
			rec.Body.String())
	}
}

func deleteFromHistory(t *testing.T, userID, query string) {
	t.Helper()
	rec := httptest.NewRecorder()
	DeleteRecentSearchHandler(rec, withAuth(
		httptest.NewRequest("DELETE", "/api/v1/search/recent"+query, nil),
		userID, "me"))
	if rec.Code != 200 {
		t.Fatalf("delete %q answered %d: %s", query, rec.Code,
			rec.Body.String())
	}
}

// What the ranker reads when it leans the feed towards your searches.
func searchesTheFeedUses(t *testing.T, userID string) []string {
	t.Helper()
	got, err := rdb.LRange(rctx, "recent_searches:"+userID, 0, -1).Result()
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func seedHistoryUsers(t *testing.T) {
	t.Helper()
	if _, err := openDB(t).Exec(`
		INSERT INTO users (id, username, password, full_name, league, visibility)
		VALUES (9101, 'hist_me', 'x', '', 'Silver', 'public'),
		       (9102, 'maya_h', 'x', 'Maya Singh', 'Gold', 'public'),
		       (9103, 'leo_h', 'x', '', 'Bronze', 'friends')
		ON CONFLICT (id) DO NOTHING`); err != nil {
		t.Fatalf("seed users: %v", err)
	}
}

func TestSearchHistory_OpenedAccountsShowNewestFirstWithTheirLeague(t *testing.T) {
	defer withDB(t)()
	resetRedis(t)
	seedHistoryUsers(t)

	openedFromSearch(t, "9101", "9102")
	openedFromSearch(t, "9101", "9103")
	openedFromSearch(t, "9101", "9102") // again: moves to the top, once
	openedFromSearch(t, "9101", "9101") // your own profile: not history

	_, accounts := historyOf(t, "9101")
	if len(accounts) != 2 {
		t.Fatalf("want 2 accounts, got %d: %+v", len(accounts), accounts)
	}
	if accounts[0].Username != "maya_h" || accounts[1].Username != "leo_h" {
		t.Fatalf("wrong order: %s, %s", accounts[0].Username,
			accounts[1].Username)
	}
	if accounts[0].League != "Gold" || accounts[0].FullName != "Maya Singh" {
		t.Fatalf("maya came back without her details: %+v", accounts[0])
	}
	if accounts[1].Visibility != "friends" {
		t.Fatalf("leo's account is private and the history lost that: %+v",
			accounts[1])
	}
}

func TestSearchHistory_DeletingASearchStopsTheFeedUsingIt(t *testing.T) {
	defer withDB(t)()
	resetRedis(t)
	RecordSearchQuery("9201", "Dance ")
	RecordSearchQuery("9201", "cooking")
	RecordSearchQuery("9201", "dance")

	recent, _ := historyOf(t, "9201")
	if strings.Join(recent, ",") != "dance,cooking" {
		t.Fatalf("history should list each search once, newest first: %v",
			recent)
	}

	// Typed differently from how it was stored: still the same search.
	deleteFromHistory(t, "9201", "?q=DANCE")
	recent, _ = historyOf(t, "9201")
	if strings.Join(recent, ",") != "cooking" {
		t.Fatalf("after deleting dance: %v", recent)
	}
	for _, q := range searchesTheFeedUses(t, "9201") {
		if q == "dance" {
			t.Fatal("gone from the bar but still steering the feed")
		}
	}
}

func TestSearchHistory_DeleteOneAccountOrEverything(t *testing.T) {
	defer withDB(t)()
	resetRedis(t)
	seedHistoryUsers(t)
	RecordSearchQuery("9101", "freestyle")
	openedFromSearch(t, "9101", "9102")
	openedFromSearch(t, "9101", "9103")

	deleteFromHistory(t, "9101", "?userId=9102")
	recent, accounts := historyOf(t, "9101")
	if len(accounts) != 1 || accounts[0].Username != "leo_h" {
		t.Fatalf("after deleting maya: %+v", accounts)
	}
	if len(recent) != 1 {
		t.Fatalf("deleting an account touched the searches: %v", recent)
	}

	deleteFromHistory(t, "9101", "")
	recent, accounts = historyOf(t, "9101")
	if len(recent) != 0 || len(accounts) != 0 {
		t.Fatalf("clear all left %v and %+v", recent, accounts)
	}
	if got := searchesTheFeedUses(t, "9101"); len(got) != 0 {
		t.Fatalf("cleared, but the feed still uses %v", got)
	}
}

func TestSearchHistory_IsReachableAndGoesWithTheAccount(t *testing.T) {
	strip := func(path string) string {
		src, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var keep []string
		for _, l := range strings.Split(string(src), "\n") {
			if !strings.HasPrefix(strings.TrimSpace(l), "//") {
				keep = append(keep, l)
			}
		}
		return strings.Join(keep, "\n")
	}
	routes := strip("main.go")
	for _, want := range []string{
		`authed(DeleteRecentSearchHandler)).Methods("DELETE")`,
		`"/search/recent/account", authed(RecordSearchedAccountHandler)`,
	} {
		if !strings.Contains(routes, want) {
			t.Errorf("no route: %s", want)
		}
	}
	if !strings.Contains(strip("account_delete.go"),
		"recentSearchAccountsKey(userID)") {
		t.Error("deleting an account leaves the accounts it looked at behind")
	}
}
