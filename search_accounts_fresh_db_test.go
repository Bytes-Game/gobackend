package main

import (
	"strings"
	"testing"
)

// The people a search finds are filled in from the database: privacy,
// league, rating and record. The index copy has no privacy setting at all
// and its league goes stale; see freshenAccounts.

func TestSearchPeople_CarryPrivacyAndTheLeagueTheyHaveNow(t *testing.T) {
	defer withDB(t)()
	d := openDB(t)
	for _, q := range []string{
		`INSERT INTO users (id, username, password, visibility, league, rating, wins, losses, full_name)
		 VALUES (7001, 'maya_fresh', 'x', 'public', 'Gold', 1310, 12, 4, 'Maya Singh')
		 ON CONFLICT (id) DO NOTHING`,
		`INSERT INTO users (id, username, password, visibility, league, rating, wins, losses)
		 VALUES (7002, 'leo_private', 'x', 'friends', 'Silver', 1080, 3, 1)
		 ON CONFLICT (id) DO NOTHING`,
	} {
		if _, err := d.Exec(q); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() { _, _ = d.Exec(`DELETE FROM users WHERE id IN (7001, 7002)`) })

	// What the search index hands back: last month's league, no privacy.
	stale := []User{
		{ID: "7002", Username: "leo_private", League: "Bronze"},
		{ID: "7001", Username: "maya_fresh", League: "Bronze"},
		{ID: "999999", Username: "gone", League: "Bronze"},
	}
	got := freshenAccounts(stale)

	if len(got) != 3 {
		t.Fatalf("got %d people, want 3 — nobody may be dropped", len(got))
	}
	// The order is the search's, not the database's.
	if got[0].ID != "7002" || got[1].ID != "7001" {
		t.Fatalf("order changed: %s, %s", got[0].ID, got[1].ID)
	}
	if got[0].Visibility != "friends" {
		t.Errorf("the private account reads %q, want \"friends\" — the app "+
			"cannot show the lock", got[0].Visibility)
	}
	if got[0].League != "Silver" || got[0].Rating != 1080 {
		t.Errorf("league %q rating %d, want Silver 1080 — still the index's "+
			"stale copy", got[0].League, got[0].Rating)
	}
	if got[1].Visibility != "public" || got[1].League != "Gold" ||
		got[1].Wins != 12 || got[1].Losses != 4 || got[1].FullName != "Maya Singh" {
		t.Errorf("public account not filled in: %+v", got[1])
	}
	// An account the database no longer has is left as the index had it.
	if got[2].League != "Bronze" || got[2].Visibility != "" {
		t.Errorf("unknown account changed: %+v", got[2])
	}
}

func TestSearchPeople_AreFilledInOnTheWayOut(t *testing.T) {
	// The handler must send the ranked people through freshenAccounts; a
	// freshen nothing calls is the bug this repo keeps hitting.
	code := codeOf(t, "search.go")
	if !strings.Contains(code, "freshenAccounts(\n\t\t\trankSearchAccounts(") {
		t.Error("SearchHandler no longer fills in the people it found, so " +
			"search shows no lock on private accounts and a stale league")
	}
}
