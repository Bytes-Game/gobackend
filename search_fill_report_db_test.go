package main

// The search service (Meilisearch) is sent every account and video when the
// server starts, and each new one as it comes. These check that the server
// says whether the search service accepted them. It used to print "Indexed
// 3 users" whatever the answer was: on 16 November another batch sent at the
// same start was refused with "too many requests", and those lines could not
// show whether theirs had been too.
//
// Each test passes its own stand-in to the code it checks, and never swaps
// the server's own connection (meili): background work left over from other
// tests reads that one, and changing it under them is a race.

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"strings"
	"sync"
	"testing"

	"github.com/meilisearch/meilisearch-go"
)

// standInSearch answers like the search service. Adding documents to an
// index gets the answer in [refuse] for that index, if there is one (429
// for "too many requests"), and "accepted" otherwise. It counts what it
// accepted, by index, and keeps the ids.
type standInSearch struct {
	mu       sync.Mutex
	refuse   map[string]int
	accepted map[string]int
	ids      map[string]map[string]bool
}

func newStandInSearch(t *testing.T, refuse map[string]int) (*standInSearch, meilisearch.ServiceManager) {
	t.Helper()
	s := &standInSearch{refuse: refuse, accepted: map[string]int{},
		ids: map[string]map[string]bool{}}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
		if r.Method != http.MethodPost || len(parts) != 3 || parts[0] != "indexes" || parts[2] != "documents" {
			http.NotFound(w, r)
			return
		}
		index := parts[1]
		if code := s.refuse[index]; code != 0 {
			w.WriteHeader(code)
			return
		}
		var docs []map[string]any
		if err := json.NewDecoder(r.Body).Decode(&docs); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		s.mu.Lock()
		s.accepted[index] += len(docs)
		if s.ids[index] == nil {
			s.ids[index] = map[string]bool{}
		}
		for _, d := range docs {
			s.ids[index][fmt.Sprint(d["id"])] = true
		}
		s.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		fmt.Fprintf(w, `{"taskUid":1,"indexUid":%q,"status":"enqueued",`+
			`"type":"documentAdditionOrUpdate","enqueuedAt":"2026-11-16T00:00:00Z"}`, index)
	}))
	t.Cleanup(srv.Close)
	return s, meilisearch.New(srv.URL, meilisearch.WithAPIKey("test-key"))
}

func (s *standInSearch) got(index string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.accepted[index]
}

func (s *standInSearch) has(index, id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ids[index][id]
}

// loggedDuring is what the server logs while [do] runs.
func loggedDuring(do func()) string {
	var logged lockedLog
	log.SetOutput(&logged)
	defer log.SetOutput(os.Stderr)
	do()
	return logged.String()
}

// What the start-up fill is given in these tests: one account and one
// video, so a refusal costs one batch of retries, not one per hundred rows
// other tests left in the database.
var (
	fillAccounts = []User{{ID: "9701", Username: "findme"}}
	fillVideos   = []Challenge{{ID: "777002"}}
)

// At start-up, every account in the database is sent: the one made here
// among them.
func TestSearchFill_AtStartEveryAccountIsSent(t *testing.T) {
	defer withDB(t)()
	mustExec(t, `DELETE FROM users WHERE id = 9701`)
	mustExec(t, `INSERT INTO users (id, username, password) VALUES (9701, 'findme', 'x')`)
	defer mustExec(t, `DELETE FROM users WHERE id = 9701`)
	search, m := newStandInSearch(t, nil)
	said := loggedDuring(func() { seedMeilisearchData(m) })
	if !search.has("users", "9701") {
		t.Fatalf("the account in the database never reached the search service. The log:\n%s", said)
	}
	if !strings.Contains(said, "to the search service at start-up: all accepted.") {
		t.Errorf("no line saying the accounts were accepted:\n%s", said)
	}
}

func TestSearchFill_SaysTheSearchServiceAcceptedThem(t *testing.T) {
	defer withDB(t)() // a video's search text is read from the database
	search, m := newStandInSearch(t, nil)
	said := loggedDuring(func() { seedSearch(m, fillAccounts, fillVideos) })

	if search.got("users") != 1 || search.got("challenges") != 1 {
		t.Fatalf("the search service got %d accounts and %d videos, want 1 each. The log:\n%s",
			search.got("users"), search.got("challenges"), said)
	}
	for _, want := range []string{
		"search: sent 1 account to the search service at start-up: all accepted.",
		"search: sent 1 video to the search service at start-up: all accepted.",
	} {
		if !strings.Contains(said, want) {
			t.Errorf("no line saying %q:\n%s", want, said)
		}
	}
	if strings.Contains(said, "failed") {
		t.Errorf("a failure was reported when nothing failed:\n%s", said)
	}
}

func TestSearchFill_SaysWhenTheSearchServiceRefusedThem(t *testing.T) {
	defer withDB(t)()
	_, m := newStandInSearch(t, map[string]int{
		"users":      http.StatusTooManyRequests,
		"challenges": http.StatusTooManyRequests,
	})
	said := loggedDuring(func() { seedSearch(m, fillAccounts, fillVideos) })

	for _, what := range []string{"1 account", "1 video"} {
		refused := regexp.MustCompile(`search: sending ` + what +
			` to the search service at start-up failed \(.*429.*\)`)
		if !refused.MatchString(said) {
			t.Errorf("the refusal of %s is not reported:\n%s", what, said)
		}
	}
	if strings.Contains(said, "all accepted") {
		t.Errorf("something is reported accepted, though the search service refused it all:\n%s", said)
	}
}

func TestSearchAdd_SaysWhenANewVideoOrAccountIsRefused(t *testing.T) {
	defer withDB(t)()
	video := Challenge{ID: "777001"}
	account := User{ID: "9702", Username: "newone"}

	_, refusing := newStandInSearch(t, map[string]int{
		"challenges": http.StatusTooManyRequests,
		"users":      http.StatusTooManyRequests,
	})
	said := loggedDuring(func() {
		indexChallengeIn(refusing, video)
		indexUserIn(refusing, account)
	})
	for _, want := range []string{
		"search: the search service refused video 777001",
		"search: the search service refused account 9702",
	} {
		if !strings.Contains(said, want) {
			t.Errorf("no line saying %q:\n%s", want, said)
		}
	}

	// Accepted: they arrive, and there is nothing to say.
	search, accepting := newStandInSearch(t, nil)
	said = loggedDuring(func() {
		indexChallengeIn(accepting, video)
		indexUserIn(accepting, account)
	})
	if search.got("challenges") != 1 || search.got("users") != 1 {
		t.Errorf("the search service got %d videos and %d accounts, want 1 each",
			search.got("challenges"), search.got("users"))
	}
	if strings.Contains(said, "refused") {
		t.Errorf("a refusal was reported when it accepted:\n%s", said)
	}
}
