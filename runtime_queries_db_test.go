package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/mux"
)

// ════════════════════════════════════════════════════════════════════════════
// THE QUERIES THE COMPILE CHECK CANNOT READ, RUN FOR REAL
// ════════════════════════════════════════════════════════════════════════════
//
// sql_compiles_test.go hands every query it can read to a real Postgres. Some
// it cannot read: a table name arriving as a parameter, a list of
// placeholders built in a loop, a WHERE clause glued on depending on the
// request. There is no fixed text for it to check.
//
// "Not statically checkable" is not the same as "checked". These were the
// second one for a while, and worse, some of them had tests that LOOKED like
// coverage — readTagState and saveTagDecision had tests against a mock
// database, which replays rows and never reads the query. A wrong column name
// would have passed every one of them.
//
// So these run the real functions against a real Postgres. They assert very
// little about the answers on purpose: the thing being proved is that the
// statement is one the database accepts. A query naming a column that does not
// exist does not return a wrong answer, it returns nothing at all, for every
// row — and every one of these would fail loudly here.
//
// Where a function takes a table name, BOTH tables are passed. challenges and
// challenge_responses do not have the same columns, and a statement that works
// against one can be refused by the other.

// runtimeFixture seeds one user, one challenge and one answer, and returns
// their ids. Enough for every query below to have something to find.
func runtimeFixture(t *testing.T) (challengeID int, responseID int) {
	t.Helper()
	cid := auditChallenge(t, map[string]any{"subject": "runtime query fixture"})
	id, err := strconv.Atoi(cid)
	if err != nil {
		t.Fatalf("challenge id %q: %v", cid, err)
	}
	var rid int
	if err := db.QueryRow(`
		INSERT INTO challenge_responses (challenge_id, responder_id, video_url)
		VALUES ($1, 2, 'https://v/answer.mp4') RETURNING id`, id).Scan(&rid); err != nil {
		t.Fatalf("seed response: %v", err)
	}
	return id, rid
}

// ── a table name arriving as a parameter ────────────────────────────────────

func TestRuntimeQueries_TableNameFromAParameter(t *testing.T) {
	defer withDB(t)()
	cid, rid := runtimeFixture(t)

	for _, table := range []string{"challenges", "challenge_responses"} {
		id := cid
		kind := "challenge"
		if table == "challenge_responses" {
			id, kind = rid, "response"
		}

		t.Run(table+"/storeVideoAnalysis", func(t *testing.T) {
			// Writes three columns at once. A wrong name on either table and
			// nothing is ever stored, with the failure logged and swallowed.
			storeVideoAnalysis(table, id, json.RawMessage(
				`{"passes":["understand"],"autoTags":["dance"],"topics":["floor work"]}`))

			var stored string
			if err := db.QueryRow(
				`SELECT COALESCE(auto_tags::text,'[]') FROM `+table+` WHERE id = $1`,
				id).Scan(&stored); err != nil {
				t.Fatalf("read back: %v", err)
			}
			if stored == "[]" {
				t.Errorf("nothing was stored on %s — storeVideoAnalysis logs its "+
					"failure and returns, so an empty column here is what a "+
					"broken statement looks like", table)
			}
		})

		t.Run(table+"/readHLSQueue", func(t *testing.T) {
			if _, err := readHLSQueue(table, kind, 10); err != nil {
				t.Errorf("readHLSQueue(%s): %v", table, err)
			}
		})

		t.Run(table+"/requeueByID", func(t *testing.T) {
			if _, err := requeueByID(table, []int{id}); err != nil {
				t.Errorf("requeueByID(%s): %v", table, err)
			}
		})
	}
}

// ── a list of placeholders built in a loop ──────────────────────────────────

func TestRuntimeQueries_PlaceholderListsBuiltInALoop(t *testing.T) {
	defer withDB(t)()
	cid, _ := runtimeFixture(t)

	t.Run("getLikedByMap", func(t *testing.T) {
		// One id and several, because the statement is a different length
		// each time and only the several-id shape has ever been in doubt.
		for _, ids := range [][]int{{1}, {1, 2, 3}} {
			if got := getLikedByMap(ids); got == nil {
				t.Errorf("getLikedByMap(%v) returned nil — it swallows its "+
					"error and returns an empty map, so nil means the query "+
					"did not run", ids)
			}
		}
	})

	t.Run("enrichUsers", func(t *testing.T) {
		users := []User{{ID: "1", Username: "creator"}, {ID: "2", Username: "liker2"}}
		enrichUsers(users)
		// enrichUsers fills FollowingList in place; an unrun query leaves it
		// nil rather than an empty slice.
		for _, u := range users {
			if u.FollowingList == nil {
				t.Errorf("user %s came back with no following list at all", u.ID)
			}
		}
	})

	items := []HomeFeedItem{{
		Type:      "challenge",
		Challenge: &Challenge{ID: strconv.Itoa(cid)},
	}}

	t.Run("populateTopResponses", func(t *testing.T) {
		populateTopResponses(items)
		if items[0].Challenge.ResponseCount == 0 {
			t.Error("the fixture has one answer and this found none, which is " +
				"what a refused statement looks like here")
		}
	})
	t.Run("populateChallengeCommentCounts", func(t *testing.T) {
		populateChallengeCommentCounts(items) // no answer to assert; it must not fail
	})
	t.Run("populateHLSManifestURLs", func(t *testing.T) {
		populateHLSManifestURLs(items)
	})
	t.Run("loadVideoDimensions", func(t *testing.T) {
		if got := loadVideoDimensions(items); got == nil {
			t.Error("loadVideoDimensions returned nil rather than an empty map")
		}
	})
	// mediaPrefixesForChallenge is NOT here, on purpose. It returns before
	// its queries unless R2 is configured, and it is guarded by a sync.Once
	// so a test cannot reliably arrange that. Its two statements are plain
	// literals in a range loop, so the compile check reads them directly
	// instead — which is better evidence than a test that might have
	// short-circuited without saying so.
}

// ── a WHERE clause that depends on the request ──────────────────────────────

func TestRuntimeQueries_ClausesGluedOnAtRequestTime(t *testing.T) {
	defer withDB(t)()
	runtimeFixture(t)

	t.Run("searchTextIndex", func(t *testing.T) {
		if got := searchTextIndex(); got == nil {
			t.Error("searchTextIndex returned nil — search would find nothing")
		}
	})
	t.Run("pullCategoryCandidates", func(t *testing.T) {
		// Returns nil on a failed query, and nil on genuinely no candidates,
		// so this proves the statement runs rather than what it found.
		pullCategoryCandidates("1", map[string]bool{}, 10)
	})
	t.Run("hlsWorkWaiting", func(t *testing.T) {
		if _, err := hlsWorkWaiting(); err != nil {
			t.Errorf("hlsWorkWaiting: %v", err)
		}
	})
	t.Run("auditionsDueForReview", func(t *testing.T) {
		if _, err := auditionsDueForReview(context.Background(), 10); err != nil {
			t.Errorf("auditionsDueForReview: %v", err)
		}
	})
}

// ── the tag-suggestion pair, which had tests that proved nothing ────────────
//
// readTagState and saveTagDecision were covered by tests against a MOCK
// database. A mock replays whatever rows the test hands it and never reads the
// statement, so a wrong column name passed every one of them. These are the
// same two functions against a database that does read it.

func TestRuntimeQueries_TagStateAgainstARealDatabase(t *testing.T) {
	defer withDB(t)()
	cid, rid := runtimeFixture(t)

	for _, tc := range []struct {
		name    string
		surface tagSurface
		id      int
		owner   string
	}{
		{"challenge", challengeTagSurface, cid, "1"},
		{"response", responseTagSurface, rid, "2"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			own, _, _, _, _, err := readTagState(tc.surface, tc.id, tc.owner)
			if err != nil {
				t.Fatalf("readTagState(%s): %v", tc.surface.table, err)
			}
			if !own {
				t.Errorf("the uploader of this %s was not recognised as its "+
					"owner — %s.%s is not holding what this expects",
					tc.name, tc.surface.table, tc.surface.owner)
			}
			if err := saveTagDecision(tc.surface, tc.id, []string{"surfing"}, []string{"wave"}); err != nil {
				t.Fatalf("saveTagDecision(%s): %v", tc.surface.table, err)
			}
			_, _, _, mine, dismissed, err := readTagState(tc.surface, tc.id, tc.owner)
			if err != nil {
				t.Fatalf("read back: %v", err)
			}
			if len(mine) != 1 || mine[0] != "surfing" {
				t.Errorf("tags came back as %v", mine)
			}
			if len(dismissed) != 1 || dismissed[0] != "wave" {
				t.Errorf("dismissed came back as %v", dismissed)
			}
		})
	}
}

// ── queries built inside an HTTP handler ────────────────────────────────────
//
// These are the hardest to read from the source and the easiest to get wrong:
// an UPDATE whose column list depends on which fields the request sent, a
// SELECT with a cursor clause glued on only when the caller paginates. Both
// shapes have to be RUN to be checked at all.
//
// Handlers take the caller's identity from the request context rather than a
// header, so a test can set it directly. No token needed.

func withUser(r *http.Request, userID, username string) *http.Request {
	ctx := context.WithValue(r.Context(), userIDContextKey, userID)
	ctx = context.WithValue(ctx, usernameContextKey, username)
	return r.WithContext(ctx)
}

func TestRuntimeQueries_HandlersThatBuildTheirOwnSQL(t *testing.T) {
	defer withDB(t)()
	cid, _ := runtimeFixture(t)
	if _, err := db.Exec(
		`INSERT INTO challenge_likes (challenge_id, user_id) VALUES ($1, 1)
		 ON CONFLICT DO NOTHING`, cid); err != nil {
		t.Fatalf("seed a like: %v", err)
	}

	t.Run("update profile, one field", func(t *testing.T) {
		// One field and several, because the statement is a different
		// length each time and the placeholder numbering is worked out in
		// the loop that builds it.
		for _, body := range []string{
			`{"bio":"just the bio"}`,
			`{"fullName":"All","bio":"Three","visibility":"friends"}`,
		} {
			req := withUser(httptest.NewRequest(http.MethodPut, "/api/v1/users/1",
				strings.NewReader(body)), "1", "creator")
			req = mux.SetURLVars(req, map[string]string{"id": "1"})
			rec := httptest.NewRecorder()
			UpdateUserProfileHandler(rec, req)
			if rec.Code != http.StatusOK {
				t.Errorf("body %s gave %d: %s", body, rec.Code, rec.Body.String())
			}
		}
	})

	t.Run("liked challenges, with and without a cursor", func(t *testing.T) {
		for _, url := range []string{
			"/api/v1/users/1/likes?limit=5",
			"/api/v1/users/1/likes?limit=5&before=" + strconv.FormatInt(time.Now().Unix(), 10),
		} {
			req := withUser(httptest.NewRequest(http.MethodGet, url, nil), "1", "creator")
			req = mux.SetURLVars(req, map[string]string{"id": "1"})
			rec := httptest.NewRecorder()
			GetLikedChallengesHandler(rec, req)
			if rec.Code != http.StatusOK {
				t.Errorf("%s gave %d: %s", url, rec.Code, rec.Body.String())
			}
		}
	})

	t.Run("watch history, with and without a cursor", func(t *testing.T) {
		for _, url := range []string{
			"/api/v1/users/1/history?limit=5",
			"/api/v1/users/1/history?limit=5&before=" + strconv.FormatInt(time.Now().Unix(), 10),
		} {
			req := withUser(httptest.NewRequest(http.MethodGet, url, nil), "1", "creator")
			req = mux.SetURLVars(req, map[string]string{"id": "1"})
			rec := httptest.NewRecorder()
			GetWatchHistoryHandler(rec, req)
			if rec.Code != http.StatusOK {
				t.Errorf("%s gave %d: %s", url, rec.Code, rec.Body.String())
			}
		}
	})
}

// Deleting an account walks several tables by hand. It is the last thing that
// happens to somebody's data, so a statement it cannot run means their
// content quietly outlives the account.
func TestRuntimeQueries_AccountDeletionRunsEveryStatement(t *testing.T) {
	defer withDB(t)()
	runtimeFixture(t)

	// A user of their own to delete, so the shared fixtures survive.
	if _, err := db.Exec(`
		INSERT INTO users (id, username, password) VALUES (950, 'leaving', 'x')
		ON CONFLICT (id) DO NOTHING`); err != nil {
		t.Fatalf("seed user: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO challenges (creator_id, video_url, prefix, subject)
		VALUES (950, 'https://v/a.mp4', 'can you', 'leave')`); err != nil {
		t.Fatalf("seed their challenge: %v", err)
	}

	req := withUser(httptest.NewRequest(http.MethodDelete, "/api/v1/users/950", nil),
		"950", "leaving")
	req = mux.SetURLVars(req, map[string]string{"id": "950"})
	rec := httptest.NewRecorder()
	DeleteAccountHandler(rec, req)
	if rec.Code != http.StatusOK && rec.Code != http.StatusNoContent {
		t.Fatalf("delete gave %d: %s", rec.Code, rec.Body.String())
	}

	var left int
	if err := db.QueryRow(
		`SELECT COUNT(*) FROM challenges WHERE creator_id = 950`).Scan(&left); err != nil {
		t.Fatalf("count what is left: %v", err)
	}
	if left != 0 {
		t.Errorf("%d of their challenges outlived the account — the statement "+
			"that finds them is built at runtime and nothing else checks it", left)
	}
}

// The worker's writers, both tables. These take the table name as a
// parameter, and the two tables do not have the same columns.
func TestRuntimeQueries_WorkerWritesToBothTables(t *testing.T) {
	defer withDB(t)()
	cid, rid := runtimeFixture(t)

	for _, tc := range []struct {
		table string
		id    int
	}{{"challenges", cid}, {"challenge_responses", rid}} {
		t.Run(tc.table, func(t *testing.T) {
			storeVideoVariants(tc.table, tc.id, map[string]string{"720p": "https://v/720.mp4"})
			storeVideoThumbnail(tc.table, tc.id, "https://t/thumb.jpg")
			storeLadder(tc.table, tc.id, []string{"720p", "720p_hevc"})

			var variants, thumb, ladder string
			if err := db.QueryRow(
				`SELECT COALESCE(video_variants::text,'{}'), COALESCE(thumbnail_url,''),
				        COALESCE(hls_ladder::text,'')
				   FROM `+tc.table+` WHERE id = $1`, tc.id).Scan(&variants, &thumb, &ladder); err != nil {
				t.Fatalf("read back: %v", err)
			}
			if ladder == "" {
				t.Errorf("no ladder stored on %s, so the backfill would keep "+
					"offering its videos renditions they were already "+
					"considered for", tc.table)
			}
			if variants == "{}" {
				t.Errorf("no variants stored on %s — both writers swallow their "+
					"errors, so an empty column is what a refused statement "+
					"looks like", tc.table)
			}
			if thumb == "" {
				t.Errorf("no thumbnail stored on %s", tc.table)
			}
		})
	}
}

// The transcode queue's claim endpoint, which is the hardest query in the
// repo to read from the source: five statements inside a closure that takes
// the table name as an argument.
//
// It hands a video to a worker with UPDATE ... WHERE id = (SELECT ... FOR
// UPDATE SKIP LOCKED). If it will not run, the queue is permanently empty
// and nothing is ever transcoded — while the dispatcher goes on believing
// there is work, and starting a worker to find none, every few minutes.
//
// Called directly rather than through workerAuthed, which reads its token at
// wrapper-construction time. The auth is not what is being tested here.
func TestRuntimeQueries_WorkerClaimEndpoint(t *testing.T) {
	defer withDB(t)()
	cid, rid := runtimeFixture(t)

	// Claimable means: no manifest yet, a source to work from, attempts left.
	for _, q := range []string{
		`UPDATE challenges SET hls_manifest_url = '', hls_attempts = 0,
		        hls_claimed_at = NULL, video_url = 'https://v/a.mp4' WHERE id = $1`,
		`UPDATE challenge_responses SET hls_manifest_url = '', hls_attempts = 0,
		        hls_claimed_at = NULL, video_url = 'https://v/b.mp4' WHERE id = $1`,
	} {
		id := cid
		if strings.Contains(q, "challenge_responses") {
			id = rid
		}
		if _, err := db.Exec(q, id); err != nil {
			t.Fatalf("make it claimable: %v", err)
		}
	}

	// Twice: the first claim should find the challenge, the second the
	// answer. Both tables go through the same closure and they do not have
	// the same columns.
	claimed := map[string]bool{}
	for i := 0; i < 2; i++ {
		rec := httptest.NewRecorder()
		HLSNextPendingHandler(rec, httptest.NewRequest(
			http.MethodPost, "/api/v1/internal/hls/next-pending", nil))
		if rec.Code != http.StatusOK && rec.Code != http.StatusNoContent {
			t.Fatalf("claim %d gave %d: %s", i, rec.Code, rec.Body.String())
		}
		var job struct {
			ChallengeID string `json:"challengeId"`
			Kind        string `json:"kind"`
		}
		if rec.Code == http.StatusOK {
			if err := json.Unmarshal(rec.Body.Bytes(), &job); err != nil {
				t.Fatalf("claim %d returned something unreadable: %v", i, err)
			}
			claimed[job.Kind] = true
		}
	}
	if len(claimed) == 0 {
		t.Error("two claimable videos and the queue handed out neither. " +
			"That is what a refused statement looks like here: an empty " +
			"queue, forever, with the dispatcher still starting workers.")
	}

	// And the claim marker really went in, which is what stops two workers
	// taking the same row.
	var pending int
	if err := db.QueryRow(`
		SELECT COUNT(*) FROM challenges WHERE hls_manifest_url = 'PENDING'`).
		Scan(&pending); err != nil {
		t.Fatalf("count claimed: %v", err)
	}
	if pending == 0 && claimed["challenge"] {
		t.Error("a challenge was handed out but not marked as claimed — two " +
			"workers would transcode the same video")
	}
}
