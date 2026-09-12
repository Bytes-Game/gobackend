package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// ════════════════════════════════════════════════════════════════════════════
// THE ONE THING HERE THAT CANNOT BE UNDONE
// ════════════════════════════════════════════════════════════════════════════
//
// Everything else admin can do is recoverable. A re-queue costs runner time.
// This destroys somebody's video, and the answers other people posted to it.
//
// So the tests are about the SHAPE of the request, not the deleting: what it
// refuses to do at all, and what it tells you afterwards. Those are the parts
// that stop a mistake, and they are all reachable without a database because
// every one of them is checked before the database is touched.

func postDelete(t *testing.T, body string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/admin/media/delete",
		strings.NewReader(body))
	AdminDeleteMediaHandler(rec, req)
	return rec
}

func TestAdminDelete_WillNotDeleteAnythingYouDidNotName(t *testing.T) {
	// The property that matters most. Every other admin endpoint has a batch
	// mode — "the oldest fifty", "everything matching" — and this one must
	// never grow one: a batch delete is one mistyped number away from
	// emptying the app.
	for _, body := range []string{
		`{}`,
		`{"confirm":"yes, delete these permanently"}`,
		`{"ids":[],"confirm":"yes, delete these permanently"}`,
		`{"limit":50,"confirm":"yes, delete these permanently"}`,
	} {
		if got := postDelete(t, body).Code; got != http.StatusBadRequest {
			t.Errorf("%s got %d, want %d — a request naming no videos must "+
				"never delete anything", body, got, http.StatusBadRequest)
		}
	}

	// And the source has no way to choose rows for itself.
	src := readSourceFile(t, "admin_media_delete.go")
	for _, forbidden := range []string{"ORDER BY", "LIMIT $", "WHERE hls_", "req.Limit"} {
		if strings.Contains(src, forbidden) {
			t.Errorf("%q appears in the delete endpoint — it has grown a way "+
				"to pick videos on its own", forbidden)
		}
	}
}

func TestAdminDelete_NeedsTheConfirmPhrase(t *testing.T) {
	// A retried curl, a wrong path in a script, a copied command — none of
	// them should destroy anything without saying what they are for.
	for _, body := range []string{
		`{"ids":[11]}`,
		`{"ids":[11],"confirm":"yes"}`,
		`{"ids":[11],"confirm":"Yes, delete these permanently"}`,
	} {
		if got := postDelete(t, body).Code; got != http.StatusBadRequest {
			t.Errorf("%s got %d, want %d", body, got, http.StatusBadRequest)
		}
	}
}

func TestAdminDelete_IsBounded(t *testing.T) {
	ids := make([]string, adminDeleteMaxBatch+1)
	for i := range ids {
		ids[i] = "1"
	}
	body := `{"ids":[` + strings.Join(ids, ",") + `],"confirm":"` +
		adminDeleteConfirmPhrase + `"}`
	if got := postDelete(t, body).Code; got != http.StatusBadRequest {
		t.Errorf("got %d, want %d for more than %d ids", got,
			http.StatusBadRequest, adminDeleteMaxBatch)
	}
	// And the ceiling stays far below the re-queue endpoint's, because
	// getting a re-queue wrong costs runner time and getting this wrong
	// costs videos.
	if adminDeleteMaxBatch >= requeueMaxBatch {
		t.Errorf("one call can delete %d videos, which is not below the %d a "+
			"re-queue may touch — the irreversible endpoint should be the "+
			"tighter one", adminDeleteMaxBatch, requeueMaxBatch)
	}
}

func TestAdminDelete_ChecksTheRequestBeforeTheDatabase(t *testing.T) {
	// db is nil here, which is the situation exactly: a bad request must be
	// answered as a bad request whether or not the database is up. Answering
	// "service unavailable" sends whoever sent it looking in the wrong place.
	if got := postDelete(t, `{"ids":[1]}`).Code; got != http.StatusBadRequest {
		t.Errorf("a request with no confirm got %d while the database was "+
			"down, want %d", got, http.StatusBadRequest)
	}
}

func TestAdminDelete_SaysWhatElseWentWithIt(t *testing.T) {
	// Deleting a battle takes the answers people posted to it — somebody
	// else's video. The count has to be taken BEFORE the delete, because
	// afterwards there is nothing left to count, and it has to reach the
	// reply, because otherwise nothing anywhere ever says it happened.
	body := funcBody(readSourceFile(t, "admin_media_delete.go"),
		"func AdminDeleteMediaHandler(")
	count := strings.Index(body, "countChallengeResponses(")
	del := strings.Index(body, "DeleteChallengeByID(")
	if count < 0 || del < 0 {
		t.Fatal("the delete loop no longer counts responses or no longer deletes")
	}
	if count > del {
		t.Error("responses are counted AFTER the delete, by which time the " +
			"rows are gone and the answer is always zero")
	}

	out, err := json.Marshal(adminDeleteResponse{
		Deleted: []adminDeletedVideo{{ID: 46, Responses: 1}},
	})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if !strings.Contains(string(out), "responsesAlsoDeleted") {
		t.Errorf("the reply does not say how many answers went with the "+
			"video: %s", out)
	}
}

func TestAdminDelete_ATypoIsNotReportedAsSuccess(t *testing.T) {
	// "deleted 4" when one id named nothing reads as a confirmation that
	// four things went. Three did.
	out, err := json.Marshal(adminDeleteResponse{
		Deleted:  []adminDeletedVideo{{ID: 11}},
		NotFound: []int{9999},
	})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if !strings.Contains(string(out), "notFound") {
		t.Errorf("ids that named nothing are not reported back: %s", out)
	}

	// And "no such challenge" must not be reported as a database failure.
	// Matched on the sentinel the delete actually returns, so rewording the
	// message cannot quietly turn every typo into a reported failure.
	if !isNoSuchChallenge(fmt.Errorf("%w: %d", errChallengeNotFound, 11)) {
		t.Error("a missing id is treated as a real failure, so a typo looks " +
			"like the database is broken")
	}
	if isNoSuchChallenge(errors.New("connection refused")) {
		t.Error("a real database failure is reported as a missing id, so a " +
			"delete that never happened looks like a typo")
	}
}

func TestAdminDelete_IsBehindTheAdminLoginAndPostOnly(t *testing.T) {
	t.Setenv("ADMIN_USER", "admin")
	t.Setenv("ADMIN_PASS", "letmein")
	rec := httptest.NewRecorder()
	adminOnly(AdminDeleteMediaHandler)(rec,
		httptest.NewRequest(http.MethodPost, "/admin/media/delete", strings.NewReader(`{}`)))
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("no password got %d, want %d", rec.Code, http.StatusUnauthorized)
	}

	main := readSourceFile(t, "main.go")
	line := ""
	for _, l := range strings.Split(main, "\n") {
		if strings.Contains(l, `"/admin/media/delete"`) {
			line = l
		}
	}
	if line == "" {
		t.Fatal("the endpoint is not routed")
	}
	if !strings.Contains(line, "adminOnly(") {
		t.Error("the delete endpoint is not behind the admin login")
	}
	if strings.Contains(line, `"GET"`) {
		t.Error("the delete endpoint answers GET, so a crawler or a pasted " +
			"link could destroy videos")
	}
}

func TestAdminDelete_TheMissingIdSentinelIsActuallyReturned(t *testing.T) {
	// The test above proves isNoSuchChallenge recognises the sentinel. It
	// cannot prove anything returns one — it builds the error itself.
	//
	// That gap is not theoretical: rewording the error inside
	// DeleteChallengeByID leaves every test green while every typo starts
	// being reported as a database failure. DeleteChallengeByID cannot be
	// called here (it dereferences a nil db), so the wiring is checked at the
	// source, the same way the ranker's hand-off to the spreader is.
	body := funcBody(readSourceFile(t, "database.go"), "func DeleteChallengeByID(")
	if body == "" {
		t.Fatal("DeleteChallengeByID is gone; move this test with it")
	}
	if !strings.Contains(body, "errChallengeNotFound") {
		t.Error("DeleteChallengeByID no longer returns the missing-id " +
			"sentinel.\n\nisNoSuchChallenge then matches nothing, so an id " +
			"that names no video is reported as a failed delete rather than " +
			"a typo — and the reply says the database is broken when it is " +
			"not.")
	}
}
