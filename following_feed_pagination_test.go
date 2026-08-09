package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

// The Following tab used to serve exactly one page and then dead-end.
//
// Its SQL carried `LIMIT $2` with the PAGE size, but pagination was applied
// afterwards in Go by slicing [offset:offset+limit] out of that same result.
// For page 2 the offset equals the limit, so the slice start was always at or
// past the end of a slice that could never be longer than the limit — every
// page after the first came back empty, forever, no matter how much the
// creators a user follows had posted. Confirmed against the live API before
// the fix: page 1 limit 5 returned 5 items with hasMore=true, and page 2
// limit 5 returned 0.
//
// The SQL limit has to cover everything the slicing will walk through:
// offset + limit, plus one probe row so hasMore is evidence rather than a
// guess.

// followingRows builds n challenge rows in the column order the handler scans,
// newest first, with ids "1".."n" so a test can name the exact rows a page
// should contain.
func followingRows(n int) *sqlmock.Rows {
	rows := sqlmock.NewRows([]string{
		"id", "creator_id", "username", "league", "video_url",
		"thumbnail_url", "prefix", "subject", "visibility", "status",
		"views", "likes", "created_at", "expires_at", "response_count",
	})
	now := time.Now()
	for i := 1; i <= n; i++ {
		created := now.Add(-time.Duration(i) * time.Minute)
		rows.AddRow(
			strconv.Itoa(i), 7, "creator", "Gold", "https://cdn/v.mp4",
			"https://cdn/t.jpg", "Who is better at", "Dancing", "arena", "open",
			100, 5, created, created.Add(24*time.Hour), 0,
		)
	}
	return rows
}

// callFollowingFeed drives the handler and returns the decoded page.
func callFollowingFeed(t *testing.T, page, limit int) (ids []string, hasMore bool) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet,
		"/api/v1/feed/following/v2?page="+strconv.Itoa(page)+
			"&limit="+strconv.Itoa(limit), nil)
	req = req.WithContext(
		context.WithValue(req.Context(), userIDContextKey, "1"))
	rec := httptest.NewRecorder()

	FollowingFeedV2Handler(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body %q)", rec.Code, rec.Body.String())
	}
	var body struct {
		Items []struct {
			Challenge struct {
				ID string `json:"id"`
			} `json:"challenge"`
		} `json:"items"`
		HasMore bool `json:"hasMore"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v (body %q)", err, rec.Body.String())
	}
	for _, it := range body.Items {
		ids = append(ids, it.Challenge.ID)
	}
	return ids, body.HasMore
}

func TestFollowingFeedV2_SecondPageIsNotEmpty(t *testing.T) {
	resetRedis(t)
	mock, cleanup := withMockDB(t)
	defer cleanup()
	mock.MatchExpectationsInOrder(false)

	// 21 rows available = a full page 2 plus the probe row. The handler must
	// ASK for that many: this WithArgs is the regression guard. With the old
	// `LIMIT $2` = limit it asked for 10 and page 2 could only ever be empty.
	mock.ExpectQuery("FROM challenges").
		WithArgs("1", 21).
		WillReturnRows(followingRows(21))

	ids, hasMore := callFollowingFeed(t, 2, 10)

	if len(ids) != 10 {
		t.Fatalf("page 2 returned %d items (%v), want 10 — the Following tab "+
			"dead-ending after one page is the bug this test exists for", len(ids), ids)
	}
	if ids[0] != "11" || ids[9] != "20" {
		t.Fatalf("page 2 = %v, want ids 11..20 — page 2 must resume exactly "+
			"where page 1 stopped, with no gap and no repeat", ids)
	}
	if !hasMore {
		t.Fatal("hasMore = false with a 21st row waiting; the client would " +
			"stop scrolling one page early")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestFollowingFeedV2_HasMoreIsFalseOnTheLastPage(t *testing.T) {
	resetRedis(t)
	mock, cleanup := withMockDB(t)
	defer cleanup()
	mock.MatchExpectationsInOrder(false)

	// 15 rows: page 2 holds the last 5 and there is no probe row behind them.
	mock.ExpectQuery("FROM challenges").
		WithArgs("1", 21).
		WillReturnRows(followingRows(15))

	ids, hasMore := callFollowingFeed(t, 2, 10)

	if len(ids) != 5 {
		t.Fatalf("page 2 returned %d items (%v), want the 5 remaining", len(ids), ids)
	}
	if hasMore {
		t.Fatal("hasMore = true on the last page — the client fetches a page " +
			"that does not exist and shows a spinner that never resolves")
	}
}

// A page that fills EXACTLY must not claim there is more behind it. The old
// `len(items) >= limit` test could not tell "full page" from "full page with
// more waiting", so the final page of a feed whose size happened to be a
// multiple of the page size always over-promised.
func TestFollowingFeedV2_ExactlyFullLastPageDoesNotOverPromise(t *testing.T) {
	resetRedis(t)
	mock, cleanup := withMockDB(t)
	defer cleanup()
	mock.MatchExpectationsInOrder(false)

	mock.ExpectQuery("FROM challenges").
		WithArgs("1", 21).
		WillReturnRows(followingRows(20))

	ids, hasMore := callFollowingFeed(t, 2, 10)

	if len(ids) != 10 {
		t.Fatalf("page 2 = %d items, want 10", len(ids))
	}
	if hasMore {
		t.Fatal("hasMore = true on an exactly-full final page")
	}
}

func TestFollowingFeedV2_FirstPageStillWorks(t *testing.T) {
	resetRedis(t)
	mock, cleanup := withMockDB(t)
	defer cleanup()
	mock.MatchExpectationsInOrder(false)

	// page 1: offset 0, so fetch is limit+1.
	mock.ExpectQuery("FROM challenges").
		WithArgs("1", 11).
		WillReturnRows(followingRows(11))

	ids, hasMore := callFollowingFeed(t, 1, 10)

	if len(ids) != 10 || ids[0] != "1" || ids[9] != "10" {
		t.Fatalf("page 1 = %v, want ids 1..10", ids)
	}
	if !hasMore {
		t.Fatal("hasMore = false with an 11th row waiting")
	}
}
