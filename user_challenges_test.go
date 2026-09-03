package main

import (
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

// challengeRowsForProfile builds the row shape challengeBaseQuery returns.
//
// Written out in full on purpose: the two columns at the end are the ones
// this file exists to defend, and a helper that hid them behind a default
// would let them be dropped again without a test noticing.
func challengeRowsForProfile(variants, manifest string) *sqlmock.Rows {
	return sqlmock.NewRows([]string{
		"id", "creator_id", "username", "league",
		"video_url", "thumbnail_url",
		"prefix", "subject", "visibility", "status", "views",
		"likes", "response_count", "created_at",
		"category", "emotion_tags", "energy_level",
		"video_variants", "hls_manifest_url",
	}).AddRow(
		258, 7, "kar", "bronze",
		"https://cdn/u/7/raw.mp4", "https://cdn/u/7/thumb.jpg",
		"can you", "beat this", "arena", "open", 3,
		1, 0, time.Now().Add(-90*24*time.Hour),
		"other", []byte(`[]`), "medium",
		variants, manifest,
	)
}

// ════════════════════════════════════════════════════════════════════════════
// A PROFILE SHOWS WHAT SOMEBODY MADE, NOT WHAT IS LIVE RIGHT NOW
// ════════════════════════════════════════════════════════════════════════════
//
// The profile used to read the arena list and filter it by creator. The arena
// list keeps a challenge only while it is answered or under a day old, which
// is right for an arena and wrong for a profile: an unanswered post passed
// twenty-four hours and disappeared from its own author's profile. On
// production the arena held one challenge against a catalogue of ninety-two.

func TestUserChallenges_KeepsAPostNobodyAnswered(t *testing.T) {
	mock, cleanup := withMockDB(t)
	defer cleanup()

	// The row is 90 days old and still 'open' — nobody ever answered it.
	// That is exactly the row the old path dropped.
	mock.ExpectQuery(regexp.QuoteMeta("WHERE c.creator_id = CAST($1 AS INT)")).
		WillReturnRows(challengeRowsForProfile(`{}`, ""))

	got := GetChallengesByCreator("7", true, 50, 0)
	if len(got) != 1 {
		t.Fatalf("a 90-day-old unanswered post is missing from its author's "+
			"own profile; got %d rows", len(got))
	}
	if got[0].ID != "258" {
		t.Errorf("wrong challenge came back: %q", got[0].ID)
	}
}

func TestUserChallenges_QueryHasNoClockInIt(t *testing.T) {
	// A guard on the SQL text rather than on a result, because the fault
	// being prevented is a clause creeping back in — an age window or a
	// status filter — and a mock row cannot show that. It is what hid
	// people's own posts from them.
	for _, banned := range []string{"INTERVAL", "NOW()"} {
		if strings.Contains(challengeBaseQuery, banned) {
			t.Errorf("challengeBaseQuery mentions %q. A profile is built on "+
				"this query and must not inherit the arena's 24-hour window.",
				banned)
		}
	}
	if strings.Contains(profileWhereClause, "INTERVAL") ||
		strings.Contains(profileWhereClause, "NOW()") {
		t.Error("the profile's own WHERE clause has a clock in it")
	}
}

// ════════════════════════════════════════════════════════════════════════════
// A PROFILE PLAYS WHAT THE WORKER MADE
// ════════════════════════════════════════════════════════════════════════════
//
// challengeBaseQuery did not select video_variants or hls_manifest_url, so
// every list built on it — the profile, the arena list, the friends list —
// handed the client the raw upload and no rungs at all. The reels feed fills
// those in through a separate pass, which is why the gap was invisible there
// and total everywhere else.

func TestUserChallenges_CarriesTheEncodedVersions(t *testing.T) {
	mock, cleanup := withMockDB(t)
	defer cleanup()

	variants := `{"480p":"https://cdn/hls/258/x/480p.mp4",` +
		`"720p":"https://cdn/hls/258/x/720p.mp4"}`
	mock.ExpectQuery(regexp.QuoteMeta("WHERE c.creator_id = CAST($1 AS INT)")).
		WillReturnRows(challengeRowsForProfile(variants,
			"https://cdn/hls/258/x/master.m3u8"))

	got := GetChallengesByCreator("7", true, 50, 0)
	if len(got) != 1 {
		t.Fatalf("expected one challenge, got %d", len(got))
	}
	if len(got[0].VideoVariants) != 2 {
		t.Fatalf("a profile was handed %d encoded versions. With none, it "+
			"plays the file straight off the camera and the app's quality "+
			"chooser has nothing to choose between.", len(got[0].VideoVariants))
	}
	if got[0].VideoVariants["720p"] != "https://cdn/hls/258/x/720p.mp4" {
		t.Errorf("720p url came back as %q", got[0].VideoVariants["720p"])
	}
	if got[0].HLSManifestURL != "https://cdn/hls/258/x/master.m3u8" {
		t.Errorf("manifest came back as %q", got[0].HLSManifestURL)
	}
}

func TestUserChallenges_NeverServesTheClaimMarkerAsAURL(t *testing.T) {
	// 'PENDING' is what the worker writes into hls_manifest_url while it
	// holds a job. A client that receives it tries to play the word.
	//
	// The conversion happens in SQL, so a mocked row cannot exercise it —
	// this checks the guard is still in the query text. The reels feed has
	// the same guard and the same reason; see populateHLSManifestURLs.
	if !strings.Contains(challengeBaseQuery, "PENDING") {
		t.Error("challengeBaseQuery no longer guards against 'PENDING'; a " +
			"challenge served mid-transcode will ship the claim marker as " +
			"its manifest url")
	}
}

func TestUserChallenges_BadVariantsJSONDoesNotLoseTheRow(t *testing.T) {
	mock, cleanup := withMockDB(t)
	defer cleanup()

	mock.ExpectQuery(regexp.QuoteMeta("WHERE c.creator_id = CAST($1 AS INT)")).
		WillReturnRows(challengeRowsForProfile(`{not json`, ""))

	got := GetChallengesByCreator("7", true, 50, 0)
	if len(got) != 1 {
		t.Fatalf("an unreadable variants column dropped the whole challenge; "+
			"the client can still fall back to videoUrl. got %d rows", len(got))
	}
	if got[0].VideoVariants != nil {
		t.Errorf("expected no variants from unreadable JSON, got %v",
			got[0].VideoVariants)
	}
}

// ════════════════════════════════════════════════════════════════════════════
// SOMEBODY ELSE'S PROFILE IS NOT A WAY ROUND VISIBILITY
// ════════════════════════════════════════════════════════════════════════════

func TestUserChallenges_AVisitorOnlySeesArenaPosts(t *testing.T) {
	mock, cleanup := withMockDB(t)
	defer cleanup()

	mock.ExpectQuery(regexp.QuoteMeta("AND c.visibility = 'arena'")).
		WillReturnRows(challengeRowsForProfile(`{}`, ""))

	if got := GetChallengesByCreator("7", false, 50, 0); len(got) != 1 {
		t.Fatalf("expected the arena-filtered query to run; got %d rows", len(got))
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("a visitor's request did not filter on visibility: %v", err)
	}
}

func TestUserChallenges_TheOwnerSeesEverythingTheyPosted(t *testing.T) {
	mock, cleanup := withMockDB(t)
	defer cleanup()

	// No visibility clause for the owner: a friends-only post is still
	// theirs and belongs on their own profile.
	mock.ExpectQuery(`creator_id = CAST\(\$1 AS INT\)\s+ORDER BY`).
		WillReturnRows(challengeRowsForProfile(`{}`, ""))

	if got := GetChallengesByCreator("7", true, 50, 0); len(got) != 1 {
		t.Fatalf("expected the unfiltered query to run; got %d rows", len(got))
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("the owner's request filtered on visibility: %v", err)
	}
}
