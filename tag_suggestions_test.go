package main

import (
	"os"
	"reflect"
	"regexp"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

// Offering the creator what the model saw.
//
// The rule lives in one pure function on purpose: it decides both what is
// SHOWN and what may be ACCEPTED, so a client cannot talk the server into
// storing a tag that was never on offer. Two copies of that rule would be a
// tag-editing endpoint nobody meant to build.

func TestSuggestable_OffersWhatTheModelSawAndTheCreatorHasNot(t *testing.T) {
	got := suggestableTags(
		[]string{"surfing", "wave", "ocean"}, // topics
		[]string{"sports", "intense"},        // auto tags
		[]string{"beach"},                    // the creator's own
		nil,                                  // nothing turned down yet
	)
	want := []string{"surfing", "wave", "ocean", "sports", "intense"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestSuggestable_TopicsComeFirst(t *testing.T) {
	// Topics are the model in its own words — what a creator would not have
	// thought to type. auto_tags come from the fixed list the ranker uses, so
	// a creator has most likely covered those already.
	got := suggestableTags([]string{"street food"}, []string{"cooking"}, nil, nil)
	if len(got) < 2 || got[0] != "street food" {
		t.Errorf("got %v, want the topic first", got)
	}
}

func TestSuggestable_DoesNotRepeatWhatTheCreatorHas(t *testing.T) {
	got := suggestableTags([]string{"surfing", "ocean"}, nil, []string{"surfing"}, nil)
	for _, g := range got {
		if g == "surfing" {
			t.Error("offered a tag the video already carries")
		}
	}
}

func TestSuggestable_DoesNotAskTwiceAboutADismissedTag(t *testing.T) {
	// The whole point of remembering "no". Without it every visit to their
	// own video offers the same rejected tags again.
	got := suggestableTags([]string{"surfing", "ocean"}, nil, nil, []string{"ocean"})
	for _, g := range got {
		if g == "ocean" {
			t.Error("offered a tag the creator already turned down")
		}
	}
}

func TestSuggestable_NeverOffersMoreThanCouldBeKept(t *testing.T) {
	// A video may carry maxTagsPerItem. Offering more than the room left is
	// a list whose last entries silently do nothing when tapped.
	mine := make([]string, maxTagsPerItem-2)
	for i := range mine {
		mine[i] = "own" + string(rune('a'+i))
	}
	got := suggestableTags(
		[]string{"a", "b", "c", "d", "e", "f"}, nil, mine, nil)
	if len(got) > 2 {
		t.Errorf("offered %d with room for 2", len(got))
	}
}

func TestSuggestable_AFullVideoIsOfferedNothing(t *testing.T) {
	mine := make([]string, maxTagsPerItem)
	for i := range mine {
		mine[i] = "own" + string(rune('a'+i))
	}
	if got := suggestableTags([]string{"a", "b"}, nil, mine, nil); len(got) != 0 {
		t.Errorf("offered %v to a video that is already full", got)
	}
}

func TestSuggestable_DoesNotRepeatItself(t *testing.T) {
	// topics and auto_tags overlap in practice — both can say "sports".
	got := suggestableTags([]string{"sports"}, []string{"sports"}, nil, nil)
	if len(got) != 1 {
		t.Errorf("got %v, want one of each", got)
	}
}

// ════════════════════════════════════════════════════════════════════════════
// THE PARTS THAT KEEP IT HONEST
// ════════════════════════════════════════════════════════════════════════════

func TestTagSuggestions_AreForTheCreatorAlone(t *testing.T) {
	src, err := os.ReadFile("tag_suggestions.go")
	if err != nil {
		t.Fatal(err)
	}
	body := string(src)
	// Both handlers, not one. A read-only leak is still a leak: the machine's
	// reading of somebody's video is not for strangers.
	if n := strings.Count(body, `if !own {`); n != 2 {
		t.Errorf("the creator check appears %d times, want 2 — one handler "+
			"hands out (or edits) another person's video", n)
	}
	if n := strings.Count(body, "http.StatusForbidden"); n != 2 {
		t.Errorf("got %d forbidden replies, want 2", n)
	}
	if !strings.Contains(body, "owner != \"\" && owner == viewerID") {
		t.Error("ownership no longer compares the uploader to the caller — an " +
			"empty id would match an unauthenticated caller")
	}
}

// ════════════════════════════════════════════════════════════════════════════
// BOTH HALVES OF A BATTLE, NOT JUST THE CHALLENGE
// ════════════════════════════════════════════════════════════════════════════
//
// The same feature over two tables. What makes that worth testing rather than
// reading is that the difference between them is three words in a struct, and
// getting one wrong does not fail — it reads the wrong table, or asks the
// wrong column who owns the video, and answers confidently either way.

func TestTagSurfaces_NameTheRightTableAndOwner(t *testing.T) {
	if challengeTagSurface.table != "challenges" ||
		challengeTagSurface.owner != "creator_id" {
		t.Errorf("challenge surface reads %s.%s",
			challengeTagSurface.table, challengeTagSurface.owner)
	}
	if responseTagSurface.table != "challenge_responses" ||
		responseTagSurface.owner != "responder_id" {
		t.Errorf("response surface reads %s.%s",
			responseTagSurface.table, responseTagSurface.owner)
	}
	// A response is not in the search index, so saving its tags must not
	// queue a reindex of a challenge that happens to share its id. That is
	// not a wasted call, it is the WRONG video being reindexed.
	if responseTagSurface.searchHasACopy {
		t.Error("saving a response's tags would reindex the challenge with " +
			"the same id — responses are not in the search index at all")
	}
	if !challengeTagSurface.searchHasACopy {
		t.Error("a challenge's tags decide who finds it, and search holds " +
			"its own copy that nothing would now update")
	}
	if responseTagSurface.notFound == challengeTagSurface.notFound {
		t.Error("both surfaces report the same not-found error, so a caller " +
			"cannot tell which thing was missing")
	}
}

// readTagState must actually query the table its surface names.
//
// A mock database cannot check a column name, but it can check which table
// was asked — which is the half of this that a copy-paste gets wrong.
func TestReadTagState_QueriesTheSurfacesOwnTable(t *testing.T) {
	for _, tc := range []struct {
		name    string
		surface tagSurface
		expect  string
	}{
		{"challenge", challengeTagSurface, `FROM challenges WHERE`},
		{"response", responseTagSurface, `FROM challenge_responses WHERE`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock, cleanup := withMockDB(t)
			defer cleanup()
			mock.ExpectQuery(regexp.QuoteMeta(tc.expect)).
				WillReturnRows(sqlmock.NewRows([]string{
					"owner", "content_topics", "auto_tags", "custom_tags", "dismissed_tags",
				}).AddRow("7", `["surfing"]`, `["sports"]`, `[]`, `[]`))

			own, topics, auto, _, _, err := readTagState(tc.surface, 1, "7")
			if err != nil {
				t.Fatalf("readTagState: %v", err)
			}
			if !own {
				t.Error("the uploader was not recognised as the owner")
			}
			if len(topics) != 1 || topics[0] != "surfing" {
				t.Errorf("topics came back as %v", topics)
			}
			if len(auto) != 1 || auto[0] != "sports" {
				t.Errorf("machine tags came back as %v", auto)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Error(err)
			}
		})
	}
}

// An unauthenticated caller must not own a row whose uploader id is blank.
func TestReadTagState_BlankOwnerIsNobody(t *testing.T) {
	mock, cleanup := withMockDB(t)
	defer cleanup()
	mock.ExpectQuery(regexp.QuoteMeta("FROM challenge_responses")).
		WillReturnRows(sqlmock.NewRows([]string{
			"owner", "content_topics", "auto_tags", "custom_tags", "dismissed_tags",
		}).AddRow("", `[]`, `[]`, `[]`, `[]`))

	own, _, _, _, _, err := readTagState(responseTagSurface, 1, "")
	if err != nil {
		t.Fatalf("readTagState: %v", err)
	}
	if own {
		t.Error("a caller with no identity was handed a video with no owner")
	}
}

// saveTagDecision must write to the table its surface names.
func TestSaveTagDecision_WritesTheSurfacesOwnTable(t *testing.T) {
	for _, tc := range []struct {
		name    string
		surface tagSurface
		expect  string
	}{
		{"challenge", challengeTagSurface, `UPDATE challenges SET custom_tags`},
		{"response", responseTagSurface, `UPDATE challenge_responses SET custom_tags`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock, cleanup := withMockDB(t)
			defer cleanup()
			mock.ExpectExec(regexp.QuoteMeta(tc.expect)).
				WillReturnResult(sqlmock.NewResult(1, 1))
			if err := saveTagDecision(tc.surface, 1, []string{"surfing"}, nil); err != nil {
				t.Fatalf("saveTagDecision: %v", err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Error(err)
			}
		})
	}
}

// ════════════════════════════════════════════════════════════════════════════
// THE SECURITY BOUNDARY
// ════════════════════════════════════════════════════════════════════════════
//
// Accepting is the only write in this feature, and it must be able to store
// nothing except what the server itself offered. Otherwise it is a general
// tag-editing endpoint that skips every rule the create path applies.
//
// Tested by CALLING it. An earlier version of this test searched the source
// for the guard instead, and passed with the guard deleted — the same string
// appears in the dismiss path two lines below.

func TestAcceptTags_StoresOnlyWhatWasOffered(t *testing.T) {
	got := acceptTags(
		[]string{"beach"},                                   // already on the video
		[]string{"surfing", "ocean"},                        // what the server offered
		[]string{"surfing", "crypto-scam", "buy followers"}, // what a client sent
	)
	want := []string{"beach", "surfing"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v — a tag that was never offered got stored", got, want)
	}
}

func TestAcceptTags_KeepsWhatWasAlreadyThere(t *testing.T) {
	got := acceptTags([]string{"beach", "summer"}, []string{"surfing"}, nil)
	if !reflect.DeepEqual(got, []string{"beach", "summer"}) {
		t.Errorf("got %v — accepting nothing must not remove anything", got)
	}
}

func TestAcceptTags_CannotPushAVideoPastTheCap(t *testing.T) {
	mine := make([]string, maxTagsPerItem-1)
	for i := range mine {
		mine[i] = "own" + string(rune('a'+i))
	}
	offered := []string{"x", "y", "z"}
	got := acceptTags(mine, offered, offered)
	if len(got) > maxTagsPerItem {
		t.Errorf("stored %d tags, cap is %d", len(got), maxTagsPerItem)
	}
}

func TestAcceptTags_NormalisesWhatItStores(t *testing.T) {
	// The client may send anything. "#Surfing " and "surfing" are one tag,
	// and the stored form has to match what everything else compares against.
	got := acceptTags(nil, []string{"surfing"}, []string{"#Surfing ", "surfing"})
	if !reflect.DeepEqual(got, []string{"surfing"}) {
		t.Errorf("got %v, want one normalised tag", got)
	}
}

func TestDismissTags_RemembersOnlyWhatWasOffered(t *testing.T) {
	// Letting a client dismiss anything would silently poison future
	// suggestions for tags the model has not even proposed yet.
	got := dismissTags([]string{"old"}, []string{"ocean"}, []string{"ocean", "everything else"})
	if !reflect.DeepEqual(got, []string{"old", "ocean"}) {
		t.Errorf("got %v, want the previous no plus the offered one", got)
	}
}

func TestDismissTags_DoesNotPileUpDuplicates(t *testing.T) {
	got := dismissTags([]string{"ocean"}, []string{"ocean"}, []string{"ocean"})
	if !reflect.DeepEqual(got, []string{"ocean"}) {
		t.Errorf("got %v", got)
	}
}

// The read, the offer check and the reply must all use the same rule. Two
// copies would drift, and the one that drifts is the one that decides what a
// client is allowed to store.
func TestTagSuggestions_OneRuleDecidesBothSides(t *testing.T) {
	src, err := os.ReadFile("tag_suggestions.go")
	if err != nil {
		t.Fatal(err)
	}
	if n := strings.Count(string(src), "suggestableTags("); n < 4 {
		t.Errorf("suggestableTags appears %d times; it must be the source of "+
			"the read, the write's offer list and the write's reply", n)
	}
}

func TestTagSuggestions_AreRegistered(t *testing.T) {
	src, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	body := string(src)
	for _, want := range []string{
		`api.HandleFunc("/challenges/{id}/tag-suggestions", authed(GetTagSuggestionsHandler)).Methods("GET", "OPTIONS")`,
		`api.HandleFunc("/challenges/{id}/tag-suggestions", authed(DecideTagSuggestionsHandler)).Methods("POST", "OPTIONS")`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("missing route: %s", want)
		}
	}
	// Order matters: mux takes the first match, and a bare {id} route placed
	// first would swallow "tag-suggestions" as a challenge id.
	sugg := strings.Index(body, `"/challenges/{id}/tag-suggestions"`)
	bare := strings.Index(body, `api.HandleFunc("/challenges/{id}", GetChallengeDetailHandler)`)
	if sugg < 0 || bare < 0 || sugg > bare {
		t.Error("the bare /challenges/{id} route comes first, so it matches " +
			"tag-suggestions as an id and this endpoint is unreachable")
	}
}

func TestDecodeTagList_ABadColumnOffersNothingRatherThanFailing(t *testing.T) {
	if got := decodeTagList([]byte(`["a","b"]`)); !reflect.DeepEqual(got, []string{"a", "b"}) {
		t.Errorf("got %v", got)
	}
	for _, bad := range []string{``, `{oops`, `{"a":1}`, `null`} {
		if got := decodeTagList([]byte(bad)); len(got) != 0 {
			t.Errorf("%q -> %v, want nothing", bad, got)
		}
	}
}

func TestDedupeStrings(t *testing.T) {
	got := dedupeStrings([]string{"a", "b", "a", "", "c", "b"})
	if !reflect.DeepEqual(got, []string{"a", "b", "c"}) {
		t.Errorf("got %v", got)
	}
}

// The column has to exist or every read fails.
func TestDismissedTagsColumnExists(t *testing.T) {
	src, err := os.ReadFile("database.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(src), "ALTER TABLE challenges ADD COLUMN dismissed_tags") {
		t.Error("dismissed_tags is never created, so remembering a 'no' " +
			"fails and the same tags are offered forever")
	}
}

// ════════════════════════════════════════════════════════════════════════════
// THE HANDLERS HAVE TO BE REACHABLE
// ════════════════════════════════════════════════════════════════════════════
//
// This repo has shipped the same bug four times: a thing that works, with
// tests that pass, and nothing anywhere calling it. Every one was found by
// cutting the wire and watching every test stay green.
//
// Routes are registered inside main(), which a test cannot call, so this reads
// the source. Comment lines are stripped first — a test that matches the
// COMMENT explaining a route instead of the route itself has happened in this
// repo too, and it passed against code with the route deleted.
func TestResponseTagSuggestions_AreRoutedAndAuthed(t *testing.T) {
	src := readSourceFile(t, "main.go")
	var code []string
	for _, line := range strings.Split(src, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "//") {
			continue
		}
		code = append(code, line)
	}
	body := strings.Join(code, "\n")

	for _, want := range []string{
		`api.HandleFunc("/challenges/responses/{id}/tag-suggestions", authed(GetResponseTagSuggestionsHandler)).Methods("GET", "OPTIONS")`,
		`api.HandleFunc("/challenges/responses/{id}/tag-suggestions", authed(DecideResponseTagSuggestionsHandler)).Methods("POST", "OPTIONS")`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("no route registers this, so the handler exists and "+
				"nothing can ever reach it:\n  %s", want)
		}
	}
}
