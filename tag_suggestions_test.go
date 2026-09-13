package main

import (
	"os"
	"reflect"
	"strings"
	"testing"
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
	if !strings.Contains(body, "creator != \"\" && creator == viewerID") {
		t.Error("ownership no longer compares the creator to the caller — an " +
			"empty creator id would match an unauthenticated caller")
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
