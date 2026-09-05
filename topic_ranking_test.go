package main

import (
	"os"
	"strings"
	"testing"
)

// ════════════════════════════════════════════════════════════════════════════
// THE FEED NOW MATCHES VIDEOS, NOT BOXES
// ════════════════════════════════════════════════════════════════════════════
//
// Relevance used to be one line: take the video's category, look it up in a
// map eighteen wide. These cover the replacement, and every place that had to
// change with it — because a signal replaced in one reader and left alone in
// five others is worse than either version on its own.

func TestRank_RelevanceComesFromWhatTheVideoIsAbout(t *testing.T) {
	// A viewer who watches nature clips. Note there is no "nature" CATEGORY —
	// that is the entire point. Under the old signal this viewer's taste could
	// not be written down at all.
	p := &UserProfile{TopicAffinity: map[string]float64{
		"bee": 0.9, "flower": 0.8, "nature": 0.9, "insect": 0.7,
	}}
	butterfly := contentFingerprint(
		[]string{"butterfly", "flower", "nature", "insect"}, []string{"chill"}, "art")
	fantasy := contentFingerprint(
		[]string{"lone hunter", "dark fantasy"}, []string{"story"}, "story")

	good, gm := topicRelevance(p.TopicAffinity, butterfly)
	bad, bm := topicRelevance(p.TopicAffinity, fantasy)
	good *= topicConfidence(gm)
	bad *= topicConfidence(bm)

	if good <= bad {
		t.Errorf("a butterfly clip scored %.2f for somebody who watches nature "+
			"videos, against %.2f for a dark fantasy scene. The butterfly's "+
			"CATEGORY is art, so the old signal could not tell these apart.",
			good, bad)
	}
	if good <= 0 {
		t.Errorf("scored %.2f — a clear match must be positive", good)
	}
}

func TestRank_ExistingViewersKeepTheirPersonalisation(t *testing.T) {
	// THE REGRESSION THIS FILE EXISTS FOR.
	//
	// Topic taste is learned when a profile rebuilds, and profiles rebuild on
	// their own schedule. At the moment this shipped, every existing viewer had
	// a full CategoryAffinity and an empty TopicAffinity — so reading only the
	// new signal switched personalisation off for all of them until their
	// profile happened to come round again.
	//
	// Caught by the existing scoring test, not by me. This pins it.
	src, err := os.ReadFile("feed_engine.go")
	if err != nil {
		t.Fatalf("read feed_engine.go: %v", err)
	}
	s := string(src)
	if !strings.Contains(s, "if matched == 0 {") ||
		!strings.Contains(s, "profile.CategoryAffinity[catKey]") {
		t.Error("relevance no longer falls back to category affinity when the " +
			"topic signal is silent. Every viewer whose profile has not been " +
			"rebuilt since topics existed loses their personalisation entirely, " +
			"and nothing about the feed looks broken while it happens.")
	}
}

func TestRank_TopicsWinWheneverTheyHaveAnythingToSay(t *testing.T) {
	// The fallback must not become a tie-breaker that quietly restores the old
	// behaviour. One matched word is enough for topics to decide.
	aff := map[string]float64{"thistle": 0.9}
	got, matched := topicRelevance(aff, []string{"thistle", "unknown"})
	if matched == 0 {
		t.Fatal("a known word did not register as a match, so the category " +
			"fallback would take over even though topics had an opinion")
	}
	if got <= 0 {
		t.Errorf("scored %.2f on a known liked subject", got)
	}
}

// ════════════════════════════════════════════════════════════════════════════
// EVERY OTHER READER MOVED TOO
// ════════════════════════════════════════════════════════════════════════════
//
// A signal replaced in the scorer and left alone everywhere else is the worst
// of both: the feed picks videos one way and then measures variety, search and
// surprise a different way. These check each consumer actually moved.

func TestRank_EveryReaderOfTheOldSignalMoved(t *testing.T) {
	for _, c := range []struct {
		file, needs, why string
	}{
		{"feed_engine.go", "topicSaturation(session.TopicsSeen",
			"session fatigue still counts categories, so three nature clips " +
				"filed under lifestyle, comedy and art read as a varied feed"},
		{"feed_engine.go", "topicOverlap(fingerprint, session.LastTopics",
			"the sequence penalty still compares one word, so two videos about " +
				"the same thing under different categories arrive back to back"},
		{"feed_engine.go", "rememberTopics(state.TopicsSeen",
			"nothing writes the topic tally, so the read side sees an empty " +
				"map forever and the new check silently does nothing"},
		{"feed_engine.go", "fresh.LastTopics = append",
			"nothing records what recent items were about, so the sequence " +
				"check has nothing to compare against"},
		{"search.go", "topicRelevance(profile.TopicAffinity",
			"search still personalises on the eighteen categories"},
		{"negative_profile_mining.go", "profile.AvoidedTopics = append",
			"rejection is still only learnable as one of eighteen things, so " +
				"skipping every jellyfish video teaches the profile to avoid " +
				"\"other\" — most of the catalogue"},
		{"surprise_injection.go", "familiarTopics[w]",
			"a wildcard only has to differ by category, so a nature clip filed " +
				"under lifestyle counts as unfamiliar ground for somebody who " +
				"watches nature clips filed under art"},
		{"admin_diagnostics.go", "TopicAffinity: p.TopicAffinity",
			"the dashboard shows a signal that no longer decides anything"},
		{"scoring_guards.go", "safeWeights(safe.TopicAffinity",
			"the field relevance is scored on is not bounded, while the one " +
				"it replaced still is"},
	} {
		src, err := os.ReadFile(c.file)
		if err != nil {
			t.Fatalf("read %s: %v", c.file, err)
		}
		if !strings.Contains(string(src), c.needs) {
			t.Errorf("%s: %s\n(missing %q)", c.file, c.why, c.needs)
		}
	}
}

func TestRank_SessionResetClearsBothTallies(t *testing.T) {
	// A reset that clears the category tally and leaves the topic one behind
	// means a "fresh" session still thinks the viewer is sick of thistles.
	src, err := os.ReadFile("feed_engine.go")
	if err != nil {
		t.Fatalf("read feed_engine.go: %v", err)
	}
	s := string(src)
	if !strings.Contains(s, "state.TopicsSeen = make(map[string]int)") {
		t.Error("resetting a session does not clear TopicsSeen, so saturation " +
			"survives the reset that exists to remove it")
	}
	if !strings.Contains(s, "state.LastTopics = nil") {
		t.Error("resetting a session does not clear LastTopics")
	}
}

// ════════════════════════════════════════════════════════════════════════════
// THE PROFILE
// ════════════════════════════════════════════════════════════════════════════

func TestProfile_OneSetOfEngagementWeights(t *testing.T) {
	// The category tally and the topic tally are built from the same events. If
	// each had its own copy of the weights they would drift the first time
	// anybody tuned one, and the two halves of a profile would disagree about
	// what a share is worth.
	src, err := os.ReadFile("feed_engine.go")
	if err != nil {
		t.Fatalf("read feed_engine.go: %v", err)
	}
	s := string(src)
	if !strings.Contains(s, "weight := engagementWeight(evType, completion)") {
		t.Error("the profile builder no longer takes both tallies from one " +
			"weight, so they can disagree about what an event is worth")
	}
	if strings.Contains(s, `categoryScores[category] += 3.0`) {
		t.Error("the old inline weights are back alongside engagementWeight; " +
			"now there are two copies to keep in step")
	}
}

func TestProfile_DislikesSurviveNormalisation(t *testing.T) {
	// Category affinity clamps negatives to zero and then needs a separate pass
	// to remember dislikes and paste them back — which a single weak positive
	// event can defeat. Topic affinity keeps the sign from the start.
	got := normalizeTopicAffinity(map[string]float64{"prank": -4.0, "food": 2.0})
	if got["prank"] >= 0 {
		t.Errorf("a rejected subject normalised to %.2f; a dislike that reads "+
			"as neutral shows people more of what they pushed away", got["prank"])
	}
	if got["food"] <= 0 {
		t.Errorf("a liked subject normalised to %.2f", got["food"])
	}
	if got["prank"] < -1 || got["food"] > 1 {
		t.Errorf("outside [-1,1]: %v", got)
	}
}

func TestProfile_FaintFeelingsAreForgotten(t *testing.T) {
	// The vocabulary is open, and the profile is decoded on every feed request.
	// A word somebody felt 0.001 about is not what makes their feed good.
	got := normalizeTopicAffinity(map[string]float64{"strong": 100, "noise": 0.01})
	if _, ok := got["noise"]; ok {
		t.Error("a negligible feeling was kept; open vocabulary plus no floor " +
			"means the profile grows forever")
	}
	if _, ok := got["strong"]; !ok {
		t.Error("the real signal was dropped")
	}
}

func TestProfile_AvoidedTopicsNeedRealRejection(t *testing.T) {
	// One bad video about a subject must not silence the subject. The threshold
	// has to sit well below a single event's worth of evidence.
	if avoidedTopicThreshold >= 0 {
		t.Fatalf("avoidedTopicThreshold is %v; it has to be a rejection",
			avoidedTopicThreshold)
	}
	mild := normalizeTopicAffinity(map[string]float64{"a": 10, "b": -1})
	if got := avoidedTopics(mild); len(got) != 0 {
		t.Errorf("got %v avoided on mild evidence; one poor video about a "+
			"subject is not a reason to stop showing the subject", got)
	}
	strong := normalizeTopicAffinity(map[string]float64{"a": 1, "b": -10})
	if got := avoidedTopics(strong); len(got) != 1 || got[0] != "b" {
		t.Errorf("got %v, want b avoided after sustained rejection", got)
	}
}

func TestProfile_SavedAndLoadedOrItIsNotAProfile(t *testing.T) {
	// A field the ranker reads but nothing persists is learned on every rebuild
	// and thrown away, which looks exactly like the feature not working.
	src, err := os.ReadFile("feed_engine.go")
	if err != nil {
		t.Fatalf("read feed_engine.go: %v", err)
	}
	s := string(src)
	for _, need := range []string{
		"topic_affinity=$29",                    // written on conflict
		"COALESCE(topic_affinity, '{}'::jsonb)", // read back
		"json.Unmarshal(topicAffJSON, &p.TopicAffinity)",
	} {
		if !strings.Contains(s, need) {
			t.Errorf("topic taste is not persisted (missing %q) — it would be "+
				"relearned and discarded on every rebuild", need)
		}
	}
}

func TestProfile_QueryOnlyAsksForColumnsThatExist(t *testing.T) {
	// The #56 lesson. Posts have neither content_topics nor auto_tags, and a
	// missing column does not come back blank — Postgres rejects the whole
	// statement, so EVERY profile would rebuild as empty and the feed would
	// quietly go generic for everyone.
	src, err := os.ReadFile("feed_engine.go")
	if err != nil {
		t.Fatalf("read feed_engine.go: %v", err)
	}
	s := string(src)
	for _, bad := range []string{"p.content_topics", "p.auto_tags"} {
		if strings.Contains(s, bad) {
			t.Errorf("the profile query asks posts for %q, which that table "+
				"does not have. One missing column empties every profile.", bad)
		}
	}
	schema, err := os.ReadFile("migrations/005_video_analysis.sql")
	if err != nil {
		t.Fatalf("read migration 005: %v", err)
	}
	if strings.Contains(string(schema), "ALTER TABLE posts") {
		t.Skip("posts gained the columns; this guard is out of date")
	}
}
