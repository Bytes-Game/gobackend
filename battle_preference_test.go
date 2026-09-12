package main

import (
	"math"
	"os"
	"strings"
	"testing"
)

// ════════════════════════════════════════════════════════════════════════════
// One rule, and it says something checkable
// ════════════════════════════════════════════════════════════════════════════
//
// There were three battle preferences and they did not agree: the feed added
// 0.30 to 0.50, explore added 0.15 to 0.20, search added 0.05. None of them
// said what they meant. Measured, the search one worked out to roughly a
// HUNDRED times audience advantage — a battle with four views beat a short
// with four hundred — which nothing in the code said and nobody would guess
// from "+0.05".

func TestBattlePreference_IsThreeNotAHundred(t *testing.T) {
	if battlePreference != 3.0 {
		t.Errorf("a battle counts as %v times a short; the product decision "+
			"is three", battlePreference)
	}
	if got := battleEngagementMultiplier(1); got != battlePreference {
		t.Errorf("a challenge with one response is a battle and should count "+
			"%v times, got %v", battlePreference, got)
	}
	if got := battleEngagementMultiplier(0); got != 1 {
		t.Errorf("a challenge with no responses IS a short and must count "+
			"once, got %v", got)
	}
	if got := battleEngagementMultiplier(-3); got != 1 {
		t.Errorf("a nonsense response count must not become a preference, got %v", got)
	}
}

// More responses does not mean more preference. A battle is a battle.
func TestBattlePreference_DoesNotGrowWithResponses(t *testing.T) {
	one := battleEngagementMultiplier(1)
	many := battleEngagementMultiplier(50)
	if one != many {
		t.Errorf("one response gives %v and fifty gives %v. The old feed code "+
			"scaled the bonus with response count, which meant a popular "+
			"battle was preferred twice — once for being a battle and again "+
			"for being popular, which the engagement figure already covers.",
			one, many)
	}
}

// ── The guard against it fragmenting again ──────────────────────────────────

func TestBattlePreference_NobodyRollsTheirOwn(t *testing.T) {
	scorers := []struct{ file, what string }{
		{"feed_engine.go", "the For You feed"},
		{"explore_feed.go", "explore"},
		{"search.go", "search"},
	}
	for _, sc := range scorers {
		src, err := os.ReadFile(sc.file)
		if err != nil {
			t.Fatalf("cannot read %s: %v", sc.file, err)
		}
		s := string(src)
		if !strings.Contains(s, "battleEngagementMultiplier(") {
			t.Errorf("%s (%s) no longer asks the shared rule how much a "+
				"battle is worth. Three copies of this is how they came to "+
				"disagree by a factor of ten in the first place.",
				sc.file, sc.what)
		}
	}

	// And the specific numbers that used to be here must not come back.
	feed, err := os.ReadFile("feed_engine.go")
	if err != nil {
		t.Fatalf("cannot read feed_engine.go: %v", err)
	}
	for _, gone := range []string{"battleBoost = 0.30", "0.30 + 0.20*math.Log1p"} {
		if strings.Contains(string(feed), gone) {
			t.Errorf("%q is back: the feed is preferring battles by its own "+
				"invented amount again", gone)
		}
	}
	exp, err := os.ReadFile("explore_feed.go")
	if err != nil {
		t.Fatalf("cannot read explore_feed.go: %v", err)
	}
	for _, gone := range []string{"battleBoost = 0.15", "battleBoost = 0.20", "battleBoost = -0.05"} {
		if strings.Contains(string(exp), gone) {
			t.Errorf("%q is back in explore", gone)
		}
	}
}

// ── What three times actually buys, on the scale search uses ────────────────

// The number the user actually cares about: how much more audience a short
// needs before it beats a battle.
func TestBattlePreference_ABattleBeatsAShortDoingUpToThreeTimesBetter(t *testing.T) {
	battle := searchEngagementScore(&contentEventAggregates{
		ViewCount: 1000, AvgCompletion: 0.9,
	}, 0, 0, battleEngagementMultiplier(1))

	// A short with a bit under three times the audience still loses.
	closeShort := searchEngagementScore(&contentEventAggregates{
		ViewCount: 2800, AvgCompletion: 0.9,
	}, 0, 0, battleEngagementMultiplier(0))
	if closeShort >= battle {
		t.Errorf("a short with 2.8x the audience (%.4f) beat the battle "+
			"(%.4f); three times is the line", closeShort, battle)
	}

	// A short with more than three times the audience wins, as it should.
	bigShort := searchEngagementScore(&contentEventAggregates{
		ViewCount: 3500, AvgCompletion: 0.9,
	}, 0, 0, battleEngagementMultiplier(0))
	if bigShort <= battle {
		t.Errorf("a short with 3.5x the audience (%.4f) still lost to the "+
			"battle (%.4f). That is the hundred-times behaviour coming back.",
			bigShort, battle)
	}
}

// Three times the engagement, not three times the score. After a logarithm
// those are wildly different, and getting it wrong was a real mistake made
// while writing this.
func TestBattlePreference_MultipliesEngagementNotTheScore(t *testing.T) {
	plain := searchEngagementScore(&contentEventAggregates{
		ViewCount: 1000, AvgCompletion: 0.9,
	}, 0, 0, 1)
	asBattle := searchEngagementScore(&contentEventAggregates{
		ViewCount: 1000, AvgCompletion: 0.9,
	}, 0, 0, 3)

	if asBattle >= plain*2 {
		t.Errorf("counting a video as a battle took its score from %.4f to "+
			"%.4f. That is multiplying the score, not the engagement — the "+
			"score is already a logarithm, so tripling it is enormous.",
			plain, asBattle)
	}
	// Tripling the engagement should move a log10-based score by log10(3),
	// divided by however hard it is squashed.
	want := math.Log10(3) / searchViewSquash
	if got := asBattle - plain; math.Abs(got-want) > 0.02 {
		t.Errorf("tripling engagement moved the score by %.4f, want about "+
			"%.4f", got, want)
	}
}
