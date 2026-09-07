package main

import (
	"os"
	"strings"
	"testing"
)

// ════════════════════════════════════════════════════════════════════════════
// Search used to see only views and likes
// ════════════════════════════════════════════════════════════════════════════
//
// Shares, comments, rewatches and whether anybody finished the video were all
// invisible to it. On a short-video app, finishing is close to the strongest
// thing a viewer can say short of sharing.

func TestSearchEngagement_SharesAndCommentsActuallyCount(t *testing.T) {
	plain := searchEngagementScore(&contentEventAggregates{
		ViewCount: 1000, AvgCompletion: 0.95,
	}, 0, 0)
	shared := searchEngagementScore(&contentEventAggregates{
		ViewCount: 1000, AvgCompletion: 0.95, ShareCount: 50, CommentCount: 40,
	}, 0, 0)
	if shared <= plain {
		t.Errorf("a video with 50 shares and 40 comments scored %.3f, no better "+
			"than the same video with none (%.3f). Search could only see views "+
			"and likes; that was the bug.", shared, plain)
	}
}

// A share is worth more than a like, and search must not have its own opinion
// about by how much.
func TestSearchEngagement_UsesTheAppsOwnPrices(t *testing.T) {
	oneShare := searchEngagementScore(&contentEventAggregates{ShareCount: 10}, 0, 0)
	oneLike := searchEngagementScore(&contentEventAggregates{LikeCount: 10}, 0, 0)
	if oneShare <= oneLike {
		t.Errorf("ten shares (%.3f) did not beat ten likes (%.3f)", oneShare, oneLike)
	}
	// And the ordering must come from engagementWeight, not a local copy.
	if engagementWeight("share", 0) <= engagementWeight("like", 0) {
		t.Fatal("engagementWeight no longer prices a share above a like; if " +
			"search had its own copy of these numbers this test would still " +
			"pass while the app disagreed with itself")
	}
	src, err := os.ReadFile("search_engagement.go")
	if err != nil {
		t.Fatalf("cannot read search_engagement.go: %v", err)
	}
	if !strings.Contains(string(src), `engagementWeight("share"`) {
		t.Error("search no longer asks engagementWeight what a share is worth, " +
			"so the app now has two opinions that will drift apart")
	}
}

// The signal that matters most on a short-video app.
func TestSearchEngagement_FinishingBeatsBouncing(t *testing.T) {
	finished := searchEngagementScore(&contentEventAggregates{
		ViewCount: 1000, AvgCompletion: 0.95,
	}, 0, 0)
	bounced := searchEngagementScore(&contentEventAggregates{
		ViewCount: 1000, AvgCompletion: 0.35,
	}, 0, 0)
	if finished <= bounced {
		t.Errorf("a thousand views people finished scored %.3f and a thousand "+
			"they bounced off scored %.3f. Watching to the end is the whole "+
			"point of the format and has to count.", finished, bounced)
	}
}

func TestSearchEngagement_BeingSkippedCountsAgainst(t *testing.T) {
	liked := searchEngagementScore(&contentEventAggregates{
		ViewCount: 500, AvgCompletion: 0.9, LikeCount: 20,
	}, 0, 0)
	skipped := searchEngagementScore(&contentEventAggregates{
		ViewCount: 500, AvgCompletion: 0.9, LikeCount: 20, SkipCount: 400,
		NotInterestedCount: 50,
	}, 0, 0)
	if skipped >= liked {
		t.Errorf("a video most people skipped scored %.3f, no worse than one "+
			"they did not (%.3f). Being served often is not being popular.",
			skipped, liked)
	}
}

// Most of the catalogue predates the analytics pipeline. Its totals live on
// its own row, and without this every one of those videos would look like
// nobody had ever watched it.
func TestSearchEngagement_OlderContentIsNotTreatedAsUnwatched(t *testing.T) {
	fromRow := searchEngagementScore(nil, 3423, 0)
	if fromRow <= 0 {
		t.Fatal("a video with 3423 views on its row scored zero because it has " +
			"no recorded events. Everything uploaded before events existed " +
			"would rank as if it had never been seen.")
	}
	quiet := searchEngagementScore(nil, 4, 0)
	if fromRow <= quiet {
		t.Errorf("3423 views (%.3f) did not beat 4 views (%.3f)", fromRow, quiet)
	}
}

// The richer source wins when it knows more, and never makes a video look
// worse than its own row already says.
func TestSearchEngagement_TheRowIsAFloorNotAnAlternative(t *testing.T) {
	// Events know about almost nothing; the row knows about thousands.
	got := searchEngagementScore(&contentEventAggregates{ViewCount: 1}, 3000, 0)
	rowOnly := searchEngagementScore(nil, 3000, 0)
	if got < rowOnly {
		t.Errorf("having a few recorded events (%.3f) made a video look worse "+
			"than having none at all (%.3f)", got, rowOnly)
	}
}

func TestSearchEngagement_NeverNegativeOrNonsense(t *testing.T) {
	cases := []*contentEventAggregates{
		{SkipCount: 10000, NotInterestedCount: 5000},
		{},
		nil,
		{ViewCount: -5, LikeCount: -5},
	}
	for i, a := range cases {
		got := searchEngagementScore(a, 0, 0)
		if got < 0 || got != got { // NaN check
			t.Errorf("case %d scored %v, outside a usable range", i, got)
		}
	}
}

// ── The silent-zero guard ───────────────────────────────────────────────────

// feed_events stores content_type as "challenge", singular. Asking for
// "challenges" returns no rows — no error, no warning, just every video
// looking unwatched. This is the same shape of mistake that once made the
// whole analysis endpoint return null.
func TestSearchEngagement_AsksForTheContentTypeThatExists(t *testing.T) {
	src, err := os.ReadFile("search_engagement.go")
	if err != nil {
		t.Fatalf("cannot read search_engagement.go: %v", err)
	}
	s := string(src)
	if strings.Contains(s, `"challenges"`) {
		t.Error(`search asks feed_events for content_type "challenges". The ` +
			`stored value is "challenge", singular, so this matches no rows ` +
			`— and returns quietly, making every video look unwatched.`)
	}
	if !strings.Contains(s, `"challenge"`) {
		t.Error("search no longer names the content type it is loading")
	}
}

// One query for the result set, not one per video.
func TestSearchEngagement_LoadsInOneBatch(t *testing.T) {
	src, err := os.ReadFile("search_engagement.go")
	if err != nil {
		t.Fatalf("cannot read search_engagement.go: %v", err)
	}
	if !strings.Contains(string(src), "loadEngagementAggregates(") {
		t.Error("search no longer batch-loads engagement, so a page of " +
			"results is a page of queries")
	}
}

// Without a database the whole thing must degrade to row counts, not break.
func TestSearchEngagement_NoDatabaseStillRanks(t *testing.T) {
	saved := db
	db = nil
	defer func() { db = saved }()

	got := searchEngagementAggregates([]challengeHit{{Ch: Challenge{ID: "1"}}})
	if len(got) != 0 {
		t.Errorf("with no database there should be no aggregates, got %d", len(got))
	}
	if searchEngagementScore(nil, 100, 5) <= 0 {
		t.Error("with no aggregates a video must still score from its own row")
	}
}

// A widely-watched video that people leave early is a middling result, not the
// bottom of the scale.
//
// engagementWeight prices a half-watched view negatively, which is correct
// when the question is "does this viewer like this sort of thing" and wrong
// when it is "how is this video doing". Without a floor, a million views that
// people bounce off scores identically to a video nobody has ever opened.
func TestSearchEngagement_WidelyWatchedButNotFinishedIsNotZero(t *testing.T) {
	bounced := searchEngagementScore(&contentEventAggregates{
		ViewCount: 1000000, AvgCompletion: 0.35,
	}, 0, 0)
	unseen := searchEngagementScore(&contentEventAggregates{}, 0, 0)
	if bounced <= unseen {
		t.Fatalf("a million views people left early scored %.3f and a video "+
			"nobody has opened scored %.3f. Being widely seen and not much "+
			"loved is a middling result, not the bottom.", bounced, unseen)
	}
	// And it still ranks below the same audience staying to the end.
	finished := searchEngagementScore(&contentEventAggregates{
		ViewCount: 1000000, AvgCompletion: 0.95,
	}, 0, 0)
	if finished <= bounced {
		t.Errorf("finishing (%.3f) must still beat bouncing (%.3f)",
			finished, bounced)
	}
}

// The one place that deliberately does not defer to engagementWeight, and why.
func TestSearchEngagement_AViewIsNeverWorthNothing(t *testing.T) {
	if searchViewValue(0) <= 0 {
		t.Fatal("a view by somebody who left immediately is worth nothing. " +
			"They still chose to open it, which is real exposure.")
	}
	if searchViewValue(0.95) <= searchViewValue(0.35) {
		t.Error("watching to the end must be worth more than bouncing")
	}
	if searchViewValue(5) > 1 {
		t.Error("a nonsense completion figure must not inflate a view past one")
	}
	// engagementWeight still says what it says for taste — this only differs
	// here, on purpose.
	if engagementWeight("view", 0.35) >= 0 {
		t.Skip("engagementWeight no longer prices a half-watch negatively; " +
			"the divergence this guards may no longer be needed")
	}
}

// The reason reactions are a rate rather than another thing added to the pile.
//
// Summed beside the view count they were invisible: a thousand views with
// twenty shares, twenty comments and twenty likes scored about 2% above the
// same thousand views with none. Views outnumber everything else so heavily
// that anything added beside them disappears.
func TestSearchEngagement_StrongReactionIsVisible(t *testing.T) {
	base := contentEventAggregates{ViewCount: 1000, AvgCompletion: 0.95}
	loved := base
	loved.LikeCount, loved.CommentCount, loved.ShareCount, loved.RewatchCount = 20, 20, 20, 20

	plain := searchEngagementScore(&base, 0, 0)
	strong := searchEngagementScore(&loved, 0, 0)
	lift := (strong - plain) / plain
	if lift < 0.10 {
		t.Errorf("an exceptional reaction — a 2%% share rate — lifted the "+
			"score by only %.1f%% (%.3f to %.3f). If shares and comments "+
			"cannot move it they are not really being counted.",
			lift*100, plain, strong)
	}
}

func TestSearchEngagement_ShareMovesItMoreThanALike(t *testing.T) {
	base := contentEventAggregates{ViewCount: 1000, AvgCompletion: 0.95}
	withLikes, withShares := base, base
	withLikes.LikeCount = 20
	withShares.ShareCount = 20
	if searchEngagementScore(&withShares, 0, 0) <= searchEngagementScore(&withLikes, 0, 0) {
		t.Error("twenty shares did not beat twenty likes, though the app " +
			"prices a share three times a like")
	}
}

// A handful of events must not run away with the ranking.
func TestSearchEngagement_ATinyAudienceCannotRunAway(t *testing.T) {
	// Ten viewers, five of whom shared it. A 50% share rate — and also five
	// people.
	tiny := searchEngagementScore(&contentEventAggregates{
		ViewCount: 10, AvgCompletion: 1, ShareCount: 5,
	}, 0, 0)
	big := searchEngagementScore(&contentEventAggregates{
		ViewCount: 100000, AvgCompletion: 0.9,
	}, 0, 0)
	if tiny >= big {
		t.Errorf("ten viewers with a wild share rate scored %.3f and a "+
			"hundred thousand scored %.3f. Reaction lifts a video; it does "+
			"not replace having an audience.", tiny, big)
	}
}

func TestSearchEngagement_PushbackPullsItDown(t *testing.T) {
	liked := searchEngagementScore(&contentEventAggregates{
		ViewCount: 1000, AvgCompletion: 0.95, LikeCount: 20,
	}, 0, 0)
	hated := searchEngagementScore(&contentEventAggregates{
		ViewCount: 1000, AvgCompletion: 0.4, SkipCount: 900, NotInterestedCount: 100,
	}, 0, 0)
	if hated >= liked*0.75 {
		t.Errorf("a video nine in ten people skipped scored %.3f against "+
			"%.3f for one they liked. Being served often is not being "+
			"popular.", hated, liked)
	}
}

func TestSearchEngagement_TheCapIsAProportion(t *testing.T) {
	if searchReactionCap <= 0 || searchReactionCap >= 1 {
		t.Fatalf("the reaction cap is %v; outside 0..1 it either switches "+
			"reactions off or lets them double a score", searchReactionCap)
	}
}
