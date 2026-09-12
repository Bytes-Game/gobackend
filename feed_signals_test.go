package main

import (
	"os"
	"strings"
	"testing"
)

// ════════════════════════════════════════════════════════════════════════════
// Two things the feed recorded and then ignored
// ════════════════════════════════════════════════════════════════════════════
//
// SAVES. feed_events recorded them. engagementWeight priced them at 1.5, above
// a comment. The profile builder counted them. But the aggregate query that
// feeds content scoring never selected them, so no video ever ranked better
// for being saved. Saving is the second-strongest thing a viewer does — they
// are saying they want this again later — and it was worth nothing.
//
// NOT INTERESTED. Recorded, loaded, carried all the way into ContentScore as
// NotInterestedCount, and then read by nothing at all. A skip is ambiguous;
// people scroll past things they like. Tapping "not interested" is not
// ambiguous, and it counted for nothing.

func TestFeedSignals_SavesAreCounted(t *testing.T) {
	src, err := os.ReadFile("content_agg_batch.go")
	if err != nil {
		t.Fatalf("cannot read content_agg_batch.go: %v", err)
	}
	if !strings.Contains(string(src), `event_type = 'save'`) {
		t.Error("the aggregate query does not ask for saves, so nothing that " +
			"scores content can see one — however many a video gets")
	}

	var agg contentEventAggregates
	agg.SaveCount = 1 // must exist as a field, not just in the SQL
	if agg.SaveCount != 1 {
		t.Error("contentEventAggregates has no place to put a save")
	}
}

func TestFeedSignals_SavingLiftsAVideo(t *testing.T) {
	base := &contentEventAggregates{ViewCount: 1000, AvgCompletion: 0.9}
	saved := &contentEventAggregates{ViewCount: 1000, AvgCompletion: 0.9, SaveCount: 30}

	plain := searchEngagementScore(base, 0, 0, 1)
	withSaves := searchEngagementScore(saved, 0, 0, 1)
	if withSaves <= plain {
		t.Errorf("thirty saves moved a video from %.4f to %.4f. Saving is the "+
			"second-strongest thing somebody does and it has to count.",
			plain, withSaves)
	}
}

// A save is worth more than a like and less than a share, and search must not
// have its own opinion about the order.
func TestFeedSignals_ASaveSitsBetweenALikeAndAShare(t *testing.T) {
	if engagementWeight("save", 0) <= engagementWeight("like", 0) {
		t.Error("a save is priced no higher than a like")
	}
	if engagementWeight("save", 0) >= engagementWeight("share", 0) {
		t.Error("a save is priced at or above a share")
	}
}

// ── Not interested ──────────────────────────────────────────────────────────

func TestFeedSignals_NotInterestedPullsAVideoDown(t *testing.T) {
	clean := notInterestedPenalty(0, 1000)
	if clean != 1 {
		t.Errorf("a video nobody rejected should keep all of its quality, got %v", clean)
	}
	disliked := notInterestedPenalty(100, 1000) // 10% said no
	if disliked >= 1 {
		t.Errorf("a video one in ten people explicitly rejected kept %v of "+
			"its quality. Tapping not-interested is unambiguous, unlike a "+
			"skip, and it counted for nothing at all before this.", disliked)
	}
	worse := notInterestedPenalty(400, 1000)
	if worse >= disliked {
		t.Error("more rejection should cost more")
	}
}

// It is a rate, so it means the same on a video shown ten times and one shown
// ten million.
func TestFeedSignals_RejectionIsARateNotACount(t *testing.T) {
	small := notInterestedPenalty(10, 100)
	big := notInterestedPenalty(100000, 1000000)
	if small != big {
		t.Errorf("the same one-in-ten rejection rate gave %v on a small "+
			"audience and %v on a large one", small, big)
	}
}

// Damped, never erased. A hard zero would let one brigade of taps delete
// somebody's upload from the platform.
func TestFeedSignals_RejectionNeverErasesAVideo(t *testing.T) {
	for _, c := range []struct{ no, views int }{
		{1000, 1000}, {5000, 1000}, {1, 1},
	} {
		got := notInterestedPenalty(c.no, c.views)
		if got < notInterestedFloor-0.0001 {
			t.Errorf("notInterestedPenalty(%d, %d) = %v, below the %v floor. "+
				"Content should be damped, not deleted by tapping.",
				c.no, c.views, got, notInterestedFloor)
		}
		if got > 1 {
			t.Errorf("rejection made a video score better: %v", got)
		}
	}
}

func TestFeedSignals_RejectionHandlesNonsense(t *testing.T) {
	for _, c := range []struct{ no, views int }{
		{0, 0}, {-5, 100}, {100, 0}, {100, -5},
	} {
		got := notInterestedPenalty(c.no, c.views)
		if got <= 0 || got > 1 {
			t.Errorf("notInterestedPenalty(%d, %d) = %v, outside 0..1",
				c.no, c.views, got)
		}
	}
}

// ── The quality formula reads all of them ───────────────────────────────────

func TestFeedSignals_QualityCountsEverything(t *testing.T) {
	src, err := os.ReadFile("feed_engine.go")
	if err != nil {
		t.Fatalf("cannot read feed_engine.go: %v", err)
	}
	s := string(src)
	for _, want := range []struct{ frag, what string }{
		{"completionScore*", "whether people watched it"},
		{"likeQ*", "likes"},
		{"shareQ*", "shares"},
		{"commentQ*", "comments"},
		{"rewatchQ*", "rewatches"},
		{"saveQ*", "saves"},
		{"cs.SkipRate", "skips"},
		{"notInterestedPenalty(", "people saying they do not want it"},
	} {
		if !strings.Contains(s, want.frag) {
			t.Errorf("the feed's quality score no longer counts %s (%q)",
				want.what, want.frag)
		}
	}
}

// One query, not two. The second copy is exactly how saves went missing.
func TestFeedSignals_OneAggregateQuery(t *testing.T) {
	src, err := os.ReadFile("feed_engine.go")
	if err != nil {
		t.Fatalf("cannot read feed_engine.go: %v", err)
	}
	if strings.Contains(string(src), `COUNT(*) FILTER (WHERE event_type = 'rewatch')`) {
		t.Error("feed_engine.go has its own copy of the engagement aggregate " +
			"query again. Two copies is how saves came to be recorded, " +
			"priced, and counted by neither of them.")
	}
	if !strings.Contains(string(src), "loadEngagementAggregates(") {
		t.Error("feed_engine.go no longer uses the shared aggregate loader")
	}
}

// The columns come back in the order the SELECT asks for them, and Scan fills
// the struct in the order its arguments are written. If those two ever drift,
// nothing errors — saves would quietly be read as rejections, and a video
// people loved would be held back for it.
func TestFeedSignals_TheColumnsAndTheScanAgree(t *testing.T) {
	src, err := os.ReadFile("content_agg_batch.go")
	if err != nil {
		t.Fatalf("cannot read content_agg_batch.go: %v", err)
	}
	s := string(src)

	// The order the query asks for them in.
	wantOrder := []string{"'view'", "'like'", "'comment'", "'skip'",
		"'rewatch'", "'share'", "'save'", "'not_interested'"}
	at := -1
	for _, ev := range wantOrder {
		i := strings.Index(s, "event_type = "+ev)
		if i < 0 {
			t.Fatalf("the aggregate query no longer counts %s", ev)
		}
		if i <= at {
			t.Errorf("%s is out of order in the query; the SELECT and the "+
				"Scan below it are positional and must line up", ev)
		}
		at = i
	}

	// The order Scan fills them in.
	scanOrder := []string{"&a.ViewCount", "&a.LikeCount", "&a.CommentCount",
		"&a.SkipCount", "&a.RewatchCount", "&a.ShareCount", "&a.SaveCount",
		"&a.NotInterestedCount"}
	at = -1
	for i, f := range scanOrder {
		j := strings.Index(s, f)
		if j < 0 {
			t.Fatalf("Scan no longer fills %s", f)
		}
		if j <= at {
			t.Errorf("%s is read out of order (position %d). The query asks "+
				"for %s in that slot, so this would put one count into "+
				"another's field with no error at all.",
				f, i, wantOrder[i])
		}
		at = j
	}
}
