package main

// EVERY feed surface must record what it served. Not the ones we know about.
//
// ════════════════════════════════════════════════════════════════════════════
// WHY THIS IS GENERIC AND THE EARLIER CHECKS WERE NOT
// ════════════════════════════════════════════════════════════════════════════
//
// The same step has now been missed twice, on two different feed paths, and
// both times it was invisible until somebody read a device log:
//
//   * SmartFeedHandler's cold-start branch served brand-new accounts and
//     never recorded a thing. Their page 1 is keyed to the page number, so
//     with nothing recorded it was identical on every refresh — for the one
//     cohort whose whole impression of the app is that first session.
//
//   * FollowingFeedV2Handler READ the watch history (sinkSeenItems) and
//     never wrote to it. That is worse than not reading at all, because it
//     works for as long as some other feed happens to record the same
//     videos. A creator you follow whose work never reaches For You could
//     be watched daily and stay permanently "unseen".
//
// There is a third instance in this file's history: finalizeFeedItems exists
// because the same cold-start branch once skipped payload enrichment, and
// every new user saw battles rendered as plain shorts. The answer that time
// was a named function and a comment asking the next person to call it.
//
// That comment did not stop this happening twice more. So this is a test
// instead, and it is deliberately not a list of handlers: a list only ever
// covers the surfaces somebody remembered, and the bug IS forgetting.
//
// The rule keys off the repo's own choke point. finalizeFeedItems and
// finalizeFeedItemsScored already carry the contract "every handler that
// serializes feed items MUST call this". So anything that calls one of them
// is serving a feed page, and a feed page that is served must be recorded.

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestEveryFeedSurfaceRecordsWhatItServed(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatalf("listing sources: %v", err)
	}

	type site struct {
		file string
		fn   string
		line int
	}
	var serves, records []site

	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		raw, err := os.ReadFile(f)
		if err != nil {
			t.Fatalf("read %s: %v", f, err)
		}
		fn := ""
		for n, line := range strings.Split(string(raw), "\n") {
			if strings.HasPrefix(line, "func ") {
				fn = funcNameOf(line)
			}
			if strings.HasPrefix(line, "func finalizeFeedItems") ||
				strings.HasPrefix(line, "func markShownBatch") {
				continue
			}
			code := line
			if i := strings.Index(code, "//"); i >= 0 {
				code = code[:i] // a mention in a comment is not a call
			}
			if strings.Contains(code, "finalizeFeedItems(") ||
				strings.Contains(code, "finalizeFeedItemsScored(") {
				serves = append(serves, site{f, fn, n})
			}
			if strings.Contains(code, "markShownBatch(") {
				records = append(records, site{f, fn, n})
			}
		}
	}

	if len(serves) == 0 {
		t.Fatal("found no feed surfaces at all. This test keys off " +
			"finalizeFeedItems; if the choke point was renamed, this test " +
			"has to follow it — do not just delete it.")
	}

	// PER SERVING SITE, NOT PER FUNCTION.
	//
	// The first version of this check keyed off the function name, and that
	// made it miss the very bug it was written for. SmartFeedHandler serves
	// from TWO branches — cold start and warm — and one markShownBatch
	// anywhere in it satisfied a per-function check. Deleting the cold-start
	// one left this test green, which is precisely the state the cold-start
	// branch shipped in.
	//
	// So each serving site must have a recording site of its own: after it,
	// and before the next serving site begins.
	for i, s := range serves {
		limit := 1 << 30
		for _, next := range serves {
			if next.file == s.file && next.line > s.line && next.line < limit {
				limit = next.line
			}
		}
		recorded := false
		for _, r := range records {
			if r.file == s.file && r.line > s.line && r.line < limit {
				recorded = true
				break
			}
		}
		if recorded {
			continue
		}
		_ = i
		t.Errorf(`%s:%d — %s serves a feed page and never records it.

Whatever it shows will be offered again as though the viewer had never seen
it: nothing ranks it down, and on any path that pages by number rather than
by history, the same page comes back on every refresh.

Call markShownBatch(userID, items) before encoding, AFTER the kind filter —
the seen-set is a claim about what reached the phone, so a Battles page must
not record the shorts it discarded. A surface that deliberately does not
record (the search grid does this, with its reasons written down) still calls
it, behind its own condition.`, s.file, s.line+1, s.fn)
	}

	// The presence half. Everything above asks whether something is MISSING,
	// and a broken scan that found nothing to check would pass all of it.
	// These are the surfaces that exist today; if one drops out of the scan,
	// the scan is broken, not the code.
	for _, want := range []string{
		"feed_engine.go:SmartFeedHandler",
		"feed_engine.go:FollowingFeedV2Handler",
		"explore_feed.go:ExploreFeedHandler",
	} {
		found := 0
		for _, s := range serves {
			if s.file+":"+s.fn == want {
				found++
			}
		}
		if found == 0 {
			t.Errorf("the scan no longer sees %s as a feed surface, so it is "+
				"no longer checking it. Either the handler was renamed, or "+
				"this test stopped working.", want)
		}
	}

	// SmartFeedHandler must show up TWICE — cold start and warm. If it ever
	// reads as one, the per-site check above has quietly become per-function
	// again and the cold-start branch is unguarded.
	smart := 0
	for _, s := range serves {
		if s.file == "feed_engine.go" && s.fn == "SmartFeedHandler" {
			smart++
		}
	}
	if smart < 2 {
		t.Errorf("SmartFeedHandler shows %d serving site(s); it has two, the "+
			"cold-start branch and the warm path. Seeing one means the "+
			"cold-start branch is no longer being checked — which is the "+
			"branch that shipped broken.", smart)
	}
}

// funcNameOf pulls the name out of a `func Name(...)` or `func (r T) Name(...)`
// declaration line.
func funcNameOf(line string) string {
	rest := strings.TrimPrefix(line, "func ")
	if strings.HasPrefix(rest, "(") { // method: skip the receiver
		if i := strings.Index(rest, ")"); i >= 0 {
			rest = strings.TrimSpace(rest[i+1:])
		}
	}
	if i := strings.IndexAny(rest, "([ "); i >= 0 {
		return rest[:i]
	}
	return ""
}
