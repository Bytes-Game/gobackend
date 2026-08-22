package main

import "testing"

// ════════════════════════════════════════════════════════════════════════════════
// 1. THE SLOT COUNT ADAPTS TO THE BACKLOG
// ════════════════════════════════════════════════════════════════════════════════

func TestAuditionSlotsForPage(t *testing.T) {
	const page = 20

	cases := []struct {
		name    string
		backlog int
		want    int
		why     string
	}{
		{
			name: "nothing waiting means no slots", backlog: 0, want: 0,
			why: "forcing an injection with an empty queue displaces a good " +
				"video to show one that was not waiting for a chance",
		},
		{name: "a handful waiting", backlog: 1, want: 1},
		{name: "just under the second slot", backlog: 50, want: 1},
		{name: "just over the second slot", backlog: 51, want: 2},
		{name: "three slots", backlog: 120, want: 3},
		{
			name: "huge backlog is capped", backlog: 100000, want: auditionMaxSlots,
			why: "past the ceiling the feed is mostly unproven video, people " +
				"leave, and the audience the auditions competed for is gone",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := auditionSlotsForPage(c.backlog, page)
			if got != c.want {
				msg := ""
				if c.why != "" {
					msg = " — " + c.why
				}
				t.Errorf("auditionSlotsForPage(backlog=%d, page=%d) = %d, want %d%s",
					c.backlog, page, got, c.want, msg)
			}
		})
	}
}

// The slot count must never be a big share of a small page. Four unproven
// videos on a five-item page is not a feed.
func TestAuditionSlotsForPage_BoundedByPageSize(t *testing.T) {
	const hugeBacklog = 100000

	for _, page := range []int{1, 2, 5, 10, 20, 50} {
		got := auditionSlotsForPage(hugeBacklog, page)
		if got > auditionMaxSlots {
			t.Errorf("page %d: %d slots exceeds the hard ceiling %d", page, got, auditionMaxSlots)
		}
		if page >= 5 && got > page/2 {
			t.Errorf("page %d: %d slots is more than half the page", page, got)
		}
		// ...but a page must never be locked out entirely, or nothing new
		// ever starts on a small-page surface.
		if got < 1 {
			t.Errorf("page %d: got %d slots with a large backlog; every page "+
				"should offer at least one chance", page, got)
		}
	}
}

func TestAuditionSlotsForPage_RejectsNonsense(t *testing.T) {
	if got := auditionSlotsForPage(-5, 20); got != 0 {
		t.Errorf("negative backlog = %d slots, want 0", got)
	}
	if got := auditionSlotsForPage(100, 0); got != 0 {
		t.Errorf("zero page size = %d slots, want 0", got)
	}
}

// ════════════════════════════════════════════════════════════════════════════════
// 2. THE AUDIENCE IS CHOSEN
// ════════════════════════════════════════════════════════════════════════════════

// The change that alters the arithmetic. A view only teaches us something when
// the viewer might plausibly have wanted the video.
func TestAuditionFit_PrefersAViewerWhoseTastesMatch(t *testing.T) {
	matches := map[string]float64{"relevance": 0.9, "embedSim": 0.8, "freshness": 0.1}
	doesNot := map[string]float64{"relevance": 0.05, "embedSim": 0.0, "freshness": 0.1}

	if auditionFit(matches) <= auditionFit(doesNot) {
		t.Error("a well-matched viewer must rank above a poorly-matched one — " +
			"this is what makes each audition impression worth more")
	}
}

// Freshness used to BE the decision, which is how a queue of older waiting
// videos never got picked while new uploads kept arriving. It still counts,
// but it can no longer beat a real interest match.
func TestAuditionFit_FreshnessCannotOutweighInterest(t *testing.T) {
	brandNewButIrrelevant := map[string]float64{"freshness": 1.0}
	olderButWellMatched := map[string]float64{"relevance": 0.8, "embedSim": 0.7, "freshness": 0.0}

	if auditionFit(brandNewButIrrelevant) >= auditionFit(olderButWellMatched) {
		t.Error("pure freshness beat a strong interest match — that is the old " +
			"behaviour, and it is why waiting videos never got their turn")
	}
}

// Between two equally-matched videos the newer one is the better bet: the older
// one has already had other chances.
func TestAuditionFit_FreshnessBreaksATie(t *testing.T) {
	older := map[string]float64{"relevance": 0.5, "freshness": 0.0}
	newer := map[string]float64{"relevance": 0.5, "freshness": 1.0}

	if auditionFit(newer) <= auditionFit(older) {
		t.Error("with interest equal, the fresher video should win")
	}
}

func TestAuditionFit_MissingSignalsDoNotVote(t *testing.T) {
	if got := auditionFit(nil); got != 0 {
		t.Errorf("auditionFit(nil) = %v, want 0", got)
	}
	if got := auditionFit(map[string]float64{}); got != 0 {
		t.Errorf("auditionFit(empty) = %v, want 0", got)
	}
}

// ════════════════════════════════════════════════════════════════════════════════
// 3. A FLOOR, NOT A CEILING
// ════════════════════════════════════════════════════════════════════════════════

// auditionItem builds a scored candidate; eligible marks it as unproven.
func auditionItem(id string, eligible bool, fit float64) ScoredItem {
	bd := map[string]float64{"relevance": fit}
	if eligible {
		bd["auditionEligible"] = 1
	}
	return ScoredItem{
		Item:           HomeFeedItem{Type: "challenge", Challenge: &Challenge{ID: id}},
		ScoreBreakdown: bd,
	}
}

func countEligible(items []ScoredItem) int {
	n := 0
	for _, it := range items {
		if it.ScoreBreakdown != nil && it.ScoreBreakdown["auditionEligible"] > 0 {
			n++
		}
	}
	return n
}

// The case the old code could not tell apart from the bad one: a page that
// already earned its share needs no help, and forcing another injection on top
// spends a slot for nothing.
func TestInjectAudition_PageThatEarnedItsShareIsLeftAlone(t *testing.T) {
	composed := []ScoredItem{
		auditionItem("a", true, 0.5),
		auditionItem("b", false, 0.5),
		auditionItem("c", true, 0.5),
		auditionItem("d", false, 0.5),
	}
	scored := append([]ScoredItem{auditionItem("waiting", true, 0.9)}, composed...)

	out := injectAuditionContent(scored, composed, 2)

	if len(out) != len(composed) {
		t.Errorf("page grew from %d to %d — two unproven videos already won "+
			"places on merit, which is the whole quota", len(composed), len(out))
	}
}

// A page that surfaced none must be topped up to the floor.
func TestInjectAudition_TopsUpAPageWithNone(t *testing.T) {
	composed := []ScoredItem{
		auditionItem("a", false, 0.5),
		auditionItem("b", false, 0.5),
		auditionItem("c", false, 0.5),
		auditionItem("d", false, 0.5),
		auditionItem("e", false, 0.5),
	}
	scored := []ScoredItem{
		auditionItem("w1", true, 0.9),
		auditionItem("w2", true, 0.8),
		auditionItem("w3", true, 0.7),
	}

	out := injectAuditionContent(scored, composed, 3)

	if got := countEligible(out); got != 3 {
		t.Errorf("page carries %d unproven videos, want the full floor of 3", got)
	}
	if len(out) != len(composed)+3 {
		t.Errorf("page length %d, want %d", len(out), len(composed)+3)
	}
}

// Partly earned: only the shortfall is made up.
func TestInjectAudition_MakesUpOnlyTheShortfall(t *testing.T) {
	composed := []ScoredItem{
		auditionItem("a", true, 0.5),
		auditionItem("b", false, 0.5),
		auditionItem("c", false, 0.5),
	}
	scored := []ScoredItem{
		auditionItem("w1", true, 0.9),
		auditionItem("w2", true, 0.8),
	}

	out := injectAuditionContent(scored, composed, 3)

	if got := countEligible(out); got != 3 {
		t.Errorf("page carries %d unproven videos, want 3", got)
	}
	if len(out) != len(composed)+2 {
		t.Errorf("injected %d, want 2 — one was already there",
			len(out)-len(composed))
	}
}

// The best-matched waiting video goes first, not an arbitrary one.
func TestInjectAudition_PicksTheBestMatchFirst(t *testing.T) {
	composed := []ScoredItem{auditionItem("a", false, 0), auditionItem("b", false, 0)}
	scored := []ScoredItem{
		auditionItem("poor", true, 0.1),
		auditionItem("best", true, 0.9),
		auditionItem("ok", true, 0.5),
	}

	out := injectAuditionContent(scored, composed, 1)

	var injected string
	for _, it := range out {
		if it.SlotType == "audition" {
			injected = it.Item.Challenge.ID
		}
	}
	if injected != "best" {
		t.Errorf("injected %q, want the best-matched candidate %q", injected, "best")
	}
}

// Something already on the page must not be injected a second time.
func TestInjectAudition_NeverDuplicatesWhatIsAlreadyThere(t *testing.T) {
	onPage := auditionItem("dup", true, 0.9)
	composed := []ScoredItem{onPage, auditionItem("x", false, 0)}
	scored := []ScoredItem{onPage, auditionItem("other", true, 0.2)}

	out := injectAuditionContent(scored, composed, 2)

	seen := map[string]int{}
	for _, it := range out {
		if it.Item.Challenge != nil {
			seen[it.Item.Challenge.ID]++
		}
	}
	if seen["dup"] > 1 {
		t.Errorf("id %q appears %d times — an item already on the page was "+
			"injected again", "dup", seen["dup"])
	}
}

func TestInjectAudition_NoSlotsOrNoCandidatesIsANoOp(t *testing.T) {
	composed := []ScoredItem{auditionItem("a", false, 0)}

	if got := injectAuditionContent(nil, composed, 0); len(got) != 1 {
		t.Error("zero slots should leave the page untouched")
	}
	if got := injectAuditionContent(nil, composed, 3); len(got) != 1 {
		t.Error("no candidates should leave the page untouched")
	}
	if got := injectAuditionContent(nil, nil, 3); got != nil {
		t.Error("an empty page should stay empty")
	}
}

// Never the first item: the opening hook decides whether the session continues
// at all, and it should be the best thing available rather than something
// nobody has vouched for.
func TestAuditionInsertPositions_NeverTakesTheOpeningSlot(t *testing.T) {
	for _, n := range []int{1, 2, 3, 4} {
		for _, pageLen := range []int{5, 10, 20} {
			for _, p := range auditionInsertPositions(n, pageLen) {
				if p == 0 {
					t.Errorf("n=%d page=%d put an audition at position 0", n, pageLen)
				}
			}
		}
	}
}

// Spread out, not stacked — a run of unproven video in a row is the thing
// spacing exists to prevent.
func TestAuditionInsertPositions_AreSpreadOut(t *testing.T) {
	got := auditionInsertPositions(3, 20)
	if len(got) != 3 {
		t.Fatalf("got %d positions, want 3", len(got))
	}
	for i := 1; i < len(got); i++ {
		if got[i] <= got[i-1] {
			t.Errorf("positions %v are not increasing — injections would stack", got)
		}
	}
}

func TestAuditionInsertPositions_HandlesTinyPages(t *testing.T) {
	for _, pageLen := range []int{0, 1, 2, 3} {
		for _, n := range []int{0, 1, 3} {
			for _, p := range auditionInsertPositions(n, pageLen) {
				if p < 0 || p > pageLen {
					t.Errorf("n=%d page=%d produced out-of-range position %d",
						n, pageLen, p)
				}
			}
		}
	}
}

// ════════════════════════════════════════════════════════════════════════════════
// 4. THE QUEUE MUST NOT EXPIRE
// ════════════════════════════════════════════════════════════════════════════════

// The lane has to be registered, or eligibility that never expires stays
// theoretical: every other lane walks a bounded recency window newest-first, so
// a video that missed its first days is unreachable without this one.
func TestAuditionLaneIsRegistered(t *testing.T) {
	for _, build := range []struct {
		name    string
		sources []candidateSource
	}{
		{"default", buildDefaultSources()},
		{"cohort", buildSourcesForCohort(CohortEngaged)},
	} {
		t.Run(build.name, func(t *testing.T) {
			for _, s := range build.sources {
				if s.name == "audition" {
					if s.fetch == nil {
						t.Error("the audition lane is registered with no fetcher")
					}
					if s.weight <= 0 {
						t.Errorf("the audition lane has weight %v, so it gets no "+
							"budget and the waiting queue stays unreachable", s.weight)
					}
					return
				}
			}
			t.Error("no audition lane — under-viewed content older than the " +
				"other lanes' windows can never be retrieved, so it can never " +
				"be shown, so it can never graduate")
		})
	}
}

// Its job is to make the queue REACHABLE, not to fill the feed with it. How
// much of a page unproven content actually gets is decided downstream by
// auditionSlotsForPage.
func TestAuditionLaneStaysSmall(t *testing.T) {
	if auditionSourceWeight <= 0 {
		t.Fatal("the audition lane needs some budget or it retrieves nothing")
	}
	if auditionSourceWeight > 0.15 {
		t.Errorf("the audition lane takes %v of the candidate budget, which "+
			"crowds out the lanes that find good content", auditionSourceWeight)
	}
}
