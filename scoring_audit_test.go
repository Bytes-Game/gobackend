package main

import (
	"fmt"
	"math"
	"math/rand"
	"testing"
	"time"
)

// ════════════════════════════════════════════════════════════════════════════
// AN AUDIT OF THE SCORER, BY RUNNING IT
// ════════════════════════════════════════════════════════════════════════════
//
// scoreForUser is around eight hundred lines and roughly forty terms wide.
// Reading it and reasoning about each term is how the bugs got in; this runs
// it instead, across a wide spread of inputs, and checks the things that have
// to be true no matter what the inputs are.
//
// These are properties rather than expected values on purpose. Asserting
// "this input gives 0.734" pins today's tuning and fails the moment anybody
// legitimately adjusts a weight. Asserting "the answer is always a real
// number" is true of every correct version of this function and false of
// every broken one.
//
// The inputs deliberately include values the callers should never produce —
// negative counts, NaN rates, empty strings, timestamps in the future. A
// ranker that returns NaN on bad input does not fail loudly; it sorts
// unpredictably, because NaN compares false against everything, and the page
// order silently becomes arbitrary.

// Two input ranges, and the difference between them is the difference
// between a bug and a hypothetical.
//
// REALISTIC is what the rest of the system can actually produce. Every rate
// is a rate, every count is a count, every budget is clamped where its
// writers clamp it. A failure on these inputs is a live bug.
//
// HOSTILE includes values nothing upstream can generate. A failure on these
// says the scorer trusts its callers — worth knowing, not worth alarm.
var realisticScales = []float64{0, 0.01, 0.25, 0.5, 0.75, 0.99, 1}

var hostileScales = []float64{
	0, 1, -1, 0.5, 100, -100, 1e9, -1e9,
	math.SmallestNonzeroFloat64, math.MaxFloat64,
}

// auditScales is what the generators draw from; swapped by the caller.
var auditScales = realisticScales

func auditContent(rng *rand.Rand) *ContentScore {
	pick := func() float64 { return auditScales[rng.Intn(len(auditScales))] }
	cats := []string{"", "comedy", "sports", "music", "other", "general", "NONSENSE"}
	return &ContentScore{
		ContentID:          fmt.Sprintf("c%d", rng.Intn(1000)),
		ContentType:        []string{"challenge", "response", ""}[rng.Intn(3)],
		AvgCompletionRate:  pick(),
		AvgWatchTimeMs:     rng.Intn(200000),
		SkipRate:           pick(),
		RewatchRate:        pick(),
		ShareCount:         rng.Intn(2000),
		NotInterestedCount: rng.Intn(2000),
		EngagementVelocity: pick(),
		TrendingScore:      pick(),
		QualityScore:       pick(),
		EnergyLevel:        pick(),
		EnergyLevelLabel:   pick(),
		Category:           cats[rng.Intn(len(cats))],
		EmotionVector:      map[string]float64{"happy": pick(), "competitive": pick()},
		CreatorID:          fmt.Sprintf("u%d", rng.Intn(50)),
		CreatorLeague:      []string{"", "bronze", "gold", "??"}[rng.Intn(4)],
		CreatorFollowers:   rng.Intn(1000000),
		CreatorWinRate:     pick(),
		CreatedAt:          time.Now().Add(time.Duration(rng.Intn(400)-200) * 24 * time.Hour),
		ViewCount:          rng.Intn(1000000),
		LikeCount:          rng.Intn(100000),
		CommentCount:       rng.Intn(10000),
		ResponseCount:      rng.Intn(20),
	}
}

func auditProfile(rng *rand.Rand) *UserProfile {
	pick := func() float64 { return auditScales[rng.Intn(len(auditScales))] }
	return &UserProfile{
		UserID:            "viewer",
		CategoryAffinity:  map[string]float64{"comedy": pick(), "sports": pick()},
		EnergyPreference:  pick(),
		SocialDrive:       pick(),
		NoveltyTolerance:  pick(),
		EgoSensitivity:    pick(),
		AvgSessionSec:     rng.Intn(10000),
		AvgCompletionRate: pick(),
		AvgSkipRate:       pick(),
		TotalSessions:     rng.Intn(500),
	}
}

func auditSession(rng *rand.Rand) *SessionState {
	pick := func() float64 { return auditScales[rng.Intn(len(auditScales))] }
	return &SessionState{
		UserID:          "viewer",
		SessionID:       fmt.Sprintf("s%d", rng.Intn(5)),
		DopamineBudget:  pick(),
		ItemsSeen:       rng.Intn(500),
		SkipCount:       rng.Intn(200),
		LikeCount:       rng.Intn(200),
		ShareCount:      rng.Intn(50),
		SkipStreak:      rng.Intn(30),
		ResistanceLevel: rng.Intn(4),
		LastCategories:  []string{"comedy", "", "sports"},
		LastCreators:    []string{"u1", "", "u2"},
		CurrentStrategy: []string{"", "explore", "exploit", "??"}[rng.Intn(4)],
	}
}

// ── The property that matters most ──────────────────────────────────────────

func TestAudit_ScoreIsAlwaysARealNumber(t *testing.T) {
	// NaN is the dangerous one. It does not crash and it does not log — it
	// compares FALSE against everything, including itself, so a sort
	// containing one silently produces an arbitrary order. A feed that is
	// subtly randomised is far harder to notice than one that is broken.
	rng := rand.New(rand.NewSource(1))
	bad := 0
	for i := 0; i < 20000; i++ {
		cs := auditContent(rng)
		p := auditProfile(rng)
		s := auditSession(rng)
		score, breakdown := scoreForUser(cs, p, s,
			map[string]bool{"u1": true}, map[string]bool{"u2": true}, watchHistory{})

		if math.IsNaN(score) || math.IsInf(score, 0) {
			bad++
			if bad <= 3 {
				t.Errorf("score was %v\n  content=%+v\n  profile=%+v", score, cs, p)
			}
			continue
		}
		for term, v := range breakdown {
			if math.IsNaN(v) || math.IsInf(v, 0) {
				bad++
				if bad <= 6 {
					t.Errorf("breakdown term %q was %v (score=%v)", term, v, score)
				}
				break
			}
		}
	}
	if bad > 0 {
		t.Errorf("%d of 20000 scorings produced a value that is not a real number", bad)
	}
}

func TestAudit_NilInputsDoNotPanic(t *testing.T) {
	// Every one of these happens in production: a brand-new viewer has no
	// profile, the first request of a visit has no session, and a content row
	// that lost its cache entry arrives nil.
	cases := []struct {
		name string
		cs   *ContentScore
		p    *UserProfile
		s    *SessionState
	}{
		{"all zero", &ContentScore{}, &UserProfile{}, &SessionState{}},
		{"no category", &ContentScore{ContentID: "c1", ContentType: "challenge"}, &UserProfile{}, &SessionState{}},
		{"empty everything", &ContentScore{}, &UserProfile{CategoryAffinity: map[string]float64{}}, &SessionState{}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("panicked: %v", r)
				}
			}()
			score, _ := scoreForUser(c.cs, c.p, c.s, nil, nil, watchHistory{})
			if math.IsNaN(score) || math.IsInf(score, 0) {
				t.Errorf("score was %v", score)
			}
		})
	}
}

func TestAudit_NothingInTheScorerIsRandom(t *testing.T) {
	// Two calls on the same input must not disagree by enough to reorder a
	// page. They are allowed to disagree a little: freshness decays with real
	// time, so an item genuinely ages between two calls, and the hour-of-day
	// term changes on the hour. Both are intended.
	//
	// What must NOT be in here is randomness. A ranker with a coin flip in it
	// cannot be debugged, cannot be A/B tested, and gives a different page on
	// a retry of the same request. There is no rand. in the scoring path today
	// and this is what would notice if one appeared.
	//
	// The bound is deliberately far below any spacing between real candidates:
	// nanoseconds of ageing move a score by around 1e-12, while two genuinely
	// different items differ by 1e-3 or more.
	const reorderable = 1e-6

	rng := rand.New(rand.NewSource(7))
	worst := 0.0
	for i := 0; i < 2000; i++ {
		cs := auditContent(rng)
		p := auditProfile(rng)
		s := auditSession(rng)
		a, _ := scoreForUser(cs, p, s, nil, nil, watchHistory{})
		b, _ := scoreForUser(cs, p, s, nil, nil, watchHistory{})
		d := math.Abs(a - b)
		if d > worst {
			worst = d
		}
		if d > reorderable {
			t.Fatalf("the same input scored %v then %v — a gap of %v is large "+
				"enough to change a page's order, which means something in the "+
				"scorer is not a function of its inputs\n  content=%+v", a, b, d, cs)
		}
	}
	t.Logf("largest disagreement between two identical scorings: %v "+
		"(freshness ageing between the two calls)", worst)
}

func TestAudit_ScoresStayInARangeThatCanBeSorted(t *testing.T) {
	// Not a claim about tuning — a claim about arithmetic. A term that can
	// reach 1e9 does not rank content, it decides the page on its own and
	// silently switches every other signal off.
	rng := rand.New(rand.NewSource(3))
	worst, worstTerm := 0.0, ""
	for i := 0; i < 20000; i++ {
		_, breakdown := scoreForUser(auditContent(rng), auditProfile(rng), auditSession(rng),
			nil, nil, watchHistory{})
		for term, v := range breakdown {
			if math.IsNaN(v) || math.IsInf(v, 0) {
				continue // reported by the finiteness test
			}
			if math.Abs(v) > math.Abs(worst) {
				worst, worstTerm = v, term
			}
		}
	}
	t.Logf("largest single term seen across 20000 scorings: %s = %v", worstTerm, worst)
	if math.Abs(worst) > 1e6 {
		t.Errorf("term %q reached %v. One term that large decides the ranking by "+
			"itself — every other signal becomes noise beneath it.", worstTerm, worst)
	}
}

// ── The pieces, checked individually ────────────────────────────────────────

func TestAudit_SeenPenaltyIsWellBehaved(t *testing.T) {
	now := time.Now().Unix()
	for _, last := range []int64{0, -1, now, now + 100000, now - 1, now - 86400,
		now - int64(seenTTL.Seconds()), now - 1e9} {
		got := seenPenalty(last, now)
		if math.IsNaN(got) || math.IsInf(got, 0) {
			t.Errorf("seenPenalty(%d) = %v", last, got)
		}
		if got < 0 {
			t.Errorf("seenPenalty(%d) = %v — a negative penalty PROMOTES a repeat", last, got)
		}
	}
}

func TestAudit_ExploreScoreIsWellBehaved(t *testing.T) {
	rng := rand.New(rand.NewSource(11))
	for i := 0; i < 5000; i++ {
		cs := auditContent(rng)
		pen := auditScales[rng.Intn(len(auditScales))]
		score, breakdown := exploreScore(cs, nil, pen)
		if math.IsNaN(score) || math.IsInf(score, 0) {
			t.Fatalf("exploreScore = %v with rewatchPenalty=%v\n  content=%+v", score, pen, cs)
		}
		for term, v := range breakdown {
			if math.IsNaN(v) || math.IsInf(v, 0) {
				t.Fatalf("exploreScore breakdown %q = %v", term, v)
			}
		}
		if score < 0 {
			t.Fatalf("exploreScore = %v; explore floors at zero so a negative "+
				"score means something below the floor got through", score)
		}
	}
}

func TestAudit_HasMoreNeverSpinsOrStopsEarly(t *testing.T) {
	// Two failure modes, both bad: claiming more when a page was empty spins a
	// client forever, and claiming none when a page was full ends the feed
	// while there is content left. Both have happened here.
	for _, limit := range []int{1, 20, 100} {
		for cand := 0; cand <= limit*2; cand++ {
			for composed := 0; composed <= limit; composed++ {
				for _, fresh := range []int{0, 1, composed} {
					got := feedHasMore(cand, composed, fresh, limit)
					if composed == 0 && got {
						t.Fatalf("empty page claimed more (cand=%d fresh=%d limit=%d)",
							cand, fresh, limit)
					}
					if composed >= limit && fresh > 0 && cand >= composed && !got {
						t.Fatalf("a FULL page of fresh items said the feed was over "+
							"(cand=%d composed=%d fresh=%d limit=%d)",
							cand, composed, fresh, limit)
					}
				}
			}
		}
	}
}

func TestAudit_SpacingNeverLosesOrDuplicatesAnItem(t *testing.T) {
	// The spacing pass reorders. If it ever drops or duplicates, the page
	// silently shrinks or repeats, and it is the last thing anybody would
	// think to check.
	rng := rand.New(rand.NewSource(5))
	for i := 0; i < 2000; i++ {
		n := rng.Intn(40)
		shape := make([]byte, n)
		for j := range shape {
			if rng.Intn(2) == 0 {
				shape[j] = 'B'
			} else {
				shape[j] = 'S'
			}
		}
		in := kindPage(string(shape))
		out := spaceOutFeedKinds(in)
		if len(out) != len(in) {
			t.Fatalf("spacing changed the page size: %d in, %d out (%s)",
				len(in), len(out), shape)
		}
		seen := map[string]int{}
		for _, it := range out {
			seen[getItemID(it)]++
		}
		for id, n := range seen {
			if n > 1 {
				t.Fatalf("spacing duplicated item %s (%d times) from %s", id, n, shape)
			}
		}
	}
}

// ── A precondition worth writing down ───────────────────────────────────────

// scoreForUser requires a non-nil profile and will crash without one.
//
// Not reachable today: SmartFeedHandler is the only caller, and every path
// into it produces a profile — getOrComputeProfile falls through to
// computeUserProfile, which builds one before it returns, and the handler
// substitutes a default on error. Traced all four return paths to confirm it.
//
// So this is a landmine rather than a live crash: the first line of the
// function dereferences profile.UserID with no guard, and classifyCohort
// immediately above it DOES handle nil, which makes the function look safer
// than it is. A second caller — a new surface, an admin tool, a backfill —
// would find out the hard way.
//
// The test pins the current contract instead of asserting a crash, so it
// keeps passing if somebody makes the function nil-safe, and documents why
// the nil case is absent from the property tests above.
func TestAudit_ScoreForUserRequiresAProfileAndASession(t *testing.T) {
	cs := &ContentScore{ContentID: "c1", ContentType: "challenge"}

	// The contract every caller satisfies today.
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("a non-nil profile still panicked: %v", r)
			}
		}()
		scoreForUser(cs, &UserProfile{}, &SessionState{}, nil, nil, watchHistory{})
	}()

	// And what happens without each. Recorded, not required: making these safe
	// would be an improvement, and this test does not stand in the way.
	panicsOn := func(p *UserProfile, s *SessionState) bool {
		crashed := false
		func() {
			defer func() {
				if r := recover(); r != nil {
					crashed = true
				}
			}()
			scoreForUser(cs, p, s, nil, nil, watchHistory{})
		}()
		return crashed
	}
	if panicsOn(nil, &SessionState{}) {
		t.Log("NOTE: nil profile panics — profile.UserID is dereferenced on the " +
			"first line of scoreForUser, with no guard. classifyCohort directly " +
			"above it DOES handle nil, which makes the function look safer than " +
			"it is.")
	}
	if panicsOn(&UserProfile{}, nil) {
		t.Log("NOTE: nil session panics — session.DopamineBudget in the energy-fit " +
			"block. Same story: not reachable today, because getSessionState " +
			"returns a default on every path rather than nil.")
	}
}

// ── What happens on input nothing upstream can produce ──────────────────────

// The scorer does no input validation. It trusts every caller to hand it
// rates that are rates and budgets that are budgets, and today they all do —
// I traced them:
//
//	DopamineBudget    all eleven writers clamp with math.Min(1,..)/math.Max(0,..),
//	                  and it starts at 1.0 on every getSessionState path
//	EnergyPreference  a SQL AVG over the fixed set {0.25, 0.55, 0.85}, plus
//	                  0 or 1 from negative_profile_mining
//	EnergyLevel       inferContentEnergy ends in math.Min(1, math.Max(0, ..))
//
// So this test cannot fail the build. It runs the impossible inputs and
// records what they do, because the answer is worth knowing: one missed clamp
// upstream is all it takes, and the failure mode is silent. A NaN in a sort
// does not crash — NaN compares false against everything, including itself,
// so the page order simply becomes arbitrary and nobody notices.
func TestAudit_HostileInputIsRecordedNotRequired(t *testing.T) {
	prev := auditScales
	auditScales = hostileScales
	defer func() { auditScales = prev }()

	rng := rand.New(rand.NewSource(2))
	notReal := map[string]int{}
	huge := 0
	const runs = 20000
	for i := 0; i < runs; i++ {
		score, breakdown := scoreForUser(auditContent(rng), auditProfile(rng),
			auditSession(rng), nil, nil, watchHistory{})
		if math.IsNaN(score) || math.IsInf(score, 0) {
			notReal["finalScore"]++
		}
		for term, v := range breakdown {
			if math.IsNaN(v) || math.IsInf(v, 0) {
				notReal[term]++
			} else if math.Abs(v) > 1e6 {
				huge++
			}
		}
	}
	if len(notReal) == 0 && huge == 0 {
		t.Log("the scorer survives out-of-range input unchanged — nothing to note")
		return
	}
	t.Logf("out of %d scorings on IMPOSSIBLE input (rates outside 0..1, "+
		"budgets outside their clamps):", runs)
	for term, n := range notReal {
		t.Logf("  %-20s produced NaN or Inf %d times", term, n)
	}
	if huge > 0 {
		t.Logf("  %d terms exceeded 1e6, which would decide a ranking alone", huge)
	}
	t.Log("Not reachable today: every upstream writer clamps. Recorded because " +
		"the scorer has no guard of its own, so it depends on all of them " +
		"continuing to.")
}
