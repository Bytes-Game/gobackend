package main

import (
	"math"
	"testing"
	"time"
)

// ════════════════════════════════════════════════════════════════════════════════
// DOES THE SCORER ACTUALLY SCORE CORRECTLY?
// ════════════════════════════════════════════════════════════════════════════════
//
// scoreForUser decides what every person sees on every page. Until this file it
// had no direct test — the suite covered the pieces around it (embeddings, the
// seen filter, MMR, calibration, the bandit) and the handler above it, but
// nothing ever called the function itself and checked the number it returned.
//
// That is a bad thing to have untested, and not because it might crash. A
// scorer fails QUIETLY. If freshness stopped counting, or a penalty applied
// with the wrong sign, nothing errors and no test goes red — the feed just gets
// slightly worse for everybody, forever, and the only signal is a retention
// number moving for reasons nobody can attribute.
//
// So these tests assert on directions rather than on magnitudes. "A newer video
// beats an identical older one" stays true through every reweighting; "a newer
// video scores 0.63" would break on the next tuning change and teach nobody
// anything. The point is to catch a term that has stopped working, not to
// freeze the current tuning.

// scoreFixture builds inputs that are valid and boring, so a test can change
// exactly one thing and attribute the difference to it.
func scoreFixture() (*ContentScore, *UserProfile, *SessionState) {
	cs := &ContentScore{
		ContentID:         "c1",
		ContentType:       "challenge",
		AvgCompletionRate: 0.5,
		SkipRate:          0.2,
		QualityScore:      0.5,
		EnergyLevel:       0.5,
		EnergyLevelLabel:  0.5,
		Category:          "comedy",
		CreatorID:         "creator1",
		CreatorLeague:     "Bronze",
		CreatedAt:         time.Now().Add(-24 * time.Hour),
		ViewCount:         1000, // well past the audition target
		LikeCount:         50,
	}
	// Every personality dimension sits at 0.5 — genuinely neutral.
	//
	// Worth spelling out, because leaving them at Go's zero value is a trap and
	// it caught the first version of this file. Zero is not "no opinion", it is
	// the EXTREME of every dimension: AttentionSpan 0 is a hard skimmer,
	// BingeIntensity 0 a casual dipper. A fixture like that quietly makes every
	// comparison a statement about extreme personalities rather than about the
	// signal under test.
	profile := &UserProfile{
		UserID:               "u1",
		CategoryAffinity:     map[string]float64{"comedy": 0.5},
		EnergyPreference:     0.5,
		SocialDrive:          0.5,
		NoveltyTolerance:     0.5,
		EgoSensitivity:       0.5,
		AttentionSpan:        0.5,
		BingeIntensity:       0.5,
		CreatorLoyalty:       0.5,
		CompetitivenessIndex: 0.5,
		MoodVolatility:       0.5,
		EventCount:           500, // past cold start, so the warm path is exercised
	}
	session := &SessionState{
		UserID:         "u1",
		SessionID:      "s1",
		StartedAt:      time.Now().Add(-5 * time.Minute),
		DopamineBudget: 1.0,
		CategoriesSeen: map[string]int{},
		CreatorsSeen:   map[string]int{},
		PageDwellMs:    map[string]int{},
	}
	return cs, profile, session
}

func scoreOnce(cs *ContentScore, p *UserProfile, s *SessionState) (float64, map[string]float64) {
	return scoreForUser(cs, p, s, map[string]bool{}, map[string]bool{}, watchHistory{})
}

// ── The failure that would never announce itself ────────────────────────────
//
// A NaN or an infinity does not crash anything. It flows into the sort, where
// every comparison against it is false, and Go's sort quietly produces garbage
// order. The feed would look shuffled and nothing anywhere would report an
// error. This is the single most important assertion in the file.
func TestScoring_NeverProducesANonFiniteScore(t *testing.T) {
	resetRedis(t)

	hostile := []struct {
		name string
		mut  func(*ContentScore, *UserProfile, *SessionState)
	}{
		{"empty everything", func(cs *ContentScore, p *UserProfile, s *SessionState) {
			*cs = ContentScore{ContentID: "x", ContentType: "challenge"}
			*p = UserProfile{UserID: "u1"}
			*s = SessionState{UserID: "u1", SessionID: "s1"}
		}},
		{"zero views and zero engagement", func(cs *ContentScore, _ *UserProfile, _ *SessionState) {
			cs.ViewCount, cs.LikeCount, cs.QualityScore = 0, 0, 0
		}},
		{"future timestamp", func(cs *ContentScore, _ *UserProfile, _ *SessionState) {
			cs.CreatedAt = time.Now().Add(48 * time.Hour)
		}},
		{"zero-time timestamp", func(cs *ContentScore, _ *UserProfile, _ *SessionState) {
			cs.CreatedAt = time.Time{}
		}},
		{"very old content", func(cs *ContentScore, _ *UserProfile, _ *SessionState) {
			cs.CreatedAt = time.Now().Add(-10 * 365 * 24 * time.Hour)
		}},
		{"exhausted session", func(_ *ContentScore, _ *UserProfile, s *SessionState) {
			s.DopamineBudget = 0
			s.SkipStreak = 50
			s.ItemsSeen = 500
			s.ResistanceLevel = 3
		}},
		{"negative dopamine budget", func(_ *ContentScore, _ *UserProfile, s *SessionState) {
			s.DopamineBudget = -1
		}},
		{"absurd engagement rates", func(cs *ContentScore, _ *UserProfile, _ *SessionState) {
			cs.AvgCompletionRate, cs.SkipRate, cs.RewatchRate = 99, 99, 99
		}},
		{"nil maps on the profile", func(_ *ContentScore, p *UserProfile, _ *SessionState) {
			p.CategoryAffinity = nil
		}},
		{"nil maps on the session", func(_ *ContentScore, _ *UserProfile, s *SessionState) {
			s.CategoriesSeen, s.CreatorsSeen, s.PageDwellMs = nil, nil, nil
		}},
	}

	for _, h := range hostile {
		t.Run(h.name, func(t *testing.T) {
			cs, p, s := scoreFixture()
			h.mut(cs, p, s)

			score, breakdown := scoreOnce(cs, p, s)

			if math.IsNaN(score) || math.IsInf(score, 0) {
				t.Fatalf("score = %v. A non-finite score does not crash — it "+
					"flows into the ranking sort, where every comparison is "+
					"false and the order silently becomes garbage", score)
			}
			for k, v := range breakdown {
				if math.IsNaN(v) || math.IsInf(v, 0) {
					t.Errorf("breakdown[%q] = %v — non-finite", k, v)
				}
			}
		})
	}
}

// ── Each term still does something ──────────────────────────────────────────
//
// One test per signal, changing one input. If a term is accidentally
// disconnected — multiplied by a weight that became zero, overwritten, dropped
// in a refactor — its test fails and names it. Nothing else in the suite would.

func TestScoring_EachSignalStillMoves(t *testing.T) {
	cases := []struct {
		name    string
		better  func(*ContentScore, *UserProfile, *SessionState)
		worse   func(*ContentScore, *UserProfile, *SessionState)
		because string
	}{
		{
			name:   "freshness",
			better: func(cs *ContentScore, _ *UserProfile, _ *SessionState) { cs.CreatedAt = time.Now().Add(-1 * time.Hour) },
			worse: func(cs *ContentScore, _ *UserProfile, _ *SessionState) {
				cs.CreatedAt = time.Now().Add(-60 * 24 * time.Hour)
			},
			because: "recent content must outrank two-month-old content, or the " +
				"feed slowly fills with whatever happened to score well once",
		},
		{
			name:    "quality",
			better:  func(cs *ContentScore, _ *UserProfile, _ *SessionState) { cs.QualityScore = 0.95 },
			worse:   func(cs *ContentScore, _ *UserProfile, _ *SessionState) { cs.QualityScore = 0.05 },
			because: "a well-made video must beat a poor one",
		},
		{
			name: "category match",
			better: func(cs *ContentScore, p *UserProfile, _ *SessionState) {
				cs.Category = "comedy"
				p.CategoryAffinity = map[string]float64{"comedy": 1.0}
			},
			worse: func(cs *ContentScore, p *UserProfile, _ *SessionState) {
				cs.Category = "comedy"
				p.CategoryAffinity = map[string]float64{"sports": 1.0}
			},
			because: "content matching what someone actually watches must win — " +
				"this is the whole personalisation claim",
		},
		{
			name: "trending",
			better: func(cs *ContentScore, _ *UserProfile, _ *SessionState) {
				cs.TrendingScore = 1.0
			},
			worse: func(cs *ContentScore, _ *UserProfile, _ *SessionState) {
				cs.TrendingScore = 0
			},
			because: "something taking off right now must outrank something flat",
		},
		{
			name: "creator fatigue",
			better: func(_ *ContentScore, _ *UserProfile, s *SessionState) {
				s.CreatorsSeen = map[string]int{}
			},
			worse: func(_ *ContentScore, _ *UserProfile, s *SessionState) {
				s.CreatorsSeen = map[string]int{"creator1": 8}
			},
			because: "the ninth video from one creator in a session must rank " +
				"below the first, or the feed turns into one person's channel",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			resetRedis(t)

			csB, pB, sB := scoreFixture()
			c.better(csB, pB, sB)
			scoreB, _ := scoreOnce(csB, pB, sB)

			csW, pW, sW := scoreFixture()
			c.worse(csW, pW, sW)
			scoreW, _ := scoreOnce(csW, pW, sW)

			if scoreB <= scoreW {
				t.Errorf("the %s signal is not moving the score: better=%v worse=%v.\n%s",
					c.name, scoreB, scoreW, c.because)
			}
		})
	}
}

// ── The audition gate ───────────────────────────────────────────────────────
//
// Whether a video is "still being measured" now drives real machinery: how many
// slots a page reserves, which retrieval lane can reach it, and whether it gets
// forced onto a page at all. All of that reads one flag out of this breakdown.

func TestScoring_MarksUnprovenContentAsUnderAudition(t *testing.T) {
	resetRedis(t)

	cs, p, s := scoreFixture()
	cs.ViewCount = 0
	_, bd := scoreOnce(cs, p, s)

	if bd["auditionEligible"] <= 0 {
		t.Error("a video with no views is not marked as under audition — the " +
			"audition lane, the per-page floor and the backlog count all read " +
			"this flag, so none of them would ever fire")
	}
	if bd["coldContentBonus"] <= 0 {
		t.Error("a video with no views gets no cold-content bonus, so it can " +
			"never out-rank established content and never gathers the views " +
			"it needs to be judged")
	}
}

func TestScoring_EstablishedContentIsNotUnderAudition(t *testing.T) {
	resetRedis(t)

	cs, p, s := scoreFixture()
	cs.ViewCount = auditionViewTarget + 1
	_, bd := scoreOnce(cs, p, s)

	if bd["auditionEligible"] > 0 {
		t.Errorf("content with %d views is still marked under audition — it "+
			"would keep taking exploration slots from content that needs them",
			cs.ViewCount)
	}
}

// The bonus has to FADE, not switch off at the threshold. A cliff means a video
// at 299 views outranks a better one at 301 by a fixed jump, which is a
// discontinuity people can farm.
func TestScoring_ColdContentBonusFadesRatherThanCliffs(t *testing.T) {
	resetRedis(t)

	bonusAt := func(views int) float64 {
		cs, p, s := scoreFixture()
		cs.ViewCount = views
		_, bd := scoreOnce(cs, p, s)
		return bd["coldContentBonus"]
	}

	zero := bonusAt(0)
	half := bonusAt(auditionViewTarget / 2)
	nearly := bonusAt(auditionViewTarget - 1)

	if !(zero > half && half > nearly) {
		t.Errorf("the cold-content bonus is not decaying with views: "+
			"0 views=%v, half=%v, nearly-graduated=%v", zero, half, nearly)
	}
	if nearly <= 0 {
		t.Error("the bonus hits zero before the view target, so the last stretch " +
			"of an audition carries no help at all")
	}
}

// ── Internal consistency ────────────────────────────────────────────────────

// The breakdown is not decoration. It is what the learning-to-rank model trains
// on, what the audition picker reads to choose an audience, and what /feed/smart
// returns under ?debug=true when somebody is trying to understand a ranking.
// A breakdown that disagrees with the score it came from silently teaches the
// model the wrong thing.
func TestScoring_BreakdownAgreesWithTheScoreItDescribes(t *testing.T) {
	resetRedis(t)

	cs, p, s := scoreFixture()
	score, bd := scoreOnce(cs, p, s)

	final, ok := bd["finalScore"]
	if !ok {
		t.Fatal("the breakdown carries no finalScore, so nothing downstream can " +
			"check its own arithmetic against the ranking")
	}
	if math.Abs(final-score) > 1e-9 {
		t.Errorf("breakdown finalScore=%v but the returned score is %v — the "+
			"explanation does not describe the decision", final, score)
	}
}

// ── Deliberate randomness, and its bound ────────────────────────────────────
//
// The scorer is NOT deterministic, on purpose, and this is worth knowing
// before anybody tries to reproduce a ranking.
//
// Once the model has enough samples it adds Thompson-sampled exploration noise:
// items it is unsure about get a random nudge so the system finds out about
// them rather than settling forever on what it already believes. That is the
// right design and it has two consequences somebody will eventually trip over:
//
//   * You cannot reproduce a ranking exactly. Two identical requests a
//     millisecond apart legitimately differ.
//   * An A/B result needs enough samples for the noise to average out. A
//     handful of observations is measuring the dice.
//
// The first version of this file asserted plain determinism and failed — but
// only in the full suite, because in isolation the model has no samples, the
// noise is switched off, and the scorer looks deterministic. That is a
// misleading thing to learn from a green test, so it is written down here.

// Everything EXCEPT the exploration term must be stable. If the base score
// wandered, that would be a real bug hiding behind the noise that is supposed
// to be there.
func TestScoring_IsStableApartFromDeliberateExploration(t *testing.T) {
	resetRedis(t)

	baseline := func() map[string]float64 {
		cs, p, s := scoreFixture()
		_, bd := scoreOnce(cs, p, s)
		return bd
	}

	first := baseline()
	for i := 0; i < 5; i++ {
		again := baseline()
		for k, v := range first {
			switch k {
			case "uncertaintyBonus", "finalScore":
				continue // carries the exploration draw by design
			case "freshness", "baseScore":
				// Time-dependent: freshness is measured against now, so it
				// decays between calls. Real, and around 1e-11 per call.
				if math.Abs(again[k]-v) > 1e-6 {
					t.Errorf("run %d: %s moved by %v, far more than clock drift",
						i+2, k, again[k]-v)
				}
				continue
			}
			if math.Abs(again[k]-v) > 1e-9 {
				t.Errorf("run %d: %s = %v, was %v. Only the exploration term is "+
					"meant to vary; anything else moving is a real instability",
					i+2, k, again[k], v)
			}
		}
	}
}

// The exploration nudge has to stay small enough that it explores rather than
// decides. Noise that could outweigh quality and freshness would not be active
// learning, it would be a shuffle.
func TestScoring_ExplorationNoiseStaysSmall(t *testing.T) {
	resetRedis(t)

	const bound = 0.25 // comfortably under what any real signal contributes

	for i := 0; i < 200; i++ {
		cs, p, s := scoreFixture()
		_, bd := scoreOnce(cs, p, s)
		if got := math.Abs(bd["uncertaintyBonus"]); got > bound {
			t.Fatalf("exploration nudge reached %v on draw %d, past the %v "+
				"bound — at that size it is deciding the ranking rather than "+
				"probing it", bd["uncertaintyBonus"], i, bound)
		}
	}
}

// A score has to stay in a range the terms added on top of it can still move.
// If the base ran away to ±1000, the LTR correction (±0.25) and the calibration
// bonus (±0.15) would be rounding error and the learned parts of the ranker
// would stop mattering without anything reporting it.
func TestScoring_StaysInARangeTheLearnedTermsCanStillAffect(t *testing.T) {
	resetRedis(t)

	extremes := []struct {
		name string
		mut  func(*ContentScore, *UserProfile, *SessionState)
	}{
		{"everything at its best", func(cs *ContentScore, p *UserProfile, s *SessionState) {
			cs.QualityScore, cs.AvgCompletionRate, cs.TrendingScore = 1, 1, 1
			cs.SkipRate = 0
			cs.CreatedAt = time.Now()
			p.CategoryAffinity = map[string]float64{"comedy": 1}
		}},
		{"everything at its worst", func(cs *ContentScore, p *UserProfile, s *SessionState) {
			cs.QualityScore, cs.AvgCompletionRate = 0, 0
			cs.SkipRate, cs.NotInterestedCount = 1, 100
			cs.CreatedAt = time.Now().Add(-365 * 24 * time.Hour)
			s.DopamineBudget, s.SkipStreak = 0, 20
			s.CreatorsSeen = map[string]int{"creator1": 20}
		}},
	}

	for _, e := range extremes {
		t.Run(e.name, func(t *testing.T) {
			cs, p, s := scoreFixture()
			e.mut(cs, p, s)
			score, _ := scoreOnce(cs, p, s)

			if score < -10 || score > 10 {
				t.Errorf("score = %v. The learned corrections that ride on top "+
					"are sized in hundredths; a base this large makes them "+
					"rounding error and the model stops mattering silently", score)
			}
		})
	}
}

// ── Personality fit ─────────────────────────────────────────────────────────
//
// This section exists because the first version of this file misread it as a
// bug. A video that 90% of people finished scored BELOW one that 80% skipped,
// which looks alarming and is correct: the fixture had AttentionSpan and
// BingeIntensity at Go's zero value, i.e. the most extreme skimmer possible,
// and a skimmer really is nudged toward punchier content.
//
// Pinning it here so the next person to look at that number finds an
// explanation instead of a mystery.

// A deep watcher should be pushed toward content that holds attention.
func TestScoring_DeepWatchersGetContentThatSustains(t *testing.T) {
	resetRedis(t)

	scoreFor := func(completion float64) float64 {
		cs, p, s := scoreFixture()
		p.AttentionSpan = 1.0  // watches things through
		p.BingeIntensity = 1.0 // long sessions
		cs.AvgCompletionRate = completion
		score, _ := scoreOnce(cs, p, s)
		return score
	}

	if scoreFor(0.9) <= scoreFor(0.2) {
		t.Error("a deep watcher is not being pushed toward content that holds " +
			"attention — the personality dimensions have stopped reaching the score")
	}
}

// ...and a skimmer toward the opposite. This is the case that looked like a bug.
func TestScoring_SkimmersGetPunchierContent(t *testing.T) {
	resetRedis(t)

	scoreFor := func(completion float64) float64 {
		cs, p, s := scoreFixture()
		p.AttentionSpan = 0.0  // does not watch things through
		p.BingeIntensity = 0.0 // dips in and out
		cs.AvgCompletionRate = completion
		score, _ := scoreOnce(cs, p, s)
		return score
	}

	if scoreFor(0.2) <= scoreFor(0.9) {
		t.Error("a skimmer is not being steered toward punchier content — the " +
			"personality nudge has stopped working, or its sign has flipped")
	}
}

// A neutral person should not be steered either way by this term. If they were,
// the dimension would be biasing everybody rather than personalising for the
// people it was measured on.
func TestScoring_NeutralPersonalityIsNotSteeredByCompletion(t *testing.T) {
	resetRedis(t)

	// Reads the term itself rather than the total: the total carries the
	// deliberate exploration draw, which would drown a comparison this precise.
	bonusFor := func(completion float64) float64 {
		cs, p, s := scoreFixture() // fixture is neutral: every dimension at 0.5
		cs.AvgCompletionRate = completion
		_, bd := scoreOnce(cs, p, s)
		return bd["personaBonus"]
	}

	if math.Abs(bonusFor(0.9)-bonusFor(0.2)) > 1e-9 {
		t.Error("completion rate is moving the score for somebody with no " +
			"measured preference either way — a personalisation term should " +
			"do nothing when there is nothing to personalise on")
	}
}

// The nudge must stay a nudge. The comment on it promises a hard bound, and a
// malformed profile must not be able to break that promise — a personality
// term that could swamp quality and freshness would not be personalisation,
// it would be the whole ranking.
func TestScoring_PersonalityNudgeStaysBounded(t *testing.T) {
	resetRedis(t)

	for _, span := range []float64{-5, 0, 0.5, 1, 5} {
		for _, binge := range []float64{-5, 0, 0.5, 1, 5} {
			for _, completion := range []float64{0.01, 0.5, 1.0, 99} {
				cs, p, s := scoreFixture()
				p.AttentionSpan, p.BingeIntensity = span, binge
				cs.AvgCompletionRate = completion
				_, bd := scoreOnce(cs, p, s)

				if got := math.Abs(bd["personaBonus"]); got > 0.08+1e-9 {
					t.Errorf("personaBonus = %v with span=%v binge=%v completion=%v, "+
						"past the ±0.08 bound its own comment promises",
						bd["personaBonus"], span, binge, completion)
				}
			}
		}
	}
}

// Content nobody has watched yet has no completion rate to read. It must come
// out neutral rather than being read as "nobody finishes this" — that would
// penalise every new upload for the deep-watcher audience, which is the exact
// opposite of the push new content is supposed to get.
func TestScoring_UnwatchedContentIsNotPenalisedForHavingNoHistory(t *testing.T) {
	resetRedis(t)

	cs, p, s := scoreFixture()
	p.AttentionSpan, p.BingeIntensity = 1.0, 1.0 // the audience that would suffer
	cs.AvgCompletionRate = 0                     // never watched
	cs.ViewCount = 0

	_, bd := scoreOnce(cs, p, s)

	if bd["personaBonus"] < 0 {
		t.Errorf("personaBonus = %v on content with no watch history — an "+
			"unmeasured video is being treated as a measured bad one, which "+
			"penalises every new upload for exactly the audience most likely "+
			"to sit through it", bd["personaBonus"])
	}
}
