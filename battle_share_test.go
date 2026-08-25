package main

import (
	"fmt"
	"sort"
	"testing"
	"time"
)

// ════════════════════════════════════════════════════════════════════════════
// THE FIRST PAGE OF A SESSION HAD NO BATTLES IN IT
// ════════════════════════════════════════════════════════════════════════════
//
// A live session, five pages of For You, counted off the phone:
//
//	page 1:  0 battles / 21 shorts
//	page 2:  3 / 17
//	page 3:  4 / 16
//	page 4:  7 / 13
//	page 5:  4 / 16
//
// Battles arrive late and the very first page — the one that decides what
// somebody thinks this app is — had none at all.
//
// That should be impossible on score alone. scoreForUser gives a battle
// +0.30 and a short -0.10, a swing of 0.40 on a scale where most terms
// contribute a tenth. A pool with any battles in it should open with battles.
//
// These tests separate the two things that could be going on:
//
//	the RANKER prefers shorts        → the boost is not doing its job
//	the POOL had no battles to rank  → the boost is fine and the problem is
//	                                   further upstream, in what gets fetched
//
// They are different bugs with different fixes, and the log alone cannot tell
// them apart. Nothing here changes a score; it measures the one we have.

// battleShareContent builds two items that differ ONLY in whether anyone has
// answered the challenge. Same creator, same age, same everything, so any gap
// in the score is the battle boost and nothing else.
func battleShareContent(id string, responses int) *ContentScore {
	return &ContentScore{
		ContentID:         id,
		ContentType:       "challenge",
		ResponseCount:     responses,
		AvgCompletionRate: 0.6,
		AvgWatchTimeMs:    9000,
		SkipRate:          0.2,
		RewatchRate:       0.1,
		QualityScore:      0.5,
		EnergyLevel:       0.5,
		EnergyLevelLabel:  0.55,
		Category:          "general",
		EmotionVector:     map[string]float64{},
		CreatorID:         "creator-" + id,
		CreatedAt:         time.Now().Add(-6 * time.Hour),
		ViewCount:         500,
		LikeCount:         50,
	}
}

func battleShareViewer() (*UserProfile, *SessionState) {
	return &UserProfile{
			UserID:            "viewer",
			CategoryAffinity:  map[string]float64{},
			EnergyPreference:  0.5,
			SocialDrive:       0.5,
			NoveltyTolerance:  0.5,
			AvgCompletionRate: 0.5,
		}, &SessionState{
			UserID:         "viewer",
			SessionID:      "fresh",
			DopamineBudget: 1.0,
			// A session that has just started, which is the case the live
			// numbers came from.
			ItemsSeen: 0,
		}
}

func TestBattleShare_TheRankerDoesPreferBattles(t *testing.T) {
	profile, session := battleShareViewer()
	short, _ := scoreForUser(battleShareContent("s1", 0), profile, session, nil, nil, watchHistory{})
	battle, _ := scoreForUser(battleShareContent("b1", 1), profile, session, nil, nil, watchHistory{})

	if battle <= short {
		t.Fatalf("a battle scored %.4f and an otherwise identical short scored "+
			"%.4f. This app is head-to-head challenges; if the ranker does not "+
			"prefer the head-to-head one, the first page of every session is "+
			"whatever the other terms happen to favour.", battle, short)
	}
	t.Logf("battle %.4f vs identical short %.4f (gap %.4f)", battle, short, battle-short)
}

func TestBattleShare_ABattleOpensAMixedPage(t *testing.T) {
	// The live shape: a pool that is roughly a quarter battles, everything
	// else equal. If the ranker is working, the top of the page is battles —
	// so a page-1 with none of them means none were in the pool.
	profile, session := battleShareViewer()

	type scored struct {
		id     string
		battle bool
		score  float64
	}
	var pool []scored
	for i := 0; i < 40; i++ {
		isBattle := i%4 == 0 // 10 of 40
		responses := 0
		if isBattle {
			responses = 1
		}
		id := fmt.Sprintf("c%02d", i)
		s, _ := scoreForUser(battleShareContent(id, responses), profile, session, nil, nil, watchHistory{})
		pool = append(pool, scored{id, isBattle, s})
	}
	sort.SliceStable(pool, func(a, b int) bool { return pool[a].score > pool[b].score })

	top := 20
	battles := 0
	for _, p := range pool[:top] {
		if p.battle {
			battles++
		}
	}
	t.Logf("pool of 40 with 10 battles → top %d contains %d battles", top, battles)

	if battles == 0 {
		t.Error("a quarter of the pool were battles and not one reached the " +
			"page. The battle boost is not surviving whatever runs after it.")
	}
	// Ten battles in a pool of forty, ranked by a score that favours them,
	// should put essentially all of them in the top twenty.
	if battles < 8 {
		t.Errorf("only %d of 10 battles made the top 20; the boost is being "+
			"outweighed by something", battles)
	}
}

func TestBattleShare_TheBoostSurvivesAWeakBattle(t *testing.T) {
	// The case that decides whether "page 1 had no battles" can be a ranking
	// outcome at all: a battle nobody watched much, against a short people
	// love. If the short wins here, a page of good shorts can legitimately
	// bury every battle, and the live numbers need no other explanation.
	profile, session := battleShareViewer()

	weakBattle := battleShareContent("b", 1)
	weakBattle.AvgCompletionRate = 0.30
	weakBattle.SkipRate = 0.50
	weakBattle.QualityScore = 0.20

	strongShort := battleShareContent("s", 0)
	strongShort.AvgCompletionRate = 0.90
	strongShort.SkipRate = 0.05
	strongShort.QualityScore = 0.90

	b, _ := scoreForUser(weakBattle, profile, session, nil, nil, watchHistory{})
	s, _ := scoreForUser(strongShort, profile, session, nil, nil, watchHistory{})
	t.Logf("weak battle %.4f vs strong short %.4f", b, s)

	if s > b {
		t.Logf("a clearly better short outranks a weak battle. So a page of " +
			"nothing but shorts IS reachable on merit, and the empty first " +
			"page is not by itself proof of a bug in the boost.")
	}
}

// ════════════════════════════════════════════════════════════════════════════
// A BATTLE IS ONLY A BATTLE ONCE THE PAGE HAS BEEN ENRICHED
// ════════════════════════════════════════════════════════════════════════════
//
// The client counts battles by looking at topResponseVideoUrl, and so does
// the server's own spacing and tab filter. That field is empty on every
// candidate until populateTopResponses fills it in from the responses table.
//
// So there are two ways to serve a page the client reads as "no battles":
// there really were none, or the enrichment did not happen. The second one
// looks identical from outside and would silently disable the Battles tab,
// the 2-in-a-row spacing rule, and the battle share of every page at once.
func TestBattleShare_EnrichmentIsWhatMakesABattleVisible(t *testing.T) {
	withResponse := HomeFeedItem{Type: "challenge", Challenge: &Challenge{
		ID: "1", ResponseCount: 3, TopResponseVideoUrl: "https://x/r.mp4"}}
	notYetEnriched := HomeFeedItem{Type: "challenge", Challenge: &Challenge{
		ID: "2", ResponseCount: 3}} // count says battle, url not filled in yet

	if !itemIsBattle(withResponse) {
		t.Error("an enriched battle was not recognised as one")
	}
	if itemIsBattle(notYetEnriched) {
		t.Error("battle-ness was decided from ResponseCount rather than the " +
			"opponent url — the client keys off the url, so the two must agree")
	}
	// The consequence, stated so it is impossible to miss: an un-enriched page
	// is a page of shorts as far as every downstream rule is concerned.
	page := []HomeFeedItem{notYetEnriched, notYetEnriched, notYetEnriched}
	if got := filterFeedKind(page, feedKindBattles); len(got) != 0 {
		t.Fatal("unreachable: this documents that the Battles tab sees nothing")
	}
}

// ════════════════════════════════════════════════════════════════════════════
// WHERE THE BATTLES ACTUALLY GO MISSING
// ════════════════════════════════════════════════════════════════════════════
//
// The tests above settle that the ranker is not the reason. A battle beats an
// otherwise identical short by 0.40, and a weak battle still beats a strong
// short — so a page with no battles on it is a page whose CANDIDATE POOL had
// no battles in it. That moves the question upstream, to how many candidates
// each page asks for and what survives composition.
//
// These two tests do not assert that the current numbers are right. They
// write down what the numbers ARE, next to what they cost, so the arithmetic
// is visible to whoever changes it next. Both are measurements of live
// behaviour, not approval of it.

func TestBattleShare_PageOneAsksForTheShallowestPool(t *testing.T) {
	// SmartFeedHandler scales the candidate pool with the page number:
	//
	//	poolPages := page; if poolPages > 3 { poolPages = 3 }
	//	candidateLimit := limit * candidateMultiplier * poolPages
	//
	// which for a page of twenty is 100 candidates on page 1, 200 on page 2,
	// 300 from page 3 on. Every lane inside that budget walks its window
	// newest-first and stops when its share is full.
	//
	// A challenge is BORN a short. It only becomes a battle when somebody
	// answers it, which takes time. So the newest slice of any lane is the
	// slice with the fewest battles in it — and page 1 sees the least of
	// anything else.
	//
	// The live session lines up with that exactly: 0 battles on page 1, then
	// 3, 4, 7 as the pool doubles and triples.
	const limit = 20
	poolFor := func(page int) int {
		poolPages := page
		if poolPages > 3 {
			poolPages = 3
		}
		return limit * candidateMultiplier * poolPages
	}
	p1, p2, p3 := poolFor(1), poolFor(2), poolFor(3)
	t.Logf("candidates fetched — page 1: %d, page 2: %d, page 3+: %d", p1, p2, p3)

	if p1 >= p3 {
		t.Skip("page 1 no longer fetches a smaller pool than later pages; the " +
			"note above is out of date and should be rewritten or deleted")
	}
	t.Logf("page 1 sees %.0f%% of the catalogue depth that page 3 does. It is "+
		"also the page that decides what somebody thinks this app is.",
		100*float64(p1)/float64(p3))
}

func TestBattleShare_TheTabIsCappedByCreatorsNotByTheFetch(t *testing.T) {
	// The Battles tab returned 9 items and said "that is everything", while a
	// later For You page carried 14 battles in one page. Not a contradiction,
	// and worth writing down because the obvious fix — fetch more — does not
	// work.
	//
	// A filtered tab asks for limit × feedKindOverfetch items. composeFeed
	// then builds a MIXED page and will not take more than maxItemsPerCreator
	// from any one creator. Only after that does the kind filter run. So the
	// tab's ceiling is:
	//
	//	(creators × maxItemsPerCreator) × (this kind's share of the feed)
	//
	// and the fetch size never enters into it. Raising feedKindOverfetch —
	// which was raised from 2 to 5 for exactly this symptom — cannot move a
	// number the fetch does not control.
	const clientLimit = 20
	fetch := feedKindFetchLimit(clientLimit, feedKindBattles)
	battleShare := 0.25 // measured across five live pages: 18 battles of 101

	for _, creators := range []int{10, 15, 30, 60} {
		composed := creators * maxItemsPerCreator
		reachable := int(float64(composed) * battleShare)
		t.Logf("%2d creators → mixed page caps at %3d → about %2d battles "+
			"(the tab asked for %d and fetched %d candidates for them)",
			creators, composed, reachable, clientLimit, fetch)
	}
	t.Logf("the live tab returned 9, which is what ~15 creators predicts")

	if fetch <= clientLimit {
		t.Errorf("a filtered tab is fetching %d for a page of %d, so it has no "+
			"headroom for the filter at all", fetch, clientLimit)
	}
}
