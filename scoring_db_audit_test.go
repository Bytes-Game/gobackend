package main

import (
	"database/sql"
	"fmt"
	"math"
	"os"
	"testing"
	"time"

	_ "github.com/lib/pq"
)

// ════════════════════════════════════════════════════════════════════════════
// THE TWO FUNCTIONS NO TEST HAD EVER RUN
// ════════════════════════════════════════════════════════════════════════════
//
// computeContentScore builds the ContentScore for EVERY video the ranker
// looks at — quality, trending, energy, category, the creator's numbers. If
// it is wrong, every other signal is reasoning about wrong facts, and none of
// the property tests in scoring_audit_test.go would notice, because they feed
// the scorer a ContentScore rather than making one.
//
// rankColdStartItems is the entire first experience of a new viewer. It runs
// instead of the main ranker until somebody has fifteen events, so it decides
// what a person sees in the minutes when they are deciding whether to stay.
//
// Both were at 0% coverage. Neither can be tested without a database, which
// is presumably why. So this brings one up.
//
// Skips cleanly when there is no database to talk to, so CI stays green
// without one — but the moment TEST_DATABASE_URL is set, these run for real
// against real Postgres with the real schema from runMigrations.

func withDB(t *testing.T) func() {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("no TEST_DATABASE_URL — skipping the database-backed audit")
	}
	conn, err := sql.Open("postgres", url)
	if err != nil {
		t.Skipf("cannot open %s: %v", url, err)
	}
	if err := conn.Ping(); err != nil {
		t.Skipf("cannot reach the database: %v", err)
	}
	prev := db
	db = conn

	// The caches are global and per-process. Left on, one test's answer is
	// served to the next; these flags exist for exactly this.
	prevCS, prevUP := disableContentScoreCache, disableUserProfileCache
	disableContentScoreCache, disableUserProfileCache = true, true

	// The real schema, built by the real migration path.
	runMigrations()
	truncateAudit(t)
	auditCreator(t)

	return func() {
		truncateAudit(t)
		disableContentScoreCache, disableUserProfileCache = prevCS, prevUP
		_ = conn.Close()
		db = prev
	}
}

func truncateAudit(t *testing.T) {
	t.Helper()
	// No RESTART IDENTITY: ids keep climbing, so no two tests can ever land on
	// the same content id and read each other's cached score.
	for _, tbl := range []string{"feed_events", "challenges", "user_profiles", "users"} {
		if _, err := db.Exec("TRUNCATE " + tbl + " CASCADE"); err != nil {
			t.Logf("truncate %s: %v", tbl, err)
		}
	}
}

// auditCreator makes sure there is somebody to own the fixtures. challenges
// has a foreign key to users, which is worth honouring rather than dropping:
// a fixture that could not exist in production tests a shape production never
// sees.
func auditCreator(t *testing.T) {
	t.Helper()
	if _, err := db.Exec(`
		INSERT INTO users (id, username, password) VALUES (1, 'creator', 'x')
		ON CONFLICT (id) DO NOTHING`); err != nil {
		t.Fatalf("seed creator: %v", err)
	}
	// Likers, so a fixture can produce likes the way production does.
	for i := 2; i <= auditLikers+1; i++ {
		if _, err := db.Exec(`
			INSERT INTO users (id, username, password) VALUES ($1, $2, 'x')
			ON CONFLICT (id) DO NOTHING`, i, fmt.Sprintf("liker%d", i)); err != nil {
			t.Fatalf("seed liker %d: %v", i, err)
		}
	}
}

// auditLikers is how many distinct people exist to press like. A like is a
// row keyed by (challenge, user), so a fixture cannot have more likes than
// there are people — the same constraint production has.
const auditLikers = 900

// auditLike records real likes on a challenge.
//
// Rows in challenge_likes, NOT the likes_count column — because that column
// is maintained by a database trigger on this table, and computeContentScore
// counts the rows rather than reading the counter.
//
// Writing the counter directly is what a first version of this fixture did,
// and it produced a state production cannot reach: counter says 700, table
// holds none. The test then "found" a bug that was really a fixture pretending
// to be a database.
func auditLike(t *testing.T, challengeID string, n int) {
	t.Helper()
	if n > auditLikers {
		t.Fatalf("fixture asked for %d likes but only %d likers are seeded; "+
			"raise auditLikers or express the case with a rate instead of a "+
			"raw count", n, auditLikers)
	}
	for i := 0; i < n; i++ {
		if _, err := db.Exec(`
			INSERT INTO challenge_likes (challenge_id, user_id) VALUES ($1, $2)
			ON CONFLICT DO NOTHING`, challengeID, 2+i); err != nil {
			t.Fatalf("like %d: %v", i, err)
		}
	}
}

// auditChallenge inserts one challenge and returns its id.
func auditChallenge(t *testing.T, opts map[string]any) string {
	t.Helper()
	get := func(k string, def any) any {
		if v, ok := opts[k]; ok {
			return v
		}
		return def
	}
	var id int
	err := db.QueryRow(`
		INSERT INTO challenges (creator_id, video_url, thumbnail_url, prefix, subject,
		                        visibility, status, category, energy_level, views,
		                        created_at)
		VALUES ($1,'https://v/x.mp4','https://t/x.jpg','can you',$2,'public','open',
		        $3,$4,$5,$6)
		RETURNING id`,
		get("creator", 1),
		get("subject", "a subject"),
		get("category", "comedy"),
		get("energy", "medium"),
		get("views", 100),
		get("created", time.Now().Add(-24*time.Hour)),
	).Scan(&id)
	if err != nil {
		t.Fatalf("insert challenge: %v", err)
	}
	cid := fmt.Sprint(id)
	if n, ok := opts["likes"].(int); ok && n > 0 {
		auditLike(t, cid, n)
	}
	return cid
}

// ── computeContentScore ─────────────────────────────────────────────────────

func TestDBAudit_ContentScoreReadsWhatIsActuallyStored(t *testing.T) {
	defer withDB(t)()

	id := auditChallenge(t, map[string]any{
		"category": "sports", "energy": "high", "views": 5000, "likes": 700,
	})

	cs := computeContentScore(id, "challenge")
	if cs == nil {
		t.Fatal("computeContentScore returned nil — the whole ranker reads this")
	}
	if cs.ContentID != id || cs.ContentType != "challenge" {
		t.Errorf("identity came back wrong: %q/%q", cs.ContentID, cs.ContentType)
	}
	if cs.Category != "sports" {
		t.Errorf("category is %q, stored as sports — every taste signal keys off "+
			"this, so a wrong one silently mis-files the video", cs.Category)
	}
	if cs.ViewCount != 5000 || cs.LikeCount != 700 {
		t.Errorf("views/likes came back %d/%d, stored as 5000/700",
			cs.ViewCount, cs.LikeCount)
	}
	if cs.EnergyLevel <= 0.5 {
		t.Errorf("energy is %v for a video stored as high; the energy-fit term "+
			"compares this against the viewer's preference, so a flat value "+
			"switches that whole signal off", cs.EnergyLevel)
	}
}

func TestDBAudit_ContentScoreIsAlwaysUsableByTheScorer(t *testing.T) {
	// The join that matters: whatever this builds gets handed straight to
	// scoreForUser. A field it leaves as NaN, or a nil map, becomes a broken
	// score or a panic one function later.
	defer withDB(t)()

	cases := []map[string]any{
		{"views": 0, "likes": 0},                                // nobody has watched it
		{"views": 1, "likes": 0},                                // one view, no likes
		{"views": 0, "likes": 5},                                // more likes than views
		{"views": 800, "likes": 800},                            // a 100% like rate
		{"category": "", "energy": ""},                          // nothing filled in
		{"category": "NOT A CATEGORY", "energy": "bogus"},       // junk the schema allows
		{"created": time.Now().Add(48 * time.Hour)},             // clock skew, from the future
		{"created": time.Now().Add(-10 * 365 * 24 * time.Hour)}, // ancient
		{"subject": ""},
	}
	for i, opts := range cases {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			id := auditChallenge(t, opts)
			cs := computeContentScore(id, "challenge")
			if cs == nil {
				t.Fatal("nil ContentScore")
			}
			for name, v := range map[string]float64{
				"AvgCompletionRate": cs.AvgCompletionRate, "SkipRate": cs.SkipRate,
				"RewatchRate": cs.RewatchRate, "EngagementVelocity": cs.EngagementVelocity,
				"TrendingScore": cs.TrendingScore, "QualityScore": cs.QualityScore,
				"EnergyLevel": cs.EnergyLevel, "EnergyLevelLabel": cs.EnergyLevelLabel,
				"CreatorWinRate": cs.CreatorWinRate,
			} {
				if math.IsNaN(v) || math.IsInf(v, 0) {
					t.Errorf("%s = %v — this goes straight into scoreForUser, and a "+
						"NaN there makes the page order arbitrary", name, v)
				}
			}
			if cs.EmotionVector == nil {
				t.Error("EmotionVector is nil; the emotion-match term ranges over it")
			}

			// The real join: does the scorer survive what this produced?
			score, breakdown := scoreForUser(cs, &UserProfile{UserID: "u1"},
				&SessionState{DopamineBudget: 0.5}, nil, nil, watchHistory{})
			if math.IsNaN(score) || math.IsInf(score, 0) {
				t.Errorf("scoring what computeContentScore built gave %v", score)
			}
			for term, v := range breakdown {
				if math.IsNaN(v) || math.IsInf(v, 0) {
					t.Errorf("term %q = %v", term, v)
				}
			}
		})
	}
}

func TestDBAudit_ContentScoreOnSomethingThatDoesNotExist(t *testing.T) {
	// A row deleted between being listed and being scored. Common enough:
	// moderation removes something while a feed request is in flight.
	defer withDB(t)()

	for _, id := range []string{"999999", "0", "-1", "", "not-a-number"} {
		t.Run(id, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("panicked on a missing row: %v", r)
				}
			}()
			cs := computeContentScore(id, "challenge")
			if cs == nil {
				t.Fatal("nil — the call site does not check, so this is a crash")
			}
			score, _ := scoreForUser(cs, &UserProfile{UserID: "u1"},
				&SessionState{DopamineBudget: 0.5}, nil, nil, watchHistory{})
			if math.IsNaN(score) || math.IsInf(score, 0) {
				t.Errorf("a missing row scored %v", score)
			}
		})
	}
}

// ── rankColdStartItems ──────────────────────────────────────────────────────

func TestDBAudit_ColdStartKeepsEveryItem(t *testing.T) {
	// It reorders a new viewer's first page. Losing one is the difference
	// between a full feed and a short one, on the single request where a
	// person decides whether the app is worth keeping.
	defer withDB(t)()

	for _, n := range []int{0, 1, 2, 3, 10, 50} {
		items := make([]HomeFeedItem, 0, n)
		for i := 0; i < n; i++ {
			items = append(items, HomeFeedItem{
				Type: "challenge",
				Challenge: &Challenge{
					ID: fmt.Sprint(i), Category: "comedy",
					Views: i * 10, Likes: i,
					CreatedAt: time.Now().Add(-time.Duration(i) * time.Hour).Format(time.RFC3339),
				},
			})
		}
		out := rankColdStartItems("newcomer", items)
		if len(out) != n {
			t.Errorf("%d items in, %d out", n, len(out))
		}
		seen := map[string]int{}
		for _, it := range out {
			seen[getItemID(it)]++
		}
		for id, c := range seen {
			if c > 1 {
				t.Errorf("item %s appears %d times", id, c)
			}
		}
	}
}

func TestDBAudit_ColdStartPutsBetterContentFirst(t *testing.T) {
	// The one thing it is for. A brand-new viewer has no taste profile, so
	// the only thing to go on is whether other people liked it — and if that
	// ordering does not work, a new person's first page is arbitrary.
	defer withDB(t)()

	mk := func(id string, views, likes int) HomeFeedItem {
		return HomeFeedItem{Type: "challenge", Challenge: &Challenge{
			ID: id, Category: "comedy", Views: views, Likes: likes,
			CreatedAt: time.Now().Add(-24 * time.Hour).Format(time.RFC3339),
		}}
	}
	// Same age, same category — only the reception differs.
	items := []HomeFeedItem{
		mk("poor", 1000, 5),
		mk("great", 1000, 800),
		mk("ok", 1000, 200),
	}
	out := rankColdStartItems("newcomer", items)
	if len(out) != 3 {
		t.Fatalf("got %d items", len(out))
	}
	if getItemID(out[0]) != "great" {
		order := []string{}
		for _, it := range out {
			order = append(order, getItemID(it))
		}
		t.Errorf("a new viewer's first page leads with %q; order was %v. The "+
			"best-received video should lead when nothing else is known.",
			getItemID(out[0]), order)
	}
}

func TestDBAudit_ColdStartIsNotFooledByVolume(t *testing.T) {
	// A video with a million views and a 0.1% like rate is worse than one
	// with a hundred views and a 50% like rate. Ranking by raw counts is the
	// classic mistake and the reason wilsonLowerBound is in there.
	defer withDB(t)()

	mk := func(id string, views, likes int) HomeFeedItem {
		return HomeFeedItem{Type: "challenge", Challenge: &Challenge{
			ID: id, Category: "comedy", Views: views, Likes: likes,
			CreatedAt: time.Now().Add(-24 * time.Hour).Format(time.RFC3339),
		}}
	}
	out := rankColdStartItems("newcomer", []HomeFeedItem{
		mk("popular-but-poor", 1000000, 1000), // 0.1% liked it
		mk("small-but-loved", 400, 200),       // 50% liked it
		mk("filler", 500, 25),
	})
	if getItemID(out[0]) != "small-but-loved" {
		order := []string{}
		for _, it := range out {
			order = append(order, getItemID(it))
		}
		t.Errorf("volume beat quality: order was %v. A million views at a 0.1%% "+
			"like rate should not outrank 400 views at 50%%.", order)
	}
}

func TestDBAudit_ColdStartSurvivesBrokenItems(t *testing.T) {
	// Real rows are missing things. A null category, an unparseable date, a
	// challenge pointer that never got filled in.
	defer withDB(t)()

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("panicked on imperfect items: %v", r)
		}
	}()
	items := []HomeFeedItem{
		{Type: "challenge", Challenge: &Challenge{ID: "a", CreatedAt: "not a date"}},
		{Type: "challenge", Challenge: &Challenge{ID: "b", Views: 0, Likes: 99}},
		{Type: "challenge", Challenge: nil},
		{Type: "challenge", Challenge: &Challenge{ID: "d", Views: -5, Likes: -5}},
		{Type: "", Challenge: &Challenge{ID: "e"}},
	}
	out := rankColdStartItems("newcomer", items)
	if len(out) != len(items) {
		t.Errorf("%d items in, %d out — imperfect rows were dropped rather than "+
			"ranked last", len(items), len(out))
	}
}

func TestDBAudit_ColdStartUsesWhatTheViewerSaidTheyLiked(t *testing.T) {
	// Onboarding asks people what they are into. If that does not reach the
	// first page, the question was for nothing.
	defer withDB(t)()

	if _, err := db.Exec(`
		INSERT INTO user_profiles (user_id, category_affinity, last_computed_at)
		VALUES ('picky', '{"sports": 0.9}'::jsonb, NOW())`); err != nil {
		t.Skipf("could not seed a profile: %v", err)
	}

	mk := func(id, cat string) HomeFeedItem {
		return HomeFeedItem{Type: "challenge", Challenge: &Challenge{
			ID: id, Category: cat, Views: 1000, Likes: 100,
			CreatedAt: time.Now().Add(-24 * time.Hour).Format(time.RFC3339),
		}}
	}
	// Identical reception; only the category differs.
	out := rankColdStartItems("picky", []HomeFeedItem{
		mk("comedy1", "comedy"), mk("sports1", "sports"), mk("music1", "music"),
	})
	if getItemID(out[0]) != "sports1" {
		order := []string{}
		for _, it := range out {
			order = append(order, getItemID(it))
		}
		t.Errorf("a viewer who said they like sports leads with %v — the "+
			"onboarding answer is not reaching their first page", order)
	}
}
