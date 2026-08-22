package main

// audition_ladder.go — try a video on a small crowd first, and only spend a big
// crowd on the ones that earn it.
//
// ════════════════════════════════════════════════════════════════════════════════
// THE PROBLEM THIS FIXES
// ════════════════════════════════════════════════════════════════════════════════
//
// audition.go decides HOW MANY slots on each page go to unproven video, who
// should see them, and how the waiting queue is reached. It fixed the budget.
//
// It did not fix the price. Every video cost the same 300 views before anyone
// was allowed to say whether it was good, whether the first fifty people loved
// it or every single one of them swiped away in under a second.
//
// That price is what caps the whole system, and the arithmetic is short:
//
//	audition slots available per day ÷ views spent per video = verdicts per day
//
// Anything uploaded above that number never gets a verdict. It is not rejected,
// because nothing ever judged it. It just waits. The creator cannot tell the
// difference — from outside, "measured and beaten" and "never looked at" feel
// identical — but they stop posting either way.
//
// Raising the slot budget buys more verdicts by taking feed away from viewers.
// Lowering the price buys more verdicts for free. This file lowers the price.
//
// ════════════════════════════════════════════════════════════════════════════════
// HOW
// ════════════════════════════════════════════════════════════════════════════════
//
// A video no longer gets one big audience. It gets a small one, and then a
// bigger one only if the small one liked it.
//
//	rung 1 — 60 views.  Everybody gets this. It is cheap.
//	rung 2 — 240 more.  Only videos that did well on rung 1.
//	after  — graduated. It now competes on merit like everything else, and the
//	         trending and breakout machinery takes it from there.
//
// A video that does badly on rung 1 is retired: it stops being handed free
// slots. It is not deleted, not hidden, not unsearchable, and not blocked from
// anyone's feed if it wins a place on merit. It simply stops costing the queue.
//
// The saving is the whole point. If half of all videos stop at rung 1, the
// average video costs 60 + 0.5 × 240 = 180 views instead of 300, so the same
// number of slots produces about 1.7× as many verdicts a day. The stricter the
// bar, the cheaper the average — which is why the bar is not a number typed in
// here but a reading taken from the videos themselves (see the bar section).
//
// ════════════════════════════════════════════════════════════════════════════════
// THE TWO THINGS THIS DELIBERATELY DOES NOT DO
// ════════════════════════════════════════════════════════════════════════════════
//
// It does not judge anyone until it can judge them fairly. The bar is a
// percentile of what other videos on the same rung actually scored. Until
// enough videos have been scored to know what "the same rung" even looks like,
// EVERYBODY IS PROMOTED. On a small app on day one, this file changes nothing
// at all — which is correct, because on a small app there is no queue and
// nothing to save.
//
// It does not make retirement permanent. A retired video gets one more full run
// up the ladder a month later. Content finding its audience weeks after it was
// posted is not a quirk of TikTok, it is a large share of what happens there,
// and a queue that forgets makes it impossible.

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"
	"sync/atomic"
	"time"
)

// ════════════════════════════════════════════════════════════════════════════════
// THE LADDER
// ════════════════════════════════════════════════════════════════════════════════

// auditionRung is one step of the ladder.
//
// views is the audience for THIS rung only, not the running total. A video on
// rung 2 has already been seen 60 times; its 240 is the extra crowd it earned.
// Counting per rung rather than cumulatively is what lets a retired video be
// given a clean second run later without pretending its old views never
// happened.
type auditionRung struct {
	name  string
	views int
}

// auditionLadder is the whole schedule. Order matters: index is the rung
// number stored in challenges.audition_stage.
//
// Sixty for the first look is a judgement call, and it is the number to revisit
// first if the promotion rate on the admin endpoint looks wrong. It is small
// enough that trying a video is cheap and large enough that a completion rate
// measured over it means something — especially now that auditions go to people
// whose taste actually matches (see auditionFit), which is what makes sixty
// matched viewers worth several hundred arbitrary ones.
//
// The rungs add up to auditionViewTarget, so everything elsewhere that asks
// "has this been measured yet" keeps meaning what it meant before.
var auditionLadder = []auditionRung{
	{name: "first look", views: 60},
	{name: "wider look", views: 240},
}

// Audition states, as stored in challenges.audition_state.
const (
	auditionStateActive    = "auditioning" // still being tried out
	auditionStateGraduated = "graduated"   // finished the ladder; ranks on merit now
	auditionStateRetired   = "retired"     // tried, not picked up; stops taking free slots
)

// auditionLadderTotal is the views a video sees across every rung.
func auditionLadderTotal() int {
	n := 0
	for _, r := range auditionLadder {
		n += r.views
	}
	return n
}

// auditionRungFor returns the rung a video is on, and whether that rung exists.
// A stage number past the end of the ladder means the video has finished it.
func auditionRungFor(stage int) (auditionRung, bool) {
	if stage < 0 || stage >= len(auditionLadder) {
		return auditionRung{}, false
	}
	return auditionLadder[stage], true
}

// auditionRungProgress reports how far through its current rung a video is, on
// a 0-to-1 scale: 0 at the moment the rung began, 1 when the rung is complete.
//
// viewsAtStageStart is the view count when the rung began, so the same function
// works for a video's first rung and for a second chance given a month later.
// Pure, so the curve can be read without a database.
func auditionRungProgress(stage, views, viewsAtStageStart int) (float64, bool) {
	rung, ok := auditionRungFor(stage)
	if !ok || rung.views <= 0 {
		return 1, false
	}
	earned := views - viewsAtStageStart
	if earned < 0 {
		earned = 0 // a view count that went backwards; treat the rung as fresh
	}
	p := float64(earned) / float64(rung.views)
	if p >= 1 {
		return 1, false // this rung is done — it is waiting for its verdict
	}
	return p, true
}

// ════════════════════════════════════════════════════════════════════════════════
// WHAT THE FEED NEEDS TO KNOW, WITHOUT ASKING THE DATABASE
// ════════════════════════════════════════════════════════════════════════════════
//
// Scoring runs for every candidate on every page for every viewer. It cannot
// afford a query per item to ask which rung a video is on.
//
// It does not have to. The set of videos currently being tried out is small by
// definition — it is the backlog, the thing the whole system exists to keep
// small — so the entire set fits in memory and is re-read on a timer. Videos
// that have graduated or retired are simply absent, which is exactly the
// question the scorer asks.

// auditionRosterLimit caps the in-memory copy. Well above any healthy backlog;
// if it is ever hit, the longest-waiting videos are the ones kept, because they
// are the ones in danger of never being seen at all.
const auditionRosterLimit = 50000

// auditionRosterEntry is one video's place on the ladder.
type auditionRosterEntry struct {
	stage      int
	stageViews int // view count when this rung began
}

// auditionRoster is the whole set, swapped atomically. A nil pointer means "not
// loaded yet", which is a different answer from "loaded and empty" and is
// handled differently below.
var auditionRoster atomic.Pointer[map[string]auditionRosterEntry]

// auditionStanding answers, for one piece of content: is it still under
// audition, and how far through its current rung is it?
//
// progress feeds the tapering push a new video gets: strongest when the rung
// has just begun, fading to nothing as the rung fills up. It resets at each
// promotion, which is intended — a video that just earned a wider audience
// should be pushed to find it.
//
// The fallback matters as much as the main path. Before the first roster load,
// and for any content type that does not audition, this returns exactly what
// the old flat rule returned. A boot with no roster yet, or a database blip,
// degrades to "everything under 300 views is under audition" rather than to
// "nothing is under audition, no new video gets pushed at all".
func auditionStanding(contentType, contentID string, views int) (progress float64, underAudition bool) {
	flat := func() (float64, bool) {
		if views >= auditionViewTarget {
			return 1, false
		}
		return float64(views) / float64(auditionViewTarget), true
	}

	roster := auditionRoster.Load()
	if roster == nil || contentType != "challenge" {
		return flat()
	}
	entry, ok := (*roster)[contentID]
	if !ok {
		// Loaded, and this video is not in it: it has graduated or retired.
		// Either way it is done taking free slots.
		return 1, false
	}
	return auditionRungProgress(entry.stage, views, entry.stageViews)
}

// refreshAuditionRoster reloads the in-memory copy from the database.
func refreshAuditionRoster() {
	if db == nil {
		return
	}
	rows, err := db.Query(`
		SELECT CAST(id AS TEXT), audition_stage, audition_stage_views
		FROM challenges
		WHERE visibility = 'arena'
		  AND status IN ('open','active','completed')
		  AND audition_state = $1
		ORDER BY created_at ASC
		LIMIT $2`, auditionStateActive, auditionRosterLimit)
	if err != nil {
		// Keep whatever is loaded. Clearing the roster on a failed read would
		// tell the scorer that every new video had finished its audition, which
		// is the most damaging wrong answer available here.
		log.Printf("audition ladder: could not refresh the roster: %v", err)
		return
	}
	defer rows.Close()

	next := make(map[string]auditionRosterEntry, 1024)
	for rows.Next() {
		var id string
		var stage, stageViews int
		if err := rows.Scan(&id, &stage, &stageViews); err != nil {
			continue
		}
		next[id] = auditionRosterEntry{stage: stage, stageViews: stageViews}
	}
	auditionRoster.Store(&next)
}

// ════════════════════════════════════════════════════════════════════════════════
// THE BAR
// ════════════════════════════════════════════════════════════════════════════════
//
// "Did people like it enough" needs a number to compare against, and any number
// written down here would be wrong. Quality readings depend on the audience,
// the categories, the kind of content people upload, and how the scoring
// formula is tuned — all of which move.
//
// So the bar is not written down. It is read off the other videos: to pass a
// rung you have to beat a share of everything else recently judged on that same
// rung. That is self-correcting. If uploads get better, the bar rises with
// them. If the quality formula is retuned tomorrow, the bar follows on its own.

// auditionPromoteFraction is the share of a rung's videos that move up.
//
// At 0.5 the top half of each rung continues. That makes the average video cost
// 60 + 0.5 × 240 = 180 views instead of a flat 300 — the same slots producing
// about 1.7× as many verdicts a day.
//
// This is the throughput dial. Lower it and more videos get a verdict, each on
// thinner evidence. Raise it and each verdict is surer, but fewer creators get
// one. Half is a deliberately gentle setting: it only ever cuts the clearly
// weaker side of a rung, never a video that was merely average.
const auditionPromoteFraction = 0.5

// auditionBarMinSamples is how many scored videos a rung needs before the bar
// means anything.
//
// Below this the answer is always "promote". A bar drawn from four videos is
// not a standard, it is an accident, and retiring somebody's upload on an
// accident is the exact failure this whole file exists to prevent. A new app
// therefore behaves precisely as it did before the ladder existed, and the
// ladder switches itself on when there is enough evidence to be fair.
const auditionBarMinSamples = 30

// auditionBarWindow is how far back the bar looks. Long enough to gather a
// sample on a quiet app, short enough that the standard tracks what is being
// uploaded now rather than what was uploaded last year.
const auditionBarWindow = "60 days"

// auditionBarReading is one rung's bar plus whether it may be used yet, so a
// review pass can read each rung once and carry the answer.
type auditionBarReading struct {
	value float64
	ready bool
}

// auditionBar returns the score to beat on this rung, and whether there is
// enough evidence for it to be applied at all.
func auditionBar(stage int) (bar float64, ready bool) {
	if db == nil {
		return 0, false
	}
	var n int
	var p sql.NullFloat64
	err := db.QueryRow(`
		SELECT COUNT(*),
		       percentile_cont($2::double precision)
		           WITHIN GROUP (ORDER BY audition_score::double precision)
		FROM challenges
		WHERE audition_score IS NOT NULL
		  AND audition_reviewed_stage = $1
		  AND audition_reviewed_at > NOW() - ($3)::interval`,
		stage, 1-auditionPromoteFraction, auditionBarWindow).Scan(&n, &p)
	if err != nil || n < auditionBarMinSamples || !p.Valid {
		return 0, false
	}
	return p.Float64, true
}

// ════════════════════════════════════════════════════════════════════════════════
// THE REVIEW
// ════════════════════════════════════════════════════════════════════════════════
//
// A background pass, not something done while somebody waits for a feed. Two
// reasons: the quality reading costs a couple of queries per video, and the
// decision writes to the row — doing that on the feed path would mean every
// request racing every other request to review the same handful of videos.

// auditionReviewInterval is how often the pass runs. A video sitting one minute
// past its rung costs nothing; the numbers this reads move over hours.
const auditionReviewInterval = 2 * time.Minute

// auditionReviewBatch bounds one pass, so a large backlog on first boot is
// worked through steadily rather than in one long stall.
const auditionReviewBatch = 200

// startAuditionReviewer runs the ladder forever.
//
// Safe to run on several instances at once. Two instances can review the same
// video and reach the same verdict; the write replaces the row's state rather
// than adding to it, so the last one wins with the same answer. Second chances
// cannot be handed out twice either: the second instance's update re-checks the
// row after waiting for the first, finds it no longer retired, and does
// nothing. The only thing a double review costs is one over-counted tick on the
// reviewed metric.
func startAuditionReviewer() {
	go func() {
		// Load the roster before the first tick. Without this the feed spends
		// the first two minutes after every deploy on the flat fallback.
		refreshAuditionRoster()
		t := time.NewTicker(auditionReviewInterval)
		defer t.Stop()
		for range t.C {
			if err := runAuditionReview(context.Background()); err != nil {
				log.Printf("audition ladder: %v", err)
			}
			refreshAuditionRoster()
		}
	}()
}

// runAuditionReview does one pass: hand out second chances, then judge
// everything that has finished a rung.
func runAuditionReview(ctx context.Context) error {
	if db == nil {
		return nil
	}
	// A failed revival must not stop the verdicts. The two halves are
	// independent, and skipping every review because second chances hit a
	// transient error would stall the queue the ladder exists to keep moving.
	if revived, err := reviveRetiredAuditions(ctx); err != nil {
		log.Printf("audition ladder: %v", err)
	} else if revived > 0 {
		log.Printf("audition ladder: gave %d retired video(s) another run.", revived)
	}

	due, err := auditionsDueForReview(ctx, auditionReviewBatch)
	if err != nil {
		return err
	}

	// One reading of the bar per rung, not one per video. The bar shifts on the
	// timescale of weeks; asking for it two hundred times in the same pass would
	// be two hundred identical answers.
	bars := make([]auditionBarReading, len(auditionLadder))
	for i := range auditionLadder {
		v, ready := auditionBar(i)
		bars[i] = auditionBarReading{value: v, ready: ready}
	}

	promoted, retired, graduated := 0, 0, 0
	for _, d := range due {
		outcome, err := reviewOneAudition(ctx, d, bars)
		if err != nil {
			log.Printf("audition ladder: reviewing challenge %s: %v", d.id, err)
			continue
		}
		switch outcome {
		case auditionStateActive:
			promoted++
		case auditionStateRetired:
			retired++
		case auditionStateGraduated:
			graduated++
		}
	}
	if promoted+retired+graduated > 0 {
		log.Printf("audition ladder: reviewed %d — %d moved up, %d finished, %d stopped here.",
			promoted+retired+graduated, promoted, graduated, retired)
		if metricAuditionReviewed != nil {
			metricAuditionReviewed.Add(float64(promoted + retired + graduated))
		}
		if metricAuditionRetired != nil {
			metricAuditionRetired.Add(float64(retired))
		}
	}
	return nil
}

// auditionRetryAfter is how long a retired video waits for its second chance.
//
// A month, because the reason to give one at all is that timing is real:
// posted at 4am, posted the day a bigger story took every feed, posted before
// the creator had any followers. A week is not long enough for any of those to
// have changed; a month usually is.
const auditionRetryAfter = "30 days"

// auditionMaxRetries caps second chances at one.
//
// Not meanness — arithmetic. Every retry is audience spent on a video that has
// already been shown to a matched crowd and did not land. Unlimited retries
// would put the queue back exactly where it started, with old videos endlessly
// re-consuming the slots that new uploads are waiting for.
const auditionMaxRetries = 1

// reviveRetiredAuditions puts eligible retired videos back at the bottom of the
// ladder with a clean rung.
//
// Both view counters are reset to the CURRENT view count, so the new run
// measures the crowd it earns from here rather than instantly re-judging it on
// views it collected months ago.
func reviveRetiredAuditions(ctx context.Context) (int, error) {
	res, err := db.ExecContext(ctx, `
		UPDATE challenges
		   SET audition_state = $1,
		       audition_stage = 0,
		       audition_stage_views = COALESCE(views, 0),
		       audition_run_views = COALESCE(views, 0),
		       audition_retries = audition_retries + 1
		 WHERE audition_state = $2
		   AND audition_retries < $3
		   AND audition_reviewed_at IS NOT NULL
		   AND audition_reviewed_at < NOW() - ($4)::interval`,
		auditionStateActive, auditionStateRetired, auditionMaxRetries, auditionRetryAfter)
	if err != nil {
		return 0, fmt.Errorf("handing out second chances: %w", err)
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

// auditionDue is one video that has finished a rung and is waiting for a
// verdict.
type auditionDue struct {
	id       string
	stage    int
	views    int
	runViews int // view count when this climb up the ladder began
}

// auditionRunSpent is how much of the ladder's audience a video has already
// used on this climb.
//
// A rung's counter restarts at every promotion, which is what a rung is for.
// This is the other half of the accounting: the total across the whole climb,
// so a video that turned up with a large view count cannot keep being promoted
// into a brand-new rung and keep drawing the new-video push with it. That is
// not a hypothetical — a first run against a real database promoted a
// 5,600-view video onto the second rung with its push reset to full strength.
func auditionRunSpent(views, runViews int) int {
	spent := views - runViews
	if spent < 0 {
		return 0
	}
	return spent
}

// auditionDueWhere builds the "has finished its rung" test from the Go ladder,
// so the ladder is described in exactly one place. Retuning a rung's size
// changes this query with it.
//
// Returns the SQL fragment and the arguments it needs, numbered from base+1.
func auditionDueWhere(base int) (string, []any) {
	var clauses []string
	var args []any
	for stage, rung := range auditionLadder {
		clauses = append(clauses, fmt.Sprintf(
			"(audition_stage = %d AND COALESCE(views,0) - audition_stage_views >= $%d)",
			stage, base+len(args)+1))
		args = append(args, rung.views)
	}
	if len(clauses) == 0 {
		return "FALSE", nil
	}
	return "(" + strings.Join(clauses, " OR ") + ")", args
}

// auditionsDueForReview finds videos that have finished a rung, oldest first.
//
// Oldest first for the same reason the retrieval lane is oldest first: the
// video that has been waiting longest is the one closest to never getting an
// answer at all.
func auditionsDueForReview(ctx context.Context, limit int) ([]auditionDue, error) {
	where, args := auditionDueWhere(1)
	args = append([]any{auditionStateActive}, args...)
	args = append(args, limit)

	q := fmt.Sprintf(`
		SELECT CAST(id AS TEXT), audition_stage, COALESCE(views,0), audition_run_views
		FROM challenges
		WHERE audition_state = $1
		  AND visibility = 'arena'
		  AND %s
		ORDER BY created_at ASC
		LIMIT $%d`, where, len(args))

	rows, err := db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("finding videos due for a verdict: %w", err)
	}
	defer rows.Close()

	var out []auditionDue
	for rows.Next() {
		var d auditionDue
		if err := rows.Scan(&d.id, &d.stage, &d.views, &d.runViews); err != nil {
			continue
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// auditionVerdict is the decision itself, with the database taken out of it:
// given where a video is and how it scored, what happens to it and which rung
// does it end up on.
//
// Kept pure because this is the function that decides whether somebody's upload
// gets a bigger audience or stops here, and that is worth being able to read
// and test on its own.
func auditionVerdict(stage, views, runViews int, score, bar float64, barReady bool) (state string, nextStage int) {
	// Below the bar is where it stops. Only where there is enough evidence to
	// draw a bar at all — on a young app there is not, and then every video
	// moves up, which is the behaviour from before the ladder existed.
	if barReady && score < bar {
		// It stays on the rung it was judged on, so a second chance a month
		// later starts from the bottom rather than from a promotion it never
		// earned.
		return auditionStateRetired, stage
	}

	if _, more := auditionRungFor(stage + 1); !more {
		// It climbed the whole ladder, rung by rung.
		return auditionStateGraduated, stage + 1
	}
	if auditionRunSpent(views, runViews) >= auditionLadderTotal() {
		// It has already been seen by more people than the whole ladder was
		// ever going to give it, so there is nothing left to find out. Without
		// this it would be promoted into a brand-new rung with its new-video
		// push reset to full strength, over and over.
		return auditionStateGraduated, stage + 1
	}
	return auditionStateActive, stage + 1
}

// reviewOneAudition judges a single video and writes the outcome. It returns
// the state the video ended up in.
func reviewOneAudition(ctx context.Context, d auditionDue, bars []auditionBarReading) (string, error) {
	// A deliberately fresh reading, not the cached one. The cache exists to
	// serve many feed requests from one computation; a verdict is written once
	// and lived with, so it is worth the two queries to judge current data.
	cs := computeContentScore(d.id, "challenge")
	score := 0.0
	if cs != nil {
		score = cs.QualityScore
	}

	// A rung with no reading is one nothing has been judged on yet, which means
	// no bar and everybody moves up.
	var bar auditionBarReading
	if d.stage >= 0 && d.stage < len(bars) {
		bar = bars[d.stage]
	}
	state, next := auditionVerdict(d.stage, d.views, d.runViews, score, bar.value, bar.ready)

	_, err := db.ExecContext(ctx, `
		UPDATE challenges
		   SET audition_state = $1,
		       audition_stage = $2,
		       audition_stage_views = $3,
		       audition_score = $4,
		       audition_reviewed_stage = $5,
		       audition_reviewed_at = NOW()
		 WHERE id = $6`,
		state, next, d.views, score, d.stage, d.id)
	if err != nil {
		return "", fmt.Errorf("writing the verdict: %w", err)
	}
	return state, nil
}
