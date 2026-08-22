package main

// audition.go — giving new videos a fair chance to be measured.
//
// ════════════════════════════════════════════════════════════════════════════════
// THE PROBLEM
// ════════════════════════════════════════════════════════════════════════════════
//
// A video cannot be ranked on merit until somebody has watched it. Until then
// every engagement signal it has is noise: five views cannot tell a good video
// from a bad one, and merit ranking asked to judge on five views will bury it
// permanently. So new content needs to be SHOWN before it can be judged, and
// showing it costs a slot that a known-good video would otherwise have.
//
// That trade is what an audition is. A new video is "under audition" while it
// is still being tried out: it gets exploration impressions so its real
// performance can be measured, and it leaves the audition on its own once there
// is an answer about it.
//
// This file decides HOW MANY of those impressions each page gives away, WHO
// should get them, and how the waiting queue is reached at all. What each video
// COSTS — a small crowd first, a bigger one only if the small one liked it — is
// audition_ladder.go.
//
// ════════════════════════════════════════════════════════════════════════════════
// WHY THE OLD SHAPE RAN OUT
// ════════════════════════════════════════════════════════════════════════════════
//
// The first version reserved exactly one slot per page for one new video. That
// is fine at a hundred uploads a day and quietly fails at fifty thousand, and
// the arithmetic is the whole story:
//
//	one slot per page × pages served per day = auditions available per day
//	auditions available ÷ views needed per video = videos measurable per day
//
// Ten thousand pages a day is ten thousand audition impressions, which at 300
// views each measures about 33 videos. Upload more than that and the surplus
// is never looked at — not because it was judged and found wanting, but
// because nobody ever found out.
//
// Four things were wrong, and they compound:
//
//	1. The slot count was FIXED while the backlog is not. One per page whether
//	   three videos are waiting or three thousand.
//	2. The audience was ARBITRARY. A cooking video shown to someone who never
//	   watches cooking mostly measures "this person is not interested", which we
//	   already knew. Views spent on the wrong viewer teach us very little, so
//	   the video needs far more of them to be judged.
//	3. The slot was a CEILING as well as a floor. A page where three new videos
//	   all deserved a place still got exactly one forced in.
//	4. The queue EXPIRED in practice. Nothing reads "under 300 views" as a
//	   deadline, but every retrieval lane orders by recency inside a bounded
//	   window — so a video that missed its chance in its first days became
//	   unreachable, and no amount of eligibility helped.
//
// This file fixes all four. See each section for the specific reasoning.

import (
	"strconv"
	"sync"
	"time"
)

// ════════════════════════════════════════════════════════════════════════════════
// 1. HOW MANY SLOTS THIS PAGE SHOULD CARRY
// ════════════════════════════════════════════════════════════════════════════════

// auditionBacklogPerSlot is how many waiting videos justify one more slot per
// page.
//
// Not an attempt to clear the queue — a page cannot, and sizing for that would
// hand the whole feed to unproven content. It is a pressure reading: the more
// is waiting, the more of each page goes to finding out about it.
const auditionBacklogPerSlot = 50

// auditionMaxSlots is the hard ceiling regardless of backlog.
//
// Four out of a twenty-item page is a fifth of the feed spent on video nobody
// has vouched for. Past that the feed stops being good, people leave, and the
// audience the auditions were competing for is gone — which helps the waiting
// creators least of all.
const auditionMaxSlots = 4

// auditionMaxPageFraction bounds slots as a share of the page, so a short page
// does not become mostly auditions. A five-item page with four of them unproven
// is not a feed.
const auditionMaxPageFraction = 0.2

// auditionSlotsForPage decides how much of this page goes to unproven video.
//
// Pure, so the curve can be read and tested without a database. Zero backlog
// means zero slots: with nothing waiting, forcing an injection would displace a
// good video to show a video that is not waiting for a chance.
func auditionSlotsForPage(backlog, pageSize int) int {
	if backlog <= 0 || pageSize <= 0 {
		return 0
	}
	slots := (backlog + auditionBacklogPerSlot - 1) / auditionBacklogPerSlot
	if slots > auditionMaxSlots {
		slots = auditionMaxSlots
	}
	byPage := int(float64(pageSize) * auditionMaxPageFraction)
	if byPage < 1 {
		byPage = 1 // even a tiny page gives one chance, or nothing new ever starts
	}
	if slots > byPage {
		slots = byPage
	}
	return slots
}

// auditionBacklogTTL bounds how stale the backlog reading may get. The number
// moves on the timescale of uploads, and it only picks a slot count between 0
// and 4 — a minute of staleness cannot change that answer meaningfully, and
// counting on every feed request would be a table scan per page view.
const auditionBacklogTTL = 60 * time.Second

var (
	auditionBacklogMu      sync.Mutex
	auditionBacklogValue   int
	auditionBacklogFetched time.Time
)

// auditionBacklog counts videos still waiting to be measured.
//
// Deliberately counts every waiting video regardless of age, matching what
// sourceAudition can actually reach — a backlog number that counted only recent
// uploads would shrink to nothing while a real queue sat behind it, and the
// slot count would fall exactly when it was most needed.
//
// "Waiting" means still on the ladder (see audition_ladder.go), not simply
// under-viewed. A video that was tried on a matched crowd and did not land is
// not waiting for anything — counting it would inflate the backlog forever and
// hand more and more of every page to video that has already had its answer.
func auditionBacklog() int {
	auditionBacklogMu.Lock()
	defer auditionBacklogMu.Unlock()
	if time.Since(auditionBacklogFetched) < auditionBacklogTTL {
		return auditionBacklogValue
	}
	if db == nil {
		return 0
	}
	var n int
	err := db.QueryRow(`
		SELECT COUNT(*) FROM challenges
		WHERE visibility = 'arena'
		  AND status IN ('open','active','completed')
		  AND audition_state = $1`, auditionStateActive).Scan(&n)
	if err != nil {
		// Keep the last good reading rather than collapsing to zero. Zero
		// would switch auditions off entirely, which is the worst response to
		// "the database hiccuped".
		return auditionBacklogValue
	}
	auditionBacklogValue = n
	auditionBacklogFetched = time.Now()
	return n
}

// ════════════════════════════════════════════════════════════════════════════════
// 2. WHO SHOULD SEE IT
// ════════════════════════════════════════════════════════════════════════════════
//
// This is the change that alters the arithmetic rather than just the budget.
//
// A view teaches us something only if the viewer might plausibly have wanted
// the video. Shown to a random person, a skip means "this was not for you",
// which is not news. Shown to somebody whose taste matches, a skip means "this
// is not good", which is the thing we are trying to find out.
//
// So the same confidence costs far fewer impressions when the audience is
// chosen. The audition budget above buys several times as much information for
// the same number of slots.
//
// The signals are already computed — every candidate carries them in its score
// breakdown. Nothing new is calculated here; it is the same numbers, read for a
// different question.

// Weights for the interest blend. Relevance leads because it is the most direct
// statement of "this viewer's tastes match this content". Embedding similarity
// is the learned version of the same idea and worth nearly as much. Creator
// affinity is a weaker signal for an unproven video — the creator may be new
// too — but is real when it fires.
const (
	auditionWeightRelevance       = 0.45
	auditionWeightEmbedSimilarity = 0.35
	auditionWeightCreatorAffinity = 0.20
)

// auditionFreshnessWeight keeps recency in the decision as a tiebreak rather
// than as the decision itself.
//
// It used to BE the decision — the old code picked whichever eligible item was
// freshest, which is how a queue of older waiting videos never got picked while
// new uploads kept arriving. Fresh still counts: between two equally-matched
// videos the newer one is the better bet, because the older one has had other
// chances.
const auditionFreshnessWeight = 0.15

// auditionFit scores how well an unproven video suits THIS viewer.
//
// Pure — it reads a breakdown map and returns a number — so the blend can be
// tested directly. Missing keys read as zero, which is correct: a signal that
// was not computed should not vote.
func auditionFit(bd map[string]float64) float64 {
	if bd == nil {
		return 0
	}
	interest := auditionWeightRelevance*bd["relevance"] +
		auditionWeightEmbedSimilarity*bd["embedSim"] +
		auditionWeightCreatorAffinity*bd["creatorAffinityBoost"]
	return interest + auditionFreshnessWeight*bd["freshness"]
}

// ════════════════════════════════════════════════════════════════════════════════
// 3. A FLOOR, NOT A CEILING
// ════════════════════════════════════════════════════════════════════════════════
//
// The old injection ignored what the page had already done. If three unproven
// videos had won places on merit, it still forced a fourth in — and, worse, a
// page that had surfaced none and a page that had surfaced three both got
// exactly one.
//
// Counting what already made it turns the slot count into a floor: "at least
// this many unproven videos on this page". A strong new video that earns two or
// three places keeps them, and no forced injection is spent on top.

// countAuditionsAlreadyPlaced reports how many unproven videos won a place on
// merit, so the injector only has to make up the shortfall.
func countAuditionsAlreadyPlaced(composed []ScoredItem) int {
	n := 0
	for _, it := range composed {
		if it.ScoreBreakdown != nil && it.ScoreBreakdown["auditionEligible"] > 0 {
			n++
		}
	}
	return n
}

// auditionInsertPositions spreads n injections through a page of the given
// size, instead of stacking them all in one place.
//
// Never index 0: the first item is the hook that decides whether the session
// continues at all, and it should be the best thing available rather than
// something unproven. Beyond that they are spaced so a viewer does not hit a
// run of unknown video — the same reasoning as spacing battles and shorts.
func auditionInsertPositions(n, pageLen int) []int {
	if n <= 0 || pageLen <= 0 {
		return nil
	}
	const firstSlot = 3 // far enough in to be reached, past the opening hook
	if firstSlot >= pageLen {
		// The page is shorter than the first slot, so there is nowhere to
		// spread anything — put them all on the end. Returning ONE position
		// here (which this did at first) silently caps a page of three at a
		// single audition however big the floor was, and the only symptom
		// would be a backlog that never drains.
		positions := make([]int, n)
		for i := range positions {
			positions[i] = pageLen
		}
		return positions
	}
	positions := make([]int, 0, n)
	span := pageLen - firstSlot
	step := span / n
	if step < 1 {
		step = 1
	}
	for i := 0; i < n; i++ {
		p := firstSlot + i*step
		if p > pageLen {
			p = pageLen
		}
		positions = append(positions, p)
	}
	return positions
}

// injectAuditionContent guarantees unproven video a share of this page.
//
// slots is the floor from auditionSlotsForPage. Anything that already won a
// place on merit counts toward it, so this only makes up the difference.
//
// Candidates are chosen by auditionFit — how well they suit this viewer — which
// is what makes each impression worth more than the arbitrary pick it replaced.
func injectAuditionContent(scored []ScoredItem, composed []ScoredItem, slots int) []ScoredItem {
	if len(composed) == 0 || slots <= 0 {
		return composed
	}
	need := slots - countAuditionsAlreadyPlaced(composed)
	if need <= 0 {
		// The page earned its share without help. This is the good case, and
		// the old code could not tell it apart from the bad one.
		return composed
	}

	inFeed := make(map[string]bool, len(composed))
	for _, it := range composed {
		inFeed[it.Item.Type+":"+getItemID(it.Item)] = true
	}

	// Rank every eligible candidate by fit, best first.
	type pick struct {
		idx int
		fit float64
	}
	var picks []pick
	for i := range scored {
		bd := scored[i].ScoreBreakdown
		if bd == nil || bd["auditionEligible"] <= 0 {
			continue
		}
		if inFeed[scored[i].Item.Type+":"+getItemID(scored[i].Item)] {
			continue // already surfaced on merit
		}
		picks = append(picks, pick{idx: i, fit: auditionFit(bd)})
	}
	if len(picks) == 0 {
		return composed
	}
	// Insertion sort: this list is short (candidates under the view target in
	// one page's pool) and the alternative pulls in a comparator for no gain.
	for i := 1; i < len(picks); i++ {
		for j := i; j > 0 && picks[j].fit > picks[j-1].fit; j-- {
			picks[j], picks[j-1] = picks[j-1], picks[j]
		}
	}
	if need > len(picks) {
		need = len(picks)
	}

	positions := auditionInsertPositions(need, len(composed))
	out := make([]ScoredItem, 0, len(composed)+need)
	next := 0
	for i := 0; i <= len(composed); i++ {
		for next < len(positions) && positions[next] == i {
			aud := scored[picks[next].idx]
			aud.SlotType = "audition"
			out = append(out, aud)
			next++
		}
		if i < len(composed) {
			out = append(out, composed[i])
		}
	}
	if metricAuditionInjected != nil {
		metricAuditionInjected.Add(float64(need))
	}
	return out
}

// ════════════════════════════════════════════════════════════════════════════════
// 4. THE QUEUE MUST NOT EXPIRE
// ════════════════════════════════════════════════════════════════════════════════
//
// Nothing ever said an audition times out. It timed out anyway.
//
// Eligibility is "still on the ladder", with no mention of age. But a video
// only gets scored if some retrieval lane returned it, and every lane walks a
// bounded recency window ordered newest-first. A video that missed its chance
// in its first days stops being returned by any of them, and its permanent
// eligibility becomes permanently theoretical.
//
// This lane closes that gap: under-viewed content of ANY age, ordered by how
// long it has been waiting. It is how a video posted last month can still
// surface — which is not a quirk, it is the mechanism behind content finding
// its audience late.

// auditionSourceWeight is this lane's share of the candidate budget.
//
// Small on purpose. Its job is to make sure the waiting queue is REACHABLE, not
// to fill the feed with it — how much of the page unproven content actually
// gets is decided by auditionSlotsForPage, downstream of retrieval. A lane that
// cannot reach the queue makes that decision unenforceable; a lane much larger
// than this would crowd out the sources that find good content.
const auditionSourceWeight = 0.05

// sourceAudition retrieves videos still waiting to be measured, oldest wait
// first, with no age limit at all.
//
// Ordered by created_at ASCENDING, which is the opposite of every other lane
// and is the entire point: the video that has been waiting longest is the one
// most in danger of never being seen. Newer uploads are already well served by
// the recency lane.
//
// The filter is audition_state, not a raw view count: a video that has been
// tried and did not land stops riding this lane, so its share of it goes to
// something that has not had its turn yet. That is the trade the ladder makes —
// see audition_ladder.go. Retiring is not removal; the video is still in the
// app, still reachable by every other lane, and gets another full run a month
// later.
func sourceAudition(userID string, limit int) []HomeFeedItem {
	if db == nil || limit <= 0 {
		return nil
	}
	rows, err := db.Query(`
		SELECT c.id, c.creator_id, u.username, u.league, c.video_url,
			c.thumbnail_url, c.prefix, c.subject, c.visibility, c.status,
			c.views, COALESCE(cl.likes,0), c.created_at,
			COALESCE(c.created_at + INTERVAL '24 hours', NOW()),
			(SELECT COUNT(*) FROM challenge_responses WHERE challenge_id = c.id)
		FROM challenges c
		JOIN users u ON c.creator_id = u.id
		LEFT JOIN (SELECT challenge_id, COUNT(*) as likes FROM challenge_likes GROUP BY challenge_id) cl
			ON cl.challenge_id = c.id
		WHERE c.visibility = 'arena'
		AND c.status IN ('open','active','completed')
		AND c.audition_state = $3
		AND c.creator_id != CAST($1 AS INT)
		ORDER BY c.created_at ASC
		LIMIT $2`, userID, limit, auditionStateActive)
	if err != nil {
		return nil // fail quiet: one lane returning nothing never breaks a feed
	}
	defer rows.Close()

	var items []HomeFeedItem
	for rows.Next() {
		var ch Challenge
		var creatorID, views, likes, rc int
		var createdAt, expiresAt time.Time
		if err := rows.Scan(&ch.ID, &creatorID, &ch.CreatorUsername, &ch.CreatorLeague,
			&ch.VideoURL, &ch.ThumbnailURL, &ch.Prefix, &ch.Subject,
			&ch.Visibility, &ch.Status, &views, &likes,
			&createdAt, &expiresAt, &rc); err != nil {
			continue
		}
		ch.CreatorID = strconv.Itoa(creatorID)
		ch.Views = views
		ch.Likes = likes
		ch.CreatedAt = createdAt.Format(time.RFC3339)
		ch.ExpiresAt = expiresAt.Format(time.RFC3339)
		ch.ResponseCount = rc
		items = append(items, HomeFeedItem{Type: "challenge", Challenge: &ch})
	}
	return items
}
