package main

// battle_leader.go — which side of a battle is ahead, carried with the video
// itself.
//
// The app opens a battle on the side that is ahead and puts the side behind
// on the turn. It used to learn who was ahead from a second request it sent
// as the battle came on screen, and it waited only 0.4 seconds for the
// answer: any longer and every swipe would feel slow. On a server as slow as
// ours that often missed, and the battle opened on the challenger whatever
// the score — the side behind first.
//
// Now every battle in a feed, a search, a profile or a saved list says who
// is ahead as it arrives ("leader"), counted exactly the way the battle page
// counts it (betterSide in battles.go): genuine votes first, then likes,
// then views, then shares. Nobody ahead — no votes or likes yet, or a dead
// heat — says nothing, and the battle opens on the challenger as before.

import (
	"context"
	"log"
	"strconv"
	"sync"
	"time"
)

const (
	leaderCreator = "creator"
	leaderAnswer  = "answer"

	// leaderBudget bounds how long one page waits for its battles to be
	// counted. A battle not counted in time just says nothing, and the app
	// falls back to asking for itself.
	leaderBudget = 1500 * time.Millisecond

	// leaderAtOnce is how many battles are counted side by side.
	leaderAtOnce = 4
)

// leaderFor reads who is ahead from a battle's standings: the creator, the
// answer shown with the video ([shownResponseID]), or nobody — nobody has
// anything yet, or two sides are level.
func leaderFor(st BattleStandings, shownResponseID string) string {
	who, ahead := "", 0
	for _, p := range st.Participants {
		if !p.Leading {
			continue
		}
		ahead++
		switch {
		case p.Role == "creator":
			who = leaderCreator
		case p.ResponseID == shownResponseID:
			who = leaderAnswer
		}
	}
	if ahead != 1 {
		return ""
	}
	return who
}

// markBattleLeaders fills Leader on every battle in [items].
func markBattleLeaders(items []HomeFeedItem) {
	if db == nil {
		return
	}
	type battle struct {
		at     []int
		answer string
	}
	battles := map[int]*battle{}
	for i, it := range items {
		ch := it.Challenge
		if it.Type != "challenge" || ch == nil || ch.TopResponseID == "" {
			continue
		}
		cid, err := strconv.Atoi(ch.ID)
		if err != nil {
			continue
		}
		if battles[cid] == nil {
			battles[cid] = &battle{answer: ch.TopResponseID}
		}
		battles[cid].at = append(battles[cid].at, i)
	}
	if len(battles) == 0 {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), leaderBudget)
	defer cancel()
	turn := make(chan struct{}, leaderAtOnce)
	var mu sync.Mutex
	var wg sync.WaitGroup
	failed := 0
	var firstErr error
	for cid, b := range battles {
		wg.Add(1)
		go func(cid int, b *battle) {
			defer wg.Done()
			turn <- struct{}{}
			defer func() { <-turn }()
			st, ok, err := loadBattleStandings(ctx, db, cid)
			mu.Lock()
			defer mu.Unlock()
			if err != nil || !ok {
				failed++
				if firstErr == nil && err != nil {
					firstErr = err
				}
				return
			}
			lead := leaderFor(st, b.answer)
			for _, i := range b.at {
				items[i].Challenge.Leader = lead
			}
		}(cid, b)
	}
	wg.Wait()
	if failed > 0 {
		log.Printf("battle leaders: %d of %d battles could not be counted (%v) — "+
			"those say nobody is ahead, and the app asks for itself", failed, len(battles), firstErr)
	}
}
