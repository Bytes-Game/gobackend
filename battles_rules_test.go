package main

import (
	"testing"
	"time"
)

// The rules that decide battles, tested as plain functions. The queries that
// feed them are tested against Postgres in battles_db_test.go.

func TestBattleLength_AtLeastAWeekAtMostAMonth(t *testing.T) {
	for in, want := range map[int]int{0: 7, 1: 7, 7: 7, 14: 14, 30: 30, 90: 30} {
		if got := clampBattleDays(in); got != want {
			t.Errorf("clampBattleDays(%d) = %d, want %d", in, got, want)
		}
	}
}

func TestLeague_EarnedNotGiven(t *testing.T) {
	if got := leagueFor(5600, 0); got != "Unranked" {
		t.Errorf("no decided battles gave %q — a league has to be earned", got)
	}
	cases := []struct {
		rating int
		want   string
	}{
		{900, "Bronze"}, {1000, "Bronze"}, {1999, "Bronze"}, {2000, "Silver"},
		{2999, "Silver"}, {3000, "Gold"}, {4000, "Platinum"}, {5000, "Diamond"},
		{5999, "Diamond"}, {6000, "Ace"}, {9000, "Ace"},
	}
	for _, c := range cases {
		if got := leagueFor(c.rating, 5); got != c.want {
			t.Errorf("rating %d → %q, want %q", c.rating, got, c.want)
		}
	}
	// Every name here must be one matchmaking understands, or two players
	// in real leagues get compared as "unknown" — and one the battle window
	// and the points table know, or a whole league battles nobody and wins
	// nothing.
	for _, r := range []int{1000, 2500, 3500, 4500, 5500, 6500} {
		l := leagueFor(r, 1)
		if _, ok := leagueTier[l]; !ok {
			t.Errorf("%q is not in leagueTier", l)
		}
		if battleWindow[l] <= 0 {
			t.Errorf("%q has no battle window", l)
		}
		if leagueStakes[l].win <= 0 || leagueStakes[l].loss <= 0 {
			t.Errorf("%q has no points table", l)
		}
	}
}

// Every league is 1,000 points wide, from the 1,000 everybody starts on.
func TestLeague_EveryLeagueIsAThousandPoints(t *testing.T) {
	if got := leagueFor(ratingStart, 1); got != "Bronze" {
		t.Fatalf("the starting points are in %q, want Bronze", got)
	}
	floor := ratingStart
	for _, want := range []string{"Silver", "Gold", "Platinum", "Diamond", "Ace"} {
		floor += 1000
		if leagueFor(floor-1, 1) == want || leagueFor(floor, 1) != want {
			t.Errorf("%s should start at exactly %d", want, floor)
		}
	}
}

func TestGenuineVote_EachWayAVoteIsRemoved(t *testing.T) {
	accepted := time.Now().Add(-48 * time.Hour)
	old := accepted.Add(-30 * 24 * time.Hour)
	participants := map[int]bool{1: true, 2: true}
	regular := voteEvidence{VoterID: 10, AccountCreated: old, Watched: true, ActiveDays: 9}

	if w, r := voteWeight(regular, accepted, participants); w != 1 || r != "" {
		t.Errorf("a regular who watched counted %v (%q), want 1", w, r)
	}
	self := regular
	self.VoterID = 2
	if w, r := voteWeight(self, accepted, participants); w != 0 || r != reasonOwnBattle {
		t.Errorf("a vote in your own battle counted %v (%q)", w, r)
	}
	fresh := regular
	fresh.AccountCreated = accepted.Add(time.Hour)
	if w, r := voteWeight(fresh, accepted, participants); w != 0 || r != reasonNewAccount {
		t.Errorf("an account made after the battle started counted %v (%q)", w, r)
	}
	blind := regular
	blind.Watched = false
	if w, r := voteWeight(blind, accepted, participants); w != 0 || r != reasonNoWatch {
		t.Errorf("a vote without watching counted %v (%q)", w, r)
	}
	thin := regular
	thin.ActiveDays = 1
	if w, r := voteWeight(thin, accepted, participants); w != 0.5 || r != "" {
		t.Errorf("a one-day account counted %v (%q), want a half that is not a removal", w, r)
	}
}

func TestGenuineVote_ABurstOfNewAccountsIsRemoved(t *testing.T) {
	base := time.Now().Add(-30 * 24 * time.Hour)
	thin := func(id, side int, created time.Time) voteEvidence {
		return voteEvidence{VoterID: id, ResponseID: side, AccountCreated: created, Watched: true, ActiveDays: 1}
	}
	votes := []voteEvidence{
		thin(10, 5, base),
		thin(11, 5, base.Add(20*time.Minute)),
		thin(12, 5, base.Add(50*time.Minute)),
		// Same hour, other side: not part of the burst for side 5.
		thin(13, 0, base.Add(10*time.Minute)),
		// A regular made in the same hour is not a throwaway.
		{VoterID: 14, ResponseID: 5, AccountCreated: base.Add(5 * time.Minute), Watched: true, ActiveDays: 12},
	}
	weights := []float64{0.5, 0.5, 0.5, 0.5, 1}
	reasons := make([]string, len(votes))
	markBursts(votes, weights, reasons)

	for i := 0; i < 3; i++ {
		if weights[i] != 0 || reasons[i] != reasonBurst {
			t.Errorf("vote %d from the burst kept weight %v", i, weights[i])
		}
	}
	if weights[3] != 0.5 {
		t.Error("a vote for the other side was swept up in somebody else's burst")
	}
	if weights[4] != 1 {
		t.Error("an established account was removed for being made near a burst")
	}

	// Spread over three hours it is not a burst.
	spread := []voteEvidence{thin(20, 5, base), thin(21, 5, base.Add(90*time.Minute)), thin(22, 5, base.Add(180*time.Minute))}
	w := []float64{0.5, 0.5, 0.5}
	markBursts(spread, w, make([]string, 3))
	for i, x := range w {
		if x != 0.5 {
			t.Errorf("vote %d from accounts made hours apart was removed", i)
		}
	}
}

func TestStandings_VotesFirstThenLikesViewsShares(t *testing.T) {
	sides := []Standing{
		{Username: "likes", Votes: 3, Likes: 50},
		{Username: "votes", Votes: 4, Likes: 0},
		{Username: "tie-a", Votes: 3, Likes: 50},
		{Username: "empty"},
	}
	rankSides(sides)
	if sides[0].Username != "votes" || sides[0].Rank != 1 || !sides[0].Leading {
		t.Errorf("one more genuine vote did not beat fifty likes: %+v", sides[0])
	}
	if sides[1].Rank != 2 || sides[2].Rank != 2 {
		t.Errorf("equal scores got different ranks: %d, %d", sides[1].Rank, sides[2].Rank)
	}
	if sides[3].Leading {
		t.Error("a side with nothing at all was marked leading")
	}

	// Votes level: likes decide.
	tied := []Standing{{Username: "a", Votes: 2, Likes: 1}, {Username: "b", Votes: 2, Likes: 3}}
	rankSides(tied)
	if tied[0].Username != "b" {
		t.Error("with votes level, more likes did not come first")
	}
}

// verdictsOf runs a battle between players on [points] where the first
// side gets [votes][0] votes, and so on, and answers each side's change.
func verdictsOf(t *testing.T, points []int, votes []float64) []battleVerdict {
	t.Helper()
	sides := make([]Standing, len(points))
	records := map[int]competitor{}
	for i := range points {
		sides[i] = Standing{Votes: votes[i], userID: i + 1}
		records[i+1] = competitor{Rating: points[i]}
	}
	got := decideBattle(sides, records)
	byUser := make([]battleVerdict, len(points))
	for _, v := range got {
		byUser[v.userID-1] = v
	}
	return byUser
}

func change(v battleVerdict) int { return v.RatingAfter - v.RatingBefore }

func TestDecide_AWinAndALossInBronze(t *testing.T) {
	vs := verdictsOf(t, []int{1000, 1000}, []float64{5, 2})
	if vs[0].Outcome != "won" || vs[1].Outcome != "lost" {
		t.Fatalf("outcomes %q / %q", vs[0].Outcome, vs[1].Outcome)
	}
	if change(vs[0]) != 40 || change(vs[1]) != -10 {
		t.Errorf("in Bronze the points moved %+d / %+d, want +40 / -10",
			change(vs[0]), change(vs[1]))
	}
}

// Each league up a win is worth less and a loss costs more, so it takes a
// better record to keep climbing.
func TestDecide_EachLeagueUpIsHarder(t *testing.T) {
	want := []struct {
		league     string
		points     int
		win, loss  int
		toClimbPct int // the share of battles you must win just to stay put
	}{
		{"Bronze", 1500, 40, 10, 20},
		{"Silver", 2500, 35, 15, 30},
		{"Gold", 3500, 30, 20, 40},
		{"Platinum", 4500, 25, 25, 50},
		{"Diamond", 5500, 20, 30, 60},
		{"Ace", 6500, 15, 35, 70},
	}
	for _, w := range want {
		// Through decideBattle, so a table nothing reads goes red here.
		won := verdictsOf(t, []int{w.points, w.points}, []float64{3, 1})
		if got := change(won[0]); got != w.win {
			t.Errorf("a win in %s is worth %+d, want +%d", w.league, got, w.win)
		}
		if got := change(won[1]); got != -w.loss {
			t.Errorf("a loss in %s costs %+d, want -%d", w.league, got, w.loss)
		}
		if pct := 100 * w.loss / (w.win + w.loss); pct != w.toClimbPct {
			t.Errorf("%s: you stay put winning %d%% of battles, want %d%%",
				w.league, pct, w.toClimbPct)
		}
	}
}

// What counts is the league each side is in, not the other side's.
func TestDecide_EachSidePaysByTheirOwnLeague(t *testing.T) {
	vs := verdictsOf(t, []int{1980, 2010}, []float64{4, 1})
	if change(vs[0]) != 40 || change(vs[1]) != -15 {
		t.Errorf("Bronze beat Silver: %+d / %+d, want +40 (a Bronze win) and "+
			"-15 (a Silver loss)", change(vs[0]), change(vs[1]))
	}
}

// One win is one win, however many sides there were.
func TestDecide_ABattleWithSeveralAnswers(t *testing.T) {
	vs := verdictsOf(t, []int{1000, 1000, 1000}, []float64{5, 3, 1})
	if change(vs[0]) != 40 || change(vs[1]) != -10 || change(vs[2]) != -10 {
		t.Errorf("three sides moved %+d / %+d / %+d, want +40 / -10 / -10",
			change(vs[0]), change(vs[1]), change(vs[2]))
	}
}

func TestDecide_TiesDrawAndNothingMeansNoResult(t *testing.T) {
	// A draw is worth half a win and half a loss: still worth something
	// in Bronze, nothing in Platinum, and it costs in Ace.
	for points, want := range map[int]int{1000: 15, 4500: 0, 6500: -10} {
		draw := decideBattle([]Standing{
			{Votes: 2, Likes: 1, userID: 1}, {Votes: 2, Likes: 1, userID: 2},
		}, map[int]competitor{1: {Rating: points}, 2: {Rating: points}})
		for _, v := range draw {
			if v.Outcome != "draw" || change(v) != want {
				t.Errorf("an exact tie on %d gave %q, %+d — want a draw, %+d",
					points, v.Outcome, change(v), want)
			}
		}
	}
	none := decideBattle([]Standing{{userID: 1}, {userID: 2}},
		map[int]competitor{1: {Rating: 1200}, 2: {Rating: 900}})
	for _, v := range none {
		if v.Outcome != "none" || v.RatingAfter != v.RatingBefore {
			t.Errorf("a battle nobody voted in, liked or watched gave %q, %d→%d",
				v.Outcome, v.RatingBefore, v.RatingAfter)
		}
	}
}

func TestDecide_BoughtVotesCostEvenAWinner(t *testing.T) {
	got := decideBattle([]Standing{
		{Votes: 4, RawVotes: 10, RemovedVotes: 6, userID: 1},
		{Votes: 3, RawVotes: 3, userID: 2},
	}, map[int]competitor{1: {Rating: 1000}, 2: {Rating: 1000}})
	for _, v := range got {
		if v.userID == 1 {
			if !v.Flagged {
				t.Fatal("six of ten votes removed and the side was not flagged")
			}
			if v.RatingAfter != 1000+40-integrityPenalty {
				t.Errorf("flagged winner ended on %d, want the win minus the penalty (%d)",
					v.RatingAfter, 1000+40-integrityPenalty)
			}
			if v.RatingAfter >= v.RatingBefore {
				t.Errorf("a win with bought votes still paid: %d → %d",
					v.RatingBefore, v.RatingAfter)
			}
		} else if v.Flagged {
			t.Error("the clean side was flagged")
		}
	}
	// In every league: buying votes costs more than the win could pay.
	for league, st := range leagueStakes {
		if integrityPenalty <= st.win {
			t.Errorf("in %s a win pays %d and bought votes cost only %d",
				league, st.win, integrityPenalty)
		}
	}
	// Two removed out of twenty is noise, not a scheme.
	clean := decideBattle([]Standing{
		{Votes: 18, RawVotes: 20, RemovedVotes: 2, userID: 1}, {Votes: 1, userID: 2},
	}, map[int]competitor{1: {Rating: 1000}, 2: {Rating: 1000}})
	for _, v := range clean {
		if v.Flagged {
			t.Error("a couple of removed votes flagged a side")
		}
	}
}
