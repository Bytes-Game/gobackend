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
	if got := leagueFor(1600, 0); got != "Unranked" {
		t.Errorf("no decided battles gave %q — a league has to be earned", got)
	}
	cases := []struct {
		rating int
		want   string
	}{
		{900, "Bronze"}, {1049, "Bronze"}, {1050, "Silver"}, {1150, "Gold"},
		{1300, "Platinum"}, {1500, "Diamond"},
	}
	for _, c := range cases {
		if got := leagueFor(c.rating, 5); got != c.want {
			t.Errorf("rating %d → %q, want %q", c.rating, got, c.want)
		}
	}
	// Every name here must be one matchmaking understands, or two players
	// in real leagues get compared as "unknown".
	for _, r := range []int{900, 1100, 1200, 1400, 1600} {
		if _, ok := leagueTier[leagueFor(r, 1)]; !ok {
			t.Errorf("%q is not in leagueTier", leagueFor(r, 1))
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

func TestDecide_WinnerGainsWhatTheLoserLoses(t *testing.T) {
	sides := []Standing{
		{Username: "c", Role: "creator", Votes: 5, userID: 1},
		{Username: "r", Role: "responder", Votes: 2, userID: 2, responseID: 9},
	}
	got := decideBattle(sides, map[int]competitor{1: {Rating: 1000}, 2: {Rating: 1000}})
	byUser := map[int]battleVerdict{}
	for _, v := range got {
		byUser[v.userID] = v
	}
	w, l := byUser[1], byUser[2]
	if w.Outcome != "won" || l.Outcome != "lost" {
		t.Fatalf("outcomes %q / %q", w.Outcome, l.Outcome)
	}
	if w.RatingAfter-w.RatingBefore != 16 || l.RatingAfter-l.RatingBefore != -16 {
		t.Errorf("equal ratings moved %+d / %+d, want +16 / -16",
			w.RatingAfter-w.RatingBefore, l.RatingAfter-l.RatingBefore)
	}
}

func TestDecide_BeatingSomebodyBetterIsWorthMore(t *testing.T) {
	upset := decideBattle([]Standing{
		{Votes: 3, userID: 1}, {Votes: 1, userID: 2},
	}, map[int]competitor{1: {Rating: 1000}, 2: {Rating: 1400}})
	expected := decideBattle([]Standing{
		{Votes: 3, userID: 2}, {Votes: 1, userID: 1},
	}, map[int]competitor{1: {Rating: 1000}, 2: {Rating: 1400}})
	gain := func(vs []battleVerdict, id int) int {
		for _, v := range vs {
			if v.userID == id {
				return v.RatingAfter - v.RatingBefore
			}
		}
		t.Fatalf("no verdict for %d", id)
		return 0
	}
	if gain(upset, 1) <= gain(expected, 2) {
		t.Errorf("an underdog's win (+%d) paid no more than a favourite's (+%d) — "+
			"the top would be a place you keep without earning", gain(upset, 1), gain(expected, 2))
	}
	if gain(upset, 2) >= 0 {
		t.Error("the favourite lost and their rating did not drop")
	}
}

func TestDecide_TiesDrawAndNothingMeansNoResult(t *testing.T) {
	draw := decideBattle([]Standing{
		{Votes: 2, Likes: 1, userID: 1}, {Votes: 2, Likes: 1, userID: 2},
	}, map[int]competitor{1: {Rating: 1000}, 2: {Rating: 1000}})
	for _, v := range draw {
		if v.Outcome != "draw" || v.RatingAfter != v.RatingBefore {
			t.Errorf("an exact tie gave %q, %d→%d", v.Outcome, v.RatingBefore, v.RatingAfter)
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
			if v.RatingAfter != 1000+16-integrityPenalty {
				t.Errorf("flagged winner ended on %d, want the win minus the penalty (%d)",
					v.RatingAfter, 1000+16-integrityPenalty)
			}
		} else if v.Flagged {
			t.Error("the clean side was flagged")
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
