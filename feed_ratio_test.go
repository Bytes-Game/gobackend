package main

import "testing"

// withRatioScoreCache re-enables the content-score cache (TestMain disables
// it binary-wide) so the synthetic scores these tests seed are actually
// served instead of computeContentScore hitting the nil test DB.
func withRatioScoreCache(t *testing.T) {
	t.Helper()
	old := disableContentScoreCache
	disableContentScoreCache = false
	t.Cleanup(func() { disableContentScoreCache = old })
}

// seedRatioScore drops a synthetic ContentScore into the short-TTL cache so
// applyBattleShortRatio's battle/short classification works without a DB.
func seedRatioScore(id string, responses int) {
	contentScoreCache.Set("challenge:"+id, &ContentScore{
		ContentID:     id,
		ContentType:   "challenge",
		ResponseCount: responses,
	})
}

func ratioItem(id string) ScoredItem {
	return ScoredItem{Item: HomeFeedItem{Type: "challenge", Challenge: &Challenge{ID: id}}}
}

func ratioIDs(items []ScoredItem) []string {
	out := make([]string, 0, len(items))
	for _, it := range items {
		out = append(out, getItemID(it.Item))
	}
	return out
}

func TestApplyBattleShortRatioInterleaves(t *testing.T) {
	withRatioScoreCache(t)
	// 6 battles + 2 shorts at the default 3:1 → B B B S B B B S.
	for i, id := range []string{"b1", "b2", "b3", "b4", "b5", "b6"} {
		seedRatioScore(id, i+1)
	}
	seedRatioScore("s1", 0)
	seedRatioScore("s2", 0)

	in := []ScoredItem{
		ratioItem("b1"), ratioItem("s1"), ratioItem("b2"), ratioItem("b3"),
		ratioItem("s2"), ratioItem("b4"), ratioItem("b5"), ratioItem("b6"),
	}
	got := ratioIDs(applyBattleShortRatio("ratio_test_user", in))
	want := []string{"b1", "b2", "b3", "s1", "b4", "b5", "b6", "s2"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("slot %d: got %v, want %v (full: %v)", i, got[i], want[i], got)
		}
	}
}

func TestApplyBattleShortRatioGracefulWhenBattlesRunDry(t *testing.T) {
	withRatioScoreCache(t)
	seedRatioScore("b1", 2)
	seedRatioScore("b2", 1)
	for _, id := range []string{"s1", "s2", "s3", "s4"} {
		seedRatioScore(id, 0)
	}
	in := []ScoredItem{
		ratioItem("s1"), ratioItem("b1"), ratioItem("s2"),
		ratioItem("b2"), ratioItem("s3"), ratioItem("s4"),
	}
	got := ratioIDs(applyBattleShortRatio("ratio_test_user", in))
	// Both battles lead (quota wants them first), then shorts drain in
	// their original relative order — nothing dropped, nothing duplicated.
	want := []string{"b1", "b2", "s1", "s2", "s3", "s4"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("slot %d: got %v, want %v (full: %v)", i, got[i], want[i], got)
		}
	}
}

func TestApplyBattleShortRatioNoOpWithoutMix(t *testing.T) {
	withRatioScoreCache(t)
	seedRatioScore("s1", 0)
	seedRatioScore("s2", 0)
	in := []ScoredItem{ratioItem("s1"), ratioItem("s2")}
	got := ratioIDs(applyBattleShortRatio("ratio_test_user", in))
	if got[0] != "s1" || got[1] != "s2" {
		t.Fatalf("all-shorts feed should pass through unchanged, got %v", got)
	}
}
