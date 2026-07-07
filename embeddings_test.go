package main

import (
	"testing"
	"time"
)

func TestL2Norm_ZeroVectorUnchanged(t *testing.T) {
	v := make([]float64, embedDim)
	out := l2norm(v)
	for _, x := range out {
		if x != 0 {
			t.Fatalf("zero vector should stay zero, got %v", out)
		}
	}
}

func TestL2Norm_UnitLength(t *testing.T) {
	v := make([]float64, embedDim)
	v[0] = 3
	v[1] = 4
	out := l2norm(v)
	var sum float64
	for _, x := range out {
		sum += x * x
	}
	if sum < 0.999 || sum > 1.001 {
		t.Fatalf("expected unit length, got sum-of-squares %v", sum)
	}
}

func TestCosineSim_Identical(t *testing.T) {
	v := make([]float64, embedDim)
	v[0] = 1
	v[1] = 1
	v = l2norm(v)
	if sim := cosineSim(v, v); sim < 0.999 {
		t.Fatalf("cosine of vector with itself should be ~1, got %v", sim)
	}
}

func TestCosineSim_Opposite(t *testing.T) {
	a := make([]float64, embedDim)
	b := make([]float64, embedDim)
	a[0] = 1
	b[0] = -1
	a = l2norm(a)
	b = l2norm(b)
	if sim := cosineSim(a, b); sim > -0.999 {
		t.Fatalf("opposite vectors should be ~-1, got %v", sim)
	}
}

func TestCosineSim_MismatchedLen(t *testing.T) {
	if sim := cosineSim([]float64{1}, []float64{1, 2}); sim != 0 {
		t.Fatalf("mismatched length should return 0, got %v", sim)
	}
}

func TestBuildContentEmbedding_Deterministic(t *testing.T) {
	cs := &ContentScore{
		Category:     "gaming",
		CreatorID:    "user-42",
		EnergyLevel:  0.7,
		QualityScore: 0.8,
		CreatedAt:    time.Now().Add(-3 * time.Hour),
		ViewCount:    100,
		LikeCount:    20,
	}
	a := buildContentEmbedding(cs, []string{"happy", "competitive"})
	b := buildContentEmbedding(cs, []string{"happy", "competitive"})
	// Near-equal, not bit-equal: the recency slot is computed from
	// time.Now() and the vector is L2-normalized, so if the clock ticks
	// between the two builds EVERY component shifts by ~1e-12 (the first
	// -race CI run caught exactly that — the detector's slowdown makes
	// crossing a time boundary between calls likely). The property under
	// test is that the HASH-TRICK slots are deterministic, which a 1e-9
	// tolerance still verifies while tolerating the documented time
	// dependence of slots 2-3 (pgvector_ann zeroes them for the same
	// reason).
	const eps = 1e-9
	for i := range a {
		d := a[i] - b[i]
		if d < -eps || d > eps {
			t.Fatalf("same inputs must produce same vector; diff at %d (%v vs %v)", i, a[i], b[i])
		}
	}
}

func TestBuildContentEmbedding_NilSafe(t *testing.T) {
	v := buildContentEmbedding(nil, nil)
	if len(v) != embedDim {
		t.Fatalf("nil input should still return an embedDim-length vector, got %d", len(v))
	}
	for _, x := range v {
		if x != 0 {
			t.Fatalf("nil ContentScore should yield zero vector")
		}
	}
}

func TestUserEmbeddingIsCold(t *testing.T) {
	v := make([]float64, embedDim)
	if !userEmbeddingIsCold(v) {
		t.Fatal("all-zero vector should be cold")
	}
	v[5] = 0.1
	if userEmbeddingIsCold(v) {
		t.Fatal("vector with non-zero entry should not be cold")
	}
}

// TestGetOrBuildContentEmbedding_MatchesUncached pins the contract that the
// Redis-cached fast path produces a vector mathematically identical to a
// fresh build. Any future change to either path that breaks this equality
// would silently change scoring downstream — fail loudly here instead.
func TestGetOrBuildContentEmbedding_MatchesUncached(t *testing.T) {
	resetRedis(t)
	cs := &ContentScore{
		ContentID:    "cv-equiv-1",
		Category:     "gaming",
		CreatorID:    "user-99",
		EnergyLevel:  0.65,
		QualityScore: 0.72,
		CreatedAt:    time.Now().Add(-90 * time.Minute),
		ViewCount:    250,
		LikeCount:    40,
	}
	emotions := []string{"happy", "intense"}

	want := buildContentEmbedding(cs, emotions)

	// First call — miss path, builds + writes cache.
	got1 := getOrBuildContentEmbedding(cs, emotions)
	// Second call — hit path, reads from Redis.
	got2 := getOrBuildContentEmbedding(cs, emotions)

	// Tolerance accounts for the microsecond-scale drift of time.Since(CreatedAt)
	// between independent calls (recency feature uses wall clock). Anything
	// above 1e-6 would mean a real algorithmic divergence.
	const eps = 1e-6
	for i := range want {
		if abs(want[i]-got1[i]) > eps {
			t.Fatalf("miss-path differs from buildContentEmbedding at %d: %v vs %v", i, want[i], got1[i])
		}
		if abs(want[i]-got2[i]) > eps {
			t.Fatalf("hit-path differs from buildContentEmbedding at %d: %v vs %v", i, want[i], got2[i])
		}
	}
}

// TestGetOrBuildContentEmbedding_NoIDFallsThrough exercises the path where
// Redis caching can't apply (missing ContentID) — should still return a
// valid normalized vector via the un-cached build.
func TestGetOrBuildContentEmbedding_NoIDFallsThrough(t *testing.T) {
	resetRedis(t)
	cs := &ContentScore{Category: "music", CreatorID: "bob", EnergyLevel: 0.4, QualityScore: 0.5, CreatedAt: time.Now()}
	v := getOrBuildContentEmbedding(cs, []string{"chill"})
	if len(v) != embedDim {
		t.Fatalf("expected length %d, got %d", embedDim, len(v))
	}
	var sum float64
	for _, x := range v {
		sum += x * x
	}
	if sum < 0.999 || sum > 1.001 {
		t.Fatalf("expected unit-length vector, got sum-of-squares %v", sum)
	}
}

func abs(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}

func TestUpdateUserEmbedding_MovesToward(t *testing.T) {
	resetRedis(t)
	cs := &ContentScore{Category: "music", CreatorID: "alice", EnergyLevel: 0.5, QualityScore: 0.6, CreatedAt: time.Now()}
	cv := buildContentEmbedding(cs, []string{"chill"})
	updateUserEmbedding("uemb1", cv, 1.0)
	updateUserEmbedding("uemb1", cv, 1.0)
	updateUserEmbedding("uemb1", cv, 1.0)
	got := getUserEmbedding("uemb1")
	if userEmbeddingIsCold(got) {
		t.Fatal("after positive updates, user vector should not be cold")
	}
	// Cosine with the content vector should be positive and reasonably high.
	sim := cosineSim(got, cv)
	if sim < 0.2 {
		t.Fatalf("after repeated positive updates, cosine should be clearly positive, got %v", sim)
	}
}
