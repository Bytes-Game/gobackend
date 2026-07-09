package main

import "testing"

func TestCoOccurrenceWritePath(t *testing.T) {
	resetRedis(t)

	// User engages A, then B, then C — B pairs with A; C pairs with B and A.
	recordCoOccurrence("u1", "challenge", "A")
	recordCoOccurrence("u1", "challenge", "B")
	recordCoOccurrence("u1", "challenge", "C")

	// lasteng holds newest-first, capped.
	list, _ := rdb.LRange(rctx, coocLastEngagedKeyPrefix+"u1", 0, -1).Result()
	if len(list) != 3 || list[0] != "challenge:C" || list[2] != "challenge:A" {
		t.Fatalf("lasteng order wrong: %v", list)
	}

	// Symmetric edges: A↔B, A↔C, B↔C each have score 1.
	for _, pair := range [][2]string{{"challenge:A", "challenge:B"}, {"challenge:B", "challenge:C"}, {"challenge:A", "challenge:C"}} {
		s1, _ := rdb.ZScore(rctx, coocKeyPrefix+pair[0], pair[1]).Result()
		s2, _ := rdb.ZScore(rctx, coocKeyPrefix+pair[1], pair[0]).Result()
		if s1 != 1 || s2 != 1 {
			t.Fatalf("edge %v not symmetric-1: %v / %v", pair, s1, s2)
		}
	}

	// A second user engaging B then C strengthens ONLY that edge.
	recordCoOccurrence("u2", "challenge", "B")
	recordCoOccurrence("u2", "challenge", "C")
	s, _ := rdb.ZScore(rctx, coocKeyPrefix+"challenge:B", "challenge:C").Result()
	if s != 2 {
		t.Fatalf("B↔C after second user = %v, want 2", s)
	}
	s, _ = rdb.ZScore(rctx, coocKeyPrefix+"challenge:A", "challenge:B").Result()
	if s != 1 {
		t.Fatalf("A↔B must stay 1, got %v", s)
	}

	// Self-pairing is never recorded.
	recordCoOccurrence("u1", "challenge", "C")
	if s, err := rdb.ZScore(rctx, coocKeyPrefix+"challenge:C", "challenge:C").Result(); err == nil && s != 0 {
		t.Fatalf("self-edge must not exist, got %v", s)
	}
}
