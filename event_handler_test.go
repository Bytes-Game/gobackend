package main

import "testing"

func TestIsHardBlockReason(t *testing.T) {
	cases := map[string]bool{
		"block":           true,
		"abuse":           true,
		"harassment":      true,
		"hate":            true,
		"threats":         true,
		"sexual_content":  true,
		"violence":        true,
		"spam":            false,
		"other":           false,
		"":                false,
		"BLOCK":           false, // case-sensitive on purpose
	}
	for reason, want := range cases {
		if got := isHardBlockReason(reason); got != want {
			t.Errorf("isHardBlockReason(%q)=%v, want %v", reason, got, want)
		}
	}
}
