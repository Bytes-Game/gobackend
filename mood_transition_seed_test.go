package main

import "testing"

// TestMoodSeedValuesAreValidEmotionLabels guards the moodHealthyNext seed graph:
// every 'to' mood must be a real content emotion tag (a member of EmotionLabels),
// otherwise that seed transition can never match a candidate and the cold-start
// regulation prior is silently dead. (Audit #28.)
func TestMoodSeedValuesAreValidEmotionLabels(t *testing.T) {
	valid := make(map[string]bool, len(EmotionLabels))
	for _, e := range EmotionLabels {
		valid[e] = true
	}
	for from, tos := range moodHealthyNext {
		for _, to := range tos {
			if !valid[to] {
				t.Errorf("moodHealthyNext[%q] contains %q which is not a valid EmotionLabel", from, to)
			}
		}
	}
}
