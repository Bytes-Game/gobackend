package main

// content_similarity.go — deciding what a video IS LIKE, instead of which box
// it goes in.
//
// ════════════════════════════════════════════════════════════════════════════
// WHY THIS REPLACED A LIST OF EIGHTEEN
// ════════════════════════════════════════════════════════════════════════════
//
// The whole "does this viewer want this video" question used to be one line:
// take the video's category, look it up in the viewer's affinity map. One word
// out of eighteen, against a taste profile eighteen wide.
//
// That fails in two directions at once, and the app's own catalogue shows both.
//
// TOO COARSE. A clip of a bee on a thistle, a butterfly on a flower, and a
// jellyfish in an aquarium are three nearly identical videos. None of them fits
// any of the eighteen, so the model correctly declines to pick one and they
// rank on nothing at all. Meanwhile their creators filed them under lifestyle,
// art and sports — three DIFFERENT categories — so the diversity check thinks
// the feed is varied while showing you the same thing three times.
//
// ALWAYS OUT OF DATE. Adding "nature" fixes those three and nothing else. Next
// month it is cooking, or cricket, or exam results, and the list is wrong
// again. A fixed vocabulary is permanently chasing the content.
//
// So the question changes from "which box" to "what is this like". Two videos
// that both say thistle, bee, pollination, nature are related whether or not
// anybody ever invented a nature category — and a viewer who watched one has
// told us something about the other, with no list in the middle.
//
// ════════════════════════════════════════════════════════════════════════════
// WHAT A VIDEO IS, HERE
// ════════════════════════════════════════════════════════════════════════════
//
// A set of words. Its topics — free-form, whatever the model saw or heard —
// plus its tags. Nothing else. Comparing two videos is comparing two sets.
//
// This file is deliberately the ONLY place that comparison is defined. Every
// consumer that used to read cs.Category — relevance, session fatigue,
// sequence variety, search, surprise, the avoided list — goes through these
// functions, so there is one answer to "are these two things alike" rather
// than six that drift apart.

import (
	"encoding/json"
	"math"
	"sort"
	"strings"
)

// ════════════════════════════════════════════════════════════════════════════
// A VIDEO'S FINGERPRINT
// ════════════════════════════════════════════════════════════════════════════

// contentFingerprint is everything known about what a video is about, as one
// set of words.
//
// Topics first, because they are the specific half — "thistle" says far more
// than "other" ever could. Tags follow: they are a closed vocabulary and so
// much coarser, but they are the only thing most of the catalogue has while
// the topics column fills in.
//
// The category is folded in as one more word rather than treated as special.
// It is a real signal — somebody chose it, or a model concluded it — but here
// it is exactly as important as any other word the video is described by,
// which is the entire change.
func contentFingerprint(topics, tags []string, category string) []string {
	out := make([]string, 0, len(topics)+len(tags)+1)
	seen := make(map[string]bool, len(topics)+len(tags)+1)
	add := func(words []string) {
		for _, w := range words {
			w = normalizeOneTag(w)
			if w == "" || w == "other" || w == "general" || seen[w] {
				continue
			}
			seen[w] = true
			out = append(out, w)
		}
	}
	add(topics)
	add(tags)
	add([]string{category})
	return out
}

// ════════════════════════════════════════════════════════════════════════════
// HOW ALIKE TWO VIDEOS ARE
// ════════════════════════════════════════════════════════════════════════════

// topicOverlap scores how much two videos have in common, from 0 to 1.
//
// Shared words over the smaller set, not over the union.
//
// The union — plain Jaccard — punishes a video for being well described. A
// video with six topics and one with two, sharing both of the smaller one's
// words, scores 2/6 under Jaccard: barely related, when in fact one video's
// entire subject is contained in the other's. Since the model writes anywhere
// between one and six topics depending on how much it could tell, Jaccard
// would make similarity depend mostly on how talkative the model felt.
//
// Over the smaller set, "everything this video is about is also true of that
// one" scores 1, which is what being alike means.
func topicOverlap(a, b []string) float64 {
	if len(a) == 0 || len(b) == 0 {
		return 0
	}
	set := make(map[string]bool, len(a))
	for _, w := range a {
		set[w] = true
	}
	shared := 0
	counted := make(map[string]bool, len(b))
	for _, w := range b {
		if counted[w] {
			continue
		}
		counted[w] = true
		if set[w] {
			shared++
		}
	}
	smaller := len(set)
	if n := len(counted); n < smaller {
		smaller = n
	}
	if smaller == 0 {
		return 0
	}
	return float64(shared) / float64(smaller)
}

// ════════════════════════════════════════════════════════════════════════════
// HOW MUCH A VIEWER WANTS IT
// ════════════════════════════════════════════════════════════════════════════

// topicRelevance is what replaced the category lookup: how well a video's
// fingerprint matches what this viewer has actually engaged with.
//
// Every word the viewer has a feeling about contributes that feeling. The
// result is the AVERAGE over the video's words rather than the sum, so a video
// described in six words is not automatically six times more relevant than one
// described in two — length of description is a fact about the model, not
// about the viewer's taste.
//
// Words the viewer has no history with contribute nothing, which is different
// from contributing zero: a video half of whose topics are new and half of
// which the viewer loves should read as "quite relevant", not "half relevant".
// So the average is over the words we have an opinion about, and the count of
// those words is returned so callers can tell a confident 0.8 from a 0.8 that
// rests on one word.
func topicRelevance(affinity map[string]float64, fingerprint []string) (score float64, matched int) {
	if len(affinity) == 0 || len(fingerprint) == 0 {
		return 0, 0
	}
	sum := 0.0
	for _, w := range fingerprint {
		if v, ok := affinity[w]; ok {
			sum += v
			matched++
		}
	}
	if matched == 0 {
		return 0, 0
	}
	return sum / float64(matched), matched
}

// topicConfidence scales a relevance score by how much it actually rests on.
//
// A viewer who has engaged with one of a video's six words has told us
// something, but much less than one who has engaged with five. Without this a
// single coincidental word — "nature" appearing on a video that is really
// about something else — would move a video as far up the feed as a genuine
// match on everything.
//
// Reaches full strength at three matched words. Below that it holds back
// rather than refusing: a brand-new platform has almost no history, and a
// signal that demands certainty before it says anything says nothing at all.
const topicConfidenceFull = 3.0

func topicConfidence(matched int) float64 {
	if matched <= 0 {
		return 0
	}
	c := float64(matched) / topicConfidenceFull
	return math.Min(1, c)
}

// ════════════════════════════════════════════════════════════════════════════
// WHAT THE VIEWER HAS HAD ENOUGH OF
// ════════════════════════════════════════════════════════════════════════════

// topicSaturation reports how heavily this session has already leaned on what
// this video is about, from 0 (nothing like it yet) to 1 (nothing but).
//
// This is the fix for the failure at the top of the file. Category fatigue
// counted "how many comedy videos", so three nature clips filed by their
// creators under lifestyle, art and sports read as three different things and
// the feed happily showed all three in a row. Counting shared WORDS sees them
// for what they are.
func topicSaturation(seen map[string]int, fingerprint []string) float64 {
	if len(seen) == 0 || len(fingerprint) == 0 {
		return 0
	}
	// The most-repeated word in this video, rather than the total: a video
	// sharing one very tired word with the session is repetitive, and summing
	// would let a well-described video hide that behind its other words.
	worst := 0
	for _, w := range fingerprint {
		if n := seen[w]; n > worst {
			worst = n
		}
	}
	if worst <= topicSaturationFree {
		return 0
	}
	over := float64(worst - topicSaturationFree)
	return math.Min(1, over/topicSaturationSpan)
}

const (
	// topicSaturationFree is how many times a word can appear in a session
	// before it counts as repetition. Two videos about cricket is a taste;
	// the third in a row is a rut.
	topicSaturationFree = 2
	// topicSaturationSpan is how many further repeats reach full saturation.
	topicSaturationSpan = 3.0
)

// rememberTopics records a shown video's words against the session.
func rememberTopics(seen map[string]int, fingerprint []string) {
	if seen == nil {
		return
	}
	for _, w := range fingerprint {
		seen[w]++
	}
}

// ════════════════════════════════════════════════════════════════════════════
// KEEPING THE PROFILE A SENSIBLE SIZE
// ════════════════════════════════════════════════════════════════════════════

// maxTopicAffinity bounds how many words one viewer's taste is remembered in.
//
// The vocabulary is open by design — that is the whole point, no list to keep
// up to date — but a profile is loaded and JSON-decoded on every feed request,
// so it cannot grow without limit. Keeping the strongest opinions and dropping
// the faintest loses almost nothing: a word the viewer felt 0.02 about is not
// what makes their feed good.
const maxTopicAffinity = 300

// trimTopicAffinity keeps the strongest opinions, positive and negative alike.
//
// Ranked by how FAR FROM ZERO a feeling is, not by how positive. A strong
// dislike is as much a part of somebody's taste as a strong like, and dropping
// the negatives first would quietly delete the "stop showing me this" signal
// while appearing to keep the profile healthy.
func trimTopicAffinity(m map[string]float64) map[string]float64 {
	if len(m) <= maxTopicAffinity {
		return m
	}
	type kv struct {
		k string
		v float64
	}
	all := make([]kv, 0, len(m))
	for k, v := range m {
		all = append(all, kv{k, v})
	}
	sort.Slice(all, func(i, j int) bool {
		ai, aj := math.Abs(all[i].v), math.Abs(all[j].v)
		if ai != aj {
			return ai > aj
		}
		return all[i].k < all[j].k // stable, so a rebuild does not churn
	})
	out := make(map[string]float64, maxTopicAffinity)
	for i := 0; i < maxTopicAffinity; i++ {
		out[all[i].k] = all[i].v
	}
	return out
}

// topicMatchesQuery reports whether a search term hits a video's fingerprint.
//
// Substring rather than equality, because a person searching "jellyfish" should
// find a video whose topic is "jellyfish tank", and searching "food" should
// find "street food". The old search could only match the eighteen category
// names, so "jellyfish" matched nothing on a platform that has one.
func topicMatchesQuery(fingerprint []string, query string) bool {
	q := normalizeOneTag(query)
	if q == "" {
		return false
	}
	for _, w := range fingerprint {
		if w == q || strings.Contains(w, q) || strings.Contains(q, w) {
			return true
		}
	}
	return false
}

// ════════════════════════════════════════════════════════════════════════════
// TURNING BEHAVIOUR INTO TASTE
// ════════════════════════════════════════════════════════════════════════════

// engagementWeight is what one thing a viewer did is worth.
//
// Pulled out of the profile builder so the category tally and the topic tally
// are driven by ONE set of numbers. They used to be one switch statement
// writing into one map; the moment a second map appeared, two copies of these
// weights would have drifted the first time anybody tuned them, and the two
// halves of the same profile would then disagree about how much a share means.
//
// Negative on purpose where the viewer pushed back. A fast scroll is not the
// absence of interest, it is a small statement against.
func engagementWeight(evType string, completion float64) float64 {
	switch evType {
	case "share":
		return 3.0
	case "rewatch":
		return 2.5
	case "save":
		return 1.5
	case "comment":
		return 1.2 // more effort than a like
	case "like":
		return 1.0
	case "view":
		// Granular watch time — more honest than a binary threshold.
		switch {
		case completion >= 0.9:
			return 1.0 // nearly finished
		case completion >= 0.7:
			return 0.5 // watched most
		case completion >= 0.5:
			return 0.1 // watched half
		case completion >= 0.3:
			return -0.1 // gave it a chance and left
		}
		return 0 // under 30% says nothing either way
	case "pause":
		return 0.3
	case "scroll_slow":
		return 0.2
	case "scroll_fast":
		return -0.3
	case "skip":
		return -0.5
	case "not_interested":
		return -2.0
	}
	return 0
}

// normalizeTopicAffinity folds raw evidence into taste in [-1, 1].
//
// Divided by the largest feeling in EITHER direction, and the sign is kept.
//
// That last part is the difference from how categories are normalised. Those
// are clamped to zero at the bottom, which throws every dislike away — so a
// separate pass has to remember mined negatives and paste them back
// afterwards, and a single weak positive event can wipe a sustained dislike.
//
// Dividing by the largest magnitude keeps both halves in one number from the
// start. A viewer who skipped every prank video ends up around -1 for "prank"
// and stays there until they actually watch one.
func normalizeTopicAffinity(raw map[string]float64) map[string]float64 {
	if len(raw) == 0 {
		return map[string]float64{}
	}
	maxAbs := 0.0
	for _, v := range raw {
		if a := math.Abs(v); a > maxAbs {
			maxAbs = a
		}
	}
	if maxAbs == 0 {
		return map[string]float64{}
	}
	out := make(map[string]float64, len(raw))
	for k, v := range raw {
		n := v / maxAbs
		if n > 1 {
			n = 1
		} else if n < -1 {
			n = -1
		}
		// A feeling too faint to matter is noise that costs profile space and
		// slows every feed request. Dropping it loses nothing.
		if math.Abs(n) < topicAffinityFloor {
			continue
		}
		out[k] = n
	}
	return trimTopicAffinity(out)
}

// topicAffinityFloor is how faint a feeling has to be before it is forgotten.
const topicAffinityFloor = 0.02

// avoidedTopicThreshold is how strongly a viewer has to have pushed back
// before a subject counts as one they actively do not want.
//
// Matched to the category equivalent's spirit: one tap is not enough. At -0.5
// after normalising, a subject has drawn sustained rejection rather than a
// single bad video, so acting on it will not silence a whole subject because
// one clip about it was poor.
const avoidedTopicThreshold = -0.5

// avoidedTopics lists the subjects a viewer has pushed back on hard enough
// that the feed should stop offering them.
//
// Sorted, so a profile rebuild that found the same things does not look like a
// change and churn the stored row.
func avoidedTopics(affinity map[string]float64) []string {
	var out []string
	for k, v := range affinity {
		if v <= avoidedTopicThreshold {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out
}

// jsonStrings decodes a JSONB text column into a string slice.
//
// Never errors: a column that is NULL, empty, or holding something unexpected
// means "nothing known about this video", which is an ordinary state on a
// platform where most of the catalogue has not been analysed. Failing here
// would take down a profile rebuild over a missing description.
func jsonStrings(raw string) []string {
	if raw == "" || raw == "[]" || raw == "null" {
		return nil
	}
	var out []string
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		return nil
	}
	return out
}

// avoidedTopicPenalty is what a video about a rejected subject scores.
//
// The same floor the avoided-category rule uses. Not lower: somebody who
// rejected one subject has not rejected the video entirely, and a penalty
// deep enough to bury it outright makes a single bad session permanent.
const avoidedTopicPenalty = -0.3
