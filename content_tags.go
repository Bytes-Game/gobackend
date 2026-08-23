package main

// content_tags.go — what a video is ABOUT, said by the person who made it.
//
// ════════════════════════════════════════════════════════════════════════════
// WHAT DECIDED THIS BEFORE
// ════════════════════════════════════════════════════════════════════════════
//
// One thing: keyword matching on the challenge's subject line.
//
//	"funny" in the text  → comedy
//	"gym" in the text    → fitness
//	nothing matched      → general
//
// Everything downstream is built on that answer. The category goes into the
// user's taste profile, into the relevance term of the score, into the
// hour-of-day routing, and into the content embedding that decides which
// videos count as similar to which. A short subject line with no keyword in
// it — which is most of them — gives the whole chain nothing to work with.
//
// A custom_tags column has existed on challenges the whole time. Nothing
// wrote it and nothing read it. This file is what makes it real.
//
// ════════════════════════════════════════════════════════════════════════════
// WHAT THE BIG FEEDS ACTUALLY DO, AND WHAT IS WORTH COPYING
// ════════════════════════════════════════════════════════════════════════════
//
// Three layers, in ascending order of how much they matter:
//
//	1. WHAT THE CREATOR SAYS — hashtags, caption, the sound they used. Cheap,
//	   immediate, available the second a video is posted, and the only signal
//	   that exists before anybody has watched it. This is the layer this file
//	   adds.
//
//	2. WHAT A MACHINE SEES — objects and scenes in the frames, speech turned
//	   into text, words read off the screen, the music identified by
//	   fingerprint. Expensive, and deliberately NOT attempted here: it needs
//	   models and a transcode pipeline this app does not have, and it would
//	   buy little that layer 3 does not already provide.
//
//	3. WHAT PEOPLE DO — who watches a video, and what else those same people
//	   watch. This is the layer that actually carries the weight at scale, and
//	   the reason the big feeds barely rely on categories at all: a video ends
//	   up sitting next to the videos its audience also liked, whatever anybody
//	   labelled it. This app already has it, in cooccurrence.go, the
//	   collaborative source, and the user/content embeddings.
//
// So layer 3 is the engine and it is already running. What was missing was
// layer 1 — and layer 1 matters most in exactly the situation this app is in
// now: a new video with no watch history, where behaviour has nothing to say
// yet and a label is all there is.
//
// ════════════════════════════════════════════════════════════════════════════
// HOW A TAG IS USED
// ════════════════════════════════════════════════════════════════════════════
//
//	as the CATEGORY   — a tag naming a real category beats the keyword guess,
//	                    because the creator knows and the guesser is guessing.
//	in the EMBEDDING  — two videos sharing a tag sit closer together, so a
//	                    viewer who finished one is offered the other even when
//	                    the two are in different categories.
//
// Tags are never a hard filter. Nothing is ever shown or hidden because of a
// tag; they only move a video's position, which keeps a mis-tagged video from
// disappearing and keeps a keyword-stuffed one from taking over.

import "strings"

const (
	// maxTagsPerItem caps how many tags one video may carry.
	//
	// Ten is generous for describing a video and mean enough to stop the
	// obvious abuse — a creator pasting fifty popular tags to appear in
	// everything. Extra tags are dropped rather than the upload rejected: a
	// creator who over-tags should still get their video posted.
	maxTagsPerItem = 10

	// maxTagLength bounds a single tag. Long enough for a real phrase,
	// short enough that nobody stores a sentence in one.
	maxTagLength = 30
)

// normalizeTags cleans what the client sent into what gets stored.
//
// Every tag is lowercased, trimmed, stripped of a leading '#', and reduced to
// letters, digits and single inner spaces. Duplicates are dropped, keeping
// first appearance, so ordering stays the creator's.
//
// Doing this ONCE at write time rather than at every read is what makes tags
// comparable at all: "#Comedy", "comedy " and "COMEDY" have to become one
// thing, or two videos about the same subject never look related and the
// whole feature is decoration.
func normalizeTags(raw []string) []string {
	if len(raw) == 0 {
		return nil
	}
	seen := make(map[string]bool, len(raw))
	out := make([]string, 0, len(raw))
	for _, t := range raw {
		t = normalizeOneTag(t)
		if t == "" || seen[t] {
			continue
		}
		seen[t] = true
		out = append(out, t)
		if len(out) >= maxTagsPerItem {
			break
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// normalizeOneTag reduces a single tag to its comparable form, or "" if
// nothing usable is left.
func normalizeOneTag(t string) string {
	t = strings.ToLower(strings.TrimSpace(t))
	t = strings.TrimLeft(t, "#")

	var b strings.Builder
	lastWasSpace := true // leading spaces are dropped
	for _, r := range t {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			b.WriteRune(r)
			lastWasSpace = false
		case r == ' ' || r == '-' || r == '_':
			// Collapse separators to a single space so "hip-hop", "hip_hop"
			// and "hip hop" are one tag rather than three.
			if !lastWasSpace {
				b.WriteRune(' ')
				lastWasSpace = true
			}
		default:
			// Anything else — punctuation, emoji, scripts this cannot fold —
			// is dropped rather than kept, so a tag can never carry something
			// that only compares equal to itself.
		}
		if b.Len() >= maxTagLength {
			break
		}
	}
	return strings.TrimSpace(b.String())
}

// tagCategoryAliases maps a few obvious words onto the category they mean, so
// a creator does not have to know the app's internal vocabulary to land in the
// right place.
//
// Deliberately short. This is a convenience for the handful of words people
// reliably reach for, not an attempt at a synonym dictionary — a long list
// here would be a second, worse copy of the keyword matcher this is meant to
// improve on.
var tagCategoryAliases = map[string]string{
	"funny":    "comedy",
	"humor":    "comedy",
	"meme":     "comedy",
	"gym":      "sports",
	"fitness":  "sports",
	"workout":  "sports",
	"football": "sports",
	"cricket":  "sports",
	"singing":  "music",
	"rap":      "music",
	"song":     "music",
	"dancing":  "dance",
	"cooking":  "food",
	"recipe":   "food",
	"tutorial": "education",
	"howto":    "education",
	"how to":   "education",
	"learn":    "education",
	"coding":   "tech",
	"gadget":   "tech",
	"vlog":     "story",
	"storytime": "story",
	"makeup":   "fashion",
	"outfit":   "fashion",
	"style":    "fashion",
	"scary":    "horror",
	"creepy":   "horror",
}

// categoryFromTags returns the category a creator's tags name, or "" if none
// of them names one.
//
// First match wins, in the creator's own order, so the tag they led with is
// the one that decides. Exact category names are checked before aliases: if
// somebody tags both "sports" and "funny", they meant sports.
func categoryFromTags(tags []string) string {
	if len(tags) == 0 {
		return ""
	}
	known := make(map[string]bool, len(ContentCategories))
	for _, c := range ContentCategories {
		known[c] = true
	}
	for _, t := range tags {
		if known[t] && t != "other" {
			return t
		}
	}
	for _, t := range tags {
		if c, ok := tagCategoryAliases[t]; ok {
			return c
		}
	}
	return ""
}

// categoryForContent decides a video's category from everything known about
// it, in order of how much the source can be trusted.
//
//	1. what the creator explicitly chose as the category
//	2. what the creator's tags say
//	3. keyword matching on the subject line — the guess
//
// The creator outranks the guesser, which is the whole point: a keyword
// matcher reading a five-word subject line is the weakest signal available
// and it used to be the only one.
func categoryForContent(explicit string, tags []string, subject, prefix, caption string) string {
	if explicit != "" && explicit != "other" && explicit != "general" {
		return explicit
	}
	if c := categoryFromTags(tags); c != "" {
		return c
	}
	return inferCategory(subject, prefix, caption)
}
