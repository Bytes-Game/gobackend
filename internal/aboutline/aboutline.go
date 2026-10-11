// Package aboutline tidies the "What is this video about?" sentence so it
// says what the video is about, and nothing about how sure the model is.
//
// The model hedges. Asked what a clip is about, it answers "It looks like a
// man is cooking biryani" or "This video appears to show a dance". The app
// already says, under every answer, that AI wrote it and it can be wrong — so
// the hedge says nothing, and the owner asked for it to go: "only show what
// the video is about, do not write looks like".
//
// Both halves use this. The worker tidies each new answer before it is
// stored, and the server tidies every answer it hands out, so the ones
// written before this existed are fixed too, without re-running anything.
package aboutline

import (
	"regexp"
	"strings"
	"unicode"
	"unicode/utf8"
)

type rule struct {
	re   *regexp.Regexp
	with string
}

// openers are hedges at the start of a sentence. They go entirely, or turn
// into the plain verb ("appears to show" → "shows").
var openers = []rule{
	{regexp.MustCompile(`(?i)^(?:it|this|that)\s+(?:looks|seems|sounds)\s+like\s+(?:a\s+(?:video|clip|reel)\s+(?:of|about|where|in which|showing)\s+)?`), ""},
	{regexp.MustCompile(`(?i)^(?:it|this)\s+(?:appears|seems)\s+(?:that|as if|as though)\s+`), ""},
	{regexp.MustCompile(`(?i)^(?:it|this)\s+(?:appears|seems)\s+to\s+be\s+(?:a\s+(?:video|clip|reel)\s+(?:of|about|where|in which|showing)\s+)?`), ""},
	{regexp.MustCompile(`(?i)^(?:looks|seems|sounds)\s+like\s+(?:a\s+(?:video|clip|reel)\s+(?:of|about|where|in which|showing)\s+)?`), ""},
	{regexp.MustCompile(`(?i)^(?:apparently|possibly|probably|perhaps|maybe|likely),?\s+`), ""},
	{regexp.MustCompile(`(?i)^(?:i\s+think|i\s+believe|i\s+guess)\s+(?:that\s+)?`), ""},
}

// within are hedges anywhere in a sentence, replaced by the plain words.
var within = []rule{
	{regexp.MustCompile(`(?i)\b(?:appears|seems)\s+to\s+be\b`), "is"},
	{regexp.MustCompile(`(?i)\b(?:appear|seem)\s+to\s+be\b`), "are"},
	{regexp.MustCompile(`(?i)\b(?:appears|seems)\s+to\s+(show|feature|capture|depict|include|contain|have|explain|describe|demonstrate)\b`), "${1}s"},
	{regexp.MustCompile(`(?i)\b(?:appear|seem)\s+to\s+(show|feature|capture|depict|include|contain|have|explain|describe|demonstrate)\b`), "${1}"},
	{regexp.MustCompile(`(?i)\bwhat\s+(?:looks|seems|appears)\s+(?:like|to\s+be)\s+`), ""},
	{regexp.MustCompile(`(?i)\b(is|are|was|were)\s+(?:possibly|probably|likely|apparently|presumably)\s+`), "${1} "},
	{regexp.MustCompile(`(?i)\b(?:possibly|probably|apparently|presumably)\s+`), ""},
}

// Plain is s with the hedging taken out, one sentence at a time, each
// starting with a capital. Anything it cannot improve it leaves alone.
func Plain(s string) string {
	s = strings.Join(strings.Fields(s), " ")
	if s == "" {
		return ""
	}
	parts := sentences(s)
	for i, p := range parts {
		for _, r := range openers {
			if loc := r.re.FindStringIndex(p); loc != nil {
				p = r.with + p[loc[1]:]
				break
			}
		}
		for _, r := range within {
			p = r.re.ReplaceAllString(p, r.with)
		}
		parts[i] = capitalise(strings.TrimSpace(p))
	}
	out := strings.TrimSpace(strings.Join(parts, " "))
	return strings.Join(strings.Fields(out), " ")
}

// sentences splits after ".", "!" or "?" followed by a space.
func sentences(s string) []string {
	var out []string
	start := 0
	for i := 0; i < len(s)-1; i++ {
		if (s[i] == '.' || s[i] == '!' || s[i] == '?') && s[i+1] == ' ' {
			out = append(out, s[start:i+1])
			start = i + 2
		}
	}
	return append(out, s[start:])
}

func capitalise(s string) string {
	r, n := utf8.DecodeRuneInString(s)
	if r == utf8.RuneError || !unicode.IsLower(r) {
		return s
	}
	return string(unicode.ToUpper(r)) + s[n:]
}
