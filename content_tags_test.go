package main

import "testing"

// Tags only work if two people describing the same thing produce the same
// stored word. Everything below is really one property tested from several
// angles: normalization has to be aggressive enough that "#Hip-Hop " and
// "hip hop" are one tag, because if they are not, nothing downstream — the
// category, the embedding, the similarity between two videos — does anything
// at all.

func TestNormalizeTags_FoldsWhatPeopleActuallyType(t *testing.T) {
	cases := map[string]string{
		"Comedy":     "comedy",
		"#comedy":    "comedy",
		"  comedy  ": "comedy",
		"###COMEDY":  "comedy",
		"hip-hop":    "hip hop",
		"hip_hop":    "hip hop",
		"Hip  Hop":   "hip hop",
		"day in life": "day in life",
	}
	for in, want := range cases {
		got := normalizeTags([]string{in})
		if len(got) != 1 || got[0] != want {
			t.Errorf("%q normalized to %v, want [%q]", in, got, want)
		}
	}
}

func TestNormalizeTags_DropsWhatCannotBeCompared(t *testing.T) {
	// Emoji, punctuation and scripts this cannot fold are removed rather than
	// stored. A tag that only ever compares equal to itself is worse than no
	// tag: it takes a slot and contributes nothing.
	for _, in := range []string{"", "   ", "#", "###", "!!!", "🔥", "***"} {
		if got := normalizeTags([]string{in}); got != nil {
			t.Errorf("%q survived normalization as %v", in, got)
		}
	}
	// …but a usable part inside noise is kept.
	if got := normalizeTags([]string{"🔥comedy🔥"}); len(got) != 1 || got[0] != "comedy" {
		t.Errorf("emoji around a real word gave %v, want [comedy]", got)
	}
}

func TestNormalizeTags_DedupesAfterFolding(t *testing.T) {
	// The case that matters: the duplicates are not duplicates until AFTER
	// folding, so deduping before it would let all three through.
	got := normalizeTags([]string{"Comedy", "#comedy", "COMEDY ", "dance"})
	if len(got) != 2 || got[0] != "comedy" || got[1] != "dance" {
		t.Errorf("got %v, want [comedy dance] — first appearance kept, order preserved", got)
	}
}

func TestNormalizeTags_CapsCountAndLength(t *testing.T) {
	many := make([]string, 40)
	for i := range many {
		many[i] = string(rune('a'+i%26)) + "tag"
	}
	if got := normalizeTags(many); len(got) > maxTagsPerItem {
		t.Errorf("kept %d tags, cap is %d — without a cap a creator pastes fifty "+
			"popular tags and appears in everything", len(got), maxTagsPerItem)
	}
	long := ""
	for i := 0; i < 200; i++ {
		long += "a"
	}
	if got := normalizeTags([]string{long}); len(got[0]) > maxTagLength {
		t.Errorf("kept a %d-character tag, cap is %d", len(got[0]), maxTagLength)
	}
}

func TestNormalizeTags_EmptyInputIsNil(t *testing.T) {
	if got := normalizeTags(nil); got != nil {
		t.Errorf("nil input gave %v", got)
	}
	if got := normalizeTags([]string{}); got != nil {
		t.Errorf("empty input gave %v", got)
	}
}

// ── Tags choosing the category ──────────────────────────────────────────────

func TestCategoryFromTags_ARealCategoryNameWins(t *testing.T) {
	if got := categoryFromTags([]string{"random", "sports", "funny"}); got != "sports" {
		t.Errorf("got %q, want sports — an exact category name beats an alias", got)
	}
}

func TestCategoryFromTags_AliasesCatchTheCommonWords(t *testing.T) {
	for tag, want := range map[string]string{
		"gym": "sports", "rap": "music", "cooking": "food", "tutorial": "education",
	} {
		if got := categoryFromTags([]string{tag}); got != want {
			t.Errorf("%q mapped to %q, want %q", tag, got, want)
		}
	}
}

func TestCategoryFromTags_NothingRecognisedIsNotAGuess(t *testing.T) {
	// Returning "" lets the caller fall through to the keyword matcher.
	// Returning a category here would be inventing one.
	if got := categoryFromTags([]string{"mydogsbirthday", "tuesday"}); got != "" {
		t.Errorf("unrecognised tags produced %q, want no answer", got)
	}
	if got := categoryFromTags(nil); got != "" {
		t.Errorf("no tags produced %q", got)
	}
}

func TestCategoryFromTags_OtherIsNotAnAnswer(t *testing.T) {
	// "other" is the absence of a category. Treating it as one would stop the
	// keyword matcher ever being reached.
	if got := categoryFromTags([]string{"other", "gym"}); got != "sports" {
		t.Errorf("got %q — a tag of 'other' must not block a real one", got)
	}
}

// ── The order of trust ──────────────────────────────────────────────────────

func TestCategoryForContent_TheCreatorOutranksTheGuesser(t *testing.T) {
	// A keyword matcher reading a five-word subject line is the weakest signal
	// available, and it used to be the only one. Anything the creator actually
	// said comes first.
	got := categoryForContent("music", []string{"gym"}, "funny prank", "can you", "")
	if got != "music" {
		t.Errorf("explicit category gave %q, want music", got)
	}
	got = categoryForContent("", []string{"gym"}, "funny prank", "can you", "")
	if got != "sports" {
		t.Errorf("tags gave %q, want sports — tags beat the keyword guess", got)
	}
	got = categoryForContent("", nil, "funny prank joke", "can you", "")
	if got != "comedy" {
		t.Errorf("with nothing from the creator, the keyword guess should still "+
			"run; got %q", got)
	}
}

func TestCategoryForContent_EmptyAndOtherFallThrough(t *testing.T) {
	// "other" and "general" are the schema's defaults, not choices. If they
	// blocked the fall-through, every video created without a category would
	// be stuck as "other" forever.
	for _, explicit := range []string{"", "other", "general"} {
		if got := categoryForContent(explicit, []string{"cooking"}, "", "", ""); got != "food" {
			t.Errorf("explicit=%q gave %q, want food", explicit, got)
		}
	}
}

func TestCategoryForContent_AlwaysAnswers(t *testing.T) {
	// Every video needs a category: it feeds the taste profile, the relevance
	// term, hour routing and the embedding. An empty one would silently drop
	// the item out of all four.
	if got := categoryForContent("", nil, "", "", ""); got == "" {
		t.Error("a video with no category, no tags and no text got no category")
	}
}

// ── Tags are a mood signal too ──────────────────────────────────────────────
//
// Moving tags out of the emotion field was right — "hip hop" is not a mood.
// But some tags ARE moods, and losing those would have quietly cost the
// ranker the mood matching it does against how a viewer says they feel.

func TestEmotionsFromTags_PicksOutTheRealMoods(t *testing.T) {
	got := emotionsFromTags([]string{"hip hop", "funny", "dance battle", "chill"})
	if len(got) != 2 {
		t.Fatalf("got %v, want the two that are real emotion labels", got)
	}
	if got[0] != "funny" || got[1] != "chill" {
		t.Errorf("got %v, want [funny chill] in the creator's own order", got)
	}
}

func TestEmotionsFromTags_SubjectTagsAreNotMoods(t *testing.T) {
	// The half that must NOT happen. If subject tags leaked into the emotion
	// vector, they would match no mood and dilute the ones that would — which
	// is exactly what was wrong before tags had their own field.
	if got := emotionsFromTags([]string{"hip hop", "gym", "pasta"}); len(got) != 0 {
		t.Errorf("subject tags produced emotions %v", got)
	}
	if got := emotionsFromTags(nil); got != nil {
		t.Errorf("no tags produced %v", got)
	}
}

func TestEmotionsForContent_ATaggedMoodSurvives(t *testing.T) {
	// The regression this exists to catch: a creator tags their video "funny"
	// and the feed still knows it is funny.
	got := emotionsForContent(nil, []string{"funny", "hip hop"}, "", "", "")
	if !containsString(got, "funny") {
		t.Errorf("got %v — a tag naming a mood must reach the emotion vector", got)
	}
}

func TestEmotionsForContent_ExplicitPicksComeFirstAndAllSourcesCombine(t *testing.T) {
	// A video can be several things at once. Stopping at the first source
	// would keep only one, so a funny nostalgic video would lose half of what
	// it is.
	got := emotionsForContent([]string{"nostalgic"}, []string{"funny"}, "", "", "")
	if len(got) == 0 || got[0] != "nostalgic" {
		t.Errorf("got %v — what the creator explicitly picked should lead", got)
	}
	if !containsString(got, "funny") {
		t.Errorf("got %v — the tagged mood was dropped once an explicit one existed", got)
	}
}

func TestEmotionsForContent_ReadsTheTagsAsTextToo(t *testing.T) {
	// "throwback" is not one of the sixteen emotion labels, but the keyword
	// table maps it to "nostalgic". Running the keyword pass over the tags as
	// well as the caption is what lets a word like that land somewhere
	// sensible instead of being thrown away for not being an exact match.
	got := emotionsForContent(nil, []string{"throwback"}, "", "", "")
	if !containsString(got, "nostalgic") {
		t.Errorf("a tag of throwback gave %v, want nostalgic among them — the "+
			"keyword pass is not reading the tags", got)
	}
}

func TestEmotionsForContent_NothingIsNil(t *testing.T) {
	if got := emotionsForContent(nil, nil, "", "", ""); got != nil {
		t.Errorf("nothing at all produced %v", got)
	}
}
