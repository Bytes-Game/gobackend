package main

import (
	"os"
	"strings"
	"testing"
)

// ════════════════════════════════════════════════════════════════════════════
// WHO DECIDES WHAT A VIDEO IS
// ════════════════════════════════════════════════════════════════════════════
//
// The creator's word used to win outright. It no longer does: the worker reads
// what is said in any language, reads what is written on screen, and looks at
// the frames, and set against that the creator's category is an unverified
// claim nothing ever checked.
//
// Not because people lie. Mostly they do not know — a first upload, or a
// child, picks whatever sounds closest, and everything downstream then treats
// that guess as fact.
//
// These tests exist because the change is easy to get catastrophically wrong
// in one specific way, which the first test below is about.

// TestCategory_TheMachineBeingSilentNeverErasesTheCreator is the one that
// matters most in this file.
//
// The model answers "other" whenever it cannot tell, and "other" is never
// stored as a tag — so its category comes back EMPTY for most of the
// catalogue. If "machine preferred" were read as "machine always wins", every
// one of those videos would lose its category the moment this shipped.
//
// That is the single most damaging thing this code could do, it would look
// like a feed-wide collapse rather than like a categorisation bug, and nothing
// else in the repo would catch it.
func TestCategory_TheMachineBeingSilentNeverErasesTheCreator(t *testing.T) {
	// The model ran and could not tell: shape tags only, no category among
	// them. Exactly what most of the catalogue looks like today.
	machineSaidNothing := []string{"talking", "fast cuts"}

	got := categoryFromEvidence(machineSaidNothing, nil, "food", "cooking", "biryani", "")
	if got.Category != "food" {
		t.Fatalf("got %q, want the creator's %q kept.\n\nThe model had no "+
			"opinion, and an absent opinion must never outrank a real one — "+
			"otherwise every video the model cannot read loses its category "+
			"at once.", got.Category, "food")
	}
	if got.Source != "creator" {
		t.Errorf("source is %q, want creator", got.Source)
	}
	if got.Disputed() {
		t.Error("reported as disputed, but only one side said anything")
	}
}

func TestCategory_TheMachineWinsWhenItHasAnOpinion(t *testing.T) {
	// The change itself. One side watched the video; the other typed a word
	// at upload time and was never checked.
	got := categoryFromEvidence(
		[]string{"horror", "scary"}, // the model read and watched it
		nil, "comedy",               // the creator said comedy
		"spooky night", "", "")
	if got.Category != "horror" {
		t.Errorf("got %q, want horror — the model examined the video and the "+
			"creator's word is an unverified claim", got.Category)
	}
	if got.Source != "machine" {
		t.Errorf("source is %q, want machine", got.Source)
	}
	if !got.Disputed() {
		t.Error("both sides had an opinion and they differ; this must be " +
			"recorded as disputed or the disagreement cannot be counted")
	}
	// Both sides are kept, not collapsed. Keeping only the winner would make
	// the disagreement rate unmeasurable, which is the whole point of
	// recording the source.
	if got.Machine != "horror" || got.Creator != "comedy" {
		t.Errorf("got machine=%q creator=%q; both must survive for the "+
			"comparison to be possible later", got.Machine, got.Creator)
	}
}

func TestCategory_AgreementIsRecordedAsItsOwnThing(t *testing.T) {
	// The strongest answer available: both looked, both said the same. Worth
	// distinguishing from "the machine overruled somebody", because the two
	// deserve different confidence downstream.
	got := categoryFromEvidence([]string{"food"}, nil, "food", "", "", "")
	if got.Source != "agreed" {
		t.Errorf("source is %q, want agreed", got.Source)
	}
	if got.Disputed() {
		t.Error("agreement reported as a dispute")
	}
}

func TestCategory_CreatorTagsDoNotWinByPosition(t *testing.T) {
	// The trap this replaced. cs.Tags is creator tags followed by machine
	// tags, and categoryFromTags takes the FIRST match — so passing the
	// merged list would hand the creator the win on ordering alone, which is
	// the old behaviour wearing a new name.
	//
	// Passing them separately is what makes the preference real.
	got := categoryFromEvidence(
		[]string{"horror"},          // machine
		[]string{"comedy", "funny"}, // creator's own tags
		"", "", "", "")
	if got.Category != "horror" {
		t.Errorf("got %q, want horror. The creator's tags won on position, "+
			"which means the two lists were merged before being weighed.",
			got.Category)
	}
	if !got.Disputed() {
		t.Error("a creator TAG naming a different category is a disagreement " +
			"just as much as an explicit category is")
	}
}

func TestCategory_NobodyKnowingStillFallsBackToTheGuess(t *testing.T) {
	// Neither side has an opinion. Keyword matching on the words is a weak
	// answer and always was, but it beats no answer at all.
	got := categoryFromEvidence(nil, nil, "", "my cooking recipe", "", "")
	if got.Source != "guess" {
		t.Errorf("source is %q, want guess", got.Source)
	}
	if got.Category == "" {
		t.Error("no category at all; the keyword fallback should still answer")
	}
	if got.Disputed() {
		t.Error("nobody said anything, so nothing can be disputed")
	}
}

func TestCategory_OtherAndGeneralAreNotClaims(t *testing.T) {
	// These are what the app stores when nobody chose, so neither is the
	// creator asserting anything. Treating them as a claim would make almost
	// every video look like it had a creator opinion to disagree with, and
	// the disagreement rate would be meaningless.
	for _, notAClaim := range []string{"", "other", "general"} {
		got := categoryFromEvidence([]string{"horror"}, nil, notAClaim, "", "", "")
		if got.Disputed() {
			t.Errorf("explicit=%q was treated as a real claim", notAClaim)
		}
		if got.Source != "machine" {
			t.Errorf("explicit=%q: source is %q, want machine", notAClaim, got.Source)
		}
	}
}

// ════════════════════════════════════════════════════════════════════════════
// AND WHAT A DISAGREEMENT COSTS
// ════════════════════════════════════════════════════════════════════════════

func TestCategory_DisputedTrustIsARealDamping(t *testing.T) {
	// Not zero: the model is not accurate enough to justify erasing a
	// category outright. Not one: half the time the video is not what its
	// category says, and the viewer asked for that category specifically.
	if disputedCategoryTrust <= 0 || disputedCategoryTrust >= 1 {
		t.Errorf("disputedCategoryTrust is %v; at 0 a disagreement erases the "+
			"category and at 1 it costs nothing, and neither is true",
			disputedCategoryTrust)
	}
}

func TestCategory_DampingNeverWeakensADislike(t *testing.T) {
	// The direction of the damping matters and is easy to get backwards.
	// Multiplying a NEGATIVE relevance by 0.6 makes it less negative — so a
	// viewer who actively avoids this category would be shown MORE of it
	// because we were unsure. When we do not know what a video is, not
	// showing it is the safe error.
	//
	// Source-level because the guard is one condition inside a scoring
	// function that needs a whole profile and session to call, and the
	// condition is the entire property.
	src, err := os.ReadFile("feed_engine.go")
	if err != nil {
		t.Fatalf("read feed_engine.go: %v", err)
	}
	if !strings.Contains(string(src), "if cs.CategoryDisputed && relevance > 0 {") {
		t.Error("the disputed damping is no longer restricted to POSITIVE " +
			"relevance. Applied to a negative it weakens an active dislike, " +
			"showing somebody more of what they avoid because we were unsure.")
	}
}

func TestCategory_TheDecisionIsMadeFromSeparateLists(t *testing.T) {
	// Guards the call site rather than the function: passing cs.Tags (already
	// merged) would compile, run, and silently restore the old behaviour.
	src, err := os.ReadFile("feed_engine.go")
	if err != nil {
		t.Fatalf("read feed_engine.go: %v", err)
	}
	s := string(src)
	if strings.Contains(s, "categoryFromEvidence(cs.Tags") ||
		strings.Contains(s, "categoryFromEvidence(\n\t\t\tcs.Tags") {
		t.Error("the merged tag list is being passed to categoryFromEvidence. " +
			"Creator tags come first in that list, so they win on position " +
			"and the machine preference does nothing.")
	}
	if !strings.Contains(s, "normalizeTags(rawAutoTags), normalizeTags(rawTags)") {
		t.Error("the machine and creator tags are no longer passed separately")
	}
}
