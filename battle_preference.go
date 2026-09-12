package main

// ════════════════════════════════════════════════════════════════════════════
// HOW MUCH MORE A BATTLE IS WORTH THAN A SHORT
// ════════════════════════════════════════════════════════════════════════════
//
// One number, one rule, one place.
//
// There used to be three, and they did not agree with each other:
//
//	the For You feed   +0.30 to +0.50, and -0.10 for being a short
//	explore            +0.15 to +0.20, and -0.05 for being a short
//	search             +0.05
//
// None of them said what they meant in terms anybody could check. The search
// one worked out, when measured, to roughly a HUNDRED TIMES audience
// advantage: a battle with four views beat a short with four hundred. Nothing
// in the code said that, and nobody would have guessed it from "+0.05".
//
// So the preference is now stated the way it is actually meant:
//
//	A battle counts as if it had three times the engagement of the same
//	video posted as a short.
//
// That is checkable. A battle beats a short doing up to three times as well,
// and loses to one doing better than that. It means the same thing in the
// feed, in explore and in search, because each of them multiplies its own
// engagement figure by it rather than adding some number of its own choosing.

// battlePreference is how many times a battle is worth a comparable short.
//
// A battle is a contest somebody answered. It is the thing this app is for, and
// shorts structurally outnumber battles — every challenge starts as a short
// before anyone responds — so without a thumb on the scale the feed fills with
// shorts. Three is that thumb, chosen as a product decision rather than
// measured from anything, which is why it lives here on its own and is one
// line to change.
const battlePreference = 3.0

// battleEngagementMultiplier is what to multiply a video's engagement by
// before scoring it.
//
// Takes the response count rather than a bool so the rule is impossible to
// apply to the wrong thing: a challenge with no responses IS a short, and
// there is no separate flag to get out of step with reality.
func battleEngagementMultiplier(responseCount int) float64 {
	if responseCount > 0 {
		return battlePreference
	}
	return 1
}
