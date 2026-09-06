package main

import "testing"

// ════════════════════════════════════════════════════════════════════════════
// A POPULAR ACCOUNT IS NOT A SEARCH RESULT FOR EVERY WORD
// ════════════════════════════════════════════════════════════════════════════
//
// Found by running the live search, not by reading the code:
//
//	/search?q=jellyfish   accounts: cyberking, stormchaser, shadowstrike...
//	/search?q=cricket     accounts: cyberking, stormchaser, shadowstrike...
//	/search?q=zzzqqqxyw   accounts: cyberking, stormchaser, shadowstrike...
//
// The same ten people, in the same order, for anything typed — including a
// word sharing no letters with any of them.
//
// The cause was two lines at the end of the scorer. Follower count and win
// rate were added whether or not a single character matched, and the caller
// keeps anything scoring above zero. So popularity WAS the match.

func TestAccounts_PopularityIsNotAMatch(t *testing.T) {
	popular := User{
		Username: "cyberking", FullName: "Cyber King", League: "gold",
		Followers: 500, Wins: 60, Losses: 20,
	}
	for _, q := range []string{"zzzqqqxyw", "jellyfish", "biryani"} {
		if got := calculateScore(popular, q); got > 0 {
			t.Errorf("a popular account scored %.2f for %q, which shares "+
				"nothing with their name. The caller keeps anything above "+
				"zero, so this account is a result for every search anybody "+
				"ever runs.", got, q)
		}
	}
}

func TestAccounts_RealMatchesStillWin(t *testing.T) {
	// The fix must not break finding people. Popularity still orders those
	// who genuinely matched.
	popular := User{Username: "cyberking", FullName: "Cyber King", Followers: 500}
	for _, c := range []struct {
		query string
		why   string
	}{
		{"cyberking", "an exact username"},
		{"cyber", "a username prefix"},
		{"cyberkin", "a username with one letter missing"},
	} {
		if got := calculateScore(popular, c.query); got <= 0 {
			t.Errorf("%s (%q) scored %.2f — searching for somebody by name "+
				"has to find them", c.why, c.query, got)
		}
	}
}

func TestAccounts_AnUnknownPersonBeatsAPopularStranger(t *testing.T) {
	// The ordering that matters. Somebody searching a username wants THAT
	// person, even if they have no followers and somebody famous is nearby.
	wanted := User{Username: "jellyfan"} // no followers, no games
	famous := User{Username: "cyberking", Followers: 100000, Wins: 900, Losses: 100}

	w := calculateScore(wanted, "jellyfan")
	f := calculateScore(famous, "jellyfan")
	if w <= f {
		t.Errorf("the person actually searched for scored %.2f and a famous "+
			"stranger scored %.2f. A search for a name must return that name.", w, f)
	}
	if f != 0 {
		t.Errorf("the famous stranger scored %.2f rather than not matching", f)
	}
}

func TestAccounts_PopularityStillBreaksTies(t *testing.T) {
	// It is a real signal in its right place: between two people who match
	// equally well, the better-known one first is the more useful order.
	quiet := User{Username: "cyberking"}
	loud := User{Username: "cyberking", Followers: 5000, Wins: 90, Losses: 10}
	if calculateScore(loud, "cyberking") <= calculateScore(quiet, "cyberking") {
		t.Error("popularity no longer orders equally good matches, so search " +
			"results are arbitrary among people with the same name")
	}
}

func TestAccounts_PopularityCannotOutrankAName(t *testing.T) {
	// The bonus has to stay small next to a name match. A hundred thousand
	// followers is 1000 points at 0.01 each — which would drown the 100 an
	// exact username is worth, and searching a name would return the platform's
	// biggest account instead.
	huge := User{Username: "someoneelse", FullName: "Someone Else", Followers: 100000}
	exact := User{Username: "jellyfan", FullName: "Jelly Fan"}
	if calculateScore(huge, "jellyfan") >= calculateScore(exact, "jellyfan") {
		t.Errorf("an account with 100k followers outranks the person whose "+
			"username was typed: %.2f vs %.2f",
			calculateScore(huge, "jellyfan"), calculateScore(exact, "jellyfan"))
	}
}

func TestAccounts_EmptyQueryMatchesNobody(t *testing.T) {
	// A blank query used to score every user on popularity alone, which is the
	// same bug wearing different clothes.
	popular := User{Username: "cyberking", Followers: 500, Wins: 60, Losses: 20}
	for _, q := range []string{"", "   "} {
		if got := calculateScore(popular, q); got > 0 {
			t.Errorf("blank query %q scored %.2f", q, got)
		}
	}
}
