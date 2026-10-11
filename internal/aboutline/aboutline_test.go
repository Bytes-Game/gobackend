package aboutline

import "testing"

func TestPlain_SaysWhatTheVideoIsAbout(t *testing.T) {
	for _, c := range []struct{ in, want string }{
		{"It looks like a man is cooking biryani in a home kitchen.",
			"A man is cooking biryani in a home kitchen."},
		{"Looks like a prank on a friend.", "A prank on a friend."},
		{"This looks like a video of two friends dancing at a wedding.",
			"Two friends dancing at a wedding."},
		{"This video appears to show a street food stall in Delhi.",
			"This video shows a street food stall in Delhi."},
		{"It seems that two brothers are arguing over the TV remote.",
			"Two brothers are arguing over the TV remote."},
		{"It appears to be a tutorial on tying a tie.", "A tutorial on tying a tie."},
		{"A woman appears to be singing a devotional song. The crowd seems to be clapping along.",
			"A woman is singing a devotional song. The crowd is clapping along."},
		{"The dancers seem to be practising for a show.", "The dancers are practising for a show."},
		{"A boy holding what looks like a cricket bat.", "A boy holding a cricket bat."},
		{"Probably a cat video. It is likely filmed at home.",
			"A cat video. It is filmed at home."},
		// Already plain: untouched.
		{"A chef plates a dessert with chocolate sauce.",
			"A chef plates a dessert with chocolate sauce."},
		{"", ""},
	} {
		if got := Plain(c.in); got != c.want {
			t.Errorf("Plain(%q)\n  got  %q\n  want %q", c.in, got, c.want)
		}
	}
}

func TestPlain_LeavesWordsThatOnlyLookAlike(t *testing.T) {
	// "like" and "looks" in their ordinary senses are not hedges.
	for _, s := range []string{
		"A girl who looks like her mother sings with her.",
		"People like this song and dance to it.",
	} {
		if got := Plain(s); got != s {
			t.Errorf("Plain(%q) = %q, want it unchanged", s, got)
		}
	}
}
