package main

import "testing"

func TestParseDeviceMaxLongSide(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		want int
	}{
		// Every client built before this parameter existed sends nothing, and
		// must keep getting an unfiltered feed.
		{"absent means no constraint", "", 0},
		{"garbage means no constraint", "abc", 0},
		{"zero means no constraint", "0", 0},
		{"negative means no constraint", "-500", 0},
		{"normal value passes through", "1280", 1280},
		{"large value passes through", "1920", 1920},
		// A client claiming it can only decode 100px would filter its own feed
		// to nothing. Far more likely a bug than a real device.
		{"absurdly small is floored", "100", minDeviceLongSide},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := parseDeviceMaxLongSide(c.raw); got != c.want {
				t.Errorf("parseDeviceMaxLongSide(%q) = %d, want %d", c.raw, got, c.want)
			}
		})
	}
}

func TestVariantLongSide(t *testing.T) {
	cases := map[string]int{
		"480p": 480, "720p": 720, "1080p": 1080,
		" 720p ": 720,
		"":       0,
		"source": 0,
		"abcp":   0,
	}
	for label, want := range cases {
		if got := variantLongSide(label); got != want {
			t.Errorf("variantLongSide(%q) = %d, want %d", label, got, want)
		}
	}
}

// The best rung that fits, not the smallest available — the goal is the best
// picture the phone can handle, not the most cautious one.
func TestBestVariantWithin_PicksLargestThatFits(t *testing.T) {
	v := VideoVariants{"480p": "u480", "720p": "u720", "1080p": "u1080"}

	cases := []struct {
		budget    int
		wantLabel string
	}{
		{1080, "1080p"},
		{1079, "720p"},
		{720, "720p"},
		{719, "480p"},
		{480, "480p"},
		{479, ""}, // nothing small enough
	}
	for _, c := range cases {
		label, url := bestVariantWithin(v, c.budget)
		if label != c.wantLabel {
			t.Errorf("budget %d: label = %q, want %q", c.budget, label, c.wantLabel)
		}
		if c.wantLabel != "" && url != v[c.wantLabel] {
			t.Errorf("budget %d: url = %q, want %q", c.budget, url, v[c.wantLabel])
		}
	}
}

func TestBestVariantWithin_EmptyCases(t *testing.T) {
	if l, _ := bestVariantWithin(nil, 720); l != "" {
		t.Error("nil variants should yield nothing")
	}
	if l, _ := bestVariantWithin(VideoVariants{}, 720); l != "" {
		t.Error("empty variants should yield nothing")
	}
	if l, _ := bestVariantWithin(VideoVariants{"720p": "u"}, 0); l != "" {
		t.Error("a zero budget means unconstrained, so no downshift should be chosen")
	}
	// A rung with a label but no URL is not usable.
	if l, _ := bestVariantWithin(VideoVariants{"480p": ""}, 720); l != "" {
		t.Error("a variant with an empty URL must not be selected")
	}
}

// The order the file header describes, one case per step.
func TestFitChallengeToDevice(t *testing.T) {
	t.Run("no ceiling named: nothing is touched", func(t *testing.T) {
		c := &Challenge{VideoURL: "orig"}
		fit, keep := fitChallengeToDevice(c, 0, videoDimensions{3840, 2160})
		if fit != fitUnconstrained || !keep {
			t.Fatalf("fit=%v keep=%v, want unconstrained/keep", fit, keep)
		}
		if c.VideoURL != "orig" {
			t.Error("an unconstrained caller must get the original URL")
		}
	})

	t.Run("adaptive wins over everything", func(t *testing.T) {
		c := &Challenge{VideoURL: "orig", HLSManifestURL: "m.m3u8",
			VideoVariants: VideoVariants{"480p": "u480"}}
		fit, keep := fitChallengeToDevice(c, 720, videoDimensions{3840, 2160})
		if fit != fitAdaptive || !keep {
			t.Fatalf("fit=%v keep=%v, want adaptive/keep", fit, keep)
		}
		if c.VideoURL != "orig" {
			t.Error("adaptive must not rewrite the URL — the player chooses its own rung")
		}
	})

	t.Run("already within budget", func(t *testing.T) {
		c := &Challenge{VideoURL: "orig"}
		fit, keep := fitChallengeToDevice(c, 1280, videoDimensions{1280, 720})
		if fit != fitAlreadySmall || !keep {
			t.Fatalf("fit=%v keep=%v, want already-small/keep", fit, keep)
		}
		if c.VideoURL != "orig" {
			t.Error("a file already small enough must not be rewritten")
		}
	})

	t.Run("too big but a smaller rung exists: downshift, keep", func(t *testing.T) {
		c := &Challenge{VideoURL: "big",
			VideoVariants: VideoVariants{"480p": "u480", "720p": "u720", "1080p": "u1080"}}
		fit, keep := fitChallengeToDevice(c, 720, videoDimensions{1920, 1080})
		if fit != fitDownshifted || !keep {
			t.Fatalf("fit=%v keep=%v, want downshifted/keep", fit, keep)
		}
		if c.VideoURL != "u720" {
			t.Errorf("VideoURL = %q, want the 720p rung", c.VideoURL)
		}
	})

	t.Run("too big with nothing smaller: the only drop", func(t *testing.T) {
		c := &Challenge{VideoURL: "big"}
		fit, keep := fitChallengeToDevice(c, 720, videoDimensions{3840, 2160})
		if fit != fitTooLarge || keep {
			t.Fatalf("fit=%v keep=%v, want too-large/drop", fit, keep)
		}
	})

	t.Run("too big, and every rung is also too big", func(t *testing.T) {
		c := &Challenge{VideoURL: "big", VideoVariants: VideoVariants{"1080p": "u1080"}}
		fit, keep := fitChallengeToDevice(c, 720, videoDimensions{1920, 1080})
		if fit != fitTooLarge || keep {
			t.Fatalf("fit=%v keep=%v, want too-large/drop", fit, keep)
		}
	})

	// The rule that keeps the feed full for older content. Unknown must not be
	// read as too large — every row uploaded before the probe existed is
	// unknown, and dropping them all would empty the feed on a weak phone.
	t.Run("never measured: kept", func(t *testing.T) {
		c := &Challenge{VideoURL: "orig"}
		fit, keep := fitChallengeToDevice(c, 720, videoDimensions{})
		if fit != fitUnknownSize || !keep {
			t.Fatalf("fit=%v keep=%v, want unknown-size/keep", fit, keep)
		}
		if c.VideoURL != "orig" {
			t.Error("an unmeasured item must not be rewritten")
		}
	})
}

// Portrait video is most of a phone feed, and it is the case a width-only
// comparison gets wrong. 1080x1920 is 1080p and must be judged as such.
func TestFitChallengeToDevice_JudgesPortraitByLongSide(t *testing.T) {
	c := &Challenge{VideoURL: "big", VideoVariants: VideoVariants{"720p": "u720"}}
	fit, keep := fitChallengeToDevice(c, 720, videoDimensions{1080, 1920})
	if fit != fitDownshifted || !keep {
		t.Fatalf("fit=%v keep=%v, want downshifted/keep — portrait 1080p was not "+
			"recognised as too large", fit, keep)
	}
	if c.VideoURL != "u720" {
		t.Errorf("VideoURL = %q, want the 720p rung", c.VideoURL)
	}
}
