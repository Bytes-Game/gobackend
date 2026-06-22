package main

import (
	"database/sql"
	"strings"
	"unicode"
)

// Energy-level classifier — fully algorithmic, no user input.
//
// Earlier version did first-match keyword lookups against small lists.
// That worked but was brittle — anything outside the ~20 keywords fell
// to the category default. Inputs like "freestyle motocross" or
// "speed cubing" never got the boost they deserved.
//
// New design: weighted scoring across multiple signals. Each signal
// contributes points to a high-score and a low-score tally; the larger
// tally wins, with a margin requirement (so a single weak match
// doesn't flip the result). Falls back to creator baseline when the
// signals are genuinely tied — your average reel from a given creator
// is the best zero-shot prior for their next one.
//
// Signals + weights:
//   * Subject strong keyword:        ±3.0
//   * Subject mild keyword:          ±1.5
//   * Caption strong keyword:        ±1.5
//   * Caption mild keyword:          ±0.75
//   * Category default:              ±2.0
//   * Creator's recent avg energy:   ±1.5 (only when DB available)
//
// Tiebreak threshold: 1.5 — the high/low gap must clear this before
// we commit to a non-medium label. Anything tighter than that is
// genuinely ambiguous and "medium" is the right answer.
//
// Why "weighted score" rather than ML: zero training data, zero infra
// cost, zero latency, and the recommender already adapts to real
// engagement signals — a misclassification here is corrected over
// time by impressions, not by re-training. Good enough at scale, and
// it's auditable: anyone can read the rules and predict the output.

// ════════════════════════════════════════════════════════════════════
// Score weights — tuneable from one place
// ════════════════════════════════════════════════════════════════════
const (
	wSubjectStrong  = 3.0
	wSubjectMild    = 1.5
	wCaptionStrong  = 1.5
	wCaptionMild    = 0.75
	wCategoryHint   = 2.0
	wCreatorBaseline = 1.5

	// Margin required to commit to "high" or "low". Tighter than this
	// → "medium". 1.5 is one full "mild" signal of headroom.
	energyTieThreshold = 1.5
)

// ════════════════════════════════════════════════════════════════════
// Keyword tables — paired high/low signals
// ════════════════════════════════════════════════════════════════════

// strongHighSubjectKeywords are activities or motifs whose energy is
// unmistakable — anything sprint/race/fight/stunt themed. Match here
// almost always means "high" unless the caption explicitly walks it
// back ("slow-motion sprint").
var strongHighSubjectKeywords = []string{
	"sprint", "race", "rac ", "racing",
	"battle", "fight", "fighting", "brawl", "spar",
	"speed", "fastest", "speedrun", "speedcube", "speedrunning",
	"stunt", "trick", "flip", "frontflip", "backflip", "wheelie",
	"freestyle", "breakdanc", "krump", "popping", "locking",
	"parkour", "tricking", "flips", "flipping bottles",
	"crash", "extreme", "intense", "wild", "savage",
	"wreck", "demolition", "smash", "explosion",
	"jumpscare", "scream",
	"prank", "harass", "roast", "diss",
	"kickboxing", "muay thai", "boxing", "mma", "ufc",
	"chase", "pursuit", "rush", "ramp",
	"drag racing", "drifting", "burnout",
	"highline", "freefall", "skydiv", "wingsuit",
	"bull rid", "bull-rid", "rodeo",
}

// mildHighSubjectKeywords are activities that lean energetic but
// could be calm depending on style. Most sports + dance + music
// performance fall here.
var mildHighSubjectKeywords = []string{
	"dance", "dancing", "salsa", "bachata", "kizomba",
	"hip hop", "tiktok danc", "kpop",
	"workout", "weightlift", "powerlift", "deadlift", "squat",
	"crossfit", "calisthen", "burpee", "jumprope", "jumping",
	"basketball", "football", "soccer", "tennis", "badminton",
	"volleyball", "hockey", "rugby", "cricket", "baseball",
	"surf", "skateboard", "snowboard", "ski", "skiing",
	"climb", "bouldering", "ropes course",
	"cycle", "cycling", "bmx", "drone", "rc car",
	"run", "running", "marathon", "hurdle", "hurdles",
	"sing", "rap", "rapping", "beatbox", "freestyl",
	"impression", "satire", "parody", "meme",
	"reaction", "react", "watch party",
	"motocross", "motorbike", "motorcycle",
	"gaming", "game", "esports", "valorant", "fortnite",
	"trampoline", "skatepark",
}

// strongLowSubjectKeywords pin the result low. Meditation, ASMR,
// poetry, slow craft work — these are the unmistakably-chill end.
var strongLowSubjectKeywords = []string{
	"meditat", "mindful", "yoga nidra",
	"asmr", "whisper", "tapping sounds",
	"lo-fi", "lofi", "ambient", "sleep",
	"calm", "peaceful", "soothing",
	"haiku", "poetry", "spoken word",
	"bonsai", "calligraph",
	"journaling", "bullet journal",
	"book review", "speed read",
	"painting along", "watercolor",
	"slow motion", "slow-motion",
	"slow cooking", "slow burn",
}

// mildLowSubjectKeywords are activities that read as calm but could
// spike in some contexts (a "pottery throw-off" is still pottery).
var mildLowSubjectKeywords = []string{
	"draw", "sketch", "doodling",
	"painting", "watercolor", "oil paint", "acrylic",
	"craft", "knit", "crochet", "embroidery", "quilt",
	"pottery", "clay", "sculpt",
	"baking", "pastry", "bread", "sourdough",
	"latte art", "barista", "coffee",
	"reading", "writing", "studying",
	"chess", "puzzle", "sudoku", "crossword",
	"rubiks", "rubik",
	"gardening", "plant", "propagation",
	"fishing", "fly fishing",
	"camping", "hiking", "backpacking",
	"asmrtist",
	"makeup tutorial", "skincare", "self care", "spa",
	"yoga", "pilates", "stretch", "flexibility",
	"photography", "drone shot", "macro photo",
	"cinematography", "color grad", "color grade",
	"chess", "code", "coding", "leetcode",
	"3d print", "soldering", "arduino", "raspberry pi",
}

// strongHighCaption keywords push the energy up regardless of the
// subject's reading. Captions are aspirational — "I went all out" is
// the creator self-classifying their content as high energy.
var strongHighCaptionKeywords = []string{
	"intense", "all out", "no holds barred", "going crazy",
	"insane", "epic", "wildest", "craziest",
	"hype", "lit", "fire", "absolute heater",
	"won't believe", "savage",
}

// mildHighCaption keywords nudge up without being decisive.
var mildHighCaptionKeywords = []string{
	"fast", "hard", "tough", "raw", "real",
	"action", "moving", "energy",
}

// strongLowCaption keywords push the energy down.
var strongLowCaptionKeywords = []string{
	"chill", "relax", "calming", "peaceful", "soothing",
	"quiet", "soft", "gentle", "slow down", "winding down",
	"asmr-like", "no talking",
}

// mildLowCaption keywords nudge down without locking in.
var mildLowCaptionKeywords = []string{
	"casual", "easy", "simple", "minimal", "subtle",
	"thoughtful", "reflective",
}

// ════════════════════════════════════════════════════════════════════
// Category hint table
// ════════════════════════════════════════════════════════════════════

// categoryHint returns the bias the category contributes. Positive →
// high; negative → low; zero → neutral (no opinion). Scaled by
// wCategoryHint at the call site so the absolute values stay readable
// here.
func categoryHint(category string) float64 {
	switch strings.ToLower(strings.TrimSpace(category)) {
	case "sports", "dance", "prank":
		return 1.0
	case "comedy", "motivation":
		return 0.5
	case "music", "gaming", "fashion", "food", "lifestyle":
		return 0.0
	case "horror":
		// Horror is bimodal — jump scares are high, slow-burn is
		// low. Calling it 0 lets the subject/caption decide which
		// kind we're looking at.
		return 0.0
	case "art", "tech", "education", "story", "emotional":
		return -0.5
	default:
		return 0.0
	}
}

// ════════════════════════════════════════════════════════════════════
// Public entrypoint
// ════════════════════════════════════════════════════════════════════

// deriveEnergyLevel returns "low" | "medium" | "high" for a new
// challenge. Backwards-compatible with the older 3-arg signature: the
// caller is free to pass empty strings for any signal it doesn't have.
// The creator-baseline path activates only when [creatorID] is
// non-empty AND we have a DB handle — otherwise we just run on the
// text + category signals.
//
// This is the canonical entry point from CreateChallenge. Tests and
// any other classifier consumers should also call it (rather than
// reaching into scoreEnergy directly) so the threshold logic stays
// in one place.
func deriveEnergyLevel(category, subject, caption string) string {
	return deriveEnergyLevelWithCreator(category, subject, caption, "")
}

// deriveEnergyLevelWithCreator is the variant that folds in the
// creator-baseline signal. Most callers should use the 3-arg
// [deriveEnergyLevel]; this version exists so CreateChallenge can
// pass the creator ID without forcing every other call site to
// thread it through.
func deriveEnergyLevelWithCreator(category, subject, caption, creatorID string) string {
	high, low := scoreEnergy(category, subject, caption, creatorID)
	gap := high - low
	if gap >= energyTieThreshold {
		return "high"
	}
	if -gap >= energyTieThreshold {
		return "low"
	}
	return "medium"
}

// scoreEnergy returns the (highScore, lowScore) tally for the inputs.
// Split out so unit tests can assert on the raw numbers rather than
// just the rounded label.
func scoreEnergy(category, subject, caption, creatorID string) (high, low float64) {
	subjectL := strings.ToLower(subject)
	captionL := strings.ToLower(caption)

	// — Subject pass —
	if containsAnyKeyword(subjectL, strongHighSubjectKeywords) {
		high += wSubjectStrong
	}
	if containsAnyKeyword(subjectL, mildHighSubjectKeywords) {
		high += wSubjectMild
	}
	if containsAnyKeyword(subjectL, strongLowSubjectKeywords) {
		low += wSubjectStrong
	}
	if containsAnyKeyword(subjectL, mildLowSubjectKeywords) {
		low += wSubjectMild
	}

	// — Caption pass —
	if containsAnyKeyword(captionL, strongHighCaptionKeywords) {
		high += wCaptionStrong
	}
	if containsAnyKeyword(captionL, mildHighCaptionKeywords) {
		high += wCaptionMild
	}
	if containsAnyKeyword(captionL, strongLowCaptionKeywords) {
		low += wCaptionStrong
	}
	if containsAnyKeyword(captionL, mildLowCaptionKeywords) {
		low += wCaptionMild
	}

	// — Category hint —
	hint := categoryHint(category) * wCategoryHint
	if hint > 0 {
		high += hint
	} else if hint < 0 {
		low += -hint
	}

	// — Creator baseline (cheap when DB is up) —
	if creatorID != "" && db != nil {
		switch creatorRecentEnergy(creatorID) {
		case "high":
			high += wCreatorBaseline
		case "low":
			low += wCreatorBaseline
		}
	}

	return high, low
}

// creatorRecentEnergy returns the modal energy level of the user's
// last few challenges. Returns "" (no opinion) for cold-start
// creators with zero or one priors — too little signal to be useful.
//
// Cheap query backed by the existing challenges index on
// (created_at DESC) — the LIMIT 5 keeps it bounded even on prolific
// creators.
func creatorRecentEnergy(creatorID string) string {
	const recentN = 5
	rows, err := db.Query(
		`SELECT COALESCE(energy_level, 'medium')
		   FROM challenges
		  WHERE creator_id = $1
		  ORDER BY created_at DESC
		  LIMIT $2`,
		creatorID, recentN,
	)
	if err == sql.ErrNoRows || err != nil {
		return ""
	}
	defer rows.Close()
	counts := map[string]int{}
	total := 0
	for rows.Next() {
		var lvl string
		if rows.Scan(&lvl) != nil {
			continue
		}
		counts[lvl]++
		total++
	}
	if total < 2 {
		// First-or-second challenge from this creator — recommender
		// hasn't accumulated enough signal yet for a confident prior.
		return ""
	}
	// Modal answer wins. On a 2-2 tie we abstain ("") so the text
	// signals decide rather than letting an arbitrary order break
	// the tie.
	bestLvl := ""
	bestN := 0
	tied := false
	for lvl, n := range counts {
		if n > bestN {
			bestLvl = lvl
			bestN = n
			tied = false
		} else if n == bestN {
			tied = true
		}
	}
	if tied {
		return ""
	}
	return bestLvl
}

// ════════════════════════════════════════════════════════════════════
// Helpers
// ════════════════════════════════════════════════════════════════════

// containsAnyKeyword is the inner loop of the subject/caption keyword
// passes. Named with the "Keyword" suffix because a more generic
// `containsAny` already exists in candidate_sources.go with a
// different signature (slice-haystack / string-needle). Keeping the
// names distinct prevents future-me from grabbing the wrong one.
//
// Matching is word-aware rather than raw substring, so short needles
// stop false-matching inside longer unrelated words ("ski" → "skincare",
// "run" → "brunch", "code" → "decode", "game" → "endgame"). The rules,
// in priority order per needle:
//   - phrase / hyphenated needle (contains ' ' or '-'): raw substring,
//     because the keyword lists deliberately use these for multi-word
//     motifs ("muay thai", "drag racing", "asmr-like", "slow-motion").
//   - short needle (<5 runes): must equal a whole word token, so "ski"
//     matches the word "ski" but never "skincare".
//   - stem needle (>=5 runes): matches a token by prefix, preserving the
//     intended inflection stems ("meditat"→"meditation", "breakdanc"→
//     "breakdancing", "speedrun"→"speedrunning").
func containsAnyKeyword(haystack string, needles []string) bool {
	var toks []string // tokenized lazily — only when a word-needle needs it
	for _, n := range needles {
		if strings.ContainsAny(n, " -") {
			if strings.Contains(haystack, n) {
				return true
			}
			continue
		}
		if toks == nil {
			toks = tokenizeWords(haystack)
		}
		stem := len(n) >= 5
		for _, t := range toks {
			if t == n || (stem && strings.HasPrefix(t, n)) {
				return true
			}
		}
	}
	return false
}

// tokenizeWords splits text into lowercase-able word tokens on any
// non-letter/non-digit rune. Local to the energy classifier so its
// matching semantics can't drift if the shared tokenizer changes.
func tokenizeWords(s string) []string {
	return strings.FieldsFunc(s, func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsDigit(r)
	})
}
