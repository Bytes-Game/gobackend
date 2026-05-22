package main

import "strings"

// Curated vocabulary for the challenge create-flow autocomplete.
//
// Two corpora:
//   * ChallengePrefixes — the left side of a challenge ("Who is better
//     at"). Small closed set, hand-tuned. About 30 templates.
//   * ChallengeSubjects — the right side ("dancing in the rain").
//     Open-vocabulary list of "challengeable" activities — verbs,
//     hobbies, skills, sports, creative pursuits. About 700 entries
//     hand-curated to bias the platform toward content that actually
//     translates into video challenges.
//
// Why hand-curated rather than scraping a dictionary:
//   * Most dictionary words don't make challenge sense ("Who is better
//     at thermodynamics?"). The hand-pick filter drops that noise.
//   * Bootstrap quality matters — on day one we have ~zero existing
//     challenges to learn from, so the autocomplete IS the corpus.
//     As real challenges accumulate, the Meilisearch index re-ranks
//     toward what users actually pick (see suggest_handlers.go).
//   * Sized to ship inside the binary — the whole file is ~30KB which
//     is well under the embed budget. No external dictionary file +
//     no extra dependency.

// ChallengePrefixes is the curated list of "left side" question templates.
// Order is the default ranking when no query is provided. When a query IS
// provided, suggest_handlers.go does a prefix-match + length-weighted rank
// so "who is" returns these in the same order, while "who has" surfaces
// the cleanest-X family first.
var ChallengePrefixes = []string{
	"Who is better at",
	"Who is the best at",
	"Who has the cleanest",
	"Who has the smoothest",
	"Who has the most insane",
	"Who can pull off",
	"Who can survive",
	"Who can take on",
	"Who is the funniest at",
	"Who has the best taste in",
	"Who has the wildest",
	"Who can do the longest",
	"Who can finish the fastest",
	"Who can go the furthest at",
	"Who has the most skill in",
	"Who can outlast me at",
	"Who can outsmart me at",
	"Who can recreate the best",
	"Who can rate the best",
	"Who has the boldest",
	"Who has the sharpest",
	"Who has the funniest take on",
	"Who has the hottest",
	"Who can beat me at",
	"Who can match my",
	"Who has more style in",
	"Who has more swagger at",
	"Who is the king of",
	"Who is the queen of",
	"Who would win at",
}

// challengeSubjectsRaw is the canonical pool of one-to-few word
// "challengeable" activities used as the seed for the Meilisearch
// `challenge_subjects` index. The slice is a flat list — categorization
// happens at search-time via prefix match + popularity rank. Roughly
// grouped here for readability:
//
//   * dance / music / performance
//   * sports / athletic
//   * gaming
//   * creative / art / craft
//   * food / cooking
//   * fashion / style
//   * comedy / pranks / impressions
//   * tech / code / DIY
//   * lifestyle / fitness / wellness
//   * social / dating / friends
//
// Adding to this list is a code-only change. The next deploy seeds the
// new entries into Meilisearch on startup (see seedChallengeSubjects).
var challengeSubjectsRaw = []string{
	// — Dance / music / performance —
	"dancing", "freestyle dancing", "hip hop dancing", "tiktok dancing",
	"contemporary dancing", "breakdancing", "tap dancing", "salsa",
	"bachata", "kizomba", "kpop dancing", "ballroom", "ballet",
	"jazz dance", "house dance", "popping", "locking", "krumping",
	"singing", "rapping", "freestyle rap", "beatboxing", "vocal runs",
	"karaoke", "high notes", "harmonizing", "humming", "whistling",
	"playing guitar", "playing piano", "playing drums", "playing violin",
	"playing ukulele", "playing flute", "playing saxophone", "playing bass",
	"djing", "producing beats", "mixing tracks", "songwriting",
	"lip syncing", "dubbing", "mimicking accents", "voice acting",
	"performing on stage", "improv", "stand up comedy",

	// — Sports / athletic / fitness —
	"basketball", "football", "soccer", "tennis", "badminton",
	"volleyball", "table tennis", "ping pong", "cricket", "baseball",
	"hockey", "ice hockey", "field hockey", "lacrosse", "rugby",
	"running", "sprinting", "marathon running", "long distance running",
	"hurdles", "high jump", "long jump", "pole vault", "shot put",
	"swimming", "freestyle swimming", "butterfly stroke", "diving",
	"surfing", "skating", "ice skating", "figure skating", "roller skating",
	"skateboarding", "longboarding", "snowboarding", "skiing", "water skiing",
	"jet skiing", "wakeboarding", "kayaking", "rowing", "canoeing",
	"cycling", "bmx", "mountain biking", "fixie tricks",
	"boxing", "kickboxing", "muay thai", "mma", "bjj", "judo", "karate",
	"taekwondo", "wrestling", "fencing", "archery", "shooting",
	"climbing", "bouldering", "rock climbing", "free climbing",
	"parkour", "tricking", "flips", "backflips", "front flips",
	"handstands", "headstands", "cartwheels", "splits",
	"yoga", "pilates", "stretching", "flexibility",
	"weightlifting", "powerlifting", "bench press", "deadlift", "squats",
	"calisthenics", "pull ups", "push ups", "dips", "muscle ups",
	"planking", "burpees", "jumping rope",
	"golf", "bowling", "pool", "billiards", "snooker", "darts",
	"chess", "speed chess",

	// — Gaming —
	"fortnite", "minecraft", "roblox", "valorant", "league of legends",
	"dota", "csgo", "apex legends", "warzone", "pubg", "free fire",
	"clash royale", "clash of clans", "brawl stars", "genshin",
	"call of duty", "battlefield", "rainbow six", "overwatch",
	"super smash bros", "mario kart", "mario party", "tekken",
	"street fighter", "mortal kombat", "guilty gear",
	"fifa", "fifa freestyle", "nba 2k", "madden",
	"speedrunning", "no scope shots", "trick shots", "build battles",
	"sandbox builds", "redstone builds", "parkour maps",
	"retro gaming", "arcade games", "rhythm games",

	// — Creative / art / craft —
	"drawing", "sketching", "doodling", "painting", "watercolor",
	"oil painting", "acrylic painting", "digital art", "pixel art",
	"animation", "stop motion", "claymation", "3d modeling", "blender",
	"sculpting", "pottery", "clay work", "origami", "paper craft",
	"calligraphy", "lettering", "graffiti", "spray painting", "mural art",
	"tattoo art", "henna", "nail art", "makeup", "sfx makeup",
	"costume design", "cosplay", "props", "set design",
	"photography", "portrait photography", "street photography",
	"wildlife photography", "macro photography", "astrophotography",
	"film photography", "polaroid", "phone photography",
	"videography", "drone shots", "cinematography", "video editing",
	"color grading", "vfx", "after effects", "premiere edits",
	"motion graphics", "title sequences", "transitions",
	"poetry", "spoken word", "haiku", "writing", "storytelling",
	"flash fiction", "screenwriting", "scripting",
	"knitting", "crochet", "sewing", "embroidery", "quilting",
	"woodworking", "carving", "metalwork", "welding", "leatherwork",
	"jewelry making", "beadwork", "soap making", "candle making",

	// — Food / cooking —
	"cooking", "baking", "pastry", "bread baking", "sourdough",
	"cake decorating", "cookies", "macarons", "donuts", "chocolate",
	"latte art", "barista skills", "coffee", "espresso", "cold brew",
	"cocktails", "mixology", "mocktails", "bartending", "flair bartending",
	"grilling", "bbq", "smoking meats", "steak", "burgers", "pizza",
	"pasta making", "noodle pulling", "ramen", "sushi rolling", "sashimi",
	"dumplings", "tacos", "tortillas", "biryani", "curry", "stir fry",
	"vegan cooking", "raw food", "salads", "smoothies", "juices",
	"meal prep", "knife skills", "plating", "food styling",
	"speed eating", "spicy food", "chili challenge", "hot wings",
	"food reviews", "taste tests", "blind taste tests",

	// — Fashion / style —
	"outfit reveals", "thrift flips", "thrifting", "ootd",
	"streetwear", "y2k fashion", "vintage fashion", "minimalism",
	"capsule wardrobes", "color matching", "layering",
	"sneaker collecting", "shoe lacing", "watch collecting",
	"hairstyling", "braiding", "curling", "straightening",
	"barbering", "fades", "shaves", "beard care",
	"skincare routine", "self care", "spa day", "manicures",

	// — Comedy / pranks / impressions —
	"pranks", "harmless pranks", "office pranks", "family pranks",
	"impressions", "celebrity impressions", "accent impressions",
	"satire", "parody", "memes", "meme reviews", "reactions",
	"roasts", "jokes", "one liners", "dad jokes", "puns",
	"storytime", "embarrassing stories", "ghost stories",
	"prank calls", "social experiments",

	// — Tech / code / DIY —
	"coding", "speed coding", "code golf", "leetcode", "hackathons",
	"web design", "ui design", "ux flows", "logo design", "branding",
	"app prototyping", "game dev", "shader art", "creative coding",
	"3d printing", "soldering", "electronics", "arduino", "raspberry pi",
	"robotics", "drones", "rc cars", "rc planes",
	"home renovation", "diy furniture", "upcycling", "restoring",
	"car detailing", "car mods", "engine swaps", "drifting",
	"motorcycle tricks", "wheelies", "stoppies",

	// — Lifestyle / wellness —
	"morning routines", "night routines", "study routines", "productivity",
	"journaling", "bullet journaling", "habit tracking", "goal setting",
	"meditation", "breathwork", "mindfulness", "manifestation",
	"reading", "speed reading", "book reviews",
	"languages", "learning languages", "polyglot", "translation",
	"travel hacks", "packing", "road trips", "solo travel",
	"camping", "hiking", "backpacking", "survival skills", "bushcraft",
	"fishing", "fly fishing", "kayak fishing", "spearfishing",
	"gardening", "plant parenting", "propagation", "bonsai",
	"pet care", "dog tricks", "cat tricks", "horse riding",

	// — Social / dating / vibes —
	"first dates", "pickup lines", "rizz", "rizz battles",
	"compliments", "small talk", "ice breakers",
	"asmr", "asmr triggers", "whispering", "tapping sounds",
	"reaction videos", "watch parties",

	// — Knowledge / mind —
	"trivia", "speed trivia", "geography", "history facts", "science facts",
	"math tricks", "mental math", "memorization", "puzzles",
	"sudoku", "crosswords", "rubiks cube", "rubiks cube speed",
	"magic tricks", "card tricks", "coin tricks", "sleight of hand",
	"escape rooms", "mystery solving",

	// — Spooky / horror / wildcards —
	"scary stories", "horror reviews", "true crime", "paranormal",
	"haunted spots", "urban exploration", "abandoned places",

	// — Quick action verbs (very generic catch-alls) —
	"jumping", "sprinting", "climbing", "spinning", "balancing",
	"juggling", "flipping coins", "stacking cups", "stacking dominoes",
	"flipping bottles",
}

// challengeSubjectIndex is a lowercase set of all known subjects for
// O(1) duplicate-check on insert. Populated once at init.
var challengeSubjectIndex map[string]bool

func init() {
	challengeSubjectIndex = make(map[string]bool, len(challengeSubjectsRaw))
	for _, s := range challengeSubjectsRaw {
		challengeSubjectIndex[strings.ToLower(strings.TrimSpace(s))] = true
	}
}

// IsKnownSubject is a constant-time lookup against the curated vocab —
// useful for downstream classifiers (e.g. energy-level heuristics) that
// want to skip unfamiliar inputs.
func IsKnownSubject(s string) bool {
	return challengeSubjectIndex[strings.ToLower(strings.TrimSpace(s))]
}

// SuggestPrefixes returns up to `limit` prefix templates ranked by
// prefix-match against `q`. Empty query returns the default order
// (highest-utility templates first).
//
// Why this lives next to the data instead of in suggest_handlers.go:
// the prefix corpus is small, fully owned by us, and never needs
// Meilisearch — keeping the picker inline avoids a network hop and
// lets the handler skip the Meili-down fallback path entirely for
// this surface.
func SuggestPrefixes(q string, limit int) []string {
	if limit <= 0 {
		limit = 8
	}
	q = strings.ToLower(strings.TrimSpace(q))
	if q == "" {
		if len(ChallengePrefixes) <= limit {
			out := make([]string, len(ChallengePrefixes))
			copy(out, ChallengePrefixes)
			return out
		}
		return append([]string(nil), ChallengePrefixes[:limit]...)
	}
	// Two-pass match: exact prefix first, then substring fallback so
	// "best at" returns "Who is the best at" even though the curated
	// template begins with "Who". Both passes preserve the curated
	// order, which is itself a hand-tuned popularity ranking.
	var prefixHits, substrHits []string
	for _, p := range ChallengePrefixes {
		lower := strings.ToLower(p)
		if strings.HasPrefix(lower, q) {
			prefixHits = append(prefixHits, p)
		} else if strings.Contains(lower, q) {
			substrHits = append(substrHits, p)
		}
	}
	out := append(prefixHits, substrHits...)
	if len(out) > limit {
		out = out[:limit]
	}
	return out
}
