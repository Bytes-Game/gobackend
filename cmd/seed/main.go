// Command seed replaces the challenge/response content in a database with a
// known set of sample reels.
//
// Why this exists
// ---------------
// Testing the feed needs content whose behaviour is predictable. The uploads
// currently in the database are a mixed bag — some transcoded, some raw 1440x2560
// originals, all served from a `pub-*.r2.dev` bucket that Cloudflare rate-limits
// hard enough to return 429 mid-scroll. That makes every playback measurement
// ambiguous: a stutter could be the player, the prefetch window, or simply the
// host refusing to answer.
//
// The clips seeded here are the Google/ExoPlayer public sample set (Blender
// Foundation open movies, CC-BY, plus Google's own promo clips). They matter for
// three reasons: they are served from a bucket that does not throttle, they honour
// HTTP range requests — which the loopback media proxy depends on — and the
// `ForBigger*` clips are 1280x720, exactly the ceiling the reels feed targets.
//
// Safety
// ------
// Deleting content is irreversible, so `-reset` alone does nothing: it must be
// paired with `-yes-delete-existing-content`. Without both, the tool only inserts.
// Nothing here runs automatically — it is a command you invoke deliberately, not
// an endpoint left exposed on the API.
//
// `-dry-run` executes every statement for real and then rolls the transaction
// back, so it reports exact before/after counts and surfaces any schema
// mismatch without keeping a single row. Prefer it before the real thing.
//
// Accounts that already exist are reused, never overwritten — this will not
// change the password on an account someone is actively using. Only newly
// created accounts get the seed password, and the output says which is which.
//
// Usage:
//
//	export DATABASE_URL='postgres://...'
//	go run ./cmd/seed -reset -dry-run                        # preview, changes nothing
//	go run ./cmd/seed -reset -yes-delete-existing-content     # wipe + seed
//	go run ./cmd/seed                                         # insert only
package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"log"
	"os"
	"strings"
	"time"

	_ "github.com/lib/pq"
	"golang.org/x/crypto/bcrypt"
)

// The sample clips.
//
// EVERY url here was verified to answer HTTP 206 to a Range request at the
// time of writing — the loopback media proxy depends on ranges, and an
// origin that ignores them silently falls back to whole-file downloads.
//
// History, and the reason for the paranoia: this file used to point at
// Google's gtv-videos-bucket. That bucket is now PRIVATE — every url in it
// answers 403 "Anonymous caller does not have storage.objects.get access".
// Running the old seed would have filled the feed with videos that could
// never play.
//
// Size is the other selection criterion, and the more important one. A
// reel is a few seconds long; the feed this replaces carried a 58 MB clip,
// a 73 MB clip and one 249 MB feature film, which is why playback felt
// slow no matter what the caching layer did. Nothing here exceeds 5.3 MB
// and nothing exceeds 720p, which is the ceiling the player targets.
type clip struct {
	url      string
	bytes    int // measured, so the "is this reel-sized?" check is not a guess
	title    string
	category string
	energy   string
	emotions []string
}

// r2Base is the public host our own bucket is served from. Everything the
// seeder references lives here now.
//
// These clips used to be hotlinked from the sites they were sourced from,
// and nine of the fourteen came from one host measured at ~1.0s to first
// byte. A reels player issues many range requests per video, so that
// second was paid over and over: the decoder got a burst, played it, and
// starved. On a device that was "the video freezes until I pause and
// play". Measured the same way afterwards, this host answers in ~0.5s and
// sits on Cloudflare's edge rather than one origin box.
//
// Objects are written by cmd/mediaimport, which keys them off a hash of
// the original source URL. Re-running it overwrites in place, so these
// URLs are stable. Do not hand-edit them: change the source list there
// and re-run the media-import workflow.
const r2Base = "https://pub-72947583733d4d108b6495e932ae4f04.r2.dev/seed/"

var clips = []clip{
	{r2Base + "7fa7093550751f14.mp4", 969201, "Can you top this entrance?", "comedy", "high", []string{"funny", "surprise"}},
	{r2Base + "c1d850ec1287e543.mp4", 1047967, "Best escape move wins", "sports", "high", []string{"excited"}},
	{r2Base + "82dc123d418f65ca.mp4", 1047954, "Show me your happy place", "lifestyle", "medium", []string{"happy"}},
	{r2Base + "d06ec0b6febc2404.mp4", 1293015, "Dream ride challenge", "lifestyle", "high", []string{"excited", "happy"}},
	{r2Base + "d5eb607f4c8e108b.mp4", 1128375, "Funniest reaction wins", "comedy", "high", []string{"funny"}},
	{r2Base + "234c279a4913634d.mp4", 1978137, "Review your setup in 30s", "tech", "medium", []string{"curious"}},
	{r2Base + "8ad67e2519c0f584.mp4", 2096842, "Street or studio — pick a side", "sports", "high", []string{"excited"}},
	{r2Base + "610809b029a7eda9.mp4", 2094185, "Story time challenge", "story", "medium", []string{"happy"}},
	{r2Base + "3227f04eab6e624f.mp4", 2429896, "Best slow-motion shot", "art", "low", []string{"calm"}},
	{r2Base + "7de928f9e4f43afa.mp4", 2757913, "Best budget find challenge", "education", "low", []string{"curious"}},
	{r2Base + "90b1846cf73325ed.mp4", 4999379, "Funniest animation dub", "comedy", "medium", []string{"funny"}},
	{r2Base + "399605c61cdd4cb7.mp4", 5241877, "Calmest scene wins", "art", "low", []string{"calm"}},
	{r2Base + "763b99b2e83fd647.mp4", 5242780, "Most cinematic 10 seconds", "art", "medium", []string{"curious"}},
	{r2Base + "4e1dee73db81e09b.mp4", 4372373, "Sci-fi one-shot challenge", "story", "medium", []string{"excited"}},
}

// maxReelBytes is the size a sample clip must stay under to belong in a
// reels feed at all. Enforced in main() so a future edit that pastes in
// another full-length movie fails loudly instead of quietly making the
// feed slow again.
const maxReelBytes = 6 * 1024 * 1024

// seedChallenges is how many challenges to create. A feed page is 20, so
// one page-worth plus enough behind it to prove pagination works.
const seedChallenges = 28

// altTitles supplies the prompt for the second pass over the clip pool.
var altTitles = []string{
	"Who can hold a straight face longest",
	"Best 3-second intro wins",
	"Recreate this shot with what you own",
	"Funniest caption for this clip",
	"Who has the steadier hand",
	"Explain this in ten seconds",
	"Best transition wins",
	"Guess the ending challenge",
	"Most creative use of one prop",
	"Who can do it slower",
	"Best sound-effect dub",
	"Calmest take wins",
	"Most cinematic angle",
	"Best plot twist in 10s",
}

func (c clip) videoURL() string { return c.url }

// thumbURL is the poster frame cut from this clip's own first second,
// uploaded next to the video by cmd/mediaimport under the same key.
//
// It used to be a random photo from picsum.photos, on the theory that a
// deterministic abstract placeholder beat a mismatched still. That was
// wrong in practice: what the service actually returns is photographs of
// stars and landscapes, and a landscape sitting on top of a jellyfish
// video does not read as a placeholder to anyone. It reads as the wrong
// video.
//
// Deriving the poster from the video URL rather than storing it as a
// second field is deliberate — the two cannot drift apart, and a clip
// added without a matching poster upload fails visibly (404) instead of
// silently showing someone else's picture.
func (c clip) thumbURL() string {
	return strings.TrimSuffix(c.url, ".mp4") + ".jpg"
}

// seedUser is a demo account. Passwords are bcrypt-hashed exactly as the
// signup path does, so these accounts can actually be logged into.
type seedUser struct {
	username string
	fullName string
}

var users = []seedUser{
	{"player1", "Player One"},
	{"player2", "Player Two"},
	{"maya", "Maya Rivera"},
	{"deven", "Deven Shah"},
	{"nina", "Nina Kapoor"},
	{"omar", "Omar Haddad"},
}

const seedPassword = "password123"

func main() {
	reset := flag.Bool("reset", false,
		"delete existing challenges and responses before inserting")
	confirm := flag.Bool("yes-delete-existing-content", false,
		"required alongside -reset; deleting content cannot be undone")
	dryRun := flag.Bool("dry-run", false,
		"run every statement, report what would change, then roll back")
	flag.Parse()

	// -dry-run rolls back, so it cannot destroy anything and does not need
	// the delete confirmation.
	if *reset && !*confirm && !*dryRun {
		log.Fatal("-reset deletes every challenge and response and cannot be undone.\n" +
			"Preview it first — this changes nothing:\n" +
			"  go run ./cmd/seed -reset -dry-run\n" +
			"Then, if the numbers look right:\n" +
			"  go run ./cmd/seed -reset -yes-delete-existing-content")
	}

	connStr := os.Getenv("DATABASE_URL")
	if connStr == "" {
		log.Fatal("DATABASE_URL environment variable is required")
	}
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatalf("open database: %v", err)
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		log.Fatalf("connect to database: %v", err)
	}

	// One transaction: a half-seeded feed (content deleted, nothing inserted)
	// is worse than either end state, so this is all-or-nothing.
	tx, err := db.Begin()
	if err != nil {
		log.Fatalf("begin: %v", err)
	}
	defer tx.Rollback()

	if *dryRun {
		log.Print("DRY RUN — every statement below really executes, then the whole " +
			"transaction is rolled back. Nothing is kept.")
	}

	// Report the starting state before touching anything, so the operator can
	// see the scale of what -reset is about to remove rather than trusting it.
	var beforeChallenges, beforeResponses, beforeUsers int
	if err := tx.QueryRow(`SELECT
			(SELECT count(*) FROM challenges),
			(SELECT count(*) FROM challenge_responses),
			(SELECT count(*) FROM users)`).
		Scan(&beforeChallenges, &beforeResponses, &beforeUsers); err != nil {
		log.Fatalf("read current counts (does the schema exist?): %v", err)
	}
	log.Printf("before: %d challenges, %d responses, %d users",
		beforeChallenges, beforeResponses, beforeUsers)

	if *reset {
		// challenge_responses, likes, dislikes and visibility rows all cascade
		// from challenges, so deleting the parent is enough.
		res, err := tx.Exec(`DELETE FROM challenges`)
		if err != nil {
			log.Fatalf("delete challenges: %v", err)
		}
		n, _ := res.RowsAffected()
		log.Printf("deleted %d existing challenges (responses/likes cascaded)", n)
	}

	userIDs := make([]int, 0, len(users))
	var created, existing []string
	for _, u := range users {
		id, isNew, err := upsertUser(tx, u)
		if err != nil {
			log.Fatalf("upsert user %s: %v", u.username, err)
		}
		userIDs = append(userIDs, id)
		if isNew {
			created = append(created, u.username)
		} else {
			existing = append(existing, u.username)
		}
	}
	// Report these separately. Pre-existing accounts keep whatever password
	// they already had — this tool will not lock someone out of an account
	// they are actively using — so claiming they all share the seed password
	// would simply be untrue.
	if len(created) > 0 {
		log.Printf("created %d new users (password %q): %v",
			len(created), seedPassword, created)
	}
	if len(existing) > 0 {
		log.Printf("reused %d existing users, passwords UNCHANGED: %v",
			len(existing), existing)
	}

	// Guard the one property that actually decides whether the feed feels
	// fast. Cheap to check, and the failure it prevents is the exact one
	// this seed exists to undo.
	for _, c := range clips {
		if c.bytes > maxReelBytes {
			log.Fatalf("clip %s is %.1f MB — a reel is seconds long; the feed "+
				"this seed replaces was slow precisely because it carried "+
				"files this size", c.url, float64(c.bytes)/(1024*1024))
		}
	}

	// AGE AND KIND ARE DELIBERATELY DECORRELATED.
	//
	// The seed this replaces stamped every response-less challenge with the
	// newest timestamp and every battle with an older one. Any feed that
	// weighs freshness — all of them do — then served every short before
	// every battle, and the app looked like it had no battles in it at all.
	//
	// So: ages descend smoothly across the whole run, and whether a
	// challenge gets a response is decided by an independent 3-of-5 cycle.
	// Battles and shorts end up evenly mixed through the timeline, which is
	// what real usage looks like and what the ranker is entitled to assume.
	inserted, battles := 0, 0
	for i := 0; i < seedChallenges; i++ {
		c := clips[i%len(clips)]
		if i >= len(clips) {
			// Second pass over the pool: same video, different prompt, so a
			// feed page is comfortably filled without hunting for another
			// dozen public clips that are all still reel-sized.
			c.title = altTitles[i%len(altTitles)]
		}
		creator := userIDs[i%len(userIDs)]
		// Spread over ~12 days, newest first, with hour-level jitter so no
		// two challenges share a timestamp. The span is kept inside the
		// Following tab's 14-day window on purpose — content older than
		// that is invisible there, and a seed whose back half cannot
		// appear in a tab is a seed that cannot test it.
		age := time.Duration(i)*10*time.Hour + time.Duration(i%7)*time.Hour
		// Views and likes are what the ranker actually reads: it orders by
		// (views + likes*3) and its strict quality tier needs views >= 10
		// and likes >= 1. Seeded flat at zero, every candidate query falls
		// through all four tiers to the "no minimum" last resort and the
		// ranking has no signal to work with — the feed degrades to plain
		// reverse-chronological and nothing about the algorithm is being
		// exercised. These spreads are arbitrary but deterministic, and
		// deliberately uncorrelated with age and with kind.
		views := 400 + (i*137)%4200
		chID, err := insertChallenge(tx, creator, c, age, views)
		if err != nil {
			log.Fatalf("insert challenge %s: %v", c.url, err)
		}
		inserted++

		likes := (i * 3) % 6
		for k := 0; k < likes && k < len(userIDs); k++ {
			liker := userIDs[(i+k+1)%len(userIDs)]
			if _, err := tx.Exec(`
				INSERT INTO challenge_likes (challenge_id, user_id)
				VALUES ($1,$2) ON CONFLICT DO NOTHING`, chID, liker); err != nil {
				log.Fatalf("insert like for challenge %d: %v", chID, err)
			}
		}

		// 3 in every 5 get a response. A feed of nothing but battles is as
		// unrealistic as a feed of nothing but shorts, and the spacing pass
		// on the server needs both kinds present to have anything to do.
		if i%5 >= 3 {
			continue
		}
		respClip := clips[(i+3)%len(clips)]
		responder := userIDs[(i+1)%len(userIDs)]
		// Responses land after their challenge, never before it.
		if err := insertResponse(tx, chID, responder, respClip, age/2); err != nil {
			log.Fatalf("insert response for challenge %d: %v", chID, err)
		}
		battles++
	}

	// Read the end state from inside the transaction. On a dry run this is the
	// only place the result is ever visible, since the rollback below discards
	// it — but every INSERT really ran, so a column mismatch or constraint
	// violation surfaces here rather than during the real thing.
	var afterChallenges, afterResponses, afterUsers int
	if err := tx.QueryRow(`SELECT
			(SELECT count(*) FROM challenges),
			(SELECT count(*) FROM challenge_responses),
			(SELECT count(*) FROM users)`).
		Scan(&afterChallenges, &afterResponses, &afterUsers); err != nil {
		log.Fatalf("read resulting counts: %v", err)
	}
	log.Printf("after:  %d challenges, %d responses, %d users",
		afterChallenges, afterResponses, afterUsers)

	if *dryRun {
		if err := tx.Rollback(); err != nil {
			log.Fatalf("rollback: %v", err)
		}
		log.Printf("DRY RUN COMPLETE — rolled back, database unchanged.")
		log.Printf("every statement executed cleanly against the real schema; "+
			"re-run with -reset -yes-delete-existing-content to keep %d challenges",
			inserted)
		return
	}

	if err := tx.Commit(); err != nil {
		log.Fatalf("commit: %v", err)
	}
	log.Printf("seeded %d challenges (%d battles, %d shorts)",
		inserted, battles, inserted-battles)
}

// upsertUser creates the account if missing and returns its id either way, so
// re-running the seed does not fail on the username unique constraint or
// clobber a password someone is already using.
func upsertUser(tx *sql.Tx, u seedUser) (id int, created bool, err error) {
	err = tx.QueryRow(`SELECT id FROM users WHERE username = $1`, u.username).Scan(&id)
	if err == nil {
		return id, false, nil
	}
	if err != sql.ErrNoRows {
		return 0, false, err
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(seedPassword), bcrypt.DefaultCost)
	if err != nil {
		return 0, false, err
	}
	// The hash goes in password_hash, NOT password. IsValidUser reads
	// password_hash first and only falls back to `password` as *plaintext*
	// (the pre-bcrypt legacy path). Writing a hash into `password` therefore
	// compares the literal "$2a$10$..." string against what the user types, so
	// the account exists but can never be logged into — an earlier version of
	// this seed did exactly that. `password` is NOT NULL, so it gets ''.
	err = tx.QueryRow(
		`INSERT INTO users (username, password, password_hash, full_name)
		 VALUES ($1,'',$2,$3) RETURNING id`,
		u.username, string(hash), u.fullName,
	).Scan(&id)
	return id, true, err
}

func insertChallenge(tx *sql.Tx, creatorID int, c clip, age time.Duration, views int) (int, error) {
	emotions, _ := json.Marshal(c.emotions)
	var id int
	// video_variants is left empty on purpose: these clips have no transcoded
	// ladder, so the client falls back to video_url. Every clip is already
	// 720p or smaller, which is the ceiling the feed wants anyway.
	err := tx.QueryRow(`
		INSERT INTO challenges
			(creator_id, video_url, thumbnail_url, prefix, subject,
			 visibility, status, category, emotion_tags, energy_level,
			 created_at, views)
		VALUES ($1,$2,$3,$4,$5,'arena','open',$6,$7,$8,$9,$10)
		RETURNING id`,
		creatorID, c.videoURL(), c.thumbURL(), "I challenge you to", c.title,
		c.category, string(emotions), c.energy, time.Now().Add(-age), views,
	).Scan(&id)
	return id, err
}

func insertResponse(tx *sql.Tx, challengeID, responderID int, c clip, age time.Duration) error {
	_, err := tx.Exec(`
		INSERT INTO challenge_responses
			(challenge_id, responder_id, video_url, thumbnail_url, created_at)
		VALUES ($1,$2,$3,$4,$5)`,
		challengeID, responderID, c.videoURL(), c.thumbURL(), time.Now().Add(-age),
	)
	return err
}
