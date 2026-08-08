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
// Usage:
//
//	export DATABASE_URL='postgres://...'
//	go run ./cmd/seed -reset -yes-delete-existing-content
//	go run ./cmd/seed                     # insert only, keep what's there
package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"

	_ "github.com/lib/pq"
	"golang.org/x/crypto/bcrypt"
)

// sampleBase is Google's long-standing public test bucket. Range-request
// capable and not rate-limited the way r2.dev is.
const sampleBase = "https://commondatastorage.googleapis.com/gtv-videos-bucket/sample"

// clip is one sample video. `short` marks the ~15-60s clips; the open movies
// run ~10 minutes, which is wrong for a reels feed, so they are used sparingly
// and only as challenge *responses* where length matters less.
type clip struct {
	file     string
	title    string
	category string
	energy   string
	emotions []string
	short    bool
}

// The ForBigger* clips are 1280x720 and ~15s — the closest thing in this set to
// a real reel, so they carry the feed. The vehicle clips are ~1 minute and add
// variety without dragging. Sintel/TearsOfSteel are 1080p+ and minutes long;
// they are deliberately excluded from challenge slots so the feed is not
// dominated by content that decodes above the 720p ceiling.
var clips = []clip{
	{"ForBiggerBlazes.mp4", "Can you top this entrance?", "comedy", "high", []string{"funny", "surprise"}, true},
	{"ForBiggerEscapes.mp4", "Best escape move wins", "sports", "high", []string{"excited"}, true},
	{"ForBiggerFun.mp4", "Show me your happy place", "lifestyle", "medium", []string{"happy"}, true},
	{"ForBiggerJoyrides.mp4", "Dream ride challenge", "lifestyle", "high", []string{"excited", "happy"}, true},
	{"ForBiggerMeltdowns.mp4", "Funniest meltdown reaction", "comedy", "high", []string{"funny"}, true},
	{"VolkswagenGTIReview.mp4", "Review your first car in 30s", "tech", "medium", []string{"curious"}, true},
	{"SubaruOutbackOnStreetAndDirt.mp4", "Street or dirt — pick a side", "sports", "high", []string{"excited"}, true},
	{"WeAreGoingOnBullrun.mp4", "Road trip story time", "story", "medium", []string{"happy"}, true},
	{"WhatCarCanYouGetForAGrand.mp4", "Best budget find challenge", "education", "low", []string{"curious"}, true},
	{"BigBuckBunny.mp4", "Animation appreciation", "art", "low", []string{"calm"}, false},
	{"ElephantsDream.mp4", "Surreal scene remake", "art", "medium", []string{"curious"}, false},
	{"TearsOfSteel.mp4", "Sci-fi one-shot challenge", "story", "medium", []string{"excited"}, false},
}

func (c clip) videoURL() string { return fmt.Sprintf("%s/%s", sampleBase, c.file) }

// thumbURL points at the matching still Google publishes alongside each clip.
func (c clip) thumbURL() string {
	name := c.file[:len(c.file)-len(".mp4")]
	return fmt.Sprintf("https://storage.googleapis.com/gtv-videos-bucket/sample/images/%s.jpg", name)
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
	flag.Parse()

	if *reset && !*confirm {
		log.Fatal("-reset deletes every challenge and response and cannot be undone.\n" +
			"Re-run with both flags if that is what you want:\n" +
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
	for _, u := range users {
		id, err := upsertUser(tx, u)
		if err != nil {
			log.Fatalf("upsert user %s: %v", u.username, err)
		}
		userIDs = append(userIDs, id)
	}
	log.Printf("ensured %d demo users (password %q)", len(userIDs), seedPassword)

	// Only the short clips become challenges — a 10-minute open movie at the
	// top of a reels feed is not a useful test of anything.
	var challengeClips, responseClips []clip
	for _, c := range clips {
		if c.short {
			challengeClips = append(challengeClips, c)
		} else {
			responseClips = append(responseClips, c)
		}
	}

	inserted := 0
	for i, c := range challengeClips {
		creator := userIDs[i%len(userIDs)]
		chID, err := insertChallenge(tx, creator, c)
		if err != nil {
			log.Fatalf("insert challenge %s: %v", c.file, err)
		}
		inserted++

		// Every challenge gets exactly one response. The battle view swipes
		// between challenger and responder, so a challenge with no response
		// exercises only half the UI — and the 3D cube not at all.
		respClip := challengeClips[(i+1)%len(challengeClips)]
		if i%4 == 3 && len(responseClips) > 0 {
			respClip = responseClips[i%len(responseClips)]
		}
		responder := userIDs[(i+1)%len(userIDs)]
		if err := insertResponse(tx, chID, responder, respClip); err != nil {
			log.Fatalf("insert response for challenge %d: %v", chID, err)
		}
	}

	if err := tx.Commit(); err != nil {
		log.Fatalf("commit: %v", err)
	}
	log.Printf("seeded %d challenges, each with one response", inserted)
	log.Printf("log in as any of: player1 … omar / %s", seedPassword)
}

// upsertUser creates the account if missing and returns its id either way, so
// re-running the seed does not fail on the username unique constraint or
// clobber a password someone is already using.
func upsertUser(tx *sql.Tx, u seedUser) (int, error) {
	var id int
	err := tx.QueryRow(`SELECT id FROM users WHERE username = $1`, u.username).Scan(&id)
	if err == nil {
		return id, nil
	}
	if err != sql.ErrNoRows {
		return 0, err
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(seedPassword), bcrypt.DefaultCost)
	if err != nil {
		return 0, err
	}
	err = tx.QueryRow(
		`INSERT INTO users (username, password, full_name) VALUES ($1,$2,$3) RETURNING id`,
		u.username, string(hash), u.fullName,
	).Scan(&id)
	return id, err
}

func insertChallenge(tx *sql.Tx, creatorID int, c clip) (int, error) {
	emotions, _ := json.Marshal(c.emotions)
	var id int
	// video_variants is left empty on purpose: these clips have no transcoded
	// ladder, so the client falls back to video_url. The ForBigger* clips are
	// already 720p, which is the ceiling the feed wants anyway.
	err := tx.QueryRow(`
		INSERT INTO challenges
			(creator_id, video_url, thumbnail_url, prefix, subject,
			 visibility, status, category, emotion_tags, energy_level)
		VALUES ($1,$2,$3,$4,$5,'arena','open',$6,$7,$8)
		RETURNING id`,
		creatorID, c.videoURL(), c.thumbURL(), "I challenge you to", c.title,
		c.category, string(emotions), c.energy,
	).Scan(&id)
	return id, err
}

func insertResponse(tx *sql.Tx, challengeID, responderID int, c clip) error {
	_, err := tx.Exec(`
		INSERT INTO challenge_responses
			(challenge_id, responder_id, video_url, thumbnail_url)
		VALUES ($1,$2,$3,$4)`,
		challengeID, responderID, c.videoURL(), c.thumbURL(),
	)
	return err
}
