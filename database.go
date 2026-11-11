package main

import (
	"crypto/subtle"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	_ "github.com/lib/pq"
)

// db is the PostgreSQL connection pool shared across the application.
var db *sql.DB

// ---------------------------------------------------------------------------------
// Initialisation
// ---------------------------------------------------------------------------------

// InitDatabase connects to PostgreSQL and runs schema migrations. A new
// database starts empty: there is no demo data any more (demo_accounts.go).
// Returns an error rather than exiting. The caller retries and, while it is
// failing, the app says so on /health instead of disappearing — see
// boot_gate.go for why that distinction cost four days of silent downtime.
func InitDatabase() error {
	connStr := os.Getenv("DATABASE_URL")
	if connStr == "" {
		return errors.New("DATABASE_URL is not set")
	}

	var err error
	db, err = sql.Open("postgres", connStr)
	if err != nil {
		return fmt.Errorf("cannot open database: %w", err)
	}

	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	if err = db.Ping(); err != nil {
		return fmt.Errorf("cannot reach database: %w", err)
	}
	log.Println("Connected to PostgreSQL")

	if err := runMigrations(); err != nil {
		return err
	}
	return nil
}

// runMigrations creates all required tables idempotently.

// alterStmts adds columns to existing tables safely. Package-level and
// not a local so its contents can be asserted without standing up a
// database — see hls_amnesty_test.go, which pins the HLS attempt-amnesty
// statements that were silently defeating the transcode retry cap.
const alterStmts = `
	DO $$ BEGIN ALTER TABLE chat_messages ADD COLUMN status VARCHAR(20) DEFAULT 'sent'; EXCEPTION WHEN duplicate_column THEN NULL; END $$;
	DO $$ BEGIN ALTER TABLE chat_messages ADD COLUMN reply_to_id INT REFERENCES chat_messages(id) ON DELETE SET NULL; EXCEPTION WHEN duplicate_column THEN NULL; END $$;
	DO $$ BEGIN ALTER TABLE chat_messages ADD COLUMN is_edited BOOLEAN DEFAULT FALSE; EXCEPTION WHEN duplicate_column THEN NULL; END $$;
	DO $$ BEGIN ALTER TABLE chat_messages ADD COLUMN is_deleted BOOLEAN DEFAULT FALSE; EXCEPTION WHEN duplicate_column THEN NULL; END $$;
	DO $$ BEGIN ALTER TABLE chat_messages ADD COLUMN edited_at TIMESTAMPTZ; EXCEPTION WHEN duplicate_column THEN NULL; END $$;
	-- Photo and voice messages (chat_media.go): what kind a message is, and
	-- its file. A photo's caption and every text message stay in message.
	ALTER TABLE chat_messages ADD COLUMN IF NOT EXISTS kind VARCHAR(10) DEFAULT 'text';
	ALTER TABLE chat_messages ADD COLUMN IF NOT EXISTS media_url TEXT;
	ALTER TABLE chat_messages ADD COLUMN IF NOT EXISTS media_duration_ms INT;
	ALTER TABLE chat_messages ADD COLUMN IF NOT EXISTS media_width INT;
	ALTER TABLE chat_messages ADD COLUMN IF NOT EXISTS media_height INT;
	ALTER TABLE chat_messages ADD COLUMN IF NOT EXISTS media_waveform TEXT;
	-- What a post is made of: 'video', or 'photo' (photo_posts.go). An
	-- answer is always the same kind as its challenge.
	ALTER TABLE challenges          ADD COLUMN IF NOT EXISTS media_type TEXT NOT NULL DEFAULT 'video';
	ALTER TABLE challenge_responses ADD COLUMN IF NOT EXISTS media_type TEXT NOT NULL DEFAULT 'video';
	-- A shared battle or short (kind 'share'): which video, and for a
	-- battle which side (chat_share.go).
	ALTER TABLE chat_messages ADD COLUMN IF NOT EXISTS shared_challenge_id INT;
	ALTER TABLE chat_messages ADD COLUMN IF NOT EXISTS shared_response_id INT;
	DO $$ BEGIN ALTER TABLE users ADD COLUMN last_seen TIMESTAMPTZ DEFAULT NOW(); EXCEPTION WHEN duplicate_column THEN NULL; END $$;
	DO $$ BEGIN ALTER TABLE users ADD COLUMN password_hash VARCHAR(255) NOT NULL DEFAULT ''; EXCEPTION WHEN duplicate_column THEN NULL; END $$;
	DO $$ BEGIN ALTER TABLE challenges ADD COLUMN category VARCHAR(30) DEFAULT 'other'; EXCEPTION WHEN duplicate_column THEN NULL; END $$;
	DO $$ BEGIN ALTER TABLE challenges ADD COLUMN emotion_tags JSONB DEFAULT '[]'; EXCEPTION WHEN duplicate_column THEN NULL; END $$;
	DO $$ BEGIN ALTER TABLE challenges ADD COLUMN energy_level VARCHAR(10) DEFAULT 'medium'; EXCEPTION WHEN duplicate_column THEN NULL; END $$;
	DO $$ BEGIN ALTER TABLE posts ADD COLUMN category VARCHAR(30) DEFAULT 'other'; EXCEPTION WHEN duplicate_column THEN NULL; END $$;
	DO $$ BEGIN ALTER TABLE posts ADD COLUMN emotion_tags JSONB DEFAULT '[]'; EXCEPTION WHEN duplicate_column THEN NULL; END $$;
	DO $$ BEGIN ALTER TABLE posts ADD COLUMN energy_level VARCHAR(10) DEFAULT 'medium'; EXCEPTION WHEN duplicate_column THEN NULL; END $$;
	DO $$ BEGIN ALTER TABLE challenges ADD COLUMN custom_tags JSONB DEFAULT '[]'; EXCEPTION WHEN duplicate_column THEN NULL; END $$;
	-- Who said what this video is, kept apart from the app's best answer.
	--
	-- category is the answer. These three say where it came from, because one
	-- column holding "the creator picked this" and "we guessed this off the
	-- title" is how a guess this server made ends up being weighed against
	-- the model as though a person had made a claim.
	--
	--   creator_category  ONLY a human's pick. '' = they did not pick.
	--   machine_category  what the model concluded. '' = it had no opinion.
	--   category_source   agreed | machine | creator | guess | '' (pre-dates this)
	--
	-- Mirrored from migrations/010 so a database built from scratch by this
	-- function has them too. A check that reads only one of the two systems
	-- is blind to half the schema — see migrations/README.md.
	DO $$ BEGIN ALTER TABLE challenges ADD COLUMN creator_category VARCHAR(30) NOT NULL DEFAULT ''; EXCEPTION WHEN duplicate_column THEN NULL; END $$;
	DO $$ BEGIN ALTER TABLE challenges ADD COLUMN machine_category VARCHAR(30) NOT NULL DEFAULT ''; EXCEPTION WHEN duplicate_column THEN NULL; END $$;
	DO $$ BEGIN ALTER TABLE challenges ADD COLUMN category_source  VARCHAR(16) NOT NULL DEFAULT ''; EXCEPTION WHEN duplicate_column THEN NULL; END $$;
	DO $$ BEGIN ALTER TABLE challenge_responses ADD COLUMN creator_category VARCHAR(30) NOT NULL DEFAULT ''; EXCEPTION WHEN duplicate_column THEN NULL; END $$;
	DO $$ BEGIN ALTER TABLE challenge_responses ADD COLUMN machine_category VARCHAR(30) NOT NULL DEFAULT ''; EXCEPTION WHEN duplicate_column THEN NULL; END $$;
	DO $$ BEGIN ALTER TABLE challenge_responses ADD COLUMN category_source  VARCHAR(16) NOT NULL DEFAULT ''; EXCEPTION WHEN duplicate_column THEN NULL; END $$;
	-- Suggestions the creator has turned down. Kept so the same ones are not
	-- offered again every time they open their own video — see
	-- tag_suggestions.go. Separate from custom_tags because "no" and "not yet
	-- asked" are different answers, and only one of them should stay quiet.
	DO $$ BEGIN ALTER TABLE challenges ADD COLUMN dismissed_tags JSONB DEFAULT '[]'; EXCEPTION WHEN duplicate_column THEN NULL; END $$;
	DO $$ BEGIN ALTER TABLE posts ADD COLUMN custom_tags JSONB DEFAULT '[]'; EXCEPTION WHEN duplicate_column THEN NULL; END $$;

	-- Multi-bitrate video variants. Maps quality label → CDN URL.
	-- video_url stays as the canonical/default URL so legacy readers keep
	-- working; video_variants is the new path for adaptive playback.
	DO $$ BEGIN ALTER TABLE challenges          ADD COLUMN video_variants JSONB DEFAULT '{}'; EXCEPTION WHEN duplicate_column THEN NULL; END $$;
	DO $$ BEGIN ALTER TABLE challenge_responses ADD COLUMN video_variants JSONB DEFAULT '{}'; EXCEPTION WHEN duplicate_column THEN NULL; END $$;
	-- Every rendition the worker settled the last time it converted this
	-- video: made, or deliberately not made for it (no 720p copy of a 480p
	-- video, no H.265 copy that would be no smaller). NULL means converted
	-- before this was recorded. The "missing" backfill reads it so it can
	-- stop offering a video a rendition it has already been considered for;
	-- see storeLadder and media_requeue.go.
	DO $$ BEGIN ALTER TABLE challenges          ADD COLUMN hls_ladder JSONB; EXCEPTION WHEN duplicate_column THEN NULL; END $$;
	DO $$ BEGIN ALTER TABLE challenge_responses ADD COLUMN hls_ladder JSONB; EXCEPTION WHEN duplicate_column THEN NULL; END $$;

	-- HLS master manifest URL produced by the background transcode worker.
	-- State machine: '' = untranscoded, 'PENDING' = claimed by a worker,
	-- anything else = the public master.m3u8 URL. Clients fall back to
	-- video_url / video_variants until the worker reports completion via
	-- POST /api/v1/internal/hls/complete.
	--
	-- Stored as TEXT (not VARCHAR(N)) because R2 + custom-domain URLs can
	-- be long once query params for cache-busting land. Default '' keeps
	-- the column safe to NOT NULL filter on without a separate IS NULL leg.
	DO $$ BEGIN ALTER TABLE challenges          ADD COLUMN hls_manifest_url TEXT DEFAULT ''; EXCEPTION WHEN duplicate_column THEN NULL; END $$;
	DO $$ BEGIN ALTER TABLE challenge_responses ADD COLUMN hls_manifest_url TEXT DEFAULT ''; EXCEPTION WHEN duplicate_column THEN NULL; END $$;
	-- HLS queue bookkeeping: hls_attempts caps how many times a broken
	-- source gets claimed (incremented at claim in HLSNextPendingHandler),
	-- hls_claimed_at lets startHLSReaper reset rows a crashed worker left
	-- stuck at 'PENDING'.
	DO $$ BEGIN ALTER TABLE challenges          ADD COLUMN hls_attempts INT DEFAULT 0; EXCEPTION WHEN duplicate_column THEN NULL; END $$;
	DO $$ BEGIN ALTER TABLE challenge_responses ADD COLUMN hls_attempts INT DEFAULT 0; EXCEPTION WHEN duplicate_column THEN NULL; END $$;
	DO $$ BEGIN ALTER TABLE challenges          ADD COLUMN hls_claimed_at TIMESTAMPTZ; EXCEPTION WHEN duplicate_column THEN NULL; END $$;
	DO $$ BEGIN ALTER TABLE challenge_responses ADD COLUMN hls_claimed_at TIMESTAMPTZ; EXCEPTION WHEN duplicate_column THEN NULL; END $$;
	-- Partial index so the transcode worker's "find next untranscoded
	-- challenge" query is a fast index scan instead of a sequential one.
	-- Filtering on the empty string is functionally the same as IS NULL
	-- for our purposes and matches the DEFAULT above.
	DO $$ BEGIN
		CREATE INDEX IF NOT EXISTS challenges_pending_hls_idx
			ON challenges (created_at)
			WHERE hls_manifest_url = '';
	EXCEPTION WHEN duplicate_table THEN NULL; END $$;
	-- Same queue index for the battle-response leg of the pipeline
	-- (responses joined the transcode queue when kind-aware claiming
	-- shipped).
	DO $$ BEGIN
		CREATE INDEX IF NOT EXISTS responses_pending_hls_idx
			ON challenge_responses (created_at)
			WHERE hls_manifest_url = '';
	EXCEPTION WHEN duplicate_table THEN NULL; END $$;

	-- Attempt amnesty: untranscoded rows that burned all their claim
	-- attempts eventually get a fresh budget. The cap exists to stop
	-- INFINITE retry loops (corrupt source, dead worker); infra failures
	-- (e.g. the first Actions run lacked ffmpeg and instantly exhausted
	-- every row's attempts) should not permanently strand a video.
	--
	-- THE 24-HOUR GATE IS THE WHOLE POINT. This block used to be
	-- unconditional, on the reasoning that it runs once per DEPLOY, so
	-- the worst case was "5 quick failures per deploy — bounded and
	-- cheap". It does not run once per deploy. It runs once per BOOT,
	-- and on Render's free tier the service sleeps after inactivity and
	-- cold-starts many times a day. The cap was therefore being cleared
	-- continuously and never actually capped anything.
	--
	-- Observed in production: the transcode worker re-claimed the same
	-- six permanently-broken rows (all pointing at a hotlink-blocked
	-- source that answers 403) on run after run, burning its whole
	-- allocation on jobs that can never succeed and hammering this
	-- backend hard enough to be 429ed by its own rate limiter.
	--
	-- Gating on hls_claimed_at turns "5 failures per cold start" into
	-- "5 failures per day", which keeps the recovery property that
	-- motivated the amnesty: a row stranded by an infra blip still frees
	-- itself automatically, just within a day rather than within minutes.
	UPDATE challenges          SET hls_attempts = 0
	 WHERE hls_manifest_url = '' AND hls_attempts >= 5
	   AND (hls_claimed_at IS NULL OR hls_claimed_at < NOW() - INTERVAL '24 hours');
	UPDATE challenge_responses SET hls_attempts = 0
	 WHERE hls_manifest_url = '' AND hls_attempts >= 5
	   AND (hls_claimed_at IS NULL OR hls_claimed_at < NOW() - INTERVAL '24 hours');

	-- NOTE: the response_count heal used to sit here, and it broke every
	-- fresh database. It reads challenges.response_count, which is not
	-- created until the denorm block far below, so on a database that did
	-- not already have that column the statement failed. lib/pq sends this
	-- whole string as ONE simple query, which Postgres runs in a single
	-- implicit transaction — so that one failure rolled back every other
	-- statement in this block: category, emotion_tags, energy_level,
	-- video_variants, password_hash, challenge_response_flags and the rest.
	-- The only symptom was a "Warning: alter table issue" line, after which
	-- boot continued happily against a half-built schema.
	--
	-- The denorm block already performs the identical heal, after creating
	-- the column, so the fix is to not duplicate it here. Anything added to
	-- this block must only reference columns that already exist by now.

	-- Extended personality dimensions on user_profiles
	DO $$ BEGIN ALTER TABLE user_profiles ADD COLUMN attention_span REAL DEFAULT 0.5; EXCEPTION WHEN duplicate_column THEN NULL; END $$;
	DO $$ BEGIN ALTER TABLE user_profiles ADD COLUMN binge_intensity REAL DEFAULT 0.5; EXCEPTION WHEN duplicate_column THEN NULL; END $$;
	DO $$ BEGIN ALTER TABLE user_profiles ADD COLUMN creator_loyalty REAL DEFAULT 0.5; EXCEPTION WHEN duplicate_column THEN NULL; END $$;
	DO $$ BEGIN ALTER TABLE user_profiles ADD COLUMN competitiveness_index REAL DEFAULT 0.5; EXCEPTION WHEN duplicate_column THEN NULL; END $$;
	DO $$ BEGIN ALTER TABLE user_profiles ADD COLUMN mood_volatility REAL DEFAULT 0.5; EXCEPTION WHEN duplicate_column THEN NULL; END $$;
	DO $$ BEGIN ALTER TABLE user_profiles ADD COLUMN strategy_success_history JSONB DEFAULT '{}'; EXCEPTION WHEN duplicate_column THEN NULL; END $$;

	-- Challenge response validation + community-moderation columns
	DO $$ BEGIN ALTER TABLE challenge_responses ADD COLUMN duration_ms INT DEFAULT 0; EXCEPTION WHEN duplicate_column THEN NULL; END $$;
	DO $$ BEGIN ALTER TABLE challenge_responses ADD COLUMN caption TEXT DEFAULT ''; EXCEPTION WHEN duplicate_column THEN NULL; END $$;
	DO $$ BEGIN ALTER TABLE challenge_responses ADD COLUMN relevance_score REAL DEFAULT 0.5; EXCEPTION WHEN duplicate_column THEN NULL; END $$;
	DO $$ BEGIN ALTER TABLE challenge_responses ADD COLUMN off_topic_flags INT DEFAULT 0; EXCEPTION WHEN duplicate_column THEN NULL; END $$;
	DO $$ BEGIN ALTER TABLE challenge_responses ADD COLUMN is_hidden BOOLEAN DEFAULT FALSE; EXCEPTION WHEN duplicate_column THEN NULL; END $$;

	-- Challenge response off-topic flagging table (community moderation)
	CREATE TABLE IF NOT EXISTS challenge_response_flags (
		response_id INT NOT NULL REFERENCES challenge_responses(id) ON DELETE CASCADE,
		user_id     INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
		reason      VARCHAR(40) NOT NULL DEFAULT 'off_topic',
		created_at  TIMESTAMPTZ DEFAULT NOW(),
		PRIMARY KEY (response_id, user_id)
	);

	-- Profile bio + user-level settings (theme, language, etc.).
	-- bio gets a dedicated column because it's shown on every profile
	-- view; settings is JSONB so we can ship new toggles without a
	-- migration per feature. Keep auth/security state OUT of settings —
	-- TOTP secrets live in their own table for least-privilege access.
	DO $$ BEGIN ALTER TABLE users ADD COLUMN bio TEXT DEFAULT ''; EXCEPTION WHEN duplicate_column THEN NULL; END $$;
	DO $$ BEGIN ALTER TABLE users ADD COLUMN settings JSONB DEFAULT '{}'; EXCEPTION WHEN duplicate_column THEN NULL; END $$;
	-- Account visibility: 'public' (default, anyone can see) | 'friends'
	-- (only followers see your posts and profile detail). Stored as a
	-- column so the feed-time WHERE clause can filter by index lookup
	-- without parsing JSON.
	DO $$ BEGIN ALTER TABLE users ADD COLUMN visibility VARCHAR(20) DEFAULT 'public'; EXCEPTION WHEN duplicate_column THEN NULL; END $$;

	-- user_blocks: A blocks B → A never sees B's content, B can't DM A.
	-- Bidirectional enforcement happens at query-time (filter both
	-- legs). Keeping this as a thin table (just blocker_id, blocked_id)
	-- means add/remove is O(1) and lookup joins against the user table
	-- stay cheap.
	CREATE TABLE IF NOT EXISTS user_blocks (
		blocker_id  INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
		blocked_id  INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
		created_at  TIMESTAMPTZ DEFAULT NOW(),
		PRIMARY KEY (blocker_id, blocked_id),
		CHECK (blocker_id <> blocked_id)
	);
	CREATE INDEX IF NOT EXISTS idx_user_blocks_blocked ON user_blocks(blocked_id);

	-- TOTP-based 2FA. One row per user once they've enrolled. Kept in a
	-- separate table from users so the SELECT * lookups elsewhere never
	-- accidentally leak the secret, and so we can add column-level
	-- revoke later if we move to a hosted secrets manager.
	--
	-- secret: base32-encoded shared secret (the QR-code payload).
	-- recovery_codes: 10 single-use backup codes, stored SHA-256-hashed
	--   so a DB read can't bypass 2FA. (Codes are 80-bit random base32
	--   themselves, so the entropy is in the code, not the hash —
	--   bcrypt would be overkill and adds a dep.)
	-- last_used_at: prevents replaying a freshly-used 6-digit code
	--   within the same 30s window.
	CREATE TABLE IF NOT EXISTS user_totp (
		user_id        INT PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
		secret         TEXT NOT NULL,
		recovery_codes JSONB NOT NULL DEFAULT '[]',
		enrolled_at    TIMESTAMPTZ DEFAULT NOW(),
		last_used_at   TIMESTAMPTZ,
		is_active      BOOLEAN DEFAULT FALSE
	);
	`

func runMigrations() error {
	schema := `
	CREATE TABLE IF NOT EXISTS users (
		id          SERIAL PRIMARY KEY,
		username	VARCHAR(50) UNIQUE NOT NULL,
		password	VARCHAR(255) NOT NULL,
		full_name	VARCHAR(100) DEFAULT '',
		wins		INT DEFAULT 0,
		losses		INT DEFAULT 0,
		league		VARCHAR(20) DEFAULT 'Bronze'
	);

	CREATE TABLE IF NOT EXISTS follows (
		follower_id  INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
		following_id INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
		created_at	 TIMESTAMPTZ DEFAULT NOW(),
		PRIMARY KEY (follower_id, following_id)
	);

	CREATE TABLE IF NOT EXISTS posts (
		id			  SERIAL PRIMARY KEY,
		author_id	  INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
		type		  VARCHAR(10) NOT NULL DEFAULT 'image',
		content_url   TEXT DEFAULT '',
		thumbnail_url TEXT DEFAULT '',
		caption		  TEXT DEFAULT '',
		views		  INT DEFAULT 0,
		created_at    TIMESTAMPTZ DEFAULT NOW()
	);

	CREATE TABLE IF NOT EXISTS post_likes (
		post_id	   INT NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
		user_id	   INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
		created_at TIMESTAMPTZ DEFAULT NOW(),
		PRIMARY KEY (post_id, user_id)
	);

	CREATE TABLE IF NOT EXISTS comments(
		id		   SERIAL PRIMARY KEY,
		post_id	   INT NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
		author_id  INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
		text	   TEXT NOT NULL,
		created_at TIMESTAMPTZ DEFAULT NOW()
	);

	CREATE TABLE IF NOT EXISTS challenges(
		id 		  	  SERIAL PRIMARY KEY,
		creator_id    INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
		video_url     TEXT DEFAULT '',
		thumbnail_url TEXT DEFAULT '',
		prefix 		  VARCHAR(100) NOT NULL DEFAULT '',
		subject       VARCHAR(100) NOT NULL DEFAULT '',
		visibility    VARCHAR(10) NOT NULL DEFAULT 'arena',
		status        VARCHAR(20) NOT NULL DEFAULT 'open',
		views         INT DEFAULT 0,
		created_at    TIMESTAMPTZ DEFAULT NOW()
	);

	CREATE TABLE IF NOT EXISTS challenge_visible_to (
		challenge_id INT NOT NULL REFERENCES challenges(id) ON DELETE CASCADE,
		user_id      INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
		PRIMARY KEY (challenge_id, user_id)
	);

	CREATE TABLE IF NOT EXISTS challenge_responses (
		id            SERIAL PRIMARY KEY,
		challenge_id  INT NOT NULL REFERENCES challenges(id) ON DELETE CASCADE,
		responder_id  INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
		video_url     TEXT DEFAULT '',
		thumbnail_url TEXT DEFAULT '',
		views         INT DEFAULT 0,
		created_at    TIMESTAMPTZ DEFAULT NOW()
	);

	CREATE TABLE IF NOT EXISTS challenge_likes (
		challenge_id  INT NOT NULL REFERENCES challenges(id) ON DELETE CASCADE,
		user_id       INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
		created_at    TIMESTAMPTZ DEFAULT NOW(),
		PRIMARY KEY  (challenge_id, user_id)
	);

	-- Explicit negative engagement on a challenge. Kept in its own table
	-- (rather than a signed column on challenge_likes) so a dislike stays a
	-- first-class, queryable fact — creator insights can report it and the
	-- ranker can join on it without interpreting a magnitude. Mutually
	-- exclusive with a like: ToggleChallengeDislike clears the like row in
	-- the same transaction.
	CREATE TABLE IF NOT EXISTS challenge_dislikes (
		challenge_id  INT NOT NULL REFERENCES challenges(id) ON DELETE CASCADE,
		user_id       INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
		created_at    TIMESTAMPTZ DEFAULT NOW(),
		PRIMARY KEY  (challenge_id, user_id)
	);

	CREATE TABLE IF NOT EXISTS challenge_response_likes (
		response_id   INT NOT NULL REFERENCES challenge_responses(id) ON DELETE CASCADE,
		user_id       INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
		created_at    TIMESTAMPTZ DEFAULT NOW(),
		PRIMARY KEY (response_id, user_id)
	);

	CREATE TABLE IF NOT EXISTS challenge_votes (
		id            SERIAL PRIMARY KEY,
		challenge_id  INT NOT NULL REFERENCES challenges(id) ON DELETE CASCADE,
		response_id   INT NOT NULL REFERENCES challenge_responses(id) ON DELETE CASCADE,
		voter_id      INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
		created_at    TIMESTAMPTZ DEFAULT NOW(),
		UNIQUE (challenge_id, voter_id)
	);

	CREATE TABLE IF NOT EXISTS watch_events (
		id            SERIAL PRIMARY KEY,
		user_id       INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
		content_id    INT NOT NULL,
		content_type  VARCHAR(20) NOT NULL,
		watch_time    INT NOT NULL DEFAULT 0,
		completed     BOOLEAN DEFAULT FALSE,
		created_at    TIMESTAMPTZ DEFAULT NOW()
	);

	CREATE TABLE IF NOT EXISTS reports (
		id            SERIAL PRIMARY KEY,
		reporter_id   INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
		target_id     INT NOT NULL,
		target_type   VARCHAR(20) NOT NULL,
		reason        VARCHAR(100) NOT NULL,
		description   TEXT DEFAULT '',
		status        VARCHAR(20) NOT NULL DEFAULT 'pending',
		created_at    TIMESTAMPTZ DEFAULT NOW()
	);

	CREATE TABLE IF NOT EXISTS chat_messages (
		id            SERIAL PRIMARY KEY,
		sender_id     INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
		receiver_id   INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
		message       TEXT NOT NULL,
		is_read       BOOLEAN DEFAULT FALSE,
		created_at    TIMESTAMPTZ DEFAULT NOW()
	);

	CREATE TABLE IF NOT EXISTS challenge_comments (
		id            SERIAL PRIMARY KEY,
		challenge_id  INT NOT NULL REFERENCES challenges(id) ON DELETE CASCADE,
		author_id     INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
		text          TEXT NOT NULL,
		created_at    TIMESTAMPTZ DEFAULT NOW()
	);

	CREATE TABLE IF NOT EXISTS saved_challenges (
		user_id       INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
		challenge_id  INT NOT NULL REFERENCES challenges(id) ON DELETE CASCADE,
		created_at    TIMESTAMPTZ DEFAULT NOW(),
		PRIMARY KEY (user_id, challenge_id)
	);

	-- ═══════════════════════════════════════════════════════════════
	-- RECOMMENDATION ENGINE TABLES
	-- ═══════════════════════════════════════════════════════════════

	-- feed_events: Every user interaction — the raw fuel for the algorithm.
	-- Captures implicit signals (watch time, skips) and explicit (likes, shares).
	CREATE TABLE IF NOT EXISTS feed_events (
		id               SERIAL PRIMARY KEY,
		user_id          TEXT NOT NULL,
		content_id       TEXT NOT NULL,
		content_type     VARCHAR(20) NOT NULL,
		event_type       VARCHAR(30) NOT NULL,
		watch_duration_ms INT DEFAULT 0,
		total_duration_ms INT DEFAULT 0,
		completion_rate  REAL DEFAULT 0,
		session_id       TEXT NOT NULL DEFAULT '',
		session_position INT DEFAULT 0,
		metadata         JSONB DEFAULT '{}',
		created_at       TIMESTAMPTZ DEFAULT NOW()
	);
	-- Add metadata column if table existed before this migration
	ALTER TABLE feed_events ADD COLUMN IF NOT EXISTS metadata JSONB DEFAULT '{}';

	-- user_profiles: Computed personality model per user.
	-- 5 psychological dimensions + behavioral metrics + ego state.
	CREATE TABLE IF NOT EXISTS user_profiles (
		user_id              TEXT PRIMARY KEY,
		category_affinity    JSONB DEFAULT '{}',
		energy_preference    REAL DEFAULT 0.5,
		social_drive         REAL DEFAULT 0.5,
		novelty_tolerance    REAL DEFAULT 0.5,
		ego_sensitivity      REAL DEFAULT 0.5,
		avg_session_sec      INT DEFAULT 0,
		active_hours         JSONB DEFAULT '[]',
		preferred_creators   JSONB DEFAULT '[]',
		avoided_categories   JSONB DEFAULT '[]',
		avg_completion_rate  REAL DEFAULT 0.5,
		avg_skip_rate        REAL DEFAULT 0,
		total_sessions       INT DEFAULT 0,
		total_watch_time_ms  BIGINT DEFAULT 0,
		recent_wins          INT DEFAULT 0,
		recent_losses        INT DEFAULT 0,
		last_computed_at     TIMESTAMPTZ DEFAULT NOW(),
		event_count          INT DEFAULT 0,
		-- Context-aware preferences: what they like WHEN
		category_by_hour     JSONB DEFAULT '{}',
		category_by_ego      JSONB DEFAULT '{}',
		emotion_preference   JSONB DEFAULT '{}',
		energy_by_hour       JSONB DEFAULT '{}'
	);

	-- Next-day-return labels, one row per closed session (written by the
	-- nightly computeSessionOutcomes job). Not consumed yet — accumulating
	-- for future long-term-value ranking; can't be backfilled later.
	CREATE TABLE IF NOT EXISTS session_outcomes (
		session_id          TEXT PRIMARY KEY,
		user_id             TEXT NOT NULL,
		session_end         TIMESTAMPTZ NOT NULL,
		returned_within_24h BOOLEAN NOT NULL,
		created_at          TIMESTAMPTZ DEFAULT NOW()
	);
	CREATE INDEX IF NOT EXISTS idx_session_outcomes_user ON session_outcomes(user_id, session_end DESC);

	CREATE TABLE IF NOT EXISTS experiments (
		id          TEXT PRIMARY KEY,
		name        TEXT NOT NULL DEFAULT '',
		description TEXT NOT NULL DEFAULT '',
		variants    JSONB NOT NULL DEFAULT '[]',
		active      BOOLEAN NOT NULL DEFAULT false,
		started_at  TIMESTAMPTZ DEFAULT NOW(),
		ended_at    TIMESTAMPTZ
	);

	CREATE TABLE IF NOT EXISTS experiment_exposures (
		id            SERIAL PRIMARY KEY,
		user_id       TEXT NOT NULL,
		experiment_id TEXT NOT NULL,
		variant_id    TEXT NOT NULL,
		session_id    TEXT NOT NULL,
		created_at    TIMESTAMPTZ DEFAULT NOW()
	);

	CREATE TABLE IF NOT EXISTS user_similarities (
		user_id          TEXT NOT NULL,
		similar_user_id  TEXT NOT NULL,
		similarity_score FLOAT NOT NULL,
		computed_at      TIMESTAMPTZ DEFAULT NOW(),
		PRIMARY KEY (user_id, similar_user_id)
	);

	-- Push notifications: device tokens, per-user prefs, outbox queue, sent log.
	CREATE TABLE IF NOT EXISTS device_tokens (
		token        TEXT PRIMARY KEY,
		user_id      TEXT NOT NULL,
		platform     VARCHAR(20) NOT NULL,         -- "fcm" | "apns"
		registered_at TIMESTAMPTZ DEFAULT NOW(),
		last_seen_at TIMESTAMPTZ DEFAULT NOW(),
		active       BOOLEAN DEFAULT TRUE
	);
	CREATE INDEX IF NOT EXISTS idx_device_tokens_user ON device_tokens(user_id) WHERE active;

	CREATE TABLE IF NOT EXISTS notification_prefs (
		user_id           TEXT PRIMARY KEY,
		friend_response   BOOLEAN DEFAULT TRUE,
		ending_soon       BOOLEAN DEFAULT TRUE,
		you_will_love     BOOLEAN DEFAULT TRUE,
		inactive_winback  BOOLEAN DEFAULT TRUE,
		quiet_hours_start INT DEFAULT 22,           -- local hour 0-23
		quiet_hours_end   INT DEFAULT 8,
		max_per_day       INT DEFAULT 4,
		updated_at        TIMESTAMPTZ DEFAULT NOW()
	);
	-- Whether a new message or missed call buzzes the phone.
	ALTER TABLE notification_prefs ADD COLUMN IF NOT EXISTS messages BOOLEAN DEFAULT TRUE;
	-- Whether the app on this phone draws message notifications itself
	-- (with Reply). Off for every phone registered by an older app, which
	-- only shows what Android draws for it.
	ALTER TABLE device_tokens ADD COLUMN IF NOT EXISTS draws_own BOOLEAN DEFAULT FALSE;

	-- The in-app notifications list: what the bell shows. Push messages
	-- go through notification_outbox; these are the rows the app lists,
	-- kept so the page has a history and not only what arrived while it
	-- was open.
	CREATE TABLE IF NOT EXISTS user_notifications (
		id            BIGSERIAL PRIMARY KEY,
		user_id       INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
		kind          VARCHAR(40) NOT NULL,
		actor_id      INT REFERENCES users(id) ON DELETE CASCADE,
		challenge_id  INT REFERENCES challenges(id) ON DELETE CASCADE,
		body          TEXT NOT NULL,
		created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
		read_at       TIMESTAMPTZ
	);
	CREATE INDEX IF NOT EXISTS idx_user_notifications_user
		ON user_notifications (user_id, created_at DESC);

	CREATE TABLE IF NOT EXISTS notification_outbox (
		id              SERIAL PRIMARY KEY,
		user_id         TEXT NOT NULL,
		trigger_kind    VARCHAR(40) NOT NULL,       -- friend_response|ending_soon|you_will_love|inactive_winback
		dedupe_key      VARCHAR(120) NOT NULL,      -- prevents duplicate triggers
		title           TEXT NOT NULL,
		body            TEXT NOT NULL,
		deeplink        TEXT,
		scheduled_at    TIMESTAMPTZ DEFAULT NOW(),
		queued_at       TIMESTAMPTZ DEFAULT NOW(),
		sent_at         TIMESTAMPTZ,
		clicked_at      TIMESTAMPTZ,
		failed_at       TIMESTAMPTZ,
		fail_reason     TEXT,
		status          VARCHAR(20) DEFAULT 'pending', -- pending|sent|clicked|failed|cancelled
		UNIQUE (user_id, dedupe_key)
	);
	CREATE INDEX IF NOT EXISTS idx_notif_outbox_pending ON notification_outbox(scheduled_at)
		WHERE status = 'pending';
	CREATE INDEX IF NOT EXISTS idx_notif_outbox_user ON notification_outbox(user_id, queued_at DESC);

	-- Storage paths waiting to be cleared from R2 after the rows that
	-- pointed at them were deleted. A table rather than a goroutine because
	-- this service restarts often and a deletion lost to a restart is a file
	-- nobody will ever look at again. Drained by startMediaDeleter; see
	-- media_delete.go for why it exists at all.
	CREATE TABLE IF NOT EXISTS pending_media_deletions (
		id            SERIAL PRIMARY KEY,
		object_prefix TEXT NOT NULL UNIQUE,
		attempts      INT NOT NULL DEFAULT 0,
		last_error    TEXT NOT NULL DEFAULT '',
		created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
	);
	CREATE INDEX IF NOT EXISTS idx_pending_media_deletions_attempts
		ON pending_media_deletions (attempts, id);
	`

	if _, err := db.Exec(schema); err != nil {
		return fmt.Errorf("base schema failed: %w", err)
	}

	// Column additions live in the package-level alterStmts.

	if _, err := db.Exec(alterStmts); err != nil {
		log.Printf("Warning: alter table issue: %v", err)
	}

	// Performance indexes
	indexes := `
	CREATE INDEX IF NOT EXISTS idx_posts_author_id ON posts(author_id);
	CREATE INDEX IF NOT EXISTS idx_posts_created_at ON posts(created_at DESC);
	CREATE INDEX IF NOT EXISTS idx_follows_follower_id ON follows(follower_id);
	CREATE INDEX IF NOT EXISTS idx_follows_following_id ON follows(following_id);
	CREATE INDEX IF NOT EXISTS idx_comments_post_id ON comments(post_id);
	CREATE INDEX IF NOT EXISTS idx_post_likes_post_id ON post_likes(post_id);
	CREATE INDEX IF NOT EXISTS idx_challenges_visibility ON challenges(visibility);
	CREATE INDEX IF NOT EXISTS idx_challenges_status ON challenges(status);
	CREATE INDEX IF NOT EXISTS idx_challenges_created_at ON challenges(created_at DESC);
	CREATE INDEX IF NOT EXISTS idx_challenge_responses_challenge_id ON challenge_responses(challenge_id);
	CREATE INDEX IF NOT EXISTS idx_challenge_responses_dedupe ON challenge_responses(responder_id, video_url);
	CREATE INDEX IF NOT EXISTS idx_challenge_responses_responder ON challenge_responses(responder_id);
	CREATE INDEX IF NOT EXISTS idx_challenge_response_flags_response ON challenge_response_flags(response_id);
	CREATE INDEX IF NOT EXISTS idx_challenge_votes_challenge_id ON challenge_votes(challenge_id);
	CREATE INDEX IF NOT EXISTS idx_watch_events_user_id ON watch_events(user_id);
	CREATE INDEX IF NOT EXISTS idx_watch_events_content ON watch_events(content_id, content_type);
	CREATE INDEX IF NOT EXISTS idx_reports_status ON reports(status);
	CREATE INDEX IF NOT EXISTS idx_users_username ON users(username);
	CREATE INDEX IF NOT EXISTS idx_chat_messages_sender ON chat_messages(sender_id, created_at DESC);
	CREATE INDEX IF NOT EXISTS idx_chat_messages_receiver ON chat_messages(receiver_id, created_at DESC);
	CREATE INDEX IF NOT EXISTS idx_chat_messages_pair ON chat_messages(sender_id, receiver_id, created_at DESC);
	CREATE INDEX IF NOT EXISTS idx_challenge_comments_challenge_id ON challenge_comments(challenge_id, created_at ASC);
	CREATE INDEX IF NOT EXISTS idx_saved_challenges_user ON saved_challenges(user_id, created_at DESC);
	CREATE INDEX IF NOT EXISTS idx_feed_events_user ON feed_events(user_id, created_at DESC);
	CREATE INDEX IF NOT EXISTS idx_feed_events_content ON feed_events(content_id, content_type);
	CREATE INDEX IF NOT EXISTS idx_feed_events_session ON feed_events(session_id);
	CREATE INDEX IF NOT EXISTS idx_feed_events_type ON feed_events(event_type);
	-- Composite used by nightly analytics job (event_type + time window scans).
	-- Without this, 50M+ row tables force a seq scan on the 7/14/30d aggregations.
	CREATE INDEX IF NOT EXISTS idx_feed_events_type_time ON feed_events(event_type, created_at DESC);
	-- Supports creator_affinity query: WHERE user_id=X AND event_type IN (...)
	CREATE INDEX IF NOT EXISTS idx_feed_events_user_type_time ON feed_events(user_id, event_type, created_at DESC);
	-- Expression indexes on metadata JSONB — partial so they stay tiny.
	CREATE INDEX IF NOT EXISTS idx_feed_events_upload_type
	  ON feed_events((metadata->>'uploadType'))
	  WHERE event_type IN ('upload_start','upload_step','upload_abandon','upload_complete');
	CREATE INDEX IF NOT EXISTS idx_feed_events_page_name
	  ON feed_events((metadata->>'pageName'))
	  WHERE event_type = 'page_exit';
	CREATE INDEX IF NOT EXISTS idx_feed_events_error_surface
	  ON feed_events((metadata->>'surface'))
	  WHERE event_type = 'error';

	CREATE INDEX IF NOT EXISTS idx_challenges_category ON challenges(category);
	CREATE INDEX IF NOT EXISTS idx_posts_category ON posts(category);
	CREATE INDEX IF NOT EXISTS idx_challenges_created_category ON challenges(created_at DESC, category);
	CREATE INDEX IF NOT EXISTS idx_posts_created_category ON posts(created_at DESC, category);

	CREATE INDEX IF NOT EXISTS idx_experiment_exposures_exp ON experiment_exposures(experiment_id, variant_id);
	CREATE INDEX IF NOT EXISTS idx_experiment_exposures_user ON experiment_exposures(user_id, experiment_id);
	-- Exposure dedup: one row per (user, experiment, session). Pre-index
	-- rows were written once per FEED PAGE, so first collapse historical
	-- duplicates (keep the earliest), then enforce uniqueness — the
	-- logExperimentExposure INSERT relies on ON CONFLICT against this.
	DELETE FROM experiment_exposures a
	 USING experiment_exposures b
	 WHERE a.id > b.id
	   AND a.user_id = b.user_id
	   AND a.experiment_id = b.experiment_id
	   AND a.session_id = b.session_id;
	CREATE UNIQUE INDEX IF NOT EXISTS uniq_experiment_exposures_session
		ON experiment_exposures(user_id, experiment_id, session_id);
	CREATE INDEX IF NOT EXISTS idx_user_similarities_user ON user_similarities(user_id, similarity_score DESC);

	-- /users/{id}/likes — reverse lookup of challenges a user liked,
	-- newest-first. challenge_likes already has (challenge_id, user_id)
	-- as PK; this index serves the (user_id, created_at DESC) query.
	CREATE INDEX IF NOT EXISTS idx_challenge_likes_user_time
		ON challenge_likes(user_id, created_at DESC);

	-- /users/{id}/history — same idea for watch_events. The existing
	-- idx_watch_events_user_id is on user_id alone which works but
	-- forces a sort; this composite gives us index-order results.
	CREATE INDEX IF NOT EXISTS idx_watch_events_user_time
		ON watch_events(user_id, created_at DESC);
	`
	if _, err := db.Exec(indexes); err != nil {
		log.Printf("Warning: index creation issue: %v", err)
	}

	// Denormalized challenge counters + triggers. The feed retrieval path used
	// to LEFT JOIN (SELECT challenge_id, COUNT(*) FROM challenge_likes GROUP BY ...)
	// and a correlated COUNT(*) on challenge_responses on EVERY request — an
	// all-time aggregate Postgres can't push the outer LIMIT into. These columns
	// make the count an O(1) column read; triggers keep them exact. Tolerant:
	// log + continue if the DB role can't create triggers, so the app still boots
	// (the columns default to 0 and the self-heal backfill corrects them later).
	denorm := `
	ALTER TABLE challenges ADD COLUMN IF NOT EXISTS likes_count INT NOT NULL DEFAULT 0;
	ALTER TABLE challenges ADD COLUMN IF NOT EXISTS response_count INT NOT NULL DEFAULT 0;

	-- Backfill / self-heal: only touches rows whose denormalized count drifted
	-- from the source tables, so once converged (and with the triggers below
	-- keeping them current) subsequent boots update nothing.
	--
	-- This is also where response_count drift gets repaired. Seed data was
	-- found in production carrying response_count=1 with ZERO matching
	-- challenge_responses rows ("ghost battles" — counted as battles in
	-- lists, but with no opponent video the client rightly renders them as
	-- shorts). The pair of statements below covers both directions: rows
	-- that have responses, and rows that have none.
	UPDATE challenges c SET likes_count = s.cnt
	  FROM (SELECT challenge_id, COUNT(*) AS cnt FROM challenge_likes GROUP BY challenge_id) s
	  WHERE s.challenge_id = c.id AND c.likes_count <> s.cnt;
	UPDATE challenges c SET likes_count = 0
	  WHERE c.likes_count <> 0 AND NOT EXISTS (SELECT 1 FROM challenge_likes WHERE challenge_id = c.id);
	UPDATE challenges c SET response_count = s.cnt
	  FROM (SELECT challenge_id, COUNT(*) AS cnt FROM challenge_responses GROUP BY challenge_id) s
	  WHERE s.challenge_id = c.id AND c.response_count <> s.cnt;
	UPDATE challenges c SET response_count = 0
	  WHERE c.response_count <> 0 AND NOT EXISTS (SELECT 1 FROM challenge_responses WHERE challenge_id = c.id);

	CREATE OR REPLACE FUNCTION bump_challenge_likes_count() RETURNS TRIGGER AS $$
	BEGIN
	  IF (TG_OP = 'INSERT') THEN
	    UPDATE challenges SET likes_count = likes_count + 1 WHERE id = NEW.challenge_id;
	  ELSIF (TG_OP = 'DELETE') THEN
	    UPDATE challenges SET likes_count = GREATEST(likes_count - 1, 0) WHERE id = OLD.challenge_id;
	  END IF;
	  RETURN NULL;
	END;
	$$ LANGUAGE plpgsql;
	DROP TRIGGER IF EXISTS trg_challenge_likes_count ON challenge_likes;
	CREATE TRIGGER trg_challenge_likes_count AFTER INSERT OR DELETE ON challenge_likes
	  FOR EACH ROW EXECUTE FUNCTION bump_challenge_likes_count();

	CREATE OR REPLACE FUNCTION bump_challenge_response_count() RETURNS TRIGGER AS $$
	BEGIN
	  IF (TG_OP = 'INSERT') THEN
	    UPDATE challenges SET response_count = response_count + 1 WHERE id = NEW.challenge_id;
	  ELSIF (TG_OP = 'DELETE') THEN
	    UPDATE challenges SET response_count = GREATEST(response_count - 1, 0) WHERE id = OLD.challenge_id;
	  END IF;
	  RETURN NULL;
	END;
	$$ LANGUAGE plpgsql;
	DROP TRIGGER IF EXISTS trg_challenge_response_count ON challenge_responses;
	CREATE TRIGGER trg_challenge_response_count AFTER INSERT OR DELETE ON challenge_responses
	  FOR EACH ROW EXECUTE FUNCTION bump_challenge_response_count();
	`
	if _, err := db.Exec(denorm); err != nil {
		log.Printf("Warning: challenge counter denormalization issue: %v", err)
	}

	// pgvector ANN (optional). If the `vector` extension can be created (Render's
	// managed Postgres, incl. free tier, supports it), add a vector(32) embedding
	// column so the embedding retrieval source can do a true nearest-neighbor
	// scan over the WHOLE catalog instead of cosine-reranking the recency pool.
	// Fully optional + tolerant: if the role can't create the extension,
	// pgvectorAvailable stays false and sourceEmbeddingNeighbors falls back to
	// the in-process cosine rerank — nothing breaks. The embedding column is
	// populated by startEmbeddingBackfillWorker(), not at request time.
	pgvectorAvailable = false
	if _, err := db.Exec(`
		CREATE EXTENSION IF NOT EXISTS vector;
		ALTER TABLE challenges ADD COLUMN IF NOT EXISTS embedding vector(32);
		ALTER TABLE challenges ADD COLUMN IF NOT EXISTS embedding_attempted_at TIMESTAMPTZ;
	`); err != nil {
		log.Printf("pgvector not enabled (%v) — embedding retrieval uses in-process cosine rerank", err)
	} else {
		pgvectorAvailable = true
		log.Println("pgvector enabled: challenges.embedding ready for ANN retrieval")
		// HNSW index so the ANN query is a graph walk instead of the
		// sequential scan the file previously (deliberately) ran.
		// Guarded separately: HNSW needs pgvector >= 0.5.0; on older
		// versions the CREATE fails and we keep seq-scan behavior (same
		// results, just slower) rather than losing pgvector entirely.
		// vector_cosine_ops matches sourceEmbeddingANN's <=> operator.
		if _, err := db.Exec(`
			CREATE INDEX IF NOT EXISTS challenges_embedding_hnsw_idx
				ON challenges USING hnsw (embedding vector_cosine_ops)
		`); err != nil {
			log.Printf("pgvector HNSW index not created (%v) — ANN uses seq scan", err)
		} else {
			log.Println("pgvector HNSW index ready: ANN retrieval is indexed")
		}
	}

	// Versioned, run-once migrations on top of the idempotent baseline above.
	// Everything from here on that CHANGES the schema (rather than adding to
	// it) lives in migrations/*.sql — see schema_migrations.go for why, and
	// migrations/README.md for how to write one.
	//
	// Still refuses to serve on failure. The difference is that it now refuses
	// out loud: the error goes back to the caller, which keeps the port bound
	// and reports it, rather than killing the process and leaving every
	// request to hang.
	if err := applyVersionedMigrations(); err != nil {
		return err
	}

	// The database's refusal of a username taken in other capital letters,
	// switched on at the first start where no two names clash, and until
	// then the clashes named in the log (usernames.go). Not fatal: sign-up
	// refuses those names either way.
	ensureUsernameLock()

	// Give the videos that already exist the answers the new provenance
	// columns were built to hold. Deliberately AFTER the versioned
	// migrations, because it reads and writes the columns they add.
	//
	// Not fatal, unlike a migration: the worst case is that old videos keep
	// the category upload gave them, which is the state the app handled for
	// its whole life until now. See backfill_category_provenance.go.
	backfillCategoryProvenance()

	log.Println("Database migrations completed")
	return nil
}

// --------------------------------------------------------------------------
// User CRUD
// --------------------------------------------------------------------------

// readUser scans basic user columns and enriches with follower count + following list
// Use for single-user lookups (3 queries total + 1 for TOTP-enabled flag).
//
// bio/visibility/settings are read in this constructor — the cost is
// one trivial column read per profile lookup vs. a follow-up round
// trip per call site. TOTP enabled state is checked in a separate
// (indexed PK lookup) query because user_totp is a sensitive table
// and we want the access to be auditable from a single chokepoint.
func readUser(id int, username, pw, fullName, bio, visibility, settingsJSON string, wins, losses int, league string, rating, draws int) User {
	u := User{
		Rating:     rating,
		Draws:      draws,
		ID:         strconv.Itoa(id),
		Username:   username,
		password:   pw,
		FullName:   fullName,
		Bio:        bio,
		Visibility: visibility,
		Wins:       wins,
		Losses:     losses,
		League:     league,
	}
	// Settings JSON. Empty/invalid → no map (omitempty drops it on
	// the wire). We don't fail the whole user fetch on a bad blob
	// — a parser hiccup shouldn't take down the profile page.
	if settingsJSON != "" && settingsJSON != "{}" {
		var s map[string]any
		if json.Unmarshal([]byte(settingsJSON), &s) == nil {
			u.Settings = s
		}
	}

	// Followers count
	if err := db.QueryRow(
		`SELECT COUNT(*) FROM follows WHERE following_id = $1`, id,
	).Scan(&u.Followers); err != nil {
		queryFailed("how many people follow user "+u.ID,
			"their profile will show zero followers", err)
	}

	// Following list (string IDs for JSON compatibility)
	rows, err := db.Query(`SELECT following_id FROM follows WHERE follower_id = $1`, id)
	if !queryFailed("who user "+u.ID+" follows",
		"their profile will say they follow nobody", err) {
		defer rows.Close()
		for rows.Next() {
			var fid int
			if rows.Scan(&fid) == nil {
				u.FollowingList = append(u.FollowingList, strconv.Itoa(fid))
			}
		}
	}
	if u.FollowingList == nil {
		u.FollowingList = []string{}
	}
	u.Following = len(u.FollowingList)

	// TOTP-enabled flag — sourced from the dedicated user_totp table
	// (see least-privilege rationale in the table comment). EXISTS
	// keeps it as an index-only lookup; we never need the secret on
	// the profile read path.
	_ = db.QueryRow(
		`SELECT EXISTS (SELECT 1 FROM user_totp WHERE user_id=$1 AND is_active=TRUE)`,
		id,
	).Scan(&u.TwoFactorEnabled)

	return u
}

// enrichUsers batch-populates Followers and FollowingList for a page of users.
// It reads only the follows rows that reference someone on the page (the IN
// lists are bounded by the caller's page size), rather than scanning the whole
// follows table — which at scale dwarfs any single page. Whole-set callers that
// genuinely need every follow use enrichUsersAll instead.
func enrichUsers(users []User) {
	if len(users) == 0 {
		return
	}

	// Bounded IN list of the page's user IDs — same placeholder-building
	// convention as populateTopResponses, which avoids a pq.Array dependency.
	// The one id set scopes both directions: follower_id IN (...) gathers each
	// page user's full following list, following_id IN (...) their full
	// follower count.
	placeholders := make([]string, 0, len(users))
	args := make([]interface{}, 0, len(users))
	for i := range users {
		id, err := strconv.Atoi(users[i].ID)
		if err != nil {
			continue
		}
		placeholders = append(placeholders, fmt.Sprintf("$%d", len(args)+1))
		args = append(args, id)
	}
	if len(args) == 0 {
		return
	}
	inList := strings.Join(placeholders, ",")
	rows, err := db.Query(
		`SELECT follower_id, following_id FROM follows
		  WHERE follower_id IN (`+inList+`) OR following_id IN (`+inList+`)`,
		args...,
	)
	if err != nil {
		return
	}
	defer rows.Close()
	applyFollows(users, rows)
}

// enrichUsersAll batch-populates Followers and FollowingList with a single scan
// of the whole follows table. Used only by whole-set callers (GetAllUsers),
// where every user is already in hand and binding every id as a query parameter
// would both cost more than the scan and risk exceeding the protocol's
// parameter limit.
func enrichUsersAll(users []User) {
	if len(users) == 0 {
		return
	}
	rows, err := db.Query(`SELECT follower_id, following_id FROM follows`)
	if err != nil {
		return
	}
	defer rows.Close()
	applyFollows(users, rows)
}

// applyFollows consumes (follower_id, following_id) rows and writes each user's
// follower count and following list back into the slice.
func applyFollows(users []User, rows *sql.Rows) {
	followingMap := make(map[string][]string) // follower → []following
	followerCount := make(map[string]int)     // user → count of followers

	for rows.Next() {
		var fid, tid int
		if rows.Scan(&fid, &tid) == nil {
			fidStr := strconv.Itoa(fid)
			tidStr := strconv.Itoa(tid)
			followingMap[fidStr] = append(followingMap[fidStr], tidStr)
			followerCount[tidStr]++
		}
	}

	for i := range users {
		users[i].Followers = followerCount[users[i].ID]
		users[i].FollowingList = followingMap[users[i].ID]
		if users[i].FollowingList == nil {
			users[i].FollowingList = []string{}
		}
		users[i].Following = len(users[i].FollowingList)
	}
}

// GetuserByUsername returns a fully enriched user, looked up by username.
//
// The SELECT now pulls bio/visibility/settings alongside the legacy
// fields so the profile page doesn't have to make a second round-trip
// for them. COALESCE(settings::text, '{}') keeps the JSON-decode path
// in readUser simple even if a freshly-inserted user pre-dates the
// migration that added the column default.
func GetUserByUsername(username string) (User, bool) {
	var id, wins, losses, rating, draws int
	var uname, pw, fullName, bio, visibility, settings, league, avatar, tag string
	err := db.QueryRow(
		`SELECT id, username, password, full_name,
		        COALESCE(bio,''), COALESCE(visibility,'public'),
		        COALESCE(settings::text,'{}'),
		        wins, losses, league, rating, draws,
		        avatar_url, profile_tag
		   FROM users WHERE username = $1`,
		username,
	).Scan(&id, &uname, &pw, &fullName, &bio, &visibility, &settings, &wins, &losses, &league, &rating, &draws, &avatar, &tag)
	if err != nil {
		return User{}, false
	}
	u := readUser(id, uname, pw, fullName, bio, visibility, settings, wins, losses, league, rating, draws)
	u.AvatarURL, u.ProfileTag = avatar, tag
	return u, true
}

// GetUserByID returns a fully enriched user, looked up by string ID.
func GetUserByID(idStr string) (User, bool) {
	idInt, err := strconv.Atoi(idStr)
	if err != nil {
		return User{}, false
	}
	var wins, losses, rating, draws int
	var uname, pw, fullName, bio, visibility, settings, league, avatar, tag string
	err = db.QueryRow(
		`SELECT id, username, password, full_name,
		        COALESCE(bio,''), COALESCE(visibility,'public'),
		        COALESCE(settings::text,'{}'),
		        wins, losses, league, rating, draws,
		        avatar_url, profile_tag
		   FROM users WHERE id = $1`,
		idInt,
	).Scan(&idInt, &uname, &pw, &fullName, &bio, &visibility, &settings, &wins, &losses, &league, &rating, &draws, &avatar, &tag)
	if err != nil {
		return User{}, false
	}
	u := readUser(idInt, uname, pw, fullName, bio, visibility, settings, wins, losses, league, rating, draws)
	u.AvatarURL, u.ProfileTag = avatar, tag
	return u, true
}

// UserExists checks whether a username is already taken — by anybody,
// whatever the capital letters: "Karan" and "karan" are one name. An account
// waiting to be deleted still holds its name for the 30 days; once it is
// deleted for good, the name is free (account_leaving.go).
func UserExists(username string) bool {
	var exists bool
	// NB: this had a missing closing paren for months — no callers
	// exercised it, so the syntax error only surfaced when the future
	// signup flow reached for it and always got `false`.
	if err := db.QueryRow(`SELECT EXISTS(SELECT 1 FROM users WHERE lower(username) = lower($1))`,
		username).Scan(&exists); err != nil {
		queryFailed("UserExists: could not check whether that username is taken",
			"reporting it as free, which lets two accounts collide on it", err)
	}
	return exists
}

// IsValidUser checks credentials against the bcrypt hash in password_hash.
//
// Migration path off the old plaintext scheme: a legacy row has an empty
// password_hash and a plaintext password. On the first successful login for
// such a row we verify the plaintext (constant-time), then upgrade it — write
// the bcrypt hash and WIPE the plaintext column. This is zero-downtime and
// spreads the rehash cost across logins instead of a non-scalable bulk job at
// boot. Once upgraded, only the bcrypt path is ever taken.
func IsValidUser(username, password string) bool {
	var stored, hash string
	err := db.QueryRow(
		`SELECT password, COALESCE(password_hash,'') FROM users WHERE username = $1`,
		username,
	).Scan(&stored, &hash)
	if err != nil {
		return false
	}

	if hash != "" {
		return checkPassword(hash, password)
	}

	// Legacy plaintext row. A row with neither a hash nor a stored password has
	// no credential on file — reject, so a blank-password attempt can't match an
	// empty column via the constant-time compare below.
	if stored == "" {
		return false
	}
	// Constant-time compare to avoid leaking length/content via timing, then
	// migrate to bcrypt and clear the plaintext.
	if subtle.ConstantTimeCompare([]byte(stored), []byte(password)) != 1 {
		return false
	}
	if h, herr := hashPassword(password); herr == nil {
		if _, uerr := db.Exec(
			`UPDATE users SET password_hash = $1, password = '' WHERE username = $2`,
			h, username,
		); uerr != nil {
			// Non-fatal: the login still succeeds; we just retry the upgrade
			// next time. Worst case the plaintext lingers one more login.
			log.Printf("password upgrade failed for %s: %v", username, uerr)
		}
	}
	return true
}

// GetAllUsers returns every user, batch-enriched with follow data. Used by the
// background jobs that legitimately need the whole set (Meilisearch indexing,
// notification fan-out, search fallback). The password column is intentionally
// NOT selected — nothing here needs it and it shouldn't sit in memory. The
// client-facing roster uses the bounded GetUsersPaginated instead.
//
// Every user that is staying: an account waiting to be deleted is not
// searched for, indexed or told anything (account_leaving.go).
func GetAllUsers() []User {
	rows, err := db.Query(`SELECT id, username, full_name, wins, losses, league FROM users
	                        WHERE deletion_requested_at IS NULL AND suspended_at IS NULL ORDER BY id`)
	if err != nil {
		log.Printf("GetAllUsers error: %v", err)
		return nil
	}
	defer rows.Close()

	var result []User
	for rows.Next() {
		var id, wins, losses int
		var uname, fullName, league string
		if rows.Scan(&id, &uname, &fullName, &wins, &losses, &league) == nil {
			result = append(result, User{
				ID:       strconv.Itoa(id),
				Username: uname,
				FullName: fullName,
				Wins:     wins,
				Losses:   losses,
				League:   league,
			})
		}
	}

	enrichUsersAll(result)
	return result
}

// GetUsersPaginated returns one ordered page of users (no password), enriched
// with follow data. This is the client-facing roster query — the old hot path
// loaded the entire users table on every login, which does not scale; the
// client now requests a bounded page.
func GetUsersPaginated(limit, offset int) []User {
	rows, err := db.Query(
		`SELECT id, username, full_name, wins, losses, league
		   FROM users WHERE deletion_requested_at IS NULL AND suspended_at IS NULL
		  ORDER BY id LIMIT $1 OFFSET $2`,
		limit, offset,
	)
	if err != nil {
		log.Printf("GetUsersPaginated error: %v", err)
		return nil
	}
	defer rows.Close()

	var result []User
	for rows.Next() {
		var id, wins, losses int
		var uname, fullName, league string
		if rows.Scan(&id, &uname, &fullName, &wins, &losses, &league) == nil {
			result = append(result, User{
				ID:       strconv.Itoa(id),
				Username: uname,
				FullName: fullName,
				Wins:     wins,
				Losses:   losses,
				League:   league,
			})
		}
	}

	enrichUsers(result)
	return result
}

// followUserPage runs a follows-join query (one direction) and returns a page
// of the joined users. dir is the column to match the subject on
// ("following_id" for followers-of-subject, "follower_id" for following-of-
// subject) and pick is the column naming the other party to return.
func followUserPage(subjectID string, matchCol, pickCol string, limit, offset int) []User {
	uid, err := strconv.Atoi(subjectID)
	if err != nil {
		return nil
	}
	// matchCol/pickCol are not user input — they're fixed literals chosen by the
	// two callers below — so interpolating them into the SQL is safe.
	q := `SELECT u.id, u.username, u.full_name, u.wins, u.losses, u.league
	        FROM follows f JOIN users u ON u.id = f.` + pickCol + `
	       WHERE f.` + matchCol + ` = $1
	       ORDER BY f.created_at DESC
	       LIMIT $2 OFFSET $3`
	rows, err := db.Query(q, uid, limit, offset)
	if err != nil {
		log.Printf("followUserPage error: %v", err)
		return nil
	}
	defer rows.Close()

	var result []User
	for rows.Next() {
		var id, wins, losses int
		var uname, fullName, league string
		if rows.Scan(&id, &uname, &fullName, &wins, &losses, &league) == nil {
			result = append(result, User{
				ID:       strconv.Itoa(id),
				Username: uname,
				FullName: fullName,
				Wins:     wins,
				Losses:   losses,
				League:   league,
			})
		}
	}
	enrichUsers(result)
	return result
}

// GetFollowers returns a page of users who follow subjectID.
func GetFollowers(subjectID string, limit, offset int) []User {
	return followUserPage(subjectID, "following_id", "follower_id", limit, offset)
}

// GetFollowing returns a page of users that subjectID follows.
func GetFollowing(subjectID string, limit, offset int) []User {
	return followUserPage(subjectID, "follower_id", "following_id", limit, offset)
}

// --------------------------------------------------------------------------------
// Post CRUD
// --------------------------------------------------------------------------------

// postBaseQuery is the common SELECT used for all post retrievals.
// It joins author info and aggregates likes + comments in a single scan.
const postBaseQuery = `
SELECT p.id, p.author_id, u.username, u.league,
	   p.type,
	   COALESCE(p.content_url,   '') AS content_url,
	   COALESCE(p.thumbnail_url, '') AS thumbnail_url,
	   p.caption, p.views,
	   COALESCE(lc.cnt, 0) AS likes,
	   COALESCE(cc.cnt, 0) AS comment_count,
	   p.created_at,
	   COALESCE(p.category, 'other') AS category,
	   COALESCE(p.emotion_tags, '[]'::JSONB) AS emotion_tags,
	   COALESCE(p.energy_level, 'medium') AS energy_level
FROM posts p
JOIN users u ON p.author_id = u.id
LEFT JOIN (SELECT post_id, COUNT(*) AS cnt FROM post_likes GROUP BY post_id) lc ON lc.post_id = p.id
LEFT JOIN (SELECT post_id, COUNT(*) AS cnt FROM comments GROUP BY post_id) cc ON cc.post_id = p.id`

// queryPosts executes a full post query string and returns enriched Post structs
// including the LikedBy list (batch-fetched).
func queryPosts(query string, args ...interface{}) []Post {
	rows, err := db.Query(query, args...)
	if err != nil {
		log.Printf("queryPosts error: %v", err)
		return nil
	}
	defer rows.Close()

	var result []Post
	var postIDs []int

	for rows.Next() {
		var id, authorID, views, likes, comments int
		var username, league, postType, contentURL, thumbnailURL, caption string
		var category, energyLevel string
		var emotionJSON []byte
		var createdAt time.Time

		if rows.Scan(&id, &authorID, &username, &league,
			&postType, &contentURL, &thumbnailURL,
			&caption, &views, &likes, &comments, &createdAt,
			&category, &emotionJSON, &energyLevel) == nil {

			var emotions []string
			json.Unmarshal(emotionJSON, &emotions)

			result = append(result, Post{
				ID:             strconv.Itoa(id),
				AuthorID:       strconv.Itoa(authorID),
				AuthorUsername: username,
				AuthorLeague:   league,
				Type:           postType,
				ContentURL:     contentURL,
				ThumbnailURL:   thumbnailURL,
				Caption:        caption,
				Views:          views,
				Likes:          likes,
				Comments:       comments,
				CreatedAt:      createdAt.UTC().Format(time.RFC3339),
				Category:       category,
				EmotionTags:    emotions,
				EnergyLevel:    energyLevel,
			})
			postIDs = append(postIDs, id)
		}
	}

	// Batch-fetch LikedBy for every returned post (1 extra query)
	if len(postIDs) > 0 {
		likedByMap := getLikedByMap(postIDs)
		for i := range result {
			if lb, ok := likedByMap[result[i].ID]; ok {
				result[i].LikedBy = lb
			}
		}
	}

	return result
}

// getLikedByMap returns a map[postip==IDStr] -> []userIDStr for the given post IDs.
func getLikedByMap(postIDs []int) map[string][]string {
	result := make(map[string][]string)
	if len(postIDs) == 0 {
		return result
	}

	placeholders := ""
	args := make([]interface{}, len(postIDs))
	for i, id := range postIDs {
		if i > 0 {
			placeholders += ", "
		}
		placeholders += fmt.Sprintf("$%d", i+1)
		args[i] = id
	}

	rows, err := db.Query(
		fmt.Sprintf(`SELECT post_id, user_id FROM post_likes WHERE post_id IN (%s)`, placeholders),
		args...,
	)
	if err != nil {
		return result
	}
	defer rows.Close()

	for rows.Next() {
		var pid, uid int
		if rows.Scan(&pid, &uid) == nil {
			pidStr := strconv.Itoa(pid)
			result[pidStr] = append(result[pidStr], strconv.Itoa(uid))
		}
	}
	return result
}

// GetAllPosts returns every post, newest first.
func GetAllPosts() []Post {
	return queryPosts(postBaseQuery + ` ORDER BY p.created_at DESC`)
}

// GetPostsPaginated returns a page of posts and whether more pages exist.
func GetPostsPaginated(page, limit int) ([]Post, bool) {
	offset := (page - 1) * limit
	// Fetch one extra row to check if there's a next page
	posts := queryPosts(postBaseQuery+` ORDER BY p.created_at DESC LIMIT $1 OFFSET $2`, limit+1, offset)
	hasMore := len(posts) > limit
	if hasMore {
		posts = posts[:limit]
	}
	return posts, hasMore
}

// GetPostsByUserID returns all posts by a given author.
func GetPostsByUserID(userID string) []Post {
	uid, err := strconv.Atoi(userID)
	if err != nil {
		return nil
	}
	return queryPosts(postBaseQuery+` WHERE p.author_id = $1 ORDER BY p.created_at DESC`, uid)
}

// GetPostByID returns a single post.
func GetPostByID(idStr string) (Post, bool) {
	id, err := strconv.Atoi(idStr)
	if err != nil {
		return Post{}, false
	}
	posts := queryPosts(postBaseQuery+` WHERE p.id = $1`, id)
	if len(posts) == 0 {
		return Post{}, false
	}
	return posts[0], true
}

// ToggleLike adds or removes a like and returns (liked, newCount, UpdatedPost).
func ToggleLike(postID, userID string) (bool, int, Post) {
	pid, err1 := strconv.Atoi(postID)
	uid, err2 := strconv.Atoi(userID)
	if err1 != nil || err2 != nil {
		return false, 0, Post{}
	}

	var exists bool
	if err := db.QueryRow(`SELECT EXISTS(SELECT 1 FROM post_likes WHERE post_id=$1 AND user_id=$2)`, pid, uid).Scan(&exists); err != nil {
		queryFailed("ToggleLike: could not check whether they had already "+
			"acted on this post",
			"treating it as not yet done, so the tap may go the wrong way", err)
	}

	if exists {
		db.Exec(`DELETE FROM post_likes WHERE post_id=$1 AND user_id=$2`, pid, uid)
	} else {
		db.Exec(`INSERT INTO post_likes (post_id, user_id) VALUES ($1,$2) ON CONFLICT DO NOTHING`, pid, uid)
	}

	post, found := GetPostByID(postID)
	if !found {
		return false, 0, Post{}
	}
	return !exists, post.Likes, post
}

// ---------------------------------------------------------------------------
// Comment CRUD
// ---------------------------------------------------------------------------

// AddComment inserts a new comment and returns the created Comment struct.
func AddComment(postID, authorID, authorUsername, text string) Comment {
	pid, err1 := strconv.Atoi(postID)
	uid, err2 := strconv.Atoi(authorID)
	if err1 != nil || err2 != nil {
		return Comment{}
	}

	var id int
	var createdAt time.Time
	err := db.QueryRow(
		`INSERT INTO comments (post_id, author_id, text) VALUES ($1,$2,$3) RETURNING id, created_at`,
		pid, uid, text,
	).Scan(&id, &createdAt)
	if err != nil {
		log.Printf("AddComment error: %v", err)
		return Comment{}
	}

	return Comment{
		ID:             strconv.Itoa(id),
		PostID:         postID,
		AuthorID:       authorID,
		AuthorUsername: authorUsername,
		Text:           text,
		CreatedAt:      createdAt.UTC().Format(time.RFC3339),
	}
}

// GetComments returns all comments for a post, with author usernames resolved via JOIN
func GetComments(postID string) []Comment {
	pid, err := strconv.Atoi(postID)
	if err != nil {
		return nil
	}

	rows, err := db.Query(
		`SELECT c.id, c.post_id, c.author_id, u.username, c.text, c.created_at
		FROM comments c
		JOIN users u ON c.author_id = u.id
		WHERE c.post_id = $1
		ORDER BY c.created_at ASC`, pid,
	)
	if err != nil {
		return nil
	}
	defer rows.Close()

	var result []Comment
	for rows.Next() {
		var cid, postIDInt, authorID int
		var username, txt string
		var createdAt time.Time
		if rows.Scan(&cid, &postIDInt, &authorID, &username, &txt, &createdAt) == nil {
			result = append(result, Comment{
				ID:             strconv.Itoa(cid),
				PostID:         strconv.Itoa(postIDInt),
				AuthorID:       strconv.Itoa(authorID),
				AuthorUsername: username,
				Text:           txt,
				CreatedAt:      createdAt.UTC().Format(time.RFC3339),
			})
		}
	}
	return result
}

// ------------------------------------------------------------------------------
// Challenge CRUD
// ------------------------------------------------------------------------------

// CreateChallenge inserts a new challenge and optional visibility rows.
func CreateChallenge(payload CreateChallengePayload) (Challenge, error) {
	creatorID, err := strconv.Atoi(payload.CreatorID)
	if err != nil {
		return Challenge{}, fmt.Errorf("invalid creator ID")
	}
	mediaType, ok := mediaTypeOf(payload.MediaType)
	if !ok {
		return Challenge{}, fmt.Errorf("a post is a video or a photo")
	}

	var id int
	var createdAt time.Time
	// The creator's own words for what this is about, cleaned and capped on
	// the way in so two videos tagged the same thing compare equal later.
	// See content_tags.go.
	tags := normalizeTags(payload.Tags)

	// Category, in order of how much the source can be trusted: what the
	// creator picked, then what their tags say, then keyword matching on the
	// subject line — which is a guess, and used to be the only input.
	category := categoryForContent(payload.Category, tags, payload.Subject, payload.Prefix, "")
	// Energy level: derived server-side from category + subject +
	// caption + creator baseline. See energy_classifier.go for the
	// weighted-scoring breakdown. Older clients that still send an
	// explicit value win — we honor it both for transition smoothness
	// and so a deliberate authoring choice can override the rule
	// table when the creator really does know better.
	energyLevel := payload.EnergyLevel
	if energyLevel == "" {
		// We pass the creator ID so the classifier can fold in the
		// "modal energy of their last 5 challenges" signal — a strong
		// zero-shot prior that beats text-only on edge-case subjects.
		// First-time creators degrade cleanly: the baseline lookup
		// returns "" and the rest of the signals decide.
		energyLevel = deriveEnergyLevelWithCreator(
			category,
			payload.Subject,
			payload.Prefix+" "+payload.Subject,
			payload.CreatorID,
		)
	}
	// Mood, from everything the creator gave us. A tag can name a mood as
	// readily as a subject — "funny", "chill" and "scary" are all on the
	// emotion list — so tags feed this as well as the category. Reading them
	// as only one or the other throws away half of what was said. See
	// emotionsForContent.
	emotions := emotionsForContent(payload.EmotionTags, tags, payload.Subject, payload.Prefix, "")
	emotionJSON, _ := json.Marshal(emotions)
	if len(emotions) == 0 {
		emotionJSON = []byte("[]")
	}
	// Always write a non-nil JSONB blob — lib/pq's default nil-byte → SQL NULL
	// translation collides with the column's JSONB type and the row gets
	// rejected. Empty map serializes to {} which the JSONB column accepts and
	// downstream readers treat as "no variants encoded yet".
	variantsJSON, _ := json.Marshal(payload.VideoVariants)
	if len(payload.VideoVariants) == 0 {
		variantsJSON = []byte("{}")
	}

	// Same non-nil rule as variantsJSON above: an empty slice must reach the
	// JSONB column as [] rather than as SQL NULL.
	tagsJSON, _ := json.Marshal(tags)
	if len(tags) == 0 {
		tagsJSON = []byte("[]")
	}

	// What the CREATOR actually said, kept apart from what we worked out.
	//
	// usableCategory strips the two words that mean "nobody chose" ('other'
	// and 'general'), so this column only ever holds a real pick by a person.
	// category above may be that pick, or their tags, or a keyword guess at
	// the subject line — and until this column existed there was no way to
	// tell which, so the ranker weighed our own guesses against the model as
	// though somebody had made a claim. See migrations/010.
	creatorCategory := usableCategory(strings.ToLower(strings.TrimSpace(payload.Category)))
	err = db.QueryRow(
		`INSERT INTO challenges (creator_id, video_url, video_variants, thumbnail_url, prefix, subject, visibility, category, creator_category, category_source, emotion_tags, custom_tags, energy_level, battle_days, media_type, music_track_id, open_to_battles)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17) RETURNING id, created_at`,
		creatorID, payload.VideoURL, variantsJSON, payload.ThumbnailURL, payload.Prefix, payload.Subject, payload.Visibility,
		category, creatorCategory, categorySourceAtUpload(creatorCategory, category), emotionJSON, tagsJSON, energyLevel,
		clampBattleDays(payload.BattleDays), mediaType, payload.musicTrack, payload.openToBattles(),
	).Scan(&id, &createdAt)
	if err != nil {
		return Challenge{}, err
	}
	countMusicUse(payload.musicTrack)

	// The row is now waiting to be converted, so ask the worker to start
	// rather than leaving it for the next scheduled run up to 30 minutes
	// away. Returns immediately and cannot fail this upload; does nothing
	// at all unless it has been switched on. See transcode_wakeup.go. A
	// photo has nothing to convert.
	if payload.VideoURL != "" && mediaType == mediaVideo {
		wakeTranscodeWorker()
	}

	// Friends-only, for friends picked by name: only those of them who
	// follow the creator. See friends_only.go.
	if payload.Visibility == "friends" && len(payload.VisibleTo) > 0 {
		saveVisibleTo(id, creatorID, payload.VisibleTo)
	}

	// Fetch the creator info.
	creator, _ := GetUserByID(payload.CreatorID)

	return Challenge{
		ID:              strconv.Itoa(id),
		MediaType:       mediaType,
		CreatorID:       payload.CreatorID,
		CreatorUsername: creator.Username,
		CreatorLeague:   creator.League,
		VideoURL:        payload.VideoURL,
		ThumbnailURL:    payload.ThumbnailURL,
		Prefix:          payload.Prefix,
		Subject:         payload.Subject,
		Visibility:      payload.Visibility,
		Status:          "open",
		ClosedToBattles: !payload.openToBattles(),
		Category:        category,
		EmotionTags:     emotions,
		Tags:            tags,
		EnergyLevel:     energyLevel,
		CreatedAt:       createdAt.UTC().Format(time.RFC3339),
		ExpiresAt:       createdAt.Add(24 * time.Hour).UTC().Format(time.RFC3339),
	}, nil
}

// challengeBaseQuery is the common SELECT for challenges.
const challengeBaseQuery = `
SELECT c.id, c.creator_id, u.username, u.league,
	COALESCE(c.video_url, '') AS video_url,
	COALESCE(c.thumbnail_url, '') AS thumbnail_url,
	c.prefix, c.subject, c.visibility, c.status, c.views,
	COALESCE(lc.cnt, 0) AS likes,
	COALESCE(rc.cnt, 0) AS response_count,
	c.created_at,
	COALESCE(c.category, 'other') AS category,
	COALESCE(c.emotion_tags, '[]') AS emotion_tags,
	COALESCE(c.energy_level, 'medium') AS energy_level,
	-- The encoded versions, and the segmented manifest.
	--
	-- Without these two columns every list built on this query hands the
	-- client the raw upload and nothing else — no rung to pick from, no
	-- manifest, no way for the app's chooser to do its job. The reels feed
	-- fills them in separately (populateHLSManifestURLs), so the gap was
	-- invisible there and total everywhere else: the profile, the arena
	-- list and the friends list all played the file exactly as it came off
	-- somebody's camera.
	--
	-- 'PENDING' is the worker's claim marker, not a URL. It has to become
	-- '' here or a client will try to play the word.
	COALESCE(c.video_variants, '{}'::jsonb)::text AS video_variants,
	CASE WHEN COALESCE(c.hls_manifest_url, '') = 'PENDING' THEN ''
	     ELSE COALESCE(c.hls_manifest_url, '') END AS hls_manifest_url,
	COALESCE(c.media_type, 'video') AS media_type,
	c.open_to_battles
FROM challenges c
JOIN users u ON c.creator_id = u.id
LEFT JOIN (SELECT challenge_id, COUNT(*) AS cnt FROM challenge_likes GROUP BY challenge_id) lc ON lc.challenge_id = c.id
LEFT JOIN (SELECT challenge_id, COUNT(*) AS cnt FROM challenge_responses
             WHERE NOT COALESCE(is_hidden, FALSE)
               AND responder_id NOT IN ` + leavingUsersSQL + `
             GROUP BY challenge_id) rc ON rc.challenge_id = c.id`

// queryChallenges executes a challenge query and returns Challenge structs.
func queryChallenges(query string, args ...interface{}) []Challenge {
	rows, err := db.Query(query, args...)
	if err != nil {
		log.Printf("queryChallenges error: %v", err)
		return nil
	}
	defer rows.Close()

	var result []Challenge
	// A row that cannot be read was dropped without a word, which looks
	// exactly like a list with nothing in it. Now it says so.
	bad := 0
	for rows.Next() {
		var id, creatorID, views, likes, respCount int
		var username, league, videoURL, thumbURL, prefix, subject, visibility, status string
		var categoryStr, energyStr, variantsJSON, manifestURL, mediaType string
		var emotionJSON []byte
		var createdAt time.Time
		var openToBattles bool

		if !scanFailed("queryChallenges: a post in a list", rows.Scan(&id, &creatorID, &username, &league,
			&videoURL, &thumbURL,
			&prefix, &subject, &visibility, &status, &views,
			&likes, &respCount, &createdAt,
			&categoryStr, &emotionJSON, &energyStr,
			&variantsJSON, &manifestURL, &mediaType, &openToBattles), &bad) {

			var emotions []string
			json.Unmarshal(emotionJSON, &emotions)

			// Unreadable variants are not a reason to drop the row — the
			// client falls back to videoUrl, which is what it did for every
			// row before these columns were selected at all.
			var variants VideoVariants
			if variantsJSON != "" && variantsJSON != "{}" {
				if err := json.Unmarshal([]byte(variantsJSON), &variants); err != nil {
					log.Printf("queryChallenges: bad video_variants on challenge %d: %v", id, err)
					variants = nil
				}
			}

			result = append(result, Challenge{
				MediaType:       mediaType,
				VideoVariants:   variants,
				HLSManifestURL:  manifestURL,
				ID:              strconv.Itoa(id),
				CreatorID:       strconv.Itoa(creatorID),
				CreatorUsername: username,
				CreatorLeague:   league,
				VideoURL:        videoURL,
				ThumbnailURL:    thumbURL,
				Prefix:          prefix,
				Subject:         subject,
				Visibility:      visibility,
				Status:          status,
				ClosedToBattles: !openToBattles,
				Views:           views,
				Likes:           likes,
				ResponseCount:   respCount,
				Category:        categoryStr,
				EmotionTags:     emotions,
				EnergyLevel:     energyStr,
				CreatedAt:       createdAt.UTC().Format(time.RFC3339),
				ExpiresAt:       createdAt.Add(24 * time.Hour).UTC().Format(time.RFC3339),
			})
		}
	}
	return result
}

// GetArenaChallenges returns all non-expired open arena challenges (within 24h).
//
// This is deliberately not the search population. The battle tab shows live
// contests, so an open challenge nobody has answered in a day is stale and
// drops off. Search has to keep it — see GetSearchableChallenges just below
// for what happened when these two were the same query.
func GetArenaChallenges() []Challenge {
	return queryChallenges(challengeBaseQuery + `
	  WHERE c.visibility = 'arena' 
	  	AND (c.status IN ('active','completed') OR (c.status = 'open' AND c.open_to_battles AND c.created_at > NOW() - INTERVAL '24 hours'))
	  ORDER BY c.created_at DESC`)
}

// GetSearchableChallenges returns every public video, for search and the
// search index.
//
// ════════════════════════════════════════════════════════════════════════════
// WHY THIS IS NOT GetArenaChallenges
// ════════════════════════════════════════════════════════════════════════════
//
// The arena list drops an unanswered challenge 24 hours after it is posted.
// That is a sensible rule for a BATTLE TAB — an open challenge nobody has
// taken up is stale after a day, and the tab exists to show live contests.
//
// It is a catastrophic rule for search, and search was using it.
//
// A video becomes 'active' only when somebody responds to it. So every upload
// nobody answered vanished from that list a day later — and search, the search
// INDEX, and the related-subject fallback all read it. Measured on the live
// server: 96 videos in the catalogue, and this returned an empty list, so no
// search could return a video no matter how good the matching was and
// Meilisearch was being seeded with nothing at all.
//
// Nobody noticed because the For You feed uses its own query with no such
// clause. The app looked healthy while search was empty.
//
// Searching is not browsing a tab. A video somebody uploaded last month is
// exactly what a person is looking for when they type its subject.
func GetSearchableChallenges() []Challenge {
	return queryChallenges(challengeBaseQuery + `
	  WHERE ` + searchableWhere("c") + `
	  ORDER BY c.created_at DESC`)
}

// GetFriendsChallenges returns challenges visible to a specific user (friends-only).
// This includes: challenges by people the user follows with visibility=friends,
// where the user is either in the visible_to list OR the list is empty (all friends).
func GetFriendsChallenges(userID string) []Challenge {
	uid, err := strconv.Atoi(userID)
	if err != nil {
		return nil
	}
	query := challengeBaseQuery + `
	WHERE c.visibility = 'friends' 
	  AND c.creator_id IN (SELECT following_id FROM follows WHERE follower_id =$1)
	  AND (
		NOT EXISTS (SELECT 1 FROM challenge_visible_to WHERE challenge_id = c.id)
		OR c.id IN (SELECT challenge_id FROM challenge_visible_to WHERE user_id = $1)
	  )
	  AND (c.status IN ('active','completed') OR (c.status = 'open' AND c.open_to_battles AND c.created_at > NOW() - INTERVAL '24 hours'))
	ORDER BY c.created_at DESC`
	return queryChallenges(query, uid)
}

// GetChallengeByIDreturns a single challenge.
func GetChallengeByID(idStr string) (Challenge, bool) {
	id, err := strconv.Atoi(idStr)
	if err != nil {
		return Challenge{}, false
	}
	results := queryChallenges(challengeBaseQuery+` WHERE c.id = $1`, id)
	if len(results) == 0 {
		return Challenge{}, false
	}
	return results[0], true
}

// DeleteChallengeByID removes a challenge row. Responses, likes,
// votes, comments, saves, and HLS rows all live in child tables that
// declare ON DELETE CASCADE against challenges(id) — see the schema
// in this file — so a single DELETE here garbage-collects every
// derived row in one transaction. We deliberately do NOT touch R2:
// the orphaned video/thumbnail objects will be reaped by a separate
// scheduled cleanup job (TODO) that diff's the bucket against the
// live challenge_id set. Doing the R2 delete inline would couple
// every UI delete to network latency + S3 sigv4 signing, and a
// failed bucket delete would leave the DB and storage out of sync
// with no easy recovery.
//
// Authorization (creator-only) is enforced at the handler layer,
// NOT here, so internal callers (admin moderation, scheduled GC)
// can use this directly.
// DeleteResponseByID removes one answer to a battle.
//
// Separate from DeleteChallengeByID because the two are not the same act.
// Deleting a challenge takes its answers with it — they have nothing left to
// answer. Deleting a single answer leaves the challenge standing with one
// fewer, which the response_count trigger in this file keeps correct without
// anything here having to remember to.
//
// Storage is queued rather than cleared inline, exactly as it is for a
// challenge, and for the same reason: a slow or failed bucket delete must not
// make the caller's delete look broken.
//
// Authorization is the caller's job, not this function's.
func DeleteResponseByID(idStr string) error {
	id, err := strconv.Atoi(idStr)
	if err != nil {
		return fmt.Errorf("invalid response id %q: %w", idStr, err)
	}
	// Read where the files live BEFORE the row goes; afterwards there is
	// nothing left to read the addresses from.
	prefixes := mediaPrefixesForResponse(id)

	res, err := db.Exec(`DELETE FROM challenge_responses WHERE id = $1`, id)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return fmt.Errorf("%w: %d", errChallengeNotFound, id)
	}
	enqueueMediaDeletions(prefixes)
	return nil
}

// errChallengeNotFound is returned, wrapped, when an id names no challenge.
// A sentinel rather than a message so a caller deciding what to report can
// use errors.Is instead of comparing text that is free to change.
var errChallengeNotFound = errors.New("no such challenge")

func DeleteChallengeByID(idStr string) error {
	id, err := strconv.Atoi(idStr)
	if err != nil {
		return fmt.Errorf("invalid challenge id %q: %w", idStr, err)
	}
	// Read where the videos live BEFORE the rows go — afterwards there is
	// nothing left to read the addresses from. Deleting the rows used to be
	// the whole job, which left every video sitting in the bucket forever,
	// unreachable from the app but still stored and still billed. See
	// media_delete.go.
	prefixes := mediaPrefixesForChallenge(id)

	res, err := db.Exec(`DELETE FROM challenges WHERE id = $1`, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		// Wrapped so callers can tell "that id names nothing" from a real
		// failure without matching on the wording — see isNoSuchChallenge.
		return fmt.Errorf("%w: %d", errChallengeNotFound, id)
	}

	// Queued, not done here: clearing a bucket is slow and can fail, and
	// neither should make the user's delete button look broken. A background
	// worker drains the queue.
	enqueueMediaDeletions(prefixes)
	return nil
}

// GetChallengeResponses returns all responses to a challenge.
func GetChallengeResponses(challengeID string) []ChallengeResponse {
	cid, err := strconv.Atoi(challengeID)
	if err != nil {
		return nil
	}

	// Hidden responses (community-flagged off-topic) are excluded from the
	// public listing — they remain in the table for audit/appeal.
	rows, err := db.Query(
		`SELECT cr.id, cr.challenge_id, cr.responder_id, u.username, u.league,
				COALESCE(cr.video_url, '') AS video_url,
				COALESCE(cr.video_variants, '{}'::jsonb),
				COALESCE(cr.thumbnail_url, '') AS thumbnail_url,
				cr.views,
				COALESCE(lc.cnt, 0) AS likes,
				COALESCE(cr.duration_ms, 0),
				COALESCE(cr.caption, ''),
				COALESCE(cr.relevance_score, 0.5),
				COALESCE(cr.off_topic_flags, 0),
				COALESCE(cr.is_hidden, FALSE),
				cr.created_at
		 FROM challenge_responses cr
		 JOIN users u ON cr.responder_id = u.id
		 LEFT JOIN (SELECT response_id, COUNT(*) AS cnt FROM challenge_response_likes GROUP BY response_id) lc ON lc.response_id = cr.id
		 WHERE cr.challenge_id = $1 AND COALESCE(cr.is_hidden, FALSE) = FALSE
		   AND cr.responder_id NOT IN `+leavingUsersSQL+`
		 ORDER BY cr.created_at ASC`, cid,
	)
	if err != nil {
		return nil
	}
	defer rows.Close()

	var result []ChallengeResponse
	for rows.Next() {
		var id, chalID, respID, views, likes, durationMs, offTopicFlags int
		var relevance float64
		var isHidden bool
		var username, league, videoURL, thumbURL, caption string
		var variantsRaw []byte
		var createdAt time.Time
		if rows.Scan(&id, &chalID, &respID, &username, &league,
			&videoURL, &variantsRaw, &thumbURL, &views, &likes,
			&durationMs, &caption, &relevance, &offTopicFlags, &isHidden,
			&createdAt) == nil {
			var variants VideoVariants
			if len(variantsRaw) > 0 {
				_ = json.Unmarshal(variantsRaw, &variants)
			}
			result = append(result, ChallengeResponse{
				ID:                strconv.Itoa(id),
				ChallengeID:       strconv.Itoa(chalID),
				ResponderID:       strconv.Itoa(respID),
				ResponderUsername: username,
				ResponderLeague:   league,
				VideoURL:          videoURL,
				VideoVariants:     variants,
				ThumbnailURL:      thumbURL,
				Views:             views,
				Likes:             likes,
				DurationMs:        durationMs,
				Caption:           caption,
				RelevanceScore:    relevance,
				OffTopicFlags:     offTopicFlags,
				IsHidden:          isHidden,
				CreatedAt:         createdAt.UTC().Format(time.RFC3339),
			})
		}
	}
	return result
}

// AcceptChallenge inserts a response and updates challenge status.
// Computes relevance score from caption vs challenge prompt at insert time
// (cheap keyword overlap — no per-upload AI inference).
func AcceptChallenge(payload AcceptChallengePayload) (ChallengeResponse, error) {
	cid, err1 := strconv.Atoi(payload.ChallengeID)
	rid, err2 := strconv.Atoi(payload.ResponderID)
	if err1 != nil || err2 != nil {
		return ChallengeResponse{}, fmt.Errorf("invalid IDs")
	}
	mediaType, ok := mediaTypeOf(payload.MediaType)
	if !ok {
		return ChallengeResponse{}, fmt.Errorf("an answer is a video or a photo")
	}

	// Compute relevance once at upload time so the feed engine can use it
	// for ranking without recomputing per-request.
	challenge, _ := GetChallengeByID(payload.ChallengeID)
	relevance := computeRelevanceScore(challenge, payload.Caption)

	// ── What this answer is, in the responder's own words ──────────────────
	//
	// The same three derivations CreateChallenge runs, against the same
	// helpers, so a battle's two videos are described in one vocabulary
	// rather than two. See content_tags.go.
	//
	// The category falls back to the challenge being answered rather than to
	// a keyword guess, because a response has almost no words of its own to
	// guess from — no prefix, no subject, and usually no caption. Without
	// that fallback the answer half of every battle would be filed as
	// "general". See categoryForResponse.
	tags := normalizeTags(payload.Tags)
	category := categoryForResponse(payload.Category, tags, challenge.Category, payload.Caption)
	// Only what the RESPONDER themselves chose, with the two words that mean
	// "nobody chose" stripped. category above may instead be their tags, the
	// challenge they answered, or a keyword guess — all perfectly good answers
	// and none of them a claim by a person. See migrations/010.
	responderCategory := usableCategory(strings.ToLower(strings.TrimSpace(payload.Category)))
	emotions := emotionsForContent(payload.EmotionTags, tags, "", "", payload.Caption)
	energyLevel := payload.EnergyLevel
	if energyLevel == "" {
		// The responder's own last five videos feed the baseline, same as a
		// creator's do. A first-time responder degrades cleanly — the lookup
		// returns "" and the rest of the signals decide.
		energyLevel = deriveEnergyLevelWithCreator(
			category,
			challenge.Subject,
			challenge.Prefix+" "+challenge.Subject+" "+payload.Caption,
			payload.ResponderID,
		)
	}

	// Same non-nil rule as variantsJSON below: lib/pq turns a nil []byte into
	// SQL NULL, which a JSONB column rejects on this path, so an empty list
	// has to reach the database as [] spelled out.
	tagsJSON, err := json.Marshal(tags)
	if err != nil || len(tags) == 0 {
		tagsJSON = []byte("[]")
	}
	emotionJSON, err := json.Marshal(emotions)
	if err != nil || len(emotions) == 0 {
		emotionJSON = []byte("[]")
	}

	// Marshal variants to JSONB. Default to '{}' (not nil) so lib/pq sends
	// a real empty JSON object instead of NULL — keeps the COALESCE in
	// populateTopResponses' SELECT and any other reader simple.
	variantsJSON := []byte("{}")
	if len(payload.VideoVariants) > 0 {
		if buf, err := json.Marshal(payload.VideoVariants); err == nil {
			variantsJSON = buf
		}
	}

	var id int
	var createdAt time.Time
	err = db.QueryRow(
		`INSERT INTO challenge_responses
			(challenge_id, responder_id, video_url, video_variants, thumbnail_url, duration_ms, caption, relevance_score,
			 category, creator_category, category_source, custom_tags, emotion_tags, energy_level, media_type, music_track_id)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16) RETURNING id, created_at`,
		cid, rid, payload.VideoURL, variantsJSON, payload.ThumbnailURL,
		payload.DurationMs, payload.Caption, relevance,
		category, responderCategory, categorySourceAtUpload(responderCategory, category),
		tagsJSON, emotionJSON, energyLevel, mediaType, payload.musicTrack,
	).Scan(&id, &createdAt)
	if err != nil {
		return ChallengeResponse{}, err
	}
	countMusicUse(payload.musicTrack)

	// Same as CreateChallenge: this answer is waiting to be converted, so
	// start the worker now instead of waiting out the timer. See
	// transcode_wakeup.go. A photo has nothing to convert.
	if payload.VideoURL != "" && mediaType == mediaVideo {
		wakeTranscodeWorker()
	}

	// The battle is on: its clock starts now and runs battle_days. Only the
	// first answer starts it; a later one joins the battle already running.
	// If this fails the answer is saved but the battle never starts, so it
	// has to say so.
	if _, err := db.Exec(`
		UPDATE challenges
		   SET status = 'active',
		       accepted_at = NOW(),
		       voting_ends_at = NOW() + battle_days * INTERVAL '1 day'
		 WHERE id = $1 AND status = 'open'`, cid); err != nil {
		log.Printf("battle %d: answered, but could not be started: %v — it stays "+
			"open with no end until this is fixed", cid, err)
	}

	responder, _ := GetUserByID(payload.ResponderID)

	return ChallengeResponse{
		ID:                strconv.Itoa(id),
		ChallengeID:       payload.ChallengeID,
		ResponderID:       payload.ResponderID,
		ResponderUsername: responder.Username,
		ResponderLeague:   responder.League,
		VideoURL:          payload.VideoURL,
		VideoVariants:     payload.VideoVariants,
		ThumbnailURL:      payload.ThumbnailURL,
		DurationMs:        payload.DurationMs,
		Caption:           payload.Caption,
		RelevanceScore:    relevance,
		CreatedAt:         createdAt.UTC().Format(time.RFC3339),
		Category:          category,
		EmotionTags:       emotions,
		Tags:              tags,
		EnergyLevel:       energyLevel,
	}, nil
}

// ToggleChallengeLike toggles a like on a challenge.
func ToggleChallengeLike(challengeID, userID string) (bool, int) {
	cid, e1 := strconv.Atoi(challengeID)
	uid, e2 := strconv.Atoi(userID)
	if e1 != nil || e2 != nil {
		return false, 0
	}

	var exists bool
	if err := db.QueryRow(`SELECT EXISTS(SELECT 1 FROM challenge_likes WHERE challenge_id=$1 AND user_id=$2)`, cid, uid).Scan(&exists); err != nil {
		queryFailed("ToggleChallengeLike: could not check whether they had already "+
			"acted on this challenge",
			"treating it as not yet done, so the tap may go the wrong way", err)
	}

	if exists {
		db.Exec(`DELETE FROM challenge_likes WHERE challenge_id=$1 AND user_id=$2`, cid, uid)
	} else {
		db.Exec(`INSERT INTO challenge_likes (challenge_id, user_id) VALUES ($1,$2) ON CONFLICT DO NOTHING`, cid, uid)
	}

	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM challenge_likes WHERE challenge_id=$1`, cid).Scan(&count); err != nil {
		queryFailed("ToggleChallengeLike: could not read challenge_likes",
			"carrying on as if the answer were empty", err)
	}
	return !exists, count
}

// ToggleChallengeDislike toggles an explicit dislike on a challenge and
// returns (disliked, dislikeCount, likeCount).
//
// A dislike and a like are mutually exclusive: turning a dislike ON drops
// any existing like row, which is why the like count comes back too — the
// client needs it to re-render the heart without a second round-trip.
// Turning a dislike OFF does NOT restore the like; the user has to press
// the heart again, matching the like/dislike semantics everywhere else.
//
// Runs inside a transaction so the "insert dislike + delete like" pair can
// never half-apply and leave a row in both tables (which would double-count
// the same user as both a positive and negative signal for one challenge).
func ToggleChallengeDislike(challengeID, userID string) (bool, int, int) {
	cid, e1 := strconv.Atoi(challengeID)
	uid, e2 := strconv.Atoi(userID)
	if e1 != nil || e2 != nil {
		return false, 0, 0
	}

	var exists bool
	if err := db.QueryRow(`SELECT EXISTS(SELECT 1 FROM challenge_dislikes WHERE challenge_id=$1 AND user_id=$2)`, cid, uid).Scan(&exists); err != nil {
		queryFailed("ToggleChallengeDislike: could not check whether they had already "+
			"acted on this challenge",
			"treating it as not yet done, so the tap may go the wrong way", err)
	}

	tx, err := db.Begin()
	if err != nil {
		return false, 0, 0
	}
	if exists {
		_, err = tx.Exec(`DELETE FROM challenge_dislikes WHERE challenge_id=$1 AND user_id=$2`, cid, uid)
	} else {
		if _, err = tx.Exec(`INSERT INTO challenge_dislikes (challenge_id, user_id) VALUES ($1,$2) ON CONFLICT DO NOTHING`, cid, uid); err == nil {
			// Disliking clears the like — they can't both be true.
			_, err = tx.Exec(`DELETE FROM challenge_likes WHERE challenge_id=$1 AND user_id=$2`, cid, uid)
		}
	}
	if err != nil {
		tx.Rollback()
		return false, 0, 0
	}
	if err := tx.Commit(); err != nil {
		return false, 0, 0
	}

	var dislikes, likes int
	if err := db.QueryRow(`SELECT COUNT(*) FROM challenge_dislikes WHERE challenge_id=$1`, cid).Scan(&dislikes); err != nil {
		queryFailed("ToggleChallengeDislike: could not read challenge_dislikes",
			"carrying on as if the answer were empty", err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM challenge_likes WHERE challenge_id=$1`, cid).Scan(&likes); err != nil {
		queryFailed("ToggleChallengeDislike: could not read challenge_likes",
			"carrying on as if the answer were empty", err)
	}
	return !exists, dislikes, likes
}

// (GetHomeFeed retired — the home reels feed is now served by SmartFeedHandler
// (challenge-only, with the battles/shorts split). The old "3 challenges then
// 1 post" interleave doesn't apply now that the post entity is gone.)

// ---------------------------------------------------------------------------
// Watch Events CRUD
// ---------------------------------------------------------------------------

// RecordWatchEvent inserts a watch event for analytics. For challenge
// content it ALSO bumps challenges.views so the displayed counter on the
// home reels grows in real time — previously the only path that
// incremented that counter was the challenge-detail page open
// (challenge_handler.go:132), which meant a user could watch hundreds of
// reels without ever moving the number on screen. Capped at one view per
// person per video per day by the video_views table (counts.go) — NOT by
// watch_events, which is only the watch history. Clearing your watch
// history deletes watch_events and leaves every view count as it was.
func RecordWatchEvent(payload WatchEventPayload) error {
	uid, err := strconv.Atoi(payload.UserID)
	if err != nil {
		return fmt.Errorf("invalid user ID")
	}
	cid, err := strconv.Atoi(payload.ContentID)
	if err != nil {
		return fmt.Errorf("invalid content ID")
	}

	_, err = db.Exec(
		`INSERT INTO watch_events (user_id, content_id, content_type, watch_time, completed)
		 VALUES ($1,$2,$3,$4,$5)`,
		uid, cid, payload.ContentType, payload.WatchTime, payload.Completed,
	)
	if err != nil {
		return err
	}

	// The view itself: once a day per person per video, for each video of
	// the card that was on screen long enough — see counts.go.
	if payload.ContentType == "challenge" {
		for _, rid := range videosWatched(cid, payload) {
			recordView(uid, cid, rid)
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Challenge Votes CRUD
// ---------------------------------------------------------------------------

// voteRefusal is a vote the rules do not allow. Status is the HTTP status to
// answer with; the message is shown to the voter as it stands.
type voteRefusal struct {
	status int
	msg    string
}

func (e voteRefusal) Error() string { return e.msg }

// CastVote records a vote in a battle. One vote per person per battle;
// voting again moves it to the side named. (For a while a second vote was
// refused; the owner asked for changing your mind to be allowed again.)
//
// A vote names a side: an answer (ResponseID), or the person who posted the
// challenge (Side "creator"). Until migration 011 there was no way to name the
// creator, so the app sent the CHALLENGE's id as the "response". That failed
// the foreign key, or — when some answer elsewhere had that number — counted
// the vote for a stranger in another battle. The old form is still understood
// so the app already on phones keeps working: a response id that is not an
// answer in this battle but equals the battle's own id means the creator.
//
// Refused: votes in your own battle, votes before anyone has accepted, votes
// after the battle has ended, and votes for an answer from another battle.
func CastVote(payload ChallengeVotePayload) (bool, error) {
	cid, err := strconv.Atoi(payload.ChallengeID)
	if err != nil {
		return false, voteRefusal{status: 400, msg: "invalid challenge id"}
	}
	vid, err := strconv.Atoi(payload.VoterID)
	if err != nil {
		return false, voteRefusal{status: 400, msg: "invalid voter id"}
	}

	var creatorID int
	var status string
	var endsAt, resolvedAt sql.NullTime
	err = db.QueryRow(`
		SELECT creator_id, status, voting_ends_at, resolved_at
		  FROM challenges WHERE id = $1`, cid).Scan(&creatorID, &status, &endsAt, &resolvedAt)
	if err == sql.ErrNoRows {
		return false, voteRefusal{status: 404, msg: "no such battle"}
	}
	if err != nil {
		return false, err
	}
	if status == "removed" {
		return false, voteRefusal{status: 409, msg: "This challenge has been removed."}
	}
	if resolvedAt.Valid || (endsAt.Valid && !endsAt.Time.After(time.Now())) {
		return false, voteRefusal{status: 409, msg: "This battle has ended."}
	}
	if status != "active" {
		return false, voteRefusal{status: 409, msg: "Nobody has accepted this challenge yet, so there is nothing to vote on."}
	}

	var answered bool
	if err := db.QueryRow(`
		SELECT EXISTS (SELECT 1 FROM challenge_responses
		                WHERE challenge_id = $1 AND responder_id = $2)`,
		cid, vid).Scan(&answered); err != nil {
		return false, err
	}
	if vid == creatorID || answered {
		return false, voteRefusal{status: 403, msg: "You can't vote in your own battle."}
	}

	var side any // nil = the creator
	if payload.Side != "creator" {
		rid, err := strconv.Atoi(payload.ResponseID)
		if err != nil {
			return false, voteRefusal{status: 400, msg: "invalid response id"}
		}
		// A hidden answer (hidden by hand or by moderation) is out of the
		// battle, and so is anything cast for it.
		var takenDown bool
		err = db.QueryRow(`
			SELECT COALESCE(is_hidden, FALSE)
			  FROM challenge_responses
			 WHERE id = $1 AND challenge_id = $2`,
			rid, cid).Scan(&takenDown)
		inThisBattle := err == nil
		if err != nil && err != sql.ErrNoRows {
			return false, err
		}
		switch {
		case inThisBattle && takenDown:
			return false, voteRefusal{status: 409, msg: "That video has been removed from this battle."}
		case inThisBattle:
			side = rid
		case rid == cid:
			// The app's old way of saying "the creator".
		default:
			return false, voteRefusal{status: 400, msg: "That answer is not part of this battle."}
		}
	}

	// One row per voter, so a second vote moves the first rather than
	// adding to it — even two taps arriving together.
	if _, err := db.Exec(
		`INSERT INTO challenge_votes (challenge_id, response_id, voter_id)
		 VALUES ($1,$2,$3)
		 ON CONFLICT (challenge_id, voter_id)
		 DO UPDATE SET response_id = EXCLUDED.response_id`,
		cid, side, vid,
	); err != nil {
		return false, err
	}
	return true, nil
}

// viewerVote is the side [voterID] voted for in battle [challengeID]:
// "creator", the answer's id, or "" if they have not voted (or it could not
// be read — said in the log).
func viewerVote(challengeID, voterID int) string {
	var side string
	err := db.QueryRow(`
		SELECT COALESCE(response_id::text, 'creator')
		  FROM challenge_votes
		 WHERE challenge_id = $1 AND voter_id = $2`,
		challengeID, voterID).Scan(&side)
	if err == sql.ErrNoRows {
		return ""
	}
	if queryFailed("reading which side someone voted for",
		"showing them as not voted", err) {
		return ""
	}
	return side
}

// GetVoteSummary returns vote counts per side for a challenge, as cast. The
// creator's side is reported under the challenge's own id, which is how the
// app has always named it. These are raw counts; which of them are genuine is
// the standings' job — see loadBattleStandings.
func GetVoteSummary(challengeID string) []VoteSummary {
	cid, err := strconv.Atoi(challengeID)
	if err != nil {
		return nil
	}

	rows, err := db.Query(
		`SELECT COALESCE(cv.response_id::text, $2) AS side,
		        COALESCE(ru.username, cu.username) AS username,
		        COUNT(*) AS votes
		   FROM challenge_votes cv
		   JOIN challenges c  ON c.id = cv.challenge_id
		   JOIN users cu      ON cu.id = c.creator_id
		   LEFT JOIN challenge_responses cr ON cr.id = cv.response_id
		   LEFT JOIN users ru ON ru.id = cr.responder_id
		  WHERE cv.challenge_id = $1
		  GROUP BY 1, 2
		  ORDER BY votes DESC`, cid, challengeID,
	)
	if queryFailed("vote summary for "+challengeID, "shown as no votes yet", err) {
		return nil
	}
	defer rows.Close()

	var result []VoteSummary
	seen := 0
	for rows.Next() {
		var side, username string
		var votes int
		if scanFailed("vote summary for "+challengeID, rows.Scan(&side, &username, &votes), &seen) {
			continue
		}
		result = append(result, VoteSummary{ResponseID: side, Username: username, Votes: votes})
	}
	return result
}

// ---------------------------------------------------------------------------
// Reports CRUD
// ---------------------------------------------------------------------------

// CreateReport keeps a report. [owner] is the account behind what was
// reported (reportedOwner), kept for the review list (moderation.go).
func CreateReport(payload ReportPayload, owner int) (Report, error) {
	reporterID, err := strconv.Atoi(payload.ReporterID)
	if err != nil {
		return Report{}, fmt.Errorf("invalid reporter ID")
	}
	targetID, err := strconv.Atoi(payload.TargetID)
	if err != nil {
		return Report{}, fmt.Errorf("invalid target ID")
	}

	var id int
	var createdAt time.Time
	err = db.QueryRow(
		`INSERT INTO reports (reporter_id, target_id, target_type, reason, description, owner_id)
		 VALUES ($1,$2,$3,$4,$5,$6) RETURNING id, created_at`,
		reporterID, targetID, payload.TargetType, payload.Reason, payload.Description, owner,
	).Scan(&id, &createdAt)
	if err != nil {
		return Report{}, err
	}

	return Report{
		ID:          strconv.Itoa(id),
		ReporterID:  payload.ReporterID,
		TargetID:    payload.TargetID,
		TargetType:  payload.TargetType,
		Reason:      payload.Reason,
		Description: payload.Description,
		Status:      "pending",
		CreatedAt:   createdAt.UTC().Format(time.RFC3339),
	}, nil
}

// --------------------------------------------------------------------------
// Chat
// --------------------------------------------------------------------------

// SendChatMessage inserts a message and returns its ID. [media] is a photo
// or voice note (checkChatMedia has already checked it); a text message
// passes ChatMedia{}.
func SendChatMessage(senderID, receiverID int, message string, replyToID *int, media ChatMedia) (int, error) {
	if media.Kind == "" {
		media.Kind = chatKindText
	}
	var id int
	err := db.QueryRow(
		`INSERT INTO chat_messages (sender_id, receiver_id, message, reply_to_id,
		        kind, media_url, media_duration_ms, media_width, media_height, media_waveform,
		        shared_challenge_id, shared_response_id)
		 VALUES ($1, $2, $3, $4, $5, NULLIF($6, ''), NULLIF($7, 0), NULLIF($8, 0), NULLIF($9, 0), NULLIF($10, ''),
		         NULLIF($11, 0), NULLIF($12, 0))
		 RETURNING id`,
		senderID, receiverID, message, replyToID,
		media.Kind, media.URL, media.DurationMs, media.Width, media.Height, waveformText(media.Waveform),
		media.ChallengeID, media.ResponseID,
	).Scan(&id)
	return id, err
}

// GetChatMessages returns messages between two users, newest first.
//
// userA is the person reading: a chat they deleted shows only what came
// after (migration 016).
func GetChatMessages(userA, userB, limit, offset int) []ChatMessage {
	rows, err := db.Query(
		`SELECT m.id, m.sender_id, s.username, m.receiver_id, r.username,
				m.message, m.is_read,
				COALESCE(m.status, 'sent') AS status,
				COALESCE(m.is_edited, FALSE) AS is_edited,
				COALESCE(m.is_deleted, FALSE) AS is_deleted,
				m.reply_to_id,
				COALESCE(rm.message, ''), COALESCE(rm.kind, 'text'),
				m.created_at,
				COALESCE(m.kind, 'text'), COALESCE(m.media_url, ''),
				COALESCE(m.media_duration_ms, 0), COALESCE(m.media_width, 0),
				COALESCE(m.media_height, 0), COALESCE(m.media_waveform, ''),
				COALESCE(m.shared_challenge_id, 0), COALESCE(m.shared_response_id, 0)
		 FROM chat_messages m
		 JOIN users s ON m.sender_id = s.id
		 JOIN users r ON m.receiver_id = r.id
		 LEFT JOIN chat_messages rm ON rm.id = m.reply_to_id
		 WHERE ((m.sender_id = $1 AND m.receiver_id = $2)
		    OR (m.sender_id = $2 AND m.receiver_id = $1))
		   -- Nothing from a chat with an account waiting to be deleted.
		   AND s.deletion_requested_at IS NULL AND r.deletion_requested_at IS NULL
		   AND s.suspended_at IS NULL AND r.suspended_at IS NULL
		   AND m.created_at > COALESCE(
		         (SELECT cleared_at FROM chat_cleared WHERE user_id = $1 AND other_id = $2),
		         '-infinity'::timestamptz)
		 ORDER BY m.created_at DESC
		 LIMIT $3 OFFSET $4`,
		userA, userB, limit, offset,
	)
	if queryFailed(fmt.Sprintf("reading the chat between users %d and %d", userA, userB),
		"the chat shows empty", err) {
		return nil
	}
	defer rows.Close()

	var result []ChatMessage
	bad := 0
	for rows.Next() {
		var id, sID, rID int
		var sName, rName, msg, status string
		var isRead, isEdited, isDeleted bool
		var replyToID *int
		var replyToText, replyToKind string
		var createdAt time.Time
		var kind, mediaURL, waveform string
		var durationMs, width, height, sharedCID, sharedRID int
		if !scanFailed(fmt.Sprintf("a message between users %d and %d", userA, userB),
			rows.Scan(&id, &sID, &sName, &rID, &rName, &msg, &isRead,
				&status, &isEdited, &isDeleted, &replyToID, &replyToText, &replyToKind, &createdAt,
				&kind, &mediaURL, &durationMs, &width, &height, &waveform,
				&sharedCID, &sharedRID), &bad) {
			cm := ChatMessage{
				ID:                strconv.Itoa(id),
				SenderID:          strconv.Itoa(sID),
				SenderUsername:    sName,
				ReceiverID:        strconv.Itoa(rID),
				ReceiverUsername:  rName,
				Message:           msg,
				IsRead:            isRead,
				Status:            status,
				IsEdited:          isEdited,
				IsDeleted:         isDeleted,
				CreatedAt:         createdAt.UTC().Format(time.RFC3339),
				Kind:              kind,
				MediaURL:          mediaURL,
				MediaDurationMs:   durationMs,
				MediaWidth:        width,
				MediaHeight:       height,
				Waveform:          waveformFrom(waveform),
				sharedChallengeID: sharedCID,
				sharedResponseID:  sharedRID,
			}
			if replyToID != nil {
				cm.ReplyToID = strconv.Itoa(*replyToID)
				cm.ReplyToText = chatPreview(replyToKind, replyToText)
			}
			result = append(result, cm)
		}
	}
	// Shared videos, as the reader may see them.
	attachShares(userA, result)
	return result
}

// MarkMessagesRead marks all messages from sender to receiver as read, and
// says how many that was. Zero means there was nothing new to read, so
// there is nobody to tell (see markReadAndTell).
func MarkMessagesRead(senderID, receiverID int) int64 {
	res, err := db.Exec(
		`UPDATE chat_messages SET is_read = TRUE, status = 'read'
		 WHERE sender_id = $1 AND receiver_id = $2 AND is_read = FALSE`,
		senderID, receiverID,
	)
	if queryFailed(fmt.Sprintf("marking messages from user %d to user %d as read", senderID, receiverID),
		"they stay unread, and the sender keeps seeing Sent", err) || res == nil {
		return 0
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0
	}
	return n
}

// GetConversations returns the list of users the given user has chatted with.
func GetConversations(userID int) []Conversation {
	// Step 1: Find all unique conversation partners
	partnerRows, err := db.Query(
		`SELECT DISTINCT
			CASE WHEN sender_id = $1 THEN receiver_id ELSE sender_id END AS other_id
		 FROM chat_messages
		 WHERE sender_id = $1 OR receiver_id = $1`,
		userID,
	)
	if err != nil {
		log.Printf("GetConversations partners error: %v", err)
		return nil
	}
	defer partnerRows.Close()

	var partnerIDs []int
	for partnerRows.Next() {
		var pid int
		if partnerRows.Scan(&pid) == nil {
			partnerIDs = append(partnerIDs, pid)
		}
	}

	if len(partnerIDs) == 0 {
		return []Conversation{}
	}

	// Step 2: For each partner, get their info, last message, and unread count
	result := make([]Conversation, 0, len(partnerIDs))
	// When each chat's last message was, to the microsecond: the list puts
	// the newest on top, and two messages in the same second must still
	// come in the order they were sent.
	var lastAt []time.Time
	for _, pid := range partnerIDs {
		var username, league string
		// A chat with an account waiting to be deleted is not on the list
		// (account_leaving.go); it comes back if they keep the account.
		err := db.QueryRow(`SELECT username, league FROM users
		                     WHERE id=$1 AND deletion_requested_at IS NULL AND suspended_at IS NULL`, pid).Scan(&username, &league)
		if err != nil {
			continue
		}

		var lastMsg, lastStatus, lastKind string
		var lastTime time.Time
		var lastFromMe bool
		// Only what came after this person last deleted the chat: a chat
		// they deleted, with nothing new since, is not on their list.
		err = db.QueryRow(
			`SELECT message, created_at, sender_id = $1,
			        CASE WHEN is_read THEN 'read' ELSE COALESCE(status, 'sent') END,
			        COALESCE(kind, 'text')
			 FROM chat_messages
			 WHERE ((sender_id=$1 AND receiver_id=$2) OR (sender_id=$2 AND receiver_id=$1))
			   AND created_at > COALESCE(
			         (SELECT cleared_at FROM chat_cleared WHERE user_id = $1 AND other_id = $2),
			         '-infinity'::timestamptz)
			 ORDER BY created_at DESC LIMIT 1`,
			userID, pid,
		).Scan(&lastMsg, &lastTime, &lastFromMe, &lastStatus, &lastKind)
		if err != nil {
			if !errors.Is(err, sql.ErrNoRows) {
				queryFailed(fmt.Sprintf("reading the last message between users %d and %d", userID, pid),
					"that chat is left off the list", err)
			}
			continue
		}

		var unread int
		if err := db.QueryRow(
			`SELECT COUNT(*) FROM chat_messages
			WHERE sender_id=$1 AND receiver_id=$2 AND is_read=FALSE
			  AND created_at > COALESCE(
			        (SELECT cleared_at FROM chat_cleared WHERE user_id = $2 AND other_id = $1),
			        '-infinity'::timestamptz)`,
			pid, userID,
		).Scan(&unread); err != nil {
			queryFailed("GetConversations: could not read chat_messages",
				"carrying on as if the answer were empty", err)
		}

		result = append(result, Conversation{
			UserID:      strconv.Itoa(pid),
			Username:    username,
			League:      league,
			LastMessage: chatPreview(lastKind, lastMsg),
			LastTime:    lastTime.UTC().Format(time.RFC3339),
			UnreadCount: unread,
			LastFromMe:  lastFromMe,
			LastStatus:  lastStatus,
		})
		lastAt = append(lastAt, lastTime)
	}

	// Newest conversation first, the way every chat app orders its list.
	order := make([]int, len(result))
	for i := range order {
		order[i] = i
	}
	sort.SliceStable(order, func(a, b int) bool {
		return lastAt[order[a]].After(lastAt[order[b]])
	})
	sorted := make([]Conversation, len(result))
	for i, k := range order {
		sorted[i] = result[k]
	}
	return sorted
}

// ---------------------------------------------------------------------------
// Challenge Comments CRUD
// ---------------------------------------------------------------------------

// AddChallengeComment inserts a new comment on a challenge.
func AddChallengeComment(challengeID, authorID, authorUsername, text string) (ChallengeComment, error) {
	cid, err := strconv.Atoi(challengeID)
	if err != nil {
		return ChallengeComment{}, fmt.Errorf("invalid challenge ID")
	}
	aid, err := strconv.Atoi(authorID)
	if err != nil {
		return ChallengeComment{}, fmt.Errorf("invalid author ID")
	}

	var id int
	var createdAt time.Time
	err = db.QueryRow(
		`INSERT INTO challenge_comments (challenge_id, author_id, text)
		 VALUES ($1,$2,$3) RETURNING id, created_at`,
		cid, aid, text,
	).Scan(&id, &createdAt)
	if err != nil {
		return ChallengeComment{}, err
	}

	return ChallengeComment{
		ID:             strconv.Itoa(id),
		ChallengeID:    challengeID,
		AuthorID:       authorID,
		AuthorUsername: authorUsername,
		Text:           text,
		CreatedAt:      createdAt.UTC().Format(time.RFC3339),
	}, nil
}

// GetChallengeComments fetches all comments for a challenge, oldest first.
func GetChallengeComments(challengeID string) []ChallengeComment {
	cid, err := strconv.Atoi(challengeID)
	if err != nil {
		return nil
	}

	rows, err := db.Query(
		`SELECT cc.id, cc.challenge_id, cc.author_id, u.username, cc.text, cc.created_at
		 FROM challenge_comments cc
		 JOIN users u ON cc.author_id = u.id
		 WHERE cc.challenge_id = $1
		 ORDER BY cc.created_at ASC`, cid,
	)
	if err != nil {
		return nil
	}
	defer rows.Close()

	var result []ChallengeComment
	for rows.Next() {
		var commentID, challengeIDInt, authorID int
		var username, text string
		var createdAt time.Time
		if rows.Scan(&commentID, &challengeIDInt, &authorID, &username, &text, &createdAt) == nil {
			result = append(result, ChallengeComment{
				ID:             strconv.Itoa(commentID),
				ChallengeID:    strconv.Itoa(challengeIDInt),
				AuthorID:       strconv.Itoa(authorID),
				AuthorUsername: username,
				Text:           text,
				CreatedAt:      createdAt.UTC().Format(time.RFC3339),
			})
		}
	}
	return result
}

// ---------------------------------------------------------------------------
// Saved Challenges
// ---------------------------------------------------------------------------

// ToggleSaveChallenge saves or unsaves a challenge for a user. Returns (saved, error).
func ToggleSaveChallenge(userID, challengeID string) (bool, error) {
	uid, err := strconv.Atoi(userID)
	if err != nil {
		return false, fmt.Errorf("invalid user ID")
	}
	cid, err := strconv.Atoi(challengeID)
	if err != nil {
		return false, fmt.Errorf("invalid challenge ID")
	}

	var exists bool
	if err := db.QueryRow(`SELECT EXISTS(SELECT 1 FROM saved_challenges WHERE user_id=$1 AND challenge_id=$2)`, uid, cid).Scan(&exists); err != nil {
		queryFailed("ToggleSaveChallenge: could not check whether they had already "+
			"acted on this challenge",
			"treating it as not yet done, so the tap may go the wrong way", err)
	}

	if exists {
		db.Exec(`DELETE FROM saved_challenges WHERE user_id=$1 AND challenge_id=$2`, uid, cid)
		return false, nil
	}
	_, err = db.Exec(`INSERT INTO saved_challenges (user_id, challenge_id) VALUES ($1,$2)`, uid, cid)
	return true, err
}

// GetSavedChallenges returns all saved challenges for a user.
func GetSavedChallenges(userID string) []Challenge {
	uid, err := strconv.Atoi(userID)
	if err != nil {
		return nil
	}

	rows, err := db.Query(
		`SELECT c.id, c.creator_id, u.username, u.league, c.video_url, c.thumbnail_url,
		        c.prefix, c.subject, c.visibility, c.status, c.views, c.created_at,
		        (SELECT COUNT(*) FROM challenge_likes WHERE challenge_id=c.id) AS likes,
		        `+visibleAnswersOfC+` AS response_count
		 FROM saved_challenges sc
		 JOIN challenges c ON sc.challenge_id = c.id
		 JOIN users u ON c.creator_id = u.id
		 WHERE sc.user_id = $1
		   AND u.deletion_requested_at IS NULL AND u.suspended_at IS NULL
		 ORDER BY sc.created_at DESC`, uid,
	)
	if err != nil {
		return nil
	}
	defer rows.Close()

	var result []Challenge
	for rows.Next() {
		var c Challenge
		var cid, creatorID, views, likes, respCount int
		var createdAt time.Time
		if rows.Scan(&cid, &creatorID, &c.CreatorUsername, &c.CreatorLeague,
			&c.VideoURL, &c.ThumbnailURL, &c.Prefix, &c.Subject,
			&c.Visibility, &c.Status, &views, &createdAt, &likes, &respCount) == nil {
			c.ID = strconv.Itoa(cid)
			c.CreatorID = strconv.Itoa(creatorID)
			c.Views = views
			c.Likes = likes
			c.ResponseCount = respCount
			c.CreatedAt = createdAt.UTC().Format(time.RFC3339)
			result = append(result, c)
		}
	}
	return result
}

// ---------------------------------------------------------------------------
// Chat Message Operations
// ---------------------------------------------------------------------------

// EditChatMessage updates the text of a message (only by sender, within 15 minutes).
func EditChatMessage(msgID int, senderID int, newText string) error {
	result, err := db.Exec(
		`UPDATE chat_messages SET message=$1, is_edited=TRUE, edited_at=NOW()
		 WHERE id=$2 AND sender_id=$3 AND is_deleted=FALSE
		   AND COALESCE(kind, 'text') <> 'voice'
		   AND created_at > NOW() - INTERVAL '15 minutes'`,
		newText, msgID, senderID,
	)
	if err != nil {
		return err
	}
	rows, _ := result.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("message not found, not yours, a voice message, or edit window expired (15 min)")
	}
	return nil
}

// DeleteChatMessage soft-deletes a message (unsend for everyone). A photo
// or voice note goes too: the message stops pointing at it, and the file is
// cleared from storage unless a forwarded copy still shows it.
func DeleteChatMessage(msgID int, senderID int) error {
	var oldURL string
	err := db.QueryRow(
		`WITH old AS (
		     SELECT id, COALESCE(media_url, '') AS media_url FROM chat_messages
		      WHERE id = $1 AND sender_id = $2
		 )
		 UPDATE chat_messages m
		    SET is_deleted = TRUE, message = 'This message was deleted',
		        kind = 'text', media_url = NULL, media_duration_ms = NULL,
		        media_width = NULL, media_height = NULL, media_waveform = NULL,
		        shared_challenge_id = NULL, shared_response_id = NULL
		   FROM old
		  WHERE m.id = old.id
		 RETURNING old.media_url`,
		msgID, senderID,
	).Scan(&oldURL)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("message not found or not yours")
	}
	if err != nil {
		return err
	}
	go forgetChatFile(oldURL)
	return nil
}

// UpdateUserLastSeen stamps account [userID] as seen now. By id, not by name:
// a connection closing just after a username change must not stamp whoever
// takes the old name next (username_change.go).
func UpdateUserLastSeen(userID string) {
	_, err := db.Exec(`UPDATE users SET last_seen = NOW() WHERE id::text = $1`, userID)
	queryFailed("stamping account "+userID+" as last seen", "it keeps the time it had", err)
}

// GetUserLastSeen returns the last_seen timestamp for a user.
func GetUserLastSeen(username string) string {
	var lastSeen time.Time
	err := db.QueryRow(`SELECT COALESCE(last_seen, NOW()) FROM users WHERE username=$1`, username).Scan(&lastSeen)
	if err != nil {
		return ""
	}
	return lastSeen.UTC().Format(time.RFC3339)
}
