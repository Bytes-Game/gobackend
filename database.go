package main

import (
	"crypto/subtle"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
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

// InitDatabase connects to PostgreSQL, runs schema migrations, and seeds
// sample data if the tables are empty.
func InitDatabase() {
	connStr := os.Getenv("DATABASE_URL")
	if connStr == "" {
		log.Fatal("DATABASE_URL environment variable is required")
	}

	var err error
	db, err = sql.Open("postgres", connStr)
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}

	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	if err = db.Ping(); err != nil {
		log.Fatalf("Failed to ping database: %v", err)
	}
	log.Println("Connected to PostgreSQL")

	runMigrations()
	seedIfEmpty()
}

// runMigrations creates all required tables idempotently.
func runMigrations() {
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
	`

	if _, err := db.Exec(schema); err != nil {
		log.Fatalf("Migration failed: %v", err)
	}

	// Add new columns to existing tables safely
	alterStmts := `
	DO $$ BEGIN ALTER TABLE chat_messages ADD COLUMN status VARCHAR(20) DEFAULT 'sent'; EXCEPTION WHEN duplicate_column THEN NULL; END $$;
	DO $$ BEGIN ALTER TABLE chat_messages ADD COLUMN reply_to_id INT REFERENCES chat_messages(id) ON DELETE SET NULL; EXCEPTION WHEN duplicate_column THEN NULL; END $$;
	DO $$ BEGIN ALTER TABLE chat_messages ADD COLUMN is_edited BOOLEAN DEFAULT FALSE; EXCEPTION WHEN duplicate_column THEN NULL; END $$;
	DO $$ BEGIN ALTER TABLE chat_messages ADD COLUMN is_deleted BOOLEAN DEFAULT FALSE; EXCEPTION WHEN duplicate_column THEN NULL; END $$;
	DO $$ BEGIN ALTER TABLE chat_messages ADD COLUMN edited_at TIMESTAMPTZ; EXCEPTION WHEN duplicate_column THEN NULL; END $$;
	DO $$ BEGIN ALTER TABLE users ADD COLUMN last_seen TIMESTAMPTZ DEFAULT NOW(); EXCEPTION WHEN duplicate_column THEN NULL; END $$;
	DO $$ BEGIN ALTER TABLE users ADD COLUMN password_hash VARCHAR(255) NOT NULL DEFAULT ''; EXCEPTION WHEN duplicate_column THEN NULL; END $$;
	DO $$ BEGIN ALTER TABLE challenges ADD COLUMN category VARCHAR(30) DEFAULT 'other'; EXCEPTION WHEN duplicate_column THEN NULL; END $$;
	DO $$ BEGIN ALTER TABLE challenges ADD COLUMN emotion_tags JSONB DEFAULT '[]'; EXCEPTION WHEN duplicate_column THEN NULL; END $$;
	DO $$ BEGIN ALTER TABLE challenges ADD COLUMN energy_level VARCHAR(10) DEFAULT 'medium'; EXCEPTION WHEN duplicate_column THEN NULL; END $$;
	DO $$ BEGIN ALTER TABLE posts ADD COLUMN category VARCHAR(30) DEFAULT 'other'; EXCEPTION WHEN duplicate_column THEN NULL; END $$;
	DO $$ BEGIN ALTER TABLE posts ADD COLUMN emotion_tags JSONB DEFAULT '[]'; EXCEPTION WHEN duplicate_column THEN NULL; END $$;
	DO $$ BEGIN ALTER TABLE posts ADD COLUMN energy_level VARCHAR(10) DEFAULT 'medium'; EXCEPTION WHEN duplicate_column THEN NULL; END $$;
	DO $$ BEGIN ALTER TABLE challenges ADD COLUMN custom_tags JSONB DEFAULT '[]'; EXCEPTION WHEN duplicate_column THEN NULL; END $$;
	DO $$ BEGIN ALTER TABLE posts ADD COLUMN custom_tags JSONB DEFAULT '[]'; EXCEPTION WHEN duplicate_column THEN NULL; END $$;

	-- Multi-bitrate video variants. Maps quality label → CDN URL.
	-- video_url stays as the canonical/default URL so legacy readers keep
	-- working; video_variants is the new path for adaptive playback.
	DO $$ BEGIN ALTER TABLE challenges          ADD COLUMN video_variants JSONB DEFAULT '{}'; EXCEPTION WHEN duplicate_column THEN NULL; END $$;
	DO $$ BEGIN ALTER TABLE challenge_responses ADD COLUMN video_variants JSONB DEFAULT '{}'; EXCEPTION WHEN duplicate_column THEN NULL; END $$;

	-- HLS master manifest URL produced by the background transcode worker.
	-- Empty string means "worker hasn't transcoded this challenge yet" —
	-- clients fall back to video_url / video_variants until the column
	-- gets populated by the worker callback (POST /api/challenges/:id/hls-ready).
	--
	-- Stored as TEXT (not VARCHAR(N)) because R2 + custom-domain URLs can
	-- be long once query params for cache-busting land. Default '' keeps
	-- the column safe to NOT NULL filter on without a separate IS NULL leg.
	DO $$ BEGIN ALTER TABLE challenges          ADD COLUMN hls_manifest_url TEXT DEFAULT ''; EXCEPTION WHEN duplicate_column THEN NULL; END $$;
	DO $$ BEGIN ALTER TABLE challenge_responses ADD COLUMN hls_manifest_url TEXT DEFAULT ''; EXCEPTION WHEN duplicate_column THEN NULL; END $$;
	-- Partial index so the transcode worker's "find next untranscoded
	-- challenge" query is a fast index scan instead of a sequential one.
	-- Filtering on the empty string is functionally the same as IS NULL
	-- for our purposes and matches the DEFAULT above.
	DO $$ BEGIN
		CREATE INDEX IF NOT EXISTS challenges_pending_hls_idx
			ON challenges (created_at)
			WHERE hls_manifest_url = '';
	EXCEPTION WHEN duplicate_table THEN NULL; END $$;

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
	`); err != nil {
		log.Printf("pgvector not enabled (%v) — embedding retrieval uses in-process cosine rerank", err)
	} else {
		pgvectorAvailable = true
		log.Println("pgvector enabled: challenges.embedding ready for ANN retrieval")
	}

	log.Println("Database migrations completed")
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
func readUser(id int, username, pw, fullName, bio, visibility, settingsJSON string, wins, losses int, league string) User {
	u := User{
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
	_ = db.QueryRow(`SELECT COUNT(*) FROM follows WHERE following_id = $1`, id).Scan(&u.Followers)

	// Following list (string IDs for JSON compatibility)
	rows, err := db.Query(`SELECT following_id FROM follows WHERE follower_id = $1`, id)
	if err == nil {
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
	var id, wins, losses int
	var uname, pw, fullName, bio, visibility, settings, league string
	err := db.QueryRow(
		`SELECT id, username, password, full_name,
		        COALESCE(bio,''), COALESCE(visibility,'public'),
		        COALESCE(settings::text,'{}'),
		        wins, losses, league
		   FROM users WHERE username = $1`,
		username,
	).Scan(&id, &uname, &pw, &fullName, &bio, &visibility, &settings, &wins, &losses, &league)
	if err != nil {
		return User{}, false
	}
	return readUser(id, uname, pw, fullName, bio, visibility, settings, wins, losses, league), true
}

// GetUserByID returns a fully enriched user, looked up by string ID.
func GetUserByID(idStr string) (User, bool) {
	idInt, err := strconv.Atoi(idStr)
	if err != nil {
		return User{}, false
	}
	var wins, losses int
	var uname, pw, fullName, bio, visibility, settings, league string
	err = db.QueryRow(
		`SELECT id, username, password, full_name,
		        COALESCE(bio,''), COALESCE(visibility,'public'),
		        COALESCE(settings::text,'{}'),
		        wins, losses, league
		   FROM users WHERE id = $1`,
		idInt,
	).Scan(&idInt, &uname, &pw, &fullName, &bio, &visibility, &settings, &wins, &losses, &league)
	if err != nil {
		return User{}, false
	}
	return readUser(idInt, uname, pw, fullName, bio, visibility, settings, wins, losses, league), true
}

// UserExists checks whether a username is already taken.
func UserExists(username string) bool {
	var exists bool
	db.QueryRow(`SELECT EXISTS(SELECT 1 FROM users WHERE username = $1`, username).Scan(&exists)
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
func GetAllUsers() []User {
	rows, err := db.Query(`SELECT id, username, full_name, wins, losses, league FROM users ORDER BY id`)
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
		   FROM users ORDER BY id LIMIT $1 OFFSET $2`,
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
	db.QueryRow(`SELECT EXISTS(SELECT 1 FROM post_likes WHERE post_id=$1 AND user_id=$2)`, pid, uid).Scan(&exists)

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

// -----------------------------------------------------------------------------
// Seed data (runs once when tables are empty)
// -----------------------------------------------------------------------------

func seedIfEmpty() {
	var count int
	db.QueryRow(`SELECT COUNT(*) FROM users`).Scan(&count)
	if count > 0 {
		log.Println("Database already has data - skipping seed")
		return
	}

	log.Println("Seeding database with initial data...")
	seedUsers()
	seedFollows()
	seedPosts()
	seedComments()
	seedChallenges()
	log.Println("Seeding complete")
}

// ReseedDatabase drops all data and re-seeds from scratch.
func ReseedDatabase() {
	log.Println("Reseeding database...")
	// TRUNCATE with RESTART IDENTITY resets all SERIAL counters to 1
	db.Exec(`TRUNCATE TABLE
		chat_messages,
		challenge_votes, challenge_response_likes, challenge_likes, challenge_responses,
		challenge_visible_to, challenges,
		comments, post_likes, posts, follows, watch_events, reports,
		users
		RESTART IDENTITY CASCADE`)
	seedUsers()
	seedFollows()
	seedPosts()
	seedComments()
	seedChallenges()
	log.Println("Reseed complete")
}

func seedUsers() {
	type su struct {
		username, password, fullName, league string
		wins, losses                         int
	}
	data := []su{
		{"player1", "pass1", "Player One", "Gold", 32, 18},
		{"player2", "pass2", "Player Two", "Diamond", 120, 45},
		{"player3", "pass3", "Player Three", "Bronze", 10, 5},
		{"shadowstrike", "pass4", "Shadow Strike", "Platinum", 95, 30},
		{"blazerunner", "pass5", "Blaze Runner", "Silver", 55, 40},
		{"stormchaser", "pass6", "Storm Chaser", "Gold", 78, 22},
		{"frostbyte", "pass7", "Frost Byte", "Dianond", 140, 50},
		{"nightowl", "pass8", "Night Owl", "Bronze", 15, 10},
		{"thunderbolt", "pass9", "Thunder Bolt", "Silver", 62, 38},
		{"cyberking", "pass10", "Cyber King", "Platinum", 100, 25},
	}
	for _, u := range data {
		// Seed with a bcrypt hash and an empty plaintext column so a fresh DB
		// never has a password at rest. Dev still logs in with the plaintext
		// values above (pass1…pass10) — they're just hashed when stored.
		hash, err := hashPassword(u.password)
		if err != nil {
			log.Printf("seed: hashing password for %s failed: %v", u.username, err)
			continue
		}
		db.Exec(
			`INSERT INTO users (username, password, password_hash, full_name, wins, losses, league)
			 VALUES ($1,'',$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING`,
			u.username, hash, u.fullName, u.wins, u.losses, u.league,
		)
	}
}

func seedFollows() {
	fm := map[int][]int{
		1: {2, 3, 4}, 2: {1, 5}, 3: {1, 2}, 4: {1, 2, 7},
		5: {2, 4, 6}, 6: {1, 5, 7}, 7: {2, 4, 6, 8},
		8: {1, 7}, 9: {4, 7, 10}, 10: {1, 2, 4, 7},
	}
	for f, list := range fm {
		for _, t := range list {
			db.Exec(`INSERT INTO follows (follower_id, following_id) VALUES ($1,$2) ON CONFLICT DO NOTHING`, f, t)
		}
	}
}

// seedAnchorTime is the "newest item" reference point in the hardcoded
// seed timestamps. Any timestamp older than this is converted into a
// relative offset from seedAnchorTime, then re-anchored to "now" at insert
// time so the seed always looks recent regardless of when the server boots.
//
// Why: hardcoded absolute timestamps go stale. Every candidate-source
// query filters by `created_at > NOW() - INTERVAL 'N days'`. If the seed
// timestamps are 30 days old but the windows are 14 days, the entire feed
// pipeline returns empty, and the user sees "Nothing to play just yet."
// This indirection eliminates the staleness problem permanently.
var seedAnchorTime = time.Date(2026, 4, 2, 14, 0, 0, 0, time.UTC) // newest hardcoded ts

// freshTimestamp converts a hardcoded seed timestamp into one anchored to
// the current wall clock. The offset (anchor − original) is preserved so
// relative ordering and spacing are intact, but the entire timeline is
// shifted forward so the newest item is "now" and everything else is
// older relative to that.
//
// Falls back to time.Now() if the input doesn't parse — never returns
// a zero time that would break the schema.
func freshTimestamp(rawIso string) time.Time {
	t, err := time.Parse(time.RFC3339, rawIso)
	if err != nil {
		return time.Now()
	}
	offset := seedAnchorTime.Sub(t)
	if offset < 0 {
		offset = 0
	}
	return time.Now().Add(-offset)
}

func seedPosts() {
	type sp struct {
		authorID                                    int
		postType, contentURL, thumbnailURL, caption string
		views                                       int
		createdAt                                   string
	}
	// Video pool — real, accessible URLs we cycle through to give every
	// "video"-type post a playable source. Without this the reels feed
	// displays black frames for the majority of items.
	vids := []struct{ url, thumb string }{
		{"https://cdn.pixabay.com/video/2026/02/09/333600_small.mp4", "https://cdn.pixabay.com/video/2026/02/09/333600_small.jpg"},
		{"https://cdn.pixabay.com/video/2026/02/15/334716_small.mp4", "https://cdn.pixabay.com/video/2026/02/15/334716_small.jpg"},
		{"https://cdn.pixabay.com/video/2026/01/09/326739_small.mp4", "https://cdn.pixabay.com/video/2026/01/09/326739_small.jpg"},
		{"https://cdn.pixabay.com/video/2026/01/19/328740_small.mp4", "https://cdn.pixabay.com/video/2026/01/19/328740_small.jpg"},
		{"https://flutter.github.io/assets-for-api-docs/assets/videos/bee.mp4", "https://flutter.github.io/assets-for-api-docs/assets/videos/butterfly.mp4"},
		{"https://media.w3.org/2010/05/sintel/trailer.mp4", "https://media.w3.org/2010/05/sintel/poster.png"},
		{"https://test-videos.co.uk/vids/bigbuckbunny/mp4/h264/360/Big_Buck_Bunny_360_10s_1MB.mp4", "https://test-videos.co.uk/vids/bigbuckbunny/poster.jpg"},
		{"https://cdn.plyr.io/static/demo/View_From_A_Blue_Moon_Trailer-576p.mp4", "https://cdn.plyr.io/static/demo/View_From_A_Blue_Moon_Trailer-HD.jpg"},
	}
	// Image pool — same idea for "image"-type posts.
	imgs := []string{
		"https://pixabay.com/get/g1cb3fb5e78308321688cd47550266beec73006700f8fd3f16549b183bd601669b6128582de0d1c92a4800ae0109174150e040eaeba050fdae70980c53ef8761c_1280.jpg",
		"https://pixabay.com/get/g6d2fb7b4dde02b4febf151624eaf6ee7f096bc81be660c3da10483538d9a3386d3a89ce0a6cfb52722f4c7e824fd261614b56eddbf31da7d1d4f96fcab32dc5e_640.jpg",
		"https://images.unsplash.com/photo-1542751371-adc38448a05e?w=800",
		"https://images.unsplash.com/photo-1511512578047-dfb367046420?w=800",
		"https://images.unsplash.com/photo-1598550476439-6847785fcea6?w=800",
	}
	data := []sp{
		{2, "video", vids[0].url, vids[0].thumb, "Diamond league gameplay - watch and learn", 5400, "2026-02-20T14:00:00Z"},
		{7, "video", vids[1].url, vids[1].thumb, "Frozen intime New combo showcase!", 8800, "2026-02-20T13:30:00Z"},
		{1, "video", vids[2].url, vids[2].thumb, "Finally broke my personal record", 1200, "2026-02-20T12:45:00Z"},
		{10, "image", imgs[0], "", "New setup reveal Ready to dominate!", 3200, "2026-02-20T11:15:00Z"},
		{4, "video", vids[3].url, vids[3].thumb, "Shadow techniques vol.3 - the comeback is real ", 4100, "2026-02-20T10:30:00Z"},
		{5, "video", vids[4].url, vids[4].thumb, "Speed run challenge accepted!", 2100, "2026-02-20T09:00:00Z"},
		{6, "video", vids[5].url, vids[5].thumb, "Storm surge combo into triple elimination", 3800, "2026-02-19T22:00:00Z"},
		{3, "image", imgs[1], "", "Just started competing - wish me luck! ", 800, "2026-02-19T20:30:00Z"},
		{9, "video", vids[6].url, vids[6].thumb, "Thunder strike compilation ", 2600, "2026-02-19T18:45:00Z"},
		{8, "video", vids[7].url, vids[7].thumb, "Late-night practice session ", 650, "2026-02-19T17:00:00Z"},
		{2, "video", vids[0].url, vids[0].thumb, "1v1 challenge against shadowstrike - who wins?", 9200, "2026-02-19T15:30:00Z"},
		{4, "image", imgs[2], "", "Platinum badge unlocked!", 5100, "2026-02-19T14:00:00Z"},
		{7, "video", vids[1].url, vids[1].thumb, "Tutorial: Advanced freeze frame technique", 4200, "2026-02-19T12:15:00Z"},
		{1, "video", vids[2].url, vids[2].thumb, "Gold league highlights - best plays this week ", 1800, "2026-02-19T10:30:00Z"},
		{10, "video", vids[3].url, vids[3].thumb, "AI - assisted training results are insane", 7500, "2026-02-19T08:00:00Z"},
		{6, "video", vids[4].url, vids[4].thumb, "Road to Platinum - day 45 of the grind ", 2400, "2026-02-18T23:00:00Z"},
		{5, "image", imgs[3], "", "New controller just dropped ", 1600, "2026-02-18T21:00:00Z"},
		{9, "video", vids[5].url, vids[5].thumb, "My best clutch moment yet - 1 HP survival!!", 4800, "2026-02-18T19:00:00Z"},
		{3, "video", vids[6].url, vids[6].thumb, "Learning from the pros Watch me improve!", 500, "2026-02-18T17:30:00Z"},
		{8, "video", vids[7].url, vids[7].thumb, "First win of the season ", 900, "2026-02-18T16:00:00Z"},
		{2, "video", vids[0].url, vids[0].thumb, "How I got to Diamond in 30 days - full guide", 12000, "2026-02-18T14:00:00Z"},
		{7, "image", imgs[4], "", "New character skin unlocked - thoughts?", 3100, "2026-02-18T12:00:00Z"},
		{4, "video", vids[1].url, vids[1].thumb, "Top 5 mistakes beginners make (avoid these!)", 5600, "2026-02-18T10:00:00Z"},
		{10, "video", vids[2].url, vids[2].thumb, "Challenge accepted: 24-hour win streak attempt ", 8900, "2026-02-18T08:00:00Z"},
		{6, "video", vids[3].url, vids[3].thumb, "Storm vs Blaze - epic rivalry match recap ", 3600, "2026-02-17T22:00:00Z"},
		{1, "image", imgs[0], "", "My journey from Bronze to Gold in one month", 2200, "2026-02-17T18:00:00Z"},
		{5, "video", vids[4].url, vids[4].thumb, "Speed kills - fastest elimination compilation", 2000, "2026-02-17T15:00:00Z"},
		{9, "video", vids[5].url, vids[5].thumb, "Team challenge highlights with frostbyte!", 1700, "2026-02-17T12:00:00Z"},
	}
	for _, p := range data {
		t := freshTimestamp(p.createdAt)
		db.Exec(
			`INSERT INTO posts (author_id, type, content_url, thumbnail_url, caption, views, created_at)
			VALUES ($1,$2,$3,$4,$5,$6,$7)`,
			p.authorID, p.postType, p.contentURL, p.thumbnailURL, p.caption, p.views, t,
		)
	}
}

func seedComments() {
	type sc struct {
		postID, authorID int
		text, createdAt  string
	}
	data := []sc{
		{1, 1, "Insane gameplay! Teach me your ways ", "2026-02-20T14:30:00Z"},
		{1, 4, "Diamond players are built different", "2026-02-20T14:45:00Z"},
		{1, 7, "GG, let's run duos sometime", "2026-02-20T15:00:00Z"},
		{2, 2, "That combo is nutty", "2026-02-20T13:45:00Z"},
		{2, 9, "How do you do the freeze cancel?", "2026-02-20T14:00:00Z"},
		{3, 2, "Nice one! Keep grinding ", "2026-02-20T13:00:00Z"},
		{3, 6, "Gold league represent!", "2026-02-20T13:15:00Z"},
		{4, 7, "Clean setup! What monitor is that?", "2026-02-20T11:45:00Z"},
		{5, 10, "Shadow gameplay is always entertaining", "2026-02-20T11:00:00Z"},
		{8, 1, "Welcome to the arena! Good luck", "2026-02-19T21:00:00Z"},
		{8, 5, "We all started somewhere, you got this!", "2026-02-19T21:30:00Z"},
		{11, 4, "Rematch? I want that W back ", "2026-02-19T16:00:00Z"},
		{11, 1, "This match was legendary!", "2026-02-19T16:30:00Z"},
		{12, 2, "Welcome to Platinum! See you in Diamond soon", "2026-02-19T14:30:00Z"},
		{14, 3, "Your highlights are so motivating!", "2026-02-19T11:00:00Z"},
		{21, 8, "This guide helped me so much, thanks!", "2026-02-18T15:00:00Z"},
		{26, 3, "Inspiring journey! I'm working on the same goal", "2026-02-17T19:00:00Z"},
	}
	for _, c := range data {
		t := freshTimestamp(c.createdAt)
		db.Exec(
			`INSERT INTO comments (post_id, author_id, text, created_at) VALUES ($1,$2,$3,$4)`,
			c.postID, c.authorID, c.text, t,
		)
	}
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

	var id int
	var createdAt time.Time
	category := payload.Category
	if category == "" {
		category = inferCategory(payload.Subject, payload.Prefix, "")
	}
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
	emotionJSON, _ := json.Marshal(payload.EmotionTags)
	if len(payload.EmotionTags) == 0 {
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

	err = db.QueryRow(
		`INSERT INTO challenges (creator_id, video_url, video_variants, thumbnail_url, prefix, subject, visibility, category, emotion_tags, energy_level)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) RETURNING id, created_at`,
		creatorID, payload.VideoURL, variantsJSON, payload.ThumbnailURL, payload.Prefix, payload.Subject, payload.Visibility,
		category, emotionJSON, energyLevel,
	).Scan(&id, &createdAt)
	if err != nil {
		return Challenge{}, err
	}

	// If friends visibility with specific users, insert visibility rows.
	if payload.Visibility == "friends" && len(payload.VisibleTo) > 0 {
		for _, uidStr := range payload.VisibleTo {
			uid, _ := strconv.Atoi(uidStr)
			if uid > 0 {
				db.Exec(`INSERT INTO challenge_visible_to (challenge_id, user_id) VALUES ($1,$2) ON CONFLICT DO NOTHING`, id, uid)
			}
		}
	}

	// Fetch the creator info.
	creator, _ := GetUserByID(payload.CreatorID)

	return Challenge{
		ID:              strconv.Itoa(id),
		CreatorID:       payload.CreatorID,
		CreatorUsername: creator.Username,
		CreatorLeague:   creator.League,
		VideoURL:        payload.VideoURL,
		ThumbnailURL:    payload.ThumbnailURL,
		Prefix:          payload.Prefix,
		Subject:         payload.Subject,
		Visibility:      payload.Visibility,
		Status:          "open",
		Category:        category,
		EmotionTags:     payload.EmotionTags,
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
	COALESCE(c.energy_level, 'medium') AS energy_level
FROM challenges c
JOIN users u ON c.creator_id = u.id
LEFT JOIN (SELECT challenge_id, COUNT(*) AS cnt FROM challenge_likes GROUP BY challenge_id) lc ON lc.challenge_id = c.id
LEFT JOIN (SELECT challenge_id, COUNT(*) AS cnt FROM challenge_responses GROUP BY challenge_id) rc ON rc.challenge_id = c.id`

// queryChallenges executes a challenge query and returns Challenge structs.
func queryChallenges(query string, args ...interface{}) []Challenge {
	rows, err := db.Query(query, args...)
	if err != nil {
		log.Printf("queryChallenges error: %v", err)
		return nil
	}
	defer rows.Close()

	var result []Challenge
	for rows.Next() {
		var id, creatorID, views, likes, respCount int
		var username, league, videoURL, thumbURL, prefix, subject, visibility, status string
		var categoryStr, energyStr string
		var emotionJSON []byte
		var createdAt time.Time

		if rows.Scan(&id, &creatorID, &username, &league,
			&videoURL, &thumbURL,
			&prefix, &subject, &visibility, &status, &views,
			&likes, &respCount, &createdAt,
			&categoryStr, &emotionJSON, &energyStr) == nil {

			var emotions []string
			json.Unmarshal(emotionJSON, &emotions)

			result = append(result, Challenge{
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
func GetArenaChallenges() []Challenge {
	return queryChallenges(challengeBaseQuery + `
	  WHERE c.visibility = 'arena' 
	  	AND (c.status IN ('active','completed') OR (c.status = 'open' AND c.created_at > NOW() - INTERVAL '24 hours'))
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
	  AND (c.status IN ('active','completed') OR (c.status = 'open' AND c.created_at > NOW() - INTERVAL '24 hours'))
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
func DeleteChallengeByID(idStr string) error {
	id, err := strconv.Atoi(idStr)
	if err != nil {
		return fmt.Errorf("invalid challenge id %q: %w", idStr, err)
	}
	res, err := db.Exec(`DELETE FROM challenges WHERE id = $1`, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("no challenge with id %d", id)
	}
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

	// Compute relevance once at upload time so the feed engine can use it
	// for ranking without recomputing per-request.
	challenge, _ := GetChallengeByID(payload.ChallengeID)
	relevance := computeRelevanceScore(challenge, payload.Caption)

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
	err := db.QueryRow(
		`INSERT INTO challenge_responses
			(challenge_id, responder_id, video_url, video_variants, thumbnail_url, duration_ms, caption, relevance_score)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8) RETURNING id, created_at`,
		cid, rid, payload.VideoURL, variantsJSON, payload.ThumbnailURL,
		payload.DurationMs, payload.Caption, relevance,
	).Scan(&id, &createdAt)
	if err != nil {
		return ChallengeResponse{}, err
	}

	// Update challenge status to "active".
	db.Exec(`UPDATE challenges SET status = 'active' WHERE id = $1 AND status = 'open'`, cid)

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
	db.QueryRow(`SELECT EXISTS(SELECT 1 FROM challenge_likes WHERE challenge_id=$1 AND user_id=$2)`, cid, uid).Scan(&exists)

	if exists {
		db.Exec(`DELETE FROM challenge_likes WHERE challenge_id=$1 AND user_id=$2`, cid, uid)
	} else {
		db.Exec(`INSERT INTO challenge_likes (challenge_id, user_id) VALUES ($1,$2) ON CONFLICT DO NOTHING`, cid, uid)
	}

	var count int
	db.QueryRow(`SELECT COUNT(*) FROM challenge_likes WHERE challenge_id=$1`, cid).Scan(&count)
	return !exists, count
}

// IncrementChallengeViews bumps the view count for a challenge.
func IncrementChallengeViews(challengeID string) {
	cid, err := strconv.Atoi(challengeID)
	if err == nil {
		db.Exec(`UPDATE challenges SET views = views + 1 WHERE id = $1`, cid)
	}
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
// reels without ever moving the number on screen. Capped at one bump per
// (user, challenge) day at the SQL level via a simple existence check
// against watch_events to keep the count from inflating from rewatches.
func RecordWatchEvent(payload WatchEventPayload) error {
	uid, err := strconv.Atoi(payload.UserID)
	if err != nil {
		return fmt.Errorf("invalid user ID")
	}
	cid, err := strconv.Atoi(payload.ContentID)
	if err != nil {
		return fmt.Errorf("invalid content ID")
	}

	// Check whether this user has already counted toward this challenge's
	// view tally today. We look at watch_events directly instead of a
	// dedicated table because the existing index on (user_id, content_id)
	// makes this a sub-millisecond probe and avoids a schema migration.
	shouldBumpViews := false
	if payload.ContentType == "challenge" {
		var prior int
		_ = db.QueryRow(`
			SELECT COUNT(*) FROM watch_events
			WHERE user_id = $1 AND content_id = $2 AND content_type = 'challenge'
			AND created_at > NOW() - INTERVAL '24 hours'`,
			uid, cid).Scan(&prior)
		shouldBumpViews = prior == 0
	}

	_, err = db.Exec(
		`INSERT INTO watch_events (user_id, content_id, content_type, watch_time, completed)
		 VALUES ($1,$2,$3,$4,$5)`,
		uid, cid, payload.ContentType, payload.WatchTime, payload.Completed,
	)
	if err != nil {
		return err
	}

	if shouldBumpViews {
		// Best effort — a failure here shouldn't fail the analytics insert
		// above, which is the source of truth for the recommender.
		go IncrementChallengeViews(payload.ContentID)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Challenge Votes CRUD
// ---------------------------------------------------------------------------

// CastVote records a user's vote on a challenge response. One vote per user per challenge.
func CastVote(payload ChallengeVotePayload) (bool, error) {
	cid, err := strconv.Atoi(payload.ChallengeID)
	if err != nil {
		return false, fmt.Errorf("invalid challenge ID")
	}
	rid, err := strconv.Atoi(payload.ResponseID)
	if err != nil {
		return false, fmt.Errorf("invalid response ID")
	}
	vid, err := strconv.Atoi(payload.VoterID)
	if err != nil {
		return false, fmt.Errorf("invalid voter ID")
	}

	// Upsert: if user already voted, update their vote
	_, err = db.Exec(
		`INSERT INTO challenge_votes (challenge_id, response_id, voter_id)
		 VALUES ($1,$2,$3)
		 ON CONFLICT (challenge_id, voter_id)
		 DO UPDATE SET response_id = $2`,
		cid, rid, vid,
	)
	if err != nil {
		return false, err
	}
	return true, nil
}

// GetVoteSummary returns vote counts per response for a challenge.
func GetVoteSummary(challengeID string) []VoteSummary {
	cid, err := strconv.Atoi(challengeID)
	if err != nil {
		return nil
	}

	rows, err := db.Query(
		`SELECT cv.response_id, u.username, COUNT(*) AS votes
		 FROM challenge_votes cv
		 JOIN challenge_responses cr ON cv.response_id = cr.id
		 JOIN users u ON cr.responder_id = u.id
		 WHERE cv.challenge_id = $1
		 GROUP BY cv.response_id, u.username
		 ORDER BY votes DESC`, cid,
	)
	if err != nil {
		return nil
	}
	defer rows.Close()

	var result []VoteSummary
	for rows.Next() {
		var respID, votes int
		var username string
		if rows.Scan(&respID, &username, &votes) == nil {
			result = append(result, VoteSummary{
				ResponseID: strconv.Itoa(respID),
				Username:   username,
				Votes:      votes,
			})
		}
	}
	return result
}

// ---------------------------------------------------------------------------
// Reports CRUD
// ---------------------------------------------------------------------------

// CreateReport inserts a new content/user report.
func CreateReport(payload ReportPayload) (Report, error) {
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
		`INSERT INTO reports (reporter_id, target_id, target_type, reason, description)
		 VALUES ($1,$2,$3,$4,$5) RETURNING id, created_at`,
		reporterID, targetID, payload.TargetType, payload.Reason, payload.Description,
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

// --------------------------------------------------------------------------------
// Seed challenges (sample data)
// --------------------------------------------------------------------------------

func seedChallenges() {
	type sc struct {
		creatorID          int
		videoURL, thumbURL string
		prefix, subject    string
		visibility, status string
		views              int
		createdAt          string
	}

	// Public sample video URLs (all verified accessible)
	v1 := "https://cdn.pixabay.com/video/2026/01/28/331030_medium.mp4"
	v2 := "https://cdn.pixabay.com/video/2021/06/06/76681-559745365_medium.mp4"
	v3 := "https://cdn.pixabay.com/video/2026/02/17/335040_medium.mp4"
	v4 := "https://cdn.pixabay.com/video/2026/02/10/333819_medium.mp4"
	v5 := "https://cdn.pixabay.com/video/2026/01/05/326081_medium.mp4"
	v6 := "https://flutter.github.io/assets-for-api-docs/assets/videos/butterfly.mp4"
	v7 := "https://flutter.github.io/assets-for-api-docs/assets/videos/bee.mp4"
	v8 := "https://media.w3.org/2010/05/sintel/trailer.mp4"
	v9 := "https://media.w3.org/2010/05/bunny/trailer.mp4"
	v10 := "https://media.w3.org/2010/05/bunny/movie.mp4"
	v11 := "https://cdn.plyr.io/static/demo/View_From_A_Blue_Moon_Trailer-720p.mp4"
	v12 := "https://test-videos.co.uk/vids/bigbuckbunny/mp4/h264/720/Big_Buck_Bunny_720_10s_1MB.mp4"
	v13 := "https://test-videos.co.uk/vids/jellyfish/mp4/h264/720/Jellyfish_720_10s_1MB.mp4"
	v14 := "https://test-videos.co.uk/vids/sintel/mp4/h264/720/Sintel_720_10s_1MB.mp4"
	v15 := "https://www.w3schools.com/html/mov_bbb.mp4"

	data := []sc{
		// === SHORTS (open, no one accepted — single video) ===
		// -- player1 (id 1) --
		{1, v3, "", "Who can beat", "This Record", "arena", "open", 950, "2026-04-02T06:00:00Z"},
		{1, v10, "", "Who is faster at", "Speed Run", "arena", "open", 1200, "2026-04-02T07:00:00Z"},
		{1, v7, "", "Who has the best", "Morning Routine", "arena", "open", 2400, "2026-04-02T08:00:00Z"},

		// -- player2 (id 2) --
		{2, v1, "", "Who is better at", "Dance Moves", "arena", "open", 3200, "2026-04-02T09:00:00Z"},
		{2, v12, "", "Who can do a better", "Freestyle Rap", "arena", "open", 2800, "2026-04-02T10:00:00Z"},
		{2, v9, "", "Who can pull off", "This Look", "arena", "open", 1700, "2026-04-02T11:00:00Z"},

		// -- player3 (id 3) --
		{3, v6, "", "Who has the best", "Cooking Skills", "arena", "open", 4200, "2026-04-02T12:00:00Z"},
		{3, v11, "", "Who plays better", "Guitar Solo", "arena", "open", 1300, "2026-04-02T13:00:00Z"},

		// -- shadowstrike (id 4) --
		{4, v11, "", "Which is the best", "Music Cover", "arena", "open", 5600, "2026-04-02T06:30:00Z"},
		{4, v4, "", "Who has better", "Reflexes", "arena", "open", 3900, "2026-04-02T07:30:00Z"},
		{4, v15, "", "Who can land", "This Combo", "arena", "open", 2100, "2026-04-02T08:30:00Z"},

		// -- blazerunner (id 5) --
		{5, v5, "", "Who can do better", "Trick Shot", "arena", "open", 1500, "2026-04-02T09:30:00Z"},
		{5, v8, "", "Who is funnier", "Stand Up Bit", "arena", "open", 4100, "2026-04-02T10:30:00Z"},
		{5, v14, "", "Who runs faster", "Sprint Challenge", "arena", "open", 2600, "2026-04-02T11:30:00Z"},

		// -- stormchaser (id 6) --
		{6, v9, "", "Who has better", "Fashion Style", "arena", "open", 3400, "2026-04-02T12:30:00Z"},
		{6, v2, "", "Who has the cooler", "Room Tour", "arena", "open", 1800, "2026-04-02T13:30:00Z"},

		// -- frostbyte (id 7) --
		{7, v2, "", "Which is the best", "Gaming Setup", "arena", "open", 1800, "2026-04-02T06:15:00Z"},
		{7, v13, "", "Who has the better", "Sports Move", "arena", "open", 1900, "2026-04-02T07:15:00Z"},
		{7, v6, "", "Who cooks better", "Ramen Bowl", "arena", "open", 3500, "2026-04-02T08:15:00Z"},

		// -- nightowl (id 8) --
		{8, v7, "", "Which is more", "Creative Art", "arena", "open", 890, "2026-04-02T09:15:00Z"},
		{8, v12, "", "Who draws better", "Anime Character", "arena", "open", 1100, "2026-04-02T10:15:00Z"},
		{8, v3, "", "Who has better", "Night Photography", "arena", "open", 750, "2026-04-02T11:15:00Z"},

		// -- thunderbolt (id 9) --
		{9, v8, "", "Who can nail", "This Comedy Bit", "arena", "open", 6700, "2026-04-02T12:15:00Z"},
		{9, v14, "", "Who throws better", "Javelin Throw", "arena", "open", 2200, "2026-04-02T13:15:00Z"},
		{9, v1, "", "Who has the better", "Dance Routine", "arena", "open", 3800, "2026-04-02T06:45:00Z"},

		// -- cyberking (id 10) --
		{10, v4, "", "Who is the real", "Champion", "arena", "open", 2100, "2026-04-02T07:45:00Z"},
		{10, v10, "", "Who builds faster", "PC Build Race", "arena", "open", 5200, "2026-04-02T08:45:00Z"},
		{10, v15, "", "Who codes faster", "Bug Fix Race", "arena", "open", 4400, "2026-04-02T09:45:00Z"},

		// === BATTLES (accepted — both challenger and opponent have videos) ===
		// IDs 29-35: original 7 arena battles. Added 10 more (IDs 38-47) below
		// the friends block so the 70/30 battle/short candidate split has
		// enough inventory to actually surface battles in the For You feed.
		{4, v14, "", "Who has better", "Strategy", "arena", "active", 4500, "2026-03-27T16:00:00Z"},      // ID 29
		{6, v15, "", "Who dances better", "Salsa", "arena", "active", 7200, "2026-03-28T10:00:00Z"},      // ID 30
		{10, v1, "", "Who sings better", "Pop Song", "arena", "active", 3100, "2026-03-26T19:00:00Z"},    // ID 31
		{1, v5, "", "Who flips better", "Pancake Flip", "arena", "active", 2900, "2026-03-30T08:00:00Z"}, // ID 32
		{9, v11, "", "Who plays better", "Drum Solo", "arena", "active", 5100, "2026-03-31T14:00:00Z"},   // ID 33
		{5, v3, "", "Who skates better", "Kickflip", "arena", "active", 6300, "2026-03-29T17:00:00Z"},    // ID 34
		{7, v9, "", "Who styles better", "Outfit Check", "arena", "active", 4700, "2026-03-30T19:00:00Z"},// ID 35

		// === FRIENDS-ONLY (some open, some battles) ===
		{2, v5, "", "Who is better", "Sniper", "friends", "open", 400, "2026-03-27T18:00:00Z"},           // ID 36
		{8, v8, "", "Who cooks better", "Pasta", "friends", "active", 560, "2026-03-28T07:00:00Z"},       // ID 37 — battle

		// === MORE ARENA BATTLES — added to give the 70/30 battle/short
		// candidate-pool split enough inventory to actually surface battles
		// in the For You feed. IDs 38-47.
		{2, v3, "", "Who's faster at", "Skateboarding", "arena", "active", 5400, "2026-04-01T08:00:00Z"},      // ID 38
		{4, v6, "", "Who has the better", "Comedy Skit", "arena", "active", 8100, "2026-04-01T10:00:00Z"},     // ID 39
		{7, v8, "", "Who plays better", "Piano Solo", "arena", "active", 4900, "2026-04-01T12:00:00Z"},        // ID 40
		{1, v12, "", "Who can do a better", "Magic Trick", "arena", "active", 6700, "2026-04-01T14:00:00Z"},   // ID 41
		{6, v9, "", "Who has the cleanest", "Free Throw", "arena", "active", 3300, "2026-04-01T16:00:00Z"},    // ID 42
		{10, v14, "", "Who reviews better", "New Phone", "arena", "active", 7800, "2026-04-01T18:00:00Z"},     // ID 43
		{3, v5, "", "Who paints better", "Sunset Scene", "arena", "active", 2400, "2026-04-01T20:00:00Z"},     // ID 44
		{8, v7, "", "Who has the spookier", "Halloween Costume", "arena", "active", 5500, "2026-03-31T22:00:00Z"}, // ID 45
		{9, v15, "", "Who has the better", "Workout Routine", "arena", "active", 4200, "2026-03-31T20:00:00Z"},    // ID 46
		{5, v1, "", "Who tells better", "Bedtime Story", "arena", "active", 3700, "2026-03-31T18:00:00Z"},         // ID 47
	}

	for _, c := range data {
		t := freshTimestamp(c.createdAt)
		db.Exec(
			`INSERT INTO challenges (creator_id, video_url, thumbnail_url, prefix, subject, visibility, status, views, created_at)
			 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
			c.creatorID, c.videoURL, c.thumbURL, c.prefix, c.subject, c.visibility, c.status, c.views, t,
		)
	}

	// Seed challenge likes — spread across all challenges.
	// {challengeID, userID}
	challengeLikes := [][2]int{
		// shorts likes
		{1, 2}, {1, 4}, {1, 6}, {1, 8}, {1, 10},
		{2, 3}, {2, 5}, {2, 7}, {2, 9},
		{3, 1}, {3, 4}, {3, 6},
		{4, 1}, {4, 3}, {4, 5}, {4, 7}, {4, 9}, {4, 10},
		{5, 2}, {5, 6}, {5, 8},
		{6, 1}, {6, 4}, {6, 7}, {6, 9},
		{7, 2}, {7, 5}, {7, 8}, {7, 10},
		{8, 1}, {8, 3}, {8, 6},
		{9, 2}, {9, 4}, {9, 7}, {9, 8}, {9, 10},
		{10, 1}, {10, 3}, {10, 5}, {10, 9},
		{11, 2}, {11, 6}, {11, 7},
		{12, 1}, {12, 3}, {12, 8}, {12, 10},
		{13, 4}, {13, 5}, {13, 9},
		{14, 1}, {14, 2}, {14, 7},
		{15, 3}, {15, 6}, {15, 10},
		{16, 1}, {16, 4}, {16, 8},
		{17, 2}, {17, 5}, {17, 9}, {17, 10},
		{18, 1}, {18, 3}, {18, 6}, {18, 7},
		{19, 2}, {19, 4}, {19, 8}, {19, 9}, {19, 10},
		{20, 1}, {20, 5}, {20, 7},
		{21, 3}, {21, 6}, {21, 9},
		{22, 2}, {22, 4}, {22, 10},
		{23, 1}, {23, 5}, {23, 8},
		{24, 3}, {24, 7}, {24, 9}, {24, 10},
		{25, 1}, {25, 2}, {25, 4}, {25, 6},
		{26, 3}, {26, 5}, {26, 8},
		{27, 1}, {27, 7}, {27, 9},
		{28, 2}, {28, 4}, {28, 6}, {28, 10},
		// battle likes (battles are IDs 29-35, friends 36-37)
		{29, 1}, {29, 3}, {29, 5}, {29, 7}, {29, 9}, {29, 10},
		{30, 2}, {30, 4}, {30, 6}, {30, 8}, {30, 1}, {30, 3}, {30, 9},
		{31, 1}, {31, 5}, {31, 7},
		{32, 2}, {32, 4}, {32, 6}, {32, 8}, {32, 10},
		{33, 1}, {33, 3}, {33, 5}, {33, 7},
		{34, 2}, {34, 4}, {34, 6}, {34, 8}, {34, 9}, {34, 10},
		{35, 1}, {35, 3}, {35, 5}, {35, 8},
		// new battle likes (IDs 38-47)
		{38, 1}, {38, 3}, {38, 7}, {38, 9}, {38, 10},
		{39, 2}, {39, 4}, {39, 6}, {39, 8}, {39, 10}, {39, 1}, {39, 5},
		{40, 1}, {40, 4}, {40, 6}, {40, 8},
		{41, 2}, {41, 3}, {41, 5}, {41, 7}, {41, 9},
		{42, 1}, {42, 4}, {42, 7}, {42, 10},
		{43, 2}, {43, 5}, {43, 8}, {43, 10}, {43, 1}, {43, 3},
		{44, 1}, {44, 6}, {44, 9},
		{45, 2}, {45, 4}, {45, 7}, {45, 8}, {45, 10},
		{46, 1}, {46, 3}, {46, 6}, {46, 9},
		{47, 2}, {47, 5}, {47, 8}, {47, 10},
	}
	for _, cl := range challengeLikes {
		db.Exec(`INSERT INTO challenge_likes (challenge_id, user_id) VALUES ($1,$2) ON CONFLICT DO NOTHING`, cl[0], cl[1])
	}

	// Seed responses for battle challenges.
	// 28 shorts (IDs 1-28), 7 battles (IDs 29-35), 2 friends (IDs 36-37).
	type sr struct {
		challengeID, responderID int
		videoURL, thumbURL       string
		views                    int
		createdAt                string
	}
	responses := []sr{
		// Original 8 responses → response IDs 1-8 (in this insertion order).
		{29, 2, v2, "", 3100, "2026-03-27T17:30:00Z"},    // resp 1 — player2 responds to shadowstrike
		{30, 9, v7, "", 5800, "2026-03-28T11:30:00Z"},    // resp 2 — thunderbolt responds to stormchaser
		{31, 3, v10, "", 2200, "2026-03-26T21:00:00Z"},   // resp 3 — player3 responds to cyberking
		{32, 7, v12, "", 2700, "2026-03-30T10:00:00Z"},   // resp 4 — frostbyte responds to player1
		{33, 4, v6, "", 4200, "2026-03-31T16:00:00Z"},    // resp 5 — shadowstrike responds to thunderbolt
		{34, 10, v15, "", 5100, "2026-03-29T19:00:00Z"},  // resp 6 — cyberking responds to blazerunner
		{35, 2, v4, "", 3900, "2026-03-30T21:00:00Z"},    // resp 7 — player2 responds to frostbyte
		{37, 1, v3, "", 340, "2026-03-28T08:00:00Z"},     // resp 8 — player1 responds to nightowl (friends battle)
		// New responses for the 10 added battles → response IDs 9-18.
		{38, 5, v4, "", 4200, "2026-04-01T09:00:00Z"},    // resp 9  — blazerunner responds to player2
		{39, 9, v11, "", 6300, "2026-04-01T11:00:00Z"},   // resp 10 — thunderbolt responds to shadowstrike
		{40, 3, v2, "", 3800, "2026-04-01T13:00:00Z"},    // resp 11 — player3 responds to frostbyte
		{41, 8, v10, "", 5100, "2026-04-01T15:00:00Z"},   // resp 12 — nightowl responds to player1
		{42, 4, v15, "", 2900, "2026-04-01T17:00:00Z"},   // resp 13 — shadowstrike responds to stormchaser
		{43, 6, v13, "", 6200, "2026-04-01T19:00:00Z"},   // resp 14 — stormchaser responds to cyberking
		{44, 7, v6, "", 1900, "2026-04-01T21:00:00Z"},    // resp 15 — frostbyte responds to player3
		{45, 10, v3, "", 4600, "2026-03-31T23:00:00Z"},   // resp 16 — cyberking responds to nightowl
		{46, 1, v8, "", 3500, "2026-03-31T21:00:00Z"},    // resp 17 — player1 responds to thunderbolt
		{47, 2, v9, "", 3100, "2026-03-31T19:00:00Z"},    // resp 18 — player2 responds to blazerunner
	}
	for _, r := range responses {
		rt := freshTimestamp(r.createdAt)
		db.Exec(
			`INSERT INTO challenge_responses (challenge_id, responder_id, video_url, thumbnail_url, views, created_at)
			 VALUES ($1,$2,$3,$4,$5,$6)`,
			r.challengeID, r.responderID, r.videoURL, r.thumbURL, r.views, rt,
		)
	}

	// Seed votes on battle challenges.
	type sv struct {
		challengeID, responseID, voterID int
	}
	votes := []sv{
		{29, 1, 1}, {29, 1, 5}, {29, 1, 8}, {29, 1, 3}, {29, 1, 10},
		{30, 2, 1}, {30, 2, 4}, {30, 2, 7}, {30, 2, 10}, {30, 2, 2}, {30, 2, 5},
		{31, 3, 2}, {31, 3, 6},
		{32, 4, 3}, {32, 4, 5}, {32, 4, 9},
		{33, 5, 1}, {33, 5, 2}, {33, 5, 6}, {33, 5, 8}, {33, 5, 10},
		{34, 6, 1}, {34, 6, 3}, {34, 6, 7}, {34, 6, 9},
		{35, 7, 1}, {35, 7, 4}, {35, 7, 6}, {35, 7, 10},
		// votes on the 10 newly added battles (IDs 38-47, response IDs 9-18)
		{38, 9, 1}, {38, 9, 3}, {38, 9, 7}, {38, 9, 10},
		{39, 10, 1}, {39, 10, 2}, {39, 10, 5}, {39, 10, 8}, {39, 10, 10},
		{40, 11, 4}, {40, 11, 6}, {40, 11, 9},
		{41, 12, 2}, {41, 12, 5}, {41, 12, 7}, {41, 12, 9},
		{42, 13, 1}, {42, 13, 5}, {42, 13, 8}, {42, 13, 10},
		{43, 14, 1}, {43, 14, 3}, {43, 14, 7}, {43, 14, 9},
		{44, 15, 2}, {44, 15, 6}, {44, 15, 10},
		{45, 16, 1}, {45, 16, 3}, {45, 16, 5}, {45, 16, 7}, {45, 16, 9},
		{46, 17, 2}, {46, 17, 6}, {46, 17, 8},
		{47, 18, 3}, {47, 18, 6}, {47, 18, 9},
	}
	for _, v := range votes {
		db.Exec(
			`INSERT INTO challenge_votes (challenge_id, response_id, voter_id) VALUES ($1,$2,$3) ON CONFLICT DO NOTHING`,
			v.challengeID, v.responseID, v.voterID,
		)
	}
}

// --------------------------------------------------------------------------
// Chat
// --------------------------------------------------------------------------

// SendChatMessage inserts a message and returns its ID.
func SendChatMessage(senderID, receiverID int, message string, replyToID *int) (int, error) {
	var id int
	err := db.QueryRow(
		`INSERT INTO chat_messages (sender_id, receiver_id, message, reply_to_id)
		 VALUES ($1, $2, $3, $4) RETURNING id`,
		senderID, receiverID, message, replyToID,
	).Scan(&id)
	return id, err
}

// GetChatMessages returns messages between two users, newest first.
func GetChatMessages(userA, userB, limit, offset int) []ChatMessage {
	rows, err := db.Query(
		`SELECT m.id, m.sender_id, s.username, m.receiver_id, r.username,
				m.message, m.is_read,
				COALESCE(m.status, 'sent') AS status,
				COALESCE(m.is_edited, FALSE) AS is_edited,
				COALESCE(m.is_deleted, FALSE) AS is_deleted,
				m.reply_to_id,
				(SELECT m2.message FROM chat_messages m2 WHERE m2.id = m.reply_to_id) AS reply_to_text,
				m.created_at
		 FROM chat_messages m
		 JOIN users s ON m.sender_id = s.id
		 JOIN users r ON m.receiver_id = r.id
		 WHERE (m.sender_id = $1 AND m.receiver_id = $2)
		    OR (m.sender_id = $2 AND m.receiver_id = $1)
		 ORDER BY m.created_at DESC
		 LIMIT $3 OFFSET $4`,
		userA, userB, limit, offset,
	)
	if err != nil {
		return nil
	}
	defer rows.Close()

	var result []ChatMessage
	for rows.Next() {
		var id, sID, rID int
		var sName, rName, msg, status string
		var isRead, isEdited, isDeleted bool
		var replyToID *int
		var replyToText *string
		var createdAt time.Time
		if rows.Scan(&id, &sID, &sName, &rID, &rName, &msg, &isRead,
			&status, &isEdited, &isDeleted, &replyToID, &replyToText, &createdAt) == nil {
			cm := ChatMessage{
				ID:              strconv.Itoa(id),
				SenderID:        strconv.Itoa(sID),
				SenderUsername:   sName,
				ReceiverID:      strconv.Itoa(rID),
				ReceiverUsername: rName,
				Message:         msg,
				IsRead:          isRead,
				Status:          status,
				IsEdited:        isEdited,
				IsDeleted:       isDeleted,
				CreatedAt:       createdAt.UTC().Format(time.RFC3339),
			}
			if replyToID != nil {
				cm.ReplyToID = strconv.Itoa(*replyToID)
			}
			if replyToText != nil {
				cm.ReplyToText = *replyToText
			}
			result = append(result, cm)
		}
	}
	return result
}

// MarkMessagesRead marks all messages from sender to receiver as read.
func MarkMessagesRead(senderID, receiverID int) {
	db.Exec(
		`UPDATE chat_messages SET is_read = TRUE
		 WHERE sender_id = $1 AND receiver_id = $2 AND is_read = FALSE`,
		senderID, receiverID,
	)
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
	var result []Conversation
	for _, pid := range partnerIDs {
		var username, league string
		err := db.QueryRow(`SELECT username, league FROM users WHERE id=$1`, pid).Scan(&username, &league)
		if err != nil {
			continue
		}

		var lastMsg string
		var lastTime time.Time
		err = db.QueryRow(
			`SELECT message, created_at FROM chat_messages
			 WHERE (sender_id=$1 AND receiver_id=$2) OR (sender_id=$2 AND receiver_id=$1)
			 ORDER BY created_at DESC LIMIT 1`,
			userID, pid,
		).Scan(&lastMsg, &lastTime)
		if err != nil {
			continue
		}

		var unread int
		db.QueryRow(
			`SELECT COUNT(*) FROM chat_messages
			 WHERE sender_id=$1 AND receiver_id=$2 AND is_read=FALSE`,
			pid, userID,
		).Scan(&unread)

		result = append(result, Conversation{
			UserID:      strconv.Itoa(pid),
			Username:    username,
			League:      league,
			LastMessage:  lastMsg,
			LastTime:     lastTime.UTC().Format(time.RFC3339),
			UnreadCount: unread,
		})
	}

	// Sort by last_time descending
	for i := 0; i < len(result); i++ {
		for j := i + 1; j < len(result); j++ {
			if result[j].LastTime > result[i].LastTime {
				result[i], result[j] = result[j], result[i]
			}
		}
	}

	return result
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
	db.QueryRow(`SELECT EXISTS(SELECT 1 FROM saved_challenges WHERE user_id=$1 AND challenge_id=$2)`, uid, cid).Scan(&exists)

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
		        (SELECT COUNT(*) FROM challenge_responses WHERE challenge_id=c.id) AS response_count
		 FROM saved_challenges sc
		 JOIN challenges c ON sc.challenge_id = c.id
		 JOIN users u ON c.creator_id = u.id
		 WHERE sc.user_id = $1
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
		   AND created_at > NOW() - INTERVAL '15 minutes'`,
		newText, msgID, senderID,
	)
	if err != nil {
		return err
	}
	rows, _ := result.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("message not found, not yours, or edit window expired (15 min)")
	}
	return nil
}

// DeleteChatMessage soft-deletes a message (unsend for everyone).
func DeleteChatMessage(msgID int, senderID int) error {
	result, err := db.Exec(
		`UPDATE chat_messages SET is_deleted=TRUE, message='This message was deleted'
		 WHERE id=$1 AND sender_id=$2`,
		msgID, senderID,
	)
	if err != nil {
		return err
	}
	rows, _ := result.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("message not found or not yours")
	}
	return nil
}

// UpdateUserLastSeen updates the user's last_seen timestamp.
func UpdateUserLastSeen(username string) {
	db.Exec(`UPDATE users SET last_seen=NOW() WHERE username=$1`, username)
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
