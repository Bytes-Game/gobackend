-- ════════════════════════════════════════════════════════════════════════════
-- BATTLES HAVE AN END, A WINNER, AND A RATING
-- ════════════════════════════════════════════════════════════════════════════
--
-- Until this, a battle went from 'open' to 'active' when somebody answered it
-- and then stayed active for ever. Nothing decided a winner, nothing wrote to
-- users.wins or users.losses, and every league on the platform was whatever
-- the column default said. See battles.go for the rules that now run.
--
-- Everything lives here only, not in runMigrations: the backfills below only
-- make sense once, against data that already exists, and numbered migrations
-- run on every database the server starts against, a fresh one included.

-- How long voting runs once a battle is accepted. Seven days at least; the
-- creator can choose longer, up to thirty.
ALTER TABLE challenges ADD COLUMN IF NOT EXISTS battle_days    INT NOT NULL DEFAULT 7;
ALTER TABLE challenges ADD COLUMN IF NOT EXISTS accepted_at    TIMESTAMPTZ;
ALTER TABLE challenges ADD COLUMN IF NOT EXISTS voting_ends_at TIMESTAMPTZ;
ALTER TABLE challenges ADD COLUMN IF NOT EXISTS resolved_at    TIMESTAMPTZ;

-- Battles already running. They were accepted when their first answer
-- arrived. Their clock starts NOW, not then: until this migration a vote for
-- the person who posted the challenge could not be recorded at all (see
-- below), so ending them on the old clock would hand every one of them to
-- whoever answered.
UPDATE challenges c
   SET accepted_at = (SELECT MIN(cr.created_at)
                        FROM challenge_responses cr
                       WHERE cr.challenge_id = c.id)
 WHERE c.accepted_at IS NULL
   AND EXISTS (SELECT 1 FROM challenge_responses cr WHERE cr.challenge_id = c.id);
UPDATE challenges
   SET voting_ends_at = NOW() + battle_days * INTERVAL '1 day'
 WHERE accepted_at IS NOT NULL
   AND voting_ends_at IS NULL;

-- ── Votes for the creator ───────────────────────────────────────────────────
--
-- A vote could only name a response. The app sent the CHALLENGE's id as the
-- "response" when somebody voted for the creator, so each such vote either
-- failed the foreign key, or — when a response happened to have that number —
-- was counted for somebody in a different battle entirely.
--
-- A vote for the creator is now a vote with no response.
ALTER TABLE challenge_votes ALTER COLUMN response_id DROP NOT NULL;

-- Put back the creator votes that landed on another battle's response.
UPDATE challenge_votes cv
   SET response_id = NULL
 WHERE cv.response_id = cv.challenge_id
   AND NOT EXISTS (SELECT 1 FROM challenge_responses cr
                    WHERE cr.id = cv.response_id
                      AND cr.challenge_id = cv.challenge_id);

-- Anything else pointing at a response from another battle is not a vote in
-- this one.
DELETE FROM challenge_votes cv
 WHERE cv.response_id IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM challenge_responses cr
                    WHERE cr.id = cv.response_id
                      AND cr.challenge_id = cv.challenge_id);

-- ── The competitor's record ─────────────────────────────────────────────────
ALTER TABLE users ADD COLUMN IF NOT EXISTS rating            INT NOT NULL DEFAULT 1000;
ALTER TABLE users ADD COLUMN IF NOT EXISTS draws             INT NOT NULL DEFAULT 0;
ALTER TABLE users ADD COLUMN IF NOT EXISTS last_battle_at    TIMESTAMPTZ;
ALTER TABLE users ADD COLUMN IF NOT EXISTS rating_decayed_at TIMESTAMPTZ;
ALTER TABLE users ADD COLUMN IF NOT EXISTS integrity_strikes INT NOT NULL DEFAULT 0;

-- Nobody has ever had a battle decided, so nobody has earned a league. The
-- column said 'Bronze' for everyone because that was its default.
ALTER TABLE users ALTER COLUMN league SET DEFAULT 'Unranked';
UPDATE users SET league = 'Unranked' WHERE wins = 0 AND losses = 0 AND draws = 0;

-- ── One row per person per decided battle ───────────────────────────────────
CREATE TABLE IF NOT EXISTS battle_results (
    challenge_id   INT NOT NULL REFERENCES challenges(id) ON DELETE CASCADE,
    user_id        INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    role           VARCHAR(10) NOT NULL,            -- 'creator' | 'responder'
    response_id    INT REFERENCES challenge_responses(id) ON DELETE SET NULL,
    outcome        VARCHAR(10) NOT NULL,            -- 'won' | 'lost' | 'draw'
    votes          REAL NOT NULL DEFAULT 0,         -- genuine, after weighting
    raw_votes      INT  NOT NULL DEFAULT 0,         -- as cast
    removed_votes  INT  NOT NULL DEFAULT 0,         -- judged not genuine
    likes          INT  NOT NULL DEFAULT 0,
    views          INT  NOT NULL DEFAULT 0,
    shares         INT  NOT NULL DEFAULT 0,
    flagged        BOOLEAN NOT NULL DEFAULT FALSE,  -- penalised for fake votes
    rating_before  INT NOT NULL,
    rating_after   INT NOT NULL,
    decided_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (challenge_id, user_id)
);
CREATE INDEX IF NOT EXISTS idx_battle_results_user
    ON battle_results (user_id, outcome, decided_at DESC);
CREATE INDEX IF NOT EXISTS idx_challenges_voting_ends
    ON challenges (voting_ends_at) WHERE resolved_at IS NULL;
