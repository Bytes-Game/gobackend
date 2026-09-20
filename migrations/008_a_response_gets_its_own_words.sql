-- Let the person answering a challenge say what their video is, too.
--
-- ════════════════════════════════════════════════════════════════════════════
-- WHY
-- ════════════════════════════════════════════════════════════════════════════
--
-- Every video in this app gets read, listened to and looked at by the worker,
-- and that includes battle answers: challenge_responses has had
-- video_analysis, auto_tags and content_topics since migrations 005 and 006.
--
-- What it has never had is the other half — the CREATOR's half. A challenge
-- carries a category the uploader picked and tags they typed. A response
-- carried neither, because the columns were never added to its table.
--
-- The cost of that is not cosmetic. categoryFromEvidence weighs three sources
-- against each other: what the creator said, what the model concluded, and a
-- keyword guess at the words. With no creator columns on this table, a
-- response could only ever be decided by the last two, and the admin view that
-- exists to answer "do the creator and the model agree" had nothing to compare
-- for half the catalogue. It reported every single response as undisputed,
-- which is not the same thing as agreeing.
--
-- ════════════════════════════════════════════════════════════════════════════
-- WHAT IS ADDED
-- ════════════════════════════════════════════════════════════════════════════
--
--   category        what this answer is about. Same eighteen words as a
--                   challenge, same 'other' default meaning "nobody said".
--   custom_tags     the responder's own words, cleaned the same way a
--                   creator's are, and kept apart from auto_tags for the same
--                   reason: a machine must never appear to put words in
--                   somebody's mouth.
--   emotion_tags    how it feels, for the mood matcher.
--   energy_level    how stimulating it is. Derived server-side when unset.
--   dismissed_tags  suggestions the responder turned down, so the same ones
--                   are not offered back every time they open their own video.
--
-- These are exactly the five columns challenges already has. Nothing new is
-- being invented here — this is the same shape, on the table that was missed.

ALTER TABLE challenge_responses
    ADD COLUMN IF NOT EXISTS category       VARCHAR(30) DEFAULT 'other',
    ADD COLUMN IF NOT EXISTS custom_tags    JSONB       DEFAULT '[]'::jsonb,
    ADD COLUMN IF NOT EXISTS emotion_tags   JSONB       DEFAULT '[]'::jsonb,
    ADD COLUMN IF NOT EXISTS energy_level   VARCHAR(10) DEFAULT 'medium',
    ADD COLUMN IF NOT EXISTS dismissed_tags JSONB       DEFAULT '[]'::jsonb;

-- ════════════════════════════════════════════════════════════════════════════
-- THE TWO INDEXES MIGRATION 005 GAVE ONLY TO CHALLENGES
-- ════════════════════════════════════════════════════════════════════════════
--
-- 005 added auto_tags to both tables and then indexed one of them. That was
-- fine while nothing asked a response what its tags were. It stops being fine
-- the moment the ranker folds a battle answer's tags into the battle, which is
-- what the Go change alongside this migration does — that question becomes a
-- sequential scan of every answer ever posted, on every scored battle.
--
-- Partial for the same reason as before: a row with no machine tags has
-- nothing to say and would only make the index bigger.

CREATE INDEX IF NOT EXISTS idx_responses_auto_tags
    ON challenge_responses USING GIN (auto_tags)
    WHERE auto_tags IS NOT NULL AND auto_tags <> '[]'::jsonb;

CREATE INDEX IF NOT EXISTS idx_responses_analyzed
    ON challenge_responses ((video_analysis IS NOT NULL))
    WHERE video_analysis IS NOT NULL;

-- ════════════════════════════════════════════════════════════════════════════
-- WHY THIS ONE DOES BACKFILL, WHEN 006 DELIBERATELY DID NOT
-- ════════════════════════════════════════════════════════════════════════════
--
-- 006 refused to backfill because it would have had to reproduce normalizeTag
-- in SQL, and a fold that is even slightly different produces values that look
-- right and match nothing.
--
-- There is no folding here. A response answers one specific challenge, and
-- that challenge's category is an already-folded value sitting one join away.
-- Copying it is a copy, not a transform, so the failure that ruled out a
-- backfill last time cannot happen.
--
-- It is also the right answer on the merits. An answer to "who is better at
-- dancing" is a dance video. Leaving every existing response at 'other' would
-- mean the feed knows less about half its catalogue than the database can
-- already tell it, for no reason but the order the columns were added in.
--
-- Only where the challenge actually made a claim. '', 'other' and 'general'
-- are what gets stored when nobody chose, and copying a non-answer across
-- would turn "nobody said" into "somebody said nothing", which usableCategory
-- cannot tell apart.

UPDATE challenge_responses r
   SET category = c.category
  FROM challenges c
 WHERE c.id = r.challenge_id
   AND COALESCE(c.category, '') NOT IN ('', 'other', 'general')
   AND COALESCE(r.category, '') IN ('', 'other');
