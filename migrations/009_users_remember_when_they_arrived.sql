-- Record when an account started.
--
-- ════════════════════════════════════════════════════════════════════════════
-- WHY
-- ════════════════════════════════════════════════════════════════════════════
--
-- engagement_quality.go works out how much to trust a viewer's signals, and
-- the first of its four components is how old the account is — a brand-new
-- account that completes twenty videos in a minute is not the same evidence
-- as a two-year-old one doing the same.
--
-- It read that from users.created_at. There is no such column and there never
-- has been. Postgres refuses a statement naming a column that does not exist,
-- so the whole query failed, and the function returns 1.0 on any error — a
-- flat, neutral trust score for every user on the platform, for as long as
-- this has existed. Nothing logged, because the error was discarded.
--
-- So: give users the column the code has always believed was there.
--
-- ════════════════════════════════════════════════════════════════════════════
-- WHY IT BACKFILLS, AND WHY NOT JUST NOW()
-- ════════════════════════════════════════════════════════════════════════════
--
-- ADD COLUMN with a default stamps every existing row with the moment the
-- migration ran. Every account on the platform would then look like it was
-- created today, and the age component would score all of them at zero — the
-- LEAST trustworthy possible reading, applied hardest to the people who have
-- been here longest. That is worse than the flat 1.0 it replaces.
--
-- So each row is lowered to the earliest trace of that account doing anything:
-- their first challenge, first post, first follow, first recorded event. An
-- account cannot have posted before it existed, so the earliest of those is a
-- safe lower bound on the truth, and it is never later than the stamp above.
--
-- This is a copy of timestamps that are already there, not a transform — the
-- reason migration 006 refused to backfill (reproducing a Go text fold in SQL)
-- does not apply to a MIN() over a column.
--
-- An account with no trace at all keeps today's stamp, which is the honest
-- answer: nothing here knows when it arrived.
--
-- No COALESCE around the sub-selects, deliberately. LEAST IGNORES NULLs — it
-- returns the smallest value that is not null, and null only when every
-- argument is null. So a MIN over a table this account has never touched
-- comes back null and drops out on its own, which is exactly the wanted
-- behaviour.
--
-- An earlier draft wrapped each one in COALESCE(..., u.created_at). It was
-- harmless and it was misleading: it implied a null could win and pull an
-- account back to the year zero, making it the most trusted on the platform.
-- It cannot. u.created_at stays in the list as the floor for an account with
-- no traces at all, and because it is never null, LEAST always has an answer.

ALTER TABLE users
    ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT NOW();

UPDATE users u
   SET created_at = LEAST(
        u.created_at,
        (SELECT MIN(created_at) FROM challenges          WHERE creator_id   = u.id),
        (SELECT MIN(created_at) FROM challenge_responses WHERE responder_id = u.id),
        (SELECT MIN(created_at) FROM posts               WHERE author_id    = u.id),
        (SELECT MIN(created_at) FROM follows             WHERE follower_id  = u.id),
        (SELECT MIN(created_at) FROM feed_events         WHERE user_id      = u.id::text)
   );

-- "Accounts created in the last N days" is a question the admin dashboard and
-- every growth number asks, and it is a range scan over this column.
CREATE INDEX IF NOT EXISTS idx_users_created_at ON users (created_at);
