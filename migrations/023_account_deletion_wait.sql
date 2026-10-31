-- Deleting an account now waits 30 days, the way TikTok does it.
--
--   deletion_requested_at — when the person asked for their account to be
--                           deleted. Set: the account is hidden everywhere,
--                           signed out everywhere, and deleted for good 30
--                           days after this (account_leaving.go). Signing in
--                           before then and choosing to keep it clears it.
--   signed_out_at         — every sign-in from before this moment stops
--                           working. Asking for deletion signs out every
--                           phone; keeping the account afterwards does not
--                           bring those sign-ins back (token_cutoff.go).
--
-- Both nullable with no default, so adding them does not rewrite the table.
ALTER TABLE users ADD COLUMN IF NOT EXISTS deletion_requested_at TIMESTAMPTZ;
ALTER TABLE users ADD COLUMN IF NOT EXISTS signed_out_at TIMESTAMPTZ;

-- The few accounts waiting to be deleted: read on almost every list (to
-- leave them out) and by the job that finishes the deletions.
CREATE INDEX IF NOT EXISTS idx_users_waiting_deletion
    ON users (deletion_requested_at) WHERE deletion_requested_at IS NOT NULL;

-- One name, one person, whatever the capital letters: "Karan" and "karan"
-- cannot be two accounts. Sign-up already lowers every new name, and the
-- check that a name is free (UserExists) now ignores case too; this makes
-- the database refuse the race the check cannot see.
--
-- Only made when no two existing names clash. If two do, the server must
-- still start: a failed migration stops it booting, and the app-level check
-- already stops any new clash. The notice names the problem in the boot log.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM users GROUP BY lower(username) HAVING COUNT(*) > 1) THEN
        RAISE NOTICE 'users_username_lower_key not created: two usernames differ only in capital letters';
    ELSE
        CREATE UNIQUE INDEX IF NOT EXISTS users_username_lower_key ON users (lower(username));
    END IF;
END $$;

-- What a leaving account did to other people's things — its likes, votes,
-- comments, follows, saves and shares — set aside for the 30 days. Taken out
-- of their tables, every count, list and score leaves them out without each
-- one having to remember to: a like it gave stops counting on that video,
-- its comments go from under the videos, it leaves follower lists. Keeping
-- the account puts them back (account_leaving.go); deleting it for good
-- throws them away. Each row as it was, so it goes back exactly.
CREATE TABLE IF NOT EXISTS leaving_set_aside (
    id         BIGSERIAL PRIMARY KEY,
    user_id    INT NOT NULL,
    table_name TEXT NOT NULL,
    row_data   JSONB NOT NULL,
    set_aside_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_leaving_set_aside_user ON leaving_set_aside (user_id, table_name);
