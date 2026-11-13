-- Reports are read and acted on, and an account can be suspended
-- (moderation.go).
--
-- reports:
--   owner_id    — the account behind what was reported: the video's or the
--                 answer's creator, the comment's author, or the account
--                 itself. Read when the report is made, so the review list
--                 can say whose it is, and "suspend" knows whom.
--   resolved_at — when the admin decided.
--   resolution  — what they decided: dismissed, removed, suspended, or
--                 removed_suspended.
--
-- users:
--   suspended_at     — set while the account is suspended: it cannot sign
--                      in, and it is hidden everywhere, the way an account
--                      waiting to be deleted is. Cleared to undo.
--   suspended_reason — what the admin wrote, shown to nobody but the admin.
ALTER TABLE reports ADD COLUMN IF NOT EXISTS owner_id INT;
ALTER TABLE reports ADD COLUMN IF NOT EXISTS resolved_at TIMESTAMPTZ;
ALTER TABLE reports ADD COLUMN IF NOT EXISTS resolution TEXT NOT NULL DEFAULT '';
CREATE INDEX IF NOT EXISTS idx_reports_open ON reports (target_type, target_id) WHERE status = 'pending';

ALTER TABLE users ADD COLUMN IF NOT EXISTS suspended_at TIMESTAMPTZ;
ALTER TABLE users ADD COLUMN IF NOT EXISTS suspended_reason TEXT NOT NULL DEFAULT '';
CREATE INDEX IF NOT EXISTS idx_users_suspended ON users (id) WHERE suspended_at IS NOT NULL;
