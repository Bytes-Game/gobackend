-- Changing a username, the way TikTok allows it: once every 30 days.
--
--   username_changed_at — when the account last changed its username. The
--                         next change is allowed 30 days after it. NULL: it
--                         never has, and may change it now
--                         (username_change.go).
--
-- Nullable with no default, so adding it does not rewrite the table.
ALTER TABLE users ADD COLUMN IF NOT EXISTS username_changed_at TIMESTAMPTZ;
