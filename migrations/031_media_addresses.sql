-- Every public address this server has handed files out at
-- (media_addresses.go).
--
-- A file's address is saved with its row when it is posted, and never
-- changes after. When the address in front of the bucket changes (from
-- R2's test address to media.flexete.com), files posted before still carry
-- the old one. They are still ours, and must still be cleared from storage
-- when their video, account or message is deleted.
--
-- The server writes the address it starts with here, at every start, and
-- reads the list back. One row per address, for good.
CREATE TABLE IF NOT EXISTS media_addresses (
    base       TEXT PRIMARY KEY,
    first_used TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
