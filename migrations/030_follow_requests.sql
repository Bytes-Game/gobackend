-- Asking to follow a private account (follow_requests.go).
--
-- A private account's videos are for its followers. Following used to
-- need nobody's say-so, so anybody could follow a private account and see
-- everything in it. Now following a private account is a request, and the
-- person asked says yes or no.
--
-- One row per person asking, per account asked. Accepted: the row becomes
-- a follow. Declined, or taken back: the row is deleted. Deleted with
-- either account.
CREATE TABLE IF NOT EXISTS follow_requests (
    requester_id INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    target_id    INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (requester_id, target_id)
);
CREATE INDEX IF NOT EXISTS follow_requests_target ON follow_requests (target_id, created_at DESC);
