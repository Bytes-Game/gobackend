-- Forgot password: a 6-digit code sent to the account's email
-- (password_reset.go). One live code per account; asking again replaces it.
--
--   code_hash  — bcrypt of the code. The code itself is never kept.
--   expires_at — 15 minutes after it was sent.
--   attempts   — wrong tries so far. After five, the code is spent.
--   sent_at    — when it was sent. Another is not sent within a minute.
--
-- Goes with the account: deleting the account deletes its code.
CREATE TABLE IF NOT EXISTS password_reset_codes (
    user_id    INT PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
    code_hash  TEXT NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    attempts   INT NOT NULL DEFAULT 0,
    sent_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
