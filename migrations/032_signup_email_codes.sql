-- A new account's email is checked before the account is made: a 6-digit
-- code goes to the email, and the sign-up only goes through with it
-- (signup_email_code.go). One live code per email; asking again replaces it.
--
--   email      — in lower case, as the account will keep it.
--   code_hash  — bcrypt of the code. The code itself is never kept.
--   expires_at — 15 minutes after it was sent.
--   attempts   — tries so far. After five wrong ones, the code is spent.
--   sent_at    — when it was sent. Another is not sent within a minute.
--
-- A code is cleared when its account is made. One nobody used is deleted,
-- with the address it went to, by the hourly account job once its 15
-- minutes are up.
CREATE TABLE IF NOT EXISTS signup_email_codes (
    email      TEXT PRIMARY KEY,
    code_hash  TEXT NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    attempts   INT NOT NULL DEFAULT 0,
    sent_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
