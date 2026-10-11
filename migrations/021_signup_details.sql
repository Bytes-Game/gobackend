-- What a new account gives us, and when its password last changed.
--
-- Sign-up used to take a username and a password and nothing else. New
-- accounts now also give:
--
--   email              — one account per address (the index below), and a
--                        way to reach the person. Private: never sent to
--                        anybody else. Empty for accounts made before this.
--   birth_date         — nobody under 13 can join. Private as well.
--   terms_accepted_at  — when they agreed to the Terms and Privacy Policy.
--
-- And for everybody:
--
--   password_changed_at — sign-ins from before this moment stop working,
--                         so changing a password signs out every other
--                         phone (see token_cutoff.go).
--
-- All nullable with no default, so this adds columns without rewriting the
-- table. Email is compared in lower case so "Ana@x.com" and "ana@x.com" are
-- one address.
ALTER TABLE users ADD COLUMN IF NOT EXISTS email TEXT;
ALTER TABLE users ADD COLUMN IF NOT EXISTS birth_date DATE;
ALTER TABLE users ADD COLUMN IF NOT EXISTS terms_accepted_at TIMESTAMPTZ;
ALTER TABLE users ADD COLUMN IF NOT EXISTS password_changed_at TIMESTAMPTZ;

CREATE UNIQUE INDEX IF NOT EXISTS users_email_lower_key
    ON users (lower(email)) WHERE email IS NOT NULL;
