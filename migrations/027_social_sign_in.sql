-- Signing in with Google or Apple (social_signin.go).
--
-- One row per account a person has connected: which provider, and the
-- provider's own id for that person ("subject"), which never changes even
-- if their email does. That id, not the email, is what finds the account
-- at the next sign-in.
--
--   email — what the provider said when the account was made, for support
--           only. The account's own email is users.email.
--
-- Goes with the account: deleting the account for good deletes the link.
-- While an account waits to be deleted the link stays, so the person can
-- sign in with Google or Apple to keep it.
CREATE TABLE IF NOT EXISTS social_logins (
    provider   TEXT NOT NULL,
    subject    TEXT NOT NULL,
    user_id    INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    email      TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (provider, subject)
);
CREATE INDEX IF NOT EXISTS social_logins_user_id ON social_logins (user_id);
