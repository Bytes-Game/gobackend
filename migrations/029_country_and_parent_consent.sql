-- Countries, and a parent's consent for young people
-- (country.go, age_rules.go, parent_consent.go).
--
-- users.country       — the two-letter country the account was last seen
--                       in ("IN"), '' while unknown. Read from the network
--                       the request came through, or failing that from the
--                       phone's own region setting.
-- challenges.country  — where its creator was when it was posted, so the
--                       feed can prefer videos from the viewer's country.
--
-- users.parent_email, users.parent_consent_at
--                     — for an account that needed a parent's consent: who
--                       gave it, and when.
ALTER TABLE users ADD COLUMN IF NOT EXISTS country TEXT NOT NULL DEFAULT '';
ALTER TABLE users ADD COLUMN IF NOT EXISTS parent_email TEXT NOT NULL DEFAULT '';
ALTER TABLE users ADD COLUMN IF NOT EXISTS parent_consent_at TIMESTAMPTZ;
ALTER TABLE challenges ADD COLUMN IF NOT EXISTS country TEXT NOT NULL DEFAULT '';

-- A sign-up that is waiting for a parent to say yes. It is NOT an account:
-- nothing here can sign in, be seen, or be searched for. It holds only what
-- is needed to ask the parent, and to make the account if they agree.
--
--   provider, subject — set when the sign-up came through Google or Apple
--                       (then password_hash is ''), so that sign-in finds
--                       it again.
--   token_hash        — the SHA-256 of the secret in the parent's link. The
--                       secret itself is only ever in the email.
--   declined_at       — the parent said no. Kept until expires_at so the
--                       young person can be told, then deleted.
--   expires_at        — after this the row is deleted, answered or not.
CREATE TABLE IF NOT EXISTS pending_signups (
    id            SERIAL PRIMARY KEY,
    username      TEXT NOT NULL,
    password_hash TEXT NOT NULL DEFAULT '',
    full_name     TEXT NOT NULL DEFAULT '',
    email         TEXT NOT NULL DEFAULT '',
    birth_date    DATE NOT NULL,
    country       TEXT NOT NULL DEFAULT '',
    provider      TEXT NOT NULL DEFAULT '',
    subject       TEXT NOT NULL DEFAULT '',
    parent_email  TEXT NOT NULL,
    token_hash    TEXT NOT NULL,
    emails_sent   INT NOT NULL DEFAULT 0,
    last_email_at TIMESTAMPTZ,
    declined_at   TIMESTAMPTZ,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at    TIMESTAMPTZ NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS pending_signups_username ON pending_signups (lower(username));
CREATE UNIQUE INDEX IF NOT EXISTS pending_signups_email ON pending_signups (lower(email)) WHERE email <> '';
CREATE UNIQUE INDEX IF NOT EXISTS pending_signups_token ON pending_signups (token_hash);
CREATE UNIQUE INDEX IF NOT EXISTS pending_signups_social ON pending_signups (provider, subject) WHERE provider <> '';
CREATE INDEX IF NOT EXISTS pending_signups_expires ON pending_signups (expires_at);

-- The record of a parent's consent: who said yes to what, when, and from
-- where. One row per account. Deleted with the account.
--
--   withdraw_hash — the SHA-256 of the secret in the "close this account"
--                   link the parent is sent with the confirmation.
--   wording       — which version of the consent words they agreed to.
CREATE TABLE IF NOT EXISTS parent_consents (
    user_id           INT PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
    parent_email      TEXT NOT NULL,
    parent_name       TEXT NOT NULL,
    relation          TEXT NOT NULL,
    parent_birth_year INT NOT NULL,
    child_birth_date  DATE NOT NULL,
    country           TEXT NOT NULL DEFAULT '',
    consent_ip        TEXT NOT NULL DEFAULT '',
    consent_country   TEXT NOT NULL DEFAULT '',
    user_agent        TEXT NOT NULL DEFAULT '',
    wording           TEXT NOT NULL,
    withdraw_hash     TEXT NOT NULL,
    consented_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE UNIQUE INDEX IF NOT EXISTS parent_consents_withdraw ON parent_consents (withdraw_hash);
