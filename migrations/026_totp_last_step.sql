-- Two-step sign-in (two_step_signin.go): the time step of the last
-- authenticator code accepted for the account.
--
-- A 6-digit code is good for about 30 seconds either side of now. Without
-- this, a code somebody watched being typed could be typed again by them
-- in that minute and a half. With it, a code is accepted once: only a code
-- from a later step than the last one used works.
ALTER TABLE user_totp ADD COLUMN IF NOT EXISTS last_used_step BIGINT;
