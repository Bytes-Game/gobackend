-- A profile photo, and a word for what a profile is about.
--
-- avatar_url is the address of the person's own photo, uploaded the way a
-- photo post is (u/<their id>/<upload>/photo.jpg). Empty means no photo:
-- the app shows their initial, as it always has. The address is checked
-- when it is set (profile_photo.go), so it can only ever be a picture that
-- person uploaded.
--
-- profile_tag is one word they pick to say what their profile is about —
-- "Influencer", "Motivator" — from a fixed list (profileTags in
-- profile_photo.go). Empty means they have not picked one.
--
-- Both are added with a constant default, which Postgres does without
-- rewriting the table.
ALTER TABLE users
    ADD COLUMN IF NOT EXISTS avatar_url  TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS profile_tag TEXT NOT NULL DEFAULT '';
