-- Free music for videos.
--
-- The video editor has a Music button. It searches every song whose licence
-- lets anybody put it in their own video, in an app that may make money, for
-- free (see free_music.go). A song somebody actually used is kept here, so
-- the post can always credit it — the licences that allow this ask for the
-- artist's name in return.
--
-- One row per song, however many posts use it. use_count is how many posts
-- have; the picker shows the most used first, the way "popular sounds" work
-- elsewhere. blocked is for a song that turns out not to be what its
-- licence claimed (somebody uploaded a film song under a free licence that
-- was never theirs to give): it stops being offered, and cannot be added to
-- a new post.
CREATE TABLE IF NOT EXISTS music_tracks (
    id              SERIAL PRIMARY KEY,
    source          TEXT NOT NULL,
    source_id       TEXT NOT NULL,
    title           TEXT NOT NULL DEFAULT '',
    artist          TEXT NOT NULL DEFAULT '',
    duration_ms     INT  NOT NULL DEFAULT 0,
    audio_url       TEXT NOT NULL DEFAULT '',
    license         TEXT NOT NULL,
    license_version TEXT NOT NULL DEFAULT '',
    license_url     TEXT NOT NULL DEFAULT '',
    source_url      TEXT NOT NULL DEFAULT '',
    provider        TEXT NOT NULL DEFAULT '',
    attribution     TEXT NOT NULL DEFAULT '',
    genres          TEXT[] NOT NULL DEFAULT '{}',
    use_count       INT  NOT NULL DEFAULT 0,
    blocked         BOOLEAN NOT NULL DEFAULT FALSE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (source, source_id)
);

CREATE INDEX IF NOT EXISTS idx_music_tracks_popular
    ON music_tracks (use_count DESC) WHERE NOT blocked;

-- Which song a post or an answer uses, if any. Empty for nearly all of them.
ALTER TABLE challenges
    ADD COLUMN IF NOT EXISTS music_track_id INT REFERENCES music_tracks(id) ON DELETE SET NULL;
ALTER TABLE challenge_responses
    ADD COLUMN IF NOT EXISTS music_track_id INT REFERENCES music_tracks(id) ON DELETE SET NULL;
