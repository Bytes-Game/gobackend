-- Music searches, kept.
--
-- Every answer Openverse gives the music picker is saved here, so the next
-- person to search the same thing — tomorrow, next week, after a restart —
-- is answered from here without spending any of the day's searches. An
-- answer is asked for again after 30 days, so new songs turn up and songs
-- that have gone away drop out; if Openverse cannot answer then, the saved
-- one is used anyway.
--
-- A saved answer is only the list of song ids, in order. The songs
-- themselves are kept once each in music_tracks, however many searches
-- found them, which is what keeps this small: a few kilobytes per search.
-- free_music.go keeps at most musicSavedMax of them, dropping the ones used
-- least recently, and with them every song that no saved search, post or
-- pick still needs.
CREATE TABLE IF NOT EXISTS music_searches (
    query      TEXT NOT NULL,
    page       INT  NOT NULL,
    source_ids TEXT[] NOT NULL DEFAULT '{}',
    has_more   BOOLEAN NOT NULL DEFAULT FALSE,
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    used_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    times_used INT NOT NULL DEFAULT 1,
    PRIMARY KEY (query, page)
);

CREATE INDEX IF NOT EXISTS idx_music_searches_used ON music_searches (used_at);

-- When a song was last found by a search or picked. A song nothing needs
-- any more is only removed once it has not been seen for a week, so one
-- somebody has just picked is never removed under them.
ALTER TABLE music_tracks
    ADD COLUMN IF NOT EXISTS seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW();
