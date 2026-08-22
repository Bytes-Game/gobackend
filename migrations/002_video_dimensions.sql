-- Record how big each uploaded video actually is.
--
-- The phone uploads straight to object storage, so nothing here has ever known
-- the real size of what was posted. The app shrinks video to 720p before
-- sending, but that is a promise made by code on someone else's device — a
-- profile run found files named "720p.mp4" that were really 1920x1080.
--
-- video_probe.go measures the file after upload and writes the answer here.
-- Two things then become possible that were not before:
--
--   * refusing anything above the ceiling at creation time, and
--   * keeping a large un-transcoded file away from a phone whose decoder
--     cannot cope with it.
--
-- NULL means "not measured". That is the honest state for every row that
-- existed before this migration, and for any upload where the probe could not
-- reach storage. Readers must treat NULL as "unknown", never as "small" —
-- assuming small is how an unmeasured 4K file reaches a cheap phone.

ALTER TABLE challenges
    ADD COLUMN IF NOT EXISTS video_width  INT,
    ADD COLUMN IF NOT EXISTS video_height INT;

ALTER TABLE challenge_responses
    ADD COLUMN IF NOT EXISTS video_width  INT,
    ADD COLUMN IF NOT EXISTS video_height INT;

-- The feed asks "is this too big for the phone that is asking", which reads
-- the larger of the two sides. A partial index keeps it small: rows that were
-- never measured are exactly the rows this index has nothing to say about.
CREATE INDEX IF NOT EXISTS idx_challenges_video_size
    ON challenges (GREATEST(video_width, video_height))
    WHERE video_width IS NOT NULL AND video_height IS NOT NULL;
