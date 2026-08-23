-- What a video turned out to be, from looking at the video.
--
-- Until now the app knew two things about a video's subject: what the creator
-- typed, and what its audience did afterwards. Both leave the same hole — a
-- video posted a minute ago with a thin caption is nearly invisible to the
-- first and completely unknown to the second, and that is exactly the moment
-- the feed most needs something to go on.
--
-- The transcode worker already downloads every upload and already runs
-- ffmpeg over it. So it now also measures the video while it has it, and
-- sends the result back with the manifest. See cmd/hls-worker/analyze.go.
--
-- WHAT IS STORED
--
--   video_analysis   the whole reading, as JSON. How fast it cuts, how loud,
--                    how much of it is not silence, how bright, plus any text
--                    found on screen and anything said out loud.
--
--   auto_tags        tags derived from all of that, in the same shape and the
--                    same normalized form as the creator's own custom_tags.
--                    Kept SEPARATE from custom_tags on purpose: a creator
--                    editing their tags must never silently wipe machine
--                    findings, and a machine must never appear to put words
--                    in a creator's mouth. The ranker reads both together.
--
-- Both are nullable and empty by default. Every pass in the worker is
-- optional and fails silent — no OCR binary means no screen text, no speech
-- model means no transcript — so "absent" is an ordinary state, not an error,
-- and readers must treat it as "not measured" rather than "measured as
-- nothing".

ALTER TABLE challenges
    ADD COLUMN IF NOT EXISTS video_analysis JSONB,
    ADD COLUMN IF NOT EXISTS auto_tags      JSONB DEFAULT '[]'::jsonb;

ALTER TABLE challenge_responses
    ADD COLUMN IF NOT EXISTS video_analysis JSONB,
    ADD COLUMN IF NOT EXISTS auto_tags      JSONB DEFAULT '[]'::jsonb;

-- "Which videos share this tag" is the question the ranker asks once tags
-- drive retrieval. A GIN index is what makes that answerable without reading
-- every row. Partial, because a row with no machine tags has nothing to say.
CREATE INDEX IF NOT EXISTS idx_challenges_auto_tags
    ON challenges USING GIN (auto_tags)
    WHERE auto_tags IS NOT NULL AND auto_tags <> '[]'::jsonb;

-- The admin view wants "how many uploads have we actually looked at", which
-- is a presence test rather than a search over the contents.
CREATE INDEX IF NOT EXISTS idx_challenges_analyzed
    ON challenges ((video_analysis IS NOT NULL))
    WHERE video_analysis IS NOT NULL;
