-- ════════════════════════════════════════════════════════════════════════════
-- VIDEOS THAT DON'T MATCH THEIR CHALLENGE
-- ════════════════════════════════════════════════════════════════════════════
--
-- Somebody could answer "Who can juggle five balls?" with a cat video and
-- nothing would happen. Somebody could post that question over a cooking clip
-- and nothing would happen either. See offtopic.go for the whole rule.
--
-- In short: a video is taken down when two things agree that it does not
-- match — the model that watches every upload, and a person who reported it —
-- or when three people with no stake in the battle all report it.

-- What the model said when asked "does this video match its challenge":
-- 'yes', 'no', 'unsure', or '' when it has not been asked yet. Kept apart
-- from video_analysis so it can be read without unpacking the JSON.
ALTER TABLE challenges          ADD COLUMN IF NOT EXISTS question_match TEXT NOT NULL DEFAULT '';
ALTER TABLE challenge_responses ADD COLUMN IF NOT EXISTS question_match TEXT NOT NULL DEFAULT '';

-- When the video was taken down for not matching. NULL = never. This is what
-- stops the same video being ruled on, and its owner penalised, twice.
ALTER TABLE challenges          ADD COLUMN IF NOT EXISTS off_topic_at TIMESTAMPTZ;
ALTER TABLE challenge_responses ADD COLUMN IF NOT EXISTS off_topic_at TIMESTAMPTZ;

-- Reports against a challenge's own video. Answers already had
-- challenge_response_flags; the creator's video had nothing. One report per
-- person per video.
CREATE TABLE IF NOT EXISTS challenge_flags (
    challenge_id INT NOT NULL REFERENCES challenges(id) ON DELETE CASCADE,
    user_id      INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    reason       VARCHAR(40) NOT NULL DEFAULT 'off_topic',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (challenge_id, user_id)
);
