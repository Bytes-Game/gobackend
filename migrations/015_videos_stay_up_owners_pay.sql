-- ════════════════════════════════════════════════════════════════════════════
-- VIDEOS STAY UP; THEIR OWNERS PAY
-- ════════════════════════════════════════════════════════════════════════════
--
-- The owner asked for this: nothing is taken down for not matching its
-- challenge, however strong the evidence. The owner of the video pays in
-- rating points instead, and somebody who reports the other side of their
-- own battle pays when the video turns out to match. See offtopic.go.

-- How far a video's owner has been charged: 0 not at all, 1 for reports
-- alone, 2 when the model and a report agree. Each charge happens once, and
-- going from 1 to 2 charges only the difference.
ALTER TABLE challenges          ADD COLUMN IF NOT EXISTS penalty_level SMALLINT NOT NULL DEFAULT 0;
ALTER TABLE challenge_responses ADD COLUMN IF NOT EXISTS penalty_level SMALLINT NOT NULL DEFAULT 0;

-- When a report was charged as false: set once the model says the video
-- does match and the reporter was in the battle. NULL = not charged.
ALTER TABLE challenge_flags          ADD COLUMN IF NOT EXISTS charged_at TIMESTAMPTZ;
ALTER TABLE challenge_response_flags ADD COLUMN IF NOT EXISTS charged_at TIMESTAMPTZ;

-- Replaced by penalty_level. Nothing was ever taken down or charged under
-- them: the app that reports had not shipped.
ALTER TABLE challenges          DROP COLUMN IF EXISTS off_topic_at;
ALTER TABLE challenge_responses DROP COLUMN IF EXISTS off_topic_at;
ALTER TABLE challenges          DROP COLUMN IF EXISTS report_penalty_at;
ALTER TABLE challenge_responses DROP COLUMN IF EXISTS report_penalty_at;
