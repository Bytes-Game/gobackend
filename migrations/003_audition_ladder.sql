-- Give every new video a small first audience instead of one big one.
--
-- Until now a new video was shown until it reached 300 views, and only then
-- was anyone allowed to say whether it was any good. Every video cost the
-- same 300 views whether it was loved or ignored from the very first showing.
--
-- That is what limits how many new videos the app can look at in a day. If a
-- page can only spare so many slots for unproven video, and each video eats
-- 300 of them, the number of videos that ever get a verdict is fixed — and
-- everything above that number just waits, forever, unwatched. Not rejected.
-- Never actually looked at.
--
-- So the audition becomes a ladder. A video gets a small first showing. If
-- people liked it, it gets a bigger one. If they clearly did not, it stops
-- taking free slots and the slots go to the next video in the queue. Most
-- videos stop at the first rung, so the average video costs far less than the
-- old flat 300, and far more videos get a real verdict per day.
--
-- The columns:
--
--   audition_state          where the video is: still being tried out
--                           ('auditioning'), finished and doing well enough to
--                           rank on its own ('graduated'), or tried and not
--                           picked up ('retired').
--   audition_stage          which rung of the ladder, counting from 0.
--   audition_stage_views    the view count when the current rung began, so a
--                           rung measures the views it actually earned rather
--                           than the total the video has ever had.
--   audition_run_views      the view count when the whole climb began. A rung
--                           resets its counter every promotion, so without this
--                           a video that arrived with a big view count — an
--                           import, a spike between two review passes, a worker
--                           that was stopped for a day — would look like it was
--                           starting fresh on every rung and would keep drawing
--                           the new-video push forever.
--   audition_score          the quality reading taken at the last review. Kept
--                           because the bar for passing is set from the other
--                           videos' readings, not from a number typed in here.
--   audition_reviewed_stage which rung that reading was for. Rung 2 videos have
--                           already beaten rung 1, so their scores run higher;
--                           comparing across rungs would fail nearly everyone.
--   audition_reviewed_at    when the last review happened.
--   audition_retries        how many second chances have been given. A retired
--                           video gets one, a month later, because "posted at a
--                           bad moment" is real and cheap to forgive.
--
-- Nothing here deletes or hides anything. A retired video stays in the app,
-- stays searchable, and can still be found and shared. Retiring only means it
-- stops being handed free slots on other people's feeds.

ALTER TABLE challenges
    ADD COLUMN IF NOT EXISTS audition_state          TEXT     NOT NULL DEFAULT 'auditioning',
    ADD COLUMN IF NOT EXISTS audition_stage          SMALLINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS audition_stage_views    INT      NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS audition_run_views      INT      NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS audition_score          REAL,
    ADD COLUMN IF NOT EXISTS audition_reviewed_stage SMALLINT,
    ADD COLUMN IF NOT EXISTS audition_reviewed_at    TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS audition_retries        SMALLINT NOT NULL DEFAULT 0;

-- Start every existing video on the rung its view count says it has already
-- earned. Without this, a video that already has 200 views would look like it
-- had just been uploaded, and would be reviewed against a rung it passed long
-- ago.
--
-- The numbers below are the ladder as it stands the day this migration runs
-- (60 views for the first rung, 240 more for the second, 300 total). They are
-- written out rather than read from the code on purpose: a migration records
-- what happened, so it must keep saying the same thing even after somebody
-- retunes the ladder.

UPDATE challenges
   SET audition_state = 'graduated'
 WHERE COALESCE(views, 0) >= 300;

UPDATE challenges
   SET audition_stage = 1,
       audition_stage_views = 60
 WHERE COALESCE(views, 0) >= 60
   AND COALESCE(views, 0) < 300;

-- The feed asks "what is still being tried out" on a timer, and the review
-- worker asks "what is due for a verdict". Both read this.
CREATE INDEX IF NOT EXISTS idx_challenges_audition_state
    ON challenges (audition_state, audition_stage)
    WHERE visibility = 'arena';

-- The review worker also asks "what did the other videos on this rung score",
-- to work out the bar. That reads only rows that have been reviewed.
CREATE INDEX IF NOT EXISTS idx_challenges_audition_scores
    ON challenges (audition_reviewed_stage, audition_reviewed_at)
    WHERE audition_score IS NOT NULL;
