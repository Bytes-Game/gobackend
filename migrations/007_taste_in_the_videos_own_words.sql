-- Remember what a viewer likes in the words videos are actually described by.
--
-- ════════════════════════════════════════════════════════════════════════════
-- WHY
-- ════════════════════════════════════════════════════════════════════════════
--
-- A taste profile was eighteen numbers wide, one per category, because that is
-- all a video could be described as. So "likes nature videos" was not a thing
-- this app could think: the bee-on-a-thistle clip, the butterfly and the
-- opening flower were filed by their creators under lifestyle, art and comedy,
-- and a viewer who watched all three learned three unrelated preferences.
--
-- These two columns hold the same evidence against what videos are actually
-- about. Open vocabulary — a subject exists the first time a video is about
-- it, so nothing has to be added here when the content changes.
--
-- topic_affinity keeps its SIGN, unlike category_affinity which is clamped at
-- zero and needs a separate pass to remember dislikes and paste them back. A
-- subject somebody has rejected sits at a negative number and stays there.
--
-- avoided_topics is the corroborated end of that: subjects pushed back on hard
-- enough to stop offering at all.

ALTER TABLE user_profiles
    ADD COLUMN IF NOT EXISTS topic_affinity JSONB DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS avoided_topics JSONB DEFAULT '[]'::jsonb;

-- No backfill, and nothing to backfill from: this is evidence that was never
-- recorded, not evidence stored in the wrong shape. Profiles rebuild from the
-- last 500 events on their own schedule, so every active viewer fills these in
-- without anything here having to guess.
