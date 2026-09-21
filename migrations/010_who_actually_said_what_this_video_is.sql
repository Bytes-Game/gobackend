-- Keep what a PERSON said about their video apart from what the app worked out.
--
-- ════════════════════════════════════════════════════════════════════════════
-- THE PROBLEM
-- ════════════════════════════════════════════════════════════════════════════
--
-- There has only ever been one category column, and three different things
-- get written into it at upload time (categoryForContent in content_tags.go):
--
--   1. the category the creator picked
--   2. a category read off the creator's tags
--   3. a keyword guess at the creator's subject line
--
-- Three sources, one column, and nothing recording which one it was.
--
-- That matters because the ranker then reads that column back and treats it
-- as "what the creator said" (categoryFromEvidence). So a guess THIS SERVER
-- made off a title gets weighed against the model's reading of the video as
-- though a human had made a claim — and the admin view reports "the creator
-- and the model disagree" about videos whose creator said nothing at all.
--
-- It also cuts the other way. Nothing ever wrote the model's conclusion back
-- here, so the column stayed at its upload-time value forever. The feed used
-- the model, because it recomputed the answer on every score; anything
-- reading the column directly — the creator dashboard's "by category", any
-- report that groups by it — saw a value decided before the video had been
-- watched.
--
-- ════════════════════════════════════════════════════════════════════════════
-- WHAT IS ADDED
-- ════════════════════════════════════════════════════════════════════════════
--
--   creator_category  ONLY what a person chose. '' means they did not choose.
--                     A guess never goes in here, whatever else happens.
--
--   machine_category  what the model concluded after watching the video.
--                     '' means it had no opinion — which is a real answer and
--                     not the same as not having been asked.
--
--   category_source   how the category column got its current value:
--                     'agreed'  both looked and said the same thing
--                     'machine' the model decided
--                     'creator' the creator decided, model had no opinion
--                     'guess'   nobody knew, keyword match on the words
--                     ''        set before this column existed
--
-- category itself keeps its meaning: the app's single best answer. Everything
-- already reading it carries on working, and starts getting a better value,
-- because storeVideoAnalysis now writes the model's verdict into it.
--
-- ════════════════════════════════════════════════════════════════════════════
-- WHY THERE IS NO BACKFILL OF creator_category
-- ════════════════════════════════════════════════════════════════════════════
--
-- Because it cannot be done honestly. For a row that already exists, the
-- category column might be a creator's pick or might be this server's guess,
-- and there is nothing anywhere that says which. Copying it across would
-- manufacture exactly the false claim this migration exists to stop — and
-- would do it to every historical row at once.
--
-- So every existing row starts with creator_category = '', meaning "we do not
-- know that anybody said anything". That is the truth. It costs almost
-- nothing in practice: a check of the live platform found 43 of 44 videos
-- with no creator category at all, because the app's dropdown defaulted to
-- 'other' and 'other' is what gets stored when nobody chose.
--
-- machine_category is left empty for the same kind of reason, and fills in on
-- its own: the next time a video is analysed, the worker writes it.

ALTER TABLE challenges
    ADD COLUMN IF NOT EXISTS creator_category VARCHAR(30) NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS machine_category VARCHAR(30) NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS category_source  VARCHAR(16) NOT NULL DEFAULT '';

ALTER TABLE challenge_responses
    ADD COLUMN IF NOT EXISTS creator_category VARCHAR(30) NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS machine_category VARCHAR(30) NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS category_source  VARCHAR(16) NOT NULL DEFAULT '';

-- ════════════════════════════════════════════════════════════════════════════
-- ONE THING CAN BE BACKFILLED, AND IT IS WORTH DOING
-- ════════════════════════════════════════════════════════════════════════════
--
-- Not the creator's word, but the SOURCE of the value already stored. Where a
-- row has a real category on it today, it was decided before the video was
-- watched, so whatever it is, it is not the machine's. Marking those 'guess'
-- rather than leaving them blank is what lets the admin view and the dashboard
-- say "nothing has watched this yet" instead of showing an answer with no
-- provenance and hoping the reader knows.
--
-- Rows with no real category keep '' — nothing was decided, so there is no
-- source to record.

UPDATE challenges
   SET category_source = 'guess'
 WHERE category_source = ''
   AND COALESCE(category, '') NOT IN ('', 'other', 'general');

UPDATE challenge_responses
   SET category_source = 'guess'
 WHERE category_source = ''
   AND COALESCE(category, '') NOT IN ('', 'other', 'general');
