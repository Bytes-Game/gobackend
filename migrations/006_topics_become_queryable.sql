-- Make what a video is ABOUT something the database can be asked about.
--
-- ════════════════════════════════════════════════════════════════════════════
-- WHY
-- ════════════════════════════════════════════════════════════════════════════
--
-- The model already writes what a video is about in its own words — "thistle",
-- "pollination", "dark fantasy", "street food". Those went into the
-- video_analysis JSON blob, where nothing can query them: answering "which
-- other videos are about thistles" meant reading and parsing every row.
--
-- So the richest thing we know about a video was write-only. The feed decided
-- what to show you from ONE word picked out of a list of eighteen, and every
-- topic the model produced sat in a column nobody could search.
--
-- This is the column that makes "which videos are like this one" a question
-- Postgres can answer.
--
-- ════════════════════════════════════════════════════════════════════════════
-- WHY A SEPARATE COLUMN RATHER THAN FOLDING INTO auto_tags
-- ════════════════════════════════════════════════════════════════════════════
--
-- auto_tags is a CLOSED vocabulary: every entry is checked against the
-- categories and feelings the backend defines, and anything else is dropped.
-- That check is what stops a model inventing a tag nothing can ever match on.
--
-- Topics are the opposite by design — open vocabulary, no list, whatever fits
-- the video. Mixing them would either force topics through a filter that
-- exists to reject them, or remove the filter that keeps auto_tags meaningful.
--
-- Read together, stored apart. Same reasoning that keeps machine tags out of
-- the creator's own tag column.

ALTER TABLE challenges
    ADD COLUMN IF NOT EXISTS content_topics JSONB DEFAULT '[]'::jsonb;

ALTER TABLE challenge_responses
    ADD COLUMN IF NOT EXISTS content_topics JSONB DEFAULT '[]'::jsonb;

-- "Which videos share this topic" is the whole point, and it is the question
-- a GIN index answers without reading every row.
--
-- Partial, because a video nothing has described has nothing to say and would
-- only make the index bigger. Most of the catalogue is in that state today.
CREATE INDEX IF NOT EXISTS idx_challenges_content_topics
    ON challenges USING GIN (content_topics)
    WHERE content_topics IS NOT NULL AND content_topics <> '[]'::jsonb;

CREATE INDEX IF NOT EXISTS idx_responses_content_topics
    ON challenge_responses USING GIN (content_topics)
    WHERE content_topics IS NOT NULL AND content_topics <> '[]'::jsonb;

-- ════════════════════════════════════════════════════════════════════════════
-- WHY THIS DOES NOT BACKFILL
-- ════════════════════════════════════════════════════════════════════════════
--
-- The obvious next line is an UPDATE lifting topics out of video_analysis for
-- the rows that already have them. It is deliberately not here.
--
-- Topics are folded before they are stored — lowercased, separators collapsed,
-- punctuation dropped, capped in length — so that "Close-Up" and "close up"
-- are one topic and not two. That folding is defined once, in Go, in
-- normalizeOneTag. A backfill would have to reproduce it exactly in SQL, and
-- anything it got even slightly wrong would produce topics that look right and
-- silently match nothing: "close-up" from a backfilled row would never equal
-- "close up" from a freshly analysed one, and the two videos would look
-- unrelated for a reason no one could see.
--
-- Two definitions of the same rule, in two languages, is a bug waiting for
-- the first video whose topic contains a hyphen. There is exactly one such
-- topic in the catalogue today, which is how close this came to shipping.
--
-- And migrations here are FATAL ON FAILURE: a mistake in a data transform does
-- not just skip the backfill, it stops the backend from booting.
--
-- So the column fills the same way it will fill forever: the worker writes it
-- when it analyses a video. The catalogue is being re-analysed anyway, and a
-- column that fills over an hour through the one code path that defines its
-- shape beats one that fills instantly in two slightly different shapes.
