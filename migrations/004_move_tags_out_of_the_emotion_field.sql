-- Put the creator's tags in the field that means tags.
--
-- The app has been collecting tags from creators for a while, and sending
-- them in the emotionTags slot — because until now the backend had nowhere
-- else to put them. The code that did it said so, and said a follow-up would
-- move them properly. This is that follow-up.
--
-- The two fields mean different things, and mixing them cost twice over:
--
--   * emotion_tags is matched against the viewer's mood, from a fixed list of
--     sixteen words (happy, chill, intense, and so on). A free-text tag like
--     "hip hop" landing there matches nothing, and dilutes the real emotion
--     values it sits beside.
--   * custom_tags is what the ranker now reads as "what is this video about" —
--     it picks the category and it goes into the embedding that decides which
--     videos are similar. Nothing was ever in it.
--
-- So every video posted so far has its subject in a field nobody reads for
-- subject, and noise in a field that is read for mood.
--
-- WHAT THIS MOVES, AND WHAT IT LEAVES ALONE
--
-- Only values that are NOT one of the sixteen real emotion labels. That test
-- is what makes this safe rather than a guess: "hip hop" is certainly a tag
-- because it is not an emotion the app has ever offered, while "chill" is
-- left where it is because it might genuinely be one. A creator who typed
-- "chill" meaning it as a subject tag loses nothing they had — that value was
-- already being used as an emotion, and still will be.
--
-- Rows that already have custom_tags are not touched. Nothing is deleted from
-- emotion_tags either: a value that is really a tag does no harm sitting in
-- the emotion list as well, and removing it would make this migration
-- destructive for the sake of tidiness.

UPDATE challenges
   SET custom_tags = (
        SELECT COALESCE(jsonb_agg(v), '[]'::jsonb)
          FROM jsonb_array_elements_text(COALESCE(emotion_tags, '[]'::jsonb)) AS v
         WHERE lower(v) NOT IN (
               'happy','sad','intense','chill','inspiring','scary','funny',
               'serious','aggressive','romantic','nostalgic','satisfying',
               'cringe','wholesome','suspenseful','empowering')
       )
 WHERE COALESCE(jsonb_array_length(custom_tags), 0) = 0
   AND COALESCE(jsonb_array_length(emotion_tags), 0) > 0;

-- Anything that came out empty — a video whose emotion_tags held only real
-- emotions — goes back to an empty array rather than sitting as a stored
-- '[]' that looks like a decision. Same thing to every reader, but it keeps
-- the column honest about which rows this touched.
UPDATE challenges
   SET custom_tags = '[]'::jsonb
 WHERE custom_tags IS NULL;

-- The ranker asks "which videos share this tag" once tags start being used
-- for retrieval. A GIN index is what makes that answerable without reading
-- every row.
CREATE INDEX IF NOT EXISTS idx_challenges_custom_tags
    ON challenges USING GIN (custom_tags)
    WHERE custom_tags IS NOT NULL;
