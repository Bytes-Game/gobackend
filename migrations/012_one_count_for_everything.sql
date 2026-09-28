-- ════════════════════════════════════════════════════════════════════════════
-- ONE COUNT FOR EVERYTHING
-- ════════════════════════════════════════════════════════════════════════════
--
-- The same battle showed different numbers in different places. The reel said
-- 20 views, the live score said 6 and 3, the battle page said 31. Each place
-- counted its own way: the page added a view every time it was opened, the
-- live score left out the players themselves and very new accounts, and
-- nothing counted views or shares per video at all.
--
-- From here a view and a share are each recorded once, per person, per
-- video, and every screen reads the same counts. See counts.go.

-- A view: one person watched one video for a moment, once a day. The row is
-- what stops the same person counting twice; the counters on challenges and
-- challenge_responses are what screens read.
--
-- response_id 0 is the creator's video. A real answer id can't be a foreign
-- key alongside that 0, so answers are checked by the code that writes here.
CREATE TABLE IF NOT EXISTS video_views (
    user_id      INT  NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    challenge_id INT  NOT NULL REFERENCES challenges(id) ON DELETE CASCADE,
    response_id  INT  NOT NULL DEFAULT 0,
    day          DATE NOT NULL,
    PRIMARY KEY (user_id, challenge_id, response_id, day)
);

-- A share: one person shared one video. Once each, so the count and the
-- "who shared" list are the same thing.
CREATE TABLE IF NOT EXISTS video_shares (
    user_id      INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    challenge_id INT NOT NULL REFERENCES challenges(id) ON DELETE CASCADE,
    response_id  INT NOT NULL DEFAULT 0,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (user_id, challenge_id, response_id)
);
CREATE INDEX IF NOT EXISTS idx_video_shares_challenge ON video_shares (challenge_id);

-- challenges.views is now the whole card: the creator's video and every
-- answer's, added up. challenge_responses.views is one answer's share of it.
-- Nothing ever wrote to challenge_responses.views before, so anything there
-- came from seed data and was never part of challenges.views. Fold it in, so
-- the total is the sum of its parts from the start.
UPDATE challenges c
   SET views = c.views + s.total
  FROM (SELECT challenge_id, SUM(views) AS total
          FROM challenge_responses
         GROUP BY challenge_id) s
 WHERE s.challenge_id = c.id
   AND s.total > 0;
