-- Standalone equivalent of `go run ./cmd/seed -reset -yes-delete-existing-content`,
-- for running the seed with no Go toolchain installed. Paste it into any
-- Postgres client: Render's dashboard PSQL, pgAdmin, DBeaver, or psql itself.
--
-- WHAT THIS DOES
--   * DELETES every row in `challenges`. Responses, likes, dislikes and
--     visibility rows disappear with them via ON DELETE CASCADE. This cannot
--     be undone.
--   * Creates six demo accounts if they are missing. Accounts that ALREADY
--     exist are left completely alone — an account someone is actively using
--     will not have its password changed.
--   * Inserts nine challenges, each with one response, pointing at Google's
--     public ExoPlayer sample clips. Those were chosen because that bucket
--     does not rate-limit the way pub-*.r2.dev does, it honours HTTP range
--     requests (which the loopback media proxy depends on), and the
--     ForBigger* clips are 1280x720 — the ceiling the reels feed targets.
--
-- SAFETY
--   Everything runs inside one transaction. To preview instead of commit,
--   change the final COMMIT to ROLLBACK: every statement still executes, the
--   counts still print, and nothing is kept.
--
-- The password hash below is bcrypt for "password123". It is written to
-- password_hash, NOT password — IsValidUser reads password_hash first and
-- treats `password` as PLAINTEXT legacy, so a hash placed there would compare
-- the literal "$2a$..." string against what the user types and never match.

BEGIN;

-- Before state, so the scale of the delete is visible rather than assumed.
SELECT 'BEFORE' AS stage,
       (SELECT count(*) FROM challenges)          AS challenges,
       (SELECT count(*) FROM challenge_responses) AS responses,
       (SELECT count(*) FROM users)               AS users;

DELETE FROM challenges;

-- Demo accounts. ON CONFLICT DO NOTHING is what protects an existing
-- account's password: a username already present is skipped entirely.
INSERT INTO users (username, password, password_hash, full_name) VALUES
  ('player1', '', '$2a$10$Zq/VRC5JcBvrMflQDwqac.xWxeLe3GCcenheoj9J/exx1Dypzb78i', 'Player One'),
  ('player2', '', '$2a$10$Zq/VRC5JcBvrMflQDwqac.xWxeLe3GCcenheoj9J/exx1Dypzb78i', 'Player Two'),
  ('maya',    '', '$2a$10$Zq/VRC5JcBvrMflQDwqac.xWxeLe3GCcenheoj9J/exx1Dypzb78i', 'Maya Rivera'),
  ('deven',   '', '$2a$10$Zq/VRC5JcBvrMflQDwqac.xWxeLe3GCcenheoj9J/exx1Dypzb78i', 'Deven Shah'),
  ('nina',    '', '$2a$10$Zq/VRC5JcBvrMflQDwqac.xWxeLe3GCcenheoj9J/exx1Dypzb78i', 'Nina Kapoor'),
  ('omar',    '', '$2a$10$Zq/VRC5JcBvrMflQDwqac.xWxeLe3GCcenheoj9J/exx1Dypzb78i', 'Omar Haddad')
ON CONFLICT (username) DO NOTHING;

-- Challenges. Creators are assigned round-robin across the six accounts by
-- position, matching what the Go seeder does.
WITH base AS (
  SELECT 'https://commondatastorage.googleapis.com/gtv-videos-bucket/sample' AS v,
         'https://storage.googleapis.com/gtv-videos-bucket/sample/images'    AS t
),
seed_users AS (
  SELECT id, row_number() OVER (ORDER BY pos) - 1 AS idx
  FROM (
    SELECT u.id, x.pos
    FROM users u
    JOIN (VALUES ('player1',0),('player2',1),('maya',2),
                 ('deven',3),('nina',4),('omar',5)) AS x(uname, pos)
      ON x.uname = u.username
  ) s
),
clips(pos, file, subject, category, energy, emotions) AS (VALUES
  (0, 'ForBiggerBlazes',              'Can you top this entrance?',   'comedy',    'high',   '["funny","surprise"]'),
  (1, 'ForBiggerEscapes',             'Best escape move wins',        'sports',    'high',   '["excited"]'),
  (2, 'ForBiggerFun',                 'Show me your happy place',     'lifestyle', 'medium', '["happy"]'),
  (3, 'ForBiggerJoyrides',            'Dream ride challenge',         'lifestyle', 'high',   '["excited","happy"]'),
  (4, 'ForBiggerMeltdowns',           'Funniest meltdown reaction',   'comedy',    'high',   '["funny"]'),
  (5, 'VolkswagenGTIReview',          'Review your first car in 30s', 'tech',      'medium', '["curious"]'),
  (6, 'SubaruOutbackOnStreetAndDirt', 'Street or dirt — pick a side', 'sports',    'high',   '["excited"]'),
  (7, 'WeAreGoingOnBullrun',          'Road trip story time',         'story',     'medium', '["happy"]'),
  (8, 'WhatCarCanYouGetForAGrand',    'Best budget find challenge',   'education', 'low',    '["curious"]')
)
INSERT INTO challenges
  (creator_id, video_url, thumbnail_url, prefix, subject,
   visibility, status, category, emotion_tags, energy_level)
SELECT su.id,
       b.v || '/' || c.file || '.mp4',
       b.t || '/' || c.file || '.jpg',
       'I challenge you to',
       c.subject, 'arena', 'open', c.category, c.emotions::jsonb, c.energy
FROM clips c
CROSS JOIN base b
JOIN seed_users su ON su.idx = c.pos % 6
ORDER BY c.pos;

-- One response per challenge, using the next clip in the rotation. The battle
-- view swipes between challenger and responder, so a challenge with no
-- response exercises half the UI and none of the 3D cube.
WITH base AS (
  SELECT 'https://commondatastorage.googleapis.com/gtv-videos-bucket/sample' AS v,
         'https://storage.googleapis.com/gtv-videos-bucket/sample/images'    AS t
),
ordered AS (
  SELECT c.id,
         row_number() OVER (ORDER BY c.id) - 1 AS pos,
         count(*)    OVER ()                   AS total
  FROM challenges c
),
files(pos, file) AS (VALUES
  (0,'ForBiggerBlazes'), (1,'ForBiggerEscapes'), (2,'ForBiggerFun'),
  (3,'ForBiggerJoyrides'), (4,'ForBiggerMeltdowns'), (5,'VolkswagenGTIReview'),
  (6,'SubaruOutbackOnStreetAndDirt'), (7,'WeAreGoingOnBullrun'),
  (8,'WhatCarCanYouGetForAGrand')
),
responders AS (
  SELECT u.id, x.pos
  FROM users u
  JOIN (VALUES ('player1',0),('player2',1),('maya',2),
               ('deven',3),('nina',4),('omar',5)) AS x(uname, pos)
    ON x.uname = u.username
)
INSERT INTO challenge_responses
  (challenge_id, responder_id, video_url, thumbnail_url)
SELECT o.id,
       r.id,
       b.v || '/' || f.file || '.mp4',
       b.t || '/' || f.file || '.jpg'
FROM ordered o
CROSS JOIN base b
JOIN files f      ON f.pos = (o.pos + 1) % o.total
JOIN responders r ON r.pos = (o.pos + 1) % 6;

-- After state. Expect 9 challenges and 9 responses.
SELECT 'AFTER' AS stage,
       (SELECT count(*) FROM challenges)          AS challenges,
       (SELECT count(*) FROM challenge_responses) AS responses,
       (SELECT count(*) FROM users)               AS users;

-- Change to ROLLBACK to preview without keeping anything.
COMMIT;
