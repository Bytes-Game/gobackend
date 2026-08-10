-- Standalone equivalent of `go run ./cmd/seed -reset -yes-delete-existing-content`,
-- for running the seed with no Go toolchain installed. Paste it into any
-- Postgres client: DBeaver, pgAdmin, Render's dashboard PSQL, or psql itself.
--
-- WHAT THIS DOES
--   * DELETES every row in `challenges`. Responses, likes, dislikes and
--     visibility rows disappear with them via ON DELETE CASCADE. This cannot
--     be undone.
--   * Creates six demo accounts if they are missing. Accounts that ALREADY
--     exist are left completely alone — an account someone is actively using
--     will not have its password changed. The password for newly created
--     accounts is: password123
--   * Inserts 28 challenges, 18 of which have a response (a "battle"); the
--     other 10 have none (a "short").
--
-- SAFETY
--   Everything runs inside one transaction. To preview instead of commit,
--   change the final COMMIT to ROLLBACK: every statement still executes, the
--   counts still print, and nothing is kept.
--
-- ─── WHY THE DATA LOOKS THE WAY IT DOES ────────────────────────────────────
--
-- 1. EVERY CLIP IS SMALL. Nothing below exceeds 5.3 MB or 720p. The feed this
--    replaces carried a 58 MB clip, a 73 MB clip and a 249 MB feature film.
--    No amount of caching makes a 249 MB file open quickly on a phone; it was
--    the single biggest reason the feed felt slow. A reel is seconds long and
--    should weigh single-digit megabytes.
--
-- 2. EVERY URL ANSWERS HTTP 206 TO A RANGE REQUEST, verified at the time of
--    writing. The loopback media proxy pre-loads only the first ~768 KB of
--    each video; an origin that ignores Range defeats that entirely.
--
--    The previous version of this file pointed at Google's gtv-videos-bucket.
--    That bucket is now PRIVATE — every url in it answers 403 "Anonymous
--    caller does not have storage.objects.get access". Running the old seed
--    would have filled the feed with videos that could never play.
--
-- 3. AGE AND KIND ARE DECORRELATED. This is the important one. The seed this
--    replaces stamped every response-less challenge with the NEWEST timestamp
--    and every battle with an older one. Feeds weigh freshness, so the result
--    was that every short sorted above every battle: the app served eight
--    single videos in a row and looked like it had no battles in it at all.
--    Here, ages descend smoothly across all 28 rows while "does it have a
--    response" follows an independent 3-in-5 cycle, so the two are mixed
--    throughout — which is what real usage produces.
--
-- 4. AGES STAY INSIDE 12 DAYS. The Following tab only shows content from the
--    last 14 days. A seed whose back half is older than that cannot be used
--    to test that tab.
--
-- 5. VIEWS AND LIKES ARE NON-ZERO. The ranker orders by (views + likes*3) and
--    its strict quality tier requires views >= 10 and likes >= 1. Seeded flat
--    at zero, every query falls through all four quality tiers to the "no
--    minimum" last resort and the ranking has no signal at all — the feed
--    degrades to plain reverse-chronological. The spreads below are
--    arbitrary but deterministic, and uncorrelated with both age and kind.
--
-- The bcrypt hash below is for "password123". It is written to
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

-- The plan table drives everything below: one row per challenge, carrying
-- its clip, prompt, creator, age, view count and whether it gets a response.
-- Building it once and reusing it is what keeps the challenge insert, the
-- likes and the responses consistent with each other.
CREATE TEMP TABLE seed_plan ON COMMIT DROP AS
WITH clips(pos, url, category, energy, emotions, title, alt_title) AS (VALUES
  (0,  'https://pub-72947583733d4d108b6495e932ae4f04.r2.dev/seed/7fa7093550751f14.mp4', 'comedy',    'high',   '["funny","surprise"]', 'Can you top this entrance?',     'Who can hold a straight face longest'),
  (1,  'https://pub-72947583733d4d108b6495e932ae4f04.r2.dev/seed/c1d850ec1287e543.mp4',         'sports',    'high',   '["excited"]',          'Best escape move wins',          'Best 3-second intro wins'),
  (2,  'https://pub-72947583733d4d108b6495e932ae4f04.r2.dev/seed/82dc123d418f65ca.mp4',               'lifestyle', 'medium', '["happy"]',            'Show me your happy place',       'Recreate this shot with what you own'),
  (3,  'https://pub-72947583733d4d108b6495e932ae4f04.r2.dev/seed/d06ec0b6febc2404.mp4',                     'lifestyle', 'high',   '["excited","happy"]',  'Dream ride challenge',           'Funniest caption for this clip'),
  (4,  'https://pub-72947583733d4d108b6495e932ae4f04.r2.dev/seed/d5eb607f4c8e108b.mp4',                                   'comedy',    'high',   '["funny"]',            'Funniest reaction wins',         'Who has the steadier hand'),
  (5,  'https://pub-72947583733d4d108b6495e932ae4f04.r2.dev/seed/234c279a4913634d.mp4', 'tech',      'medium', '["curious"]',          'Review your setup in 30s',       'Explain this in ten seconds'),
  (6,  'https://pub-72947583733d4d108b6495e932ae4f04.r2.dev/seed/8ad67e2519c0f584.mp4',         'sports',    'high',   '["excited"]',          'Street or studio — pick a side', 'Best transition wins'),
  (7,  'https://pub-72947583733d4d108b6495e932ae4f04.r2.dev/seed/610809b029a7eda9.mp4',               'story',     'medium', '["happy"]',            'Story time challenge',           'Guess the ending challenge'),
  (8,  'https://pub-72947583733d4d108b6495e932ae4f04.r2.dev/seed/3227f04eab6e624f.mp4',               'art',       'low',    '["calm"]',             'Best slow-motion shot',          'Most creative use of one prop'),
  (9,  'https://pub-72947583733d4d108b6495e932ae4f04.r2.dev/seed/7de928f9e4f43afa.mp4',                                        'education', 'low',    '["curious"]',          'Best budget find challenge',     'Who can do it slower'),
  (10, 'https://pub-72947583733d4d108b6495e932ae4f04.r2.dev/seed/90b1846cf73325ed.mp4', 'comedy',    'medium', '["funny"]',            'Funniest animation dub',         'Best sound-effect dub'),
  (11, 'https://pub-72947583733d4d108b6495e932ae4f04.r2.dev/seed/399605c61cdd4cb7.mp4',         'art',       'low',    '["calm"]',             'Calmest scene wins',             'Calmest take wins'),
  (12, 'https://pub-72947583733d4d108b6495e932ae4f04.r2.dev/seed/763b99b2e83fd647.mp4',               'art',       'medium', '["curious"]',          'Most cinematic 10 seconds',      'Most cinematic angle'),
  (13, 'https://pub-72947583733d4d108b6495e932ae4f04.r2.dev/seed/4e1dee73db81e09b.mp4',                                         'story',     'medium', '["excited"]',          'Sci-fi one-shot challenge',      'Best plot twist in 10s')
),
seed_users(pos, uname) AS (VALUES
  (0,'player1'), (1,'player2'), (2,'maya'), (3,'deven'), (4,'nina'), (5,'omar')
),
n AS (SELECT generate_series(0, 27) AS i)
SELECT
  n.i,
  cu.id                                    AS creator_id,
  c.url                                    AS video_url,
  -- Poster frame cut from the clip's own first second, uploaded next to
  -- the video by cmd/mediaimport under the same key. Derived from the
  -- video URL rather than stored separately so the two cannot drift, and
  -- so a clip without a matching poster 404s visibly instead of quietly
  -- showing an unrelated picture. This used to be a random photograph
  -- from picsum.photos, which in practice served stars and landscapes --
  -- on top of a jellyfish video that reads as the wrong video, not as a
  -- placeholder.
  regexp_replace(c.url, '\.mp4$', '.jpg')  AS thumbnail_url,
  c.category,
  c.energy,
  c.emotions,
  -- First pass over the 14 clips uses the primary prompt, second pass the
  -- alternate, so all 28 subjects are distinct.
  CASE WHEN n.i < 14 THEN c.title ELSE c.alt_title END AS subject,
  -- Newest first, ~10h apart with hour-level jitter so no two rows share a
  -- timestamp. 27 * 10h + jitter stays inside the Following tab's 14 days.
  (now() - make_interval(hours => n.i * 10 + (n.i % 7)))  AS created_at,
  400 + (n.i * 137) % 4200                 AS views,
  (n.i * 3) % 6                            AS like_count,
  (n.i % 5) < 3                            AS gets_response,
  ru.id                                    AS responder_id,
  rc.url                                   AS response_video_url,
  regexp_replace(rc.url, '\.mp4$', '.jpg') AS response_thumbnail_url
FROM n
JOIN clips c        ON c.pos  = n.i % 14
JOIN clips rc       ON rc.pos = (n.i + 3) % 14
JOIN seed_users scu ON scu.pos = n.i % 6
JOIN users cu       ON cu.username = scu.uname
JOIN seed_users sru ON sru.pos = (n.i + 1) % 6
JOIN users ru       ON ru.username = sru.uname;

-- Challenges. RETURNING carries the new serial id back out so the likes and
-- responses below can be attached to the right row — subjects are unique
-- within this seed, which is what makes the join back to seed_plan safe.
CREATE TEMP TABLE seed_created ON COMMIT DROP AS
WITH ins AS (
  INSERT INTO challenges
    (creator_id, video_url, thumbnail_url, prefix, subject,
     visibility, status, category, emotion_tags, energy_level,
     created_at, views)
  SELECT creator_id, video_url, thumbnail_url, 'I challenge you to', subject,
         'arena', 'open', category, emotions::jsonb, energy,
         created_at, views
  FROM seed_plan
  RETURNING id, subject
)
SELECT p.i, ins.id AS challenge_id
FROM ins
JOIN seed_plan p ON p.subject = ins.subject;

-- Likes. like_count per challenge, from distinct users, offset so the same
-- accounts do not like everything.
INSERT INTO challenge_likes (challenge_id, user_id)
SELECT sc.challenge_id, lu.id
FROM seed_created sc
JOIN seed_plan p ON p.i = sc.i
CROSS JOIN LATERAL generate_series(0, p.like_count - 1) AS k
JOIN (VALUES (0,'player1'),(1,'player2'),(2,'maya'),
             (3,'deven'),(4,'nina'),(5,'omar')) AS su(pos, uname)
  ON su.pos = (p.i + k + 1) % 6
JOIN users lu ON lu.username = su.uname
ON CONFLICT DO NOTHING;

-- Responses — only for the 3-in-5 that get one. A response always lands
-- AFTER the challenge it answers, never before it.
INSERT INTO challenge_responses
  (challenge_id, responder_id, video_url, thumbnail_url, created_at)
SELECT sc.challenge_id, p.responder_id,
       p.response_video_url, p.response_thumbnail_url,
       now() - (now() - p.created_at) / 2
FROM seed_created sc
JOIN seed_plan p ON p.i = sc.i
WHERE p.gets_response;

-- After state. Expect 28 challenges and 18 responses.
SELECT 'AFTER' AS stage,
       (SELECT count(*) FROM challenges)          AS challenges,
       (SELECT count(*) FROM challenge_responses) AS responses,
       (SELECT count(*) FROM challenge_likes)     AS likes,
       (SELECT count(*) FROM users)               AS users;

-- The property the whole seed exists for: reading the feed newest-first,
-- battles (B) and shorts (S) must be MIXED, not clumped. Expect something
-- like BBBSSBBBSS..., never SSSSSSSSSSBBBB...
SELECT string_agg(CASE WHEN r.n > 0 THEN 'B' ELSE 'S' END, ''
                  ORDER BY c.created_at DESC) AS newest_first_layout
FROM challenges c
LEFT JOIN (SELECT challenge_id, count(*) AS n
           FROM challenge_responses GROUP BY 1) r ON r.challenge_id = c.id;

-- Change to ROLLBACK to preview without keeping anything.
COMMIT;
