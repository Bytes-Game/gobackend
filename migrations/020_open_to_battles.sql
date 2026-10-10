-- Whether anyone may answer a post with their own video.
--
-- On ("open to battles") is how every post has always worked: somebody
-- answers it, and the two videos battle for votes. Off makes it a normal
-- post — people watch, like and comment, but nobody can answer it, and the
-- feed treats it the way it treats any short. The owner can change it at
-- any time (see battles_open.go).
--
-- Every post that already exists stays open, and new ones are open unless
-- the creator says otherwise. A constant default is added without
-- rewriting the table.
ALTER TABLE challenges
    ADD COLUMN IF NOT EXISTS open_to_battles BOOLEAN NOT NULL DEFAULT TRUE;
