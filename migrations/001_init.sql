-- Battle Arena - PostgreSQL schema
-- Tables are also auto-created by the Go backend on startup (runMigrations).
-- This file is kept as a reference / manual migration script.

CREATE TABLE IF NOT EXISTS users (
    id          SERIAL PRIMARY KEY,
    username    VARCHAR(50) UNIQUE NOT NULL,
    password    VARCHAR(255) NOT NULL,
    full_name   VARCHAR(100) DEFAULT '',
    wins        INT DEFAULT 0,
    losses      INT DEFAULT 0,
    league      VARCHAR(20) DEFAULT 'Bronze'
);

CREATE TABLE IF NOT EXISTS follows(
    follower_id  INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    following_id INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    created_at   TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (follower_id, following_id)
);

CREATE TABLE IF NOT EXISTS posts (
    id                SERIAL PRIMARY KEY,
    author_id         INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    type              VARCHAR(10) NOT NULL DEFAULT 'image',
    content_url       TEXT DEFAULT '',
    thumbnail_url     TEXT DEFAULT '',
    caption           TEXT DEFAULT '',
    views             INT DEFAULT 0,
    created_at        TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS post_likes (
    post_id    INT NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
    user_id    INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (post_id, user_id)
);

CREATE TABLE IF NOT EXISTS comments(
    id         SERIAL PRIMARY KEY,
    post_id    INT NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
    author id  INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    text       Text NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);