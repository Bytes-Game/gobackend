-- Muted chats (chat_mute.go). One row per person per chat they muted: no
-- notification on their phone for that person's messages, and that chat
-- left out of the number on the Messages button. The messages themselves
-- arrive as ever. Unmuting deletes the row.
--
-- Goes with either account: deleting one deletes its rows.
CREATE TABLE IF NOT EXISTS chat_mutes (
    user_id  INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    other_id INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    muted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (user_id, other_id)
);
