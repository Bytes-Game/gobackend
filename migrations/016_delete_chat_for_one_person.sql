-- "Delete chat", for the person who deletes it.
--
-- Deleting a chat takes it off that person's list, with every message in it
-- up to that moment. The other person keeps their copy, the way it works in
-- every chat app: one person deleting cannot take a conversation away from
-- the other. A new message after that starts the chat again for both, with
-- only the new messages showing for the person who deleted it.
--
-- One row per person per chat: when they last deleted it. Nothing is
-- removed from chat_messages.
CREATE TABLE IF NOT EXISTS chat_cleared (
    user_id    INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    other_id   INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    cleared_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (user_id, other_id)
);
