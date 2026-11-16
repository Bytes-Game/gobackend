package main

// chat_mute.go — muting a chat, and the number on the Messages button.
//
// Muting a chat stops the notifications for that person's messages: no
// push on the phone, and no pop-up in the app (the live message says it is
// from a muted chat). The messages still arrive and the chat still shows
// them, unread in bold, the way every chat app does it. Calls still ring:
// muting is for messages.
//
//	POST /api/v1/chat/mute {"otherUserId", "muted": true|false}
//	    200 {"muted": bool}
//
// The Messages button shows how many PEOPLE have sent you messages you
// have not read yet, not how many messages. A muted chat is not counted.
//
//	GET /api/v1/chat/unread
//	    200 {"people": n}

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"

	"github.com/lib/pq"
)

// chatMuted is whether [userID] muted their chat with [otherID]. A database
// that cannot answer says no, and says so in the log: a notification too
// many is better than a message nobody hears about.
func chatMuted(userID, otherID int) bool {
	var muted bool
	err := db.QueryRow(`SELECT EXISTS (SELECT 1 FROM chat_mutes WHERE user_id = $1 AND other_id = $2)`,
		userID, otherID).Scan(&muted)
	if queryFailed(fmt.Sprintf("whether user %d muted their chat with user %d", userID, otherID),
		"treating it as not muted; the notification goes", err) {
		return false
	}
	return muted
}

// msgFromMutedChat is whether the receiver of [msg] muted their chat with
// its sender.
func msgFromMutedChat(msg ChatMessage) bool {
	rid, err1 := strconv.Atoi(msg.ReceiverID)
	sid, err2 := strconv.Atoi(msg.SenderID)
	if err1 != nil || err2 != nil {
		return false
	}
	return chatMuted(rid, sid)
}

// mutedChatsOf is everybody [userID] has muted, by account id.
func mutedChatsOf(userID int) map[int]bool {
	muted := map[int]bool{}
	rows, err := db.Query(`SELECT other_id FROM chat_mutes WHERE user_id = $1`, userID)
	if queryFailed(fmt.Sprintf("which chats user %d muted", userID),
		"showing every chat as not muted", err) {
		return muted
	}
	defer rows.Close()
	failed := 0
	for rows.Next() {
		var other int
		if scanFailed("which chats user "+strconv.Itoa(userID)+" muted (that chat shows as not muted)",
			rows.Scan(&other), &failed) {
			continue
		}
		muted[other] = true
	}
	return muted
}

// MuteChatHandler — POST /api/v1/chat/mute {"otherUserId", "muted"}: mutes
// or unmutes the caller's chat with somebody. Only ever the caller's own:
// who is asking comes from their sign-in.
func MuteChatHandler(w http.ResponseWriter, r *http.Request) {
	var body struct {
		OtherUserID string `json:"otherUserId"`
		Muted       *bool  `json:"muted"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 4<<10)).Decode(&body); err != nil || body.Muted == nil {
		http.Error(w, `{"error":"otherUserId and muted are needed"}`, http.StatusBadRequest)
		return
	}
	me, _ := strconv.Atoi(authUserID(r))
	other, _ := strconv.Atoi(body.OtherUserID)
	if me == 0 || other == 0 || me == other {
		http.Error(w, `{"error":"otherUserId and muted are needed"}`, http.StatusBadRequest)
		return
	}
	var err error
	if *body.Muted {
		_, err = db.Exec(`INSERT INTO chat_mutes (user_id, other_id) VALUES ($1, $2)
		                  ON CONFLICT (user_id, other_id) DO NOTHING`, me, other)
	} else {
		_, err = db.Exec(`DELETE FROM chat_mutes WHERE user_id = $1 AND other_id = $2`, me, other)
	}
	var pqErr *pq.Error
	if errors.As(err, &pqErr) && pqErr.Code == "23503" {
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "There's no such account."})
		return
	}
	if err != nil {
		serverFault(w, fmt.Sprintf("setting user %d's chat with user %d to muted=%v", me, other, *body.Muted), err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"muted": *body.Muted})
}

// unreadChatPeople is how many people have sent [userID] messages they
// have not read, leaving out muted chats. It counts what the chat list
// shows as unread: not before a chat was deleted, and not from an account
// that is waiting to be deleted or is suspended (GetConversations).
func unreadChatPeople(userID int) (int, error) {
	var n int
	err := db.QueryRow(`
		SELECT COUNT(DISTINCT m.sender_id)
		  FROM chat_messages m
		  JOIN users u ON u.id = m.sender_id
		 WHERE m.receiver_id = $1
		   AND m.is_read = FALSE
		   AND u.deletion_requested_at IS NULL AND u.suspended_at IS NULL
		   AND m.created_at > COALESCE(
		         (SELECT c.cleared_at FROM chat_cleared c
		           WHERE c.user_id = $1 AND c.other_id = m.sender_id),
		         '-infinity'::timestamptz)
		   AND NOT EXISTS (SELECT 1 FROM chat_mutes cm
		                    WHERE cm.user_id = $1 AND cm.other_id = m.sender_id)`,
		userID).Scan(&n)
	return n, err
}

// UnreadChatsHandler — GET /api/v1/chat/unread: the number on the Messages
// button, for the caller.
func UnreadChatsHandler(w http.ResponseWriter, r *http.Request) {
	me, _ := strconv.Atoi(authUserID(r))
	if me == 0 {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	n, err := unreadChatPeople(me)
	if err != nil {
		serverFault(w, fmt.Sprintf("counting who user %d has unread messages from", me), err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"people": n})
}
