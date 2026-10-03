package main

// chat_push.go — a new message, or a missed call, buzzes the phone.
//
// Messages never go to the notifications page. That page is for things
// that happen around your videos and battles; a message is a push on the
// phone, the way every chat app does it: who wrote, what they said, and a
// tap opens the chat. One line per chat in the phone's list — a newer
// message replaces the older one there, and the title counts how many are
// waiting ("maya (3 messages)").
//
// These are sent straight away, not through the push queue in
// notifications.go: the queue's daily cap and quiet hours are for the
// app's own nudges ("your battle ends soon"), never for a person writing
// to you.
//
// The push only reaches phones that have registered for it (the app does
// that once Firebase is set up — see the app's push_service.dart). The
// app ignores one that arrives while it is open: the message is already
// on screen.

import (
	"fmt"
	"log"
	"strconv"
)

const (
	TriggerChatMessage TriggerKind = "chat_message"
	TriggerMissedCall  TriggerKind = "missed_call"

	// pushChannelMessages is the Android notification channel the app
	// creates for chats and calls: it pops up on screen and makes a sound.
	pushChannelMessages = "messages"

	// pushBodyLimit keeps a message's text under both phones' limits.
	pushBodyLimit = 180
)

// pushNow sends one push to every phone the person has, now. Says so in
// the log when it reached none of them; a phone that has gone for good
// (app deleted) is forgotten so it is not tried again. Returns how many
// phones it reached.
func pushNow(userID string, p OutboxRow) int {
	tokens := activeTokensForUser(userID)
	if len(tokens) == 0 {
		return 0
	}
	p.UserID = userID
	sent := 0
	reason := ""
	for _, r := range getCurrentSender().Send(p, tokens) {
		if r.OK {
			sent++
			continue
		}
		if r.Dead {
			deactivateDeviceToken(r.Token)
		}
		if reason == "" {
			reason = r.Reason
		}
	}
	if sent == 0 {
		log.Printf("push %s to user %s reached none of their %d phone(s): %s",
			p.TriggerKind, userID, len(tokens), reason)
	}
	return sent
}

// pushChatMessage tells the receiver's phone about a new message — unless
// they turned "Messages and calls" off in their notification settings.
func pushChatMessage(msg ChatMessage) {
	sid, err1 := strconv.Atoi(msg.SenderID)
	rid, err2 := strconv.Atoi(msg.ReceiverID)
	if err1 != nil || err2 != nil {
		return
	}
	if !loadNotificationPrefs(msg.ReceiverID).allowedByPrefs(TriggerChatMessage) {
		return
	}
	title := msg.SenderUsername
	var waiting int
	err := db.QueryRow(`
		SELECT COUNT(*) FROM chat_messages
		 WHERE sender_id = $1 AND receiver_id = $2 AND is_read IS NOT TRUE`,
		sid, rid).Scan(&waiting)
	if !queryFailed(fmt.Sprintf("counting messages from user %d waiting for user %d", sid, rid),
		"the push goes without the count", err) && waiting > 1 {
		title = fmt.Sprintf("%s (%d messages)", msg.SenderUsername, waiting)
	}
	pushNow(msg.ReceiverID, OutboxRow{
		TriggerKind: TriggerChatMessage,
		Title:       title,
		Body:        truncateText(chatPreview(msg.Kind, msg.Message), pushBodyLimit),
		Tag:         "chat_" + msg.SenderID,
		Channel:     pushChannelMessages,
		AppDraws:    true,
		Data: map[string]string{
			"type":           "chat",
			"senderId":       msg.SenderID,
			"senderUsername": msg.SenderUsername,
			"messageId":      msg.ID,
			"kind":           msg.Kind,
		},
	})
}

// pushMissedCall tells somebody's phone they missed a call from
// [callerID]. A tap opens the chat, where the call buttons are.
func pushMissedCall(toID, callerID, callerName string, video bool) {
	if !loadNotificationPrefs(toID).allowedByPrefs(TriggerMissedCall) {
		return
	}
	title := "Missed call"
	if video {
		title = "Missed video call"
	}
	pushNow(toID, OutboxRow{
		TriggerKind: TriggerMissedCall,
		Title:       title,
		Body:        callerName + " tried to call you",
		Tag:         "call_" + callerID,
		Channel:     pushChannelMessages,
		AppDraws:    true,
		Data: map[string]string{
			"type":           "missed_call",
			"senderId":       callerID,
			"senderUsername": callerName,
		},
	})
}
