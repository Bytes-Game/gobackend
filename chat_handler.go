package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/gorilla/mux"
)

// SendMessagePayload is the JSON body for sending a chat message.
type SendMessagePayload struct {
	SenderID   string `json:"senderId"`
	ReceiverID string `json:"receiverId"`
	Message    string `json:"message"`
	ReplyToID  string `json:"replyToId,omitempty"`
}

// maxChatMessageLen bounds a single chat message. Generous for real
// conversation (~15 sentences), tight enough that a hostile client
// can't stuff megabytes into a TEXT column per request — the send
// endpoint previously had no cap at all.
const maxChatMessageLen = 4000

// SendMessageHandler handles POST /api/v1/chat/send
func SendMessageHandler(w http.ResponseWriter, r *http.Request) {
	var payload SendMessagePayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}
	// The sender is the authenticated user — you can't send as someone else.
	payload.SenderID = authUserID(r)

	// Rate limit: the "chat" bucket (1/s, burst 10) existed in
	// actionLimitTable all along but chat was the one mutating surface
	// that never consulted it.
	if !allowAction(payload.SenderID, "chat") {
		writeRateLimited(w, "chat")
		return
	}

	senderID, _ := strconv.Atoi(payload.SenderID)
	receiverID, _ := strconv.Atoi(payload.ReceiverID)
	if senderID == 0 || receiverID == 0 || payload.Message == "" {
		http.Error(w, "senderId, receiverId, and message are required", http.StatusBadRequest)
		return
	}
	if len(payload.Message) > maxChatMessageLen {
		http.Error(w, "message too long", http.StatusRequestEntityTooLarge)
		return
	}

	// Respect blocks in either direction — a block means no DMs, same
	// as every mainstream social product. Fail-open on query error
	// (chat availability beats block strictness on a transient DB blip).
	var blocked bool
	if db != nil {
		_ = db.QueryRow(`SELECT EXISTS(
			SELECT 1 FROM user_blocks
			 WHERE (blocker_id = $1 AND blocked_id = $2)
			    OR (blocker_id = $2 AND blocked_id = $1))`,
			senderID, receiverID).Scan(&blocked)
	}
	if blocked {
		http.Error(w, "cannot message this user", http.StatusForbidden)
		return
	}
	// They take messages only from people they follow.
	if messagesRefused(senderID, receiverID) {
		http.Error(w, "cannot message this user", http.StatusForbidden)
		return
	}

	var replyToID *int
	if payload.ReplyToID != "" {
		if rid, err := strconv.Atoi(payload.ReplyToID); err == nil && rid > 0 {
			replyToID = &rid
		}
	}

	msgID, err := SendChatMessage(senderID, receiverID, payload.Message, replyToID)
	if err != nil {
		log.Printf("SendChatMessage error: %v", err)
		http.Error(w, "Failed to send message", http.StatusInternalServerError)
		return
	}

	// Get sender info for the response and notification
	sender, _ := GetUserByID(payload.SenderID)
	receiver, _ := GetUserByID(payload.ReceiverID)

	msg := ChatMessage{
		ID:              strconv.Itoa(msgID),
		SenderID:        payload.SenderID,
		SenderUsername:   sender.Username,
		ReceiverID:      payload.ReceiverID,
		ReceiverUsername: receiver.Username,
		Message:         payload.Message,
		IsRead:          false,
		Status:          "sent",
		ReplyToID:       payload.ReplyToID,
		CreatedAt:       time.Now().UTC().Format(time.RFC3339),
	}

	// Send real-time via WebSocket if receiver is online
	go deliverChatMessage(receiver.Username, msg)
	// And a push to their phone (never to the notifications page).
	go pushChatMessage(msg)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(msg)
}

// GetMessagesHandler handles GET /api/v1/chat/messages/{userId}/{otherUserId}
func GetMessagesHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	// You may only read your own conversations.
	if _, ok := requirePathUser(w, r, vars["userId"]); !ok {
		return
	}
	userID, _ := strconv.Atoi(vars["userId"])
	otherID, _ := strconv.Atoi(vars["otherUserId"])

	limit := 50
	offset := 0
	if l := r.URL.Query().Get("limit"); l != "" {
		if v, err := strconv.Atoi(l); err == nil {
			limit = v
		}
	}
	if o := r.URL.Query().Get("offset"); o != "" {
		if v, err := strconv.Atoi(o); err == nil {
			offset = v
		}
	}

	messages := GetChatMessages(userID, otherID, limit, offset)
	if messages == nil {
		messages = []ChatMessage{}
	}

	// Opening the chat reads what they sent, and tells them so straight
	// away: their phone turns "Sent" into "Seen" without being reloaded.
	go markReadAndTell(otherID, userID)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(messages)
}

// GetConversationsHandler handles GET /api/v1/chat/conversations/{userId}
func GetConversationsHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	if _, ok := requirePathUser(w, r, vars["userId"]); !ok {
		return
	}
	userID, _ := strconv.Atoi(vars["userId"])

	conversations := GetConversations(userID)
	if conversations == nil {
		conversations = []Conversation{}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(conversations)
}

// MarkReadHandler handles POST /api/v1/chat/read
func MarkReadHandler(w http.ResponseWriter, r *http.Request) {
	var payload struct {
		SenderID   string `json:"senderId"`
		ReceiverID string `json:"receiverId"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "Invalid body", http.StatusBadRequest)
		return
	}
	// The receiver (the one marking messages read) is the authenticated user.
	payload.ReceiverID = authUserID(r)
	sID, _ := strconv.Atoi(payload.SenderID)
	rID, _ := strconv.Atoi(payload.ReceiverID)
	// Reject a non-numeric senderId with a clear 400 instead of silently
	// no-op'ing — matches SendMessageHandler / EditMessageHandler.
	if sID == 0 {
		http.Error(w, "senderId must be a valid integer", http.StatusBadRequest)
		return
	}
	markReadAndTell(sID, rID)
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, `{"ok":true}`)
}

// ClearChatHandler handles POST /api/v1/chat/clear body:{ otherUserId }.
//
// "Delete chat": the chat with that person goes off the list of the person
// asking, with every message up to now. Only for them — the other person
// keeps theirs, as in every chat app. A new message later starts it again,
// showing only what came after (migration 016).
func ClearChatHandler(w http.ResponseWriter, r *http.Request) {
	var payload struct {
		OtherUserID string `json:"otherUserId"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "Invalid body", http.StatusBadRequest)
		return
	}
	me, _ := strconv.Atoi(authUserID(r))
	other, _ := strconv.Atoi(payload.OtherUserID)
	if me == 0 || other == 0 || me == other {
		http.Error(w, "otherUserId required", http.StatusBadRequest)
		return
	}
	_, err := db.Exec(`
		INSERT INTO chat_cleared (user_id, other_id, cleared_at)
		VALUES ($1, $2, NOW())
		ON CONFLICT (user_id, other_id) DO UPDATE SET cleared_at = NOW()`, me, other)
	if queryFailed(fmt.Sprintf("deleting the chat with user %d for user %d", other, me),
		"the chat stays on their list", err) {
		http.Error(w, "could not delete the chat", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprint(w, `{"ok":true}`)
}

// EditMessageHandler handles POST /api/v1/chat/edit body:{ messageId, senderId, text }
func EditMessageHandler(w http.ResponseWriter, r *http.Request) {
	var payload struct {
		MessageID string `json:"messageId"`
		SenderID  string `json:"senderId"`
		Text      string `json:"text"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "Invalid body", http.StatusBadRequest)
		return
	}
	// Only the author may edit; the author is the authenticated user.
	payload.SenderID = authUserID(r)
	msgID, _ := strconv.Atoi(payload.MessageID)
	senderID, _ := strconv.Atoi(payload.SenderID)
	if msgID == 0 || senderID == 0 || payload.Text == "" {
		http.Error(w, "messageId, senderId, and text required", http.StatusBadRequest)
		return
	}
	if err := EditChatMessage(msgID, senderID, payload.Text); err != nil {
		http.Error(w, err.Error(), http.StatusForbidden)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprint(w, `{"ok":true}`)
}

// DeleteMessageHandler handles POST /api/v1/chat/delete body:{ messageId, senderId }
func DeleteMessageHandler(w http.ResponseWriter, r *http.Request) {
	var payload struct {
		MessageID string `json:"messageId"`
		SenderID  string `json:"senderId"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "Invalid body", http.StatusBadRequest)
		return
	}
	// Only the author may delete; the author is the authenticated user.
	payload.SenderID = authUserID(r)
	msgID, _ := strconv.Atoi(payload.MessageID)
	senderID, _ := strconv.Atoi(payload.SenderID)
	if msgID == 0 || senderID == 0 {
		http.Error(w, "messageId and senderId required", http.StatusBadRequest)
		return
	}
	if err := DeleteChatMessage(msgID, senderID); err != nil {
		http.Error(w, err.Error(), http.StatusForbidden)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprint(w, `{"ok":true}`)
}

// ForwardMessageHandler forwards a message to another user.
// POST /api/v1/chat/forward body:{ messageId, senderId, receiverId }
func ForwardMessageHandler(w http.ResponseWriter, r *http.Request) {
	var payload struct {
		MessageID  string `json:"messageId"`
		SenderID   string `json:"senderId"`
		ReceiverID string `json:"receiverId"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "Invalid body", http.StatusBadRequest)
		return
	}
	// The forwarder is the authenticated user.
	payload.SenderID = authUserID(r)
	msgID, _ := strconv.Atoi(payload.MessageID)
	senderID, _ := strconv.Atoi(payload.SenderID)
	receiverID, _ := strconv.Atoi(payload.ReceiverID)
	if msgID == 0 || senderID == 0 || receiverID == 0 {
		http.Error(w, "messageId, senderId, and receiverId required", http.StatusBadRequest)
		return
	}

	// The original, from a chat the forwarder is in. Anybody could
	// forward ANY message before, from any chat, by guessing its number.
	// A deleted message is not forwarded either: its text is just "This
	// message was deleted".
	var originalText string
	err := db.QueryRow(`
		SELECT message FROM chat_messages
		 WHERE id = $1 AND (sender_id = $2 OR receiver_id = $2)
		   AND is_deleted IS NOT TRUE`, msgID, senderID).Scan(&originalText)
	if err != nil {
		queryFailed(fmt.Sprintf("finding message %d for user %d to forward", msgID, senderID),
			"answering not found", err)
		http.Error(w, "Message not found", http.StatusNotFound)
		return
	}
	// A block means no messages, forwarded ones included.
	var blocked bool
	err = db.QueryRow(`SELECT EXISTS(
		SELECT 1 FROM user_blocks
		 WHERE (blocker_id = $1 AND blocked_id = $2)
		    OR (blocker_id = $2 AND blocked_id = $1))`,
		senderID, receiverID).Scan(&blocked)
	if queryFailed(fmt.Sprintf("checking blocks between users %d and %d", senderID, receiverID),
		"forwarding anyway, as sending does", err) {
		blocked = false
	}
	if blocked || messagesRefused(senderID, receiverID) {
		http.Error(w, "cannot message this user", http.StatusForbidden)
		return
	}

	// Send as a new message (no reply reference for forwards)
	newMsgID, err := SendChatMessage(senderID, receiverID, originalText, nil)
	if err != nil {
		http.Error(w, "Failed to forward", http.StatusInternalServerError)
		return
	}

	sender, _ := GetUserByID(payload.SenderID)
	receiver, _ := GetUserByID(payload.ReceiverID)

	msg := ChatMessage{
		ID:              strconv.Itoa(newMsgID),
		SenderID:        payload.SenderID,
		SenderUsername:   sender.Username,
		ReceiverID:      payload.ReceiverID,
		ReceiverUsername: receiver.Username,
		Message:         originalText,
		Status:          "sent",
		CreatedAt:       time.Now().UTC().Format(time.RFC3339),
	}

	go deliverChatMessage(receiver.Username, msg)
	go pushChatMessage(msg)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(msg)
}

// SaveChallengeHandler toggles save on a challenge.
// POST /api/v1/save body:{ userId, challengeId }
func SaveChallengeHandler(w http.ResponseWriter, r *http.Request) {
	var payload struct {
		UserID      string `json:"userId"`
		ChallengeID string `json:"challengeId"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "Invalid body", http.StatusBadRequest)
		return
	}
	// The saver is the authenticated user.
	payload.UserID = authUserID(r)
	saved, err := ToggleSaveChallenge(payload.UserID, payload.ChallengeID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"saved":       saved,
		"challengeId": payload.ChallengeID,
	})
}

// GetSavedChallengesHandler returns saved challenges for a user.
// GET /api/v1/saved/{userId}
func GetSavedChallengesHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	userID, ok := requirePathUser(w, r, vars["userId"])
	if !ok {
		return
	}

	challenges := GetSavedChallenges(userID)
	if challenges == nil {
		challenges = []Challenge{}
	}
	populateTopResponsesChallenges(challenges)
	markViewerStateChallenges(userID, challenges)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(challenges)
}

// deliverChatMessage sends a chat message to a user via WebSocket —
// locally, or through the cross-replica relay when the recipient is
// connected to a different replica. Offline recipients simply don't get
// a push (the message is already durable in chat_messages).
func deliverChatMessage(recipientUsername string, msg ChatMessage) {
	// Wrap the message in a notification-like envelope with type "chat"
	envelope := map[string]interface{}{
		"type":             "chat",
		"message":          msg.Message,
		"senderId":         msg.SenderID,
		"senderUsername":   msg.SenderUsername,
		"receiverId":       msg.ReceiverID,
		"receiverUsername": msg.ReceiverUsername,
		"messageId":        msg.ID,
		"timestamp":        msg.CreatedAt,
	}

	data, err := json.Marshal(envelope)
	if err != nil {
		return
	}

	if !wsDeliver(recipientUsername, data) {
		if IsUserOnline(recipientUsername) {
			log.Printf("Failed to deliver chat message to %s", recipientUsername)
		}
		return
	}
	// It reached their phone: the sender's "Sent" becomes "Delivered".
	if id, err := strconv.Atoi(msg.ID); err == nil {
		markDeliveredAndTell(id, msg.SenderUsername, msg.ReceiverID)
	}
}
