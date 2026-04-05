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
}

// SendMessageHandler handles POST /api/v1/chat/send
func SendMessageHandler(w http.ResponseWriter, r *http.Request) {
	var payload SendMessagePayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	senderID, _ := strconv.Atoi(payload.SenderID)
	receiverID, _ := strconv.Atoi(payload.ReceiverID)
	if senderID == 0 || receiverID == 0 || payload.Message == "" {
		http.Error(w, "senderId, receiverId, and message are required", http.StatusBadRequest)
		return
	}

	msgID, err := SendChatMessage(senderID, receiverID, payload.Message)
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
		CreatedAt:       time.Now().UTC().Format(time.RFC3339),
	}

	// Send real-time via WebSocket if receiver is online
	go deliverChatMessage(receiver.Username, msg)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(msg)
}

// GetMessagesHandler handles GET /api/v1/chat/messages/{userId}/{otherUserId}
func GetMessagesHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
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

	// Mark messages from otherUser as read
	go MarkMessagesRead(otherID, userID)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(messages)
}

// GetConversationsHandler handles GET /api/v1/chat/conversations/{userId}
func GetConversationsHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
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
	sID, _ := strconv.Atoi(payload.SenderID)
	rID, _ := strconv.Atoi(payload.ReceiverID)
	MarkMessagesRead(sID, rID)
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, `{"ok":true}`)
}

// deliverChatMessage sends a chat message to a user via WebSocket.
func deliverChatMessage(recipientUsername string, msg ChatMessage) {
	conn, isOnline := IsUserOnline(recipientUsername)
	if !isOnline || conn == nil {
		return
	}

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

	if err := conn.WriteMessage(1, data); err != nil {
		log.Printf("Failed to deliver chat message to %s: %v", recipientUsername, err)
	}
}
