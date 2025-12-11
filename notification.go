package main

import (
	"encoding/json"
	"log"
	"time"

	"github.com/gorilla/websocket"
)

type Notification struct {
	EventType string `json:"event_type"`
	Payload   struct {
		ButtonName string `json:"button_name"`
	} `json:"payload"`
	Timestamp time.Time `json:"timestamp"`
}

func HandleNotification(username string, notificationJSON []byte) {
	var notification Notification
	err := json.Unmarshal(notificationJSON, &notification)
	if err != nil {
		log.Println("Error unmarshalling notification:", err)
		return
	}

	clientsMu.Lock()
	conn, ok := clients[username]
	clientsMu.Unlock()

	if ok {
		log.Printf("Sending notification to user %s via WebSocket", username)
		err := conn.WriteMessage(websocket.TextMessage, notificationJSON)
		if err != nil {
			log.Printf("Error sending message to user %s: %v", username, err)
			// The connection might be dead, remove it.
			clientsMu.Lock()
			delete(clients, username)
			clientsMu.Unlock()
		}
	} else {
		log.Printf("User %s is offline. Storing notification.", username)
		// Here you would add logic to store the notification for later delivery
	}
}
