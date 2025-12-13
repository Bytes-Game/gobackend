package main

import (
	"log"

	"github.com/gorilla/websocket"
)

// HandleNotification finds the WebSocket connection for a given user and relays the
// raw notification JSON to them. If the user is offline, it stores the notification in Redis.
func HandleNotification(username string, notificationJSON []byte) {
	clientsMu.Lock()
	conn, ok := clients[username]
	clientsMu.Unlock()

	if ok {
		// User is online, send the notification directly.
		log.Printf("Relaying notification to user %s via WebSocket", username)
		err := conn.WriteMessage(websocket.TextMessage, notificationJSON)
		if err != nil {
			log.Printf("Error sending message to user %s: %v", username, err)
			// Clean up the dead connection.
			clientsMu.Lock()
			delete(clients, username)
			clientsMu.Unlock()
		}
	} else {
		// User is offline, store the notification in Redis.
		log.Printf("User %s is offline. Storing notification in Redis.", username)
		StoreNotificationInRedis(username, notificationJSON)
	}
}
