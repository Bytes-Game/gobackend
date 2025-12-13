package main

import (
	"log"
	"github.com/gorilla/websocket"
)

// HandleNotification determines if a user is online for real-time delivery
// or offline for storage using the in-memory store.
func HandleNotification(username string, notificationJSON []byte) {

	// First, check if the user is currently connected via WebSocket.
	clientsMu.Lock()
	conn, isOnline := clients[username]
	clientsMu.Unlock()

	if isOnline {
		// --- REAL-TIME PATH ---
		// The user is online, so send the notification directly over the WebSocket.
		log.Printf("User %s is ONLINE. Relaying notification via WebSocket.", username)
		
		err := conn.WriteMessage(websocket.TextMessage, notificationJSON)
		
		if err != nil {
			// The connection was likely closed between the check and the send.
			log.Printf("Error sending message to user %s: %v. Storing for later.", username, err)
			
			// Clean up the dead connection.
			clientsMu.Lock()
			delete(clients, username)
			clientsMu.Unlock()

			// Since the send failed, fall back to the offline storage logic.
			StoreNotificationInRedis(username, notificationJSON)
		}

	} else {
		// --- OFFLINE PATH ---
		// The user is not connected. Use the function from redis.go to store it.
		StoreNotificationInRedis(username, notificationJSON)
	}
}
