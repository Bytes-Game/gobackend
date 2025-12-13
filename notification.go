package main

import (
	"log"

	"github.com/gorilla/websocket"
)

// HandleNotification finds the WebSocket connection for a given user and relays the
// raw notification JSON to them.
func HandleNotification(username string, notificationJSON []byte) {
	// Lock the mutex to safely access the clients map.
	clientsMu.Lock()
	// Look up the WebSocket connection for the target user.
	conn, ok := clients[username]
	// Unlock the mutex as soon as we're done with the map.
	clientsMu.Unlock()

	// Check if a connection was found.
	if ok {
		// A connection exists, so the user is online.
		log.Printf("Relaying notification to user %s via WebSocket", username)

		// Send the raw JSON message over the WebSocket.
		err := conn.WriteMessage(websocket.TextMessage, notificationJSON)
		if err != nil {
			// If there's an error writing the message, the connection is likely dead.
			log.Printf("Error sending message to user %s: %v", username, err)

			// Clean up the dead connection.
			clientsMu.Lock()
			delete(clients, username)
			clientsMu.Unlock()
		}
	} else {
		// No connection was found, so the user is offline.
		log.Printf("User %s is offline. Storing notification.", username)
		// In a real application, you would add logic here to save the
		// notification to a database for later delivery.
	}
}
