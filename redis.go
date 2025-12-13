package main

import (
	"log"
	"sync"

	"github.com/gorilla/websocket"
)

// notificationStore will act as our in-memory database, simulating Redis.
// The key is the username, and the value is a slice of their notifications.
var notificationStore = make(map[string][][]byte)
var storeMu sync.Mutex // A mutex to make our in-memory store safe for concurrent use.

// InitRedis is renamed to InitNotificationStore to better reflect its purpose.
// It now simply logs that the in-memory system is ready.
func InitRedis() {
	log.Println("In-memory notification store initialized (simulating Redis).")
}

// StoreNotificationInRedis now stores the notification in our local map.
func StoreNotificationInRedis(username string, notificationJSON []byte) {
	storeMu.Lock()
	defer storeMu.Unlock()

	// Append the new notification to the user's list.
	notificationStore[username] = append(notificationStore[username], notificationJSON)
	log.Printf("Stored notification for offline user %s in memory.", username)
}

// SendStoredNotificationsFromRedis now reads from our local map.
func SendStoredNotificationsFromRedis(conn *websocket.Conn, username string) {
	storeMu.Lock()

	// Check if there are any notifications for this user.
	notifications, found := notificationStore[username]
	if !found || len(notifications) == 0 {
		storeMu.Unlock()
		return // No notifications, so we can stop.
	}

	// We have notifications. Delete them from the map so they aren't sent again.
	delete(notificationStore, username)
	storeMu.Unlock()

	log.Printf("Found %d stored notifications for user %s. Sending now.", len(notifications), username)
	for _, notification := range notifications {
		if err := conn.WriteMessage(websocket.TextMessage, []byte(notification)); err != nil {
			log.Printf("Error sending stored notification to %s: %v", username, err)
			// In a real app, you might want to re-queue the unsent messages.
			// For now, we will log the error and stop.
			return
		}
	}
}
