package main

import (
	"log"
	"sync"
)

// notificationStore will act as our in-memory database, simulating Redis.
// The key is the username, and the value is a slice of their notifications.
var notificationStore = make(map[string][]Notification)
var storeMu sync.Mutex // A mutex to make our in-memory store safe for concurrent use.
	
// InitRedis is renamed to InitNotificationStore to better reflect its purpose.
// It now simply logs that the in-memory system is ready.
func InitRedis() {
	log.Println("In-memory notification store initialized (simulating Redis).")
}

// StoreNotificationInRedis now stores the notification struct in our local map.
func StoreNotificationInRedis(username string, notification Notification) {
	storeMu.Lock()
	defer storeMu.Unlock()
	// Append the new notification to the user's list.
	notificationStore[username]= append(notificationStore[username], notification)
	log.Printf("Stored notification for offline user %s in memory.", username)
}

// GetStoredNotifications retrieves all stored notifications for a given user.
func GetStoredNotifications(username string) ([]Notification, bool) {
	storeMu.Lock()
	defer storeMu.Unlock()

	notifications, found := notificationStore[username]
	return notifications, found
}


// ClearstoredNotifications removes all notifications for a user after they have been delivered.
func ClearStoredNotifications(username string){
	storeMu.Lock()
	defer storeMu.Unlock()

	delete(notificationStore, username)
	log.Printf("Cleared stored notifications for user %s.", username)
}
