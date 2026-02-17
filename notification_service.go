package main

import (
	"encoding/json"
	"fmt"
	"log"
)

// SendFollowNotification creates and sends a notification when a user follows another.
// It checks if the recipient is online. If so, it sends the notification directly.
// If the recipient is offline, it stores the notification in our mock Redis.
func SendFollowNotification(payload FollowEventPayload) {
	// The user being followed is the one who should receive the notification.
	recipientUsername := payload.FollowingUsername

	notification := Notification{
		Type:    "follow",
		Message: fmt.Sprintf("%s started following you.", payload.FollowerUsername),
	}

	// Check if the user is connected via WebSocket.
	conn, isOnline := IsUserOnline(recipientUsername)

	if isOnline && conn != nil {
		log.Printf("User %s is ONLINE. Sending notification directly.", recipientUsername)
		// Send the notification over the WebSocket.
		notificationJSON, _ := json.Marshal(notification)
		if err := conn.WriteMessage(1, notificationJSON); err != nil {
			log.Printf("Error sending notification to %s: %v", recipientUsername, err)
		}
	} else {
		log.Printf("User %s is OFFLINE. Storing notification.", recipientUsername)
		// Store the notification for later delivery.
		StoreNotificationInRedis(recipientUsername, notification)
	}
}

// SendStoredNotifications is called when a user connects via WebSocket.
// It retrieves any stored notifications from our mock Redis and sends them to the user.
func SendStoredNotifications(username string) {
	notifications, found := GetStoredNotifications(username)

	if !found || len(notifications) == 0 {
		log.Printf("No stored notifications found for %s.", username)
		return
	}

	log.Printf("Found %d stored notifications for %s. Sending them now.", len(notifications), username)

	conn, isOnline := IsUserOnline(username)
	if !isOnline {
		log.Printf("Cannot send stored notifications because user %s is not online.", username)
		return
	}

	for _, notification := range notifications {
		notificationJSON, _ := json.Marshal(notification)
		if err := conn.WriteMessage(1, notificationJSON); err != nil {
			log.Printf("Error sending stored notification to %s: %v", username, err)
			// Decide if you want to stop or continue on error
		} else {
			log.Printf("Successfully sent stored notification to %s", username)
		}
	}

	// After sending, clear the stored notifications.
	ClearStoredNotifications(username)
	log.Printf("Cleared stored notifications for %s.", username)
}
