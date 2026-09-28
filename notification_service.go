package main

import (
	"encoding/json"
	"fmt"
	"log"
	"time"
)

// SendLikeNotification sends a notification when someone likes a post.
func SendLikeNotification(likerUsername, postAuthorUsername, caption string) {
	// Truncate caption for display
	displayCaption := caption
	if len(displayCaption) > 40 {
		displayCaption = displayCaption[:40] + "..."
	}

	notification := Notification{
		Type:      "like",
		Message:   fmt.Sprintf("%s liked your post: \"%s\"", likerUsername, displayCaption),
		Timestamp: time.Now().UTC().Format(time.RFC3339),
	}

	deliverNotification(postAuthorUsername, notification)
}

// SendCommentNotification sends a notification when someone comments on a post.
func SendCommentNotification(commenterUsername, postAuthorUsername, commentText, caption string) {
	// Truncate for display
	displayComment := commentText
	if len(displayComment) > 50 {
		displayComment = displayComment[:50] + "..."
	}

	notification := Notification{
		Type:      "comment",
		Message:   fmt.Sprintf("%s commented on your post: \"%s\"", commenterUsername, displayComment),
		Timestamp: time.Now().UTC().Format(time.RFC3339),
	}

	deliverNotification(postAuthorUsername, notification)
}

// deliverNotification is a helper that sends a notification to a user
// (directly if online — on any replica — stored for later if offline).
func deliverNotification(recipientUsername string, notification Notification) {
	notificationJSON, _ := json.Marshal(notification)
	if wsDeliver(recipientUsername, notificationJSON) {
		log.Printf("User %s is ONLINE. Sent notification.", recipientUsername)
		return
	}
	log.Printf("User %s is OFFLINE. Storing notification.", recipientUsername)
	StoreNotificationInRedis(recipientUsername, notification)
}

// SendStoredNotifications is called when a user connects via WebSocket
// It retrieves any stored notifications from our mock Redis and sends them to the user.
func SendStoredNotifications(username string) {
	notifications, found := GetStoredNotifications(username)

	if !found || len(notifications) == 0 {
		log.Printf("No stored notifications found for %s.", username)
		return
	}

	log.Printf("Found %d stored notifications for %s. Sending them now.", len(notifications), username)

	// Called from the connect path of THIS replica, so local send is the
	// right primitive (no relay — the user just connected here).
	for _, notification := range notifications {
		notificationJSON, _ := json.Marshal(notification)
		if !wsSendLocal(username, notificationJSON) {
			log.Printf("Error sending stored notification to %s (disconnected mid-flush)", username)
		} else {
			log.Printf("Successfully sent stored notification to %s", username)
		}
	}

	// After sending all notifications, clear the stored notifications
	ClearStoredNotifications(username)
	log.Printf("Cleared stored notifications for %s.", username)
}
