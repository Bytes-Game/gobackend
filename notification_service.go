package main

import (
	"encoding/json"
	"fmt"
	"log"
	"time"
)

// SendFollowNotification creates and sends a notification when a user follows another.
// It checks if the recipient is online. If so, it sends the notification directly.
// If the recipient is offline, it stores the notification in our mock Redis.
func SendFollowNotification(payload FollowEventPayload) {
	// The user being followed is the one who should receive the notification.
	recipientUsername := payload.FollowingUsername

	notification := Notification{
		Type:      "follow",
		Message:   fmt.Sprintf("%s started following you.", payload.FollowerUsername),
		Timestamp: time.Now().UTC().Format(time.RFC3339),
	}

	deliverNotification(recipientUsername, notification)
}

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

// SendChallengeNotification notifies friends about a new challenge.
func SendChallengeNotification(creatorUsername, challengeTitle string, visibleTo []string) {
	notification := Notification{
		Type:      "challenge",
		Message:   fmt.Sprintf("%s created a new challenge: \"%s\"", creatorUsername, challengeTitle),
		Timestamp: time.Now().UTC().Format(time.RFC3339),
	}

	if len(visibleTo) > 0 {
		// Notify specific friends.
		for _, uidStr := range visibleTo {
			user, found := GetUserByID(uidStr)
			if found {
				deliverNotification(user.Username, notification)
			}
		}
	} else {
		// Notify all followers of the creator.
		creator, found := GetUserByUsername(creatorUsername)
		if !found {
			return
		}
		// Get all users who follow the creator
		allUsers := GetAllUsers()
		for _, u := range allUsers {
			for _, fid := range u.FollowingList {
				if fid == creator.ID {
					deliverNotification(u.Username, notification)
					break
				}
			}
		}
	}
}

// SendChallengeAcceptedNotification notifies the challenger that someone accepted.
func SendChallengeAcceptedNotification(responderUsername, challengerUsername, challengeTitle string) {
	notification := Notification{
		Type:      "challenge_accepted",
		Message:   fmt.Sprintf("%s accepted your challenge: \"%s\"", responderUsername, challengeTitle),
		Timestamp: time.Now().UTC().Format(time.RFC3339),
	}
	deliverNotification(challengerUsername, notification)
}

// deliverNotification is a helper that sends a notification to a user
// (directly if online, stored for later if offline).
func deliverNotification(recipientUsername string, notification Notification) {
	conn, isOnline := IsUserOnline(recipientUsername)

	if isOnline && conn != nil {
		log.Printf("User %s is ONLINE. Sending notification directly.", recipientUsername)
		notificationJSON, _ := json.Marshal(notification)
		if err := conn.WriteMessage(1, notificationJSON); err != nil {
			log.Printf("Error sending notification to %s: %v", recipientUsername, err)
		}
	} else {
		log.Printf("User %s is OFFLINE. Storing notification.", recipientUsername)
		StoreNotificationInRedis(recipientUsername, notification)
	}
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

	conn, isOnline := IsUserOnline(username)
	if !isOnline {
		log.Printf("Cannot send stored notifications because user %s is not online.", username)
		return
	}

	for _, notification := range notifications {
		notificationJSON, _ := json.Marshal(notification)
		if err := conn.WriteMessage(1, notificationJSON); err != nil {
			log.Printf("Error sending stored notification to %s: %v", username, err)
			// Decide if you want to stop or continue sending other notifications
		} else {
			log.Printf("Successfully sent stored notification to %s", username)
		}
	}

	// After sending all notifications, clear the stored notifications
	ClearStoredNotifications(username)
	log.Printf("Cleared stored notifications for %s.", username)
}
