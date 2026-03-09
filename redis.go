package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"time"

	"github.com/redis/go-redis/v9"
)

// rdb is the Valkey (Redis-compatible) client used for notification storage.
var rdb *redis.Client

// rctx is the default context used for all Valkey operations.
var rctx = context.Background()

// InitRedis connects to the Valkey instance.
// It reads the connection URL from VALKEY_URL (preferred) or REDIS_URL.
func InitRedis() {
	url := os.Getenv("VALKEY_URL")
	if url == "" {
		url = os.Getenv("REDIS_URL")
	}
	if url == "" {
		log.Fatal("VALKEY_URL (or REDIS_URL) environment variable is required")
	}

	opts, err := redis.ParseURL(url)
	if err != nil {
		log.Fatalf("Invalid Valkey/Redis URL: %v", err)
	}

	rdb = redis.NewClient(opts)

	ctxTimeout, cancel := context.WithTimeout(rctx, 5*time.Second)
	defer cancel()

	if err := rdb.Ping(ctxTimeout).Err(); err != nil {
		log.Fatalf("Failed to connect to Valkey: %v", err)
	}
	log.Println("Connected to Valkey")
}

// StoreNotificationInRedis appends a notification to the user's Valkey list.
// Notifications expire after 30 days so memory doesn't grow unbounded.
func StoreNotificationInRedis(username string, notification Notification) {
	data, err := json.Marshal(notification)
	if err != nil {
		log.Printf("Failed to marshal notification: %v", err)
		return
	}

	key := "notifications:" + username
	if err := rdb.RPush(rctx, key, data).Err(); err != nil {
		log.Printf("Failed to store notification for %s: %v", username, err)
		return
	}

	// Auto-expire after 30 days.
	rdb.Expire(rctx, key, 30*24*time.Hour)
	log.Printf("Stored notification for offline user %s in Valkey.", username)
}

// GetStoredNotifications retrieves all pending notifications for a user.
func GetStoredNotifications(username string) ([]Notification, bool) {
	key := "notifications:" + username
	data, err := rdb.LRange(rctx, key, 0, -1).Result()
	if err != nil || len(data) == 0 {
		return nil, false
	}

	var notifications []Notification
	for _, d := range data {
		var n Notification
		if json.Unmarshal([]byte(d), &n) == nil {
			notifications = append(notifications, n)
		}
	}
	return notifications, len(notifications) > 0
}

// ClearStoredNotifications removes all pending notifications for a user
// after they have been delivered via WebSocket.
func ClearStoredNotifications(username string) {
	key := "notifications:" + username
	rdb.Del(rctx, key)
	log.Printf("Cleared stored notifications for user %s.", username)
}
