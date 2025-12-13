package main

import (
	"log"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

var clients = make(map[string]*websocket.Conn)
var clientsMu sync.Mutex

func WebSocketHandler(w http.ResponseWriter, r *http.Request) {
	params := r.URL.Query()
	username := params.Get("username")
	if username == "" {
		http.Error(w, "Username is required", http.StatusBadRequest)
		return
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("Error upgrading to WebSocket:", err)
		return
	}

	clientsMu.Lock()
	clients[username] = conn
	clientsMu.Unlock()

	log.Printf("User %s connected via WebSocket", username)

	// --- INTEGRATION POINT ---
	// After connecting, immediately check for and send any stored notifications for this user.
	// We run this in a new goroutine so it doesn't block the main read loop.
	go SendStoredNotificationsFromRedis(conn, username)

	// Keep the connection open and listen for messages (for disconnect detection).
	for {
		_, _, err := conn.ReadMessage()
		if err != nil {
			log.Printf("User %s disconnected from WebSocket", username)
			clientsMu.Lock()
			delete(clients, username)
			clientsMu.Unlock()
			break // Exit the loop to close the connection handler.
		}
	}
}
