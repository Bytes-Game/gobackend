package main

import (
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// Define constants for the heartbeat mechanism.
const (
	// Time allowed to write a message to the peer.
	writeWait = 10 * time.Second

	// Time allowed to read the next pong message from the peer.
	pongWait = 60 * time.Second

	// Send pings to peer with this period. Must be less than pongWait.
	pingPeriod = (pongWait * 9) / 10
)

// upgrader holds the websocket connection configuration.
var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin:     func(r *http.Request) bool { return true },
}

// clients now maps usernames (string) to their websocket connection.
var clients = make(map[string]*websocket.Conn)

// clientsMu is a mutex to protect the clients map from concurrent access.
var clientsMu sync.Mutex

// WebsocketHandler upgrades the HTTP connection and manages the client lifecycle.
func WebsocketHandler(w http.ResponseWriter, r *http.Request) {
	// Extract username from the URL path
	pathParts := strings.Split(r.URL.Path, "/")
	if len(pathParts) < 3 || pathParts[2] == "" {
		log.Println("Username is missing from WebSocket path")
		http.Error(w, "Username required", http.StatusBadRequest)
		return
	}
	username := pathParts[2]

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("Failed to upgrade connection for %s: %v", username, err)
		return
	}

	// Use a defer statement for robust cleanup. This will always be called
	// when the function exits (either by returning or after the read loop breaks).
	defer func() {
		clientsMu.Lock()
		delete(clients, username)
		clientsMu.Unlock()
		conn.Close()
		log.Printf("WebSocket for %s disconnected and cleaned up", username)
	}()

	// Register the new client.
	clientsMu.Lock()
	clients[username] = conn
	clientsMu.Unlock()
	log.Printf("WebSocket for %s connected", username)

	// --- Full Heartbeat Implementation ---

	// Set the pong handler to reset the read deadline, keeping the connection alive.
	conn.SetReadDeadline(time.Now().Add(pongWait))
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	// This goroutine is responsible for sending pings to the client.
	go func() {
		ticker := time.NewTicker(pingPeriod)
		defer ticker.Stop()
		for {
			<-ticker.C
			// Set a write deadline for the ping message.
			conn.SetWriteDeadline(time.Now().Add(writeWait))
			// Send a ping. If it fails, the connection is considered dead.
			// The main read loop below will detect this and exit.
			if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				log.Printf("Ping failed for %s, connection will be closed: %v", username, err)
				return // Stop the pinger.
			}
		}
	}()

	// This is the main read loop for the connection.
	// It blocks waiting for messages from the client.
	for {
		_, message, err := conn.ReadMessage()
		if err != nil {
			// If there's an error, we assume the connection is closed.
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("WebSocket read error for %s: %v", username, err)
			}
			break // Exit the read loop, which will trigger the deferred cleanup.
		}
		log.Printf("Received message from %s: %s", username, message)
	}
}
