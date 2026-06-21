package main

import (
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/mux"
	"github.com/gorilla/websocket"
)

const (
	writeWait  = 10 * time.Second
	pongWait   = 60 * time.Second
	pingPeriod = (pongWait * 9) / 10
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin:     func(r *http.Request) bool { return true },
}

var clients = make(map[string]*websocket.Conn)
var clientsMu sync.Mutex

// WebsocketHandler upgrades the HTTP connection and manages the client lifecycle.
func WebsocketHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	username := vars["username"]
	if username == "" {
		log.Println("Username is missing from WebSocket path")
		http.Error(w, "Username required", http.StatusBadRequest)
		return
	}

	// Authenticate the socket. Browsers can't set an Authorization header on a
	// WebSocket handshake, so the client passes its session token as a query
	// param. The token's username must match the path — otherwise a caller could
	// open a socket as someone else and receive their realtime chat/notifications.
	claims, err := parseToken(r.URL.Query().Get("token"))
	if err != nil || claims.Username != username {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("Failed to upgrade connection for %s: %v", username, err)
		return
	}

	defer func() {
		clientsMu.Lock()
		delete(clients, username)
		clientsMu.Unlock()
		conn.Close()
		// Update last_seen on disconnect
		go UpdateUserLastSeen(username)
		log.Printf("WebSocket for %s disconnected and cleaned up", username)
	}()

	clientsMu.Lock()
	clients[username] = conn
	clientsMu.Unlock()
	log.Printf("WebSocket for %s connected", username)

	go SendStoredNotifications(username)

	conn.SetReadDeadline(time.Now().Add(pongWait))
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	go func() {
		ticker := time.NewTicker(pingPeriod)
		defer ticker.Stop()
		for {
			<-ticker.C
			conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				log.Printf("Ping failed for %s, connection will be closed: %v", username, err)
				return
			}
		}
	}()

	for {
		_, message, err := conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("WebSocket read error for %s: %v", username, err)
			}
			break
		}
		log.Printf("Received message from %s: %s", username, message)
	}
}

// IsUserOnline checks if a user is currently connected via websocket.
func IsUserOnline(username string) (*websocket.Conn, bool) {
	clientsMu.Lock()
	defer clientsMu.Unlock()
	conn, ok := clients[username]
	return conn, ok
}

// GetOnlineUsernames returns a list of all currently connected usernames.
func GetOnlineUsernames() []string {
	clientsMu.Lock()
	defer clientsMu.Unlock()
	usernames := make([]string, 0, len(clients))
	for u := range clients {
		usernames = append(usernames, u)
	}
	return usernames
}

// OnlineStatusHandler returns whether a user is online + their last seen.
// GET /api/v1/chat/online/{username}
func OnlineStatusHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	username := vars["username"]

	_, online := IsUserOnline(username)
	lastSeen := ""
	if !online {
		lastSeen = GetUserLastSeen(username)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"username": username,
		"online":   online,
		"lastSeen": lastSeen,
	})
}
