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

	// wsPresenceTTL bounds how long a crashed replica's ghost presence
	// survives in Redis. Refreshed on every ping tick (54s), so a live
	// connection never expires; a dead replica's entries clear within
	// ~2 minutes.
	wsPresenceTTL = 2 * time.Minute

	// wsRelayChannel carries cross-replica deliveries: a user connected
	// to replica B still gets the chat message that replica A's HTTP
	// handler produced. Only used when multiReplica() is on.
	wsRelayChannel = "ws:relay"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin:     func(r *http.Request) bool { return true },
}

// wsClient wraps a connection with a write mutex. gorilla/websocket
// forbids concurrent writers on one conn — chat delivery, notification
// delivery, next-reel hints, and the keepalive ping ticker all write
// from separate goroutines, which could previously interleave and
// panic ("concurrent write to websocket connection"). Every write MUST
// go through writeMessage.
type wsClient struct {
	conn *websocket.Conn
	mu   sync.Mutex
}

func (c *wsClient) writeMessage(msgType int, data []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.conn.SetWriteDeadline(time.Now().Add(writeWait))
	return c.conn.WriteMessage(msgType, data)
}

var clients = make(map[string]*wsClient)
var clientsMu sync.Mutex

// wsPresenceKey is the per-user Redis presence marker. A plain key with
// a TTL (not a set member) so expiry is native and per-user.
func wsPresenceKey(username string) string { return "ws:online:" + username }

func wsMarkOnline(username string) {
	if rdb == nil || !multiReplica() {
		return
	}
	_ = rdb.Set(rctx, wsPresenceKey(username), "1", wsPresenceTTL).Err()
}

func wsMarkOffline(username string) {
	if rdb == nil || !multiReplica() {
		return
	}
	_ = rdb.Del(rctx, wsPresenceKey(username)).Err()
}

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
	client := &wsClient{conn: conn}

	defer func() {
		clientsMu.Lock()
		// Only clear the map entry if it's still OUR connection — a fast
		// reconnect may have replaced it, and deleting the newcomer's
		// entry would silently disconnect them from deliveries.
		if clients[username] == client {
			delete(clients, username)
			wsMarkOffline(username)
		}
		clientsMu.Unlock()
		conn.Close()
		// Update last_seen on disconnect
		go UpdateUserLastSeen(username)
		log.Printf("WebSocket for %s disconnected and cleaned up", username)
	}()

	clientsMu.Lock()
	clients[username] = client
	clientsMu.Unlock()
	wsMarkOnline(username)
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
			if err := client.writeMessage(websocket.PingMessage, nil); err != nil {
				log.Printf("Ping failed for %s, connection will be closed: %v", username, err)
				return
			}
			// Keep the cross-replica presence marker alive.
			wsMarkOnline(username)
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

// wsSendLocal writes a text payload to a user connected to THIS replica.
// Returns false when the user isn't connected here or the write failed.
func wsSendLocal(username string, data []byte) bool {
	clientsMu.Lock()
	client, ok := clients[username]
	clientsMu.Unlock()
	if !ok {
		return false
	}
	if err := client.writeMessage(websocket.TextMessage, data); err != nil {
		log.Printf("ws write failed for %s: %v", username, err)
		return false
	}
	return true
}

// wsRelayEnvelope is the message shape published on wsRelayChannel.
type wsRelayEnvelope struct {
	Username string          `json:"u"`
	Payload  json.RawMessage `json:"p"`
}

// wsDeliver sends a text payload to a user wherever they're connected:
// this replica directly, or — in multi-replica mode — any replica via
// Redis pub/sub. Returns true when the payload was delivered locally or
// handed to the relay while the user shows presence somewhere; false
// means "treat as offline" (callers fall back to the stored-notification
// queue or simply skip, exactly as before).
func wsDeliver(username string, data []byte) bool {
	if wsSendLocal(username, data) {
		return true
	}
	if !multiReplica() || rdb == nil {
		return false
	}
	// Cross-replica: only relay when the user shows presence somewhere —
	// publishing to nobody is harmless but returning true for a truly
	// offline user would skip the offline-queue fallback.
	if n, err := rdb.Exists(rctx, wsPresenceKey(username)).Result(); err != nil || n == 0 {
		return false
	}
	env, err := json.Marshal(wsRelayEnvelope{Username: username, Payload: data})
	if err != nil {
		return false
	}
	if err := rdb.Publish(rctx, wsRelayChannel, env).Err(); err != nil {
		return false
	}
	return true
}

// startWSRelay subscribes this replica to the cross-replica delivery
// channel. No-op outside multi-replica mode.
func startWSRelay() {
	if !multiReplica() || rdb == nil {
		return
	}
	go func() {
		sub := rdb.Subscribe(rctx, wsRelayChannel)
		defer sub.Close()
		for msg := range sub.Channel() {
			var env wsRelayEnvelope
			if err := json.Unmarshal([]byte(msg.Payload), &env); err != nil {
				continue
			}
			// Deliver only if the user is on THIS replica; other
			// replicas got the same publish and check their own maps.
			wsSendLocal(env.Username, env.Payload)
		}
	}()
}

// IsUserOnline checks if a user is currently connected — to this replica
// always, and to any replica when multi-replica presence is on.
func IsUserOnline(username string) bool {
	clientsMu.Lock()
	_, ok := clients[username]
	clientsMu.Unlock()
	if ok {
		return true
	}
	if multiReplica() && rdb != nil {
		if n, err := rdb.Exists(rctx, wsPresenceKey(username)).Result(); err == nil && n > 0 {
			return true
		}
	}
	return false
}

// GetOnlineUsernames returns a list of usernames connected to THIS
// replica. In multi-replica mode this is a partial view (admin surface
// only — aggregating across replicas isn't worth a Redis scan here).
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

	online := IsUserOnline(username)
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
