package main

// chat_live.go — what a chat shows the moment it happens: "Delivered",
// "Seen" and "typing…".
//
// ════════════════════════════════════════════════════════════════════════
// THE BUG THIS FIXES
// ════════════════════════════════════════════════════════════════════════
//
// Opening a chat marked the other person's messages as read in the
// database, and told nobody. The person who sent them kept seeing "Sent"
// until they left the chat and opened it again, because only opening it
// asked the database. "Delivered" was never set at all.
//
// Now the server tells the sender the moment either happens, down the same
// live connection their new messages already arrive on:
//
//	{"type":"chat_delivered","receiverId":"7","messageIds":["41"]}
//	{"type":"chat_read","readerId":"7","readerUsername":"maya"}
//
// A message is delivered when it reaches the other person's phone: at once
// if their app is open, or the next time it connects (deliverWaiting).
//
// ════════════════════════════════════════════════════════════════════════
// WHAT A PHONE MAY SEND
// ════════════════════════════════════════════════════════════════════════
//
// The live connection used to go one way. Anything a phone said down it was
// written to the log and dropped. A phone may now send a short list of
// things meant for one other person — "I'm typing", and the messages that
// set up a call (calls.go) — and the server passes them on:
//
//	phone sends    {"type":"typing","to":"7","typing":true}
//	7 receives     {"type":"typing","from":"3","fromUsername":"leo","typing":true}
//
// The server fills in who it is from itself, so nobody can pretend to be
// somebody else. Nothing passes between two people when either has blocked
// the other. Anything not on the list is dropped, with a log line saying so.

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strconv"
	"time"
)

// liveEventLimit is the biggest single thing a phone may send. The largest
// real one is a call's opening message, which describes the phone's sound
// and video in a few kilobytes.
const liveEventLimit = 64 << 10

// relayed is what a phone may send to somebody else, and the rate limit in
// actionLimitTable each counts against ("" for none beyond liveConn's own
// cap). Only starting a call and typing are limited by person: the rest
// only ever follow a call that was allowed to start.
var relayed = map[string]string{
	"typing":       "typing",
	"call_offer":   "call",
	"call_ringing": "",
	"call_answer":  "",
	"call_ice":     "",
	"call_decline": "",
	"call_busy":    "",
	"call_end":     "",
}

// How fast one connection may send things on, whatever they are. Setting up
// a call sends a quick burst of a dozen or two small network messages, so
// the burst is generous; the steady rate is not.
const (
	liveBurst   = 120
	livePerSec  = 30.0
	livePeerTTL = time.Minute
)

// liveConn is one phone's live connection, as the relay sees it. Used only
// by that connection's own read loop, so it needs no lock.
type liveConn struct {
	userID   string
	username string

	// peers remembers for a minute who each person this phone talks to is,
	// and whether either has blocked the other, so a burst of typing or call
	// set-up does not ask the database every time.
	peers map[string]livePeer

	// offered is the calls this phone started that may still be ringing.
	// "They missed my call" is only believed for one of these, once.
	offered map[string]offeredCall

	tokens float64
	last   time.Time
}

type livePeer struct {
	username string
	blocked  bool
	// refuses: they take calls and messages only from people they follow,
	// and do not follow this person. See privacy_settings.go.
	refuses bool
	exists  bool
	at      time.Time
}

type offeredCall struct {
	to       string
	username string
	video    bool
	at       time.Time
}

func newLiveConn(userID, username string) *liveConn {
	return &liveConn{
		userID:   userID,
		username: username,
		peers:    map[string]livePeer{},
		offered:  map[string]offeredCall{},
		tokens:   liveBurst,
		last:     time.Now(),
	}
}

// allow spends one token from this connection's budget.
func (c *liveConn) allow() bool {
	now := time.Now()
	c.tokens += now.Sub(c.last).Seconds() * livePerSec
	if c.tokens > liveBurst {
		c.tokens = liveBurst
	}
	c.last = now
	if c.tokens < 1 {
		return false
	}
	c.tokens--
	return true
}

// handle passes one thing this phone sent on to the person it names.
func (c *liveConn) handle(raw []byte) {
	var ev map[string]interface{}
	if err := json.Unmarshal(raw, &ev); err != nil {
		log.Printf("live: %s sent something that is not JSON, dropped: %v", c.username, err)
		return
	}
	kind, _ := ev["type"].(string)
	limit, ok := relayed[kind]
	if !ok {
		log.Printf("live: %s sent %q, which a phone may not send, dropped", c.username, kind)
		return
	}
	to, _ := ev["to"].(string)
	if to == "" || to == c.userID {
		return
	}
	if !c.allow() || (limit != "" && !allowAction(c.userID, limit)) {
		log.Printf("live: %s is sending %s too fast, dropped", c.username, kind)
		if kind == "call_offer" {
			c.tellUnavailable(ev, to)
		}
		return
	}
	peer := c.peer(to)
	if !peer.exists || peer.blocked || (peer.refuses && kind == "call_offer") {
		// Said the same way whichever it is: a caller is not told that
		// somebody has blocked them, or takes calls only from people they
		// follow.
		if kind == "call_offer" {
			c.tellUnavailable(ev, to)
		}
		return
	}

	delete(ev, "to")
	ev["from"] = c.userID
	ev["fromUsername"] = c.username
	data, err := json.Marshal(ev)
	if err != nil {
		return
	}
	delivered := wsDeliver(peer.username, data)

	switch kind {
	case "call_offer":
		c.offerSent(ev, to, peer.username, delivered)
	case "call_end":
		c.callEnded(ev)
	}
}

// peer looks up the person [id] names, and whether either of the two has
// blocked the other.
func (c *liveConn) peer(id string) livePeer {
	if p, ok := c.peers[id]; ok && time.Since(p.at) < livePeerTTL {
		return p
	}
	p := livePeer{at: time.Now()}
	uid, err := strconv.Atoi(id)
	if err != nil {
		c.peers[id] = p
		return p
	}
	me, _ := strconv.Atoi(c.userID)
	err = db.QueryRow(`
		SELECT u.username, EXISTS(
			SELECT 1 FROM user_blocks
			 WHERE (blocker_id = $1 AND blocked_id = $2)
			    OR (blocker_id = $2 AND blocked_id = $1)),
		       COALESCE(u.settings->>'messages', 'everyone') = 'following'
		       AND NOT EXISTS (SELECT 1 FROM follows f
		                        WHERE f.follower_id = u.id AND f.following_id = $1)
		  FROM users u WHERE u.id = $2`, me, uid).Scan(&p.username, &p.blocked, &p.refuses)
	if errors.Is(err, sql.ErrNoRows) {
		c.peers[id] = p
		return p
	}
	if queryFailed(fmt.Sprintf("looking up user %s for something %s sent live", id, c.username),
		"it is not passed on; they will not see it", err) {
		return p // not remembered: the next one asks again
	}
	p.exists = true
	c.peers[id] = p
	return p
}

// tellLive sends one live signal to a person, wherever they are connected.
// Nothing is kept for somebody offline: each of these describes a moment
// that has passed by the time they come back, and what it changed is in
// the database for when they do.
func tellLive(username string, payload map[string]interface{}) bool {
	data, err := json.Marshal(payload)
	if err != nil {
		return false
	}
	return wsDeliver(username, data)
}

// markReadAndTell marks everything [senderID] sent [readerID] as read and,
// when that changed anything, tells the sender at once.
func markReadAndTell(senderID, readerID int) {
	if MarkMessagesRead(senderID, readerID) == 0 {
		return
	}
	var sender, reader string
	err := db.QueryRow(`
		SELECT s.username, r.username
		  FROM users s, users r
		 WHERE s.id = $1 AND r.id = $2`, senderID, readerID).Scan(&sender, &reader)
	if queryFailed(fmt.Sprintf("finding who to tell that user %d read user %d's messages", readerID, senderID),
		"they see Seen the next time they open the chat instead of now", err) {
		return
	}
	tellLive(sender, map[string]interface{}{
		"type":           "chat_read",
		"readerId":       strconv.Itoa(readerID),
		"readerUsername": reader,
		"timestamp":      time.Now().UTC().Format(time.RFC3339),
	})
}

// markDeliveredAndTell records that message [msgID] reached the other
// person's phone and tells its sender. A message already read stays read:
// the two can race, and Seen must never go back to Delivered.
func markDeliveredAndTell(msgID int, senderUsername, receiverID string) {
	res, err := db.Exec(`
		UPDATE chat_messages SET status = 'delivered'
		 WHERE id = $1 AND is_read IS NOT TRUE
		   AND COALESCE(status, 'sent') = 'sent'`, msgID)
	if queryFailed(fmt.Sprintf("marking message %d delivered", msgID),
		"its sender keeps seeing Sent until it is read", err) || res == nil {
		return
	}
	n, err := res.RowsAffected()
	if err != nil || n == 0 {
		return
	}
	tellLive(senderUsername, map[string]interface{}{
		"type":       "chat_delivered",
		"receiverId": receiverID,
		"messageIds": []string{strconv.Itoa(msgID)},
	})
}

// deliverWaiting runs when a phone connects. Everything sent to this person
// while their app was closed has now reached them, so it is marked
// delivered and each sender is told.
func deliverWaiting(userID int) {
	rows, err := db.Query(`
		WITH d AS (
			UPDATE chat_messages SET status = 'delivered'
			 WHERE receiver_id = $1 AND is_read IS NOT TRUE
			   AND COALESCE(status, 'sent') = 'sent'
			RETURNING id, sender_id)
		SELECT d.id, u.username FROM d JOIN users u ON u.id = d.sender_id`, userID)
	if queryFailed(fmt.Sprintf("marking messages to user %d delivered as they came online", userID),
		"their senders keep seeing Sent until the messages are read", err) {
		return
	}
	defer rows.Close()
	bySender := map[string][]string{}
	bad := 0
	for rows.Next() {
		var id int
		var sender string
		if scanFailed("messages delivered as somebody came online", rows.Scan(&id, &sender), &bad) {
			continue
		}
		bySender[sender] = append(bySender[sender], strconv.Itoa(id))
	}
	receiver := strconv.Itoa(userID)
	for sender, ids := range bySender {
		tellLive(sender, map[string]interface{}{
			"type":       "chat_delivered",
			"receiverId": receiver,
			"messageIds": ids,
		})
	}
}
