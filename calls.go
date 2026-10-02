package main

// calls.go — audio and video calls between two people.
//
// The sound and the picture do not come through this server. They go
// straight from one phone to the other (WebRTC — the same way most calling
// apps work). What the server does is the introduction: the two phones
// have to swap a description of what they can send and a list of network
// addresses they can be reached at before they can find each other, and
// they swap those through the live connection the app already keeps open
// for chat. chat_live.go passes them on; this file adds the parts that are
// about calls only.
//
// A call, as the phones send it:
//
//	call_offer     caller → callee   "I'm calling" + caller's description
//	call_ringing   callee → caller   it is ringing on their screen
//	call_answer    callee → caller   picked up + callee's description
//	call_ice       both ways         network addresses, a few at a time
//	call_decline   callee → caller   turned down
//	call_busy      callee → caller   already on another call
//	call_end       either            hung up; reason "missed" if never answered
//
// And one from the server: call_unavailable, to the caller, when the person
// they rang is not online (or has blocked them, or the caller is ringing
// too often). Said the same way in every case.
//
// ════════════════════════════════════════════════════════════════════════
// WHY A CALL CAN CONNECT AND STILL HAVE NO SOUND OR PICTURE
// ════════════════════════════════════════════════════════════════════════
//
// Two phones on ordinary Wi-Fi can usually find each other with nothing
// but a free public address-finder (a STUN server; Google runs one). Two
// phones on some mobile networks cannot: the network hides them so well
// that neither can reach the other, and the call needs a relay in the
// middle (a TURN server) to carry the sound and picture. Nobody runs one of
// those for free at any size, so it is set by the owner:
//
//	TURN_URLS        e.g. turn:relay.example.com:3478,turns:relay.example.com:5349
//	TURN_USERNAME
//	TURN_CREDENTIAL
//
// Without them most calls work and some do not, and the app says so when a
// call cannot connect rather than sitting on "Connecting…" forever.

import (
	"encoding/json"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

// callRingLimit is how long a call is believed to be ringing. The app gives
// up at 45 seconds; this leaves room for a slow network.
const callRingLimit = 2 * time.Minute

// noteMissedCall is the notification kind for "maya tried to call you."
const noteMissedCall = "missed_call"

// offerSent remembers a call this phone started, or — when the person is
// not online to ring — tells the caller so and leaves the other person a
// missed call to find when they are back.
func (c *liveConn) offerSent(ev map[string]interface{}, to, username string, delivered bool) {
	for id, call := range c.offered {
		if time.Since(call.at) > callRingLimit {
			delete(c.offered, id)
		}
	}
	callID, _ := ev["callId"].(string)
	video, _ := ev["video"].(bool)
	if !delivered {
		c.missedCall(to, username, video)
		c.tellUnavailable(ev, to)
		return
	}
	if callID != "" {
		c.offered[callID] = offeredCall{to: to, username: username, video: video, at: time.Now()}
	}
}

// callEnded leaves a missed call when the caller hung up before anybody
// answered. Only for a call this phone really started, and only once.
func (c *liveConn) callEnded(ev map[string]interface{}) {
	callID, _ := ev["callId"].(string)
	call, ok := c.offered[callID]
	if !ok {
		return
	}
	delete(c.offered, callID)
	if reason, _ := ev["reason"].(string); reason == "missed" && time.Since(call.at) <= callRingLimit {
		c.missedCall(call.to, call.username, call.video)
	}
}

// tellUnavailable answers a call that cannot ring.
func (c *liveConn) tellUnavailable(ev map[string]interface{}, to string) {
	callID, _ := ev["callId"].(string)
	tellLive(c.username, map[string]interface{}{
		"type":   "call_unavailable",
		"callId": callID,
		"from":   to,
	})
}

// missedCall puts "leo tried to call you." in the person's notifications.
func (c *liveConn) missedCall(to, username string, video bool) {
	uid, err := strconv.Atoi(to)
	if err != nil {
		return
	}
	me, _ := strconv.Atoi(c.userID)
	body := "tried to call you."
	if video {
		body = "tried to video call you."
	}
	notifyUser(InboxNote{
		UserID:    uid,
		Username:  username,
		Kind:      noteMissedCall,
		ActorID:   me,
		ActorName: c.username,
		Body:      body,
	})
	// And on their phone. Not waited for: this runs inside the caller's
	// connection, and a slow push service must not hold it up.
	go pushMissedCall(to, c.userID, c.username, video)
}

// iceServers is where a phone may look for its own public address, and the
// relay to fall back on when there is one (see the top of this file).
func iceServers() []map[string]interface{} {
	servers := []map[string]interface{}{{
		"urls": []string{"stun:stun.l.google.com:19302", "stun:stun1.l.google.com:19302"},
	}}
	var urls []string
	for _, u := range strings.Split(os.Getenv("TURN_URLS"), ",") {
		if u = strings.TrimSpace(u); u != "" {
			urls = append(urls, u)
		}
	}
	if len(urls) > 0 {
		servers = append(servers, map[string]interface{}{
			"urls":       urls,
			"username":   os.Getenv("TURN_USERNAME"),
			"credential": os.Getenv("TURN_CREDENTIAL"),
		})
	}
	return servers
}

// CallIceServersHandler handles GET /api/v1/calls/ice. "relay" says whether
// a relay is set up, so the app knows a call that cannot connect is a
// network the relay would have fixed.
func CallIceServersHandler(w http.ResponseWriter, r *http.Request) {
	servers := iceServers()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"iceServers": servers,
		"relay":      len(servers) > 1,
	})
}
