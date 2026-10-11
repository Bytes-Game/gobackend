package main

// forwarded_shape.go — one log line that says how the host passes on the
// caller's address.
//
// clientIP takes the first address in X-Forwarded-For. That is right if the
// host (Render) puts the real caller first and drops whatever the caller
// sent, and wrong if it only adds to what the caller sent — then anybody can
// type their own first address, and every per-address limit (sign-in
// guesses, new accounts) can be dodged by typing a new one each time.
// Render's own pages and its users disagree about which it does.
//
// So the server says, once per start, what the first forwarded request
// looked like: how many addresses it carried and which other address
// headers came with it. Never the addresses themselves. The app sends no
// such header, so on a normal phone request one address means the host
// wrote the list itself, and the first entry is the caller. More than one
// means something on the way added entries, and which one is the caller
// has to be worked out before clientIP can be trusted.

import (
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
)

var forwardedShapeOnce sync.Once

// forwardedShape describes a request's address headers without the
// addresses: how many are in X-Forwarded-For, and which others are there.
func forwardedShape(r *http.Request) string {
	n := 0
	for _, part := range strings.Split(r.Header.Get("X-Forwarded-For"), ",") {
		if strings.TrimSpace(part) != "" {
			n++
		}
	}
	var others []string
	for _, h := range []string{"X-Real-IP", "True-Client-IP", "CF-Connecting-IP", "Forwarded"} {
		if r.Header.Get(h) != "" {
			others = append(others, h)
		}
	}
	also := "none"
	if len(others) > 0 {
		also = strings.Join(others, ", ")
	}
	return "X-Forwarded-For carries " + strconv.Itoa(n) + " address(es); other address headers: " + also
}

// noteForwardedShape logs forwardedShape for the first request that came
// through a proxy, once per start.
func noteForwardedShape(r *http.Request) {
	if r.Header.Get("X-Forwarded-For") == "" {
		return
	}
	forwardedShapeOnce.Do(func() {
		log.Printf("how the host forwards the caller's address (first request since start): %s. "+
			"On a request from the app, one address means the host writes the "+
			"list and the per-address limits hold; more than one means entries "+
			"were added on the way and clientIP's choice needs checking.",
			forwardedShape(r))
	})
}
