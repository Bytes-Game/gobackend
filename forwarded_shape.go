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
	"os"
	"strconv"
	"strings"
	"sync"
)

// trustCloudflareAddress is whether the header Cloudflare writes with the
// caller's address (CF-Connecting-IP) is believed.
//
// Yes on Render: Render puts Cloudflare in front of every service, and
// Cloudflare replaces that header on the way in, so a caller cannot set it.
// Render marks its machines with RENDER=true. Anywhere else it is believed
// only when the owner says the same is true there, with
// TRUST_CF_CONNECTING_IP=true: on a host with no Cloudflare in front, the
// header would be the caller's to type.
func trustCloudflareAddress() bool {
	if v := strings.ToLower(strings.TrimSpace(os.Getenv("TRUST_CF_CONNECTING_IP"))); v != "" {
		return v == "true" || v == "1" || v == "yes"
	}
	return strings.EqualFold(strings.TrimSpace(os.Getenv("RENDER")), "true")
}

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
		using := "the first address in X-Forwarded-For, which a caller can choose"
		if trustCloudflareAddress() && r.Header.Get("CF-Connecting-IP") != "" {
			using = "CF-Connecting-IP, which only Cloudflare can write"
		} else if trustCloudflareAddress() {
			using += " — CF-Connecting-IP is trusted here but did not arrive, so the per-address limits can be dodged"
		}
		log.Printf("how the host forwards the caller's address (first request since start): %s. "+
			"The per-address limits use %s.", forwardedShape(r), using)
	})
}
