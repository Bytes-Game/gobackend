package main

import (
	"encoding/json"
	"log"
	"sync"
	"time"
)

// ─────────────────────────────────────────────────────────────────────────────
// SERVER-PUSHED NEXT-REEL HINTS
//
// When a user starts watching a challenge, we push them a tiny invisible
// WebSocket message carrying the URL of the reel the ranker thinks they're
// most likely to want next. The mobile client uses it to call
// VideoPlayerService.prefetch(url) so that — by the time the user actually
// swipes — the next controller is already initialized and the first frame is
// in memory.
//
// This is purely additive on top of the client's own velocity-aware
// prefetch. The client always knows the next page-item's URL because the
// feed handler returns ~20 items per page; what the SERVER knows that the
// CLIENT doesn't is the FRESHEST trending signal (events from the last
// few minutes that landed AFTER the client's page was fetched). So the
// hint is most valuable when:
//   - the user is near the end of their current page,
//   - or the trending Z-set has moved since the client's last fetch.
//
// We deliberately:
//   - skip storage when offline (a stale hint has zero value),
//   - throttle per-user (one hint per 2s) so a fast scroller doesn't
//     flood their own WS pipe,
//   - use the existing sourceTrendingRealtime candidate source so we
//     don't double up on backend complexity — that source already
//     applies seen/blocked filtering for the user.
// ─────────────────────────────────────────────────────────────────────────────

// hintThrottleWindow is the minimum gap between two hints to the same
// user. 2s is short enough that a viewer pausing for 3s on a reel still
// gets a fresh hint for whatever's next, but long enough that a fast
// scroller burning through 10 reels in 5s only gets ~3 hints (one every
// other swipe). The client's velocity-aware prefetch covers the gap.
const hintThrottleWindow = 2 * time.Second

// hintSweepThreshold triggers the stale-stamp sweep of hintLastSent.
// Every entry older than hintThrottleWindow is dead weight; sweeping
// only past this size keeps the common case allocation-free.
const hintSweepThreshold = 4096

var (
	hintLastSentMu sync.Mutex
	hintLastSent   = make(map[string]time.Time) // username -> last hint time
)

// SendNextReelHint picks a likely-next reel for the given user and pushes
// its video URL over the WebSocket so the client can warm a controller
// before the user swipes. Safe to call in a goroutine — it does its own
// throttling, lookups, and error swallowing. No-op when:
//   - user is offline (no Conn — Redis-storing a hint is worse than nothing),
//   - throttled (last hint < hintThrottleWindow ago),
//   - the trending source returned nothing or only the URL the user is
//     already watching.
//
// Inputs are the watch-event fields we already have at the call site:
// userID is the numeric user ID (matched against the Challenges' creator
// graph) and currentContentID is what they just started watching (so we
// can avoid recommending the same thing back).
func SendNextReelHint(userID, currentContentID string) {
	if userID == "" {
		return
	}
	user, found := GetUserByID(userID)
	if !found || user.Username == "" {
		return
	}
	username := user.Username

	// Throttle: skip if we pushed a hint to this user too recently.
	hintLastSentMu.Lock()
	last, has := hintLastSent[username]
	if has && time.Since(last) < hintThrottleWindow {
		hintLastSentMu.Unlock()
		return
	}
	// Tentatively reserve the slot — even if we end up not sending
	// (e.g. user went offline between checks), the throttle stamp is
	// still correct because the next call will re-check time.Since().
	hintLastSent[username] = time.Now()
	// Opportunistic sweep: every stamp is stale after the throttle
	// window, so anything older is pure garbage. Without this the map
	// grew by one entry per distinct user for the life of the process
	// (a slow leak at the millions-of-users target). Amortized: only
	// runs when the map has grown past the sweep threshold.
	if len(hintLastSent) > hintSweepThreshold {
		cutoff := time.Now().Add(-hintThrottleWindow)
		for u, t := range hintLastSent {
			if t.Before(cutoff) {
				delete(hintLastSent, u)
			}
		}
	}
	hintLastSentMu.Unlock()

	// Only do real work if the user is actually online (on any replica).
	// There's no point picking and sending if no WebSocket is there to
	// deliver — and storing an ephemeral prefetch URL in Redis for
	// "later" delivery is worse than nothing (the URL could be evicted
	// from CDN, the user's interests could have shifted, etc.).
	if !IsUserOnline(username) {
		return
	}

	// Pull a handful of candidates so we have something to fall back
	// on if the top one collides with what they're already watching.
	candidates := sourceTrendingRealtime(userID, 5)
	if len(candidates) == 0 {
		return
	}

	chosenURL := ""
	for _, item := range candidates {
		// Only challenges (the feed is challenge-only post the post
		// retirement) and only those with a playable URL.
		if item.Challenge == nil {
			continue
		}
		if item.Challenge.ID == currentContentID {
			continue // don't hint the reel they just started
		}
		url := item.Challenge.VideoURL
		if url == "" {
			continue
		}
		chosenURL = url
		break
	}
	if chosenURL == "" {
		return
	}

	notif := Notification{
		Type:      "next_reel_hint",
		Message:   "", // invisible — client suppresses
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		VideoURL:  chosenURL,
	}
	payload, err := json.Marshal(notif)
	if err != nil {
		return
	}
	if !wsDeliver(username, payload) {
		// Lost connection mid-write. Don't try to recover — the next
		// watch event will trigger a fresh hint if the user reconnects.
		// Drop the throttle stamp so the reconnected client gets a
		// hint on the very next watch event instead of waiting out the
		// window against a stale timestamp.
		hintLastSentMu.Lock()
		delete(hintLastSent, username)
		hintLastSentMu.Unlock()
		log.Printf("next_reel_hint: delivery failed for %s", username)
	}
}
