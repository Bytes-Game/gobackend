package main

// country.go — which country a request comes from, and which country an
// account is in.
//
// Three things in this server depend on the country:
//
//   - how old somebody must be to join, and whether a parent must agree
//     first (age_rules.go, parent_consent.go);
//   - the feed, which prefers videos made in the viewer's own country
//     (sameCountryBonus, below);
//   - the record of a parent's consent, which says where it was given.
//
// ════════════════════════════════════════════════════════════════════════
// WHERE THE COUNTRY COMES FROM
// ════════════════════════════════════════════════════════════════════════
//
// First, from the network. A request reaches this server through a network
// that knows which country the phone's internet address is in, and says so
// in a header: Cloudflare (which sits in front of Render, and in front of
// flexete.com) sends CF-IPCountry. The others in networkCountryHeaders are
// what the same thing is called on other hosts, so moving host does not
// silently switch this off.
//
// Failing that, from the phone: the app sends its region setting in
// X-Device-Country. That is what the person chose, not where they are, so
// it is only the fallback.
//
// Neither is proof. Somebody determined can pretend to be elsewhere, as on
// every app. It is good enough to pick the right rules for honest people,
// which is what the law asks for.
//
// GET /signup/rules answers with the country this server sees for the
// caller, so whether the host passes the header can be checked from
// outside in one request.

import (
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/lib/pq"
)

// networkCountryHeaders are the headers a network in front of this server
// may carry the visitor's country in, in the order they are trusted.
var networkCountryHeaders = []string{
	"CF-IPCountry",              // Cloudflare (Render, flexete.com, DigitalOcean App Platform)
	"CloudFront-Viewer-Country", // Amazon
	"X-Vercel-IP-Country",       // Vercel
	"X-AppEngine-Country",       // Google
	"Fly-Client-Country",        // Fly.io
}

// deviceCountryHeader is the app's own word for its country: the region
// the phone is set to.
const deviceCountryHeader = "X-Device-Country"

// cleanCountry is a country code in one form — two capital letters — or ""
// for anything that is not one. "XX" and "ZZ" are how networks say "not
// known", and "T1" is Cloudflare's code for the Tor network: none of them
// is a country.
func cleanCountry(raw string) string {
	c := strings.ToUpper(strings.TrimSpace(raw))
	if len(c) != 2 || c[0] < 'A' || c[0] > 'Z' || c[1] < 'A' || c[1] > 'Z' {
		return ""
	}
	switch c {
	case "XX", "ZZ", "T1":
		return ""
	}
	return c
}

// countryOf is the country [r] comes from, and how it is known: "network"
// (the internet address), "device" (the phone's region setting), or ""
// with no country when neither says.
func countryOf(r *http.Request) (country, source string) {
	for _, h := range networkCountryHeaders {
		if c := cleanCountry(r.Header.Get(h)); c != "" {
			return c, "network"
		}
	}
	if c := cleanCountry(r.Header.Get(deviceCountryHeader)); c != "" {
		return c, "device"
	}
	return "", ""
}

// ─── the country an account is in ───────────────────────────────────────

// countryFresh is how long a remembered "no country known" is trusted
// before the database is asked again.
const countryFresh = 10 * time.Minute

// countryCacheMax is how many accounts are remembered at once. Past it the
// memory is emptied and fills again from the next requests: every signed-in
// request says where it is from, so nothing is lost but a database read.
const countryCacheMax = 50_000

type countryEntry struct {
	country string
	checked time.Time
}

var countryCache = struct {
	sync.RWMutex
	byUser map[string]countryEntry
}{byUser: map[string]countryEntry{}}

func rememberedCountry(userID string) (countryEntry, bool) {
	countryCache.RLock()
	defer countryCache.RUnlock()
	e, ok := countryCache.byUser[userID]
	return e, ok
}

func rememberCountry(userID, country string) {
	countryCache.Lock()
	defer countryCache.Unlock()
	if len(countryCache.byUser) >= countryCacheMax {
		countryCache.byUser = map[string]countryEntry{}
	}
	countryCache.byUser[userID] = countryEntry{country: country, checked: time.Now()}
}

// noteCountry remembers that [userID] was just seen in [country], and
// saves it to the account when it changed. Called for every signed-in
// request (authed), so it does nothing at all when the country is the one
// already remembered.
func noteCountry(userID, country string) {
	if userID == "" || country == "" {
		return
	}
	if e, ok := rememberedCountry(userID); ok && e.country == country {
		return
	}
	rememberCountry(userID, country)
	if db == nil {
		return
	}
	go func() {
		_, err := db.Exec(`UPDATE users SET country = $2 WHERE id::text = $1 AND country <> $2`,
			userID, country)
		queryFailed("saving that account "+userID+" is now in "+country,
			"the feed keeps using the country it had", err)
	}()
}

// viewerCountry is the country [userID] was last seen in, or "".
func viewerCountry(userID string) string {
	if userID == "" {
		return ""
	}
	if e, ok := rememberedCountry(userID); ok && (e.country != "" || time.Since(e.checked) < countryFresh) {
		return e.country
	}
	if db == nil {
		return ""
	}
	var c string
	err := db.QueryRow(`SELECT country FROM users WHERE id::text = $1`, userID).Scan(&c)
	// No such account reads as no country; a real failure says so.
	if queryFailed("which country account "+userID+" is in",
		"the feed does not prefer any country for them", err) {
		c = ""
	}
	rememberCountry(userID, c)
	return c
}

// forgetCountries drops everything remembered, for tests.
func forgetCountries() {
	countryCache.Lock()
	defer countryCache.Unlock()
	countryCache.byUser = map[string]countryEntry{}
}

// ─── the feed ───────────────────────────────────────────────────────────

// sameCountryBonus is what a video made in the viewer's own country gains
// in the feed. The size of the feed's other bounded bonuses (the largest is
// 0.10): enough to bring local videos up among equals, never enough to put
// a bad local video above a good one from elsewhere. Videos from other
// countries are never hidden.
const sameCountryBonus = 0.10

// countryBonus is that bonus for one video and one viewer: sameCountryBonus
// when both countries are known and the same, otherwise nothing.
func countryBonus(viewer, video string) float64 {
	if viewer == "" || video == "" || viewer != video {
		return 0
	}
	return sameCountryBonus
}

// addCountryBonus adds that bonus to one video's [score] for one viewer,
// and notes it in the score's [breakdown]. It is scaled by the same
// "negativeMult" the rest of the score was: a video from a creator the
// viewer blocked stays at exactly nothing, wherever it is from.
func addCountryBonus(score float64, breakdown map[string]float64, viewer, video string) float64 {
	bonus := countryBonus(viewer, video)
	if bonus == 0 {
		return score
	}
	if mult, ok := breakdown["negativeMult"]; ok {
		bonus *= mult
	}
	breakdown["countryBonus"] = bonus
	return score + bonus
}

// stampCountry records where a new video's creator was when they posted
// it. An unknown country leaves the video to follow its creator's own.
func stampCountry(challengeID, country string) {
	if db == nil || challengeID == "" || country == "" {
		return
	}
	_, err := db.Exec(`UPDATE challenges SET country = $2 WHERE id::text = $1`, challengeID, country)
	queryFailed("saving which country video "+challengeID+" was posted from",
		"the feed uses its creator's country instead", err)
}

// videoCountries is the country of each of [ids] (challenge ids): where it
// was posted from, or failing that where its creator is. Videos with no
// known country are left out.
func videoCountries(ids []string) map[string]string {
	out := map[string]string{}
	if db == nil || len(ids) == 0 {
		return out
	}
	rows, err := db.Query(`
		SELECT c.id::text, COALESCE(NULLIF(c.country, ''), u.country, '')
		  FROM challenges c JOIN users u ON u.id = c.creator_id
		 WHERE c.id::text = ANY($1)`, pq.Array(ids))
	if queryFailed("which countries a page of videos is from",
		"new viewers are shown them without preferring their own country", err) {
		return out
	}
	defer rows.Close()
	bad := 0
	for rows.Next() {
		var id, c string
		if scanFailed("a video's country", rows.Scan(&id, &c), &bad) {
			continue
		}
		if c != "" {
			out[id] = c
		}
	}
	return out
}
