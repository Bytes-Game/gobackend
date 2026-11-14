package main

// media_addresses.go — every public address this server has handed files
// out at.
//
// The bucket has one front door at a time: R2_PUBLIC_BASE_URL, the address
// put in front of every new file's name. A file's full address is saved
// with its row when it is posted, and never changes after. So on the day
// the door changes (from R2's test address to media.flexete.com), every
// file posted before that day still carries the old address.
//
// Those files are still ours: the same bucket, the same names. But the
// code that clears storage asked "is this file ours?" by comparing with
// today's address alone, and for those files the answer was no. A video
// posted under the old address could never have been cleared: deleting it,
// deleting the account, or a moderator removing it took the row away and
// left the file where it was, still reachable by its address, with nothing
// logged. The same for a replaced profile photo and a deleted chat photo.
//
// So the server writes down each address it starts with (the table
// media_addresses), and the clearing code accepts any address on the list.
//
// Only for clearing. A NEW upload must still be under today's address
// (ownUpload, validProfilePhoto, the chat check): the list is not a way to
// post a file kept somewhere else.

import (
	"fmt"
	"log"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"
)

// earlierMediaAddresses is every address on record other than today's.
// Written once at start (rememberMediaAddresses) and read by
// keyOfStoredFile on any request after that.
var earlierMediaAddresses struct {
	sync.RWMutex
	list []string
}

// publicAddressList reads addresses separated by commas, as
// R2_OLD_PUBLIC_BASE_URLS holds them: no spaces around one, no slash at
// its end, no empty entry.
func publicAddressList(s string) []string {
	var out []string
	for _, part := range strings.Split(s, ",") {
		if a := strings.TrimRight(strings.TrimSpace(part), "/"); a != "" {
			out = append(out, a)
		}
	}
	return out
}

// everyPublicAddress is today's address and every earlier one: those listed
// by hand (OldPublicBaseURLs) and those this server wrote down itself. No
// slash at the end, none empty, none twice.
//
// The longest first. When one address is the start of another (https://x
// and https://x/bucket), a file saved under the longer one would otherwise
// be read as a file named "bucket/..." under the shorter.
func (c *R2Config) everyPublicAddress() []string {
	if c == nil {
		return nil
	}
	earlierMediaAddresses.RLock()
	written := earlierMediaAddresses.list
	earlierMediaAddresses.RUnlock()

	seen := map[string]bool{}
	var out []string
	for _, group := range [][]string{{c.PublicBaseURL}, c.OldPublicBaseURLs, written} {
		for _, a := range group {
			a = strings.TrimRight(strings.TrimSpace(a), "/")
			if a == "" || seen[a] {
				continue
			}
			seen[a] = true
			out = append(out, a)
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return len(out[i]) > len(out[j]) })
	return out
}

// keyOfStoredFile turns a saved public address back into the file's name in
// the bucket: "https://media.flexete.com/u/29/4ab2/720p.mp4" gives
// "u/29/4ab2/720p.mp4". It answers for today's address and for every
// earlier one. ours is false for an address that was never ours.
//
// The name is exactly what follows the address: nothing is tidied or
// checked here. What a name must look like is for the caller to decide.
func (c *R2Config) keyOfStoredFile(publicURL string) (key string, ours bool) {
	if c == nil || publicURL == "" {
		return "", false
	}
	for _, base := range c.everyPublicAddress() {
		if rest, found := strings.CutPrefix(publicURL, base+"/"); found {
			return rest, true
		}
	}
	return "", false
}

// mediaAddressRetry is how long to wait before asking the database again
// when the addresses could not be read at start.
var mediaAddressRetry = 30 * time.Second

// rememberMediaAddresses writes down the address new files are handed out
// at today, reads back every address this database has on record, and
// keeps the earlier ones for keyOfStoredFile. At start, before anything
// is deleted.
//
// If the database cannot be asked at that moment, it is asked again until
// it can. Without the list a file saved under an earlier address is not
// cleared when it is deleted, and a server can run for weeks: that is too
// long to go without it because of one bad moment at start.
func rememberMediaAddresses() {
	cfg, err := loadR2Config()
	if err != nil || cfg == nil {
		// Storage is not set up: no file was ever handed out, at any address.
		return
	}
	if readMediaAddresses(cfg) {
		return
	}
	wait := mediaAddressRetry
	go func() {
		for {
			time.Sleep(wait)
			if readMediaAddresses(cfg) {
				return
			}
		}
	}()
}

// readMediaAddresses is one go at it. False when the database could not be
// asked: what is kept then is only what was listed by hand, and the
// reason is already in the log.
func readMediaAddresses(cfg *R2Config) bool {
	earlier, asked := recordMediaAddresses(cfg.PublicBaseURL, cfg.OldPublicBaseURLs)
	earlierMediaAddresses.Lock()
	earlierMediaAddresses.list = earlier
	earlierMediaAddresses.Unlock()
	if asked {
		log.Print(mediaAddressLine(cfg.PublicBaseURL, cfg.PublicBaseGuessed, earlier))
	}
	return asked
}

// recordMediaAddresses is the database half: [today] and the addresses
// [listed] by hand go on record, and every address on record other than
// [today] comes back, oldest first.
//
// asked is false when any of it failed. The answer is then [listed], all
// that is known without the database, and the caller tries again later.
// With no database at all there is nothing to try again: [listed], true.
func recordMediaAddresses(today string, listed []string) (earlier []string, asked bool) {
	if db == nil {
		return listed, true
	}
	asked = true
	for _, a := range append([]string{today}, listed...) {
		if a == "" {
			continue
		}
		_, err := db.Exec(`INSERT INTO media_addresses (base) VALUES ($1)
		                   ON CONFLICT (base) DO NOTHING`, a)
		if queryFailed("writing down the address files are handed out at",
			"if it is not on record when the address next changes, files "+
				"saved under it are never cleared from storage. Trying again "+
				"shortly", err) {
			asked = false
		}
	}
	rows, err := db.Query(`SELECT base FROM media_addresses
	                        WHERE base <> $1 ORDER BY first_used, base`, today)
	if queryFailed("reading the earlier addresses files were handed out at",
		"until this works, files saved under an earlier address are not "+
			"cleared from storage when deleted, unless it is listed in "+
			"R2_OLD_PUBLIC_BASE_URLS. Trying again shortly", err) {
		return listed, false
	}
	defer rows.Close()
	out := []string{}
	bad := 0
	for rows.Next() {
		var base string
		if scanFailed("an earlier address files were handed out at",
			rows.Scan(&base), &bad) {
			continue
		}
		out = append(out, base)
	}
	return out, asked
}

// mediaAddressLine is what the server says at start about where files are
// handed out: the line starting "media:" in the log. It is how the owner
// sees that a change of address has taken, and that the old one is still
// known.
//
// [guessed] is for a server with no R2_PUBLIC_BASE_URL at all. It then
// hands files out at a made-up address that Cloudflare does not answer at,
// and nothing else would say so: the uploads succeed and never load.
func mediaAddressLine(today string, guessed bool, earlier []string) string {
	line := "media: new files are handed out at " + today + "."
	if guessed {
		line = "media: R2_PUBLIC_BASE_URL is not set. New files are handed out at a " +
			"guessed address, " + today + ", which Cloudflare does not answer at: " +
			"videos and photos sent from the app will not load. Set it to the " +
			"bucket's public address (Cloudflare, R2, the bucket, Settings)."
	}
	switch len(earlier) {
	case 0:
		line += " No earlier address is on record."
	case 1:
		line += " Files saved under the earlier address are still ours, and " +
			"still cleared from storage when deleted: " + earlier[0] + "."
	default:
		line += fmt.Sprintf(" Files saved under the %d earlier addresses are "+
			"still ours, and still cleared from storage when deleted: %s.",
			len(earlier), strings.Join(earlier, ", "))
	}
	if u, err := url.Parse(today); err == nil && !guessed &&
		strings.HasSuffix(u.Hostname(), ".r2.dev") {
		line += " This is R2's test address: Cloudflare slows it down and " +
			"keeps no copy near the viewer. The before-release list says " +
			"how to use media.flexete.com instead."
	}
	return line
}
