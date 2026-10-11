package main

// post_media.go — a post's video and picture must be files its poster
// uploaded to our own storage.
//
// The app uploads straight to storage, then tells the server where the file
// went. The server used to take whatever address it was told, for posts and
// for answers alike. Two things followed from that:
//
//   - The server opens that address itself, to measure the video before
//     accepting it (gateUpload). Any address: one inside the server's own
//     network, which nothing outside should reach, included. Pointing a
//     server at its own private addresses is a classic way in.
//   - Anybody could post a video hosted anywhere — someone else's upload,
//     or a site that logs every phone that plays it — and everybody's feed
//     would load it.
//
// Profile photos (profile_photo.go) and chat files (chat_media.go) were
// already held to "your own upload, in our storage". Posts and answers now
// are too, and the check comes before anything is fetched.

import (
	"regexp"
	"strings"
)

// postMediaStorage is where uploads live. Tests swap in a stand-in.
var postMediaStorage = loadR2Config

// postMediaRest is what may follow u/<id>/ in an upload's address: the one
// upload folder buildObjectKey makes, then one file — a video, a picture,
// or a photo. No further folders, no query string.
var postMediaRest = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}/[A-Za-z0-9_]{1,24}\.(mp4|jpg)$`)

// ownUpload is true when [url] is a file [userID] uploaded to our storage.
// When storage is not set up nothing is, the same way the profile photo
// check fails: an upload could not have reached it anyway.
func ownUpload(userID, url string) bool {
	cfg, err := postMediaStorage()
	if err != nil || cfg == nil || cfg.PublicBaseURL == "" || userID == "" {
		return false
	}
	own := cfg.PublicURL("u/" + userID + "/")
	if !strings.HasPrefix(url, own) {
		return false
	}
	return postMediaRest.MatchString(strings.TrimPrefix(url, own))
}

// postMediaRefusal says what is wrong with a post's or an answer's files,
// in words for the poster, or "" when they are all theirs. The video (or
// photo) must be there; the picture and the other sizes are optional.
func postMediaRefusal(userID, video, thumbnail string, variants VideoVariants) string {
	const notYours = "That upload didn't come through properly. Try posting again."
	if strings.TrimSpace(video) == "" {
		return "" // said more precisely by the checks that follow
	}
	if !ownUpload(userID, video) {
		return notYours
	}
	if thumbnail != "" && !ownUpload(userID, thumbnail) {
		return notYours
	}
	for _, u := range variants {
		if u != "" && !ownUpload(userID, u) {
			return notYours
		}
	}
	return ""
}
