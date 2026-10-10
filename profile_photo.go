package main

// profile_photo.go — a person's profile photo, the word they pick for what
// their profile is about, and handing the app many people's photos at once.
//
// ════════════════════════════════════════════════════════════════════════
// WHERE A PHOTO COMES FROM
// ════════════════════════════════════════════════════════════════════════
//
// The app uploads it the way it uploads a photo post: it asks /media/presign
// for a "photo", puts the picture there, and then sets the address it was
// given as the photo with PATCH /users/{id}. So a profile photo is always
// u/<their own id>/<upload>/photo.jpg on our own storage.
//
// That is checked when it is set (validProfilePhoto). Taken as sent, anyone
// could point their photo at any address on the internet — a picture that
// changes after it is approved, or a tracking pixel every viewer's phone
// would fetch — or at somebody else's upload.
//
// ════════════════════════════════════════════════════════════════════════
// WHY THE APP ASKS FOR PHOTOS BY NAME
// ════════════════════════════════════════════════════════════════════════
//
// A person appears in thirty places in the app — chats, comments, search,
// the reel, the lists of who liked and who voted — and nearly all of them
// know only a username. Rather than add a photo to every one of those
// answers, the app keeps its own book of photos by username and asks for
// the ones it does not know, many at once (UserAvatarsHandler). One
// question, a hundred names, a few bytes each.

import (
	"net/http"
	"regexp"
	"strings"

	"github.com/lib/pq"
)

// profileTags are the words a profile can be described by. The app shows
// the same list (lib/config/profile_tags.dart); keep the two in step, or
// the app offers a word the server refuses.
var profileTags = []string{
	"Creator",
	"Influencer",
	"Motivator",
	"Philanthropist",
	"Entertainer",
	"Comedian",
	"Dancer",
	"Singer",
	"Musician",
	"Rapper",
	"Artist",
	"Actor",
	"Photographer",
	"Vlogger",
	"Storyteller",
	"Gamer",
	"Athlete",
	"Fitness coach",
	"Educator",
	"Student",
	"Tech enthusiast",
	"Entrepreneur",
	"Foodie",
	"Chef",
	"Traveller",
	"Fashion",
	"Beauty",
	"Lifestyle",
	"Spiritual",
	"Activist",
	"Pet lover",
}

// validProfileTag is true for one of profileTags, or "" (no tag).
func validProfileTag(tag string) bool {
	if tag == "" {
		return true
	}
	for _, t := range profileTags {
		if t == tag {
			return true
		}
	}
	return false
}

// profilePhotoStorage is where uploads live. Tests swap in a stand-in.
var profilePhotoStorage = loadR2Config

// profilePhotoRest is what may follow u/<id>/ in a photo's address: the one
// upload folder buildObjectKey makes, then the photo. No further folders,
// no query string.
var profilePhotoRest = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}/` +
	regexp.QuoteMeta(photoObjectName) + `$`)

// validProfilePhoto is true when [url] is "" (no photo) or a photo that
// [userID] uploaded to our own storage.
func validProfilePhoto(userID, url string) bool {
	if url == "" {
		return true
	}
	cfg, err := profilePhotoStorage()
	if err != nil || cfg == nil || cfg.PublicBaseURL == "" || userID == "" {
		return false
	}
	own := cfg.PublicURL("u/" + userID + "/")
	if !strings.HasPrefix(url, own) {
		return false
	}
	return profilePhotoRest.MatchString(strings.TrimPrefix(url, own))
}

// avatarsMaxNames caps one question. The app asks for the names on screen,
// which is a few dozen at most.
const avatarsMaxNames = 100

// UserAvatarsHandler — GET /api/v1/users/avatars?names=a,b,c
//
// Each named person's photo address, "" for someone with no photo. A name
// with no account is left out, which the app also reads as "no photo".
func UserAvatarsHandler(w http.ResponseWriter, r *http.Request) {
	seen := map[string]bool{}
	names := []string{}
	for _, n := range strings.Split(r.URL.Query().Get("names"), ",") {
		n = strings.TrimSpace(n)
		if n == "" || seen[n] {
			continue
		}
		seen[n] = true
		names = append(names, n)
		if len(names) == avatarsMaxNames {
			break
		}
	}
	avatars := map[string]string{}
	if len(names) == 0 {
		writeJSON(w, http.StatusOK, map[string]any{"avatars": avatars})
		return
	}
	rows, err := db.Query(
		`SELECT username, avatar_url FROM users WHERE username = ANY($1)`,
		pq.Array(names))
	if queryFailed("reading people's profile photos", "answering with an error", err) {
		http.Error(w, "could not read the photos", http.StatusInternalServerError)
		return
	}
	defer rows.Close()
	bad := 0
	for rows.Next() {
		var name, url string
		if scanFailed("a profile photo", rows.Scan(&name, &url), &bad) {
			continue
		}
		avatars[name] = url
	}
	writeJSON(w, http.StatusOK, map[string]any{"avatars": avatars})
}

// ProfileTagsHandler — GET /api/v1/profile/tags
//
// The words a profile may be described by, in the order the app shows them.
func ProfileTagsHandler(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"tags": profileTags})
}
