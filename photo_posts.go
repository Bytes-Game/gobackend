package main

// photo_posts.go — challenges made with a photo instead of a video.
//
// "Who looks better", "which is the better meme": some challenges are about
// a picture, not a clip. A photo challenge is posted with one photo, and it
// is answered with a photo — a battle compares like with like, so a photo
// challenge cannot be answered with a video or the other way round.
//
// A photo post is stored exactly where a video post is. Its picture is in
// video_url (the "media" of the post), a smaller copy in thumbnail_url, and
// media_type says which it is. Keeping it in the same column means every
// feed, profile, search, share and battle page carries it with no change of
// its own; the app reads media_type and shows a picture instead of starting
// a player.
//
// What a photo does NOT go through, because each of these is about video:
//
//   - the conversion worker (hlsClaimableWhere): nothing to convert, and the
//     worker would fail on a JPEG five times a day for ever;
//   - the model that judges whether a video matches its challenge, which
//     runs inside that worker — so it never sees a photo;
//   - the upload gate that measures a video's size and length (video_probe.go)
//     and the minimum and maximum length of an answer.
//
// Views, likes, votes, comments, shares, saves and reports work the same as
// for a video: they are about the post, not about what it is made of.

import (
	"strconv"
	"strings"

	"github.com/lib/pq"
)

const (
	mediaVideo = "video"
	mediaPhoto = "photo"
)

// mediaTypeOf reads what the app said a post is made of: "photo", or a video
// for anything else an older app might send (which sends nothing at all).
// The second answer is false for a word that is neither, so a typo is
// refused rather than quietly posted as a video.
func mediaTypeOf(s string) (string, bool) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", mediaVideo:
		return mediaVideo, true
	case mediaPhoto:
		return mediaPhoto, true
	}
	return "", false
}

// photoObjectName is the file a photo upload is kept under — distinct from a
// thumbnail's, which shares its upload.
const photoObjectName = "photo.jpg"

// fillMediaTypes puts media_type on every challenge in [chs] that does not
// already carry it. Every list sent to the app comes through
// markViewerState, which calls this, so a photo is a photo on every screen —
// the feeds build their records without the shared query that selects it.
func fillMediaTypes(chs []*Challenge) {
	if db == nil || len(chs) == 0 {
		return
	}
	byID := map[int64][]*Challenge{}
	ids := make([]int64, 0, len(chs))
	for _, c := range chs {
		if c == nil || c.MediaType != "" {
			continue
		}
		id, err := strconv.ParseInt(c.ID, 10, 64)
		if err != nil {
			continue
		}
		if _, dup := byID[id]; !dup {
			ids = append(ids, id)
		}
		byID[id] = append(byID[id], c)
	}
	if len(ids) == 0 {
		return
	}
	rows, err := db.Query(`
		SELECT id, COALESCE(media_type, 'video') FROM challenges
		 WHERE id = ANY($1::int[])`, pq.Array(ids))
	if queryFailed("reading which posts are photos",
		"they go out marked as videos, and the app tries to play a picture", err) {
		return
	}
	defer rows.Close()
	bad := 0
	for rows.Next() {
		var id int64
		var kind string
		if scanFailed("a post's media type", rows.Scan(&id, &kind), &bad) {
			continue
		}
		for _, c := range byID[id] {
			c.MediaType = kind
		}
	}
	if err := rows.Err(); err != nil {
		queryFailed("reading which posts are photos", "some go out unmarked", err)
	}
}

// answerKindRefusal says why an answer of [answer] media cannot answer a
// challenge of [challenge] media, or "" when it can.
func answerKindRefusal(challenge, answer string) string {
	if challenge == "" {
		challenge = mediaVideo
	}
	if challenge == answer {
		return ""
	}
	if challenge == mediaPhoto {
		return "this is a photo challenge — answer it with a photo"
	}
	return "this is a video challenge — answer it with a video"
}
