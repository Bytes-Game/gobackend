package main

// Photo and voice messages in chat.
//
// How one is sent:
//
//  1. The app asks POST /api/v1/chat/media for a place to put the file. It
//     gets back a one-off upload address and the address the file will be
//     read from, always inside the sender's own chat folder:
//     chat/<their id>/<random name>/photo.jpg, or .../voice.m4a. Each file
//     has a folder of its own so the cleanup that clears deleted videos
//     (which works folder by folder) can clear it too.
//  2. The app uploads the file straight to storage (Cloudflare R2), the
//     same way videos go.
//  3. The app sends the message as usual, with the kind ("photo" or
//     "voice") and that read address. The server only accepts an address
//     from the sender's own chat folder: nobody can make a message show a
//     picture from somewhere else, or from someone else's folder.
//
// A photo may have a caption (the message text). A voice note has none; it
// carries how long it is and a row of loudness levels the app drew while
// recording, so the bubble can show its shape before anyone presses play.
//
// The file names are long and random, so a file can only be fetched by
// someone who was sent its address.
//
// Everywhere a message is shown as one line — the chat list, a reply's
// quote, the phone notification — a photo reads "📷 Photo" (or its
// caption) and a voice note "🎤 Voice message".

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"path"
	"regexp"
	"strconv"
	"strings"
)

const (
	chatKindText  = "text"
	chatKindPhoto = "photo"
	chatKindVoice = "voice"

	// A voice note is at most two minutes; the app stops recording there.
	// A little over, for the time it takes to stop.
	chatVoiceMaxMs = 125_000
	chatVoiceMinMs = 300

	// The loudness levels drawn on a voice note: at most this many, each
	// 0 to 100.
	chatWaveformMax = 64

	// A photo's size in pixels, so the bubble has the right shape before
	// the picture arrives. The app shrinks photos well below this.
	chatPhotoMaxSide = 10_000
)

// ChatMedia is what a photo or voice message carries besides its text.
type ChatMedia struct {
	Kind       string
	URL        string
	DurationMs int
	Width      int
	Height     int
	Waveform   []int
	// A shared video (kind "share"): which one, and which side of a battle.
	ChallengeID int
	ResponseID  int
}

// chatMediaStorage is where chat files live. Tests swap in a stand-in.
var chatMediaStorage = loadR2Config

// chatMediaFolder is the read address every file in [userID]'s chat folder
// starts with. Empty when storage is not set up.
func chatMediaFolder(userID string) string {
	cfg, err := chatMediaStorage()
	if err != nil || cfg == nil || cfg.PublicBaseURL == "" || userID == "" {
		return ""
	}
	return cfg.PublicURL("chat/" + userID + "/")
}

// chatMediaFile is the file's name, and what the file is, for each kind.
func chatMediaFile(kind string) (name, contentType string, ok bool) {
	switch kind {
	case chatKindPhoto:
		return "photo.jpg", "image/jpeg", true
	case chatKindVoice:
		return "voice.m4a", "audio/mp4", true
	}
	return "", "", false
}

// chatMediaRest is what may follow the sender's folder in a file's address:
// one random folder name, then the file. Nothing else — no further folders,
// no query string.
var chatMediaRest = regexp.MustCompile(`^[0-9a-z]{8,40}/(photo\.jpg|voice\.m4a)$`)

// ChatMediaPresignHandler handles POST /api/v1/chat/media body:{ kind }.
// Answers where to upload the file and where it will be read from.
func ChatMediaPresignHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}
	var body struct {
		Kind string `json:"kind"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid body", http.StatusBadRequest)
		return
	}
	me := authUserID(r)
	if me == "" {
		http.Error(w, "sign in first", http.StatusUnauthorized)
		return
	}
	file, contentType, ok := chatMediaFile(body.Kind)
	if !ok {
		http.Error(w, `kind must be "photo" or "voice"`, http.StatusBadRequest)
		return
	}
	// The same limit as sending: a file is only ever uploaded to be sent.
	if !allowAction(me, "chat") {
		writeRateLimited(w, "chat")
		return
	}
	cfg, err := chatMediaStorage()
	if err != nil {
		http.Error(w, "media storage not configured: "+err.Error(), http.StatusServiceUnavailable)
		return
	}
	key := fmt.Sprintf("chat/%s/%s/%s", me, newUploadID(), file)
	uploadURL, err := cfg.PresignPutURL(key, presignExpiry)
	if err != nil {
		http.Error(w, "failed to sign URL: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"uploadUrl":   uploadURL,
		"publicUrl":   cfg.PublicURL(key),
		"contentType": contentType,
		"expiresIn":   int(presignExpiry.Seconds()),
	})
}

// checkChatMedia makes a message's photo or voice note safe to keep, or
// says why it cannot be. A text message keeps none of the media fields.
func checkChatMedia(senderID string, text string, m ChatMedia) (ChatMedia, error) {
	if m.Kind == "" {
		m.Kind = chatKindText
	}
	switch m.Kind {
	case chatKindText:
		if strings.TrimSpace(text) == "" {
			return m, errors.New("message is required")
		}
		return ChatMedia{Kind: chatKindText}, nil
	case chatKindShare:
		// A note with it is optional. Nothing of a file is kept.
		sid, _ := strconv.Atoi(senderID)
		if err := checkShare(sid, m.ChallengeID, m.ResponseID); err != nil {
			return m, err
		}
		return ChatMedia{Kind: chatKindShare, ChallengeID: m.ChallengeID,
			ResponseID: m.ResponseID}, nil
	case chatKindPhoto, chatKindVoice:
	default:
		return m, fmt.Errorf("unknown message kind %q", m.Kind)
	}
	// What follows the sender's own folder must be exactly one random
	// folder name and the file. An address outside their folder is not
	// shortened by TrimPrefix, so it can never match.
	folder := chatMediaFolder(senderID)
	file, _, _ := chatMediaFile(m.Kind)
	rest := strings.TrimPrefix(m.URL, folder)
	if folder == "" || !chatMediaRest.MatchString(rest) || path.Base(rest) != file {
		return m, errors.New("the file must be one uploaded for this message")
	}
	if m.Kind == chatKindPhoto {
		if m.Width <= 0 || m.Height <= 0 ||
			m.Width > chatPhotoMaxSide || m.Height > chatPhotoMaxSide {
			return m, errors.New("photo size is missing or too large")
		}
		m.DurationMs, m.Waveform = 0, nil
		return m, nil
	}
	if text != "" {
		return m, errors.New("a voice message has no text")
	}
	if m.DurationMs < chatVoiceMinMs || m.DurationMs > chatVoiceMaxMs {
		return m, errors.New("a voice message is up to two minutes long")
	}
	if len(m.Waveform) > chatWaveformMax {
		return m, errors.New("too many waveform levels")
	}
	for _, v := range m.Waveform {
		if v < 0 || v > 100 {
			return m, errors.New("waveform levels are 0 to 100")
		}
	}
	m.Width, m.Height = 0, 0
	return m, nil
}

// setMedia puts a photo or voice note on a message being sent out.
func (m *ChatMessage) setMedia(media ChatMedia) {
	m.Kind = media.Kind
	if m.Kind == "" {
		m.Kind = chatKindText
	}
	m.MediaURL = media.URL
	m.MediaDurationMs = media.DurationMs
	m.MediaWidth = media.Width
	m.MediaHeight = media.Height
	m.Waveform = media.Waveform
	m.sharedChallengeID = media.ChallengeID
	m.sharedResponseID = media.ResponseID
}

// chatPreview is a message as one line: the chat list, a reply's quote, a
// phone notification.
func chatPreview(kind, text string) string {
	switch kind {
	case chatKindPhoto:
		if strings.TrimSpace(text) != "" {
			return "📷 " + text
		}
		return "📷 Photo"
	case chatKindVoice:
		return "🎤 Voice message"
	case chatKindShare:
		if strings.TrimSpace(text) != "" {
			return "🎬 " + text
		}
		return "🎬 Shared a video"
	}
	return text
}

// waveformText stores a voice note's levels as "12,40,7"; waveformFrom
// reads them back. Anything unreadable is left out rather than guessed.
func waveformText(levels []int) string {
	if len(levels) == 0 {
		return ""
	}
	parts := make([]string, len(levels))
	for i, v := range levels {
		parts[i] = strconv.Itoa(v)
	}
	return strings.Join(parts, ",")
}

func waveformFrom(text string) []int {
	if text == "" {
		return nil
	}
	parts := strings.Split(text, ",")
	out := make([]int, 0, len(parts))
	for _, p := range parts {
		v, err := strconv.Atoi(strings.TrimSpace(p))
		if err != nil {
			continue
		}
		out = append(out, v)
	}
	return out
}

// forgetChatFile clears a deleted message's file from storage — unless
// another message still shows it (a forwarded copy points at the same
// file).
func forgetChatFile(url string) {
	if url == "" || db == nil {
		return
	}
	var stillUsed bool
	err := db.QueryRow(`SELECT EXISTS(
		SELECT 1 FROM chat_messages WHERE media_url = $1 AND is_deleted IS NOT TRUE)`,
		url).Scan(&stillUsed)
	if queryFailed("checking whether a deleted message's file is still shown elsewhere",
		"keeping the file, to be safe", err) || stillUsed {
		return
	}
	cfg, err := chatMediaStorage()
	if err != nil || cfg == nil {
		return
	}
	base := strings.TrimRight(cfg.PublicBaseURL, "/") + "/"
	key := strings.TrimPrefix(url, base)
	parts := strings.Split(key, "/")
	if key == url || len(parts) != 4 || parts[0] != "chat" || !chatMediaRest.MatchString(parts[2]+"/"+parts[3]) {
		return
	}
	// The file's own folder: chat/<sender>/<random name>/
	enqueueMediaDeletions([]string{path.Dir(key) + "/"})
}
