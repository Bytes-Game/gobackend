package main

// Photo and voice messages, through the real server and a real database.
// Only storage is a stand-in: it signs upload addresses the same way, but
// nothing is uploaded.

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
)

const testMediaBase = "https://media.test"

func withChatStorage(t *testing.T) {
	t.Helper()
	before := chatMediaStorage
	chatMediaStorage = func() (*R2Config, error) {
		return &R2Config{AccountID: "acc", Bucket: "bucket", AccessKeyID: "key",
			SecretAccessKey: "secret", PublicBaseURL: testMediaBase}, nil
	}
	t.Cleanup(func() { chatMediaStorage = before })
	if _, err := db.Exec(`DELETE FROM pending_media_deletions WHERE object_prefix LIKE 'chat/%'`); err != nil {
		t.Fatal(err)
	}
}

// freshChatLimit fills the sending limit again: these tests send more than
// a person would in a few seconds.
func freshChatLimit() {
	actionLimitersMu.Lock()
	delete(actionLimiters, "chat")
	actionLimitersMu.Unlock()
}

func jsonOf(t *testing.T, res *http.Response) map[string]interface{} {
	t.Helper()
	b, _ := io.ReadAll(res.Body)
	var out map[string]interface{}
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("answer %d %q is not JSON: %v", res.StatusCode, b, err)
	}
	return out
}

// slot asks for somewhere to upload, as [id].
func slot(t *testing.T, srv *httptest.Server, id int, kind string) map[string]interface{} {
	t.Helper()
	res := authedDo(t, srv, id, "POST", "/api/v1/chat/media", `{"kind":"`+kind+`"}`)
	if res.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(res.Body)
		t.Fatalf("upload slot for a %s: %d %s", kind, res.StatusCode, b)
	}
	return jsonOf(t, res)
}

func sendJSON(t *testing.T, srv *httptest.Server, from, to int, fields string) *http.Response {
	t.Helper()
	freshChatLimit()
	return authedDo(t, srv, from, "POST", "/api/v1/chat/send",
		`{"receiverId":"`+strconv.Itoa(to)+`",`+fields+`}`)
}

func chatBetween(t *testing.T, srv *httptest.Server, me, other int) []map[string]interface{} {
	t.Helper()
	res := authedDo(t, srv, me, "GET",
		fmt.Sprintf("/api/v1/chat/messages/%d/%d", me, other), "")
	out := []map[string]interface{}{}
	if err := json.NewDecoder(res.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	return out
}

func lastMessageFor(t *testing.T, srv *httptest.Server, me, other int) string {
	t.Helper()
	res := authedDo(t, srv, me, "GET", fmt.Sprintf("/api/v1/chat/conversations/%d", me), "")
	var list []Conversation
	if err := json.NewDecoder(res.Body).Decode(&list); err != nil {
		t.Fatal(err)
	}
	for _, c := range list {
		if c.UserID == strconv.Itoa(other) {
			return c.LastMessage
		}
	}
	t.Fatalf("no chat with %d on %d's list: %+v", other, me, list)
	return ""
}

func TestChatMedia_AnUploadSlotIsInTheSendersOwnFolder(t *testing.T) {
	srv := liveSetup(t)
	withChatStorage(t)
	photo := slot(t, srv, liveLeo, "photo")
	folder := fmt.Sprintf("%s/chat/%d/", testMediaBase, liveLeo)
	if url := photo["publicUrl"].(string); !strings.HasPrefix(url, folder) ||
		!strings.HasSuffix(url, "/photo.jpg") {
		t.Fatalf("a photo goes to %s, not leo's folder %s", url, folder)
	}
	if photo["contentType"] != "image/jpeg" ||
		!strings.Contains(photo["uploadUrl"].(string), "X-Amz-Signature=") {
		t.Fatalf("photo slot %v", photo)
	}
	voice := slot(t, srv, liveLeo, "voice")
	if url := voice["publicUrl"].(string); !strings.HasSuffix(url, "/voice.m4a") ||
		voice["contentType"] != "audio/mp4" {
		t.Fatalf("voice slot %v", voice)
	}
	if voice["publicUrl"] == photo["publicUrl"] {
		t.Fatal("two uploads were given the same place")
	}
	res := authedDo(t, srv, liveLeo, "POST", "/api/v1/chat/media", `{"kind":"video"}`)
	if res.StatusCode != http.StatusBadRequest {
		t.Fatalf("a video slot from chat answered %d", res.StatusCode)
	}
}

func TestChatMedia_APhotoArrivesWithItsCaptionEverywhere(t *testing.T) {
	srv := liveSetup(t)
	withChatStorage(t)
	p := withPusher(t)
	phone(t, liveMaya, "maya-phone")
	maya := dial(t, srv, liveMaya)
	url := slot(t, srv, liveLeo, "photo")["publicUrl"].(string)

	res := sendJSON(t, srv, liveLeo, liveMaya,
		`"kind":"photo","mediaUrl":"`+url+`","mediaWidth":1080,"mediaHeight":1350,"message":"look"`)
	if res.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(res.Body)
		t.Fatalf("send photo: %d %s", res.StatusCode, b)
	}
	sent := jsonOf(t, res)
	if sent["kind"] != "photo" || sent["mediaUrl"] != url {
		t.Fatalf("the answer to the sender %v", sent)
	}

	live := next(t, maya, "chat", 3*time.Second)
	if live["kind"] != "photo" || live["mediaUrl"] != url ||
		live["mediaWidth"] != float64(1080) || live["message"] != "look" {
		t.Fatalf("maya's live message %v", live)
	}
	got := chatBetween(t, srv, liveMaya, liveLeo)
	if len(got) != 1 || got[0]["kind"] != "photo" || got[0]["mediaUrl"] != url ||
		got[0]["mediaWidth"] != float64(1080) || got[0]["mediaHeight"] != float64(1350) ||
		got[0]["message"] != "look" {
		t.Fatalf("the chat shows %v", got)
	}
	if last := lastMessageFor(t, srv, liveMaya, liveLeo); last != "📷 look" {
		t.Fatalf("the chat list reads %q", last)
	}
	if push := p.waitFor(t, 1); push.row.Body != "📷 look" || push.row.Data["kind"] != "photo" {
		t.Fatalf("the notification reads %q (%v)", push.row.Body, push.row.Data)
	}

	// Without a caption it reads "Photo".
	url2 := slot(t, srv, liveLeo, "photo")["publicUrl"].(string)
	sendJSON(t, srv, liveLeo, liveMaya, `"kind":"photo","mediaUrl":"`+url2+`","mediaWidth":800,"mediaHeight":600`)
	if last := lastMessageFor(t, srv, liveMaya, liveLeo); last != "📷 Photo" {
		t.Fatalf("a photo with no caption reads %q", last)
	}
}

func TestChatMedia_AVoiceNoteKeepsItsLengthAndShape(t *testing.T) {
	srv := liveSetup(t)
	withChatStorage(t)
	p := withPusher(t)
	phone(t, liveMaya, "maya-phone")
	url := slot(t, srv, liveLeo, "voice")["publicUrl"].(string)
	res := sendJSON(t, srv, liveLeo, liveMaya,
		`"kind":"voice","mediaUrl":"`+url+`","mediaDurationMs":4200,"waveform":[10,50,90]`)
	if res.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(res.Body)
		t.Fatalf("send voice: %d %s", res.StatusCode, b)
	}
	got := chatBetween(t, srv, liveMaya, liveLeo)
	if len(got) != 1 || got[0]["kind"] != "voice" || got[0]["mediaUrl"] != url ||
		got[0]["mediaDurationMs"] != float64(4200) {
		t.Fatalf("the chat shows %v", got)
	}
	if w, _ := got[0]["waveform"].([]interface{}); len(w) != 3 || w[2] != float64(90) {
		t.Fatalf("the voice note's shape came back as %v", got[0]["waveform"])
	}
	if last := lastMessageFor(t, srv, liveMaya, liveLeo); last != "🎤 Voice message" {
		t.Fatalf("the chat list reads %q", last)
	}
	if push := p.waitFor(t, 1); push.row.Body != "🎤 Voice message" {
		t.Fatalf("the notification reads %q", push.row.Body)
	}
	// A voice note cannot be edited into text.
	id := got[0]["id"].(string)
	res = authedDo(t, srv, liveLeo, "POST", "/api/v1/chat/edit", `{"messageId":"`+id+`","text":"hi"}`)
	if res.StatusCode != http.StatusForbidden {
		t.Fatalf("editing a voice note answered %d", res.StatusCode)
	}
}

func TestChatMedia_OnlyAFileOfYourOwnIsAccepted(t *testing.T) {
	srv := liveSetup(t)
	withChatStorage(t)
	photo := slot(t, srv, liveLeo, "photo")["publicUrl"].(string)
	voice := slot(t, srv, liveLeo, "voice")["publicUrl"].(string)
	mayas := slot(t, srv, liveMaya, "photo")["publicUrl"].(string)
	size := `,"mediaWidth":100,"mediaHeight":100`
	for name, fields := range map[string]string{
		"someone else's photo":     `"kind":"photo","mediaUrl":"` + mayas + `"` + size,
		"a picture from elsewhere": `"kind":"photo","mediaUrl":"https://evil.example/x/photo.jpg"` + size,
		"a voice file as a photo":  `"kind":"photo","mediaUrl":"` + voice + `"` + size,
		"a deeper path":            `"kind":"photo","mediaUrl":"` + strings.TrimSuffix(photo, "photo.jpg") + `a/photo.jpg"` + size,
		"an address with a query":  `"kind":"photo","mediaUrl":"` + photo + `?x=1"` + size,
		"a photo with no size":     `"kind":"photo","mediaUrl":"` + photo + `"`,
		"a voice note too long":    `"kind":"voice","mediaUrl":"` + voice + `","mediaDurationMs":200000`,
		"a voice note with text":   `"kind":"voice","mediaUrl":"` + voice + `","mediaDurationMs":2000,"message":"hi"`,
		"a level over 100":         `"kind":"voice","mediaUrl":"` + voice + `","mediaDurationMs":2000,"waveform":[101]`,
		"an empty text":            `"message":"   "`,
		"a kind nobody knows":      `"kind":"sticker","mediaUrl":"` + photo + `"`,
	} {
		if res := sendJSON(t, srv, liveLeo, liveMaya, fields); res.StatusCode != http.StatusBadRequest {
			t.Errorf("%s: answered %d", name, res.StatusCode)
		}
	}
	if got := chatBetween(t, srv, liveMaya, liveLeo); len(got) != 0 {
		t.Fatalf("refused messages were kept: %v", got)
	}
	// A text that names a file keeps only its words.
	sendJSON(t, srv, liveLeo, liveMaya, `"message":"hi","mediaUrl":"`+photo+`"`)
	got := chatBetween(t, srv, liveMaya, liveLeo)
	if len(got) != 1 || got[0]["kind"] != "text" || got[0]["mediaUrl"] != nil {
		t.Fatalf("a text message kept a file: %v", got)
	}
	// And the real ones are accepted (so the refusals above are not
	// refusing everything).
	if res := sendJSON(t, srv, liveLeo, liveMaya, `"kind":"photo","mediaUrl":"`+photo+`"`+size); res.StatusCode != http.StatusOK {
		t.Fatalf("leo's own photo answered %d", res.StatusCode)
	}
	if res := sendJSON(t, srv, liveLeo, liveMaya, `"kind":"voice","mediaUrl":"`+voice+`","mediaDurationMs":2000`); res.StatusCode != http.StatusOK {
		t.Fatalf("leo's own voice note answered %d", res.StatusCode)
	}
}

func TestChatMedia_RepliesQuoteItAndForwardsCarryIt(t *testing.T) {
	srv := liveSetup(t)
	withChatStorage(t)
	photo := slot(t, srv, liveLeo, "photo")["publicUrl"].(string)
	sendJSON(t, srv, liveLeo, liveMaya, `"kind":"photo","mediaUrl":"`+photo+`","mediaWidth":10,"mediaHeight":10`)
	photoID := chatBetween(t, srv, liveMaya, liveLeo)[0]["id"].(string)
	sendJSON(t, srv, liveMaya, liveLeo, `"message":"nice","replyToId":"`+photoID+`"`)
	got := chatBetween(t, srv, liveMaya, liveLeo)
	if got[0]["replyToText"] != "📷 Photo" {
		t.Fatalf("a reply to a photo quotes %q", got[0]["replyToText"])
	}

	voice := slot(t, srv, liveLeo, "voice")["publicUrl"].(string)
	sendJSON(t, srv, liveLeo, liveMaya, `"kind":"voice","mediaUrl":"`+voice+`","mediaDurationMs":3000,"waveform":[5,6]`)
	voiceID := chatBetween(t, srv, liveMaya, liveLeo)[0]["id"].(string)
	freshChatLimit()
	res := authedDo(t, srv, liveMaya, "POST", "/api/v1/chat/forward",
		`{"messageId":"`+voiceID+`","receiverId":"`+strconv.Itoa(liveSam)+`"}`)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("forward answered %d", res.StatusCode)
	}
	fw := chatBetween(t, srv, liveSam, liveMaya)
	if len(fw) != 1 || fw[0]["kind"] != "voice" || fw[0]["mediaUrl"] != voice ||
		fw[0]["mediaDurationMs"] != float64(3000) {
		t.Fatalf("sam got %v", fw)
	}
}

func queuedChatDeletions(t *testing.T) []string {
	t.Helper()
	rows, err := db.Query(`SELECT object_prefix FROM pending_media_deletions
		WHERE object_prefix LIKE 'chat/%' ORDER BY object_prefix`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var p string
		if err := rows.Scan(&p); err != nil {
			t.Fatal(err)
		}
		out = append(out, p)
	}
	return out
}

func TestChatMedia_UnsendingAPhotoTakesTheFileToo(t *testing.T) {
	srv := liveSetup(t)
	withChatStorage(t)
	url := slot(t, srv, liveLeo, "photo")["publicUrl"].(string)
	sendJSON(t, srv, liveLeo, liveMaya, `"kind":"photo","mediaUrl":"`+url+`","mediaWidth":10,"mediaHeight":10`)
	id := chatBetween(t, srv, liveMaya, liveLeo)[0]["id"].(string)

	res := authedDo(t, srv, liveLeo, "POST", "/api/v1/chat/delete", `{"messageId":"`+id+`"}`)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("delete answered %d", res.StatusCode)
	}
	got := chatBetween(t, srv, liveMaya, liveLeo)
	if got[0]["kind"] != "text" || got[0]["mediaUrl"] != nil ||
		got[0]["message"] != "This message was deleted" {
		t.Fatalf("an unsent photo still shows: %v", got[0])
	}
	want := strings.TrimSuffix(strings.TrimPrefix(url, testMediaBase+"/"), "photo.jpg")
	var queued []string
	for i := 0; i < 40; i++ {
		if queued = queuedChatDeletions(t); len(queued) > 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if len(queued) != 1 || queued[0] != want {
		t.Fatalf("queued for clearing %v, want [%s]", queued, want)
	}
}

func TestChatMedia_AForwardedCopyKeepsTheFile(t *testing.T) {
	srv := liveSetup(t)
	withChatStorage(t)
	url := slot(t, srv, liveLeo, "photo")["publicUrl"].(string)
	sendJSON(t, srv, liveLeo, liveMaya, `"kind":"photo","mediaUrl":"`+url+`","mediaWidth":10,"mediaHeight":10`)
	id := chatBetween(t, srv, liveMaya, liveLeo)[0]["id"].(string)
	freshChatLimit()
	authedDo(t, srv, liveMaya, "POST", "/api/v1/chat/forward",
		`{"messageId":"`+id+`","receiverId":"`+strconv.Itoa(liveSam)+`"}`)

	authedDo(t, srv, liveLeo, "POST", "/api/v1/chat/delete", `{"messageId":"`+id+`"}`)
	time.Sleep(500 * time.Millisecond)
	if q := queuedChatDeletions(t); len(q) != 0 {
		t.Fatalf("the file was queued for clearing while sam's copy still shows it: %v", q)
	}
	if fw := chatBetween(t, srv, liveSam, liveMaya); len(fw) != 1 || fw[0]["mediaUrl"] != url {
		t.Fatalf("sam's copy %v", fw)
	}
}
