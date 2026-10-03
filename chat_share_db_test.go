package main

// Sharing a battle or a short into a chat — against a real database,
// through the same routes the app uses.
//
// A shared video used to arrive as a line of text and a raw link. Now the
// message keeps which video it is, and the video comes back attached so the
// app can draw it as a card.

import (
	"encoding/json"
	"io"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"
)

// shareVideo makes a video by [creator]: a battle with an answer by
// [answerer] when answerer > 0, else a short. Answers the challenge id and
// the answer's id (0 for a short).
func shareVideo(t *testing.T, creator, answerer int, visibility string) (int, int) {
	t.Helper()
	var cid int
	if err := db.QueryRow(`
		INSERT INTO challenges (creator_id, video_url, thumbnail_url, prefix, subject,
		                        visibility, status)
		VALUES ($1, 'https://cdn/c.mp4', 'https://cdn/c.jpg', 'Who can', 'juggle five',
		        $2, 'active')
		RETURNING id`, creator, visibility).Scan(&cid); err != nil {
		t.Fatal(err)
	}
	rid := 0
	if answerer > 0 {
		if err := db.QueryRow(`
			INSERT INTO challenge_responses (challenge_id, responder_id, video_url, thumbnail_url)
			VALUES ($1, $2, 'https://cdn/r.mp4', 'https://cdn/r.jpg')
			RETURNING id`, cid, answerer).Scan(&rid); err != nil {
			t.Fatal(err)
		}
	}
	return cid, rid
}

// shareAs sends a share from [from] to [to], and answers the status and the
// message the sender got back.
func shareAs(t *testing.T, srv *httptest.Server, from, to, cid, rid int, note string) (int, ChatMessage) {
	t.Helper()
	body := `{"receiverId":"` + strconv.Itoa(to) + `","kind":"share","challengeId":"` +
		strconv.Itoa(cid) + `","message":` + strconv.Quote(note)
	if rid > 0 {
		body += `,"responseId":"` + strconv.Itoa(rid) + `"`
	}
	res := authedDo(t, srv, from, "POST", "/api/v1/chat/send", body+`}`)
	raw, _ := io.ReadAll(res.Body)
	var msg ChatMessage
	_ = json.Unmarshal(raw, &msg)
	return res.StatusCode, msg
}

// chatAs reads the chat [me] has with [other], as [me].
func chatAs(t *testing.T, srv *httptest.Server, me, other int) []ChatMessage {
	t.Helper()
	res := authedDo(t, srv, me, "GET",
		"/api/v1/chat/messages/"+strconv.Itoa(me)+"/"+strconv.Itoa(other), "")
	var out []ChatMessage
	if err := json.NewDecoder(res.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestShare_ABattleArrivesAsTheVideoNotALink(t *testing.T) {
	srv := liveSetup(t)
	cid, rid := shareVideo(t, liveSam, liveLeo, "arena")
	maya := dial(t, srv, liveMaya)

	code, sent := shareAs(t, srv, liveLeo, liveMaya, cid, rid, "")
	if code != 200 {
		t.Fatalf("sharing = %d", code)
	}
	if sent.Kind != "share" || sent.Shared == nil || sent.Shared.Challenge == nil {
		t.Fatalf("the sender's copy carries no video: %+v", sent)
	}

	// Live, to her phone: the video itself, not a link.
	ev := next(t, maya, "chat", 3*time.Second)
	shared, _ := ev["shared"].(map[string]interface{})
	ch, _ := shared["challenge"].(map[string]interface{})
	if ev["kind"] != "share" || ch["id"] != strconv.Itoa(cid) {
		t.Fatalf("the live message is not the shared video: %v", ev)
	}
	if shared["responseId"] != strconv.Itoa(rid) {
		t.Fatalf("which side was shared is lost: %v", shared)
	}

	// And when she opens the chat: the battle, with both sides.
	msgs := chatAs(t, srv, liveMaya, liveLeo)
	if len(msgs) != 1 || msgs[0].Shared == nil || msgs[0].Shared.Challenge == nil {
		t.Fatalf("the chat has no shared video: %+v", msgs)
	}
	got := msgs[0].Shared.Challenge
	if got.ID != strconv.Itoa(cid) || got.CreatorUsername != "sam" ||
		got.ThumbnailURL != "https://cdn/c.jpg" || got.TopResponseVideoUrl != "https://cdn/r.mp4" {
		t.Fatalf("the shared battle is missing what the card shows: %+v", got)
	}

	// One line, where one line is shown.
	var last string
	if err := db.QueryRow(`SELECT kind FROM chat_messages WHERE id = $1`, sent.ID).
		Scan(&last); err != nil || last != "share" {
		t.Fatalf("stored as %q (%v)", last, err)
	}
	if got := chatPreview("share", ""); got != "🎬 Shared a video" {
		t.Fatalf("preview = %q", got)
	}
	for _, c := range GetConversations(liveMaya) {
		if c.Username == "leo" && c.LastMessage != "🎬 Shared a video" {
			t.Fatalf("the chat list shows %q", c.LastMessage)
		}
	}
}

func TestShare_ANoteGoesWithIt(t *testing.T) {
	srv := liveSetup(t)
	cid, _ := shareVideo(t, liveSam, 0, "arena")
	code, sent := shareAs(t, srv, liveLeo, liveMaya, cid, 0, "look at this")
	if code != 200 || sent.Message != "look at this" || sent.Shared == nil {
		t.Fatalf("sharing with a note = %d %+v", code, sent)
	}
	if got := chatPreview("share", "look at this"); got != "🎬 look at this" {
		t.Fatalf("preview = %q", got)
	}
}

func TestShare_YouCanOnlyShareAVideoYouCanWatch(t *testing.T) {
	srv := liveSetup(t)
	// Sam's friends-only video, for his followers; leo does not follow him.
	cid, _ := shareVideo(t, liveSam, 0, "friends")
	if code, _ := shareAs(t, srv, liveLeo, liveMaya, cid, 0, ""); code != 400 {
		t.Fatalf("leo sharing a video he cannot watch = %d, want 400", code)
	}
	// Nor a video that does not exist, nor an answer from another battle.
	if code, _ := shareAs(t, srv, liveLeo, liveMaya, 99999999, 0, ""); code != 400 {
		t.Fatalf("sharing a missing video = %d, want 400", code)
	}
	other, otherAnswer := shareVideo(t, liveSam, liveMaya, "arena")
	mine, _ := shareVideo(t, liveSam, 0, "arena")
	_ = other
	if code, _ := shareAs(t, srv, liveLeo, liveMaya, mine, otherAnswer, ""); code != 400 {
		t.Fatalf("sharing an answer from another battle = %d, want 400", code)
	}
}

func TestShare_AFriendsOnlyVideoIsUnavailableToSomeoneItIsNotFor(t *testing.T) {
	srv := liveSetup(t)
	// Leo follows sam, so he can watch and share sam's friends-only video.
	if _, err := db.Exec(`INSERT INTO follows (follower_id, following_id) VALUES ($1, $2)`,
		liveLeo, liveSam); err != nil {
		t.Fatal(err)
	}
	cid, _ := shareVideo(t, liveSam, 0, "friends")
	code, sent := shareAs(t, srv, liveLeo, liveMaya, cid, 0, "")
	if code != 200 || sent.Shared == nil || sent.Shared.Challenge == nil {
		t.Fatalf("leo, who may watch it, sharing = %d %+v", code, sent)
	}
	// Maya does not follow sam: she sees that something was shared, not
	// what.
	msgs := chatAs(t, srv, liveMaya, liveLeo)
	if len(msgs) != 1 || msgs[0].Shared == nil {
		t.Fatalf("maya's chat = %+v", msgs)
	}
	if msgs[0].Shared.Challenge != nil {
		t.Fatalf("maya is shown a friends-only video that is not for her: %+v",
			msgs[0].Shared.Challenge)
	}
	// Leo still sees it in his own copy of the chat.
	mine := chatAs(t, srv, liveLeo, liveMaya)
	if len(mine) != 1 || mine[0].Shared == nil || mine[0].Shared.Challenge == nil {
		t.Fatalf("leo can no longer see the video he shared: %+v", mine)
	}
}

func TestShare_ADeletedVideoIsUnavailable(t *testing.T) {
	srv := liveSetup(t)
	cid, _ := shareVideo(t, liveSam, 0, "arena")
	if code, _ := shareAs(t, srv, liveLeo, liveMaya, cid, 0, ""); code != 200 {
		t.Fatalf("sharing = %d", code)
	}
	if _, err := db.Exec(`DELETE FROM challenges WHERE id = $1`, cid); err != nil {
		t.Fatal(err)
	}
	msgs := chatAs(t, srv, liveMaya, liveLeo)
	if len(msgs) != 1 || msgs[0].Shared == nil || msgs[0].Shared.Challenge != nil {
		t.Fatalf("a deleted video still shows: %+v", msgs)
	}
}

func TestShare_ForwardedItIsStillTheVideo(t *testing.T) {
	srv := liveSetup(t)
	cid, _ := shareVideo(t, liveSam, 0, "arena")
	_, sent := shareAs(t, srv, liveLeo, liveMaya, cid, 0, "")
	res := authedDo(t, srv, liveMaya, "POST", "/api/v1/chat/forward",
		`{"messageId":"`+sent.ID+`","receiverId":"`+strconv.Itoa(liveSam)+`"}`)
	if res.StatusCode != 200 {
		t.Fatalf("forwarding = %d", res.StatusCode)
	}
	msgs := chatAs(t, srv, liveSam, liveMaya)
	if len(msgs) != 1 || msgs[0].Kind != "share" || msgs[0].Shared == nil ||
		msgs[0].Shared.Challenge == nil || msgs[0].Shared.Challenge.ID != strconv.Itoa(cid) {
		t.Fatalf("the forwarded copy is not the video: %+v", msgs)
	}
}

func TestShare_DeletingTheMessageLetsGoOfTheVideo(t *testing.T) {
	srv := liveSetup(t)
	cid, _ := shareVideo(t, liveSam, 0, "arena")
	_, sent := shareAs(t, srv, liveLeo, liveMaya, cid, 0, "")
	res := authedDo(t, srv, liveLeo, "POST", "/api/v1/chat/delete",
		`{"messageId":"`+sent.ID+`"}`)
	if res.StatusCode != 200 {
		t.Fatalf("deleting = %d", res.StatusCode)
	}
	msgs := chatAs(t, srv, liveMaya, liveLeo)
	if len(msgs) != 1 || msgs[0].Shared != nil || !msgs[0].IsDeleted {
		t.Fatalf("a deleted share still shows its video: %+v", msgs)
	}
}
