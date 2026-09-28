package main

// inbox.go — the notifications list: what the bell in the app shows.
//
// ════════════════════════════════════════════════════════════════════════
// THE LIST USED TO FORGET EVERYTHING
// ════════════════════════════════════════════════════════════════════════
//
// A notification went down the socket if the person was online, or into a
// Redis list if not, and that list was emptied the moment they connected.
// The app kept what arrived in memory. So the notifications page showed
// only what had come in since the app last opened — close it, and the page
// was empty again, however much had happened.
//
// Every notification is now a row, per person, with who it is from and
// which challenge it is about. The page reads the rows; the socket still
// delivers them live, marked with the same id so the app can tell a live
// one from one it already has.
//
// ════════════════════════════════════════════════════════════════════════
// WHAT IS NOT A NOTIFICATION
// ════════════════════════════════════════════════════════════════════════
//
// Votes. Each vote used to tell the person voted for who voted, which on a
// live battle is a stream of pings nobody asked for. Who voted is a list on
// the battle, for its two players to look at when they want to — see
// voters.go.

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Kinds of notification, as the app reads them.
const (
	noteFollow          = "follow"
	noteFriendChallenge = "friend_challenge"
	noteAccepted        = "challenge_accepted"
	noteBattleStarted   = "battle_started"
	noteBattleWon       = "battle_won"
)

// TriggerBattleWon is the phone push for "you won your battle". Sent under
// the battle-updates setting, the same one as "your battle ends soon".
const TriggerBattleWon TriggerKind = "battle_won"

// InboxNote is one notification to one person.
type InboxNote struct {
	UserID int
	// Recipient's username, for the live socket. Looked up when empty.
	Username    string
	Kind        string
	ActorID     int
	ActorName   string
	ChallengeID int
	// The rest of the sentence after the actor's name: "started following
	// you". The app sets the name in bold in front of it.
	Body string
}

// InboxItem is a notification as the app receives it, from the list or
// live down the socket.
type InboxItem struct {
	ID             string `json:"id"`
	Type           string `json:"type"`
	Message        string `json:"message"` // the whole sentence, for older apps
	Text           string `json:"text"`    // the sentence without the name
	Timestamp      string `json:"timestamp"`
	Read           bool   `json:"read"`
	ActorID        string `json:"actorId,omitempty"`
	ActorUsername  string `json:"actorUsername,omitempty"`
	ActorLeague    string `json:"actorLeague,omitempty"`
	ChallengeID    string `json:"challengeId,omitempty"`
	ChallengeTitle string `json:"challengeTitle,omitempty"`
	ThumbnailURL   string `json:"thumbnailUrl,omitempty"`
}

func sentence(actor, body string) string {
	if actor == "" {
		return body
	}
	return actor + " " + body
}

// notifyUser keeps [n] in the person's list and sends it to them live if
// they are online. Never fails the action that caused it: a notification
// that could not be kept says so in the log and the action goes on.
func notifyUser(n InboxNote) {
	if db == nil || n.UserID <= 0 || n.UserID == n.ActorID {
		return
	}
	var actor, challenge any
	if n.ActorID > 0 {
		actor = n.ActorID
	}
	if n.ChallengeID > 0 {
		challenge = n.ChallengeID
	}
	var id int64
	var at time.Time
	err := db.QueryRow(`
		INSERT INTO user_notifications (user_id, kind, actor_id, challenge_id, body)
		VALUES ($1, $2, $3, $4, $5)
		RETURNING id, created_at`,
		n.UserID, n.Kind, actor, challenge, n.Body).Scan(&id, &at)
	if queryFailed("keeping a notification for user "+strconv.Itoa(n.UserID),
		"it goes out live if they are online, but will not be in their list", err) {
		at = time.Now()
	}

	username := n.Username
	if username == "" {
		if u, ok := GetUserByID(strconv.Itoa(n.UserID)); ok {
			username = u.Username
		}
	}
	if username == "" {
		return
	}
	item := InboxItem{
		Type:          n.Kind,
		Message:       sentence(n.ActorName, n.Body),
		Text:          n.Body,
		Timestamp:     at.UTC().Format(time.RFC3339),
		ActorUsername: n.ActorName,
	}
	if id > 0 {
		item.ID = strconv.FormatInt(id, 10)
	}
	if n.ActorID > 0 {
		item.ActorID = strconv.Itoa(n.ActorID)
	}
	if n.ChallengeID > 0 {
		item.ChallengeID = strconv.Itoa(n.ChallengeID)
	}
	data, err := json.Marshal(item)
	if err != nil {
		return
	}
	wsDeliver(username, data)
}

// challengeTitle is how a challenge is quoted in a notification.
func challengeTitle(prefix, subject string) string {
	t := strings.TrimSpace(prefix + " " + subject)
	if t != "" && !strings.HasSuffix(t, "?") {
		t += "?"
	}
	return truncateText(t, 80)
}

// GET /api/v1/notifications — the signed-in person's list, newest first,
// with how many they have not seen.
func ListNotificationsHandler(w http.ResponseWriter, r *http.Request) {
	uid, err := strconv.Atoi(authUserID(r))
	if err != nil || uid <= 0 {
		http.Error(w, "sign in to see notifications", http.StatusUnauthorized)
		return
	}
	limit := parseIntOrDefault(r.URL.Query().Get("limit"), 60, 200)
	items := []InboxItem{}
	rows, err := db.Query(`
		SELECT n.id, n.kind, n.body, n.created_at, n.read_at IS NOT NULL,
		       COALESCE(n.actor_id::text, ''), COALESCE(a.username, ''),
		       COALESCE(a.league, ''),
		       COALESCE(n.challenge_id::text, ''),
		       COALESCE(c.prefix, ''), COALESCE(c.subject, ''),
		       COALESCE(c.thumbnail_url, '')
		  FROM user_notifications n
		  LEFT JOIN users a ON a.id = n.actor_id
		  LEFT JOIN challenges c ON c.id = n.challenge_id
		 WHERE n.user_id = $1
		 ORDER BY n.created_at DESC, n.id DESC
		 LIMIT $2`, uid, limit)
	if queryFailed("reading the notifications list", "answering with an error", err) {
		http.Error(w, "could not read notifications", http.StatusInternalServerError)
		return
	}
	defer rows.Close()
	bad := 0
	for rows.Next() {
		var it InboxItem
		var id int64
		var at time.Time
		var prefix, subject string
		if scanFailed("a notification", rows.Scan(&id, &it.Type, &it.Text, &at,
			&it.Read, &it.ActorID, &it.ActorUsername, &it.ActorLeague,
			&it.ChallengeID, &prefix, &subject, &it.ThumbnailURL), &bad) {
			continue
		}
		it.ID = strconv.FormatInt(id, 10)
		it.Timestamp = at.UTC().Format(time.RFC3339)
		it.Message = sentence(it.ActorUsername, it.Text)
		if it.ChallengeID != "" {
			it.ChallengeTitle = challengeTitle(prefix, subject)
		}
		items = append(items, it)
	}
	if err := rows.Err(); err != nil {
		queryFailed("reading the notifications list", "the list may be short", err)
	}
	unread := 0
	err = db.QueryRow(`SELECT COUNT(*) FROM user_notifications
	                    WHERE user_id = $1 AND read_at IS NULL`, uid).Scan(&unread)
	queryFailed("counting unread notifications", "showing none as unread", err)
	writeJSON(w, http.StatusOK, map[string]any{"items": items, "unread": unread})
}

// POST /api/v1/notifications/read — everything seen.
func MarkNotificationsReadHandler(w http.ResponseWriter, r *http.Request) {
	uid, err := strconv.Atoi(authUserID(r))
	if err != nil || uid <= 0 {
		http.Error(w, "sign in first", http.StatusUnauthorized)
		return
	}
	_, err = db.Exec(`UPDATE user_notifications SET read_at = NOW()
	                   WHERE user_id = $1 AND read_at IS NULL`, uid)
	if queryFailed("marking notifications read", "they stay unread", err) {
		http.Error(w, "could not mark them read", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

// ── What happens that people are told about ─────────────────────────────

// SendFollowNotification tells someone they have a new follower.
func SendFollowNotification(payload FollowEventPayload) {
	followed, ok := GetUserByUsername(payload.FollowingUsername)
	if !ok {
		return
	}
	follower, _ := GetUserByUsername(payload.FollowerUsername)
	uid, _ := strconv.Atoi(followed.ID)
	fid, _ := strconv.Atoi(follower.ID)
	notifyUser(InboxNote{
		UserID:    uid,
		Username:  followed.Username,
		Kind:      noteFollow,
		ActorID:   fid,
		ActorName: payload.FollowerUsername,
		Body:      "started following you.",
	})
}

// SendChallengeAcceptedNotification tells a creator their challenge has an
// answer, and so a battle.
func SendChallengeAcceptedNotification(c Challenge, responderID, responderName string) {
	uid, _ := strconv.Atoi(c.CreatorID)
	rid, _ := strconv.Atoi(responderID)
	cid, _ := strconv.Atoi(c.ID)
	notifyUser(InboxNote{
		UserID:      uid,
		Username:    c.CreatorUsername,
		Kind:        noteAccepted,
		ActorID:     rid,
		ActorName:   responderName,
		ChallengeID: cid,
		Body: fmt.Sprintf("accepted your challenge “%s” — the battle is on.",
			challengeTitle(c.Prefix, c.Subject)),
	})
}

// SendBattleStartedNotification tells the person who answered that their
// battle is live.
//
// Only the challenger used to be told, which read as an odd asymmetry: two
// people are now in a contest that people will vote on, and one of them
// found out. The challenge is named, because a responder may have answered
// several.
func SendBattleStartedNotification(c Challenge, responderID, responderName string) {
	uid, _ := strconv.Atoi(responderID)
	creatorID, _ := strconv.Atoi(c.CreatorID)
	cid, _ := strconv.Atoi(c.ID)
	notifyUser(InboxNote{
		UserID:      uid,
		Username:    responderName,
		Kind:        noteBattleStarted,
		ActorID:     creatorID,
		ActorName:   c.CreatorUsername,
		ChallengeID: cid,
		Body: fmt.Sprintf("is battling you in “%s” — voting is open.",
			challengeTitle(c.Prefix, c.Subject)),
	})
}

// SendBattleWonNotification tells the winner of a battle that has just been
// decided that they won, with the score: "You won “Who can juggle five?”
// against leo, 14–9." In their list, and as a push to their phone.
//
// Nobody used to be told. A battle ran for a week and ended in silence; the
// only way to find out was to go and look.
//
// A clear win only. A draw, or a battle nobody took part in, has no winner
// to tell. The losing side is not told either — the owner asked for the
// winner's note, and "you lost" is not a ping anybody wants.
func SendBattleWonNotification(challengeID int, verdicts []battleVerdict) {
	var winner *battleVerdict
	for i := range verdicts {
		if verdicts[i].Outcome == "won" {
			winner = &verdicts[i]
		}
	}
	if winner == nil || db == nil {
		return
	}
	// Who they beat: the best of the rest, which in a two-sided battle is
	// simply the other side.
	var beaten *battleVerdict
	for i := range verdicts {
		v := &verdicts[i]
		if v.userID == winner.userID {
			continue
		}
		if beaten == nil || v.Rank < beaten.Rank {
			beaten = v
		}
	}

	var prefix, subject string
	err := db.QueryRow(`
		SELECT COALESCE(prefix, ''), COALESCE(subject, '')
		  FROM challenges WHERE id = $1`, challengeID).Scan(&prefix, &subject)
	queryFailed("reading the question of a battle to tell its winner",
		"the note leaves the question out", err)
	title := challengeTitle(prefix, subject)

	body := "You won"
	if title != "" {
		body += fmt.Sprintf(" “%s”", title)
	}
	score := ""
	if beaten != nil {
		body += " against " + beaten.Username
		// The score only when votes decided it. Level on votes means it
		// went to likes, views or shares, and "3–3" would read like a draw.
		if winner.Votes != beaten.Votes {
			score = voteCount(winner.Votes) + "–" + voteCount(beaten.Votes)
			body += ", " + score
		}
	}
	body += "."

	notifyUser(InboxNote{
		UserID:      winner.userID,
		Username:    winner.Username,
		Kind:        noteBattleWon,
		ChallengeID: challengeID,
		Body:        body,
	})

	pushBody := title
	if score != "" {
		pushBody = fmt.Sprintf("%s — %s", title, score)
	}
	if _, _, err := enqueueNotification(EnqueueParams{
		UserID:      strconv.Itoa(winner.userID),
		TriggerKind: TriggerBattleWon,
		DedupeKey:   "battle_won:" + strconv.Itoa(challengeID),
		Title:       "You won your battle 🏆",
		Body:        pushBody,
		Deeplink:    fmt.Sprintf("devf://challenge/%d", challengeID),
	}); err != nil {
		log.Printf("battle %d: could not queue the winner's push for user %d: "+
			"%v — they still have it in their list", challengeID, winner.userID, err)
	}
}

// voteCount is a vote count as people read it: "14", "4.5". A vote from a
// very new account counts half, so counts are not always whole.
func voteCount(v float64) string {
	return strconv.FormatFloat(v, 'f', -1, 64)
}
