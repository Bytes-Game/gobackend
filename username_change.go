package main

// username_change.go — changing your username, the way TikTok allows it:
// once every 30 days.
//
// The new name follows sign-up's rules (usernameProblem) and must not be
// taken in any capital letters. A new account may change its name at once;
// after that, once every 30 days. The old name is free for anybody straight
// away, as on TikTok.
//
// Everything that finds a person by their name has to move with it. A
// sign-in carries the name it was made with, and the live connection, the
// "who is online" mark and the notifications waiting for somebody offline
// are all filed under it. So a change:
//
//   - signs out every other phone, whose sign-ins carry the old name, and
//     hands this one a fresh sign-in with the new name;
//   - closes the live connection filed under the old name, so it cannot
//     pick up what is meant for whoever takes that name next; the app
//     reconnects under the new one;
//   - moves the notifications waiting under the old name to the new one;
//   - updates the search index.
//
// What does not follow: links to the old profile address (/u/old_name), and
// "@old_name" written in comments and bios, which are plain text. Both lead
// to whoever takes the name next, or to nobody. TikTok's old links break too.

import (
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/lib/pq"
)

// usernameChangeWait is how long after one change the next is allowed.
const usernameChangeWait = 30 * 24 * time.Hour

// nextUsernameChange is when an account that last changed its name at
// [changedAt] may change it again: the zero time when it may now.
func nextUsernameChange(changedAt *time.Time, now time.Time) time.Time {
	if changedAt == nil {
		return time.Time{}
	}
	if next := changedAt.Add(usernameChangeWait); next.After(now) {
		return next
	}
	return time.Time{}
}

// timeOrNull is [t] as the app reads dates, or null for the zero time.
func timeOrNull(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t.UTC().Format(time.RFC3339)
}

// currentUsername is the name account [userID] has now — which a sign-in made
// before a change does not carry. sql.ErrNoRows when there is no such account.
func currentUsername(userID string) (string, error) {
	var name string
	err := db.QueryRow(`SELECT username FROM users WHERE id::text = $1`, userID).Scan(&name)
	return name, err
}

// UsernameStatusHandler — GET /api/v1/users/{id}/username, your own.
//
// {"username": "...", "nextChangeAt": RFC3339 or null}. null: it may change
// now. What the app's Change username page says before anything is typed.
func UsernameStatusHandler(w http.ResponseWriter, r *http.Request) {
	uid, ok := requirePathUser(w, r, mux.Vars(r)["id"])
	if !ok {
		return
	}
	var name string
	var changedAt *time.Time
	err := db.QueryRow(`SELECT username, username_changed_at FROM users WHERE id::text = $1`,
		uid).Scan(&name, &changedAt)
	if err == sql.ErrNoRows {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	if queryFailed("account "+uid+"'s username and when it last changed",
		"the Change username page says it cannot load", err) {
		http.Error(w, "temporarily unavailable", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"username":     name,
		"nextChangeAt": timeOrNull(nextUsernameChange(changedAt, time.Now())),
	})
}

// ChangeUsernameHandler — PUT /api/v1/users/{id}/username, your own.
// Body {"username": "new_name"}.
//
//	200 {"user", "token", "nextChangeAt"} — changed. The token replaces this
//	    phone's sign-in: the change signed out every sign-in made before it.
//	400 {"field": "username", "error"} — not allowed, or already your name.
//	409 {"field": "username", "error"} — taken.
//	409 {"field": "username", "error", "nextChangeAt", "tooSoon": true} —
//	    changed less than 30 days ago.
func ChangeUsernameHandler(w http.ResponseWriter, r *http.Request) {
	uid, ok := requirePathUser(w, r, mux.Vars(r)["id"])
	if !ok {
		return
	}
	if !allowAction(uid, "username_change") {
		writeRateLimited(w, "username_change")
		return
	}
	var body struct {
		Username string `json:"username"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 4<<10)).Decode(&body); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}
	name := strings.ToLower(strings.TrimSpace(body.Username))
	if why := usernameProblem(name); why != "" {
		writeFieldError(w, http.StatusBadRequest, refuse("username", why))
		return
	}

	var old string
	err := db.QueryRow(`SELECT username FROM users WHERE id::text = $1`, uid).Scan(&old)
	if queryFailed("account "+uid+"'s username before changing it", "the change is refused", err) {
		http.Error(w, "temporarily unavailable", http.StatusInternalServerError)
		return
	}
	if name == old {
		writeFieldError(w, http.StatusBadRequest, refuse("username", "That's already your username."))
		return
	}
	var taken bool
	err = db.QueryRow(`SELECT EXISTS (SELECT 1 FROM users WHERE lower(username) = lower($1) AND id::text <> $2)`,
		name, uid).Scan(&taken)
	if queryFailed("whether "+name+" is taken", "the change is refused", err) {
		http.Error(w, "temporarily unavailable", http.StatusInternalServerError)
		return
	}
	if taken {
		writeFieldError(w, http.StatusConflict, refuse("username", "That username is taken."))
		return
	}

	// Once every 30 days, held by the statement itself, so two changes sent
	// at the same moment cannot both pass. The sign-out moment is to the
	// second, from this server's clock: the same clock that stamps the fresh
	// sign-in below, so that sign-in is never older.
	at := time.Now().Truncate(time.Second)
	res, err := db.Exec(`
		UPDATE users SET username = $1, username_changed_at = $2, signed_out_at = $2
		 WHERE id::text = $3
		   AND (username_changed_at IS NULL OR username_changed_at <= $4)`,
		name, at, uid, at.Add(-usernameChangeWait))
	if err != nil {
		// Somebody took the name a moment ago: the database's lock refuses
		// the second (usernames.go).
		var pqErr *pq.Error
		if errors.As(err, &pqErr) && pqErr.Code == "23505" {
			writeFieldError(w, http.StatusConflict, refuse("username", "That username is taken."))
			return
		}
		log.Printf("user %s: changing the username to %q failed: %v", uid, name, err)
		http.Error(w, "temporarily unavailable", http.StatusInternalServerError)
		return
	}
	if n, _ := res.RowsAffected(); n == 0 {
		// Changed less than 30 days ago. Read when, for the date to give.
		var changedAt *time.Time
		err := db.QueryRow(`SELECT username_changed_at FROM users WHERE id::text = $1`, uid).Scan(&changedAt)
		if queryFailed("when account "+uid+" last changed its username",
			"the refusal gives no date", err) {
			changedAt = nil
		}
		writeTooSoon(w, nextUsernameChange(changedAt, time.Now()))
		return
	}

	forgetCutoff(uid)
	moveLiveState(old, name)
	user, _ := GetUserByID(uid)
	go IndexUser(user)

	token, err := issueToken(uid, name)
	if err != nil {
		log.Printf("user %s changed their username to %q, but no new sign-in could be made: %v",
			uid, name, err)
		http.Error(w, "Your username was changed. Please sign in again.", http.StatusInternalServerError)
		return
	}
	log.Printf("user %s changed their username from %q to %q", uid, old, name)
	writeJSON(w, http.StatusOK, map[string]any{
		"user":         user,
		"token":        token,
		"nextChangeAt": timeOrNull(at.Add(usernameChangeWait)),
	})
}

// writeTooSoon refuses a change made less than 30 days after the last one.
// The words are for apps that do not read nextChangeAt.
func writeTooSoon(w http.ResponseWriter, next time.Time) {
	words := "You can change your username once every 30 days."
	if !next.IsZero() {
		words = "You can change your username again on " + next.UTC().Format("2 January 2006") + "."
	}
	writeJSON(w, http.StatusConflict, map[string]any{
		"field":        "username",
		"error":        words,
		"nextChangeAt": timeOrNull(next),
		"tooSoon":      true,
	})
}

// moveLiveState moves what is filed under a person's name when it changes.
// The live connection under the old name is closed: the app reconnects under
// the new one. The notifications waiting for them go to the new name — and
// whatever was left under the new name by somebody who had it before is
// thrown away, not inherited.
func moveLiveState(oldName, newName string) {
	wsDisconnect(oldName)
	if rdb == nil {
		return
	}
	oldKey, newKey := storedNotificationsKey(oldName), storedNotificationsKey(newName)
	if err := rdb.Del(rctx, newKey).Err(); err != nil {
		log.Printf("username change %q → %q: notifications left under the new name by somebody "+
			"before could not be cleared: %v", oldName, newName, err)
	}
	if n, err := rdb.Exists(rctx, oldKey).Result(); err == nil && n > 0 {
		if err := rdb.Rename(rctx, oldKey, newKey).Err(); err != nil {
			log.Printf("username change %q → %q: waiting notifications could not be moved: %v",
				oldName, newName, err)
		}
	}
}
