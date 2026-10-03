package main

// privacy_settings.go — two privacy choices a person makes for themselves,
// kept in their settings (users.settings) and honoured here on the server,
// so no app can get round them:
//
//   - "messages": "everyone" (the default) or "following". With
//     "following", only people they follow can message or call them.
//     Everyone else is told the same thing a blocked sender is told —
//     nothing about why.
//
//     A PRIVATE account (visibility "friends") is "following" whatever the
//     setting says. The app greys "Everyone" out for a private account, and
//     a choice the screen says is off must not still be on underneath —
//     which it would be for anyone who picked "Everyone" before going
//     private.
//   - "showActivity": true (the default) or false. With false, nobody sees
//     that they are online now, or when they were last here.
//
// An unreadable setting lets things through: a person can still be
// reached, and is told nothing wrong, while the database is unwell.

import (
	"database/sql"
	"errors"
	"strconv"
)

// onlyFromFollowedSQL is true for the user `u` when they take messages and
// calls only from people they follow: they chose that, or their account is
// private. One copy, used by both messages (messagesRefused) and live calls
// (liveConn.peer): there were two, and only one of them learned about
// private accounts.
const onlyFromFollowedSQL = `(COALESCE(u.settings->>'messages', 'everyone') = 'following'
		        OR COALESCE(u.visibility, 'public') = 'friends')`

// messagesRefused reports whether [receiverID] takes no messages from
// [senderID]: they chose "only people I follow", or their account is
// private, and they do not follow them.
func messagesRefused(senderID, receiverID int) bool {
	if db == nil {
		return false
	}
	var refused bool
	err := db.QueryRow(`
		SELECT `+onlyFromFollowedSQL+`
		       AND NOT EXISTS (SELECT 1 FROM follows f
		                        WHERE f.follower_id = u.id AND f.following_id = $1)
		  FROM users u WHERE u.id = $2`, senderID, receiverID).Scan(&refused)
	if errors.Is(err, sql.ErrNoRows) {
		return false
	}
	if queryFailed("checking whether user "+strconv.Itoa(receiverID)+
		" takes messages from user "+strconv.Itoa(senderID),
		"letting the message through", err) {
		return false
	}
	return refused
}

// activityHidden reports whether [username] chose not to show when they
// are active.
func activityHidden(username string) bool {
	if db == nil {
		return false
	}
	var hidden bool
	err := db.QueryRow(`
		SELECT COALESCE(settings->>'showActivity', 'true') = 'false'
		  FROM users WHERE username = $1`, username).Scan(&hidden)
	if errors.Is(err, sql.ErrNoRows) {
		return false
	}
	if queryFailed("reading whether "+username+" shows when they are active",
		"showing it", err) {
		return false
	}
	return hidden
}
