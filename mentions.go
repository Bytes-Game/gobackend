package main

// Mentioning people in a comment: "@leo look at this!"
//
// Whoever is named with an @ is told — in their notifications list, live if
// they are online, and as a push to their phone: "maya mentioned you in a
// comment". Tapping it opens the video.
//
// Not everyone named is told:
//
//   - nobody is told about their own comment;
//   - nobody is told across a block, either way round;
//   - nobody is told about a video they may not watch — naming someone in a
//     comment on a friends-only video must not show it to a person it is
//     not for;
//   - a name that is nobody's is just text.
//
// At most mentionMax people per comment, so a comment cannot be used to
// ping a crowd.
//
// The app does the rest of mentions on its own: suggesting people as you
// type after an @, and drawing @names as links to their profiles, in
// comments and in chat alike. Chat sends no extra notification — the one
// other person in a chat already hears about every message in it.

import (
	"fmt"
	"log"
	"regexp"
	"strconv"
	"strings"

	"github.com/lib/pq"
)

const (
	noteMention = "mention"

	// TriggerMention is the push for "someone mentioned you". Sent under
	// the friends' activity setting, with a friend's answer and a friend's
	// challenge.
	TriggerMention TriggerKind = "mention"

	// mentionMax is how many people one comment can mention.
	mentionMax = 10
)

// mentionRe finds @names: a username's own letters (signup.go), after the
// start of the text or something that cannot be part of a name — so an
// e-mail address is not a mention of its domain.
var mentionRe = regexp.MustCompile(`(?:^|[^a-zA-Z0-9_.@])@([a-zA-Z0-9_.]{3,20})`)

// mentionedNames are the distinct usernames [text] mentions, lowercased, in
// the order they first appear, at most mentionMax.
func mentionedNames(text string) []string {
	var out []string
	seen := map[string]bool{}
	for _, m := range mentionRe.FindAllStringSubmatch(text, -1) {
		name := strings.ToLower(strings.TrimRight(m[1], "."))
		if len(name) < 3 || seen[name] {
			continue
		}
		seen[name] = true
		out = append(out, name)
		if len(out) == mentionMax {
			break
		}
	}
	return out
}

// mayWatch reports whether [viewerID] may watch challenge [challengeID]
// (see mayWatchSQL).
func mayWatch(viewerID, challengeID int) (bool, error) {
	var ok bool
	err := db.QueryRow(`
		SELECT EXISTS (SELECT 1 FROM challenges c
		                WHERE c.id = $1 AND `+mayWatchSQL+`)`,
		challengeID, viewerID).Scan(&ok)
	return ok, err
}

// notifyCommentMentions tells everyone a new comment mentions, who may be
// told. Answers who was told, for the tests and the log.
func notifyCommentMentions(challengeID, commentID, authorID int, authorName, text string) []string {
	names := mentionedNames(text)
	if len(names) == 0 || db == nil {
		return nil
	}
	rows, err := db.Query(`
		SELECT u.id, u.username FROM users u
		 WHERE LOWER(u.username) = ANY($1) AND u.id <> $2
		   AND NOT EXISTS (SELECT 1 FROM user_blocks b
		                    WHERE (b.blocker_id = u.id AND b.blocked_id = $2)
		                       OR (b.blocker_id = $2 AND b.blocked_id = u.id))`,
		pq.Array(names), authorID)
	if queryFailed(fmt.Sprintf("finding who comment %d mentions", commentID),
		"nobody is told they were mentioned", err) {
		return nil
	}
	type person struct {
		id   int
		name string
	}
	var people []person
	bad := 0
	for rows.Next() {
		var p person
		if !scanFailed("a person a comment mentions", rows.Scan(&p.id, &p.name), &bad) {
			people = append(people, p)
		}
	}
	if err := rows.Err(); err != nil {
		queryFailed("reading who a comment mentions", "some are not told", err)
	}
	rows.Close()

	quote := truncateText(strings.TrimSpace(text), 80)

	var told []string
	for _, p := range people {
		ok, err := mayWatch(p.id, challengeID)
		if queryFailed(fmt.Sprintf("checking user %d may watch challenge %d", p.id, challengeID),
			"not telling them about the mention, to be safe", err) || !ok {
			continue
		}
		notifyUser(InboxNote{
			UserID:      p.id,
			Username:    p.name,
			Kind:        noteMention,
			ActorID:     authorID,
			ActorName:   authorName,
			ChallengeID: challengeID,
			Body:        fmt.Sprintf("mentioned you in a comment: “%s”", quote),
		})
		if _, _, err := enqueueNotification(EnqueueParams{
			UserID:      strconv.Itoa(p.id),
			TriggerKind: TriggerMention,
			DedupeKey:   "mention:comment:" + strconv.Itoa(commentID),
			Title:       fmt.Sprintf("%s mentioned you", authorName),
			Body:        quote,
			Deeplink:    fmt.Sprintf("devf://challenge/%d", challengeID),
		}); err != nil {
			log.Printf("comment %d: could not queue a mention push for user %d: "+
				"%v — they still have it in their list", commentID, p.id, err)
		}
		told = append(told, p.name)
	}
	return told
}
