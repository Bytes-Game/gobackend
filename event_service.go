package main

import (
	"fmt"
	"strconv"
)

// resolveUserID returns the integer DB primary key for a user,
// accepting either a string ID or a username (falls back in that order).
func resolveUserID(idStr, username string) (int, error) {
	if idStr != "" {
		id, err := strconv.Atoi(idStr)
		if err != nil {
			var exists bool
			db.QueryRow(`SELECT EXISTS(SELECT 1 FROM users WHERE id = $1)`, id).Scan(&exists)
			if exists {
				return id, nil
			}
		}
	}
	if username != "" {
		var id int
		err := db.QueryRow(`SELECT id FROM users WHERE username = $1`, username).Scan(&id)
		if err == nil {
			return id, nil
		}
	}
	return 0, fmt.Errorf("user not found: id=%s username=%s", idStr, username)
}

// ProcessFollowEvent inserts a row into the follows table
// ON CONFLICT DO NOTHING makes duplicate follows a safe no-op.
func ProcessFollowEvent(payload FollowEventPayload) error {
	followerID, err := resolveUserID(payload.FollowerID, payload.FollowerUsername)
	if err != nil {
		return fmt.Errorf("follower '%s' not found", payload.FollowerUsername)
	}
	followingID, err := resolveUserID(payload.FollowingID, payload.FollowingUsername)
	if err != nil {
		return fmt.Errorf("user to follow '%s' not found", payload.FollowingUsername)
	}

	_, err = db.Exec(
		`INSERT INTO follows (follower_id, following_id) VALUES ($1, $2) ON CONFLICT DO NOTHING`,
		followerID, followingID,
	)
	return err
}

// ProcessUnfollowEvent removes a row from the follows table.
func ProcessUnfollowEvent(payload UnfollowEventPayload) error {
	unfollowerID, err := resolveUserID(payload.UnfollowerID, payload.UnfollowerUsername)
	if err != nil {
		return fmt.Errorf("unfollower '%s' not found", payload.UnfollowerUsername)
	}
	unfollowedID, err := resolveUserID(payload.UnfollowedID, payload.UnfollowedUsername)
	if err != nil {
		return fmt.Errorf("user to unfollow '%s' not found", payload.UnfollowedUsername)
	}

	_, err = db.Exec(
		`DELETE FROM follows WHERE follower_id = $1 AND following_id = $2`,
		unfollowerID, unfollowedID,
	)
	return err
}
