package main

import "fmt"

// ProcessFollowEvent dynamically updates the database when a follow event occurs.
func ProcessFollowEvent(payload FollowEventPayload) error {
	usersDBMu.Lock()
	defer usersDBMu.Unlock()

	var follower, following *User
	var followerIndex, followingIndex = -1, -1

	for i := range users {
		if users[i].ID == payload.FollowerID {
			follower = &users[i]
			followerIndex = i
		}
		if users[i].ID == payload.FollowingID {
			following = &users[i]
			followingIndex = i
		}
	}

	if follower == nil {
		return fmt.Errorf("follower with ID %s not found", payload.FollowerID)
	}
	if following == nil {
		return fmt.Errorf("user to follow with ID %s not found", payload.FollowingID)
	}

	// DYNAMIC UPDATE: Increment the `Followers` count for the user being followed.
	users[followingIndex].Followers++

	// DYNAMIC UPDATE: Add the followed user's ID to the follower's `FollowingList`.
	users[followerIndex].FollowingList = append(users[followerIndex].FollowingList, payload.FollowingID)

	return nil
}

// ProcessUnfollowEvent dynamically updates the database when an unfollow event occurs.
func ProcessUnfollowEvent(payload UnfollowEventPayload) error {
	usersDBMu.Lock()
	defer usersDBMu.Unlock()

	var unfollower, unfollowed *User
	var unfollowerIndex, unfollowedIndex = -1, -1

	for i := range users {
		if users[i].ID == payload.UnfollowerID {
			unfollower = &users[i]
			unfollowerIndex = i
		}
		if users[i].ID == payload.UnfollowedID {
			unfollowed = &users[i]
			unfollowedIndex = i
		}
	}

	if unfollower == nil {
		return fmt.Errorf("unfollower with ID %s not found", payload.UnfollowerID)
	}
	if unfollowed == nil {
		return fmt.Errorf("user to unfollow with ID %s not found", payload.UnfollowedID)
	}

	// DYNAMIC UPDATE: Decrement the `Followers` count for the user being unfollowed.
	users[unfollowedIndex].Followers--

	// DYNAMIC UPDATE: Remove the unfollowed user from the unfollower's `FollowingList`.
	newFollowingList := []string{}
	for _, id := range users[unfollowerIndex].FollowingList {
		if id != payload.UnfollowedID {
			newFollowingList = append(newFollowingList, id)
		}
	}
	users[unfollowerIndex].FollowingList = newFollowingList

	return nil
}
