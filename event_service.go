package main

import "fmt"

// ProcessFollowEvent dynamically updates the database when a follow event occurs
// It looks up users by ID first, then falls back to username for flexibility.
func ProcessFollowEvent(payload FollowEventPayload) error {
usersDBMu.Lock()
defer usersDBMu.Unlock()

var followerIndex, followingIndex = -1, -1

for i := range users {
if (payload.FollowerID != "" && users[i].ID == payload.FollowerID) ||
(payload.FollowerID == "" && users[i].Username == payload.FollowerUsername) {
followerIndex = i
}
if (payload.FollowingID != "" && users[i].ID == payload.FollowingID) ||
(payload.FollowingID == "" && users[i].Username == payload.FollowingUsername) {
followingIndex = i
}
}

if followerIndex == -1 {
return fmt.Errorf("follower '%s' not found", payload.FollowerUsername)
}
if followingIndex == -1 {
return fmt.Errorf("user to follow '%s' not found", payload.FollowingUsername)
}

// Prevent duplicate follows
for _, id := range users[followerIndex].FollowingList {
if id == users[followingIndex].ID {
return nil // Already following, no-op
}
}

// DYNAMIC UPDATE: Increment the `Followers` count for the user being followed
users[followingIndex].Followers++

// DYNAMIC UPDATE: Add the followed user's ID to the follower's `FollowingList`
users[followerIndex].FollowingList = append(users[followerIndex].FollowingList, users[followingIndex].ID)

return nil
}

// ProcessUnfollowEvent dynamically updates the database when an unfollow event occurs.
// It looks up users by ID first, then falls back to username for flexibility.
func ProcessUnfollowEvent(payload UnfollowEventPayload) error {
usersDBMu.Lock()
defer usersDBMu.Unlock()

var unfollowerIndex, unfollowedIndex =-1, -1

for i := range users {
if(payload.UnfollowerID != "" && users[i].ID == payload.UnfollowerID) ||
(payload.UnfollowerID == "" && users[i].Username == payload.UnfollowerUsername) {
unfollowerIndex = i
}
if (payload.UnfollowedID != "" && users[i].ID == payload.UnfollowedID) ||
(payload.UnfollowedID == "" && users[i].Username == payload.UnfollowedUsername) {
unfollowedIndex = i
}
}

if unfollowerIndex == -1 {
return fmt.Errorf("unfollower '%s' not found", payload.UnfollowerUsername)
}
if unfollowedIndex == -1 {
return fmt.Errorf("user to unfollow '%s' not found", payload.UnfollowedUsername)
}

// DYNAMIC UPDATE: Decrement the `Followers` count for the user being unfollowed.
if users[unfollowedIndex].Followers > 0 {
users[unfollowedIndex].Followers--
}

// DYNAMIC UPDATE: Remove the unfollowed user from the unfollower's `FollowingList`.
newFollowingList := []string{}
for _, id := range users[unfollowerIndex].FollowingList {
if id != users[unfollowedIndex].ID {
newFollowingList = append(newFollowingList, id)
}
}
users[unfollowerIndex].FollowingList = newFollowingList

return nil
}
