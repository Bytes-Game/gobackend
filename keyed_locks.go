package main

import "sync"

// ─────────────────────────────────────────────────────────────────────────────
// KEYED LOCKS — serialize read-modify-write on a string key within this process.
//
// SessionState and UserProfile are both persisted as a single blob/row and
// mutated by several concurrent code paths (event handler, batch handler,
// impression goroutine, the feed handler, the negative-feedback miner, the
// impression aggregator, strategy-outcome recording, profile recompute). Each
// did load → modify → save with no synchronization, so two concurrent writers
// would both read the same starting value and the second save would clobber the
// first — silently losing exactly the ItemsSeen / DopamineBudget / SkipStreak /
// CategoryAffinity / AvoidedCategories signals the recommender is built on.
//
// shardedMutex serializes those RMW sequences by key. Because every writer for a
// given key takes the same lock, each one loads the latest committed state, so
// updates merge instead of clobbering. Memory is bounded: a fixed pool of
// mutexes (key → shard by hash) rather than one mutex per distinct user/session
// forever. Same key always maps to the same shard; two distinct keys may rarely
// share a shard (a harmless bit of extra serialization).
//
// SCOPE: in-process — correct for a single instance (Render free tier). For a
// multi-instance deployment these RMW sequences need Redis WATCH/MULTI or a Lua
// CAS; an in-process lock can't serialize across replicas. Documented so the
// next person scaling out knows to revisit.
// ─────────────────────────────────────────────────────────────────────────────

type shardedMutex struct {
	shards [256]sync.Mutex
}

func newShardedMutex() *shardedMutex { return &shardedMutex{} }

// lock acquires the shard for key and returns its unlock func. Usage:
//
//	unlock := keyLocks.lock(userID)
//	defer unlock()
func (s *shardedMutex) lock(key string) func() {
	// FNV-1a, inline — no allocation on the hot path.
	var h uint32 = 2166136261
	for i := 0; i < len(key); i++ {
		h ^= uint32(key[i])
		h *= 16777619
	}
	mu := &s.shards[h%uint32(len(s.shards))]
	mu.Lock()
	return mu.Unlock
}

// sessionKeyLocks serializes SessionState RMW per (userID:sessionID).
var sessionKeyLocks = newShardedMutex()

// profileKeyLocks serializes UserProfile RMW per userID. Lock ordering rule to
// avoid deadlock: any path that needs BOTH takes the session lock first, then
// the profile lock (never the reverse) — see updateSessionFromEvent.
var profileKeyLocks = newShardedMutex()
