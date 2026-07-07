package main

import (
	"crypto/rand"
	"encoding/hex"
	"sync"
	"time"
)

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
// SCOPE: the shard mutex serializes within this process; with
// MULTI_REPLICA=1 a best-effort Redis lease (SET NX PX + owner-checked
// release) additionally serializes across replicas. The lease is
// fail-open by design — see lock() below.
// ─────────────────────────────────────────────────────────────────────────────

type shardedMutex struct {
	shards [256]sync.Mutex
}

func newShardedMutex() *shardedMutex { return &shardedMutex{} }

// lock acquires the shard for key and returns its unlock func. Usage:
//
//	unlock := keyLocks.lock(userID)
//	defer unlock()
//
// In multi-replica mode a Redis lease is layered ON TOP of the local
// shard mutex: the local mutex serializes goroutines in this process
// (cheap, always), the lease serializes processes. Lease acquisition
// spins briefly and then proceeds WITHOUT the lease rather than fail —
// for recommender state, a rare lost update beats stalling the event
// pipeline behind a slow peer (same fail-open philosophy as every other
// Redis dependency here).
func (s *shardedMutex) lock(key string) func() {
	// FNV-1a, inline — no allocation on the hot path.
	var h uint32 = 2166136261
	for i := 0; i < len(key); i++ {
		h ^= uint32(key[i])
		h *= 16777619
	}
	mu := &s.shards[h%uint32(len(s.shards))]
	mu.Lock()

	if !multiReplica() || rdb == nil {
		return mu.Unlock
	}
	release := acquireRedisLease("lk:"+key, redisLeaseTTL)
	return func() {
		release()
		mu.Unlock()
	}
}

const (
	// redisLeaseTTL bounds how long a crashed holder can block peers.
	// RMW sections here are load-blob → mutate → save-blob: a few Redis
	// or Postgres round-trips, single-digit milliseconds normally.
	redisLeaseTTL = 3 * time.Second
	// redisLeaseWait is the max total time we spin waiting for a peer
	// before proceeding un-leased (fail-open).
	redisLeaseWait = 500 * time.Millisecond
)

// releaseLeaseLua releases a lease only if we still hold it — deleting
// unconditionally could release a lease a peer legitimately acquired
// after ours expired.
const releaseLeaseLua = `
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
`

// acquireRedisLease takes a best-effort cross-process lease and returns
// its release func. Always returns a usable func — on timeout or Redis
// failure it returns a no-op and the caller proceeds un-leased.
func acquireRedisLease(key string, ttl time.Duration) func() {
	tok := make([]byte, 12)
	if _, err := rand.Read(tok); err != nil {
		return func() {}
	}
	token := hex.EncodeToString(tok)
	deadline := time.Now().Add(redisLeaseWait)
	backoff := 5 * time.Millisecond
	for {
		ok, err := rdb.SetNX(rctx, key, token, ttl).Result()
		if err != nil {
			return func() {} // Redis down — proceed un-leased
		}
		if ok {
			return func() {
				_ = rdb.Eval(rctx, releaseLeaseLua, []string{key}, token).Err()
			}
		}
		if time.Now().After(deadline) {
			return func() {} // peer is slow/crashed — proceed un-leased
		}
		time.Sleep(backoff)
		if backoff < 80*time.Millisecond {
			backoff *= 2
		}
	}
}

// sessionKeyLocks serializes SessionState RMW per (userID:sessionID).
var sessionKeyLocks = newShardedMutex()

// profileKeyLocks serializes UserProfile RMW per userID. Lock ordering rule to
// avoid deadlock: any path that needs BOTH takes the session lock first, then
// the profile lock (never the reverse) — see updateSessionFromEvent.
var profileKeyLocks = newShardedMutex()
