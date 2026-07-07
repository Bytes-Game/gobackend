package main

// cluster.go — multi-replica coordination switches.
//
// The backend was explicitly single-instance by design: in-process rate
// limiters, in-process keyed locks, and an in-process WebSocket
// connection map. That's correct and fast on one Render instance, but
// every one of those structures silently misbehaves the moment a second
// replica starts (split rate budgets, lost cross-replica lock
// exclusion, chat/notifications only reaching users who happen to be
// connected to the same replica).
//
// Rather than pay distributed-coordination latency on every request
// while we run one replica, the Redis-backed variants are gated behind
// one env switch:
//
//	MULTI_REPLICA=1 (or "true")
//
// Flip it when scaling out. With it unset, behavior and performance are
// byte-identical to the single-instance design.

import (
	"os"
	"strings"
	"sync"
)

var (
	multiReplicaOnce sync.Once
	multiReplicaVal  bool
)

// multiReplica reports whether this process should coordinate shared
// state through Redis instead of process memory. Read once — flipping
// it requires a restart, which is exactly when replica counts change.
func multiReplica() bool {
	multiReplicaOnce.Do(func() {
		v := strings.TrimSpace(strings.ToLower(os.Getenv("MULTI_REPLICA")))
		multiReplicaVal = v == "1" || v == "true" || v == "yes"
	})
	return multiReplicaVal
}
