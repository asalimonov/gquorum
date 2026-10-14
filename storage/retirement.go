package storage

import (
	"context"
	"time"
)

// RetiredReplica identifies quarantined Raft state. Token is opaque and belongs
// to the store that enumerated it; it is not an application storage path.
type RetiredReplica struct {
	Identity      Identity
	Token         string
	QuarantinedAt time.Time
}

// RetirementStore separates explicit physical collection from consensus writes.
// Collect revalidates the exact token and identity and is idempotent if absent.
// Retention decisions belong to the node's lifecycle hooks, not this store.
type RetirementStore interface {
	Retired(context.Context) ([]RetiredReplica, error)
	Collect(context.Context, RetiredReplica) error
}
