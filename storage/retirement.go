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
// Retention decisions belong to the node's lifecycle hooks, not this store.
// Collect succeeds after exact deletion or when retained store-local proof
// establishes the same attempt and location is absent. Missing data without
// proof returns an error matching fs.ErrNotExist; foreign/corrupt proof refuses.
// An absent Retired entry alone is never proof of collection. Durable backends
// must retain proof across restart. Volatile stores cannot prove collection
// after process loss and must not manufacture a durable collection milestone.
//
// ForgetCollection releases only exact collection proof after retired data is
// absent. Missing proof is repeatable. The caller must first verify metadata
// acceptance/settlement and preserve its collected journal row until this call
// succeeds; neither this method nor settlement authorizes physical deletion.
type RetirementStore interface {
	Retired(context.Context) ([]RetiredReplica, error)
	Collect(context.Context, RetiredReplica) error
	ForgetCollection(context.Context, RetiredReplica) error
}
