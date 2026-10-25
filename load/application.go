package load

import (
	"context"

	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

// ReplicaRef identifies a replica within a load group.
type ReplicaRef struct {
	GroupID   string
	ReplicaID uint64
}

// ReplicaSpec supplies an application factory with the exact approved replica
// and node identity.
type ReplicaSpec struct {
	Identity        storage.Identity
	Node            topology.Identity
	Group           topology.Group
	ApprovedVersion uint64
	Recovery        *RecoveryInfo
}

// Application transfers machine lifetime ownership from a successful factory
// to Node. Close is idempotent, bounded by its context, and runs after engine
// and Run work has joined. A failed Close retains ownership and prevents a
// successor.
// Callbacks may read status or RequestStop; they must not wait for their own
// group's lifecycle or a Stop that has to join them.
type Application struct {
	StateMachine StateMachine
	// Run is optional background work for this exact managed replica. Node
	// invokes it after replay, startup authorization and handle publication.
	// Its context ends on replica stop, failure, fencing or Node shutdown, not
	// leadership loss. Run must join its children before returning and must not
	// wait for teardown
	// that joins itself. Node joins Run and the engine before calling Close.
	// A nil return completes the work without restarting it. An unexpected
	// error
	// fails and holds this replica until an explicit StartLoadGroup retries it.
	Run   func(context.Context, *Group) error
	Close func(context.Context) error
}
