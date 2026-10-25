package loadsnapshot

import (
	"context"
	"time"

	"github.com/asalimonov/gquorum/internal/raftengine"
	"github.com/asalimonov/gquorum/internal/raftstate"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/load/snapshot"
	"github.com/asalimonov/gquorum/metrics"
	"go.etcd.io/raft/v3/raftpb"
)

// OwnerKey pins one exact replica in one engine incarnation.
type OwnerKey struct {
	Peer  load.Peer
	RunID string
}

// Owner is a private managed handle; none of its methods may redirect to a
// successor after a wait. Its Context belongs to that exact engine lifetime.
type Owner interface {
	Key() OwnerKey
	Context() context.Context
	SnapshotBounds() raftengine.SnapshotBounds
	PreflightSnapshot(context.Context, raftpb.Message) (raftengine.SnapshotPreflight, error)
	PrepareOwnedSnapshot(context.Context, raftpb.Message) (raftstate.Snapshot, error)
	StepOwnedSnapshot(context.Context, raftengine.SnapshotIngress) error
}

// Ports joins transport coordination to managed authorization without importing
// managed or root gquorum. Callbacks never run under the coordinator mutex.
type Ports struct {
	Authorize            func(context.Context, snapshot.Key) (Owner, error)
	ResolvePeer          func(context.Context, Owner, uint64) (load.Peer, error)
	ValidateOutgoing     func(context.Context, Owner, load.Peer) error
	RegisterDelivery     func(Owner, raftengine.SnapshotAttempt, load.Peer, time.Time) (uint64, error)
	ReportDelivery       func(Owner, snapshot.Key, snapshot.Ack, error)
	NoteSnapshotProgress func(Owner, raftengine.SnapshotAttempt, metrics.SnapshotCatchUp)
	Transport            snapshot.Transport
}
