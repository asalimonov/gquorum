//nolint:unused // Receive/send ownership fields are connected by plan tasks 7–9.
//lint:file-ignore U1000 Receive/send fields are connected by plan tasks 7–9.
package loadsnapshot

import (
	"context"
	"hash"
	"sync"
	"time"

	"github.com/asalimonov/gquorum/internal/raftengine"
	"github.com/asalimonov/gquorum/load/snapshot"
)

type snapshotDirection uint8

const (
	incoming snapshotDirection = iota
	outgoing
)

type snapshotOwner struct {
	key    OwnerKey
	handle Owner
}
type snapshotReceive struct {
	key                         snapshot.Key
	manifest                    snapshot.Manifest
	owner                       snapshotOwner
	attachment                  uint64
	state                       snapshot.DeliveryState
	staging                     snapshotStaging
	next                        uint64
	chunkBytes                  uint32
	digest                      hash.Hash
	started, progress, deadline time.Time
	reservation                 *snapshotReservation
	cancel                      context.CancelFunc
	attached                    *snapshotAttachment
	completion                  *snapshotCompletion
}
type snapshotReservation struct {
	direction   snapshotDirection
	bytes       uint64
	owner       snapshotOwner
	coordinator *Coordinator
	released    bool // guarded by coordinator.mu
}
type snapshotAttachment struct {
	done   chan struct{}
	cancel context.CancelFunc
}
type snapshotCompletion struct {
	mu      sync.Mutex
	done    chan struct{}
	once    sync.Once
	outcome raftengine.SnapshotInstallOutcome
	err     error
}
type snapshotTerminal struct {
	owner    snapshotOwner
	manifest snapshot.Manifest
	ack      snapshot.Ack
	outcome  snapshot.ReceiverOutcome
	expires  time.Time
	sequence uint64
}

// SourceSnapshotID identifies one immutable engine publication, not just its
// native index/term. Digest values never retain the source payload.
type SourceSnapshotID struct {
	RunID                          string
	Generation, Index, Term, Bytes uint64
}
