//nolint:unused // Resource primitives are wired by the receive/send tasks 7–9.
package loadsnapshot

import (
	"context"
	"math"
	"sync"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/load/snapshot"
)

const (
	snapshotHistoryLimit    = 256
	snapshotTerminalMinimum = 256
	snapshotTerminalScale   = 4
	snapshotPageLimit       = 128
	snapshotTextLimit       = 512
	snapshotDigestsPerRun   = 2
)

// Coordinator owns Node-wide volatile transfer reservations and bounded
// metadata. Payload work, callbacks and joins happen outside mu.
type Coordinator struct {
	waiters      int
	workers      int
	ownerWorkers map[OwnerKey]int
	fencedOwners map[OwnerKey]struct{}
	historyIDs   []uint64

	options                      snapshot.Options
	ports                        Ports
	configErr                    error
	mu                           sync.Mutex
	ctx                          context.Context
	cancel                       context.CancelFunc
	started, closed              bool
	changed                      chan struct{}
	operations                   int
	incoming                     map[snapshot.Key]*snapshotReceive
	groups                       map[string]*snapshotReceive
	groupReservations            map[string]*snapshotReservation
	reservations                 map[*snapshotReservation]struct{}
	terminal                     map[snapshot.Key]*snapshotTerminal
	incomingCount, outgoingCount int
	receiveBytes, sendBytes      uint64
	sequence                     uint64
	history                      []snapshot.Status
	historySequence              uint64
	terminalSequence             uint64
	digests                      map[SourceSnapshotID][32]byte
	digestOrder                  map[string][]SourceSnapshotID
}

// NewCoordinator validates/copies policy without callbacks or resource work.
// Invalid internal construction remains refused; NewNode normally already
// resolved the same policy before creating a Host.
func NewCoordinator(options snapshot.Options, ports Ports) *Coordinator {
	resolved, err := options.Resolve()
	return &Coordinator{options: resolved, ports: ports, configErr: err, changed: make(chan struct{}),
		incoming: make(map[snapshot.Key]*snapshotReceive), groups: make(map[string]*snapshotReceive),
		groupReservations: make(map[string]*snapshotReservation), reservations: make(map[*snapshotReservation]struct{}),
		terminal: make(map[snapshot.Key]*snapshotTerminal), digests: make(map[SourceSnapshotID][32]byte),
		digestOrder:  make(map[string][]SourceSnapshotID),
		ownerWorkers: make(map[OwnerKey]int), fencedOwners: make(map[OwnerKey]struct{})}
}

// Start binds the coordinator to the Host lifetime once. It never restarts a
// fenced coordinator; Host replacement constructs a new exact owner.
func (c *Coordinator) Start(parent context.Context) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.started || c.closed {
		return
	}
	if c.configErr != nil || parent.Err() != nil {
		c.closed = true
		c.signalLocked()
		return
	}
	c.ctx, c.cancel = context.WithCancel(parent)
	c.started = true
	c.workers++
	go func() { defer c.workerReturned(OwnerKey{}); c.expiryLoop() }()
	c.signalLocked()
}

// Fence refuses new work and cancels the lifetime without releasing ownership.
func (c *Coordinator) Fence() {
	c.mu.Lock()
	wasClosed := c.closed
	c.closed = true
	cancel := c.cancel
	if !wasClosed {
		c.signalLocked()
	}
	c.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// Stop joins reservations and operations; a timed-out stop retains ownership
// for a later join. Cancellation is never proof that a worker has returned.
func (c *Coordinator) Stop(ctx context.Context) error {
	c.Fence()
	c.cancelReceives(OwnerKey{})
	return c.joinSnapshotOwnership(ctx, OwnerKey{})
}

func (c *Coordinator) signalLocked() {
	if c.waiters == 0 {
		return
	}
	if c.changed != nil {
		close(c.changed)
	}
	c.changed = make(chan struct{})
}

func (c *Coordinator) liveLocked() error {
	if c.configErr != nil {
		return c.configErr
	}
	if c.closed {
		return &snapshot.TransferError{Reason: snapshot.ReasonStopped}
	}
	if !c.started || c.ctx == nil {
		return fault.ErrReplicaNotReady
	}
	if c.ctx.Err() != nil {
		return &snapshot.TransferError{Reason: snapshot.ReasonStopped, Cause: c.ctx.Err()}
	}
	return nil
}

func (c *Coordinator) reserve(
	direction snapshotDirection, owner snapshotOwner, n uint64,
) (*snapshotReservation, error) {
	// These private-owner calls are outside the coordinator lock.
	if owner.handle == nil || owner.handle.Key() != owner.key || !validOwnerKey(owner.key) {
		return nil, transferError(snapshot.ReasonIdentity)
	}
	ownerCtx := owner.handle.Context()
	if ownerCtx == nil || ownerCtx.Err() != nil {
		return nil, transferError(snapshot.ReasonStopped)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.liveLocked(); err != nil {
		return nil, err
	}
	if ownerCtx.Err() != nil {
		return nil, transferError(snapshot.ReasonStopped)
	}
	if _, fenced := c.fencedOwners[owner.key]; fenced {
		return nil, transferError(snapshot.ReasonStopped)
	}
	group := owner.key.Peer.Replica.GroupID
	if err := c.checkReservationLocked(direction, group, n); err != nil {
		return nil, err
	}

	r := &snapshotReservation{direction: direction, bytes: n, owner: owner, coordinator: c}
	c.reservations[r] = struct{}{}
	if direction == incoming {
		c.incomingCount++
		c.receiveBytes += n
		c.groupReservations[group] = r
	} else {
		c.outgoingCount++
		c.sendBytes += n
	}
	c.signalLocked()
	return r, nil
}

func (c *Coordinator) release(r *snapshotReservation) {
	if r == nil || r.coordinator != c {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if r.released {
		return
	}
	if _, owned := c.reservations[r]; !owned {
		return
	}
	r.released = true
	delete(c.reservations, r)
	if r.direction == incoming {
		c.incomingCount--
		c.receiveBytes -= r.bytes
		group := r.owner.key.Peer.Replica.GroupID
		if c.groupReservations[group] == r {
			delete(c.groupReservations, group)
		}
	} else {
		c.outgoingCount--
		c.sendBytes -= r.bytes
	}
	r.owner.handle = nil
	c.signalLocked()
}

func validOwnerKey(key OwnerKey) bool {
	return boundedSnapshotText(key.RunID) && validSnapshotPeer(key.Peer)
}
func transferError(reason string) error { return &snapshot.TransferError{Reason: reason} }

// nextAttachmentLocked never reuses a generation after integer exhaustion.
func (c *Coordinator) nextAttachmentLocked() (uint64, error) {
	if c.sequence == math.MaxUint64 {
		return 0, transferError(snapshot.ReasonTransferConflict)
	}
	c.sequence++
	return c.sequence, nil
}

func (c *Coordinator) checkReservationLocked(direction snapshotDirection, group string, n uint64) error {
	if n == 0 {
		return transferError(snapshot.ReasonMalformed)
	}
	if n > c.options.MaxSnapshotBytes {
		return &snapshot.TransferError{Reason: snapshot.ReasonSizeLimit,
			Size: n, Limit: c.options.MaxSnapshotBytes}
	}
	switch direction {
	case incoming:
		if _, busy := c.groupReservations[group]; busy {
			return transferError(snapshot.ReasonGroupBusy)
		}
		if c.incomingCount >= c.options.MaxIncoming || c.receiveBytes > c.options.MaxReceiveBytes ||
			n > c.options.MaxReceiveBytes-c.receiveBytes {
			return transferError(snapshot.ReasonCapacity)
		}
	case outgoing:
		if c.outgoingCount >= c.options.MaxOutgoing || c.sendBytes > c.options.MaxSendBytes ||
			n > c.options.MaxSendBytes-c.sendBytes {
			return transferError(snapshot.ReasonCapacity)
		}
	default:
		return transferError(snapshot.ReasonMalformed)
	}
	return nil
}
