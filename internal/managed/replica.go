package managed

import (
	"context"
	"errors"
	"reflect"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/loadhandle"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/internal/metastore"
	"github.com/asalimonov/gquorum/internal/raftengine"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3"
)

// StartReason distinguishes explicit, recovery and approved-operator startup
// requests.
type StartReason uint8

// StartExplicit and the following reasons identify the origin of replica
// startup.
const (
	StartExplicit StartReason = iota
	StartRecovery
	StartOperator
)

// StopReason distinguishes an explicit local hold from an approved operator
// stop.
type StopReason uint8

// StopExplicit and StopOperator identify local-hold and approved-plan stops.
const (
	StopExplicit StopReason = iota
	StopOperator
)

type replica struct {
	runDone           chan struct{}
	runCancel         context.CancelFunc
	authorityKey      topology.AuthorityKey
	startGrant        *topology.AuthorityGrant
	startSequence     uint64
	identity          storage.Identity
	node              topology.Identity
	store             storage.Store // resolved from the approved storage class by preflight
	ctx               context.Context
	cancel            context.CancelFunc
	startDone         chan struct{}
	state             string
	err               error
	core              *raftengine.Group
	coreJoined        bool
	handle            *loadhandle.Handle
	app               load.Application
	ownsApp, released bool
	release           *releaseAttempt
	deliveries        map[uint64]delivery
	deliveryOrder     []uint64 // ordinary tokens in registration order; snapshots never enter this FIFO
	startGuard        func(context.Context) error
}
type releaseAttempt struct {
	done chan struct{}
	err  error // immutable after done closes
}

// StartReplica reserves an exact approved replica and waits for its shared
// startup result.
func (h *Host) StartReplica(ctx context.Context, ref load.ReplicaRef, why StartReason) (*loadhandle.Handle, error) {
	slot, err := h.reserveStart(ctx, ref, why)
	if err != nil {
		return nil, err
	}
	return h.waitStarted(ctx, slot)
}

func (h *Host) waitStarted(ctx context.Context, slot *replica) (*loadhandle.Handle, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-slot.startDone:
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if slot.state == "ready" && slot.handle != nil {
		return slot.handle, nil
	}
	if slot.err != nil {
		return nil, slot.err
	}
	return nil, fault.ErrStopped
}

func (h *Host) acceptStart(ctx context.Context, ref load.ReplicaRef, why StartReason) (*replica, error) {
	return h.acceptGuardedStart(ctx, ref, why, nil)
}

func (h *Host) acceptGuardedStart(ctx context.Context, ref load.ReplicaRef, why StartReason,
	guard func(context.Context) error) (*replica, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if why > StartOperator {
		return nil, fault.ErrInvalid
	}
	node := h.cfg.Identity()
	id := storage.Identity{NodeName: node.NodeName, ClusterID: node.ClusterID, GroupID: ref.GroupID,
		ReplicaID: ref.ReplicaID}
	if _, err := h.startScope(ctx, node, id); err != nil {
		return nil, err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.started {
		return nil, fault.ErrReplicaNotReady
	}
	if h.quiescing || h.stopped || h.ctx.Err() != nil {
		return nil, fault.ErrStopped
	}
	if err := h.recoveryStartAllowedLocked(); err != nil {
		return nil, err
	}
	grant, err := h.startAuthority(ctx, id, node)
	if err != nil {
		return nil, err
	}
	slot := h.slots[id.GroupID]
	if slot != nil && !slot.released && (slot.identity != id || slot.node != node) {
		return nil, fault.ErrIdentityMismatch
	}
	if why == StartExplicit {
		delete(h.holds, id)
		delete(h.runFailures, id)
	}
	if h.holds[id] {
		return nil, fault.New(fault.ReplicaNotReady, "local replica is explicitly held", nil)
	}
	if existing, err := reusableStart(slot); err != nil || existing != nil {
		return existing, err
	}
	owned, cancel := context.WithCancel(h.ctx)
	slot = &replica{
		identity:   id,
		node:       node,
		ctx:        owned,
		cancel:     cancel,
		startDone:  make(chan struct{}),
		state:      "starting",
		deliveries: make(map[uint64]delivery),
	}
	if grant != nil {
		slot.authorityKey, slot.startGrant = grant.Key, grant
	}
	slot.startGuard = guard
	h.slots[id.GroupID] = slot
	go h.runReplicaStartup(slot)
	return slot, nil
}

func (h *Host) startScope(ctx context.Context, node topology.Identity,
	id storage.Identity) (metaengine.LoadScope, error) {
	if err := topology.ValidateLoadID(id.GroupID); err != nil {
		return metaengine.LoadScope{}, err
	}
	if id.ReplicaID == 0 {
		return metaengine.LoadScope{}, fault.ErrInvalid
	}
	if !completeIdentity(node) {
		return metaengine.LoadScope{}, fault.ErrReplicaNotReady
	}
	if h.cfg.Identity() != node {
		return metaengine.LoadScope{}, fault.ErrIdentityMismatch
	}
	scope, err := h.cfg.Scope(ctx, id)
	if err != nil {
		return scope, err
	}
	if scope.Version == 0 || scope.Group.ID != id.GroupID || scope.Replica.ReplicaID != id.ReplicaID ||
		scope.Replica.NodeName != id.NodeName {
		return metaengine.LoadScope{}, fault.ErrIdentityMismatch
	}
	scope.Group = (topology.Document{Groups: []topology.Group{scope.Group}}).Clone().Groups[0]
	return scope, ctx.Err()
}

func (h *Host) runReplicaStartup(slot *replica) {
	h.notifyChange()
	defer h.notifyChange()
	err := h.openReplica(slot)
	if err == nil {
		h.mu.Lock()
		active := slot.ctx.Err() == nil
		if active && slot.state == "starting" && h.checkStartupFloor(slot) == nil {
			finishErr := h.finishStartupEffect(slot)
			if finishErr == nil {
				slot.handle, slot.state = loadhandle.New(slot.core, slot.identity), "ready"
				delete(h.parked, slot.identity.GroupID)
				h.startApplicationRunLocked(slot)
				close(slot.startDone)
				h.mu.Unlock()
				return
			}
			err = finishErr
		}
		h.mu.Unlock()
		if err == nil {
			err = fault.ErrStopped
		}
	}
	slot.cancel()
	releaseErr := h.releaseResources(slot)
	h.mu.Lock()
	slot.err = errors.Join(err, releaseErr)
	if slot.state != "stopping" {
		slot.state = "failed"
		var parked *ParkedError
		if errors.As(err, &parked) {
			slot.state = "parked"
		}
	}
	close(slot.startDone)
	h.mu.Unlock()
}

func (h *Host) openReplica(slot *replica) error {
	ctx := slot.ctx
	if err := h.checkStartupFloor(slot); err != nil {
		return err
	}
	preflight, err := h.preflight(ctx, slot)
	if err != nil {
		return err
	}
	scope, boot, probe := preflight.scope, preflight.boot, preflight.probe
	if err := h.checkGuardedStartup(slot); err != nil {
		return err
	}
	storageState := freshReplicaStorage
	if probe.Exists {
		storageState = retainedReplicaStorage
	}
	if err := h.beginStartupEffect(slot, storageState); err != nil {
		return err
	}
	app, err := h.acquireApplication(slot, scope)
	if err != nil {
		return err
	}
	current, err := h.startScope(ctx, slot.node, slot.identity)
	if err != nil {
		return err
	}
	// Version is document context for the factory. Independent approvals may
	// advance it while this exact assignment and startup seed remain unchanged.
	if current.Replica != scope.Replica || !probe.HasLog && current.Founding != scope.Founding ||
		!reflect.DeepEqual(current.Group, scope.Group) {
		return fault.ErrVersionConflict
	}
	// Metadata may learn a retained creation receipt while the factory waits.
	// An unchanged declaration cannot hide newly proven missing durable state.
	if _, err := h.authorizeBoot(slot, bootEvidence{probe: probe, scope: current}); err != nil {
		return err
	}
	if err := h.checkGuardedStartup(slot); err != nil {
		return err
	}
	seed := replicaMembership(current, probe, slot.startGrant)
	core, err := raftengine.NewContext(ctx, h.engineOptions(slot, app, seed.mode, seed.peers))
	if err != nil {
		return h.park(slot, parkReason(err), err)
	}
	h.mu.Lock()
	slot.core = core
	h.mu.Unlock()
	return h.startReplicaEngine(slot, core, boot)
}

func (h *Host) engineOptions(slot *replica, app load.Application, mode raftengine.Mode,
	peers []raft.Peer) raftengine.Options {
	options := raftengine.Options{
		NodeName:            slot.identity.NodeName,
		ClusterID:           slot.identity.ClusterID,
		GroupID:             slot.identity.GroupID,
		ReplicaID:           slot.identity.ReplicaID,
		InstanceID:          slot.node.InstanceID,
		Mode:                mode,
		Peers:               peers,
		Metrics:             h.cfg.Metrics.Local(),
		Store:               slot.store,
		Machine:             machineAdapter{app.StateMachine},
		Transport:           engineTransport{h, slot},
		Timing:              raftengine.Timing(h.cfg.Engine.Timing),
		SnapshotThreshold:   h.cfg.Engine.SnapshotThreshold,
		MaxPendingProposals: h.cfg.Engine.MaxPendingProposals,
		MaxWatchers:         h.cfg.Engine.MaxWatchers,
		ProposalTimeout:     h.cfg.Engine.ProposalTimeout,
		Reads:               raftengine.ReadOptions(h.cfg.Engine.Reads),
		MaxUncommittedBytes: h.cfg.Engine.MaxUncommittedBytes,
		MaxApplyBatchBytes:  h.cfg.Engine.MaxApplyBatchBytes,
		Events:              h.cfg.Events,
		Logger:              h.cfg.Logger,
	}
	options.ManagedAuthority, options.MemberID = true, slot.node.MemberID
	if resolve := h.cfg.ResolvePeer; resolve != nil {
		groupID := slot.identity.GroupID
		options.PeerName = func(ctx context.Context, replicaID uint64) (string, error) {
			peer, err := resolve(ctx, load.ReplicaRef{GroupID: groupID, ReplicaID: replicaID})
			return peer.Node.NodeName, err
		}
	}
	if mode == raftengine.Founding {
		options.InitialAuthority = slot.startGrant
	}
	return options
}

type machineAdapter struct{ load.StateMachine }

func (m machineAdapter) Apply(ctx context.Context, command raftengine.Command) (raftengine.Result, error) {
	result, err := m.StateMachine.Apply(ctx, load.Command{Index: command.Index, Term: command.Term,
		RequestID: command.RequestID, Data: command.Data})
	return raftengine.Result(result), err
}

// StopReplica serializes teardown of an exact replica and installs a hold for
// explicit stops.
func (h *Host) StopReplica(parent context.Context, ref load.ReplicaRef, why StopReason) error {
	ctxHandle, err := h.beginOperation(parent, ref.GroupID)
	ctx, done := ctxHandle.Context, ctxHandle.Release
	if err != nil {
		return err
	}
	defer done()
	return h.stopOwnedReplica(ctx, ref, why)
}

func (h *Host) stopOwnedReplica(ctx context.Context, ref load.ReplicaRef, why StopReason) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if why > StopOperator || ref.ReplicaID == 0 {
		return fault.ErrInvalid
	}
	if err := topology.ValidateLoadID(ref.GroupID); err != nil {
		return err
	}
	node := h.cfg.Identity()
	id := storage.Identity{NodeName: node.NodeName, ClusterID: node.ClusterID, GroupID: ref.GroupID,
		ReplicaID: ref.ReplicaID}
	h.mu.Lock()
	known := h.slots[ref.GroupID]
	owned := known != nil && known.identity == id && known.node == node
	h.mu.Unlock()
	if why == StopExplicit && !owned {
		if _, err := h.startScope(ctx, node, id); err != nil {
			return err
		}
	}
	h.mu.Lock()
	slot := h.slots[ref.GroupID]
	if slot != nil && (slot.identity != id || slot.node != node) {
		h.mu.Unlock()
		return fault.ErrIdentityMismatch
	}
	if why == StopExplicit {
		h.holds[id] = true
	}
	if slot == nil {
		h.mu.Unlock()
		return nil
	}
	attempt := h.beginStopLocked(slot)
	h.mu.Unlock()
	return waitRelease(ctx, attempt)
}

// beginStopLocked reserves one teardown. Callbacks run without the registry
// mutex; a concurrent Stop waits on this attempt and a later retry may retry
// Close.
func (h *Host) beginStopLocked(slot *replica) *releaseAttempt {
	if slot.release != nil {
		select {
		case <-slot.release.done:
		default:
			return slot.release
		}
	}
	attempt := &releaseAttempt{done: make(chan struct{})}
	slot.release, slot.state = attempt, "stopping"
	slot.cancel()
	go func() {
		<-slot.startDone
		h.notifyChange()
		defer h.notifyChange()
		err := h.releaseResources(slot)
		h.mu.Lock()
		if err == nil {
			if h.slots[slot.identity.GroupID] == slot {
				delete(h.slots, slot.identity.GroupID)
			}
		} else {
			slot.err = err
		}
		attempt.err = err
		close(attempt.done)
		h.mu.Unlock()
	}()
	return attempt
}

func waitRelease(ctx context.Context, attempt *releaseAttempt) error {
	select {
	case <-attempt.done:
		return attempt.err
	case <-ctx.Done():
		return fault.New(fault.ShutdownTimeout, "replica teardown has not joined", ctx.Err())
	}
}

// Only the startup worker or its joined, serialized teardown calls this method.
// A timeout retains ownership; no successor may overlap unresolved engine work
// or application Close. Borrowed storage and transport are never closed here.
func (h *Host) releaseResources(slot *replica) error {
	h.mu.Lock()
	core, joined, app, owned := slot.core, slot.coreJoined, slot.app, slot.ownsApp
	runDone, runCancel := slot.runDone, slot.runCancel
	h.mu.Unlock()
	if runCancel != nil {
		runCancel()
	}
	if core != nil && !joined {
		ctx, cancel := context.WithTimeout(context.Background(), h.cfg.EffectTimeout)
		err := core.Stop(ctx)
		cancel()
		if err != nil {
			return err
		}
		h.mu.Lock()
		slot.coreJoined = true
		clear(slot.deliveries)
		slot.deliveryOrder = nil
		h.mu.Unlock()
	}
	if runDone != nil {
		ctx, cancel := context.WithTimeout(context.Background(), h.cfg.CallbackTimeout)
		select {
		case <-runDone:
			cancel()
		case <-ctx.Done():
			err := fault.New(fault.ShutdownTimeout, "application Run has not joined", ctx.Err())
			cancel()
			return err
		}
	}
	if owned && app.Close != nil {
		ctx, cancel := context.WithTimeout(context.Background(), h.cfg.CallbackTimeout)
		err := app.Close(ctx)
		if err == nil {
			err = ctx.Err()
		}
		cancel()
		if err != nil {
			return err
		}
	}
	h.mu.Lock()
	slot.app, slot.ownsApp, slot.released = load.Application{}, false, true
	slot.handle = nil
	h.mu.Unlock()
	return nil
}

func (m machineAdapter) SupportsQuery() bool {
	_, ok := m.StateMachine.(load.QueryMachine)
	return ok
}

func (m machineAdapter) Query(ctx context.Context, query []byte) (raftengine.Result, error) {
	machine, ok := m.StateMachine.(load.QueryMachine)
	if !ok {
		return raftengine.Result{}, fault.ErrUnsupported
	}
	result, err := machine.Query(ctx, query)
	return raftengine.Result(result), err
}

func reusableStart(slot *replica) (*replica, error) {
	if slot != nil {
		if slot.state == "stopping" || slot.release != nil || slot.state != "starting" && slot.state != "ready" &&
			!slot.released {
			return nil, fault.ErrReplicaNotReady
		}
		if !slot.released {
			return slot, nil
		}
	}
	return nil, nil
}

func (h *Host) checkGuardedStartup(slot *replica) error {
	if err := h.checkStartupFloor(slot); err != nil {
		return err
	}
	if slot.startGuard != nil {
		if err := slot.startGuard(slot.ctx); err != nil {
			return err
		}
	}
	return nil
}

func (h *Host) acquireApplication(slot *replica, scope metaengine.LoadScope) (load.Application, error) {
	ctx := slot.ctx
	spec := load.ReplicaSpec{
		Identity:        slot.identity,
		Node:            slot.node,
		Group:           (topology.Document{Groups: []topology.Group{scope.Group}}).Clone().Groups[0],
		ApprovedVersion: scope.Version,
	}
	call, cancel := context.WithTimeout(ctx, h.cfg.CallbackTimeout)
	app, factoryErr := h.cfg.Factory(call, spec)
	callErr := call.Err()
	cancel()
	// A result returned with no factory error transfers resources even when the
	// call's deadline elapsed. A failed factory owns its partial acquisitions.
	// Account for that distinction before checking the owned startup context.
	if factoryErr != nil {
		return load.Application{}, factoryErr
	}
	h.mu.Lock()
	slot.app, slot.ownsApp = app, true
	h.mu.Unlock()
	if callErr != nil {
		return load.Application{}, callErr
	}
	if app.StateMachine == nil {
		return load.Application{}, fault.New(fault.Invalid, "factory returned no state machine", nil)
	}
	if err := ctx.Err(); err != nil {
		return load.Application{}, err
	}
	return app, nil
}

type replicaRaftSeed struct {
	mode  raftengine.Mode
	peers []raft.Peer
}

func replicaMembership(scope metaengine.LoadScope, probe storage.Probe,
	grant *topology.AuthorityGrant) replicaRaftSeed {
	mode := raftengine.Joining
	var peers []raft.Peer
	if !probe.HasLog && scope.Founding {
		mode = raftengine.Founding
		for _, target := range grant.Targets {
			peers = append(peers, raft.Peer{ID: target.ReplicaID})
		}
	}
	return replicaRaftSeed{mode: mode, peers: peers}
}

func (h *Host) startReplicaEngine(slot *replica, core *raftengine.Group, boot bootResult) error {
	ctx := slot.ctx
	if _, err := h.startScope(ctx, slot.node, slot.identity); err != nil {
		return err
	}
	if err := h.checkGuardedStartup(slot); err != nil {
		return err
	}
	recordReplicaErr := h.cfg.RecordReplica(ctx, slot.identity.GroupID, metastore.ReplicaRecord{
		NodeName: slot.identity.NodeName, ClusterID: slot.identity.ClusterID,
		ReplicaID: slot.identity.ReplicaID, Origin: boot.origin,
		CreatedAt: time.Now().UTC()}, boot.previous)
	if recordReplicaErr != nil {
		return recordReplicaErr
	}
	if err := core.Start(ctx); err != nil {
		return err
	}
	if err := core.WaitReady(ctx); err != nil {
		return err
	}
	_, err := h.startScope(ctx, slot.node, slot.identity)
	if err == nil && slot.startGuard != nil {
		err = slot.startGuard(ctx)
	}
	if err == nil {
		err = h.checkStartupFloor(slot)
	}
	return err
}
