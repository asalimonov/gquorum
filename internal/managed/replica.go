package managed

import (
	"context"
	"errors"
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

type StartReason uint8

const (
	StartExplicit StartReason = iota
	StartRecovery
	StartOperator
)

type StopReason uint8

const (
	StopExplicit StopReason = iota
	StopOperator
)

type replica struct {
	identity          storage.Identity
	node              topology.Identity
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
	startGuard        func(context.Context) error
}
type releaseAttempt struct {
	done chan struct{}
	err  error // immutable after done closes
}

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

func (h *Host) acceptGuardedStart(ctx context.Context, ref load.ReplicaRef, why StartReason, guard func(context.Context) error) (*replica, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if why > StartOperator {
		return nil, fault.ErrInvalid
	}
	node := h.cfg.Identity()
	id := storage.Identity{NodeName: node.NodeName, ClusterID: node.ClusterID, GroupID: ref.GroupID, ReplicaID: ref.ReplicaID}
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
	slot := h.slots[id.GroupID]
	if slot != nil && !slot.released && (slot.identity != id || slot.node != node) {
		return nil, fault.ErrIdentityMismatch
	}
	if why == StartExplicit {
		delete(h.holds, id)
	}
	if h.holds[id] {
		return nil, fault.New(fault.ReplicaNotReady, "local replica is explicitly held", nil)
	}
	if slot != nil {
		if slot.state == "stopping" || slot.release != nil || slot.state != "starting" && slot.state != "ready" && !slot.released {
			return nil, fault.ErrReplicaNotReady
		}
		if !slot.released {
			return slot, nil
		}
	}
	owned, cancel := context.WithCancel(h.ctx)
	slot = &replica{identity: id, node: node, ctx: owned, cancel: cancel, startDone: make(chan struct{}), state: "starting", deliveries: make(map[uint64]delivery)}
	slot.startGuard = guard
	h.slots[id.GroupID] = slot
	go h.startReplica(slot)
	return slot, nil
}

func (h *Host) startScope(ctx context.Context, node topology.Identity, id storage.Identity) (metaengine.LoadScope, error) {
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
	if scope.Version == 0 || scope.Group.ID != id.GroupID || scope.Replica.ReplicaID != id.ReplicaID || scope.Replica.NodeName != id.NodeName {
		return metaengine.LoadScope{}, fault.ErrIdentityMismatch
	}
	scope.Group = (topology.Document{Groups: []topology.Group{scope.Group}}).Clone().Groups[0]
	return scope, ctx.Err()
}

func (h *Host) startReplica(slot *replica) {
	err := h.openReplica(slot)
	if err == nil {
		h.mu.Lock()
		if slot.ctx.Err() == nil && slot.state == "starting" {
			slot.handle, slot.state = loadhandle.New(slot.core, slot.identity), "ready"
			delete(h.parked, slot.identity.GroupID)
			close(slot.startDone)
			h.mu.Unlock()
			return
		}
		h.mu.Unlock()
		err = fault.ErrStopped
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
	scope, boot, probe, err := h.preflight(ctx, slot)
	if err != nil {
		return err
	}
	if slot.startGuard != nil {
		if err := slot.startGuard(ctx); err != nil {
			return err
		}
	}
	spec := load.ReplicaSpec{Identity: slot.identity, Node: slot.node, Group: (topology.Document{Groups: []topology.Group{scope.Group}}).Clone().Groups[0], ApprovedVersion: scope.Version}
	call, cancel := context.WithTimeout(ctx, h.cfg.CallbackTimeout)
	app, factoryErr := h.cfg.Factory(call, spec)
	callErr := call.Err()
	cancel()
	// A result returned with no factory error transfers resources even when the
	// call's deadline elapsed. A failed factory owns its partial acquisitions.
	// Account for that distinction before checking the owned startup context.
	if factoryErr != nil {
		return factoryErr
	}
	h.mu.Lock()
	slot.app, slot.ownsApp = app, true
	h.mu.Unlock()
	if callErr != nil {
		return callErr
	}
	if app.StateMachine == nil {
		return fault.New(fault.Invalid, "factory returned no state machine", nil)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	current, err := h.startScope(ctx, slot.node, slot.identity)
	if err != nil {
		return err
	}
	if current.Version != scope.Version {
		return fault.ErrVersionConflict
	}
	if slot.startGuard != nil {
		if err := slot.startGuard(ctx); err != nil {
			return err
		}
	}
	mode := raftengine.Joining
	var peers []raft.Peer
	if !probe.HasLog && scope.Founding {
		mode = raftengine.Founding
		for _, r := range scope.Group.Replicas {
			if r.Role == topology.ReplicaVoter {
				peers = append(peers, raft.Peer{ID: r.ReplicaID})
			}
		}
	}
	core, err := raftengine.NewContext(ctx, h.engineOptions(slot, app, mode, peers))
	if err != nil {
		return h.park(slot, parkReason(err), err)
	}
	h.mu.Lock()
	slot.core = core
	h.mu.Unlock()
	if _, err := h.startScope(ctx, slot.node, slot.identity); err != nil {
		return err
	}
	if slot.startGuard != nil {
		if err := slot.startGuard(ctx); err != nil {
			return err
		}
	}
	if err := h.cfg.RecordReplica(ctx, slot.identity.GroupID, metastore.ReplicaRecord{NodeName: slot.identity.NodeName, ClusterID: slot.identity.ClusterID, ReplicaID: slot.identity.ReplicaID, Origin: boot.origin, CreatedAt: time.Now().UTC()}, boot.previous); err != nil {
		return err
	}
	if err := core.Start(ctx); err != nil {
		return err
	}
	if err := core.WaitReady(ctx); err != nil {
		return err
	}
	_, err = h.startScope(ctx, slot.node, slot.identity)
	if err == nil && slot.startGuard != nil {
		err = slot.startGuard(ctx)
	}
	return err
}

func (h *Host) engineOptions(slot *replica, app load.Application, mode raftengine.Mode, peers []raft.Peer) raftengine.Options {
	return raftengine.Options{NodeName: slot.identity.NodeName, ClusterID: slot.identity.ClusterID, GroupID: slot.identity.GroupID, ReplicaID: slot.identity.ReplicaID,
		InstanceID: slot.node.InstanceID, Mode: mode, Peers: peers, Metrics: h.cfg.Metrics.Local(), Store: h.cfg.Store, Machine: machineAdapter{app.StateMachine}, Transport: engineTransport{h, slot},
		Timing: raftengine.Timing(h.cfg.Engine.Timing), SnapshotThreshold: h.cfg.Engine.SnapshotThreshold, MaxPendingProposals: h.cfg.Engine.MaxPendingProposals,
		ProposalTimeout:     h.cfg.Engine.ProposalTimeout,
		MaxUncommittedBytes: h.cfg.Engine.MaxUncommittedBytes, MaxApplyBatchBytes: h.cfg.Engine.MaxApplyBatchBytes, Events: h.cfg.Events, Logger: h.cfg.Logger}
}

type machineAdapter struct{ load.StateMachine }

func (m machineAdapter) Apply(ctx context.Context, command raftengine.Command) (raftengine.Result, error) {
	result, err := m.StateMachine.Apply(ctx, load.Command(command))
	return raftengine.Result(result), err
}

func (h *Host) StopReplica(ctx context.Context, ref load.ReplicaRef, why StopReason) error {
	ctx, done, err := h.beginOperation(ctx, ref.GroupID)
	if err != nil {
		return err
	}
	defer done()
	return h.stopReplica(ctx, ref, why)
}

func (h *Host) stopReplica(ctx context.Context, ref load.ReplicaRef, why StopReason) error {
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
	id := storage.Identity{NodeName: node.NodeName, ClusterID: node.ClusterID, GroupID: ref.GroupID, ReplicaID: ref.ReplicaID}
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
// mutex; a concurrent Stop waits on this attempt and a later retry may retry Close.
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
	h.mu.Unlock()
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
		h.mu.Unlock()
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
