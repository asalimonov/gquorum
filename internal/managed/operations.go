package managed

import (
	"context"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

type groupOperation struct {
	token chan struct{}
	users int
}

type ownedWork struct {
	Context context.Context
	Release func()
}

type workLifetime uint8

const (
	ordinaryWork workLifetime = iota
	authorityWork
)

// Outbound work is joined without taking a local group gate: a local transport
// shortcut may dispatch back to ApplyAction, which owns that gate itself.
func (h *Host) beginWork(parent context.Context) (ownedWork, error) {
	return h.beginOwnedWork(parent, ordinaryWork)
}

func (h *Host) beginAuthorityWork(parent context.Context) (ownedWork, error) {
	return h.beginOwnedWork(parent, authorityWork)
}

// Host.mu protects admission and the matching join channel, including owned
// workers which outlive their requesting courier. Both lifetimes share group
// gates, but participation fencing cancels only ordinary work.
func (h *Host) admitWorkLocked(lifetime workLifetime) (context.Context, error) {
	if !h.started || h.shutdownRequested || h.ctx.Err() != nil {
		return nil, fault.ErrStopped
	}
	if lifetime == authorityWork {
		if h.authorityClosed || h.authorityContext.Err() != nil {
			return nil, fault.ErrStopped
		}
		if h.authorityCount == 0 {
			h.authorityDone = make(chan struct{})
		}
		h.authorityCount++
		return h.authorityContext, nil
	}
	if h.quiescing || h.stopped {
		return nil, fault.ErrStopped
	}
	if h.operationCount == 0 {
		h.operationsDone = make(chan struct{})
	}
	h.operationCount++
	return h.operationContext, nil
}

func (h *Host) releaseWorkLocked(lifetime workLifetime) {
	if lifetime == authorityWork {
		h.authorityCount--
		if h.authorityCount == 0 {
			close(h.authorityDone)
		}
		return
	}
	h.operationCount--
	if h.operationCount == 0 {
		close(h.operationsDone)
	}
}

func (h *Host) beginOwnedWork(parent context.Context, lifetime workLifetime) (ownedWork, error) {
	if err := parent.Err(); err != nil {
		return ownedWork{}, err
	}
	h.mu.Lock()
	ctxLifetime, err := h.admitWorkLocked(lifetime)
	if err != nil {
		h.mu.Unlock()
		return ownedWork{}, err
	}
	h.mu.Unlock()
	ctx, cancel := context.WithCancel(parent)
	stop := context.AfterFunc(ctxLifetime, cancel)
	return ownedWork{Context: ctx, Release: func() {
		stop()
		cancel()
		h.mu.Lock()
		h.releaseWorkLocked(lifetime)
		h.mu.Unlock()
	}}, nil
}

// beginOperation serializes an entire local effect with start/stop admission.
// Registered waiters and callbacks are canceled and joined by Quiesce. The
// registry mutex is never held while waiting or calling application code.
func (h *Host) beginOperation(parent context.Context, group string) (ownedWork, error) {
	return h.beginOwnedOperation(parent, group, ordinaryWork)
}

func (h *Host) beginAuthorityOperation(parent context.Context, group string) (ownedWork, error) {
	return h.beginOwnedOperation(parent, group, authorityWork)
}

func (h *Host) beginOwnedOperation(parent context.Context, group string,
	lifetime workLifetime) (ownedWork, error) {
	if err := parent.Err(); err != nil {
		return ownedWork{}, err
	}
	if err := topology.ValidateLoadID(group); err != nil {
		return ownedWork{}, err
	}
	h.mu.Lock()
	ctxLifetime, err := h.admitWorkLocked(lifetime)
	if err != nil {
		h.mu.Unlock()
		return ownedWork{}, err
	}
	gate := h.operations[group]
	if gate == nil {
		gate = &groupOperation{token: make(chan struct{}, 1)}
		h.operations[group] = gate
	}
	gate.users++
	h.mu.Unlock()
	ctx, cancel := context.WithCancel(parent)
	stop := context.AfterFunc(ctxLifetime, cancel)
	acquired := false
	release := func() {
		stop()
		cancel()
		if acquired {
			<-gate.token
		}
		h.mu.Lock()
		gate.users--
		if gate.users == 0 {
			delete(h.operations, group)
		}
		h.releaseWorkLocked(lifetime)
		h.mu.Unlock()
	}
	select {
	case gate.token <- struct{}{}:
		acquired = true
	case <-ctx.Done():
		release()
		return ownedWork{}, ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		release()
		return ownedWork{}, err
	}
	return ownedWork{Context: ctx, Release: release}, nil
}

func (h *Host) reserveStart(parent context.Context, ref load.ReplicaRef, why StartReason) (*replica, error) {
	if err := topology.ValidateLoadID(ref.GroupID); err != nil {
		return nil, err
	}
	if ref.ReplicaID == 0 || why > StartOperator {
		return nil, fault.ErrInvalid
	}
	if why != StartOperator {
		if err := h.reconcileStartupAuthority(parent, ref); err != nil {
			return nil, err
		}
	}
	// A pending fence may hold the group gate while joining an old
	// application callback. Refuse new admission immediately; the check in
	// acceptStart still revalidates after acquiring the gate.
	node := h.cfg.Identity()
	h.mu.Lock()
	var err error
	if !h.started || h.quiescing || h.stopped || h.shutdownRequested || h.ctx.Err() != nil {
		err = fault.ErrStopped
	} else {
		_, err = h.startAuthority(parent,
			storage.Identity{NodeName: node.NodeName, ClusterID: node.ClusterID, GroupID: ref.GroupID,
				ReplicaID: ref.ReplicaID}, node)
	}
	h.mu.Unlock()
	if err != nil {
		return nil, err
	}
	ctxHandle, err := h.beginOperation(parent, ref.GroupID)
	ctx, done := ctxHandle.Context, ctxHandle.Release
	if err != nil {
		return nil, err
	}
	defer done()
	return h.acceptStart(ctx, ref, why)
}
