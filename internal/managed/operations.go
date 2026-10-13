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

// Outbound work is joined without taking a local group gate: a local transport
// shortcut may dispatch back to ApplyAction, which owns that gate itself.
func (h *Host) beginWork(parent context.Context) (context.Context, func(), error) {
	return h.beginOwnedWork(parent, false)
}

func (h *Host) beginAuthorityWork(parent context.Context) (context.Context, func(), error) {
	return h.beginOwnedWork(parent, true)
}

// Host.mu protects admission and the matching join channel, including owned
// workers which outlive their requesting courier. Both lifetimes share group
// gates, but participation fencing cancels only ordinary work.
func (h *Host) admitWorkLocked(authority bool) (context.Context, error) {
	if !h.started || h.shutdownRequested || h.ctx.Err() != nil {
		return nil, fault.ErrStopped
	}
	if authority {
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

func (h *Host) releaseWorkLocked(authority bool) {
	if authority {
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

func (h *Host) beginOwnedWork(parent context.Context, authority bool) (context.Context, func(), error) {
	if err := parent.Err(); err != nil {
		return nil, nil, err
	}
	h.mu.Lock()
	lifetime, err := h.admitWorkLocked(authority)
	if err != nil {
		h.mu.Unlock()
		return nil, nil, err
	}
	h.mu.Unlock()
	ctx, cancel := context.WithCancel(parent)
	stop := context.AfterFunc(lifetime, cancel)
	return ctx, func() {
		stop()
		cancel()
		h.mu.Lock()
		h.releaseWorkLocked(authority)
		h.mu.Unlock()
	}, nil
}

// beginOperation serializes an entire local effect with start/stop admission.
// Registered waiters and callbacks are canceled and joined by Quiesce. The
// registry mutex is never held while waiting or calling application code.
func (h *Host) beginOperation(parent context.Context, group string) (context.Context, func(), error) {
	return h.beginOwnedOperation(parent, group, false)
}

func (h *Host) beginAuthorityOperation(parent context.Context, group string) (context.Context, func(), error) {
	return h.beginOwnedOperation(parent, group, true)
}

func (h *Host) beginOwnedOperation(parent context.Context, group string, authority bool) (context.Context, func(), error) {
	if err := parent.Err(); err != nil {
		return nil, nil, err
	}
	if err := topology.ValidateLoadID(group); err != nil {
		return nil, nil, err
	}
	h.mu.Lock()
	lifetime, err := h.admitWorkLocked(authority)
	if err != nil {
		h.mu.Unlock()
		return nil, nil, err
	}
	gate := h.operations[group]
	if gate == nil {
		gate = &groupOperation{token: make(chan struct{}, 1)}
		h.operations[group] = gate
	}
	gate.users++
	h.mu.Unlock()
	ctx, cancel := context.WithCancel(parent)
	stop := context.AfterFunc(lifetime, cancel)
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
		h.releaseWorkLocked(authority)
		h.mu.Unlock()
	}
	select {
	case gate.token <- struct{}{}:
		acquired = true
	case <-ctx.Done():
		release()
		return nil, nil, ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		release()
		return nil, nil, err
	}
	return ctx, release, nil
}

func (h *Host) reserveStart(ctx context.Context, ref load.ReplicaRef, why StartReason) (*replica, error) {
	if err := topology.ValidateLoadID(ref.GroupID); err != nil {
		return nil, err
	}
	if ref.ReplicaID == 0 || why > StartOperator {
		return nil, fault.ErrInvalid
	}
	if why != StartOperator {
		if err := h.reconcileStartupAuthority(ctx, ref); err != nil {
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
		_, err = h.startAuthority(ctx, storage.Identity{NodeName: node.NodeName, ClusterID: node.ClusterID, GroupID: ref.GroupID, ReplicaID: ref.ReplicaID}, node)
	}
	h.mu.Unlock()
	if err != nil {
		return nil, err
	}
	ctx, done, err := h.beginOperation(ctx, ref.GroupID)
	if err != nil {
		return nil, err
	}
	defer done()
	return h.acceptStart(ctx, ref, why)
}
