package managed

import (
	"context"
	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/topology"
)

type groupOperation struct {
	token chan struct{}
	users int
}

// Outbound work is joined without taking a local group gate: a local transport
// shortcut may dispatch back to ApplyAction, which owns that gate itself.
func (h *Host) beginWork(parent context.Context) (context.Context, func(), error) {
	if err := parent.Err(); err != nil {
		return nil, nil, err
	}
	h.mu.Lock()
	if !h.started || h.quiescing || h.stopped || h.ctx.Err() != nil {
		h.mu.Unlock()
		return nil, nil, fault.ErrStopped
	}
	if h.operationCount == 0 {
		h.operationsDone = make(chan struct{})
	}
	h.operationCount++
	lifetime := h.operationContext
	h.mu.Unlock()
	ctx, cancel := context.WithCancel(parent)
	stop := context.AfterFunc(lifetime, cancel)
	return ctx, func() {
		stop()
		cancel()
		h.mu.Lock()
		h.operationCount--
		if h.operationCount == 0 {
			close(h.operationsDone)
		}
		h.mu.Unlock()
	}, nil
}

// beginOperation serializes an entire local effect with start/stop admission.
// Registered waiters and callbacks are canceled and joined by Quiesce. The
// registry mutex is never held while waiting or calling application code.
func (h *Host) beginOperation(parent context.Context, group string) (context.Context, func(), error) {
	if err := parent.Err(); err != nil {
		return nil, nil, err
	}
	if err := topology.ValidateLoadID(group); err != nil {
		return nil, nil, err
	}
	h.mu.Lock()
	if !h.started || h.quiescing || h.stopped || h.ctx.Err() != nil {
		h.mu.Unlock()
		return nil, nil, fault.ErrStopped
	}
	gate := h.operations[group]
	if gate == nil {
		gate = &groupOperation{token: make(chan struct{}, 1)}
		h.operations[group] = gate
	}
	gate.users++
	if h.operationCount == 0 {
		h.operationsDone = make(chan struct{})
	}
	h.operationCount++
	lifetime := h.operationContext
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
		h.operationCount--
		if h.operationCount == 0 {
			close(h.operationsDone)
		}
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
	ctx, done, err := h.beginOperation(ctx, ref.GroupID)
	if err != nil {
		return nil, err
	}
	defer done()
	return h.acceptStart(ctx, ref, why)
}
