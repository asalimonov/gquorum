package gquorum

import (
	"context"
	"slices"
	"sync"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/lifecycle"
)

// lifecycleRunner is the single admission gate shared by readiness, retention
// and removal. Deadlines cancel calls; ownership ends only when code returns.
// Node and the managed host can use different workers without overlapping calls
// to the same selected hooks. No node/storage mutex is held during caller code.
type lifecycleRunner struct {
	hooks    lifecycle.Hooks
	timeout  time.Duration
	ctx      context.Context
	cancel   context.CancelFunc
	gate     chan struct{}
	mu       sync.Mutex
	closed   bool
	active   int
	done     chan struct{}
	callback context.Context
	kind     string
}

func newLifecycleRunner(parent context.Context, selected lifecycle.Hooks, timeout time.Duration) *lifecycleRunner {
	ctx, cancel := context.WithCancel(parent)
	r := &lifecycleRunner{
		hooks:   selected,
		timeout: timeout,
		ctx:     ctx,
		cancel:  cancel,
		gate:    make(chan struct{}, 1),
		done:    make(chan struct{}),
	}
	close(r.done)
	return r
}

type lifecycleCall struct {
	Context context.Context
	Release func()
}

func (r *lifecycleRunner) begin(parent context.Context, kind string) (lifecycleCall, error) {
	if err := parent.Err(); err != nil {
		return lifecycleCall{}, err
	}
	r.mu.Lock()
	if r.closed || r.ctx.Err() != nil {
		r.mu.Unlock()
		return lifecycleCall{}, fault.ErrStopped
	}
	if r.active == 0 {
		r.done = make(chan struct{})
	}
	r.active++
	r.mu.Unlock()
	ctx, cancel := context.WithTimeout(parent, r.timeout)
	joined := make(chan struct{})
	stop := context.AfterFunc(r.ctx, func() { cancel(); close(joined) })
	release := func() {
		if !stop() {
			<-joined
		}
		cancel()
		r.mu.Lock()
		r.active--
		if r.active == 0 {
			close(r.done)
		}
		r.mu.Unlock()
	}
	select {
	case r.gate <- struct{}{}:
		if r.ctx.Err() != nil {
			cancel()
		}
		if err := ctx.Err(); err != nil {
			<-r.gate
			release()
			return lifecycleCall{}, err
		}
		r.mu.Lock()
		r.callback, r.kind = ctx, kind
		r.mu.Unlock()
		return lifecycleCall{Context: ctx, Release: func() {
			r.mu.Lock()
			r.callback, r.kind = nil, ""
			r.mu.Unlock()
			<-r.gate
			release()
		}}, nil
	case <-ctx.Done():
		release()
		return lifecycleCall{}, ctx.Err()
	}
}

// Error exposes a stalled callback without detaching it or admitting a retry.
func (r *lifecycleRunner) Error() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.callback != nil && r.callback.Err() != nil {
		return fault.New(fault.Unavailable, "lifecycle "+r.kind+" callback has not returned after cancellation",
			r.callback.Err())
	}
	return nil
}

func (r *lifecycleRunner) Stop(ctx context.Context) error {
	r.mu.Lock()
	r.closed = true
	r.cancel()
	done := r.done
	r.mu.Unlock()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fault.New(fault.ShutdownTimeout, "lifecycle callback has not returned", ctx.Err())
	}
}

func (r *lifecycleRunner) Readiness(parent context.Context,
	in lifecycle.ReadinessInput) (lifecycle.ReadinessDecision, error) {
	call, err := r.begin(parent, "readiness")
	ctx, done := call.Context, call.Release
	if err != nil {
		return lifecycle.ReadinessDecision{}, err
	}
	defer done()
	in.MissingFounders = slices.Clone(in.MissingFounders)
	out, err := r.hooks.Readiness(ctx, in)
	if err == nil {
		err = ctx.Err()
	}
	if err != nil {
		return lifecycle.ReadinessDecision{}, err
	}
	switch out.State {
	case lifecycle.Ready, lifecycle.Waiting, lifecycle.Failed:
		return out, nil
	default:
		return lifecycle.ReadinessDecision{}, fault.New(fault.Invalid, "invalid lifecycle readiness decision", nil)
	}
}

func (r *lifecycleRunner) Retention(parent context.Context,
	in lifecycle.RetentionInput) (lifecycle.RetentionDecision, error) {
	call, err := r.begin(parent, "retention")
	ctx, done := call.Context, call.Release
	if err != nil {
		return lifecycle.RetentionDecision{}, err
	}
	defer done()
	out, err := r.hooks.Retention(ctx, in)
	if err == nil {
		err = ctx.Err()
	}
	if err != nil {
		return lifecycle.RetentionDecision{}, err
	}
	return out, nil
}

func (r *lifecycleRunner) Removal(parent context.Context,
	in lifecycle.RemovalInput) (lifecycle.RemovalDecision, error) {
	// A custom hooks never gets to waive the mandatory exact removal fence.
	id, removal := in.Identity, in.Removal
	if !in.Fenced || !removal.Fenced || id.NodeName == "" || id.ClusterID == "" || id.InstanceID == "" ||
		id.MemberID == 0 || removal.MemberID != id.MemberID || removal.Version == 0 || removal.AppliedIndex == 0 ||
		removal.Reason == "" {
		return lifecycle.RemovalDecision{}, fault.ErrIdentityMismatch
	}
	call, err := r.begin(parent, "removal")
	ctx, done := call.Context, call.Release
	if err != nil {
		return lifecycle.RemovalDecision{}, err
	}
	defer done()
	out, err := r.hooks.Removal(ctx, in)
	if err == nil {
		err = ctx.Err()
	}
	if err != nil {
		return lifecycle.RemovalDecision{}, err
	}
	if out.Action != lifecycle.StopNode && out.Action != lifecycle.KeepDiagnostics {
		return lifecycle.RemovalDecision{}, fault.New(fault.Invalid, "invalid lifecycle removal decision", nil)
	}
	return out, nil
}
